package server

import (
	"bufio"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"baseDB/internal/engine"
	"baseDB/internal/protocol"
)

// Defaults for Options.
const (
	DefaultMaxConns     = 1000
	DefaultIdleTimeout  = 10 * time.Minute
	DefaultWriteTimeout = 30 * time.Second
)

// Options configures a Server.
type Options struct {
	// MaxConns is the number of clients served at once (default 1000).
	// One more gets "ERR BUSY" and is disconnected.
	MaxConns int
	// IdleTimeout disconnects a client that sends nothing for this long
	// (default 10 minutes). Its open transaction is rolled back, so a client
	// that went away cannot hold locks forever.
	IdleTimeout time.Duration
	// WriteTimeout disconnects a client that does not read its responses
	// (default 30 seconds).
	WriteTimeout time.Duration
	// Dir is the database directory, shown by the stats command.
	Dir string
}

// Stats counts connections.
type Stats struct {
	Accepted uint64 // connections served
	Refused  uint64 // connections refused because MaxConns was reached
	Active   int64  // connections open now
}

// Server serves one database to many TCP clients. Every connection gets
// its own goroutine and Session; the engine's lock manager and group
// commit handle the concurrency between them.
type Server struct {
	db   *engine.DB
	opts Options

	mu     sync.Mutex
	ln     net.Listener
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup

	accepted, refused atomic.Uint64
	active            atomic.Int64
}

// ErrServerClosed is returned by Serve after Close.
var ErrServerClosed = errors.New("server: closed")

// New returns a server for db. The caller still owns db: close it after
// the server.
func New(db *engine.DB, opts Options) *Server {
	if opts.MaxConns <= 0 {
		opts.MaxConns = DefaultMaxConns
	}
	if opts.IdleTimeout == 0 {
		opts.IdleTimeout = DefaultIdleTimeout
	}
	if opts.WriteTimeout == 0 {
		opts.WriteTimeout = DefaultWriteTimeout
	}
	return &Server{db: db, opts: opts, conns: make(map[net.Conn]struct{})}
}

// Listen opens a TCP listener on addr (e.g. "127.0.0.1:7070"; port 0 picks
// a free port) and serves it in the background. Use Addr to see the
// address, Close to stop.
func (s *Server) Listen(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	go s.Serve(ln)
	return nil
}

// Addr returns the listening address (after Listen or Serve).
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// Serve accepts connections on ln until Close.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		ln.Close()
		return ErrServerClosed
	}
	s.ln = ln
	s.mu.Unlock()

	for {
		conn, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			if closed {
				return ErrServerClosed
			}
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				continue
			}
			return err
		}
		if s.active.Load() >= int64(s.opts.MaxConns) {
			s.refused.Add(1)
			conn.SetWriteDeadline(time.Now().Add(time.Second))
			protocol.Err(protocol.CodeBusy, "too many connections (limit %d), try again later", s.opts.MaxConns).Write(conn)
			conn.Close()
			continue
		}
		if !s.track(conn) {
			conn.Close()
			return ErrServerClosed
		}
		s.accepted.Add(1)
		s.active.Add(1)
		go s.handle(conn)
	}
}

func (s *Server) track(conn net.Conn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.conns[conn] = struct{}{}
	s.wg.Add(1)
	return true
}

// Close stops accepting, disconnects every client (their open transactions
// are rolled back) and waits until all connection goroutines are done.
func (s *Server) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	var err error
	if s.ln != nil {
		err = s.ln.Close()
	}
	for c := range s.conns {
		c.Close() // unblocks the connection's read
	}
	s.mu.Unlock()
	s.wg.Wait()
	return err
}

// Stats returns connection counters.
func (s *Server) Stats() Stats {
	return Stats{Accepted: s.accepted.Load(), Refused: s.refused.Load(), Active: s.active.Load()}
}

// handle serves one connection: read a line, run it, write the response.
// Responses are flushed only when no further request is already buffered,
// so a client that pipelines many requests gets their responses in one
// write.
func (s *Server) handle(conn net.Conn) {
	sess := NewSession(s.db, s.opts.Dir)
	defer func() {
		sess.Close() // roll back a transaction the client left open
		conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		s.active.Add(-1)
		s.wg.Done()
	}()

	r := bufio.NewReaderSize(conn, protocol.MaxLine)
	w := bufio.NewWriter(conn)
	for {
		conn.SetReadDeadline(time.Now().Add(s.opts.IdleTimeout))
		line, err := protocol.ReadLine(r)
		if errors.Is(err, protocol.ErrLineTooLong) {
			s.reply(conn, w, protocol.Err(protocol.CodeSyntax, "line longer than %d bytes", protocol.MaxLine))
			w.Flush()
			return // the rest of the line cannot be told apart from the next request
		}
		if err != nil {
			return // client left, idle timeout, or server closing
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		args, err := protocol.Tokenize(line)
		var resp protocol.Response
		switch {
		case err != nil:
			resp = protocol.Err(protocol.CodeSyntax, "%v", err)
		case strings.EqualFold(args[0], "quit") || strings.EqualFold(args[0], "exit"):
			s.reply(conn, w, protocol.OK("bye"))
			w.Flush()
			return
		case strings.EqualFold(args[0], "help"):
			resp = protocol.OK("")
			for _, c := range Commands() {
				resp.Rows = append(resp.Rows, protocol.Row{Key: []byte(c.Usage()), Value: []byte(c.Help)})
			}
		default:
			resp = sess.Exec(args)
		}
		if !s.reply(conn, w, resp) {
			return
		}
		if r.Buffered() == 0 {
			if err := w.Flush(); err != nil {
				return
			}
		}
	}
}

func (s *Server) reply(conn net.Conn, w *bufio.Writer, resp protocol.Response) bool {
	conn.SetWriteDeadline(time.Now().Add(s.opts.WriteTimeout))
	return resp.Write(w) == nil
}
