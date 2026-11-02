// Package client connects to a baseDB server.
//
//	c, err := client.Dial("127.0.0.1:7070")
//	defer c.Close()
//	err = c.Put([]byte("user:1"), []byte("Batuhan"))
//	v, ok, err := c.Get([]byte("user:1"))
//
//	err = c.Begin()                       // a transaction on this connection
//	err = c.Put(...)
//	err = c.Commit()                      // or c.Rollback()
//
// A Client is one connection and one session: its transaction belongs to
// it. Use one Client per goroutine that works independently (a Client is
// safe for concurrent use, but commands from several goroutines share its
// transaction and wait for each other).
package client

import (
	"bufio"
	"errors"
	"net"
	"strconv"
	"sync"
	"time"

	"baseDB/internal/protocol"
)

// Errors returned by the server, to test with errors.Is.
var (
	ErrDeadlock = &protocol.Error{Code: protocol.CodeDeadlock} // transaction rolled back: retry it
	ErrTxFailed = &protocol.Error{Code: protocol.CodeTxFailed} // commit failed, nothing applied
	ErrTooLarge = &protocol.Error{Code: protocol.CodeTooLarge}
	ErrNoTx     = &protocol.Error{Code: protocol.CodeNoTx}
	ErrBusy     = &protocol.Error{Code: protocol.CodeBusy} // server has too many connections
	ErrClosed   = &protocol.Error{Code: protocol.CodeClosed}
)

// KV is one key/value pair of a scan.
type KV struct{ Key, Value []byte }

// Client is a connection to a baseDB server.
type Client struct {
	mu   sync.Mutex
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

// Dial connects to the server at addr.
func Dial(addr string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, r: bufio.NewReaderSize(conn, protocol.MaxLine), w: bufio.NewWriter(conn)}, nil
}

// Do sends one command (name and arguments) and returns the server's
// response. An ERR response is returned as a response, not as an error;
// the error is only for network problems. The typed methods below are
// usually more convenient.
func (c *Client) Do(args ...string) (protocol.Response, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.w.WriteString(protocol.Line(args...)); err != nil {
		return protocol.Response{}, err
	}
	if err := c.w.Flush(); err != nil {
		return protocol.Response{}, err
	}
	return protocol.ReadResponse(c.r)
}

// call is Do with ERR responses turned into errors.
func (c *Client) call(args ...string) (protocol.Response, error) {
	resp, err := c.Do(args...)
	if err != nil {
		return resp, err
	}
	return resp, resp.Err()
}

// Put stores value under key. Outside a transaction it is durable when it
// returns.
func (c *Client) Put(key, value []byte) error {
	_, err := c.call("put", string(key), string(value))
	return err
}

// Get returns the value of key and whether it exists.
func (c *Client) Get(key []byte) ([]byte, bool, error) {
	resp, err := c.call("get", string(key))
	if err != nil {
		return nil, false, err
	}
	if resp.Status == protocol.StatusNotFound {
		return nil, false, nil
	}
	return resp.Value, true, nil
}

// Delete removes key and reports whether it existed.
func (c *Client) Delete(key []byte) (bool, error) {
	resp, err := c.call("delete", string(key))
	if err != nil {
		return false, err
	}
	return resp.Status != protocol.StatusNotFound, nil
}

// Scan returns the pairs with start <= key < end (nil = unbounded).
func (c *Client) Scan(start, end []byte) ([]KV, error) {
	args := []string{"scan"}
	switch {
	case end != nil:
		args = append(args, string(start), string(end))
	case start != nil:
		args = append(args, string(start))
	}
	resp, err := c.call(args...)
	if err != nil {
		return nil, err
	}
	out := make([]KV, len(resp.Rows))
	for i, r := range resp.Rows {
		out[i] = KV{Key: r.Key, Value: r.Value}
	}
	return out, nil
}

// Count returns the number of keys in [start, end) (nil = unbounded).
func (c *Client) Count(start, end []byte) (int, error) {
	args := []string{"count"}
	switch {
	case end != nil:
		args = append(args, string(start), string(end))
	case start != nil:
		args = append(args, string(start))
	}
	resp, err := c.call(args...)
	if err != nil {
		return 0, err
	}
	return strconv.Atoi(resp.Message)
}

// Begin starts a transaction on this connection.
func (c *Client) Begin() error {
	_, err := c.call("begin")
	return err
}

// Commit applies the transaction's writes. ErrTxFailed: nothing applied.
func (c *Client) Commit() error {
	_, err := c.call("commit")
	return err
}

// Rollback discards the transaction's writes.
func (c *Client) Rollback() error {
	_, err := c.call("rollback")
	return err
}

// Ping checks that the server answers.
func (c *Client) Ping() error {
	_, err := c.call("ping")
	return err
}

// Close closes the connection. An open transaction is rolled back by the
// server.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.w.WriteString(protocol.Line("quit"))
	c.w.Flush()
	return c.conn.Close()
}

// IsRetryable reports whether err means "the transaction was rolled back
// because of a conflict; running it again may succeed".
func IsRetryable(err error) bool { return errors.Is(err, ErrDeadlock) }
