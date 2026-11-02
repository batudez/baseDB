package main

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"baseDB/client"
	"baseDB/internal/engine"
	"baseDB/internal/protocol"
	"baseDB/internal/server"
)

// backend runs commands. The commands themselves live in server.Session,
// so the terminal program behaves exactly the same whether it opened the
// database itself (local) or talks to a server (remote).
type backend interface {
	Exec(args []string) (protocol.Response, error) // error: connection lost
	Close() error
}

// local: the database is opened in this process.
type local struct {
	db   *engine.DB
	sess *server.Session
}

func (l *local) Exec(args []string) (protocol.Response, error) { return l.sess.Exec(args), nil }
func (l *local) Close() error {
	l.sess.Close()
	return l.db.Close()
}

// remote: a server owns the database; commands go over TCP.
type remote struct{ c *client.Client }

func (r *remote) Exec(args []string) (protocol.Response, error) { return r.c.Do(args...) }
func (r *remote) Close() error                                  { return r.c.Close() }

// shell reads commands and prints their results for a person.
type shell struct {
	b      backend
	out    io.Writer
	where  string // "" for a local database, the server address otherwise
	inTx   bool   // a transaction is open (begin ... commit/rollback)
	single bool   // single-command mode: transactions make no sense
}

// errExit is returned by the exit command.
var errExit = errors.New("exit")

func (s *shell) printf(format string, a ...any) { fmt.Fprintf(s.out, format, a...) }

// prompt is the interactive prompt: where the database is, and whether a
// transaction is open.
func (s *shell) prompt() string {
	p := "basedb"
	if s.where != "" {
		p += "@" + s.where
	}
	if s.inTx {
		p += "(tx)"
	}
	return p + "> "
}

// finish rolls back a transaction left open at exit or end of input.
func (s *shell) finish() {
	if !s.inTx {
		return
	}
	s.inTx = false
	if resp, err := s.b.Exec([]string{"rollback"}); err == nil && resp.Status == protocol.StatusOK {
		s.printf("open transaction %s\n", resp.Message)
	}
}

// exec runs one command line, already split into arguments.
func (s *shell) exec(args []string) error {
	if len(args) == 0 {
		return nil
	}
	name := strings.ToLower(args[0])
	switch name {
	case "help":
		s.help()
		return nil
	case "exit", "quit":
		return errExit
	case "begin":
		if s.single {
			return errors.New("a transaction needs several commands: use the prompt or a script")
		}
	}

	resp, err := s.b.Exec(args)
	if err != nil {
		return fmt.Errorf("connection to the server lost: %w", err)
	}
	s.trackTx(name, resp)
	if err := resp.Err(); err != nil {
		return err
	}
	s.print(name, resp)
	return nil
}

// trackTx follows whether a transaction is open, from the responses.
func (s *shell) trackTx(name string, resp protocol.Response) {
	switch {
	case name == "begin" && resp.Status == protocol.StatusOK:
		s.inTx = true
	case name == "commit", name == "rollback" && resp.Status == protocol.StatusOK:
		s.inTx = false // a commit ends the transaction even when it fails
	case resp.Code == protocol.CodeDeadlock, resp.Code == protocol.CodeClosed:
		s.inTx = false // the server rolled it back
	}
}

func (s *shell) print(name string, resp protocol.Response) {
	switch {
	case resp.Status == protocol.StatusValue:
		s.printf("%s\n", protocol.Quote(resp.Value))
	case resp.Status == protocol.StatusNotFound:
		s.printf("(not found)\n")
	case name == "scan" || name == "prefix":
		for _, r := range resp.Rows {
			s.printf("%s → %s\n", protocol.Display(r.Key), protocol.Quote(r.Value))
		}
		s.printf("(%s)\n", server.Plural(len(resp.Rows), "key"))
	case name == "count":
		s.printf("%s\n", resp.Message)
	case name == "stats":
		for _, r := range resp.Rows {
			s.printf("%-15s %s\n", r.Key, r.Value)
		}
	case resp.Message == "":
		s.printf("OK\n")
	default:
		s.printf("OK: %s\n", resp.Message)
	}
}

func (s *shell) help() {
	s.printf("Commands:\n")
	for _, c := range server.Commands() {
		s.printf("  %-28s %s\n", c.Usage(), c.Help)
	}
	s.printf("  %-28s %s\n", "help", "show this list")
	s.printf("  %-28s %s\n", "exit", "quit (also: quit, Ctrl+C); an open transaction is rolled back")
	s.printf(`
Keys and values are text. Use double quotes for spaces or an empty value,
and escapes inside quotes: \" \\ \n \t \r \0 \xHH (any byte).
  put "user name" "Batuhan Deniz"
  put bin "\x00\xff"

Transactions group commands: all their writes are applied together at
commit (or not at all), and other transactions cannot see or change the
keys they use until then.
  begin
  put acct:1 90
  put acct:2 110
  commit
`)
}

// friendly turns errors from opening or connecting into plain messages.
func friendly(err error, dir string) string {
	switch {
	case errors.Is(err, engine.ErrLocked):
		return fmt.Sprintf("the database in %s is already open in another basedb process", dir)
	case errors.Is(err, client.ErrBusy):
		return "the server has too many connections, try again later"
	}
	return err.Error()
}
