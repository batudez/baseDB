// Package server makes a baseDB database available over TCP.
//
// A Session runs commands for one client (one connection, or the local
// terminal program) and keeps its open transaction. The Server accepts
// connections and gives each its own Session and goroutine.
//
// See docs/ARCHITECTURE.md (week 8).
package server

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"baseDB/internal/engine"
	"baseDB/internal/page"
	"baseDB/internal/protocol"
)

// Command describes one command, for usage messages and help.
type Command struct {
	Name string
	Args string // argument syntax
	Help string
	min  int
	max  int
	run  func(s *Session, args []string) protocol.Response
}

// Usage returns "name args".
func (c *Command) Usage() string { return strings.TrimSpace(c.Name + " " + c.Args) }

// commandList is in the order help shows them.
var commandList = []*Command{
	{Name: "put", Args: "<key> <value>", Help: "store value under key (insert or update)", min: 2, max: 2, run: (*Session).put},
	{Name: "get", Args: "<key>", Help: "show the value of key", min: 1, max: 1, run: (*Session).get},
	{Name: "delete", Args: "<key>", Help: "remove key", min: 1, max: 1, run: (*Session).delete},
	{Name: "scan", Args: "[start] [end]", Help: "list keys in [start, end) in order; no arguments = all", min: 0, max: 2, run: (*Session).scan},
	{Name: "prefix", Args: "<prefix>", Help: "list keys that start with prefix", min: 1, max: 1, run: (*Session).prefix},
	{Name: "count", Args: "[start] [end]", Help: "count keys in [start, end)", min: 0, max: 2, run: (*Session).count},
	{Name: "begin", Args: "", Help: "start a transaction: the next commands run in it, isolated (2PL)", min: 0, max: 0, run: (*Session).begin},
	{Name: "commit", Args: "", Help: "apply the transaction's writes, all together and durably", min: 0, max: 0, run: (*Session).commit},
	{Name: "rollback", Args: "", Help: "discard the transaction's writes", min: 0, max: 0, run: (*Session).rollback},
	{Name: "stats", Args: "", Help: "show tree shape, pages, buffer pool, log, locks, commits", min: 0, max: 0, run: (*Session).stats},
	{Name: "verify", Args: "", Help: "check the B+ tree and free list for consistency", min: 0, max: 0, run: (*Session).verify},
	{Name: "checkpoint", Args: "", Help: "write all pages to the data file and truncate the log", min: 0, max: 0, run: (*Session).checkpoint},
	{Name: "ping", Args: "", Help: "check that the server answers", min: 0, max: 0, run: func(*Session, []string) protocol.Response { return protocol.OK("PONG") }},
}

var commands = map[string]*Command{}

func init() {
	for _, c := range commandList {
		commands[c.Name] = c
	}
	commands["del"] = commands["delete"]
}

// Commands lists the commands a Session understands, in help order.
func Commands() []*Command { return commandList }

// Session is the state of one client: its open transaction. It is not safe
// for concurrent use (one client sends one command at a time).
type Session struct {
	db  *engine.DB
	dir string // shown by stats
	tx  *engine.Tx
}

// NewSession starts a session on db (dir is only used for display).
func NewSession(db *engine.DB, dir string) *Session { return &Session{db: db, dir: dir} }

// InTx reports whether a transaction is open, and how many keys it wrote.
func (s *Session) InTx() (bool, int) {
	if s.tx == nil {
		return false, 0
	}
	return true, s.tx.Writes()
}

// Close rolls back an open transaction (the client left without commit).
func (s *Session) Close() {
	if s.tx != nil {
		s.tx.Rollback()
		s.tx = nil
	}
}

// Exec runs one command, already split into arguments.
func (s *Session) Exec(args []string) protocol.Response {
	if len(args) == 0 {
		return protocol.Err(protocol.CodeUsage, "empty command")
	}
	c, ok := commands[strings.ToLower(args[0])]
	if !ok {
		return protocol.Err(protocol.CodeUnknown, "unknown command %q (type help for the list)", args[0])
	}
	if n := len(args) - 1; n < c.min || n > c.max {
		return protocol.Err(protocol.CodeUsage, "usage: %s", c.Usage())
	}
	resp := c.run(s, args[1:])
	if s.tx != nil && s.tx.Done() {
		// The engine rolled the transaction back (deadlock victim, closing).
		s.tx = nil
	}
	return resp
}

// store is what the data commands work on: the open transaction if there
// is one, otherwise the database (each command its own transaction).
type store interface {
	Get(key []byte) ([]byte, bool, error)
	Put(key, value []byte) error
	Delete(key []byte) (bool, error)
	Scan(start, end []byte) engine.Iterator
}

func (s *Session) store() store {
	if s.tx != nil {
		return s.tx
	}
	return s.db
}

// fail turns an engine error into an ERR response with a plain message.
func (s *Session) fail(err error) protocol.Response {
	switch {
	case errors.Is(err, engine.ErrDeadlock):
		return protocol.Err(protocol.CodeDeadlock, "deadlock: the transaction was rolled back, run it again")
	case errors.Is(err, engine.ErrClosed):
		return protocol.Err(protocol.CodeClosed, "the database is closing")
	case errors.Is(err, page.ErrKeyTooLarge):
		return protocol.Err(protocol.CodeTooLarge, "key is too long (limit %d bytes)", page.MaxKeySize)
	case errors.Is(err, page.ErrValueTooLarge):
		return protocol.Err(protocol.CodeTooLarge, "value is too long (limit %d bytes)", page.MaxValueSize)
	case errors.Is(err, engine.ErrTxTooLarge):
		return protocol.Err(protocol.CodeTooLarge, "%v", err)
	}
	return protocol.Err(protocol.CodeInternal, "%v", err)
}

func (s *Session) put(args []string) protocol.Response {
	k, v := []byte(args[0]), []byte(args[1])
	if len(k) > page.MaxKeySize {
		return protocol.Err(protocol.CodeTooLarge, "key is %d bytes, the limit is %d", len(k), page.MaxKeySize)
	}
	if len(v) > page.MaxValueSize {
		return protocol.Err(protocol.CodeTooLarge, "value is %d bytes, the limit is %d", len(v), page.MaxValueSize)
	}
	if err := s.store().Put(k, v); err != nil {
		return s.fail(err)
	}
	return protocol.OK("")
}

func (s *Session) get(args []string) protocol.Response {
	v, ok, err := s.store().Get([]byte(args[0]))
	switch {
	case err != nil:
		return s.fail(err)
	case !ok:
		return protocol.NotFound()
	}
	return protocol.Value(v)
}

func (s *Session) delete(args []string) protocol.Response {
	found, err := s.store().Delete([]byte(args[0]))
	switch {
	case err != nil:
		return s.fail(err)
	case !found:
		return protocol.NotFound()
	}
	return protocol.OK("")
}

func bounds(args []string) (start, end []byte) {
	if len(args) > 0 {
		start = []byte(args[0])
	}
	if len(args) > 1 {
		end = []byte(args[1])
	}
	return start, end
}

// list returns the pairs in [start, end); with rows false only the count.
// The OK message is the number of keys.
func (s *Session) list(start, end []byte, rows bool) protocol.Response {
	var resp protocol.Response
	n := 0
	it := s.store().Scan(start, end)
	for it.Next() {
		if rows {
			resp.Rows = append(resp.Rows, protocol.Row{Key: it.Key(), Value: it.Value()})
		}
		n++
	}
	if err := it.Err(); err != nil {
		return s.fail(err)
	}
	resp.Status, resp.Message = protocol.StatusOK, strconv.Itoa(n)
	return resp
}

func (s *Session) scan(args []string) protocol.Response {
	start, end := bounds(args)
	return s.list(start, end, true)
}

func (s *Session) prefix(args []string) protocol.Response {
	p := []byte(args[0])
	return s.list(p, PrefixEnd(p), true)
}

func (s *Session) count(args []string) protocol.Response {
	start, end := bounds(args)
	return s.list(start, end, false)
}

// PrefixEnd returns the smallest key greater than every key starting with
// p: p with its last byte incremented (trailing 0xff bytes dropped first).
// nil means no upper bound (p is empty or all 0xff).
func PrefixEnd(p []byte) []byte {
	end := append([]byte(nil), p...)
	for len(end) > 0 {
		if last := len(end) - 1; end[last] < 0xff {
			end[last]++
			return end
		}
		end = end[:len(end)-1]
	}
	return nil
}

func (s *Session) begin([]string) protocol.Response {
	if s.tx != nil {
		return protocol.Err(protocol.CodeTxOpen, "a transaction is already open (commit or rollback first)")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return s.fail(err)
	}
	s.tx = tx
	return protocol.OK("transaction started")
}

func (s *Session) commit([]string) protocol.Response {
	if s.tx == nil {
		return protocol.Err(protocol.CodeNoTx, "no open transaction (begin first)")
	}
	tx, n := s.tx, s.tx.Writes()
	s.tx = nil // finished whether the commit succeeds or not
	if err := tx.Commit(); err != nil {
		r := s.fail(err)
		r.Code, r.Message = protocol.CodeTxFailed, "commit failed, nothing was applied: "+r.Message
		return r
	}
	return protocol.OK("committed " + Plural(n, "write"))
}

func (s *Session) rollback([]string) protocol.Response {
	if s.tx == nil {
		return protocol.Err(protocol.CodeNoTx, "no open transaction (begin first)")
	}
	n := s.tx.Writes()
	s.tx.Rollback()
	s.tx = nil
	return protocol.OK("rolled back, " + Plural(n, "write") + " discarded")
}

func (s *Session) stats([]string) protocol.Response {
	st, err := s.db.Stats()
	if err != nil {
		return s.fail(err)
	}
	resp := protocol.OK("")
	for _, r := range StatsRows(st, s.dir) {
		resp.Rows = append(resp.Rows, protocol.Row{Key: []byte(r[0]), Value: []byte(r[1])})
	}
	return resp
}

func (s *Session) verify([]string) protocol.Response {
	st, err := s.db.Stats()
	if err != nil {
		return s.fail(err)
	}
	return protocol.OK(fmt.Sprintf("tree and free list are consistent (%s, %s, %d free pages)",
		Plural(st.Tree.Keys, "key"), Plural(st.Tree.Leaves+st.Tree.Internals, "page"), st.Tree.FreePages))
}

func (s *Session) checkpoint([]string) protocol.Response {
	before, err := s.db.Stats()
	if err != nil {
		return s.fail(err)
	}
	if err := s.db.Checkpoint(); err != nil {
		return s.fail(err)
	}
	after, err := s.db.Stats()
	if err != nil {
		return s.fail(err)
	}
	return protocol.OK(fmt.Sprintf("log %s → %s", Size(before.LogBytes), Size(after.LogBytes)))
}

// StatsRows formats the database statistics as (name, text) rows.
func StatsRows(st engine.Stats, dir string) [][2]string {
	var rows [][2]string
	row := func(name, format string, a ...any) { rows = append(rows, [2]string{name, fmt.Sprintf(format, a...)}) }
	t, p, r, l, c := st.Tree, st.Pool, st.Recovery, st.Locks, st.Commits

	row("directory", "%s", dir)
	row("keys", "%d", t.Keys)
	row("tree", "height %d, %s, %s", t.Height, Plural(t.Leaves, "leaf"), Plural(t.Internals, "internal node"))
	row("pages", "%d allocated: %d in the tree, %d free (data file %s)",
		st.AllocatedPages, t.Leaves+t.Internals, t.FreePages, Size(int64(st.AllocatedPages+1)*page.PageSize))
	hitRatio := 0.0
	if total := p.Hits + p.Misses; total > 0 {
		hitRatio = 100 * float64(p.Hits) / float64(total)
	}
	row("buffer pool", "%d frames (%s), %d hits, %d misses (%.1f%% hits), %d evictions, %d page writes",
		st.Options.PoolFrames, Size(int64(st.Options.PoolFrames)*page.PageSize), p.Hits, p.Misses, hitRatio, p.Evictions, p.Writes)
	row("log", "%s (checkpoint at %s)", Size(st.LogBytes), Size(st.Options.CheckpointBytes))
	row("last recovery", "%d transactions read, %d records redone, %d already on disk, %d torn pages rebuilt",
		r.Transactions, r.RecordsApplied, r.RecordsSkipped, r.TornPages)
	if r.Uncommitted > 0 || r.LogTail != nil {
		row("", "%d uncommitted transactions ignored; log tail cut: %v", r.Uncommitted, r.LogTail)
	}
	row("locks", "%d granted, %d waited, %d deadlocks resolved, %d held now", l.Granted, l.Waits, l.Deadlocks, l.Held)
	perFsync := 0.0
	if c.Batches > 0 {
		perFsync = float64(c.Commits) / float64(c.Batches)
	}
	mode := "group commit"
	if st.Options.NoGroupCommit {
		mode = "group commit off"
	}
	row("commits", "%d commits in %d fsyncs (%.1f per fsync, %s)", c.Commits, c.Batches, perFsync, mode)
	row("limits", "key ≤ %d bytes, value ≤ %d bytes, ≤ %d keys written per transaction",
		page.MaxKeySize, page.MaxValueSize, engine.MaxTxnWrites)
	return rows
}

// Plural returns "1 key", "2 keys", "3 leaves".
func Plural(n int, word string) string {
	if n == 1 {
		return "1 " + word
	}
	if strings.HasSuffix(word, "f") {
		return fmt.Sprintf("%d %sves", n, word[:len(word)-1])
	}
	return fmt.Sprintf("%d %ss", n, word)
}

// Size formats a byte count.
func Size(b int64) string {
	switch {
	case b >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(b)/(1<<10))
	}
	return fmt.Sprintf("%d B", b)
}
