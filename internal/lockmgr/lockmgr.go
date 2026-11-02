// Package lockmgr is the lock manager for strict two-phase locking (2PL).
//
// Transactions lock resources (the whole database, or single keys) in one of
// five modes. A lock that conflicts with a lock held by another transaction
// waits; waiting is first-come first-served per resource, so no transaction
// starves. All locks of a transaction are released together by ReleaseAll,
// after it commits or aborts (that is what makes the 2PL "strict").
//
// Deadlocks are detected when they happen: whenever a transaction has to
// wait, the lock manager follows the wait-for graph from it, and if the path
// leads back to it, that request fails with ErrDeadlock (the requester is
// the victim and must abort).
//
// See docs/ARCHITECTURE.md (week 7).
package lockmgr

import (
	"errors"
	"fmt"
	"sync"
)

// Mode is a lock mode. IS, IX and SIX are intention modes used on the
// database resource (multi-granularity locking): they say "I hold, or will
// hold, S/X locks on single keys below".
type Mode uint8

const (
	IS  Mode = iota + 1 // intention shared: will read single keys
	IX                  // intention exclusive: will write single keys
	S                   // shared: read (on the database: read everything)
	SIX                 // S + IX: read everything and write some keys
	X                   // exclusive: write (on the database: everything)
)

func (m Mode) String() string {
	switch m {
	case IS:
		return "IS"
	case IX:
		return "IX"
	case S:
		return "S"
	case SIX:
		return "SIX"
	case X:
		return "X"
	}
	return fmt.Sprintf("Mode(%d)", uint8(m))
}

// compatible[a][b] says whether a lock in mode a held by one transaction
// allows another transaction to hold mode b on the same resource:
//
//	     IS  IX  S   SIX X
//	IS   ✓   ✓   ✓   ✓   -
//	IX   ✓   ✓   -   -   -
//	S    ✓   -   ✓   -   -
//	SIX  ✓   -   -   -   -
//	X    -   -   -   -   -
var compatible = [6][6]bool{
	IS:  {IS: true, IX: true, S: true, SIX: true},
	IX:  {IS: true, IX: true},
	S:   {IS: true, S: true},
	SIX: {IS: true},
}

// Compatible reports whether modes a and b can be held at the same time by
// two different transactions.
func Compatible(a, b Mode) bool { return compatible[a][b] }

// covers reports whether holding a already gives everything b gives.
func covers(a, b Mode) bool {
	switch a {
	case X:
		return true
	case SIX:
		return b != X
	case S:
		return b == S || b == IS
	case IX:
		return b == IX || b == IS
	case IS:
		return b == IS
	}
	return false
}

// join is the weakest mode that covers both a and b: what a transaction
// holding a ends up with when it also asks for b (a lock upgrade).
func join(a, b Mode) Mode {
	switch {
	case covers(a, b):
		return a
	case covers(b, a):
		return b
	case a == X || b == X:
		return X
	}
	return SIX // {IX, S} and anything with SIX that is not covered
}

// TxnID identifies a transaction to the lock manager.
type TxnID uint64

// Resource names a lockable thing (the caller decides the naming).
type Resource string

var (
	// ErrDeadlock: waiting for this lock would close a cycle in the wait-for
	// graph. The requester holds its other locks still; it must abort and
	// call ReleaseAll.
	ErrDeadlock = errors.New("lockmgr: deadlock, transaction chosen as victim")

	// ErrClosed: the lock manager was closed (the database is closing).
	ErrClosed = errors.New("lockmgr: closed")
)

// Stats counts lock manager activity.
type Stats struct {
	Granted   uint64 // lock requests granted (immediately or after waiting)
	Waits     uint64 // requests that had to wait
	Deadlocks uint64 // requests refused as deadlock victims
	Held      int    // locks currently held
	Waiting   int    // requests currently waiting
}

// Manager is a lock table. It is safe for concurrent use; one transaction
// must not request locks from several goroutines at once.
type Manager struct {
	mu      sync.Mutex
	table   map[Resource]*resource
	held    map[TxnID]map[Resource]Mode
	waiting map[TxnID]*request // the one request each waiting transaction waits on
	closed  bool
	stats   Stats
}

type resource struct {
	holders map[TxnID]Mode
	queue   []*request // waiting requests, served from the front
}

type request struct {
	txn  TxnID
	res  Resource
	mode Mode // the mode wanted (for an upgrade: joined with the held one)
	done chan error
}

// New returns an empty lock manager.
func New() *Manager {
	return &Manager{
		table:   make(map[Resource]*resource),
		held:    make(map[TxnID]map[Resource]Mode),
		waiting: make(map[TxnID]*request),
	}
}

// Lock acquires res in mode for txn, waiting as long as a conflicting lock
// is held or an earlier request is queued. If txn already holds res, the
// lock is upgraded to the join of both modes (a no-op if the held mode
// already covers mode). It returns ErrDeadlock if waiting would deadlock.
func (m *Manager) Lock(txn TxnID, res Resource, mode Mode) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	r := m.table[res]
	if r == nil {
		r = &resource{holders: make(map[TxnID]Mode)}
		m.table[res] = r
	}
	cur, holds := r.holders[txn]
	want := mode
	if holds {
		if want = join(cur, mode); want == cur {
			m.mu.Unlock()
			return nil
		}
	}

	// Grant now if nothing conflicts. A new request also waits behind
	// queued ones (first come, first served); an upgrade does not, since
	// the queued requests may well be waiting for this very transaction.
	if (holds || len(r.queue) == 0) && grantable(r, txn, want) {
		m.grant(r, txn, res, want)
		m.mu.Unlock()
		return nil
	}

	req := &request{txn: txn, res: res, mode: want, done: make(chan error, 1)}
	if holds {
		r.queue = append([]*request{req}, r.queue...) // upgrades go first
	} else {
		r.queue = append(r.queue, req)
	}
	m.waiting[txn] = req
	if m.deadlocked(txn) {
		m.removeRequest(r, req)
		m.stats.Deadlocks++
		m.serve(r, res) // requests behind the victim may go now
		m.mu.Unlock()
		return ErrDeadlock
	}
	m.stats.Waits++
	m.mu.Unlock()
	return <-req.done
}

// ReleaseAll releases every lock of txn and lets waiting requests proceed.
// Call it once the transaction has committed (durably) or aborted.
func (m *Manager) ReleaseAll(txn TxnID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if req := m.waiting[txn]; req != nil { // only if misused concurrently
		m.removeRequest(m.table[req.res], req)
		req.done <- ErrClosed
	}
	for res := range m.held[txn] {
		r := m.table[res]
		delete(r.holders, txn)
		m.serve(r, res)
		if len(r.holders) == 0 && len(r.queue) == 0 {
			delete(m.table, res)
		}
	}
	delete(m.held, txn)
}

// Close fails every waiting request with ErrClosed, and every later Lock.
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	for txn, req := range m.waiting {
		m.removeRequest(m.table[req.res], req)
		req.done <- ErrClosed
		delete(m.waiting, txn)
	}
}

// Holds returns the mode txn holds on res (0 if none).
func (m *Manager) Holds(txn TxnID, res Resource) Mode {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.held[txn][res]
}

// Stats returns a snapshot of the counters.
func (m *Manager) Stats() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	st := m.stats
	for _, locks := range m.held {
		st.Held += len(locks)
	}
	st.Waiting = len(m.waiting)
	return st
}

// grantable: txn can hold mode on r next to every other holder.
func grantable(r *resource, txn TxnID, mode Mode) bool {
	for h, hm := range r.holders {
		if h != txn && !Compatible(hm, mode) {
			return false
		}
	}
	return true
}

func (m *Manager) grant(r *resource, txn TxnID, res Resource, mode Mode) {
	r.holders[txn] = mode
	if m.held[txn] == nil {
		m.held[txn] = make(map[Resource]Mode)
	}
	m.held[txn][res] = mode
	m.stats.Granted++
}

// serve grants queued requests from the front for as long as they fit.
// It stops at the first one that does not: later requests must not
// overtake it, or it could wait forever.
func (m *Manager) serve(r *resource, res Resource) {
	for len(r.queue) > 0 {
		req := r.queue[0]
		if !grantable(r, req.txn, req.mode) {
			return
		}
		r.queue = r.queue[1:]
		delete(m.waiting, req.txn)
		m.grant(r, req.txn, res, req.mode)
		req.done <- nil
	}
}

func (m *Manager) removeRequest(r *resource, req *request) {
	for i, q := range r.queue {
		if q == req {
			r.queue = append(r.queue[:i], r.queue[i+1:]...)
			break
		}
	}
	delete(m.waiting, req.txn)
}

// blockers returns the transactions req is waiting for: holders whose mode
// conflicts with it, and requests queued before it whose mode conflicts
// with it (first come, first served makes it wait for those too).
func (m *Manager) blockers(req *request) []TxnID {
	r := m.table[req.res]
	var out []TxnID
	for h, hm := range r.holders {
		if h != req.txn && !Compatible(hm, req.mode) {
			out = append(out, h)
		}
	}
	for _, q := range r.queue {
		if q == req {
			break
		}
		if q.txn != req.txn && !Compatible(q.mode, req.mode) {
			out = append(out, q.txn)
		}
	}
	return out
}

// deadlocked reports whether the wait-for graph has a path from start back
// to start. Checking only at the moment start begins to wait is enough:
// every new wait-for edge either starts at start (its own request) or ends
// at it (requests queued behind an upgrade now wait for it), so every new
// cycle passes through start. A grant adds edges only to a transaction that
// is running, not waiting, and a running transaction has no edge out.
func (m *Manager) deadlocked(start TxnID) bool {
	visited := map[TxnID]bool{}
	var reaches func(t TxnID) bool
	reaches = func(t TxnID) bool {
		req := m.waiting[t]
		if req == nil {
			return false // t is running, not waiting: no edge out of it
		}
		for _, b := range m.blockers(req) {
			if b == start {
				return true
			}
			if !visited[b] {
				visited[b] = true
				if reaches(b) {
					return true
				}
			}
		}
		return false
	}
	return reaches(start)
}
