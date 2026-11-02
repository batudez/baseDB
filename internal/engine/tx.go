package engine

import (
	"bytes"
	"errors"
	"fmt"
	"slices"

	"baseDB/internal/btree"
	"baseDB/internal/buffer"
	"baseDB/internal/lockmgr"
	"baseDB/internal/page"
)

// Transactions use strict two-phase locking (2PL) with deferred writes:
//
//   - Every Get locks its key shared, every Put/Delete locks its key
//     exclusive, every Scan locks the whole database shared, before touching
//     anything. Locks are only released after Commit (once the commit is
//     durable) or Rollback, so the schedule is serializable.
//   - Locks have two levels (multi-granularity locking): the database itself
//     (IS / IX / S / SIX) and single keys (S / X). A scan's S lock on the
//     database conflicts with every writer's IX, so no key can appear in or
//     vanish from a scanned range until the scanning transaction ends: no
//     phantoms.
//   - Writes are kept in the transaction's private write set and applied to
//     the B+ tree only at Commit, all together as one mini-transaction (one
//     WAL transaction). So an uncommitted write never reaches a page,
//     Rollback just forgets the write set, and recovery stays redo-only.
//   - Deadlocks are detected by the lock manager when they happen; the
//     transaction whose request would close the cycle is rolled back and
//     gets ErrDeadlock. The caller retries it.
//
// See docs/ARCHITECTURE.md (week 7).

// MaxTxnWrites bounds the write set of one transaction (distinct keys).
// Every page the commit touches must fit in the buffer pool at once, so the
// real limit can be lower for large values or a small pool; Commit then
// returns ErrTxTooLarge and applies nothing.
const MaxTxnWrites = 1000

var (
	// ErrDeadlock: the transaction was chosen as a deadlock victim and has
	// been rolled back. Run it again.
	ErrDeadlock = lockmgr.ErrDeadlock

	// ErrTxDone: the transaction was already committed or rolled back.
	ErrTxDone = errors.New("engine: transaction already finished")

	// ErrTxTooLarge: the transaction writes more than fits in one commit.
	ErrTxTooLarge = errors.New("engine: transaction too large")
)

// Lock names: the database, and one resource per key.
const dbResource lockmgr.Resource = "\x00database"

func keyResource(key []byte) lockmgr.Resource { return lockmgr.Resource("k" + string(key)) }

// Tx is a transaction. It must be used by one goroutine at a time and end
// with Commit or Rollback.
type Tx struct {
	db     *DB
	id     lockmgr.TxnID
	writes map[string]pending // final state of each key written
	done   bool
}

type pending struct {
	value   []byte
	deleted bool
}

// Begin starts a transaction.
func (db *DB) Begin() (*Tx, error) {
	if err := db.begin(); err != nil {
		return nil, err
	}
	defer db.end()
	return &Tx{db: db, id: lockmgr.TxnID(db.nextTxn.Add(1)), writes: map[string]pending{}}, nil
}

// lock takes the database intention lock, then the key lock (parents before
// children). If the lock manager refuses (deadlock, closing), the whole
// transaction is rolled back.
func (tx *Tx) lock(key []byte, write bool) error {
	dbMode, keyMode := lockmgr.IS, lockmgr.S
	if write {
		dbMode, keyMode = lockmgr.IX, lockmgr.X
	}
	if err := tx.db.locks.Lock(tx.id, dbResource, dbMode); err != nil {
		return tx.abort(err)
	}
	if err := tx.db.locks.Lock(tx.id, keyResource(key), keyMode); err != nil {
		return tx.abort(err)
	}
	return nil
}

func (tx *Tx) abort(err error) error {
	tx.Rollback()
	if errors.Is(err, lockmgr.ErrClosed) {
		return ErrClosed
	}
	return err
}

// Get returns the value of key as this transaction sees it: its own write
// if it wrote the key, otherwise the committed value. The key stays locked
// shared until the end, so repeated reads give the same answer.
func (tx *Tx) Get(key []byte) ([]byte, bool, error) {
	return tx.get(key, false)
}

// GetForUpdate is Get with the key locked exclusive right away, like SQL's
// SELECT ... FOR UPDATE. Use it when the transaction will write the key it
// reads: two transactions that both Get (shared) and then Put (exclusive)
// the same key deadlock on the lock upgrade, while with GetForUpdate the
// second one simply waits for the first.
func (tx *Tx) GetForUpdate(key []byte) ([]byte, bool, error) {
	return tx.get(key, true)
}

func (tx *Tx) get(key []byte, forUpdate bool) ([]byte, bool, error) {
	if tx.done {
		return nil, false, ErrTxDone
	}
	if err := tx.lock(key, forUpdate); err != nil {
		return nil, false, err
	}
	if p, ok := tx.writes[string(key)]; ok {
		if p.deleted {
			return nil, false, nil
		}
		return bytes.Clone(p.value), true, nil
	}
	return tx.committedGet(key)
}

func (tx *Tx) committedGet(key []byte) ([]byte, bool, error) {
	if err := tx.db.begin(); err != nil {
		return nil, false, err
	}
	defer tx.db.end()
	return tx.db.tree.Get(key)
}

// Put stores value under key when the transaction commits.
func (tx *Tx) Put(key, value []byte) error {
	if tx.done {
		return ErrTxDone
	}
	if len(key) > page.MaxKeySize {
		return page.ErrKeyTooLarge
	}
	if len(value) > page.MaxValueSize {
		return page.ErrValueTooLarge
	}
	if err := tx.checkSize(key); err != nil {
		return err
	}
	if err := tx.lock(key, true); err != nil {
		return err
	}
	tx.writes[string(key)] = pending{value: bytes.Clone(value)}
	return nil
}

// Delete removes key when the transaction commits, and reports whether the
// key exists as this transaction sees it.
func (tx *Tx) Delete(key []byte) (bool, error) {
	if tx.done {
		return false, ErrTxDone
	}
	if err := tx.checkSize(key); err != nil {
		return false, err
	}
	if err := tx.lock(key, true); err != nil {
		return false, err
	}
	var existed bool
	if p, ok := tx.writes[string(key)]; ok {
		existed = !p.deleted
	} else {
		var err error
		if _, existed, err = tx.committedGet(key); err != nil {
			return false, err
		}
	}
	tx.writes[string(key)] = pending{deleted: true}
	return existed, nil
}

func (tx *Tx) checkSize(key []byte) error {
	if _, ok := tx.writes[string(key)]; !ok && len(tx.writes) >= MaxTxnWrites {
		return fmt.Errorf("%w: more than %d keys written", ErrTxTooLarge, MaxTxnWrites)
	}
	return nil
}

// Scan iterates over keys in [start, end) (nil = unbounded) as this
// transaction sees them: committed data with its own writes laid over it.
// It locks the whole database shared, so no other transaction can change
// any key until this one ends (this is what prevents phantoms).
func (tx *Tx) Scan(start, end []byte) Iterator {
	if tx.done {
		return &txIterator{err: ErrTxDone}
	}
	if err := tx.db.locks.Lock(tx.id, dbResource, lockmgr.S); err != nil {
		return &txIterator{err: tx.abort(err)}
	}
	var own []ownWrite
	for k, p := range tx.writes {
		kb := []byte(k)
		if (start == nil || bytes.Compare(kb, start) >= 0) && (end == nil || bytes.Compare(kb, end) < 0) {
			own = append(own, ownWrite{key: kb, pending: p})
		}
	}
	slices.SortFunc(own, func(a, b ownWrite) int { return bytes.Compare(a.key, b.key) })
	return &txIterator{tree: tx.db.tree.Scan(start, end), own: own}
}

// Writes returns how many keys the transaction has written so far.
func (tx *Tx) Writes() int { return len(tx.writes) }

// Done reports whether the transaction has finished: committed, rolled
// back, or rolled back by the engine (deadlock victim, database closing).
func (tx *Tx) Done() bool { return tx.done }

// Commit applies the write set as one atomic, durable unit, then releases
// every lock. If it fails, nothing was applied and the transaction is
// rolled back. Either way the transaction is finished.
func (tx *Tx) Commit() error {
	if tx.done {
		return ErrTxDone
	}
	tx.done = true
	err := tx.apply()
	tx.db.locks.ReleaseAll(tx.id) // only now: strict 2PL
	tx.writes = nil
	if err != nil {
		return err
	}
	return tx.db.maybeCheckpoint()
}

func (tx *Tx) apply() error {
	if len(tx.writes) == 0 {
		return nil
	}
	ops := make([]btree.Op, 0, len(tx.writes))
	for k, p := range tx.writes {
		ops = append(ops, btree.Op{Key: []byte(k), Value: p.value, Delete: p.deleted})
	}
	slices.SortFunc(ops, func(a, b btree.Op) int { return bytes.Compare(a.Key, b.Key) })

	err := tx.db.commitOps(ops) // group commit (commit.go)
	if errors.Is(err, buffer.ErrNoFreeFrame) {
		return fmt.Errorf("%w: its pages do not fit in the buffer pool at once (%v)", ErrTxTooLarge, err)
	}
	return err
}

// Rollback discards the write set and releases every lock. Calling it on a
// finished transaction does nothing.
func (tx *Tx) Rollback() {
	if tx.done {
		return
	}
	tx.done = true
	tx.writes = nil
	tx.db.locks.ReleaseAll(tx.id)
}

// Iterator walks key/value pairs in key order (see btree.Iterator).
type Iterator interface {
	Next() bool
	Key() []byte
	Value() []byte
	Err() error
}

type ownWrite struct {
	key []byte
	pending
}

// txIterator merges the committed keys (tree) with the transaction's own
// writes (own, sorted): an own write replaces the committed value of the
// same key, an own delete hides it.
type txIterator struct {
	tree       Iterator
	own        []ownWrite
	i          int
	tk, tv     []byte // next committed pair, if treeHas
	treeHas    bool
	treeDone   bool
	key, value []byte
	err        error
}

func (it *txIterator) Next() bool {
	for it.err == nil {
		if !it.treeHas && !it.treeDone && it.tree != nil {
			if it.tree.Next() {
				it.tk, it.tv, it.treeHas = it.tree.Key(), it.tree.Value(), true
			} else if it.err = it.tree.Err(); it.err == nil {
				it.treeDone = true
			}
			continue
		}
		ownHas := it.i < len(it.own)
		switch {
		case !ownHas && !it.treeHas:
			return false
		case ownHas && (!it.treeHas || bytes.Compare(it.own[it.i].key, it.tk) <= 0):
			w := it.own[it.i]
			it.i++
			if it.treeHas && bytes.Equal(w.key, it.tk) {
				it.treeHas = false // replaced by the own write
			}
			if w.deleted {
				continue
			}
			it.key, it.value = w.key, w.value
			return true
		default:
			it.key, it.value = it.tk, it.tv
			it.treeHas = false
			return true
		}
	}
	return false
}

func (it *txIterator) Key() []byte   { return it.key }
func (it *txIterator) Value() []byte { return it.value }
func (it *txIterator) Err() error    { return it.err }
