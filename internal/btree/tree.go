// Package btree implements a disk-resident B+ tree mapping byte-string keys
// to byte-string values. Values live in the leaves (a clustered index), the
// leaves are linked through rightSibling for range scans, and all page
// accesses go through the buffer pool.
//
// Deletes remove the record from its leaf. A leaf that becomes empty is
// removed from the tree and its page goes to the free list (delete.go);
// nodes that still hold records are never merged or redistributed.
//
// Every Put and Delete is one mini-transaction (package mtr): it is logged in
// the WAL and durable when the call returns.
//
// See docs/ARCHITECTURE.md (weeks 2 and 3) for the design decisions.
package btree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"baseDB/internal/buffer"
	"baseDB/internal/mtr"
	"baseDB/internal/page"
	"baseDB/internal/wal"
)

// ErrCorrupted is returned for structural problems found in the tree.
var ErrCorrupted = errors.New("btree: corrupted")

// MetaStore holds the root page ID durably (*pager.Pager).
type MetaStore = mtr.MetaStore

// Tree is a B+ tree. It is safe for concurrent use: one RWMutex allows many
// readers or one writer. (Fine-grained latch crabbing is out of scope.)
type Tree struct {
	mu   sync.RWMutex
	pool *buffer.Pool
	meta MetaStore
	log  *wal.Log
	root page.PageID

	// broken is set when a commit could not be made durable (the log flush
	// failed). The changes are then in memory but not in the log, so every
	// later operation fails instead of building on them.
	broken error

	// afterMutation, if set, is called with the dirty pages of every
	// successful mutation (tests use it to check page collection).
	afterMutation func(dirty []page.PageID)
}

// Open returns the tree whose root is recorded in meta. An empty database
// has no root; the first Put creates it. log must be the WAL the pool
// flushes against; it may be nil only for a tree that is never modified.
//
// The pool must have enough frames for one mutation, which keeps its whole
// path pinned: about 2×height+1 frames. 16 is plenty for small trees.
func Open(pool *buffer.Pool, meta MetaStore, log *wal.Log) *Tree {
	return &Tree{pool: pool, meta: meta, log: log, root: meta.Meta().RootPageID}
}

// Get returns a copy of the value stored under key, and whether it exists.

// Function take only 1 parameter key which is a byte array
// and returns value, found information and an error.
func (t *Tree) Get(key []byte) ([]byte, bool, error) {

	// allow concurrent reads while protecting the tree from writes.
	t.mu.RLock()
	defer t.mu.RUnlock()

	// checking if the tree is broken
	if t.broken != nil {
		return nil, false, t.broken
	}

	// checking the pageID
	if t.root == page.InvalidPageID {
		return nil, false, nil
	}

	// starting from the root id
	id := t.root
	for {
		// first we fetch the current page from the buffer pool
		f, err := t.pool.Fetch(id)
		if err != nil {
			return nil, false, err
		}

		// lock the page for reading
		f.RLatch()
		p := f.Page()
		var value []byte
		found, leaf := false, false
		switch p.Type() {
		case page.TypeInternal:
			// Find the child that may contain the key
			id = childAt(p, childIndex(p, key))
		case page.TypeLeaf:
			// search for the key in the leaf node
			leaf = true
			var i int
			if i, found = leafSearch(p, key); found {
				// copy the value so it remains valid after releasing the page
				value = bytes.Clone(cellValue(p, i))
			}
		default:
			// an unexpected page type indicates possible tree corruption
			err = fmt.Errorf("%w: page %d has type %d", ErrCorrupted, p.ID(), p.Type())
		}

		// release the readlock
		f.RUnlatch()

		// release the page's pin in the buffer pool.
		if uerr := t.pool.Unpin(f.PageID(), false); err == nil {
			err = uerr
		}
		// stop on error or after searching a leaf node
		if err != nil || leaf {
			return value, found, err
		}
	}
}

// Put inserts key with value, or replaces the value if key exists.
func (t *Tree) Put(key, value []byte) error {
	cell, err := page.EncodeCell(key, value)
	if err != nil {
		return err
	}
	return t.mutate(func(m *mutation) error { return t.put(m, key, cell) })
}

// Delete removes key. It reports whether the key existed.
func (t *Tree) Delete(key []byte) (bool, error) {
	var found bool
	err := t.mutate(func(m *mutation) error {
		var err error
		found, err = t.delete(m, key)
		return err
	})
	return found, err
}

// Op is one write of an Apply batch.
type Op struct {
	Key    []byte
	Value  []byte // ignored for deletes
	Delete bool
}

// Apply performs all ops, in order, as one mini-transaction: they are logged
// as one WAL transaction and become durable together, or (on any error) none
// of them is applied. Deleting a missing key is not an error.
//
// Every page the batch touches stays pinned until the commit, so a batch
// that touches more pages than the buffer pool holds fails with
// buffer.ErrNoFreeFrame (and changes nothing).
func (t *Tree) Apply(ops []Op) error {
	cells := make([][]byte, len(ops))
	for i, op := range ops {
		if op.Delete {
			continue
		}
		var err error
		if cells[i], err = page.EncodeCell(op.Key, op.Value); err != nil {
			return err // validated before anything changes
		}
	}
	return t.mutate(func(m *mutation) error {
		for i, op := range ops {
			var err error
			if op.Delete {
				_, err = t.delete(m, op.Key)
			} else {
				err = t.put(m, op.Key, cells[i])
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// mutate runs fn inside a mini-transaction and commits it.
//
// If fn fails (an I/O error, a full pool), the mini-transaction is aborted:
// every page it changed is restored from its before-image, nothing is
// logged, and the tree stays usable. If the commit itself fails (the log
// cannot be made durable), the tree is marked broken.
func (t *Tree) mutate(fn func(m *mutation) error) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.broken != nil {
		return t.broken
	}

	m := &mutation{Mtr: mtr.Begin(t.pool, t.log, t.meta), root: t.root}
	if err := fn(m); err != nil {
		m.Abort()
		return err
	}
	dirty := m.DirtyPages()
	if _, err := m.Commit(); err != nil {
		t.broken = fmt.Errorf("btree: commit not durable, tree unusable: %w", err)
		return err
	}

	if m.rootChanged {
		t.root = m.root
	}
	if t.afterMutation != nil {
		t.afterMutation(dirty)
	}
	return nil
}

// pathStep is one internal node on the way from the root to a leaf.
type pathStep struct {
	p        *page.Page
	childIdx int
}

// descend walks from the root to the leaf that should contain key, fetching
// every page into m. It returns the leaf and the internal nodes above it.
func (t *Tree) descend(m *mutation, key []byte) (*page.Page, []pathStep, error) {
	var path []pathStep
	p, err := m.fetch(m.root)
	for err == nil && p.Type() == page.TypeInternal {
		i := childIndex(p, key)
		path = append(path, pathStep{p, i})
		p, err = m.fetch(childAt(p, i))
	}
	if err != nil {
		return nil, nil, err
	}
	if p.Type() != page.TypeLeaf {
		return nil, nil, fmt.Errorf("%w: page %d has type %d", ErrCorrupted, p.ID(), p.Type())
	}
	return p, path, nil
}

func (t *Tree) put(m *mutation, key, cell []byte) error {
	if m.root == page.InvalidPageID {
		leaf, err := m.create(page.TypeLeaf)
		if err != nil {
			return err
		}
		fill(leaf, [][]byte{cell})
		m.setRoot(leaf.ID())
		return nil
	}

	leaf, path, err := t.descend(m, key)
	if err != nil {
		return err
	}
	idx, found := leafSearch(leaf, key)
	if found {
		err = leaf.UpdateCell(idx, cell)
	} else {
		err = leaf.InsertCell(idx, cell)
	}
	if err == nil {
		m.markDirty(leaf)
		return nil
	}
	if !errors.Is(err, page.ErrNoSpace) {
		return err
	}

	// The leaf is full: split it, then insert the separator into the parent,
	// splitting upward as long as parents are full too.
	sep, right, err := t.split(m, leaf, cellsWith(leaf, idx, cell, found))
	if err != nil {
		return err
	}
	for i := len(path) - 1; i >= 0; i-- {
		parent := path[i]
		icell := encodeInternal(sep, right)
		pos := parent.childIdx + 1
		if parent.p.InsertCell(pos, icell) == nil {
			m.markDirty(parent.p)
			return nil
		}
		sep, right, err = t.split(m, parent.p, cellsWith(parent.p, pos, icell, false))
		if err != nil {
			return err
		}
	}

	// The root itself split: grow the tree by one level.
	root, err := m.create(page.TypeInternal)
	if err != nil {
		return err
	}
	fill(root, [][]byte{encodeInternal(nil, m.root), encodeInternal(sep, right)})
	m.setRoot(root.ID())
	return nil
}

// split distributes cells (the node's cells plus the one that did not fit)
// between node p and a new right sibling. It returns the separator key for
// the parent and the new node's ID.
//
// Leaf: the separator is a copy of the right node's first key.
// Internal: the first right cell's key moves up as the separator, and that
// cell becomes the right node's -infinity cell.
func (t *Tree) split(m *mutation, p *page.Page, cells [][]byte) ([]byte, page.PageID, error) {
	// Allocate first: if that fails, p has not been modified yet.
	right, err := m.create(p.Type())
	if err != nil {
		return nil, page.InvalidPageID, err
	}

	k := splitPoint(cells)
	sepKey, _, _ := page.DecodeCell(cells[k])
	sep := bytes.Clone(sepKey)
	rightCells := cells[k:]

	if p.Type() == page.TypeLeaf {
		right.SetRightSibling(p.RightSibling())
		p.SetRightSibling(right.ID())
	} else {
		_, child, _ := page.DecodeCell(cells[k])
		first := encodeInternal(nil, page.PageID(binary.LittleEndian.Uint32(child)))
		rightCells = append([][]byte{first}, cells[k+1:]...)
	}

	rebuild(p, cells[:k])
	fill(right, rightCells)
	m.markDirty(p)
	return sep, right.ID(), nil
}
