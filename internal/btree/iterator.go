package btree

import (
	"bytes"
	"fmt"

	"baseDB/internal/page"
)

// Iterator walks keys in [start, end) in ascending order.
//
//	it := tree.Scan(start, end)
//	for it.Next() {
//		use(it.Key(), it.Value())
//	}
//	if err := it.Err(); err != nil { ... }
//
// The iterator holds no locks or pins between calls to Next. It copies the
// matching records of one leaf at a time, then finds the next leaf by
// searching again for the successor of the last key it returned, walking
// the rightSibling chain from there. Re-searching (instead of keeping a page
// pointer across calls) stays correct even if writers split leaves in
// between. Each batch is a consistent snapshot of its leaf; the scan as a
// whole is not a snapshot of the tree.
type Iterator struct {
	t    *Tree
	seek []byte // smallest key not yet returned
	end  []byte // nil = unbounded

	buf  []entry
	pos  int
	done bool
	err  error

	key, value []byte
}

type entry struct{ key, value []byte }

// Scan returns an iterator over keys k with start <= k < end. A nil start
// means from the first key, a nil end means up to the last key.
func (t *Tree) Scan(start, end []byte) *Iterator {
	return &Iterator{t: t, seek: bytes.Clone(start), end: bytes.Clone(end)}
}

// Next advances to the next record. It returns false at the end or on error.
func (it *Iterator) Next() bool {
	for it.pos >= len(it.buf) {
		if it.done || it.err != nil {
			return false
		}
		it.fill()
	}
	e := it.buf[it.pos]
	it.pos++
	it.key, it.value = e.key, e.value
	return true
}

// Key returns the current key. It stays valid after Next.
func (it *Iterator) Key() []byte { return it.key }

// Value returns the current value. It stays valid after Next.
func (it *Iterator) Value() []byte { return it.value }

// Err returns the error that stopped the iteration, if any.
func (it *Iterator) Err() error { return it.err }

// fill loads the next batch: the matching records of the first leaf (at or
// after seek) that has any.
func (it *Iterator) fill() {
	t := it.t
	t.mu.RLock()
	defer t.mu.RUnlock()
	it.buf, it.pos = it.buf[:0], 0

	if t.broken != nil {
		it.err = t.broken
		return
	}
	if t.root == page.InvalidPageID {
		it.done = true
		return
	}

	id, err := t.findLeaf(it.seek)
	for err == nil {
		var next page.PageID
		next, err = it.collect(id)
		if err != nil || it.done {
			break
		}
		if next == page.InvalidPageID {
			it.done = true
			break
		}
		if len(it.buf) > 0 {
			// Resume strictly after the last key: its successor in byte
			// order is the key with a zero byte appended.
			last := it.buf[len(it.buf)-1].key
			it.seek = append(bytes.Clone(last), 0)
			break
		}
		id = next // empty leaf (lazy deletes) or nothing >= seek: keep going
	}
	it.err = err
}

// collect copies the records of leaf id that fall in [seek, end) and returns
// the next leaf. It sets done when a key >= end is seen.
func (it *Iterator) collect(id page.PageID) (page.PageID, error) {
	f, err := it.t.pool.Fetch(id)
	if err != nil {
		return page.InvalidPageID, err
	}
	f.RLatch()
	p := f.Page()
	if p.Type() != page.TypeLeaf {
		err = fmt.Errorf("%w: page %d in leaf chain has type %d", ErrCorrupted, id, p.Type())
	} else {
		start, _ := leafSearch(p, it.seek)
		for i := start; i < p.NumSlots(); i++ {
			k, v, _ := page.DecodeCell(p.Cell(i))
			if it.end != nil && bytes.Compare(k, it.end) >= 0 {
				it.done = true
				break
			}
			it.buf = append(it.buf, entry{bytes.Clone(k), bytes.Clone(v)})
		}
	}
	next := p.RightSibling()
	f.RUnlatch()
	if uerr := it.t.pool.Unpin(id, false); err == nil {
		err = uerr
	}
	return next, err
}

// findLeaf returns the leaf whose key range contains key.
func (t *Tree) findLeaf(key []byte) (page.PageID, error) {
	id := t.root
	for {
		f, err := t.pool.Fetch(id)
		if err != nil {
			return page.InvalidPageID, err
		}
		f.RLatch()
		p := f.Page()
		typ := p.Type()
		next := page.InvalidPageID
		if typ == page.TypeInternal {
			next = childAt(p, childIndex(p, key))
		}
		f.RUnlatch()
		if err := t.pool.Unpin(id, false); err != nil {
			return page.InvalidPageID, err
		}
		switch typ {
		case page.TypeLeaf:
			return id, nil
		case page.TypeInternal:
			id = next
		default:
			return page.InvalidPageID, fmt.Errorf("%w: page %d has type %d", ErrCorrupted, id, typ)
		}
	}
}
