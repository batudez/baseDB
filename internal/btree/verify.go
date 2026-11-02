package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"baseDB/internal/page"
)

// Verify checks the structural invariants of the whole tree:
//   - every reachable page is a leaf or internal node, reached exactly once
//   - keys inside every node are strictly ascending
//   - separators are valid: every key in child i lies in [key_i, key_i+1)
//     of its parent (bounds inherited from above), cell 0 of an internal
//     node has an empty key
//   - all leaves are at the same depth
//   - the rightSibling chain visits exactly the leaves, left to right
//   - the root recorded in the meta page matches the tree
//   - the free list holds only free pages, none of them in the tree, no cycle
//   - no page is leaked: every allocated page (1 .. NextPageID-1) is either
//     in the tree or on the free list
//
// It reads every page, so it is meant for tests and debugging.
func (t *Tree) Verify() error {
	_, err := t.verify()
	return err
}

// Stats describes the shape of the tree and its free list.
type Stats struct {
	Height    int // 0 for an empty tree, 1 for a single leaf
	Leaves    int
	Internals int
	Keys      int
	FreePages int
}

// Stats walks the whole tree and the free list (validating them like
// Verify, which it returns the error of) and reports their shape. It reads
// every page, so it is meant for admin commands and tests, not hot paths.
func (t *Tree) Stats() (Stats, error) {
	st, err := t.verify()
	return Stats{
		Height:    st.height,
		Leaves:    st.leaves,
		Internals: st.internals,
		Keys:      st.keys,
		FreePages: st.free,
	}, err
}

type treeStats struct {
	height, leaves, internals, keys int
	free                            int // pages on the free list
}

func (t *Tree) verify() (treeStats, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.broken != nil {
		return treeStats{}, t.broken
	}
	meta := t.meta.Meta()
	if meta.RootPageID != t.root {
		return treeStats{}, fmt.Errorf("%w: meta root %d, tree root %d", ErrCorrupted, meta.RootPageID, t.root)
	}

	v := &verifier{t: t, seen: make(map[page.PageID]bool)}
	if t.root != page.InvalidPageID {
		if err := v.node(t.root, nil, nil, 1); err != nil {
			return treeStats{}, err
		}
	}
	if err := v.freeList(meta.FreeListHead); err != nil {
		return treeStats{}, err
	}
	if used, allocated := len(v.seen), int(meta.NextPageID)-1; used != allocated {
		return treeStats{}, fmt.Errorf("%w: %d pages allocated, but only %d are in the tree or the free list (leak)",
			ErrCorrupted, allocated, used)
	}
	for i, id := range v.leafIDs {
		want := page.InvalidPageID
		if i+1 < len(v.leafIDs) {
			want = v.leafIDs[i+1]
		}
		if v.siblings[i] != want {
			return treeStats{}, fmt.Errorf("%w: leaf %d rightSibling = %d, want %d",
				ErrCorrupted, id, v.siblings[i], want)
		}
	}
	v.stats.height = v.leafDepth
	v.stats.leaves = len(v.leafIDs)
	return v.stats, nil
}

type verifier struct {
	t         *Tree
	seen      map[page.PageID]bool
	leafDepth int
	leafIDs   []page.PageID // in key order
	siblings  []page.PageID // rightSibling of each leaf
	stats     treeStats
}

// freeList walks the free list: every page must be a free page that is not
// in the tree and not visited twice.
func (v *verifier) freeList(head page.PageID) error {
	for id := head; id != page.InvalidPageID; {
		if v.seen[id] {
			return fmt.Errorf("%w: free page %d is also in the tree or appears twice on the free list", ErrCorrupted, id)
		}
		v.seen[id] = true
		s, err := v.read(id)
		if err != nil {
			return err
		}
		if s.typ != page.TypeFree {
			return fmt.Errorf("%w: page %d on the free list has type %d", ErrCorrupted, id, s.typ)
		}
		v.stats.free++
		id = s.right // rightSibling holds the next free page
	}
	return nil
}

// snapshot is a copy of what the verifier needs from one page, so no pin is
// held while recursing into children.
type snapshot struct {
	typ    page.Type
	right  page.PageID
	keys   [][]byte
	values [][]byte
}

func (v *verifier) read(id page.PageID) (snapshot, error) {
	f, err := v.t.pool.Fetch(id)
	if err != nil {
		return snapshot{}, err
	}
	f.RLatch()
	p := f.Page()
	s := snapshot{typ: p.Type(), right: p.RightSibling()}
	for i := 0; i < p.NumSlots() && err == nil; i++ {
		var k, val []byte
		if k, val, err = page.DecodeCell(p.Cell(i)); err == nil {
			s.keys = append(s.keys, bytes.Clone(k))
			s.values = append(s.values, bytes.Clone(val))
		}
	}
	f.RUnlatch()
	if uerr := v.t.pool.Unpin(id, false); err == nil {
		err = uerr
	}
	if err != nil {
		return snapshot{}, fmt.Errorf("page %d: %w", id, err)
	}
	return s, nil
}

// node checks the subtree rooted at id, whose keys must lie in [lo, hi)
// (nil = unbounded).
func (v *verifier) node(id page.PageID, lo, hi []byte, depth int) error {
	fail := func(format string, args ...any) error {
		return fmt.Errorf("%w: page %d: %s", ErrCorrupted, id, fmt.Sprintf(format, args...))
	}
	if id == page.InvalidPageID {
		return fail("nil child pointer")
	}
	if v.seen[id] {
		return fail("reached twice (cycle or shared child)")
	}
	v.seen[id] = true

	s, err := v.read(id)
	if err != nil {
		return err
	}
	inRange := func(k []byte) bool {
		return (lo == nil || bytes.Compare(k, lo) >= 0) && (hi == nil || bytes.Compare(k, hi) < 0)
	}

	switch s.typ {
	case page.TypeLeaf:
		if v.leafDepth == 0 {
			v.leafDepth = depth
		} else if depth != v.leafDepth {
			return fail("leaf at depth %d, other leaves at %d", depth, v.leafDepth)
		}
		for i, k := range s.keys {
			if i > 0 && bytes.Compare(s.keys[i-1], k) >= 0 {
				return fail("keys not ascending at cell %d", i)
			}
			if !inRange(k) {
				return fail("key %q outside parent range [%q, %q)", k, lo, hi)
			}
		}
		v.leafIDs = append(v.leafIDs, id)
		v.siblings = append(v.siblings, s.right)
		v.stats.keys += len(s.keys)
		return nil

	case page.TypeInternal:
		v.stats.internals++
		n := len(s.keys)
		if n == 0 {
			return fail("internal node without children")
		}
		if len(s.keys[0]) != 0 {
			return fail("cell 0 key is %q, want empty", s.keys[0])
		}
		for i := 1; i < n; i++ {
			k := s.keys[i]
			if i > 1 && bytes.Compare(s.keys[i-1], k) >= 0 {
				return fail("separators not ascending at cell %d", i)
			}
			if (lo != nil && bytes.Compare(k, lo) <= 0) || !inRange(k) {
				return fail("separator %q outside range (%q, %q)", k, lo, hi)
			}
		}
		for i := 0; i < n; i++ {
			if len(s.values[i]) != childSize {
				return fail("cell %d child pointer has %d bytes", i, len(s.values[i]))
			}
			childLo, childHi := lo, hi
			if i > 0 {
				childLo = s.keys[i]
			}
			if i+1 < n {
				childHi = s.keys[i+1]
			}
			child := page.PageID(binary.LittleEndian.Uint32(s.values[i]))
			if err := v.node(child, childLo, childHi, depth+1); err != nil {
				return err
			}
		}
		return nil

	default:
		return fail("unexpected page type %d", s.typ)
	}
}
