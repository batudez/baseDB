package btree

import (
	"fmt"

	"baseDB/internal/page"
)

// Delete removes the record from its leaf. If that leaves the leaf empty,
// the leaf is removed from the tree and its page goes to the free list:
//
//  1. unlink it from the leaf chain: its left neighbor (which may hang under
//     a different parent) now points to its right neighbor
//  2. remove its cell from the parent; if the parent becomes empty too,
//     free it and remove it from its own parent, and so on upward
//  3. if the root is left with a single child, that child becomes the root
//     (the tree gets one level shorter); if the root itself becomes empty,
//     the tree is empty
//
// Leaves that still hold records are never merged or rebalanced, even if
// nearly empty: only empty pages are given back. Everything happens inside
// the Put/Delete mini-transaction, so it is logged and atomic like any other
// change.
func (t *Tree) delete(m *mutation, key []byte) (bool, error) {
	if m.root == page.InvalidPageID {
		return false, nil
	}
	leaf, path, err := t.descend(m, key)
	if err != nil {
		return false, err
	}
	idx, found := leafSearch(leaf, key)
	if !found {
		return false, nil
	}
	leaf.DeleteCell(idx)
	m.markDirty(leaf)
	if leaf.NumSlots() > 0 {
		return true, nil
	}
	return true, t.removeEmptyLeaf(m, leaf, path)
}

func (t *Tree) removeEmptyLeaf(m *mutation, leaf *page.Page, path []pathStep) error {
	if len(path) == 0 { // the root is this leaf: the tree becomes empty
		m.free(leaf)
		m.setRoot(page.InvalidPageID)
		return nil
	}

	left, err := t.leftNeighbor(m, path)
	if err != nil {
		return err
	}
	if left != nil {
		left.SetRightSibling(leaf.RightSibling())
		m.markDirty(left)
	}
	m.free(leaf)

	for i := len(path) - 1; i >= 0; i-- {
		node := path[i].p
		removeChild(node, path[i].childIdx)
		m.markDirty(node)
		if node.NumSlots() > 0 {
			return t.shrinkRoot(m, path[0].p)
		}
		m.free(node) // the node had only this child
		if i == 0 {
			m.setRoot(page.InvalidPageID)
		}
	}
	return nil
}

// removeChild deletes cell idx of an internal node. If the -infinity cell
// (idx 0) goes, the next cell becomes the new -infinity cell: its child now
// also covers the (empty) key range of the removed one.
func removeChild(node *page.Page, idx int) {
	node.DeleteCell(idx)
	if idx == 0 && node.NumSlots() > 0 {
		// Same child, empty key: the cell only shrinks, so it always fits.
		if err := node.UpdateCell(0, encodeInternal(nil, childAt(node, 0))); err != nil {
			panic(fmt.Sprintf("btree: page %d: shrinking cell 0: %v", node.ID(), err))
		}
	}
}

// leftNeighbor returns the leaf just left of the leaf at the end of path,
// or nil if that leaf is the leftmost one. It goes up to the lowest
// ancestor where the path did not take the first child, then down the
// rightmost path of the subtree to the left.
func (t *Tree) leftNeighbor(m *mutation, path []pathStep) (*page.Page, error) {
	for j := len(path) - 1; j >= 0; j-- {
		if path[j].childIdx == 0 {
			continue
		}
		id := childAt(path[j].p, path[j].childIdx-1)
		for {
			p, err := m.fetch(id)
			if err != nil {
				return nil, err
			}
			switch p.Type() {
			case page.TypeLeaf:
				return p, nil
			case page.TypeInternal:
				id = childAt(p, p.NumSlots()-1)
			default:
				return nil, fmt.Errorf("%w: page %d has type %d", ErrCorrupted, id, p.Type())
			}
		}
	}
	return nil, nil
}

// shrinkRoot makes the only child of an internal root the new root, as long
// as that applies, so the tree never has a root with a single child.
func (t *Tree) shrinkRoot(m *mutation, root *page.Page) error {
	for root.Type() == page.TypeInternal && root.NumSlots() == 1 {
		child := childAt(root, 0)
		m.free(root)
		m.setRoot(child)
		var err error
		if root, err = m.fetch(child); err != nil {
			return err
		}
	}
	return nil
}
