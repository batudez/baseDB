package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"baseDB/internal/page"
)

// Node formats. Both are slotted pages whose cells are page.EncodeCell cells,
// kept sorted by key.
//
// Leaf (type TypeLeaf):
//
//	cell i = (key_i, value_i)            key_0 < key_1 < ... < key_n-1
//	rightSibling = next leaf in key order, InvalidPageID for the last leaf
//
// Internal (type TypeInternal), n cells = n children:
//
//	cell 0 = ("",    child_0)           key ignored: acts as -infinity
//	cell i = (key_i, child_i)           child_i holds keys in [key_i, key_i+1)
//
// Storing the leftmost child as an ordinary cell with an empty key means an
// internal node needs no extra header field, and both node types share the
// same insert/split code paths.

const childSize = 4

func encodeInternal(key []byte, child page.PageID) []byte {
	var v [childSize]byte
	binary.LittleEndian.PutUint32(v[:], uint32(child))
	cell, err := page.EncodeCell(key, v[:])
	if err != nil {
		panic(err) // separator keys come from stored keys, so they always fit
	}
	return cell
}

func cellKey(p *page.Page, i int) []byte {
	k, _, err := page.DecodeCell(p.Cell(i))
	if err != nil {
		panic(fmt.Sprintf("btree: page %d cell %d: %v", p.ID(), i, err))
	}
	return k
}

func cellValue(p *page.Page, i int) []byte {
	_, v, err := page.DecodeCell(p.Cell(i))
	if err != nil {
		panic(fmt.Sprintf("btree: page %d cell %d: %v", p.ID(), i, err))
	}
	return v
}

func childAt(p *page.Page, i int) page.PageID {
	return page.PageID(binary.LittleEndian.Uint32(cellValue(p, i)))
}

// leafSearch returns the position of the first cell with key >= key, and
// whether that cell's key equals key.
func leafSearch(p *page.Page, key []byte) (int, bool) {
	n := p.NumSlots()
	i := sort.Search(n, func(i int) bool { return bytes.Compare(cellKey(p, i), key) >= 0 })
	return i, i < n && bytes.Equal(cellKey(p, i), key)
}

// childIndex returns the index of the child whose key range contains key:
// the last cell i such that i == 0 or key_i <= key.
func childIndex(p *page.Page, key []byte) int {
	n := p.NumSlots()
	// First i >= 1 with key_i > key; the child is the one before it.
	i := 1 + sort.Search(n-1, func(j int) bool { return bytes.Compare(cellKey(p, j+1), key) > 0 })
	return i - 1
}

// cellsWith returns copies of all cells of p, with cell placed at position
// idx: inserted when replace is false, replacing cell idx otherwise.
func cellsWith(p *page.Page, idx int, cell []byte, replace bool) [][]byte {
	n := p.NumSlots()
	cells := make([][]byte, 0, n+1)
	for i := 0; i < n; i++ {
		if i == idx {
			cells = append(cells, cell)
			if replace {
				continue
			}
		}
		cells = append(cells, bytes.Clone(p.Cell(i)))
	}
	if idx == n {
		cells = append(cells, cell)
	}
	return cells
}

// splitPoint returns how many cells go to the left node: the smallest k
// whose cells take at least half of the bytes, clamped so that both sides
// are non-empty. With the page.MaxKeySize/MaxValueSize limits both halves are
// guaranteed to fit in a page (see docs/ARCHITECTURE.md, week 2).
func splitPoint(cells [][]byte) int {
	total := 0
	for _, c := range cells {
		total += len(c) + page.SlotSize
	}
	k, acc := 0, 0
	for k < len(cells) && 2*acc < total {
		acc += len(cells[k]) + page.SlotSize
		k++
	}
	return max(1, min(k, len(cells)-1))
}

// rebuild replaces the cells of p, keeping its header (ID, type, LSN,
// rightSibling).
func rebuild(p *page.Page, cells [][]byte) {
	id, typ, lsn, right := p.ID(), p.Type(), p.LSN(), p.RightSibling()
	p.Init(id, typ)
	p.SetLSN(lsn)
	p.SetRightSibling(right)
	fill(p, cells)
}

func fill(p *page.Page, cells [][]byte) {
	for i, c := range cells {
		if err := p.InsertCell(i, c); err != nil {
			panic(fmt.Sprintf("btree: page %d: split half does not fit: %v", p.ID(), err))
		}
	}
}
