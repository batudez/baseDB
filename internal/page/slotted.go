package page

import "fmt"

// Slotted page layout (leaf and internal B+ tree nodes):
//
//	0              28
//	┌──────────────┬────────────────────┬────────────┬─────────────────┐
//	│ common header│ slot array →       │ free space │ ← cells         │
//	│              │ [off u16][len u16] │            │                 │
//	└──────────────┴────────────────────┴────────────┴─────────────────┘
//	                                    ↑            ↑
//	                    HeaderSize + numSlots*4      freeSpacePtr
//
// Slots are kept dense and in the order chosen by the caller (the B+ tree
// keeps them sorted by key), so inserting at position i shifts 4-byte slots,
// never cell bodies. Deleted cells leave holes in the cell area; Compact
// reclaims them when an insert needs contiguous space.
const SlotSize = 4

// MaxCellSize is the largest cell that fits into an empty page.
const MaxCellSize = PageSize - HeaderSize - SlotSize

func (p *Page) NumSlots() int           { return p.u16(offNumSlots) }
func (p *Page) setNumSlots(n int)       { p.setU16(offNumSlots, n) }
func (p *Page) freeSpacePtr() int       { return p.u16(offFreeSpacePtr) }
func (p *Page) setFreeSpacePtr(off int) { p.setU16(offFreeSpacePtr, off) }
func (p *Page) slotPos(i int) int       { return HeaderSize + i*SlotSize }
func (p *Page) slotEnd() int            { return p.slotPos(p.NumSlots()) }
func (p *Page) contiguousFree() int     { return p.freeSpacePtr() - p.slotEnd() }

func (p *Page) slot(i int) (off, length int) {
	pos := p.slotPos(i)
	return p.u16(pos), p.u16(pos + 2)
}

func (p *Page) setSlot(i, off, length int) {
	pos := p.slotPos(i)
	p.setU16(pos, off)
	p.setU16(pos+2, length)
}

func (p *Page) checkIndex(i, limit int) {
	if i < 0 || i >= limit {
		panic(fmt.Sprintf("page: slot index %d out of range [0,%d)", i, limit))
	}
}

func (p *Page) liveBytes() int {
	n := 0
	for i := 0; i < p.NumSlots(); i++ {
		_, length := p.slot(i)
		n += length
	}
	return n
}

// FreeSpace returns the number of bytes available for new cells and slots,
// counting holes that compaction would reclaim.
func (p *Page) FreeSpace() int {
	return PageSize - p.slotEnd() - p.liveBytes()
}

// Fits reports whether a new cell of the given size can be inserted.
func (p *Page) Fits(cellLen int) bool {
	return cellLen+SlotSize <= p.FreeSpace()
}

// Cell returns the content of slot i. The slice aliases the page and is only
// valid until the page is modified.
func (p *Page) Cell(i int) []byte {
	p.checkIndex(i, p.NumSlots())
	off, length := p.slot(i)
	return p.data[off : off+length : off+length]
}

// InsertCell stores cell as slot i, shifting slots i.. one position right.
func (p *Page) InsertCell(i int, cell []byte) error {
	n := p.NumSlots()
	p.checkIndex(i, n+1)
	if !p.Fits(len(cell)) {
		return ErrNoSpace
	}
	if len(cell)+SlotSize > p.contiguousFree() {
		p.Compact()
	}

	off := p.freeSpacePtr() - len(cell)
	copy(p.data[off:], cell)
	p.setFreeSpacePtr(off)

	start, end := p.slotPos(i), p.slotPos(n)
	copy(p.data[start+SlotSize:end+SlotSize], p.data[start:end])
	p.setNumSlots(n + 1)
	p.setSlot(i, off, len(cell))
	return nil
}

// DeleteCell removes slot i, shifting the following slots one position left.
func (p *Page) DeleteCell(i int) {
	n := p.NumSlots()
	p.checkIndex(i, n)
	off, length := p.slot(i)

	start, end := p.slotPos(i), p.slotPos(n)
	copy(p.data[start:end-SlotSize], p.data[start+SlotSize:end])
	p.setNumSlots(n - 1)

	switch {
	case n == 1:
		p.setFreeSpacePtr(PageSize) // page is empty: no holes left
	case off == p.freeSpacePtr():
		p.setFreeSpacePtr(off + length) // lowest cell: reclaim directly
	}
	// Otherwise the cell becomes a hole until the next Compact.
}

// UpdateCell replaces the content of slot i, keeping its position.
func (p *Page) UpdateCell(i int, cell []byte) error {
	p.checkIndex(i, p.NumSlots())
	off, length := p.slot(i)

	if len(cell) <= length {
		copy(p.data[off:], cell)
		p.setSlot(i, off, len(cell))
		return nil
	}
	if len(cell)-length > p.FreeSpace() {
		return ErrNoSpace
	}

	// Release the old cell, then allocate a new one like InsertCell does.
	p.setSlot(i, 0, 0)
	if len(cell) > p.contiguousFree() {
		p.Compact()
	}
	off = p.freeSpacePtr() - len(cell)
	copy(p.data[off:], cell)
	p.setFreeSpacePtr(off)
	p.setSlot(i, off, len(cell))
	return nil
}

// Compact moves all live cells to the end of the page so that all free space
// is contiguous. Slot order is unchanged.
func (p *Page) Compact() {
	var buf [PageSize]byte
	end := PageSize
	for i := 0; i < p.NumSlots(); i++ {
		off, length := p.slot(i)
		end -= length
		copy(buf[end:], p.data[off:off+length])
		p.setSlot(i, end, length)
	}
	copy(p.data[end:], buf[end:])
	p.setFreeSpacePtr(end)
}
