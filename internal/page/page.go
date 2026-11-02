// Package page defines the on-disk page format shared by every layer of the
// storage engine: a fixed-size byte array that starts with a common header.
//
// See docs/ARCHITECTURE.md (week 1) for the reasoning behind the layout.
package page

import (
	"encoding/binary"
	"hash/crc32"
)

// PageSize is the unit of I/O between memory and disk. 4 KiB matches the OS
// page size and the physical sector size of most SSDs.
const PageSize = 4096

// Offsets inside a page (including PageSize itself, the end of an empty
// cell area) are stored as uint16; this fails to compile if PageSize is ever
// raised beyond what uint16 can hold.
const _ = uint16(PageSize)

// PageID identifies a page by its position in the data file:
// byte offset = PageID * PageSize.
type PageID uint32

const (
	// MetaPageID is the fixed location of the database meta page.
	MetaPageID PageID = 0

	// InvalidPageID is the nil page pointer. Page 0 is the meta page, which is
	// never a child or sibling of another page, so 0 can double as nil. As a
	// result a zeroed page has all of its pointers set to nil.
	InvalidPageID PageID = 0
)

// LSN (log sequence number) identifies a WAL record. Every page stores the
// LSN of the last record applied to it (the pageLSN).
type LSN uint64

// Type says what kind of content a page holds.
type Type uint8

const (
	TypeUnused   Type = iota // allocated but never initialized
	TypeMeta                 // page 0
	TypeLeaf                 // B+ tree leaf
	TypeInternal             // B+ tree internal node
	TypeFree                 // on the free list (see freelist.go)
)

// iota gives incremental num to types unused -> 1 typeMeta -> 2 ...

// Common header, present on every page (28 bytes):
//
//	offset size field
//	0      4    pageID        the page's own ID, detects misdirected writes
//	4      1    type
//	5      3    reserved
//	8      8    LSN           LSN of the last WAL record applied to this page
//	16     2    numSlots      number of slots (slotted.go)
//	18     2    freeSpacePtr  start of the cell area; PageSize when empty
//	20     4    rightSibling  next page at the same level / next free page
//	24     4    CRC32C        over every byte of the page except this field
const (
	offID           = 0
	offType         = 4
	offLSN          = 8
	offNumSlots     = 16
	offFreeSpacePtr = 18
	offRightSibling = 20
	offChecksum     = 24

	HeaderSize = 28
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Page is the in-memory image of one disk page.
type Page struct {
	data [PageSize]byte
}

// Data returns the raw bytes of the page. The slice aliases the page.
func (p *Page) Data() []byte { return p.data[:] }

// Init zeroes the page and writes a fresh header describing an empty
// slotted page.
func (p *Page) Init(id PageID, t Type) {
	p.data = [PageSize]byte{}
	p.SetID(id)
	p.SetType(t)
	p.setFreeSpacePtr(PageSize)
}

func (p *Page) u16(off int) int       { return int(binary.LittleEndian.Uint16(p.data[off:])) }
func (p *Page) setU16(off int, v int) { binary.LittleEndian.PutUint16(p.data[off:], uint16(v)) }

func (p *Page) ID() PageID      { return PageID(binary.LittleEndian.Uint32(p.data[offID:])) }
func (p *Page) SetID(id PageID) { binary.LittleEndian.PutUint32(p.data[offID:], uint32(id)) }

func (p *Page) Type() Type     { return Type(p.data[offType]) }
func (p *Page) SetType(t Type) { p.data[offType] = byte(t) }

func (p *Page) LSN() LSN       { return LSN(binary.LittleEndian.Uint64(p.data[offLSN:])) }
func (p *Page) SetLSN(lsn LSN) { binary.LittleEndian.PutUint64(p.data[offLSN:], uint64(lsn)) }

func (p *Page) RightSibling() PageID {
	return PageID(binary.LittleEndian.Uint32(p.data[offRightSibling:]))
}
func (p *Page) SetRightSibling(id PageID) {
	binary.LittleEndian.PutUint32(p.data[offRightSibling:], uint32(id))
}

// IsZero reports whether every byte of the page is zero, i.e. the page was
// allocated but has never been written.
func (p *Page) IsZero() bool {
	return p.data == [PageSize]byte{}
}

func (p *Page) computeChecksum() uint32 {
	crc := crc32.Update(0, castagnoli, p.data[:offChecksum])
	return crc32.Update(crc, castagnoli, p.data[offChecksum+4:])
}

// UpdateChecksum stores the checksum of the current contents in the header.
// Only the pager calls this, on a private copy right before writing.
func (p *Page) UpdateChecksum() {
	binary.LittleEndian.PutUint32(p.data[offChecksum:], p.computeChecksum())
}

// VerifyChecksum reports whether the stored checksum matches the contents.
// An all-zero page is valid: it was allocated but never written.
func (p *Page) VerifyChecksum() bool {
	stored := binary.LittleEndian.Uint32(p.data[offChecksum:])
	if stored == p.computeChecksum() {
		return true
	}
	return p.IsZero()
}
