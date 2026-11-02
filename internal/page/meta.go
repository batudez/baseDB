package page

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Meta page (page 0) body, after the common header:
//
//	offset size field
//	28     8    magic             "BASEDB01"
//	36     4    version
//	40     4    pageSize          must equal PageSize
//	44     4    rootPageID        B+ tree root; InvalidPageID when empty
//	48     4    nextPageID        first page ID never handed out
//	52     4    freeListHead      first free page; InvalidPageID when empty
//	56     8    lastAppliedIndex  last replicated log entry applied locally
const (
	MetaMagic     = "BASEDB01"
	FormatVersion = 1

	offMagic            = HeaderSize
	offVersion          = HeaderSize + 8
	offPageSize         = HeaderSize + 12
	offRootPageID       = HeaderSize + 16
	offNextPageID       = HeaderSize + 20
	offFreeListHead     = HeaderSize + 24
	offLastAppliedIndex = HeaderSize + 28
)

var ErrBadMeta = errors.New("page: invalid meta page")

// Meta holds the mutable fields of the meta page. Magic, version and page
// size are constants written and checked by WriteMeta/ReadMeta.
type Meta struct {
	RootPageID       PageID
	NextPageID       PageID
	FreeListHead     PageID
	LastAppliedIndex uint64
}

// NewMeta returns the meta of an empty database: page 0 is taken by the
// meta page itself, so the first allocation returns page 1.
func NewMeta() Meta {
	return Meta{NextPageID: MetaPageID + 1}
}

// WriteMeta formats p as the meta page holding m.
func (p *Page) WriteMeta(m Meta) {
	p.Init(MetaPageID, TypeMeta)
	copy(p.data[offMagic:], MetaMagic)
	binary.LittleEndian.PutUint32(p.data[offVersion:], FormatVersion)
	binary.LittleEndian.PutUint32(p.data[offPageSize:], PageSize)
	binary.LittleEndian.PutUint32(p.data[offRootPageID:], uint32(m.RootPageID))
	binary.LittleEndian.PutUint32(p.data[offNextPageID:], uint32(m.NextPageID))
	binary.LittleEndian.PutUint32(p.data[offFreeListHead:], uint32(m.FreeListHead))
	binary.LittleEndian.PutUint64(p.data[offLastAppliedIndex:], m.LastAppliedIndex)
}

// ReadMeta decodes p as the meta page, validating its constant fields.
func (p *Page) ReadMeta() (Meta, error) {
	if p.ID() != MetaPageID || p.Type() != TypeMeta {
		return Meta{}, fmt.Errorf("%w: not a meta page", ErrBadMeta)
	}
	if string(p.data[offMagic:offMagic+len(MetaMagic)]) != MetaMagic {
		return Meta{}, fmt.Errorf("%w: bad magic", ErrBadMeta)
	}
	if v := binary.LittleEndian.Uint32(p.data[offVersion:]); v != FormatVersion {
		return Meta{}, fmt.Errorf("%w: version %d, want %d", ErrBadMeta, v, FormatVersion)
	}
	if s := binary.LittleEndian.Uint32(p.data[offPageSize:]); s != PageSize {
		return Meta{}, fmt.Errorf("%w: page size %d, want %d", ErrBadMeta, s, PageSize)
	}
	m := Meta{
		RootPageID:       PageID(binary.LittleEndian.Uint32(p.data[offRootPageID:])),
		NextPageID:       PageID(binary.LittleEndian.Uint32(p.data[offNextPageID:])),
		FreeListHead:     PageID(binary.LittleEndian.Uint32(p.data[offFreeListHead:])),
		LastAppliedIndex: binary.LittleEndian.Uint64(p.data[offLastAppliedIndex:]),
	}
	if m.NextPageID <= MetaPageID {
		return Meta{}, fmt.Errorf("%w: nextPageID %d", ErrBadMeta, m.NextPageID)
	}
	return m, nil
}
