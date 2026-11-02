// Package wal implements the write-ahead log: the record format, appending
// and flushing records, and reading them back with corruption checks.
//
// See docs/ARCHITECTURE.md (week 3) for the design decisions.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"baseDB/internal/page"
)

// RecordType says what a record describes.
type RecordType uint8

const (
	TypeBegin  RecordType = iota + 1 // a transaction starts
	TypePage                         // after-image of the changed bytes of one page
	TypeMeta                         // new contents of the meta page fields
	TypeCommit                       // the transaction is complete and durable
)

func (t RecordType) String() string {
	switch t {
	case TypeBegin:
		return "BEGIN"
	case TypePage:
		return "PAGE"
	case TypeMeta:
		return "META"
	case TypeCommit:
		return "COMMIT"
	}
	return fmt.Sprintf("RecordType(%d)", uint8(t))
}

// Record layout (all integers little-endian):
//
//	offset size field
//	0      4    length   size of the whole record, header included
//	4      4    CRC32C   over bytes [8, length)
//	8      8    LSN      file offset of this record (self-check)
//	16     8    txnID
//	24     1    type
//	25     ...  payload (depends on type)
//
// PAGE payload:
//
//	pageID u32 | flags u8 | numRuns u16 | numRuns × (offset u16, length u16, bytes)
//
// flags bit 0 (init): the page starts from all zeros before the runs are
// applied. Used for newly created pages, whose previous disk contents are
// meaningless.
//
// META payload:
//
//	rootPageID u32 | nextPageID u32 | freeListHead u32 | lastAppliedIndex u64
//
// BEGIN and COMMIT have no payload.
const (
	offLength = 0
	offCRC    = 4
	offLSN    = 8
	offTxn    = 16
	offType   = 24

	recordHeaderSize = 25
	pageHeaderSize   = 4 + 1 + 2
	runHeaderSize    = 4
	metaPayloadSize  = 4 + 4 + 4 + 8

	flagInit = 1 << 0

	// MaxRecordSize bounds the length field. A PAGE record holds at most one
	// page of data plus run headers (well under 8 KiB), so anything larger
	// is corruption.
	MaxRecordSize = 32 << 10
)

var (
	// ErrCorrupt means a record failed validation: bad length, checksum,
	// LSN, type or payload.
	ErrCorrupt = errors.New("wal: corrupt record")

	// ErrIncomplete means the log ends in the middle of a record, which is
	// what a crash during a log write leaves behind.
	ErrIncomplete = errors.New("wal: incomplete record at end of log")
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Run is a contiguous range of bytes of a page.
type Run struct {
	Off  int
	Data []byte
}

// Record is one decoded log record.
type Record struct {
	LSN   page.LSN
	TxnID uint64
	Type  RecordType

	// TypePage
	PageID page.PageID
	Init   bool
	Runs   []Run

	// TypeMeta
	Meta page.Meta
}

func (r *Record) size() int {
	switch r.Type {
	case TypePage:
		n := recordHeaderSize + pageHeaderSize
		for _, run := range r.Runs {
			n += runHeaderSize + len(run.Data)
		}
		return n
	case TypeMeta:
		return recordHeaderSize + metaPayloadSize
	default:
		return recordHeaderSize
	}
}

// appendTo encodes r (whose LSN must already be set) at the end of dst.
func (r *Record) appendTo(dst []byte) ([]byte, error) {
	size := r.size()
	if size > MaxRecordSize {
		return dst, fmt.Errorf("wal: record of %d bytes exceeds MaxRecordSize", size)
	}
	start := len(dst)
	dst = append(dst, make([]byte, size)...)
	b := dst[start:]

	binary.LittleEndian.PutUint32(b[offLength:], uint32(size))
	binary.LittleEndian.PutUint64(b[offLSN:], uint64(r.LSN))
	binary.LittleEndian.PutUint64(b[offTxn:], r.TxnID)
	b[offType] = byte(r.Type)

	p := b[recordHeaderSize:]
	switch r.Type {
	case TypePage:
		binary.LittleEndian.PutUint32(p[0:], uint32(r.PageID))
		if r.Init {
			p[4] = flagInit
		}
		binary.LittleEndian.PutUint16(p[5:], uint16(len(r.Runs)))
		p = p[pageHeaderSize:]
		for _, run := range r.Runs {
			binary.LittleEndian.PutUint16(p[0:], uint16(run.Off))
			binary.LittleEndian.PutUint16(p[2:], uint16(len(run.Data)))
			copy(p[runHeaderSize:], run.Data)
			p = p[runHeaderSize+len(run.Data):]
		}
	case TypeMeta:
		binary.LittleEndian.PutUint32(p[0:], uint32(r.Meta.RootPageID))
		binary.LittleEndian.PutUint32(p[4:], uint32(r.Meta.NextPageID))
		binary.LittleEndian.PutUint32(p[8:], uint32(r.Meta.FreeListHead))
		binary.LittleEndian.PutUint64(p[12:], r.Meta.LastAppliedIndex)
	}

	binary.LittleEndian.PutUint32(b[offCRC:], crc32.Checksum(b[offLSN:], castagnoli))
	return dst, nil
}

// decodeRecord decodes and validates the single record in b, which was read
// from file offset lsn. The returned record aliases b.
func decodeRecord(b []byte, lsn page.LSN) (Record, error) {
	fail := func(format string, args ...any) (Record, error) {
		return Record{}, fmt.Errorf("%w at LSN %d: %s", ErrCorrupt, lsn, fmt.Sprintf(format, args...))
	}
	if len(b) < recordHeaderSize || int(binary.LittleEndian.Uint32(b[offLength:])) != len(b) {
		return fail("bad length")
	}
	if crc32.Checksum(b[offLSN:], castagnoli) != binary.LittleEndian.Uint32(b[offCRC:]) {
		return fail("checksum mismatch")
	}
	r := Record{
		LSN:   page.LSN(binary.LittleEndian.Uint64(b[offLSN:])),
		TxnID: binary.LittleEndian.Uint64(b[offTxn:]),
		Type:  RecordType(b[offType]),
	}
	if r.LSN != lsn {
		return fail("record says LSN %d", r.LSN)
	}

	p := b[recordHeaderSize:]
	switch r.Type {
	case TypeBegin, TypeCommit:
		if len(p) != 0 {
			return fail("%v record with payload", r.Type)
		}
	case TypeMeta:
		if len(p) != metaPayloadSize {
			return fail("META payload of %d bytes", len(p))
		}
		r.Meta = page.Meta{
			RootPageID:       page.PageID(binary.LittleEndian.Uint32(p[0:])),
			NextPageID:       page.PageID(binary.LittleEndian.Uint32(p[4:])),
			FreeListHead:     page.PageID(binary.LittleEndian.Uint32(p[8:])),
			LastAppliedIndex: binary.LittleEndian.Uint64(p[12:]),
		}
	case TypePage:
		if len(p) < pageHeaderSize {
			return fail("PAGE payload too short")
		}
		r.PageID = page.PageID(binary.LittleEndian.Uint32(p[0:]))
		if r.PageID == page.MetaPageID {
			return fail("PAGE record for the meta page")
		}
		if p[4]&^flagInit != 0 {
			return fail("unknown flags %#x", p[4])
		}
		r.Init = p[4]&flagInit != 0
		n := int(binary.LittleEndian.Uint16(p[5:]))
		p = p[pageHeaderSize:]
		r.Runs = make([]Run, 0, n)
		for i := 0; i < n; i++ {
			if len(p) < runHeaderSize {
				return fail("run %d truncated", i)
			}
			off := int(binary.LittleEndian.Uint16(p[0:]))
			length := int(binary.LittleEndian.Uint16(p[2:]))
			if len(p) < runHeaderSize+length {
				return fail("run %d truncated", i)
			}
			if off+length > page.PageSize {
				return fail("run %d [%d, %d) outside the page", i, off, off+length)
			}
			r.Runs = append(r.Runs, Run{Off: off, Data: p[runHeaderSize : runHeaderSize+length]})
			p = p[runHeaderSize+length:]
		}
		if len(p) != 0 {
			return fail("%d trailing bytes after runs", len(p))
		}
	default:
		return fail("unknown type %d", r.Type)
	}
	return r, nil
}
