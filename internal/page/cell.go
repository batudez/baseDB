package page

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Size limits. With these, at least three maximum-size cells fit in a page,
// so a node split always produces two non-empty halves. There are no
// overflow pages.
const (
	MaxKeySize   = 256
	MaxValueSize = 1024

	cellHeaderSize = 4
	MaxKVCellSize  = cellHeaderSize + MaxKeySize + MaxValueSize
)

var (
	ErrNoSpace       = errors.New("page: not enough free space")
	ErrKeyTooLarge   = fmt.Errorf("page: key larger than %d bytes", MaxKeySize)
	ErrValueTooLarge = fmt.Errorf("page: value larger than %d bytes", MaxValueSize)
	ErrBadCell       = errors.New("page: malformed cell")
)

// EncodeCell builds a key/value cell:
//
//	[keyLen u16][valueLen u16][key][value]
//
// Internal B+ tree nodes use the same format with the child page ID as value.
func EncodeCell(key, value []byte) ([]byte, error) {
	if len(key) > MaxKeySize {
		return nil, ErrKeyTooLarge
	}
	if len(value) > MaxValueSize {
		return nil, ErrValueTooLarge
	}
	cell := make([]byte, cellHeaderSize+len(key)+len(value))
	binary.LittleEndian.PutUint16(cell[0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(cell[2:], uint16(len(value)))
	copy(cell[cellHeaderSize:], key)
	copy(cell[cellHeaderSize+len(key):], value)
	return cell, nil
}

// DecodeCell splits a cell into key and value. Both slices alias cell.
func DecodeCell(cell []byte) (key, value []byte, err error) {
	if len(cell) < cellHeaderSize {
		return nil, nil, ErrBadCell
	}
	kl := int(binary.LittleEndian.Uint16(cell[0:]))
	vl := int(binary.LittleEndian.Uint16(cell[2:]))
	if cellHeaderSize+kl+vl != len(cell) {
		return nil, nil, ErrBadCell
	}
	key = cell[cellHeaderSize : cellHeaderSize+kl]
	value = cell[cellHeaderSize+kl:]
	return key, value, nil
}
