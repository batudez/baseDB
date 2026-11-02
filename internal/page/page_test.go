package page

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"testing"
)

func TestHeaderRoundTrip(t *testing.T) {
	var p Page
	p.Init(42, TypeLeaf)
	p.SetLSN(123456789)
	p.SetRightSibling(43)

	if p.ID() != 42 || p.Type() != TypeLeaf || p.LSN() != 123456789 || p.RightSibling() != 43 {
		t.Fatalf("header = (%d, %d, %d, %d)", p.ID(), p.Type(), p.LSN(), p.RightSibling())
	}
	if p.NumSlots() != 0 || p.FreeSpace() != PageSize-HeaderSize {
		t.Fatalf("new page: slots=%d free=%d", p.NumSlots(), p.FreeSpace())
	}
}

func TestChecksum(t *testing.T) {
	var p Page
	if !p.VerifyChecksum() {
		t.Fatal("zero page must be valid")
	}

	p.Init(7, TypeLeaf)
	if p.VerifyChecksum() {
		t.Fatal("non-zero page without checksum must be invalid")
	}
	p.UpdateChecksum()
	if !p.VerifyChecksum() {
		t.Fatal("checksum must verify after UpdateChecksum")
	}
	for _, off := range []int{0, 10, 23, 28, 1000, PageSize - 1} {
		q := p
		q.Data()[off] ^= 0xFF
		if q.VerifyChecksum() {
			t.Fatalf("corruption at offset %d not detected", off)
		}
	}
}

func TestMetaRoundTrip(t *testing.T) {
	var p Page
	m := Meta{RootPageID: 5, NextPageID: 99, FreeListHead: 17, LastAppliedIndex: 1 << 40}
	p.WriteMeta(m)
	got, err := p.ReadMeta()
	if err != nil {
		t.Fatal(err)
	}
	if got != m {
		t.Fatalf("meta = %+v, want %+v", got, m)
	}

	p.Data()[offMagic] = 'X'
	if _, err := p.ReadMeta(); !errors.Is(err, ErrBadMeta) {
		t.Fatalf("err = %v, want ErrBadMeta", err)
	}
}

func TestCellEncoding(t *testing.T) {
	cell, err := EncodeCell([]byte("key"), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	k, v, err := DecodeCell(cell)
	if err != nil || string(k) != "key" || string(v) != "value" {
		t.Fatalf("decode = %q, %q, %v", k, v, err)
	}

	if _, err := EncodeCell(make([]byte, MaxKeySize+1), nil); !errors.Is(err, ErrKeyTooLarge) {
		t.Fatalf("err = %v, want ErrKeyTooLarge", err)
	}
	if _, err := EncodeCell(nil, make([]byte, MaxValueSize+1)); !errors.Is(err, ErrValueTooLarge) {
		t.Fatalf("err = %v, want ErrValueTooLarge", err)
	}
	if _, _, err := DecodeCell(cell[:len(cell)-1]); !errors.Is(err, ErrBadCell) {
		t.Fatalf("err = %v, want ErrBadCell", err)
	}
}

// At least three maximum-size cells must fit in a page, so that a B+ tree
// node split always leaves both halves non-empty.
func TestThreeMaxCellsFit(t *testing.T) {
	cell, err := EncodeCell(make([]byte, MaxKeySize), make([]byte, MaxValueSize))
	if err != nil {
		t.Fatal(err)
	}
	var p Page
	p.Init(1, TypeLeaf)
	for i := 0; i < 3; i++ {
		must(t, p.InsertCell(i, cell))
	}
}

func TestSlottedBasic(t *testing.T) {
	var p Page
	p.Init(1, TypeLeaf)

	must(t, p.InsertCell(0, []byte("bbb")))
	must(t, p.InsertCell(0, []byte("a")))
	must(t, p.InsertCell(2, []byte("cc")))
	assertCells(t, &p, "a", "bbb", "cc")

	p.DeleteCell(1)
	assertCells(t, &p, "a", "cc")

	must(t, p.UpdateCell(0, []byte("a-much-longer-value")))
	must(t, p.UpdateCell(1, []byte("c")))
	assertCells(t, &p, "a-much-longer-value", "c")

	p.DeleteCell(0)
	p.DeleteCell(0)
	if p.NumSlots() != 0 || p.FreeSpace() != PageSize-HeaderSize || p.contiguousFree() != PageSize-HeaderSize {
		t.Fatalf("empty page: slots=%d free=%d", p.NumSlots(), p.FreeSpace())
	}
}

func TestSlottedFullAndCompaction(t *testing.T) {
	var p Page
	p.Init(1, TypeLeaf)
	cell := bytes.Repeat([]byte{'x'}, 100)

	n := 0
	for p.InsertCell(n, cell) == nil {
		n++
	}
	if want := (PageSize - HeaderSize) / (100 + SlotSize); n != want {
		t.Fatalf("fit %d cells, want %d", n, want)
	}
	if err := p.InsertCell(0, cell); !errors.Is(err, ErrNoSpace) {
		t.Fatalf("err = %v, want ErrNoSpace", err)
	}

	// Free every other cell: space is now fragmented, not contiguous.
	for i := n - 2; i >= 0; i -= 2 {
		p.DeleteCell(i)
	}
	// A cell larger than any single hole only fits after compaction.
	big := bytes.Repeat([]byte{'y'}, 300)
	must(t, p.InsertCell(0, big))
	if !bytes.Equal(p.Cell(0), big) {
		t.Fatal("big cell corrupted")
	}
	for i := 1; i < p.NumSlots(); i++ {
		if !bytes.Equal(p.Cell(i), cell) {
			t.Fatalf("cell %d corrupted by compaction", i)
		}
	}
}

func TestSlottedKeepsHeader(t *testing.T) {
	var p Page
	p.Init(9, TypeInternal)
	p.SetRightSibling(1234)
	p.SetLSN(55)
	for i := 0; i < 20; i++ {
		must(t, p.InsertCell(i, bytes.Repeat([]byte{byte(i)}, 150)))
	}
	for i := 0; i < 10; i++ {
		p.DeleteCell(0)
	}
	p.Compact()
	if p.ID() != 9 || p.Type() != TypeInternal || p.RightSibling() != 1234 || p.LSN() != 55 {
		t.Fatal("slot operations changed header fields")
	}
}

// TestSlottedRandom runs random operations against a slice model.
func TestSlottedRandom(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	var p Page
	p.Init(1, TypeLeaf)
	var model [][]byte

	randCell := func() []byte {
		c := make([]byte, rng.Intn(300))
		rng.Read(c)
		return c
	}

	for step := 0; step < 20000; step++ {
		switch op := rng.Intn(3); {
		case op == 0 || len(model) == 0:
			i, c := rng.Intn(len(model)+1), randCell()
			err := p.InsertCell(i, c)
			if err == nil {
				model = append(model[:i], append([][]byte{c}, model[i:]...)...)
			} else if !errors.Is(err, ErrNoSpace) {
				t.Fatal(err)
			}
		case op == 1:
			i := rng.Intn(len(model))
			p.DeleteCell(i)
			model = append(model[:i], model[i+1:]...)
		default:
			i, c := rng.Intn(len(model)), randCell()
			err := p.UpdateCell(i, c)
			if err == nil {
				model[i] = c
			} else if !errors.Is(err, ErrNoSpace) {
				t.Fatal(err)
			}
		}

		if p.NumSlots() != len(model) {
			t.Fatalf("step %d: %d slots, model has %d", step, p.NumSlots(), len(model))
		}
		for i, c := range model {
			if !bytes.Equal(p.Cell(i), c) {
				t.Fatalf("step %d: cell %d differs", step, i)
			}
		}
	}
}

func TestFreeListPage(t *testing.T) {
	var p Page
	p.InitFree(8, 3)
	if p.Type() != TypeFree || p.NextFree() != 3 {
		t.Fatalf("free page = type %d next %d", p.Type(), p.NextFree())
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func assertCells(t *testing.T, p *Page, want ...string) {
	t.Helper()
	var got []string
	for i := 0; i < p.NumSlots(); i++ {
		got = append(got, string(p.Cell(i)))
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("cells = %q, want %q", got, want)
	}
}

func TestCellEncodingBoundaries(t *testing.T) {
	tests := []struct {
		name  string
		key   []byte
		value []byte
	}{
		{
			name:  "empty key and value",
			key:   nil,
			value: nil,
		},
		{
			name:  "maximum key",
			key:   bytes.Repeat([]byte{'k'}, MaxKeySize),
			value: []byte("value"),
		},
		{
			name:  "maximum value",
			key:   []byte("key"),
			value: bytes.Repeat([]byte{'v'}, MaxValueSize),
		},
		{
			name:  "maximum key and value",
			key:   bytes.Repeat([]byte{'k'}, MaxKeySize),
			value: bytes.Repeat([]byte{'v'}, MaxValueSize),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := EncodeCell(tt.key, tt.value)
			if err != nil {
				t.Fatalf("EncodeCell: %v", err)
			}

			key, value, err := DecodeCell(encoded)
			if err != nil {
				t.Fatalf("DecodeCell: %v", err)
			}

			if !bytes.Equal(key, tt.key) {
				t.Fatalf("key mismatch")
			}
			if !bytes.Equal(value, tt.value) {
				t.Fatalf("value mismatch")
			}
		})
	}
}

func TestDecodeCellRejectsMalformedLengths(t *testing.T) {
	// Header, says there is only 5 byte
	// but there is only 1 byte.
	malformedKey := []byte{
		5, 0, // key length = 5
		0, 0, // value length = 0
		'x',
	}

	// Header, says value is only 3 byte
	// but there is only 1 byte
	malformedValue := []byte{
		1, 0, // key length = 1
		3, 0, // value length = 3
		'k', 'v',
	}

	tests := []struct {
		name string
		data []byte
	}{
		{name: "empty cell", data: nil},
		{name: "short header", data: []byte{1, 0, 0}},
		{name: "key length exceeds data", data: malformedKey},
		{name: "value length exceeds data", data: malformedValue},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := DecodeCell(tt.data)
			if !errors.Is(err, ErrBadCell) {
				t.Fatalf("err = %v, want ErrBadCell", err)
			}
		})
	}
}
