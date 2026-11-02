package wal

import (
	"bytes"
	"errors"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"baseDB/internal/page"
)

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func openTemp(t *testing.T) (*Log, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wal.log")
	l, err := Open(path)
	must(t, err)
	t.Cleanup(func() { l.Close() })
	return l, path
}

// appendTxn logs one committed transaction with the given page records.
func appendTxn(t *testing.T, l *Log, pages ...*Record) []*Record {
	t.Helper()
	id := l.BeginTxn()
	recs := []*Record{{Type: TypeBegin}}
	recs = append(recs, pages...)
	recs = append(recs, &Record{Type: TypeCommit})
	for _, r := range recs {
		r.TxnID = id
	}
	must(t, l.Append(recs...))
	return recs
}

func pageRec(id page.PageID, fill byte) *Record {
	return &Record{Type: TypePage, PageID: id, Runs: []Run{{Off: 100, Data: bytes.Repeat([]byte{fill}, 50)}}}
}

func TestRecordRoundTrip(t *testing.T) {
	recs := []*Record{
		{Type: TypeBegin, TxnID: 7},
		{Type: TypePage, TxnID: 7, PageID: 3, Init: true, Runs: []Run{
			{Off: 0, Data: []byte{1, 2, 3}},
			{Off: page.PageSize - 2, Data: []byte{9, 9}},
		}},
		{Type: TypePage, TxnID: 7, PageID: 4, Runs: []Run{}},
		{Type: TypeMeta, TxnID: 7, Meta: page.Meta{RootPageID: 5, NextPageID: 9, FreeListHead: 2, LastAppliedIndex: 1 << 50}},
		{Type: TypeCommit, TxnID: 7},
	}
	for i, r := range recs {
		r.LSN = page.LSN(1000 + i)
		b, err := r.appendTo(nil)
		must(t, err)
		got, err := decodeRecord(b, r.LSN)
		must(t, err)
		if !reflect.DeepEqual(&got, r) {
			t.Fatalf("round trip of %v:\n got %+v\nwant %+v", r.Type, got, *r)
		}
	}
}

func TestDecodeRejectsCorruption(t *testing.T) {
	good := &Record{Type: TypePage, LSN: 64, TxnID: 1, PageID: 3, Runs: []Run{{Off: 10, Data: []byte("abc")}}}
	b, err := good.appendTo(nil)
	must(t, err)

	cases := map[string]func() ([]byte, page.LSN){
		"flipped payload byte": func() ([]byte, page.LSN) { c := bytes.Clone(b); c[len(c)-1] ^= 1; return c, 64 },
		"flipped header byte":  func() ([]byte, page.LSN) { c := bytes.Clone(b); c[offTxn] ^= 1; return c, 64 },
		"wrong position (LSN)": func() ([]byte, page.LSN) { return b, 128 },
		"truncated":            func() ([]byte, page.LSN) { return b[:len(b)-1], 64 },
		"run outside page": func() ([]byte, page.LSN) {
			r := *good
			r.Runs = []Run{{Off: page.PageSize - 1, Data: []byte("xy")}}
			c, _ := r.appendTo(nil) // encoder does not validate; decoder must
			return c, 64
		},
		"unknown type": func() ([]byte, page.LSN) {
			r := Record{Type: 99, LSN: 64}
			c, _ := r.appendTo(nil)
			return c, 64
		},
		"page record for meta page": func() ([]byte, page.LSN) {
			r := *good
			r.PageID = page.MetaPageID
			c, _ := r.appendTo(nil)
			return c, 64
		},
	}
	for name, mk := range cases {
		buf, lsn := mk()
		if _, err := decodeRecord(buf, lsn); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: err = %v, want ErrCorrupt", name, err)
		}
	}
}

// randomChange makes a random slotted-page change, like the B+ tree does.
func randomChange(rng *rand.Rand, p *page.Page) {
	switch n := p.NumSlots(); {
	case n == 0 || rng.Intn(3) > 0:
		c := make([]byte, 1+rng.Intn(200))
		rng.Read(c)
		_ = p.InsertCell(rng.Intn(n+1), c)
	case rng.Intn(2) == 0:
		p.DeleteCell(rng.Intn(n))
	default:
		c := make([]byte, rng.Intn(300))
		rng.Read(c)
		_ = p.UpdateCell(rng.Intn(n), c)
	}
	if rng.Intn(10) == 0 {
		p.SetRightSibling(page.PageID(rng.Uint32()))
	}
	if rng.Intn(20) == 0 {
		p.Compact()
	}
}

// TestDiffApplySlottedPage checks that diffs of slotted-page changes
// (header fields, slot directory and cell data together) replay exactly.
func TestDiffApplySlottedPage(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	var p, replayed page.Page
	p.Init(5, page.TypeLeaf)

	// Creation is logged as an init record against an all-zero page.
	r := Record{Type: TypePage, PageID: 5, Init: true, LSN: 1, Runs: Diff(&page.Page{}, &p)}
	replayed.Data()[2000] = 0xEE // stale disk contents must be wiped by init
	Apply(&replayed, &r)
	p.SetLSN(1)

	for step := 2; step < 3000; step++ {
		before := p
		randomChange(rng, &p)
		r := Record{Type: TypePage, PageID: 5, LSN: page.LSN(step), Runs: Diff(&before, &p)}
		Apply(&replayed, &r)
		p.SetLSN(r.LSN)
		if replayed != p {
			t.Fatalf("step %d: replayed page differs", step)
		}
	}
	// Applying a record a second time changes nothing (idempotent).
	again := replayed
	before := p
	randomChange(rng, &p)
	last := Record{Type: TypePage, PageID: 5, LSN: 9999, Runs: Diff(&before, &p)}
	Apply(&again, &last)
	Apply(&again, &last)
	p.SetLSN(9999)
	if again != p {
		t.Fatal("applying the same record twice changed the result")
	}
}

func TestDiffIsSmallForSmallChanges(t *testing.T) {
	var before page.Page
	before.Init(1, page.TypeLeaf)
	after := before
	must(t, after.InsertCell(0, []byte("hello")))
	runs := Diff(&before, &after)
	total := 0
	for _, r := range runs {
		total += len(r.Data)
	}
	// numSlots + freeSpacePtr, one slot, 5 cell bytes: a few dozen bytes,
	// not a 4 KiB page image.
	if total > 32 {
		t.Fatalf("diff of a 5-byte insert logs %d bytes in %d runs", total, len(runs))
	}
}

func TestAppendFlushReopen(t *testing.T) {
	l, path := openTemp(t)
	first := appendTxn(t, l, pageRec(1, 'a'), pageRec(2, 'b'))
	second := appendTxn(t, l, &Record{Type: TypeMeta, Meta: page.Meta{RootPageID: 1, NextPageID: 3}})

	if first[0].LSN != fileHeaderSize {
		t.Fatalf("first LSN = %d, want %d", first[0].LSN, fileHeaderSize)
	}
	for i := 1; i < len(first); i++ {
		if first[i].LSN <= first[i-1].LSN {
			t.Fatal("LSNs not increasing")
		}
	}
	if d := l.DurableLSN(); d > first[0].LSN {
		t.Fatalf("records durable before flush (durable=%d)", d)
	}
	must(t, l.FlushUntil(first[3].LSN))
	if d := l.DurableLSN(); d <= first[3].LSN {
		t.Fatalf("durable = %d after FlushUntil(%d)", d, first[3].LSN)
	}
	must(t, l.Close())

	l2, err := Open(path)
	must(t, err)
	defer l2.Close()
	if l2.TailError() != nil {
		t.Fatalf("intact log reported tail error %v", l2.TailError())
	}
	txns, err := l2.Transactions()
	must(t, err)
	if len(txns) != 2 || len(txns[0].Records) != 2 || txns[1].Records[0].Type != TypeMeta {
		t.Fatalf("transactions = %+v", txns)
	}
	if txns[0].CommitLSN != first[3].LSN || txns[1].CommitLSN != second[2].LSN {
		t.Fatal("commit LSNs changed across reopen")
	}
	if id := l2.BeginTxn(); id <= second[0].TxnID {
		t.Fatalf("txn ID %d reused after reopen (last was %d)", id, second[0].TxnID)
	}
}

func TestFlushUntil(t *testing.T) {
	l, _ := openTemp(t)
	must(t, l.FlushUntil(0)) // a never-logged page: nothing to do
	recs := appendTxn(t, l, pageRec(1, 'x'))
	end := recs[len(recs)-1].LSN
	if err := l.FlushUntil(end + 1000); err == nil {
		t.Fatal("flushing beyond the end of the log should fail")
	}
	must(t, l.FlushUntil(end))
	if l.DurableLSN() <= end {
		t.Fatal("commit record not durable")
	}
}

func TestTornTailIsCutOff(t *testing.T) {
	l, path := openTemp(t)
	appendTxn(t, l, pageRec(1, 'a'))
	recs := appendTxn(t, l, pageRec(2, 'b'))
	must(t, l.Close())

	// Simulate a crash in the middle of writing the second COMMIT record.
	commit := recs[len(recs)-1].LSN
	must(t, os.Truncate(path, int64(commit)+5))

	l2, err := Open(path)
	must(t, err)
	if !errors.Is(l2.TailError(), ErrIncomplete) {
		t.Fatalf("TailError = %v, want ErrIncomplete", l2.TailError())
	}
	if st, _ := os.Stat(path); st.Size() != int64(commit) {
		t.Fatalf("file size = %d, want truncated to %d", st.Size(), commit)
	}
	txns, err := l2.Transactions()
	must(t, err)
	if len(txns) != 1 {
		t.Fatalf("%d committed transactions, want 1 (the torn one has no COMMIT)", len(txns))
	}

	// New records go where the torn one was and are readable.
	appendTxn(t, l2, pageRec(3, 'c'))
	must(t, l2.Close())
	txns, err = ReadTransactions(path)
	must(t, err)
	if len(txns) != 2 || txns[1].Records[0].PageID != 3 {
		t.Fatalf("after append: %+v", txns)
	}
}

func TestCorruptRecordStopsTheLog(t *testing.T) {
	l, path := openTemp(t)
	first := appendTxn(t, l, pageRec(1, 'a'))
	appendTxn(t, l, pageRec(2, 'b'))
	must(t, l.Close())

	// Flip a byte inside the first transaction's PAGE record.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	must(t, err)
	_, err = f.WriteAt([]byte{0xFF}, int64(first[1].LSN)+recordHeaderSize+10)
	must(t, err)
	f.Close()

	if _, err := ReadTransactions(path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("ReadTransactions err = %v, want ErrCorrupt", err)
	}
	l2, err := Open(path)
	must(t, err)
	defer l2.Close()
	if !errors.Is(l2.TailError(), ErrCorrupt) {
		t.Fatalf("TailError = %v, want ErrCorrupt", l2.TailError())
	}
	// Everything from the bad record on is gone: the first transaction lost
	// its PAGE and COMMIT records, so nothing committed remains.
	txns, err := l2.Transactions()
	must(t, err)
	if len(txns) != 0 {
		t.Fatalf("%d transactions survived, want 0", len(txns))
	}
	if l2.DurableLSN() != first[1].LSN {
		t.Fatalf("log ends at %d, want %d", l2.DurableLSN(), first[1].LSN)
	}
}

func TestUncommittedTxnIsIgnored(t *testing.T) {
	l, _ := openTemp(t)
	appendTxn(t, l, pageRec(1, 'a'))
	id := l.BeginTxn()
	must(t, l.Append(&Record{Type: TypeBegin, TxnID: id}, &Record{Type: TypePage, TxnID: id, PageID: 2}))
	must(t, l.Flush())

	txns, err := l.Transactions()
	must(t, err)
	if len(txns) != 1 {
		t.Fatalf("%d transactions, want only the committed one", len(txns))
	}
}

func TestBadGroupingIsCorrupt(t *testing.T) {
	cases := map[string][]*Record{
		"page outside txn":    {{Type: TypePage, TxnID: 1, PageID: 2}},
		"commit w/o begin":    {{Type: TypeCommit, TxnID: 1}},
		"duplicate begin":     {{Type: TypeBegin, TxnID: 1}, {Type: TypeBegin, TxnID: 1}},
		"reused after commit": {{Type: TypeBegin, TxnID: 1}, {Type: TypeCommit, TxnID: 1}, {Type: TypeBegin, TxnID: 1}},
	}
	for name, recs := range cases {
		l, _ := openTemp(t)
		must(t, l.Append(recs...))
		must(t, l.Flush())
		if _, err := l.Transactions(); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: err = %v, want ErrCorrupt", name, err)
		}
	}
}

func TestBadHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "junk.log")
	must(t, os.WriteFile(path, []byte("this is not a log file"), 0o644))
	if _, err := Open(path); !errors.Is(err, ErrBadLogFile) {
		t.Fatalf("err = %v, want ErrBadLogFile", err)
	}
}

func TestResetTruncatesAndContinuesLSNs(t *testing.T) {
	l, path := openTemp(t)
	for i := 0; i < 50; i++ {
		appendTxn(t, l, pageRec(page.PageID(i+1), 'a'))
	}
	must(t, l.Flush())
	end := l.DurableLSN()
	bigSize := l.Size()
	lastTxn := l.BeginTxn()

	meta := page.Meta{RootPageID: 7, NextPageID: 51, FreeListHead: 3}
	must(t, l.Reset(meta))
	if l.StartLSN() != end {
		t.Fatalf("StartLSN = %d, want the old end %d", l.StartLSN(), end)
	}
	if l.Size() >= bigSize/10 {
		t.Fatalf("log not truncated: %d bytes (was %d)", l.Size(), bigSize)
	}
	txns, err := l.Transactions()
	must(t, err)
	if len(txns) != 1 || len(txns[0].Records) != 1 || txns[0].Records[0].Meta != meta {
		t.Fatalf("after Reset: %+v, want one META transaction", txns)
	}
	if txns[0].ID <= lastTxn {
		t.Fatal("transaction IDs went backwards")
	}

	// New records continue the LSN sequence and survive a reopen.
	recs := appendTxn(t, l, pageRec(9, 'z'))
	if recs[0].LSN <= end {
		t.Fatalf("new LSN %d not after the old end %d", recs[0].LSN, end)
	}
	must(t, l.Close())
	l2, err := Open(path)
	must(t, err)
	defer l2.Close()
	if l2.StartLSN() != end || l2.TailError() != nil {
		t.Fatalf("reopen: start %d, tail %v", l2.StartLSN(), l2.TailError())
	}
	txns, err = l2.Transactions()
	must(t, err)
	if len(txns) != 2 || txns[1].Records[0].LSN != recs[1].LSN {
		t.Fatalf("after reopen: %+v", txns)
	}
}

// After a Reset, LSNs no longer equal file offsets: cutting a torn tail must
// still land on the right record boundary.
func TestTornTailAfterReset(t *testing.T) {
	l, path := openTemp(t)
	for i := 0; i < 20; i++ {
		appendTxn(t, l, pageRec(1, 'a'))
	}
	must(t, l.Reset(page.Meta{NextPageID: 2}))
	appendTxn(t, l, pageRec(1, 'b'))
	torn := appendTxn(t, l, pageRec(1, 'c'))
	must(t, l.Close())
	st, err := os.Stat(path)
	must(t, err)
	must(t, os.Truncate(path, st.Size()-2))

	l2, err := Open(path)
	must(t, err)
	defer l2.Close()
	if !errors.Is(l2.TailError(), ErrIncomplete) || l2.DurableLSN() != torn[len(torn)-1].LSN {
		t.Fatalf("tail %v, log ends at %d, want ErrIncomplete at %d", l2.TailError(), l2.DurableLSN(), torn[len(torn)-1].LSN)
	}
	txns, err := l2.Transactions()
	must(t, err)
	if len(txns) != 2 { // the Reset META transaction and 'b'
		t.Fatalf("%d transactions, want 2", len(txns))
	}
}

func TestStaleTempFileIsRemoved(t *testing.T) {
	l, path := openTemp(t)
	must(t, l.Close())
	must(t, os.WriteFile(path+".tmp", []byte("half-written replacement"), 0o644))
	l2, err := Open(path)
	must(t, err)
	defer l2.Close()
	if _, err := os.Stat(path + ".tmp"); !os.IsNotExist(err) {
		t.Fatalf("stale temp file still there: %v", err)
	}
}
