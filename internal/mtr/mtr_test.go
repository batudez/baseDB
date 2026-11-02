package mtr

import (
	"path/filepath"
	"testing"

	"baseDB/internal/buffer"
	"baseDB/internal/page"
	"baseDB/internal/pager"
	"baseDB/internal/wal"
)

type env struct {
	pg   *pager.Pager
	log  *wal.Log
	pool *buffer.Pool
}

func newEnv(t *testing.T, frames int) *env {
	t.Helper()
	dir := t.TempDir()
	pg, err := pager.Open(filepath.Join(dir, "data.db"))
	must(t, err)
	log, err := wal.Open(filepath.Join(dir, "wal.log"))
	must(t, err)
	t.Cleanup(func() { log.Close(); pg.Close() })
	return &env{pg: pg, log: log, pool: buffer.New(pg, frames, log)}
}

func (e *env) begin() *Mtr { return Begin(e.pool, e.log, e.pg) }

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// committedPage creates a leaf with one cell in its own mini-transaction.
func committedPage(t *testing.T, e *env, cell string) page.PageID {
	t.Helper()
	m := e.begin()
	p, err := m.Create(page.TypeLeaf)
	must(t, err)
	must(t, p.InsertCell(0, []byte(cell)))
	id := p.ID()
	_, err = m.Commit()
	must(t, err)
	return id
}

func TestCommitLogsOneGroupedTransaction(t *testing.T) {
	e := newEnv(t, 8)
	existing := committedPage(t, e, "old")

	m := e.begin()
	created, err := m.Create(page.TypeLeaf)
	must(t, err)
	must(t, created.InsertCell(0, []byte("new")))
	p, err := m.Fetch(existing)
	must(t, err)
	must(t, p.InsertCell(1, []byte("second")))
	m.MarkDirty(p)
	m.UpdateMeta(func(meta *page.Meta) { meta.RootPageID = created.ID() })
	createdID := created.ID()
	commitLSN, err := m.Commit()
	must(t, err)

	txns, err := e.log.Transactions()
	must(t, err)
	if len(txns) != 2 {
		t.Fatalf("%d transactions, want 2", len(txns))
	}
	txn := txns[1]
	if txn.CommitLSN != commitLSN {
		t.Fatalf("commit LSN %d, log says %d", commitLSN, txn.CommitLSN)
	}
	// Both page changes and the meta change are in the same transaction.
	var types []wal.RecordType
	for _, r := range txn.Records {
		types = append(types, r.Type)
	}
	if len(types) != 3 || types[0] != wal.TypePage || types[1] != wal.TypePage || types[2] != wal.TypeMeta {
		t.Fatalf("record types = %v, want [PAGE PAGE META]", types)
	}
	if !txn.Records[0].Init || txn.Records[1].Init {
		t.Fatal("only the created page should be an init record")
	}
	if meta := txn.Records[2].Meta; meta.RootPageID != createdID || meta.NextPageID != createdID+1 {
		t.Fatalf("logged meta = %+v", meta)
	}

	// The commit is durable, the meta change applied, page LSNs stamped.
	if e.log.DurableLSN() <= commitLSN {
		t.Fatal("COMMIT record not durable after Commit")
	}
	if e.pg.Meta().RootPageID != createdID {
		t.Fatal("meta change not applied after commit")
	}
	for i, id := range []page.PageID{createdID, existing} {
		f, err := e.pool.Fetch(id)
		must(t, err)
		if f.Page().LSN() != txn.Records[i].LSN {
			t.Fatalf("page %d LSN = %d, want its PAGE record's LSN %d", id, f.Page().LSN(), txn.Records[i].LSN)
		}
		must(t, e.pool.Unpin(id, false))
	}
}

func TestNothingChangedLogsNothing(t *testing.T) {
	e := newEnv(t, 8)
	id := committedPage(t, e, "x")
	before := e.log.DurableLSN()

	m := e.begin()
	p, err := m.Fetch(id)
	must(t, err)
	m.MarkDirty(p) // marked, but bytes unchanged
	lsn, err := m.Commit()
	must(t, err)
	if lsn != 0 || e.log.DurableLSN() != before {
		t.Fatalf("unchanged mini-transaction wrote to the log (lsn %d)", lsn)
	}
}

func TestAbortRestoresPages(t *testing.T) {
	e := newEnv(t, 8)
	id := committedPage(t, e, "keep")
	f, err := e.pool.Fetch(id)
	must(t, err)
	want := *f.Page()
	must(t, e.pool.Unpin(id, false))
	logEnd := e.log.DurableLSN()

	m := e.begin()
	p, err := m.Fetch(id)
	must(t, err)
	p.DeleteCell(0)
	must(t, p.InsertCell(0, []byte("changed")))
	m.MarkDirty(p)
	m.UpdateMeta(func(meta *page.Meta) { meta.RootPageID = 42 })
	m.Abort()

	f, err = e.pool.Fetch(id)
	must(t, err)
	if *f.Page() != want {
		t.Fatal("Abort did not restore the page")
	}
	must(t, e.pool.Unpin(id, false))
	if e.pg.Meta().RootPageID == 42 {
		t.Fatal("Abort applied the meta change")
	}
	if e.log.DurableLSN() != logEnd {
		t.Fatal("Abort wrote to the log")
	}
}

// TestPagesStayPinnedUntilCommit checks the no-steal rule: while a
// mini-transaction runs, its pages cannot be evicted, so an uncommitted
// change can never reach the data file.
func TestPagesStayPinnedUntilCommit(t *testing.T) {
	e := newEnv(t, 2)
	m := e.begin()
	_, err := m.Create(page.TypeLeaf)
	must(t, err)
	_, err = m.Create(page.TypeLeaf)
	must(t, err)
	if _, err := e.pool.NewPage(); err == nil {
		t.Fatal("a page of a running mini-transaction was evicted")
	}
	_, err = m.Commit()
	must(t, err)
	if _, err := e.pool.NewPage(); err != nil {
		t.Fatalf("pages still pinned after commit: %v", err)
	}
}

// TestLogRebuildsPages replays the log onto empty pages and compares with
// what the buffer pool wrote to the data file: the log alone is enough to
// reproduce every page, slot directory and cell included.
func TestLogRebuildsPages(t *testing.T) {
	e := newEnv(t, 4) // small pool: pages are evicted between transactions
	var ids []page.PageID
	for i := 0; i < 10; i++ {
		ids = append(ids, committedPage(t, e, "start"))
	}
	for step := 0; step < 500; step++ {
		m := e.begin()
		id := ids[step%len(ids)]
		p, err := m.Fetch(id)
		must(t, err)
		switch n := p.NumSlots(); {
		case n > 0 && step%3 == 0:
			p.DeleteCell(step % n)
		case n > 0 && step%5 == 0:
			must(t, p.UpdateCell(step%n, []byte("updated-value-that-is-longer")))
		default:
			if err := p.InsertCell(n, []byte("cell-"+string(rune('a'+step%26)))); err != nil {
				p.Compact()
			}
		}
		if step%7 == 0 {
			p.SetRightSibling(ids[(step+1)%len(ids)])
		}
		m.MarkDirty(p)
		_, err = m.Commit()
		must(t, err)
	}
	must(t, e.pool.FlushAll())

	txns, err := e.log.Transactions()
	must(t, err)
	replayed := map[page.PageID]*page.Page{}
	for _, txn := range txns {
		for i := range txn.Records {
			r := &txn.Records[i]
			if r.Type != wal.TypePage {
				continue
			}
			if replayed[r.PageID] == nil {
				replayed[r.PageID] = &page.Page{}
			}
			wal.Apply(replayed[r.PageID], r)
		}
	}
	for _, id := range ids {
		var onDisk page.Page
		must(t, e.pg.ReadPage(id, &onDisk))
		got := *replayed[id]
		got.UpdateChecksum() // the in-memory checksum field is never maintained
		if got != onDisk {
			t.Fatalf("page %d rebuilt from the log differs from the data file", id)
		}
	}
}

func TestFreeAndReusePages(t *testing.T) {
	e := newEnv(t, 8)
	a := committedPage(t, e, "a")
	b := committedPage(t, e, "b")
	next := e.pg.Meta().NextPageID

	// Free both pages in one transaction: the free list is b -> a.
	m := e.begin()
	for _, id := range []page.PageID{a, b} {
		p, err := m.Fetch(id)
		must(t, err)
		m.Free(p)
	}
	_, err := m.Commit()
	must(t, err)
	if e.pg.Meta().FreeListHead != b {
		t.Fatalf("free list head = %d, want %d", e.pg.Meta().FreeListHead, b)
	}
	txns, err := e.log.Transactions()
	must(t, err)
	last := txns[len(txns)-1]
	if !last.Records[0].Init || !last.Records[1].Init || last.Records[2].Meta.FreeListHead != b {
		t.Fatalf("free not logged as init records + META: %+v", last.Records)
	}

	// Two creates reuse b then a; the third grows the file.
	m = e.begin()
	var got []page.PageID
	for i := 0; i < 3; i++ {
		p, err := m.Create(page.TypeLeaf)
		must(t, err)
		if p.NumSlots() != 0 || p.Type() != page.TypeLeaf {
			t.Fatal("reused page not reformatted")
		}
		got = append(got, p.ID())
	}
	_, err = m.Commit()
	must(t, err)
	if got[0] != b || got[1] != a || got[2] != next {
		t.Fatalf("created pages %v, want [%d %d %d]", got, b, a, next)
	}
	if meta := e.pg.Meta(); meta.FreeListHead != page.InvalidPageID || meta.NextPageID != next+1 {
		t.Fatalf("meta after reuse = %+v", meta)
	}
}

// TestAbortGivesPagesBack: an aborted Mtr leaks nothing. Pages taken from
// the free list go back on it, pages that grew the file are given back.
func TestAbortGivesPagesBack(t *testing.T) {
	e := newEnv(t, 8)
	freed := committedPage(t, e, "x")
	m := e.begin()
	p, err := m.Fetch(freed)
	must(t, err)
	m.Free(p)
	_, err = m.Commit()
	must(t, err)
	before := e.pg.Meta()

	m = e.begin()
	r, err := m.Create(page.TypeLeaf) // from the free list
	must(t, err)
	n, err := m.Create(page.TypeLeaf) // grows the file
	must(t, err)
	if r.ID() != freed || n.ID() != before.NextPageID {
		t.Fatalf("created %d, %d", r.ID(), n.ID())
	}
	m.Abort()

	if after := e.pg.Meta(); after != before {
		t.Fatalf("meta after abort = %+v, want %+v", after, before)
	}
	f, err := e.pool.Fetch(freed)
	must(t, err)
	if f.Page().Type() != page.TypeFree {
		t.Fatal("page taken from the free list was not restored")
	}
	must(t, e.pool.Unpin(freed, false))
	// The given-back ID is handed out again.
	m = e.begin()
	n2, err := m.Create(page.TypeLeaf)
	must(t, err)
	n3, err := m.Create(page.TypeLeaf)
	must(t, err)
	if n2.ID() != freed || n3.ID() != before.NextPageID {
		t.Fatalf("after abort: created %d, %d", n2.ID(), n3.ID())
	}
	m.Abort()
}

// TestFullPageImageAfterCheckpoint: once the log is reset (checkpoint), the
// first change to an old page is logged as a full image, later ones as
// diffs again.
func TestFullPageImageAfterCheckpoint(t *testing.T) {
	e := newEnv(t, 8)
	id := committedPage(t, e, "old")
	must(t, e.pool.FlushAll())
	must(t, e.log.Reset(e.pg.Meta()))

	change := func(cell string) wal.Record {
		m := e.begin()
		p, err := m.Fetch(id)
		must(t, err)
		must(t, p.InsertCell(p.NumSlots(), []byte(cell)))
		m.MarkDirty(p)
		_, err = m.Commit()
		must(t, err)
		txns, err := e.log.Transactions()
		must(t, err)
		return txns[len(txns)-1].Records[0]
	}
	if first := change("one"); !first.Init {
		t.Fatal("first change after the checkpoint is not a full page image")
	}
	if second := change("two"); second.Init {
		t.Fatal("second change logged as a full image again")
	}
}
