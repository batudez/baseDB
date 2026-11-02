package pager

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"baseDB/internal/page"
)

func openTemp(t *testing.T) (*Pager, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data.db")
	pg, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pg.Close() })
	return pg, path
}

func allocAndWrite(t *testing.T, pg *Pager, fill byte) page.PageID {
	t.Helper()
	id, err := pg.AllocatePage()
	if err != nil {
		t.Fatal(err)
	}
	var p page.Page
	p.Init(id, page.TypeLeaf)
	p.SetLSN(page.LSN(id) * 10)
	p.Data()[page.HeaderSize] = fill
	if err := pg.WritePage(id, &p); err != nil {
		t.Fatal(err)
	}
	return id
}

func TestNewFile(t *testing.T) {
	pg, path := openTemp(t)
	if m := pg.Meta(); m != page.NewMeta() {
		t.Fatalf("meta = %+v, want %+v", m, page.NewMeta())
	}
	if st, _ := os.Stat(path); st.Size() != page.PageSize {
		t.Fatalf("new file size = %d, want one page", st.Size())
	}
	if id, _ := pg.AllocatePage(); id != 1 {
		t.Fatalf("first allocation = %d, want 1", id)
	}
}

func TestReopenKeepsPagesAndMeta(t *testing.T) {
	pg, path := openTemp(t)
	for i := 0; i < 5; i++ {
		allocAndWrite(t, pg, byte(i+100))
	}
	pg.UpdateMeta(func(m *page.Meta) {
		m.RootPageID = 3
		m.LastAppliedIndex = 77
	})
	if err := pg.Close(); err != nil {
		t.Fatal(err)
	}

	pg, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer pg.Close()
	want := page.Meta{RootPageID: 3, NextPageID: 6, LastAppliedIndex: 77}
	if m := pg.Meta(); m != want {
		t.Fatalf("meta = %+v, want %+v", m, want)
	}
	for id := page.PageID(1); id <= 5; id++ {
		var p page.Page
		if err := pg.ReadPage(id, &p); err != nil {
			t.Fatal(err)
		}
		if p.ID() != id || p.LSN() != page.LSN(id)*10 || p.Data()[page.HeaderSize] != byte(id+99) {
			t.Fatalf("page %d read back wrong", id)
		}
	}
}

func TestOpenRejectsNonDatabaseFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "junk.db")
	if err := os.WriteFile(path, make([]byte, page.PageSize), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); !errors.Is(err, page.ErrBadMeta) {
		t.Fatalf("err = %v, want ErrBadMeta", err)
	}
}

func TestCorruptedChecksum(t *testing.T) {
	pg, path := openTemp(t)
	a := allocAndWrite(t, pg, 1)
	b := allocAndWrite(t, pg, 2)

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteAt([]byte{0xFF}, int64(b)*page.PageSize+2000); err != nil {
		t.Fatal(err)
	}
	f.Close()

	var p page.Page
	if err := pg.ReadPage(b, &p); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
	if err := pg.ReadPage(a, &p); err != nil {
		t.Fatalf("page %d should be intact: %v", a, err)
	}
}

func TestCorruptedMetaPage(t *testing.T) {
	pg, path := openTemp(t)
	pg.Close()

	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteAt([]byte{0xFF}, 100)
	f.Close()

	if _, err := Open(path); !errors.Is(err, ErrChecksum) {
		t.Fatalf("err = %v, want ErrChecksum", err)
	}
}

func TestPageIDChecks(t *testing.T) {
	pg, _ := openTemp(t)
	var p page.Page

	if err := pg.ReadPage(page.MetaPageID, &p); !errors.Is(err, ErrMetaPage) {
		t.Fatalf("err = %v, want ErrMetaPage", err)
	}
	if err := pg.ReadPage(5, &p); !errors.Is(err, ErrPageOutOfRange) {
		t.Fatalf("err = %v, want ErrPageOutOfRange", err)
	}

	// Allocated but never written: reads as zeros.
	id, _ := pg.AllocatePage()
	p.Data()[100] = 0xAA
	if err := pg.ReadPage(id, &p); err != nil || !p.IsZero() {
		t.Fatalf("unwritten page: err=%v zero=%v", err, p.IsZero())
	}

	p.Init(id+1, page.TypeLeaf)
	if err := pg.WritePage(id, &p); !errors.Is(err, ErrMisdirected) {
		t.Fatalf("err = %v, want ErrMisdirected", err)
	}
}

func TestWriteDoesNotModifySource(t *testing.T) {
	pg, _ := openTemp(t)
	id, _ := pg.AllocatePage()
	var p page.Page
	p.Init(id, page.TypeLeaf)
	before := p
	if err := pg.WritePage(id, &p); err != nil {
		t.Fatal(err)
	}
	if p != before {
		t.Fatal("WritePage modified its input")
	}
}

func TestOpenIgnoringMetaAndSetMeta(t *testing.T) {
	pg, path := openTemp(t)
	for i := 0; i < 4; i++ {
		allocAndWrite(t, pg, byte(i))
	}
	pg.Close()

	// Tear the meta page: Open refuses, OpenIgnoringMeta opens.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteAt([]byte{0xFF}, 50)
	f.Close()
	if _, err := Open(path); !errors.Is(err, ErrChecksum) {
		t.Fatalf("Open = %v, want ErrChecksum", err)
	}
	pg2, err := OpenIgnoringMeta(path)
	if err != nil {
		t.Fatal(err)
	}
	defer pg2.Close()
	if n, _ := pg2.NumPages(); n != 5 || pg2.Meta().NextPageID != 5 {
		t.Fatalf("NumPages %d, NextPageID %d; want 5, 5", n, pg2.Meta().NextPageID)
	}

	// SetMeta restores every field, NextPageID included, and Sync persists it.
	want := page.Meta{RootPageID: 2, NextPageID: 9, FreeListHead: 3, LastAppliedIndex: 7}
	pg2.SetMeta(want)
	if err := pg2.Sync(); err != nil {
		t.Fatal(err)
	}
	pg3, err := Open(path)
	if err != nil {
		t.Fatalf("meta page still damaged after SetMeta+Sync: %v", err)
	}
	defer pg3.Abandon()
	if pg3.Meta() != want {
		t.Fatalf("meta = %+v, want %+v", pg3.Meta(), want)
	}
}

func TestUnallocatePage(t *testing.T) {
	pg, _ := openTemp(t)
	a, _ := pg.AllocatePage()
	b, _ := pg.AllocatePage()
	if err := pg.UnallocatePage(a); err == nil {
		t.Fatal("unallocated a page that is not the newest")
	}
	if err := pg.UnallocatePage(b); err != nil {
		t.Fatal(err)
	}
	if c, _ := pg.AllocatePage(); c != b {
		t.Fatalf("got page %d, want the given-back %d", c, b)
	}
}
