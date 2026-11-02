package btree

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"baseDB/internal/buffer"
	"baseDB/internal/page"
	"baseDB/internal/wal"
)

// TestFailedPutIsRolledBack fills a tree through a pool too small for a root
// split at height 2. The Put that needs the split fails half-way; the
// mini-transaction is aborted, its page changes are undone, nothing is
// logged, and the tree stays usable.
func TestFailedPutIsRolledBack(t *testing.T) {
	e := newEnvFrames(t, 4)
	var failed int
	for i := 0; ; i++ {
		err := e.tree.Put(key(i), value(i))
		if errors.Is(err, buffer.ErrNoFreeFrame) {
			failed = i
			break
		}
		if err != nil {
			t.Fatalf("Put(%d): %v", i, err)
		}
		if i > 100000 {
			t.Fatal("pool never ran out of frames")
		}
	}

	mustVerify(t, e.tree)
	for i := 0; i < failed; i++ {
		mustGet(t, e.tree, key(i), value(i))
	}
	mustGet(t, e.tree, key(failed), nil)

	txns, err := e.log.Transactions()
	must(t, err)
	if len(txns) != failed {
		t.Fatalf("%d committed transactions, want one per successful Put (%d)", len(txns), failed)
	}

	// Smaller changes that need fewer frames still work.
	if _, err := e.tree.Delete(key(0)); err != nil {
		t.Fatalf("tree unusable after rollback: %v", err)
	}
	mustGet(t, e.tree, key(0), nil)
	mustVerify(t, e.tree)
}

// memStore is an in-memory data file + meta page, used as the target of a
// replay. It implements buffer.Disk and MetaStore.
type memStore struct {
	pages map[page.PageID]page.Page
	meta  page.Meta
}

func newMemStore() *memStore {
	return &memStore{pages: map[page.PageID]page.Page{}, meta: page.NewMeta()}
}

func (s *memStore) ReadPage(id page.PageID, dst *page.Page) error {
	*dst = s.pages[id] // missing pages read as zeros
	return nil
}
func (s *memStore) WritePage(id page.PageID, src *page.Page) error { s.pages[id] = *src; return nil }
func (s *memStore) Sync() error                                    { return nil }
func (s *memStore) AllocatePage() (page.PageID, error) {
	id := s.meta.NextPageID
	s.meta.NextPageID++
	return id, nil
}
func (s *memStore) UnallocatePage(id page.PageID) error {
	if id+1 != s.meta.NextPageID {
		return fmt.Errorf("unallocate %d, newest is %d", id, s.meta.NextPageID-1)
	}
	s.meta.NextPageID--
	return nil
}
func (s *memStore) Meta() page.Meta                  { return s.meta }
func (s *memStore) UpdateMeta(fn func(m *page.Meta)) { fn(&s.meta) }

// replay applies every committed transaction to an empty store.
func replay(txns []wal.Txn) *memStore {
	s := newMemStore()
	for _, txn := range txns {
		for i := range txn.Records {
			r := &txn.Records[i]
			switch r.Type {
			case wal.TypePage:
				p := s.pages[r.PageID]
				wal.Apply(&p, r)
				s.pages[r.PageID] = p
			case wal.TypeMeta:
				s.meta = r.Meta
			}
		}
	}
	return s
}

// TestLogRebuildsTree runs random operations without ever flushing data
// pages explicitly, then rebuilds the database from the log alone: the
// rebuilt tree must pass Verify and hold exactly the model's contents, and
// its pages must equal what the buffer pool wrote to the data file.
func TestLogRebuildsTree(t *testing.T) {
	e := newEnv(t)
	rng := rand.New(rand.NewSource(9))
	model := map[string][]byte{}
	for step := 0; step < 20000; step++ {
		n := rng.Intn(3000)
		k := key(n)
		if rng.Intn(4) == 0 {
			if _, err := e.tree.Delete(k); err != nil {
				t.Fatal(err)
			}
			delete(model, string(k))
			continue
		}
		v := []byte(fmt.Sprintf("v%d-%s", step, bytes.Repeat([]byte{'x'}, rng.Intn(300))))
		mustPut(t, e.tree, k, v)
		model[string(k)] = v
	}

	txns, err := e.log.Transactions()
	must(t, err)
	store := replay(txns)

	// The rebuilt database is a valid tree with the same contents.
	pool := buffer.New(store, testFrames, nil)
	rebuilt := Open(pool, store, nil)
	st := mustVerify(t, rebuilt)
	if st.keys != len(model) {
		t.Fatalf("rebuilt tree has %d keys, model %d", st.keys, len(model))
	}
	var want []string
	for k := range model {
		want = append(want, k)
	}
	sort.Strings(want)
	got := scanAll(t, rebuilt, nil, nil)
	for i, en := range got {
		if string(en.key) != want[i] || !bytes.Equal(en.value, model[want[i]]) {
			t.Fatalf("rebuilt entry %d = %q, want %q", i, en.key, want[i])
		}
	}
	if store.meta != e.pg.Meta() {
		t.Fatalf("rebuilt meta %+v, live meta %+v", store.meta, e.pg.Meta())
	}

	// Byte for byte, the log reproduces the data file.
	must(t, e.pool.FlushAll())
	for id, p := range store.pages {
		var onDisk page.Page
		must(t, e.pg.ReadPage(id, &onDisk))
		p.UpdateChecksum() // the in-memory checksum field is never maintained
		if p != onDisk {
			t.Fatalf("page %d rebuilt from the log differs from the data file", id)
		}
	}
	t.Logf("%d transactions rebuilt %d pages (%+v)", len(txns), len(store.pages), st)
}

// TestEveryWriteIsLogged checks the transaction structure of single
// operations: a plain insert logs exactly one page, a root split logs the
// three pages plus the new root pointer, and a no-op delete logs nothing.
func TestEveryWriteIsLogged(t *testing.T) {
	e := newEnv(t)
	count := func() int {
		txns, err := e.log.Transactions()
		must(t, err)
		return len(txns)
	}

	mustPut(t, e.tree, key(1), value(1))
	if found, _ := e.tree.Delete(key(999)); found || count() != 1 {
		t.Fatalf("deleting a missing key wrote a transaction (%d)", count())
	}

	oldRoot := e.tree.root
	for i := 2; e.tree.root == oldRoot; i++ {
		mustPut(t, e.tree, key(i), value(i))
	}
	txns, err := e.log.Transactions()
	must(t, err)
	last := txns[len(txns)-1]
	var pages, metas int
	for _, r := range last.Records {
		switch r.Type {
		case wal.TypePage:
			pages++
		case wal.TypeMeta:
			metas++
			if r.Meta.RootPageID != e.tree.root {
				t.Fatalf("logged root %d, tree root %d", r.Meta.RootPageID, e.tree.root)
			}
		}
	}
	if pages != 3 || metas != 1 {
		t.Fatalf("root split logged %d pages and %d meta records, want 3 and 1", pages, metas)
	}
	if prev := txns[len(txns)-2]; len(prev.Records) != 1 || prev.Records[0].Type != wal.TypePage {
		t.Fatalf("plain insert logged %+v, want one PAGE record", prev.Records)
	}
}
