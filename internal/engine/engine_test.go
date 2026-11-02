package engine

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"baseDB/internal/page"
	"baseDB/internal/recovery"
	"baseDB/internal/wal"
)

func key(i int) []byte { return []byte(fmt.Sprintf("key%06d", i)) }
func val(i int) []byte {
	return []byte(fmt.Sprintf("value-%d-%s", i, bytes.Repeat([]byte{'v'}, i%200)))
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func mustOpen(t *testing.T, dir string, opts Options) *DB {
	t.Helper()
	db, err := Open(dir, opts)
	must(t, err)
	return db
}

// fast: real recovery and file formats, but no fsync (durability across a
// power failure is not what these tests check; the log contents are).
var fast = Options{NoSync: true, PoolFrames: 16}

// contents returns everything in the database.
func contents(t *testing.T, db *DB) map[string]string {
	t.Helper()
	out := map[string]string{}
	it := db.Scan(nil, nil)
	for it.Next() {
		out[string(it.Key())] = string(it.Value())
	}
	must(t, it.Err())
	return out
}

func checkContents(t *testing.T, db *DB, want map[string]string, context string) {
	t.Helper()
	if err := db.Verify(); err != nil {
		t.Fatalf("%s: %v", context, err)
	}
	got := contents(t, db)
	if len(got) != len(want) {
		t.Fatalf("%s: %d keys, want %d", context, len(got), len(want))
	}
	for k, v := range want {
		if got[k] != v {
			t.Fatalf("%s: %q = %.20q, want %.20q", context, k, got[k], v)
		}
	}
}

func TestPutCloseOpenGet(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, Options{})
	must(t, db.Put([]byte("hello"), []byte("world")))
	must(t, db.Close())

	db = mustOpen(t, dir, Options{})
	defer db.Close()
	v, ok, err := db.Get([]byte("hello"))
	if err != nil || !ok || string(v) != "world" {
		t.Fatalf("Get after reopen = %q, %v, %v", v, ok, err)
	}
	if st := db.RecoveryStats(); st.RecordsApplied != 0 || st.PagesWritten != 0 {
		t.Fatalf("clean shutdown left work for recovery: %+v", st)
	}
}

func TestDeleteAndUpdateSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, fast)
	for i := 0; i < 1000; i++ {
		must(t, db.Put(key(i), val(i)))
	}
	for i := 0; i < 1000; i += 3 {
		found, err := db.Delete(key(i))
		if err != nil || !found {
			t.Fatalf("Delete(%d) = %v, %v", i, found, err)
		}
	}
	for i := 1; i < 1000; i += 3 {
		must(t, db.Put(key(i), []byte("updated")))
	}
	must(t, db.Close())

	db = mustOpen(t, dir, fast)
	defer db.Close()
	for i := 0; i < 1000; i++ {
		v, ok, err := db.Get(key(i))
		must(t, err)
		switch i % 3 {
		case 0:
			if ok {
				t.Fatalf("deleted key %d is back", i)
			}
		case 1:
			if string(v) != "updated" {
				t.Fatalf("key %d = %q, want the update", i, v)
			}
		default:
			if !bytes.Equal(v, val(i)) {
				t.Fatalf("key %d = %q", i, v)
			}
		}
	}
	must(t, db.Verify())
}

func TestClosedDB(t *testing.T) {
	db := mustOpen(t, t.TempDir(), fast)
	must(t, db.Close())
	if err := db.Put([]byte("k"), nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Put after Close = %v, want ErrClosed", err)
	}
	if err := db.Close(); !errors.Is(err, ErrClosed) {
		t.Fatalf("second Close = %v, want ErrClosed", err)
	}
}

// randomOps applies n random puts/deletes to db and model.
func randomOps(t *testing.T, rng *rand.Rand, db *DB, model map[string]string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		k := key(rng.Intn(2000))
		if rng.Intn(4) == 0 {
			_, err := db.Delete(k)
			must(t, err)
			delete(model, string(k))
			continue
		}
		v := bytes.Repeat([]byte{byte('a' + rng.Intn(26))}, rng.Intn(400))
		must(t, db.Put(k, v))
		model[string(k)] = string(v)
	}
}

// TestManySessions alternates clean shutdowns and crashes. A crash loses
// every page that was only in the buffer pool; recovery must bring them back.
func TestManySessions(t *testing.T) {
	dir := t.TempDir()
	rng := rand.New(rand.NewSource(4))
	model := map[string]string{}
	for session := 0; session < 12; session++ {
		db := mustOpen(t, dir, fast)
		checkContents(t, db, model, fmt.Sprintf("start of session %d", session))
		randomOps(t, rng, db, model, 500)
		if session%2 == 0 {
			db.crash()
		} else {
			must(t, db.Close())
		}
	}
}

func TestCrashRecoveryRedoesPages(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, fast)
	model := map[string]string{}
	randomOps(t, rand.New(rand.NewSource(1)), db, model, 3000)
	db.crash() // nothing flushed: the data file is far behind the log

	db = mustOpen(t, dir, fast)
	defer db.Close()
	st := db.RecoveryStats()
	if st.RecordsApplied == 0 || st.PagesWritten == 0 || !st.MetaRestored {
		t.Fatalf("recovery did no work: %+v", st)
	}
	checkContents(t, db, model, "after crash")
	t.Logf("%+v", st)
}

func TestRecoveryIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, fast)
	model := map[string]string{}
	randomOps(t, rand.New(rand.NewSource(2)), db, model, 2000)
	db.crash()

	// Crash again right after recovery, before any new work, then once more.
	db = mustOpen(t, dir, fast)
	first := db.RecoveryStats()
	db.crash()
	db = mustOpen(t, dir, fast)
	defer db.Close()
	second := db.RecoveryStats()

	if first.RecordsApplied == 0 {
		t.Fatal("first recovery did nothing")
	}
	if second.RecordsApplied != 0 || second.PagesWritten != 0 {
		t.Fatalf("recovery was not durable, second run redid work: %+v", second)
	}
	checkContents(t, db, model, "after two recoveries")
}

// TestUncommittedTxnIsNotApplied writes a transaction without COMMIT into
// the log by hand: one that would wipe the root page and change the root
// pointer. Recovery must ignore it.
func TestUncommittedTxnIsNotApplied(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, fast)
	model := map[string]string{}
	randomOps(t, rand.New(rand.NewSource(3)), db, model, 1000)
	root := db.pg.Meta().RootPageID
	db.crash()

	log, err := wal.Open(filepath.Join(dir, LogFileName))
	must(t, err)
	txn := log.BeginTxn()
	must(t, log.Append(
		&wal.Record{Type: wal.TypeBegin, TxnID: txn},
		&wal.Record{Type: wal.TypePage, TxnID: txn, PageID: root, Init: true}, // root → all zeros
		&wal.Record{Type: wal.TypeMeta, TxnID: txn, Meta: page.Meta{RootPageID: 9999, NextPageID: 10000}},
	))
	must(t, log.Close())

	db = mustOpen(t, dir, fast)
	defer db.Close()
	if st := db.RecoveryStats(); st.Uncommitted != 1 {
		t.Fatalf("Uncommitted = %d, want 1", st.Uncommitted)
	}
	if db.pg.Meta().RootPageID != root {
		t.Fatal("META of the uncommitted transaction was applied")
	}
	checkContents(t, db, model, "after uncommitted txn")
}

// TestTornCommitRecord crashes in the middle of writing a COMMIT record:
// that transaction is lost, the previous one is not.
func TestTornCommitRecord(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, fast)
	must(t, db.Put([]byte("a"), []byte("committed")))
	logPath := filepath.Join(dir, LogFileName)
	must(t, db.Put([]byte("b"), []byte("torn")))
	db.crash()

	st, err := os.Stat(logPath)
	must(t, err)
	const commitSize = 25 // a COMMIT record is just the record header
	commitStart := st.Size() - commitSize
	must(t, os.Truncate(logPath, st.Size()-3)) // last bytes of COMMIT missing

	db = mustOpen(t, dir, fast)
	defer db.Close()
	rs := db.RecoveryStats()
	if !errors.Is(rs.LogTail, wal.ErrIncomplete) || rs.Uncommitted != 1 {
		t.Fatalf("LogTail = %v, Uncommitted = %d; want ErrIncomplete, 1", rs.LogTail, rs.Uncommitted)
	}
	checkContents(t, db, map[string]string{"a": "committed"}, "after torn commit")
	// The log is cut at the torn record. BEGIN and PAGE of "b" stay in the
	// file, but without COMMIT they are ignored forever.
	if st, _ := os.Stat(logPath); st.Size() != commitStart {
		t.Fatalf("log cut at %d, want %d (start of the torn COMMIT)", st.Size(), commitStart)
	}
	// New writes go after the cut and survive the next restart.
	must(t, db.Put([]byte("c"), []byte("new")))
}

// TestTornDataPageIsRebuilt: after a checkpoint the log no longer holds a
// page's history, so a page torn while being written is rebuilt from the
// full page image its first change after the checkpoint logged.
func TestTornDataPageIsRebuilt(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, fast)
	model := map[string]string{}
	randomOps(t, rand.New(rand.NewSource(5)), db, model, 1500)
	must(t, db.Close()) // checkpoint: the log is truncated

	db = mustOpen(t, dir, fast)
	k := sortedFirstKey(model)
	must(t, db.Put([]byte(k), []byte("changed after the checkpoint")))
	model[k] = "changed after the checkpoint"
	leaf := leafOf(t, db, []byte(k))
	must(t, db.pool.FlushAll()) // the leaf is written...
	db.crash()                  // ...and the process dies

	// The write of the leaf was torn: second half garbage.
	f, err := os.OpenFile(filepath.Join(dir, DataFileName), os.O_RDWR, 0)
	must(t, err)
	_, err = f.WriteAt(bytes.Repeat([]byte{0xAB}, page.PageSize/2), int64(leaf)*page.PageSize+page.PageSize/2)
	must(t, err)
	f.Close()

	db = mustOpen(t, dir, fast)
	defer db.Close()
	if st := db.RecoveryStats(); st.TornPages != 1 {
		t.Fatalf("TornPages = %d, want 1", st.TornPages)
	}
	checkContents(t, db, model, "after torn leaf")
}

func sortedFirstKey(m map[string]string) string {
	first := ""
	for k := range m {
		if first == "" || k < first {
			first = k
		}
	}
	return first
}

// leafOf returns the ID of the leaf holding key.
func leafOf(t *testing.T, db *DB, key []byte) page.PageID {
	t.Helper()
	txns, err := db.log.Transactions()
	must(t, err)
	last := txns[len(txns)-1] // the Put of key: one PAGE record for its leaf
	if len(last.Records) != 1 || !last.Records[0].Init {
		t.Fatalf("last transaction %+v: want one full-page-image PAGE record", last.Records)
	}
	return last.Records[0].PageID
}

func TestTornMetaPageIsRestored(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, fast)
	model := map[string]string{}
	randomOps(t, rand.New(rand.NewSource(6)), db, model, 1500)
	must(t, db.Close())

	f, err := os.OpenFile(filepath.Join(dir, DataFileName), os.O_RDWR, 0)
	must(t, err)
	_, err = f.WriteAt([]byte{0xFF, 0xFF}, 40) // inside the meta fields
	must(t, err)
	f.Close()

	db = mustOpen(t, dir, fast)
	defer db.Close()
	if !db.RecoveryStats().MetaRestored {
		t.Fatal("meta page not restored from the log")
	}
	checkContents(t, db, model, "after torn meta page")
}

func TestUnrecoverableIsReported(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, fast)
	must(t, db.Put([]byte("k"), []byte("v")))
	must(t, db.Close())

	// Damage the meta page AND lose the log: nothing to rebuild it from.
	f, err := os.OpenFile(filepath.Join(dir, DataFileName), os.O_RDWR, 0)
	must(t, err)
	f.WriteAt([]byte{0xFF}, 40)
	f.Close()
	must(t, os.Remove(filepath.Join(dir, LogFileName)))

	if _, err := Open(dir, fast); !errors.Is(err, recovery.ErrUnrecoverable) {
		t.Fatalf("Open = %v, want ErrUnrecoverable", err)
	}
}

func TestStats(t *testing.T) {
	db := mustOpen(t, t.TempDir(), fast)
	defer db.Close()
	for i := 0; i < 1000; i++ {
		must(t, db.Put(key(i), val(i)))
	}
	for i := 0; i < 1000; i += 2 {
		_, err := db.Delete(key(i))
		must(t, err)
	}
	st, err := db.Stats()
	must(t, err)
	if st.Tree.Keys != 500 || st.Tree.Height < 2 || st.LogBytes == 0 || st.Pool.Hits == 0 {
		t.Fatalf("stats = %+v", st)
	}
	if used := st.Tree.Leaves + st.Tree.Internals + st.Tree.FreePages; used != st.AllocatedPages {
		t.Fatalf("leaves+internals+free = %d, allocated %d", used, st.AllocatedPages)
	}
}
