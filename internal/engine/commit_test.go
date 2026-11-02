package engine

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

	"baseDB/internal/btree"
)

// concurrentPuts runs workers goroutines that each put n distinct keys and
// returns the keys that were acknowledged (Put returned nil).
func concurrentPuts(t *testing.T, db *DB, workers, n int) [][]byte {
	t.Helper()
	var mu sync.Mutex
	var acked [][]byte
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				k := []byte(fmt.Sprintf("w%02d-k%04d", w, i))
				if err := db.Put(k, k); err != nil {
					return // the database was closed or crashed under us
				}
				mu.Lock()
				acked = append(acked, k)
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	return acked
}

// TestGroupCommitSharesFsyncs: with real fsync and many concurrent writers,
// commits share fsyncs (fewer batches than commits). With group commit off,
// every commit has its own.
func TestGroupCommitSharesFsyncs(t *testing.T) {
	for _, off := range []bool{false, true} {
		db := mustOpen(t, t.TempDir(), Options{NoGroupCommit: off}) // real fsync
		acked := concurrentPuts(t, db, 16, 30)
		st, err := db.Stats()
		must(t, err)
		if len(acked) != 16*30 || st.Commits.Commits != 16*30 {
			t.Fatalf("group commit off=%v: %d acked, %d commits", off, len(acked), st.Commits.Commits)
		}
		perFsync := float64(st.Commits.Commits) / float64(st.Commits.Batches)
		t.Logf("group commit off=%v: %d commits in %d fsyncs (%.1f per fsync)", off, st.Commits.Commits, st.Commits.Batches, perFsync)
		if off && st.Commits.Batches != st.Commits.Commits {
			t.Fatalf("without group commit every commit needs its own fsync")
		}
		if !off && perFsync < 2 {
			t.Fatalf("16 concurrent writers shared fsyncs only %.1f to 1", perFsync)
		}
		for _, k := range acked {
			if v := getStr(t, db, string(k)); v != string(k) {
				t.Fatalf("%s = %q", k, v)
			}
		}
		must(t, db.Verify())
		must(t, db.Close())
	}
}

// TestGroupCommitAfterCrash: every acknowledged write of concurrent
// committers survives a crash, whatever batch it was part of.
func TestGroupCommitAfterCrash(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, fast)
	acked := concurrentPuts(t, db, 16, 100)
	db.crash()

	db = mustOpen(t, dir, fast)
	defer db.Close()
	got := contents(t, db)
	for _, k := range acked {
		if got[string(k)] != string(k) {
			t.Fatalf("acknowledged %s lost in the crash", k)
		}
	}
	if len(got) != len(acked) {
		t.Fatalf("%d keys after crash, %d acknowledged", len(got), len(acked))
	}
}

// TestBatchTooBigFallsBack: two transactions that each fit in the buffer
// pool but not together are applied one by one, and both succeed.
func TestBatchTooBigFallsBack(t *testing.T) {
	db := mustOpen(t, t.TempDir(), Options{NoSync: true, PoolFrames: 12})
	defer db.Close()
	big := bytes.Repeat([]byte{'v'}, 700) // 5 per leaf: many leaves
	for i := 0; i < 400; i++ {
		must(t, db.Put(key(i), big))
	}
	before, _ := db.Stats()

	// Each request touches 6 leaves far apart (plus the root): 7 pages fit
	// alone in 12 frames; both together need 13 and do not.
	mk := func(first int) *commitRequest {
		var ops []btree.Op
		for i := 0; i < 6; i++ {
			ops = append(ops, btree.Op{Key: key(first + i*60), Value: []byte("updated")})
		}
		return &commitRequest{ops: ops, done: make(chan error, 1)}
	}
	a, b := mk(0), mk(30)
	db.applyBatch([]*commitRequest{a, b})
	must(t, <-a.done)
	must(t, <-b.done)
	for _, r := range []*commitRequest{a, b} {
		for _, op := range r.ops {
			if getStr(t, db, string(op.Key)) != "updated" {
				t.Fatalf("%s not updated", op.Key)
			}
		}
	}
	after, _ := db.Stats()
	if got := after.Commits.Batches - before.Commits.Batches; got != 2 {
		t.Fatalf("fallback used %d fsyncs, want 2 (one per transaction)", got)
	}
	must(t, db.Verify())
}
