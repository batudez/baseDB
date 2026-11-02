package engine

import (
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"baseDB/internal/page"
)

func TestSecondOpenIsLocked(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, fast)
	if _, err := Open(dir, fast); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Open = %v, want ErrLocked", err)
	}
	must(t, db.Close())
	db = mustOpen(t, dir, fast) // free again after Close
	db.crash()
	db = mustOpen(t, dir, fast) // and after a crash (the OS drops the lock)
	must(t, db.Close())
}

// TestCheckpointBoundsTheLog: with a small checkpoint size, the log never
// grows much beyond it, however many writes happen.
func TestCheckpointBoundsTheLog(t *testing.T) {
	dir := t.TempDir()
	opts := fast
	opts.CheckpointBytes = 64 << 10
	db := mustOpen(t, dir, opts)
	model := map[string]string{}
	rng := rand.New(rand.NewSource(13))
	maxLog := int64(0)
	for i := 0; i < 20; i++ {
		randomOps(t, rng, db, model, 300)
		maxLog = max(maxLog, db.log.Size())
	}
	// One transaction can add at most a few full pages past the limit.
	if limit := opts.CheckpointBytes + 8*page.PageSize; maxLog > limit {
		t.Fatalf("log reached %d bytes, limit %d", maxLog, limit)
	}
	db.crash()

	db = mustOpen(t, dir, opts)
	defer db.Close()
	checkContents(t, db, model, "after crash with checkpoints")
	if st := db.RecoveryStats(); st.Transactions > 1000 {
		t.Fatalf("recovery read %d transactions: the log was not truncated", st.Transactions)
	}
}

// TestNoCheckpointStorm: many commits that all find the log over the limit
// at once run one checkpoint, not one each.
func TestNoCheckpointStorm(t *testing.T) {
	opts := fast
	opts.CheckpointBytes = 1 << 30 // no automatic checkpoint while filling
	db := mustOpen(t, t.TempDir(), opts)
	defer db.Close()
	for i := 0; db.log.Size() < 32<<10; i++ {
		must(t, db.Put([]byte(fmt.Sprintf("k%05d", i)), make([]byte, 100)))
	}
	db.opts.CheckpointBytes = 32 << 10 // now over the limit (no goroutines yet: no race)
	before := db.checkpoints.Load()

	var start, done sync.WaitGroup
	start.Add(1)
	for g := 0; g < 64; g++ {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			if err := db.maybeCheckpoint(); err != nil {
				t.Error(err)
			}
		}()
	}
	start.Done()
	done.Wait()
	if n := db.checkpoints.Load() - before; n != 1 {
		t.Fatalf("64 commits over the limit ran %d checkpoints, want 1", n)
	}
	if db.log.Size() >= db.opts.CheckpointBytes {
		t.Fatalf("log still %d bytes after the checkpoint", db.log.Size())
	}
}

// TestCheckpointsUnderConcurrentWriters: with many writers, every
// automatic checkpoint is earned: there are at most as many as the log
// bytes written divided by CheckpointBytes. And the data is all there.
func TestCheckpointsUnderConcurrentWriters(t *testing.T) {
	opts := fast
	opts.CheckpointBytes = 16 << 10
	dir := t.TempDir()
	db := mustOpen(t, dir, opts)
	firstLSN := db.log.DurableLSN()
	before := db.checkpoints.Load()

	const writers, puts = 32, 100
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < puts; i++ {
				if err := db.Put([]byte(fmt.Sprintf("w%02d-%03d", w, i)), make([]byte, 100)); err != nil {
					t.Error(err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	logged := int64(db.log.DurableLSN() - firstLSN)
	n := int64(db.checkpoints.Load() - before)
	if n == 0 {
		t.Fatalf("%d bytes logged, no checkpoint: the test does not reach the limit", logged)
	}
	if limit := logged / opts.CheckpointBytes; n > limit {
		t.Fatalf("%d checkpoints for %d bytes of log, at most %d expected", n, logged, limit)
	}
	t.Logf("%d bytes logged by %d commits, %d checkpoints", logged, writers*puts, n)

	db.crash()
	db = mustOpen(t, dir, opts)
	defer db.Close()
	for w := 0; w < writers; w++ {
		for i := 0; i < puts; i++ {
			if _, ok, err := db.Get([]byte(fmt.Sprintf("w%02d-%03d", w, i))); err != nil || !ok {
				t.Fatalf("w%02d-%03d after crash: found=%v err=%v", w, i, ok, err)
			}
		}
	}
}

// TestCleanCloseLeavesAlmostNothing: Close checkpoints, so the next Open
// reads one META transaction and redoes nothing.
func TestCleanCloseLeavesAlmostNothing(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, fast)
	model := map[string]string{}
	randomOps(t, rand.New(rand.NewSource(14)), db, model, 2000)
	must(t, db.Close())

	st, err := os.Stat(filepath.Join(dir, LogFileName))
	must(t, err)
	if st.Size() > 1024 {
		t.Fatalf("log is %d bytes after a clean Close", st.Size())
	}
	db = mustOpen(t, dir, fast)
	defer db.Close()
	if rs := db.RecoveryStats(); rs.Transactions != 1 || rs.RecordsApplied != 0 {
		t.Fatalf("recovery after clean Close: %+v", rs)
	}
	checkContents(t, db, model, "after clean Close")
}

// TestCrashDuringCheckpoint: the crash hits after the pages were written
// but before the log was replaced (with a half-written replacement file
// left behind). Recovery replays the old log onto pages that are already
// up to date, which changes nothing.
func TestCrashDuringCheckpoint(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, fast)
	model := map[string]string{}
	randomOps(t, rand.New(rand.NewSource(15)), db, model, 1500)
	must(t, db.pool.FlushAll()) // checkpoint step 2 done...
	db.crash()                  // ...step 3 never happens
	must(t, os.WriteFile(filepath.Join(dir, LogFileName+".tmp"), []byte("half"), 0o644))

	db = mustOpen(t, dir, fast)
	defer db.Close()
	if rs := db.RecoveryStats(); rs.RecordsApplied != 0 || rs.RecordsSkipped == 0 {
		t.Fatalf("recovery after crash during checkpoint: %+v (want everything skipped)", rs)
	}
	checkContents(t, db, model, "after crash during checkpoint")
}

// TestCrashAfterCheckpoint: a checkpoint, more writes, a crash. Recovery
// starts from the checkpointed data file and the short log after it.
func TestCrashAfterCheckpoint(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, fast)
	model := map[string]string{}
	rng := rand.New(rand.NewSource(16))
	randomOps(t, rng, db, model, 1500)
	must(t, db.Checkpoint())
	randomOps(t, rng, db, model, 500)
	db.crash()

	db = mustOpen(t, dir, fast)
	defer db.Close()
	if rs := db.RecoveryStats(); rs.Transactions > 501 {
		t.Fatalf("recovery read %d transactions, want only those after the checkpoint", rs.Transactions)
	}
	checkContents(t, db, model, "after crash following a checkpoint")
}

// TestFileStopsGrowingWithDeletes: fill and empty the database repeatedly,
// with restarts in between. Freed pages are reused, so the data file does
// not grow after the first round.
func TestFileStopsGrowingWithDeletes(t *testing.T) {
	dir := t.TempDir()
	var firstSize int64
	for round := 0; round < 4; round++ {
		db := mustOpen(t, dir, fast)
		for i := 0; i < 2000; i++ {
			must(t, db.Put(key(i), val(i)))
		}
		for i := 0; i < 2000; i++ {
			_, err := db.Delete(key(i))
			must(t, err)
		}
		must(t, db.Verify())
		must(t, db.Close())
		st, err := os.Stat(filepath.Join(dir, DataFileName))
		must(t, err)
		if round == 0 {
			firstSize = st.Size()
		} else if st.Size() != firstSize {
			t.Fatalf("round %d: data file grew from %d to %d bytes", round, firstSize, st.Size())
		}
	}
}
