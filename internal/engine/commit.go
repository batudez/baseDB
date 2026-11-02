package engine

import (
	"bytes"
	"errors"
	"slices"
	"sync"

	"baseDB/internal/btree"
	"baseDB/internal/buffer"
)

// Group commit.
//
// A commit costs one fsync of the log, about a millisecond. With one
// committer at a time that caps the database at about 1,000 commits per
// second, however many clients there are. Group commit shares the fsync:
//
//	committer A ──┐                ┌── A, B, C all durable after ONE fsync
//	committer B ──┼─► queue ─► leader: apply A+B+C as one mini-transaction
//	committer C ──┘                └── (while it runs, D, E queue up for the next batch)
//
// The first committer to find no leader becomes the leader. It takes
// everything queued, applies it as one btree.Apply (one WAL transaction,
// one fsync), wakes the waiting committers, and repeats until the queue is
// empty. While a batch is being written and synced, new commits pile up in
// the queue, so the batch size grows with the load by itself: no timer, no
// tuning.
//
// Why merging transactions is safe:
//   - Strict 2PL: every committer still holds exclusive locks on all keys it
//     writes, so the transactions of one batch write disjoint keys. Their
//     order inside the batch does not matter.
//   - A batch is one WAL transaction, so after a crash it is all there or
//     not there at all. That is stronger than needed: no committer of the
//     batch had been told "committed" yet.
//   - Each committer returns only after the batch's fsync, and releases its
//     locks only after that (tx.Commit), so nothing becomes visible before it
//     is durable.
//
// If a batch does not fit in the buffer pool at once (every touched page
// stays pinned until the commit), it is retried one transaction at a time,
// so a batch can never make a transaction fail that would succeed alone.

// maxBatchOps bounds the writes merged into one batch, so that a batch's
// pages stay well inside the buffer pool (a leaf per write in the worst
// case, plus the path above it).
func (db *DB) maxBatchOps() int { return max(1, db.opts.PoolFrames/4) }

type commitRequest struct {
	ops  []btree.Op
	done chan error
}

type committer struct {
	mu      sync.Mutex
	queue   []*commitRequest
	leading bool // a leader is applying batches

	// Counters, protected by mu.
	commits uint64 // transactions committed (with writes)
	batches uint64 // batches applied (= log fsyncs for commits)
}

// CommitStats counts group commit activity.
type CommitStats struct {
	Commits uint64 // transactions with writes that committed
	Batches uint64 // batches they were applied in, i.e. commit fsyncs
}

func (c *committer) stats() CommitStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CommitStats{Commits: c.commits, Batches: c.batches}
}

// commitOps makes ops durable, through group commit unless it is disabled.
func (db *DB) commitOps(ops []btree.Op) error {
	if db.opts.NoGroupCommit {
		err := db.applyOps(ops)
		db.gc.mu.Lock()
		if err == nil {
			db.gc.commits++
			db.gc.batches++
		}
		db.gc.mu.Unlock()
		return err
	}

	req := &commitRequest{ops: ops, done: make(chan error, 1)}
	db.gc.mu.Lock()
	db.gc.queue = append(db.gc.queue, req)
	if db.gc.leading {
		db.gc.mu.Unlock()
		return <-req.done // the leader will apply it
	}
	db.gc.leading = true
	db.gc.mu.Unlock()

	// Leader: apply batches until nothing is queued.
	for {
		db.gc.mu.Lock()
		batch := db.takeBatch()
		if len(batch) == 0 {
			db.gc.leading = false
			db.gc.mu.Unlock()
			break
		}
		db.gc.mu.Unlock()
		db.applyBatch(batch)
	}
	return <-req.done
}

// takeBatch removes up to maxBatchOps writes worth of requests from the
// queue (at least one request). Called with gc.mu held.
func (db *DB) takeBatch() []*commitRequest {
	n, ops := 0, 0
	for n < len(db.gc.queue) && (n == 0 || ops+len(db.gc.queue[n].ops) <= db.maxBatchOps()) {
		ops += len(db.gc.queue[n].ops)
		n++
	}
	batch := db.gc.queue[:n:n]
	db.gc.queue = db.gc.queue[n:]
	return batch
}

func (db *DB) applyBatch(batch []*commitRequest) {
	var ops []btree.Op
	for _, r := range batch {
		ops = append(ops, r.ops...)
	}
	slices.SortFunc(ops, func(a, b btree.Op) int { return bytes.Compare(a.Key, b.Key) })

	err := db.applyOps(ops)
	if err != nil && len(batch) > 1 && errors.Is(err, buffer.ErrNoFreeFrame) {
		// Too big together: give every transaction its own chance.
		for _, r := range batch {
			e := db.applyOps(r.ops)
			db.finishCommit(r, e, 1)
		}
		return
	}
	for i, r := range batch {
		batches := uint64(0)
		if i == 0 {
			batches = 1
		}
		db.finishCommit(r, err, batches)
	}
}

func (db *DB) finishCommit(r *commitRequest, err error, batches uint64) {
	db.gc.mu.Lock()
	if err == nil {
		db.gc.commits++
		db.gc.batches += batches
	}
	db.gc.mu.Unlock()
	r.done <- err
}

// applyOps applies ops as one mini-transaction (one WAL transaction, one
// fsync) under the database's shared lock (so Checkpoint/Close wait for it).
func (db *DB) applyOps(ops []btree.Op) error {
	if err := db.begin(); err != nil {
		return err
	}
	defer db.end()
	return db.tree.Apply(ops)
}
