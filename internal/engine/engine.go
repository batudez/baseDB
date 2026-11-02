// Package engine is the storage engine as one unit: a directory holding a
// data file and a write-ahead log, opened with crash recovery, offering
// Get/Put/Delete/Scan, checkpointed so the log stays small, and closed in
// the right order.
//
//	db, err := engine.Open(dir, engine.Options{})
//	err = db.Put(key, value)          // durable when it returns
//	v, ok, err := db.Get(key)
//	found, err := db.Delete(key)
//	it := db.Scan(start, end)
//	err = db.Close()
//
// Transactions (Begin, tx.go) use strict two-phase locking.
//
// See docs/ARCHITECTURE.md (weeks 4, 5 and 7).
package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"baseDB/internal/btree"
	"baseDB/internal/buffer"
	"baseDB/internal/lockfile"
	"baseDB/internal/lockmgr"
	"baseDB/internal/pager"
	"baseDB/internal/recovery"
	"baseDB/internal/wal"
)

const (
	DataFileName = "data.db"
	LogFileName  = "wal.log"
	LockFileName = "LOCK"

	DefaultPoolFrames      = 256     // 1 MiB of cached pages
	DefaultCheckpointBytes = 4 << 20 // checkpoint when the log reaches 4 MiB
)

var (
	// ErrClosed is returned for operations on a closed DB.
	ErrClosed = errors.New("engine: database is closed")

	// ErrLocked is returned by Open when another process (or another open
	// DB in this process) is using the directory.
	ErrLocked = lockfile.ErrLocked
)

// Options configures a DB.
type Options struct {
	// PoolFrames is the buffer pool size in pages (default 256).
	PoolFrames int
	// CheckpointBytes: after a write, if the log is at least this large, a
	// checkpoint runs and the log is truncated (default 4 MiB). It bounds
	// the log's disk use and the time and memory recovery needs.
	CheckpointBytes int64
	// NoSync skips fsync on commit. Faster, but a power failure can lose
	// recent commits. For tests only.
	NoSync bool
	// NoGroupCommit gives every commit its own fsync instead of sharing one
	// between concurrent commits (commit.go). Only for measuring what group
	// commit gains.
	NoGroupCommit bool
}

// DB is an open database. It is safe for concurrent use; Close must not run
// concurrently with other calls.
type DB struct {
	// mu: operations hold it shared; Checkpoint and Close hold it
	// exclusively, so no write is in progress while they run.
	mu     sync.RWMutex
	closed bool
	opts   Options

	lock     *lockfile.Lock
	pg       *pager.Pager
	log      *wal.Log
	pool     *buffer.Pool
	tree     *btree.Tree
	recovery recovery.Stats

	locks   *lockmgr.Manager // key and database locks of transactions (tx.go)
	nextTxn atomic.Uint64
	gc      committer // group commit (commit.go)

	checkpointing atomic.Bool   // an automatic checkpoint is waiting or running (maybeCheckpoint)
	checkpoints   atomic.Uint64 // checkpoints run since Open (tests)
}

// Open opens (or creates) the database in dir and recovers it:
//
//  0. lock the directory, so no other process can use it at the same time
//  1. open the log; a torn or corrupt tail left by a crash is cut off
//  2. open the data file; if its meta page is torn, open it anyway and let
//     recovery restore the meta page from the log
//  3. recovery: replay committed transactions onto the data file, sync
//  4. only now build the buffer pool and the tree on the recovered state
//  5. if the log is already over the checkpoint size, checkpoint now
func Open(dir string, opts Options) (db *DB, err error) {
	if opts.PoolFrames == 0 {
		opts.PoolFrames = DefaultPoolFrames
	}
	if opts.CheckpointBytes == 0 {
		opts.CheckpointBytes = DefaultCheckpointBytes
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("engine: %w", err)
	}

	lock, err := lockfile.Acquire(filepath.Join(dir, LockFileName))
	if err != nil {
		return nil, err
	}
	defer func() {
		if err != nil {
			lock.Release()
		}
	}()

	log, err := wal.OpenWithOptions(filepath.Join(dir, LogFileName), wal.Options{NoSync: opts.NoSync})
	if err != nil {
		return nil, err
	}
	dataPath := filepath.Join(dir, DataFileName)
	metaDamaged := false
	pg, err := pager.Open(dataPath)
	if errors.Is(err, pager.ErrChecksum) {
		// The meta page was torn by a crash during pager.Sync. Its contents
		// are in the log's last META record.
		metaDamaged = true
		pg, err = pager.OpenIgnoringMeta(dataPath)
	}
	if err != nil {
		log.Close()
		return nil, err
	}

	stats, err := recovery.Run(pg, log, metaDamaged)
	if err != nil {
		pg.Abandon() // don't write a meta page we could not recover
		log.Close()
		return nil, err
	}

	pool := buffer.New(pg, opts.PoolFrames, log)
	db = &DB{
		opts:     opts,
		lock:     lock,
		pg:       pg,
		log:      log,
		pool:     pool,
		tree:     btree.Open(pool, pg, log),
		recovery: stats,
		locks:    lockmgr.New(),
	}
	if log.Size() >= opts.CheckpointBytes {
		if err := db.checkpointLocked(); err != nil {
			db.closeFiles()
			return nil, err
		}
	}
	return db, nil
}

// RecoveryStats reports what recovery did when the DB was opened.
func (db *DB) RecoveryStats() recovery.Stats { return db.recovery }

func (db *DB) begin() error {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrClosed
	}
	return nil
}

func (db *DB) end() { db.mu.RUnlock() }

// Get returns the committed value stored under key and whether it exists.
// It takes no locks (read committed): it never sees uncommitted writes, but
// two Gets may see different values if a transaction commits in between.
// Use a transaction for repeatable reads.
func (db *DB) Get(key []byte) ([]byte, bool, error) {
	if err := db.begin(); err != nil {
		return nil, false, err
	}
	defer db.end()
	return db.tree.Get(key)
}

// Put stores value under key (insert or update), as a transaction of its
// own: it waits for transactions that lock the key. When it returns nil, the
// change is durable: it survives a crash.
func (db *DB) Put(key, value []byte) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	if err := tx.Put(key, value); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

// Delete removes key and reports whether it existed, as a transaction of
// its own. Durable on return.
func (db *DB) Delete(key []byte) (bool, error) {
	tx, err := db.Begin()
	if err != nil {
		return false, err
	}
	found, err := tx.Delete(key)
	if err != nil {
		tx.Rollback()
		return false, err
	}
	return found, tx.Commit()
}

// Scan iterates over committed keys in [start, end) (nil = unbounded)
// without taking locks (read committed, like Get): each batch of keys is
// consistent, but a transaction that commits during a long scan can be
// seen in part. Use Tx.Scan for a consistent, phantom-free scan. The
// iterator must not be used after Close.
func (db *DB) Scan(start, end []byte) Iterator {
	return db.tree.Scan(start, end)
}

// Verify checks the structure of the B+ tree and the free list (see
// btree.Tree.Verify).
func (db *DB) Verify() error {
	if err := db.begin(); err != nil {
		return err
	}
	defer db.end()
	return db.tree.Verify()
}

// Stats is a snapshot of the database's state, for admin commands.
type Stats struct {
	Tree           btree.Stats
	AllocatedPages int   // data pages ever allocated (in the tree or free)
	LogBytes       int64 // current size of the write-ahead log
	Options        Options
	Pool           buffer.Stats   // since Open
	Recovery       recovery.Stats // what recovery did at Open
	Locks          lockmgr.Stats  // since Open
	Commits        CommitStats    // since Open: commits and the fsyncs they shared
}

// Stats walks the tree (see btree.Tree.Stats) and gathers the counters of
// the other layers. An error means the tree failed validation.
func (db *DB) Stats() (Stats, error) {
	if err := db.begin(); err != nil {
		return Stats{}, err
	}
	defer db.end()
	tree, err := db.tree.Stats()
	return Stats{
		Tree:           tree,
		AllocatedPages: int(db.pg.Meta().NextPageID) - 1,
		LogBytes:       db.log.Size(),
		Options:        db.opts,
		Pool:           db.pool.Stats(),
		Recovery:       db.recovery,
		Locks:          db.locks.Stats(),
		Commits:        db.gc.stats(),
	}, err
}

// maybeCheckpoint runs an automatic checkpoint once the log has reached
// CheckpointBytes. With many writers, every commit that ends past the limit
// asks for one; if each ran, they would queue on db.mu and truncate an
// already empty log again and again (a checkpoint storm). So:
//   - only one automatic checkpoint waits or runs at a time; the others
//     return at once, that one truncates the log for them
//   - the size is checked again under the exclusive lock, where no write
//     can change it: a checkpoint that ran while this one waited (another
//     automatic one, or a manual Checkpoint) has already done the work
func (db *DB) maybeCheckpoint() error {
	if db.log.Size() < db.opts.CheckpointBytes {
		return nil
	}
	if !db.checkpointing.CompareAndSwap(false, true) {
		return nil
	}
	defer db.checkpointing.Store(false)

	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed || db.log.Size() < db.opts.CheckpointBytes {
		return nil // Close checkpoints anyway; or someone else already did
	}
	if err := db.checkpointLocked(); err != nil {
		return fmt.Errorf("engine: the write is durable, but the checkpoint after it failed: %w", err)
	}
	return nil
}

// Checkpoint makes the data file hold everything and truncates the log:
//
//  1. wait until no operation is running (exclusive lock)
//  2. write every dirty page (each after its log records: the WAL rule),
//     then write the meta page and fsync the data file
//  3. replace the log with an empty one that continues at the same LSN and
//     holds only a META snapshot (wal.Log.Reset, an atomic file replace)
//
// A crash before step 3 completes leaves the old log, which recovery
// replays harmlessly onto pages that are already up to date. After step 3
// the next change to every page is logged as a full page image, so torn
// pages stay repairable although their older history is gone.
func (db *DB) Checkpoint() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	return db.checkpointLocked()
}

func (db *DB) checkpointLocked() error {
	db.checkpoints.Add(1)
	if err := db.pool.FlushAll(); err != nil {
		return err
	}
	return db.log.Reset(db.pg.Meta())
}

// Close shuts the database down cleanly:
//
//  1. checkpoint: all pages and the meta page written and synced, log
//     truncated, so the next Open has (almost) nothing to read or redo
//  2. close the log and the data file
//  3. release the directory lock
//
// Transactions still open are rolled back: requests waiting for a lock fail
// with ErrClosed right away (they must not keep Close waiting), and later
// operations on them fail with ErrClosed.
func (db *DB) Close() error {
	db.locks.Close() // first: a transaction waiting for a lock holds no db.mu
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	db.closed = true
	err := db.checkpointLocked()
	return errors.Join(err, db.closeFiles())
}

func (db *DB) closeFiles() error {
	return errors.Join(db.log.Close(), db.pg.Close(), db.lock.Release())
}

// crash closes the files without writing anything, as if the process died
// at this moment: dirty pages in the buffer pool are lost, committed changes
// are in the log, and the OS releases the directory lock. For crash tests
// only.
func (db *DB) crash() {
	db.locks.Close()
	db.mu.Lock()
	defer db.mu.Unlock()
	db.closed = true
	db.log.Abandon()
	db.pg.Abandon()
	db.lock.Release()
}
