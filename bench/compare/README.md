# baseDB compared with other storage engines

This program runs the same workloads against baseDB and five well-known embedded
storage engines, and prints the results side by side.

| Store | What it is | Why it is in the comparison |
|---|---|---|
| **baseDB** | B+ tree, own buffer pool (ARC), write-ahead log with redo recovery, 2PL, group commit (Go) | this project |
| **Pebble** (CockroachDB) | LSM tree: memtable + WAL + sorted files, background compaction (Go) | an LSM tree in the same language |
| **RocksDB** (Meta) | LSM tree, the C++ engine Pebble was modelled on | the reference LSM implementation |
| **SQLite** | B-tree (a `WITHOUT ROWID` table: the rows live in the key's B-tree), WAL journal (C) | the most widely used embedded database |
| **WiredTiger** (MongoDB) | B-tree with a write-ahead log, MVCC, row-level concurrency (C) | a production B-tree with a log, like baseDB |
| **redb** | copy-on-write B-tree, no log, one writer at a time (Rust) | a modern B-tree that is crash-safe without a log |

`basedb-nogc` (baseDB with group commit off) can be added with `-dbs`. The first two
result sets (`compare1`, `compare2`) also contain **bbolt**, which was replaced by these
engines from `compare3` on.

## Setup (Windows, once)

baseDB and Pebble are Go. The other four are C, C++ or Rust and need:

1. **MSYS2** with gcc, and `C:\msys64\ucrt64\bin` on `PATH` (as for the race detector):
   ```
   pacman -S mingw-w64-ucrt-x86_64-gcc mingw-w64-ucrt-x86_64-rocksdb mingw-w64-ucrt-x86_64-sqlite3 ^
             mingw-w64-ucrt-x86_64-cmake mingw-w64-ucrt-x86_64-ninja mingw-w64-ucrt-x86_64-python
   ```
   RocksDB and SQLite come from these packages.
2. **WiredTiger** has no package for MinGW and officially builds on Windows only with
   Visual Studio. `deps/build-wiredtiger.ps1` downloads 11.3.1, patches it for gcc
   (`deps/patch_wiredtiger.py`: MSVC switches and intrinsics only, WiredTiger's Windows
   I/O code is kept) and installs a static library into `deps/install/wiredtiger`:
   ```
   powershell -ExecutionPolicy Bypass -File deps\build-wiredtiger.ps1
   ```
3. **redb** is a Rust library without a C interface. `deps/redb` is a small Rust crate that
   gives it one; `deps/build-redb.ps1` builds it into `deps/install/redb/baseredb.dll`.
   It needs Rust with the GNU target (`rustup-init.exe --default-host x86_64-pc-windows-gnu`):
   ```
   powershell -ExecutionPolicy Bypass -File deps\build-redb.ps1
   ```

Run from **PowerShell or cmd** (cgo does not work from Git Bash on this machine), with
`CGO_ENABLED=1`. Without cgo, only baseDB, Pebble and redb are available.

## Run it

```
cd bench/compare
go run .                                    # everything, 3 runs, about 20 minutes
go run . -runs 1 -duration 1s               # a quick look
go run . -dbs basedb,rocksdb -workloads writers -writers 1,8,64
go run . -sync=false                        # no fsync anywhere: the code paths only
go run . -h                                 # all flags
```

**Every run is saved** as `bench/compareN.txt` (the tables, plus the date, machine, command
line and the exact version of every store) and `bench/compareN.csv` (every single run), with
N the next free number. `-results=` (empty) turns saving off, for experiments.

This is a separate Go module (its own `go.mod`), so baseDB itself still has no dependencies,
and `go test ./...` in the project root does not build it.

## Workloads

| Name | What it does | What it shows |
|---|---|---|
| `bulk-seq` | insert all keys in increasing order, 1,000 per transaction | write path without the fsync limit; page fill |
| `bulk-rand` | the same in random order | cost of random inserts (B-tree vs LSM) |
| `read` | 100% random point reads (YCSB C) | lookup path, cache |
| `read-mostly` | 95% reads, 5% updates (YCSB B) | |
| `mixed` | 50% reads, 50% updates (YCSB A) | |
| `update` | 100% updates, one key per transaction | cost of one durable commit |
| `scan` | read 10 keys from a random start (YCSB E) | range scans |
| `writers` | 1, 8, 64 goroutines inserting new keys, one per transaction | group commit |
| `space` | directory size after loading, after deleting the lower half of the keys, after inserting as many new keys | space use and reuse |

Keys are `user` + 12 digits (16 bytes), values 100 random bytes, different for every key
(so that compression gains nothing). The read and update workloads run on 100,000 loaded keys,
after one full scan so every store starts with a warm cache, with one thread: they compare
the cost of one operation. Concurrency is measured only by `writers`.

## Fairness rules

1. **Same durability.** With `-sync=true` (the default) every store fsyncs every commit:
   baseDB and redb by default, Pebble with `pebble.Sync`, RocksDB with `sync=true`, SQLite
   with `synchronous=FULL` in WAL mode, WiredTiger with `commit_transaction("sync=on")`.
   Comparing a store that syncs with one that does not would compare a disk with memory.
2. **Same cache.** Every store gets `-cache` (64 MiB, larger than the data): baseDB's
   buffer pool, Pebble's and RocksDB's block cache, SQLite's `cache_size`, WiredTiger's
   `cache_size`, redb's cache. Everything else is each engine's default.
3. **Same work.** Same keys, values and operation order (fixed `-seed`), one key per
   transaction outside the bulk workloads, a new directory for every run.
4. **One foreign call per operation.** The C, C++ and Rust stores are called through cgo or
   a DLL, which costs a little per call. Each operation (a get with its copy, a whole scan,
   a whole batch) is one call, done in C or Rust (`sqlite.go`, `rocksdb.go`,
   `wiredtiger.go`, `deps/redb`), so the call cost is paid once, not once per key. SQLite is
   used through its C API, not `database/sql`, whose overhead is as large as a cached read.
5. **Updates really change the value.** baseDB logs only the bytes that changed; rewriting a
   key with the same value logs nothing and skips the fsync. Every update writes a new
   value so that baseDB pays like the others.
6. **Same space measure.** `on disk` is the data after a clean Close: the directory without
   write-ahead log files. After Close every store has checkpointed or flushed, so its logs
   hold nothing still needed, but two keep log files as reserve: WiredTiger creates each
   log at full size (100 MiB) and prepares the next (200 MiB of zeros), and Pebble keeps old
   logs to recycle (about 10 MiB). So no store's logs are counted.
7. **Different values.** Every key has its own random value (a window of a 4 MiB random
   pool), so compression gains nothing. `compare1`–`compare3` had one value for all keys,
   which let the compressing stores (Pebble, RocksDB) shrink their files.
8. **Repeated.** `-runs` (3) repeats everything; the tables show the median.
9. **Latency.** `avg` is exact (time / operations). `p50`/`p99` are timed per operation,
   and the clock on Windows is coarse: `≈0` means below its resolution.

Compare stores **within one result file**. Between files (other days), the machine itself
varies by 10–40% on CPU-bound rows; to measure a code change, use an interleaved A/B test
(like `bench/checkpoint_ab.txt`).

## Results

| File | Stores | Notes |
|---|---|---|
| [compare1](../compare1.txt) ([csv](../compare1.csv)) | baseDB, bbolt, bbolt `Batch`, Pebble | before the checkpoint storm fix |
| [compare2](../compare2.txt) ([csv](../compare2.csv)) | baseDB, bbolt, bbolt `Batch`, Pebble | after the fix |
| [compare3](../compare3.txt) ([csv](../compare3.csv)) | baseDB, Pebble, SQLite, RocksDB, redb, WiredTiger | **flawed, kept for the record**: one value for all keys (RocksDB compressed 100,000 keys to 1.6 MiB), and the space measure counted Pebble's recycled logs |
| [compare4](../compare4.txt) ([csv](../compare4.csv)) | baseDB, Pebble, SQLite, RocksDB, redb, WiredTiger | six engines, a value per key, logs not counted in `on disk` |

The A/B test of the checkpoint fix is in [../checkpoint_ab.txt](../checkpoint_ab.txt). The
discussion of every result is in [docs/ARCHITECTURE.md](../../docs/ARCHITECTURE.md), Week 9.
