# baseDB

A disk-based key-value store written in Go, built from scratch as a final project:
slotted pages, a buffer pool with ARC replacement, a B+ tree, a write-ahead log
with crash recovery, checkpoints, page reuse, transactions with two-phase locking,
and a TCP server with group commit. It runs embedded (in the terminal program or
in a Go program) or as a server for many clients at once.

The design and the reasons behind every decision are in
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Build

Requires Go 1.23 or newer.

```
go build -o basedb ./cmd/basedb          # Windows: go build -o basedb.exe ./cmd/basedb
```

## Run

**Interactive** (a prompt, when started from a terminal):

```
$ basedb -dir ./mydb
baseDB: database in ./mydb. Type help for commands, exit to quit.
basedb> put user:1 "Batuhan"
OK
basedb> put user:2 "Ayşe Yılmaz"
OK
basedb> get user:1
"Batuhan"
basedb> scan user: user;
user:1 → "Batuhan"
user:2 → "Ayşe Yılmaz"
(2 keys)
basedb> delete user:1
OK
basedb> exit
```

**One command** (for scripts; exit code 0 on success, 1 on error):

```
$ basedb -dir ./mydb get user:2
"Ayşe Yılmaz"
```

**A script** (one command per line; `#` starts a comment; the exit code is 1 if
any command failed):

```
$ basedb -dir ./mydb < commands.txt
```

The directory is created if it does not exist.

### As a server

One process owns the database, and many clients use it at the same time:

```
$ basedb -dir ./mydb -serve 127.0.0.1:7070
baseDB server: database in ./mydb, listening on 127.0.0.1:7070 (Ctrl+C to stop)
```

In other terminals, the same prompt and commands, over TCP:

```
$ basedb -connect 127.0.0.1:7070
baseDB: connected to 127.0.0.1:7070. Type help for commands, exit to quit.
basedb@127.0.0.1:7070> put user:1 "Batuhan"
OK
```

```
$ basedb -connect 127.0.0.1:7070 get user:1        # one command
"Batuhan"
```

The protocol is plain text, so `telnet 127.0.0.1 7070` works too: type `put a 1`
and get `OK`, `get a` and get `VALUE "1"`. There is no authentication: keep the
server on `127.0.0.1` unless the network is trusted.

From Go, use the [client](client/client.go) package:

```go
c, err := client.Dial("127.0.0.1:7070")
err = c.Put([]byte("user:1"), []byte("Batuhan"))
v, ok, err := c.Get([]byte("user:1"))
err = c.Begin(); /* ... */ err = c.Commit()   // a transaction on this connection
```

### Flags

| Flag | Default | Meaning |
|---|---|---|
| `-dir` | `basedb-data` | database directory |
| `-pool` | `256` | buffer pool size in 4 KiB pages (256 = 1 MiB) |
| `-prompt` | on for a terminal | show the `basedb>` prompt |
| `-serve` | | run as a server on this address |
| `-connect` | | talk to a server at this address instead of opening `-dir` |
| `-max-conns` | `1000` | server: clients served at once |
| `-idle-timeout` | `10m` | server: disconnect silent clients (their open transaction is rolled back) |
| `-group-commit` | `true` | share one fsync between concurrent commits |

## Commands

| Command | Does |
|---|---|
| `put <key> <value>` | store value under key (insert or update) |
| `get <key>` | show the value, or `(not found)` |
| `delete <key>` | remove key (alias `del`) |
| `scan [start] [end]` | list keys in `[start, end)` in order; no arguments = all |
| `prefix <prefix>` | list keys starting with prefix |
| `count [start] [end]` | count keys in `[start, end)` |
| `begin` | start a transaction: the following commands run in it |
| `commit` | apply the transaction's writes, all together and durably |
| `rollback` | discard the transaction's writes |
| `stats` | tree height, page counts, buffer pool hit ratio, log size, what recovery did |
| `verify` | check the B+ tree and the free list for consistency |
| `checkpoint` | write all pages to the data file and truncate the log |
| `help` | list the commands |
| `exit` / `quit` / Ctrl+C | close the database cleanly and quit |

**Quoting.** Arguments are separated by spaces. Use double quotes for spaces or an
empty value. Inside quotes, these escapes work: `\"` `\\` `\n` `\t` `\r` `\0` `\xHH`
(any byte). Output uses the same notation, so it can be pasted back:

```
basedb> put "full name" "Batuhan \"B\" Deniz"
basedb> put bin "\x00\xff"
basedb> get bin
"\x00\xff"
```

**Limits:** keys up to 256 bytes, values up to 1024 bytes, and up to 1000 keys
written per transaction.

### Transactions

```
basedb> begin
OK: transaction started
basedb(tx: 0 writes)> put acct:1 90
OK
basedb(tx: 1 write)> put acct:2 110
OK
basedb(tx: 2 writes)> commit
OK: committed 2 writes
```

- **All or nothing:** a transaction's writes are applied together at `commit`, as
  one logged unit, even across a crash. `rollback` discards them, and so does `exit`
  with a transaction still open.
- **Isolated:** transactions use strict two-phase locking, so concurrent
  transactions behave as if they ran one after another. A scan inside a transaction
  sees no keys appear or vanish until it ends. If two transactions deadlock, one is
  rolled back and has to be run again.
- Outside a transaction, every `put`/`delete` is a transaction of its own.

In Go, transactions look like this:

```go
tx, err := db.Begin()
v, ok, err := tx.GetForUpdate([]byte("acct:1")) // read, locked for writing
err = tx.Put([]byte("acct:1"), []byte("90"))
err = tx.Commit()                                // or tx.Rollback()
```

### `stats` example

After 300 puts into a new database:

```
directory       ./mydb
keys            300
tree            height 2, 2 leaves, 1 internal node
pages           3 allocated: 3 in the tree, 0 free (data file 16.0 KiB)
buffer pool     256 frames (1.0 MiB), 398 hits, 0 misses (100.0% hits), 0 evictions, 0 page writes
log             38.2 KiB (checkpoint at 4.0 MiB)
last recovery   0 transactions read, 0 records redone, 0 already on disk, 0 torn pages rebuilt
locks           600 granted, 0 waited, 0 deadlocks resolved, 0 held now
commits         300 commits in 300 fsyncs (1.0 per fsync, group commit)
limits          key ≤ 256 bytes, value ≤ 1024 bytes, ≤ 1000 keys written per transaction
```

(One writer at a time shares nothing: 1 commit per fsync. Under a server with many
clients the same line shows many commits per fsync, see Performance below.)

## Guarantees

- **Durable:** when `put` or `delete` prints `OK`, the change is in the write-ahead
  log and fsynced, so it survives a crash or a power failure.
- **Crash-safe:** after a crash, the next start replays the log. An operation (or a
  whole transaction) is either fully there or not at all, including page splits. Torn
  page writes, and a torn meta page, are repaired from the log.
- **Isolated:** transactions are serializable (strict two-phase locking, with
  deadlock detection).
- **One process at a time:** a second `basedb` on the same directory is refused
  ("already open in another basedb process"). The lock disappears automatically if
  the process dies.
- **Bounded files:** the log is truncated at checkpoints (every 4 MiB, and on exit).
  Deleted space is reused, so the data file stops growing under a delete-heavy load.
- **Many clients:** a server serves up to 1,000 connections at once. A client that
  disconnects or stays silent too long loses its open transaction, so it can't block
  others.

## Performance

Writes through the TCP server, with real fsync and 100-byte values (`go run
./cmd/basedb-bench`; numbers from the development machine, see
[bench/group_commit.csv](bench/group_commit.csv)):

| clients | group commit off | group commit on | writes per fsync |
|---|---|---|---|
| 1 | 1,128 writes/s | 1,109 writes/s | 1.0 |
| 8 | 1,125 | 4,042 | 3.6 |
| 32 | 1,114 | 16,305 | 14.2 |
| 64 | 1,126 | **31,648** | **31.1** |

Every commit must reach the disk (fsync) before it is acknowledged, and the disk does
about 1,000 fsyncs per second. Without group commit, that caps the database at about
1,000 writes/s however many clients there are. Group commit lets concurrent commits share
one fsync, so throughput grows with the number of clients: 28× at 64 clients. (It was 14×
before the checkpoint storm fix: many writers used to run one redundant checkpoint each;
see [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) §9.6.)

### Compared with other storage engines

The same workloads against five production embedded engines, with fsync on every commit in
all of them and the same 64 MiB cache (`cd bench/compare && go run .`; setup, method and
fairness rules in [bench/compare/README.md](bench/compare/README.md); full tables in
[bench/compare4.txt](bench/compare4.txt)). Median of 3 runs, 100,000 keys:

| Workload | baseDB | Pebble | RocksDB | SQLite | WiredTiger | redb |
|---|---|---|---|---|---|---|
| point reads | 644,000/s | 376,000 | 477,000 | 319,000 | **795,000** | 507,000 |
| scans of 10 keys | 205,000/s | 293,000 | 168,000 | 142,000 | **584,000** | 394,000 |
| random bulk insert | 42,600/s | **717,000** | 364,000 | 38,300 | 132,000 | 39,200 |
| updates, 1 per transaction | 1,135/s | 1,158 | 1,131 | **1,696** | 1,175 | 903 |
| 64 concurrent writers | 34,929/s | **37,132** | 32,672 | 1,126 | 8,300 | 923 |
| on disk, 100,000 keys | 17.6 MiB | **10.9** | **10.9** | 13.4 | 13.0 | 32.1 |

baseDB is second of six on point reads and with many concurrent writers (group commit), and
pays one fsync per commit like the others. It is behind on scans (copying iterator) and on
space (the split rule leaves half-full pages), and the LSM trees win random inserts by
design. [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) (Week 9, §9.7) explains every row.
Every benchmark run is kept: `bench/compare1` … `compareN`.

## Files in the database directory

| File | Content |
|---|---|
| `data.db` | the pages: meta page (page 0), B+ tree nodes, free pages |
| `wal.log` | the write-ahead log since the last checkpoint |
| `LOCK` | held open while a process uses the database |

## Tests

```
go test ./...                 # all packages (about 20 s)
go test -short ./...          # skips the process-kill test, shorter model test
go test -race ./...           # with the data race detector (about 2 min)
```

The race detector needs cgo and a C compiler. On Windows, install gcc through
[MSYS2](https://www.msys2.org) (`pacman -S mingw-w64-ucrt-x86_64-gcc`), put
`C:\msys64\ucrt64\bin` on `PATH`, and run the tests from **PowerShell or cmd**. Run
from inside Git Bash, gcc and the race detector fail to start (gcc exits silently,
ThreadSanitizer reports "failed to allocate … error code: 87").

The tests include:
- a 100,000-operation model test of the B+ tree against a Go map
- a crash matrix that builds every on-disk state a crash can leave during splits,
  updates, deletes, page reuse and a multi-key transaction commit (1,120 states)
- concurrency tests for transactions: lost updates, a bank-transfer audit, phantoms,
  deadlocks
- server tests: 100 clients at once, transactions across connections, disconnects and
  idle clients releasing their locks, pipelining, the exact protocol bytes
- group commit: shared fsyncs, and every acknowledged write surviving a crash
- a test that kills a writing process at random moments and checks every
  acknowledged write

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for the full list.

## Project layout

```
cmd/basedb/          terminal program: local, server (-serve) and client (-connect) modes
cmd/basedb-bench/    load generator for the performance numbers
bench/compare/       baseDB vs Pebble, RocksDB, SQLite, WiredTiger, redb (a separate Go module)
client/              Go client library for the server
internal/server/     TCP server and the commands (Session)
internal/protocol/   the text protocol: quoting, requests, responses
internal/engine/     the database: Open, Get/Put/Delete/Scan, Begin (transactions), group commit, Checkpoint, Stats, Close
internal/btree/      B+ tree
internal/mtr/        mini-transactions: page changes → one logged, atomic unit
internal/wal/        write-ahead log
internal/recovery/   redo at startup
internal/buffer/     buffer pool, ARC replacement
internal/pager/      data file and log file I/O (every fsync lives here)
internal/page/       page format, slotted pages, meta page, free list
internal/lockfile/   one-process-at-a-time directory lock
internal/lockmgr/    lock manager for transactions (2PL, deadlock detection)
docs/                architecture and design decisions
```
