# baseDB — Architecture

A disk-based, distributed key-value store written in Go.

The project follows a week-by-week plan. Each week fixes a set of architectural
decisions. This document records each decision, why it was made, and how it
shows up in the code.

| Week | Topic | Status |
|---|---|---|
| 1 | Pages, pager, buffer pool | ✔ implemented |
| 2 | B+ tree (search, insert, split, scan, delete) | ✔ implemented |
| 3 | WAL, LSN, commit mechanism | ✔ implemented |
| 4 | Crash recovery, delete, storage tests | ✔ implemented |
| 5 | Embedded hardening: lock file, checkpoints, page reuse | ✔ implemented |
| 6 | Terminal program (`cmd/basedb`) | ✔ implemented |
| 7 | Transactions: strict two-phase locking | ✔ implemented |
| 8 | TCP server, client, group commit, benchmark | ✔ implemented |
| 9 | Comparison with other engines (bbolt, Pebble; then SQLite, RocksDB, redb, WiredTiger) | ✔ measured |
| 10+ | *(plan: replication)* | — |

---

## Week 1 — Storage foundation

**Goal:** read and write the disk, cache pages in memory, and evict pages.

**Pass criterion:** write 10,000 pages and read them back with a small buffer pool
(`TestTenThousandPages`: 16 frames, also after reopening the file).

### Package layout

```
internal/page/     page format: header, slotted page, cells, meta page, free list
internal/pager/    the data file: read/write, allocation, checksums, meta, fsync
internal/buffer/   buffer pool: frames, page table, pin/dirty, ARC replacer
```

Dependencies point downward only: `buffer → pager → page`.

### 1.1 Fixed 4096-byte pages

`page.PageSize = 4096`, a compile-time constant.

- **Why 4 KiB:** it matches the OS page size and the physical sector size of most SSDs, so
  a page write is normally a single device write. Smaller pages reduce B+ tree fan-out.
  Larger ones increase write amplification.
- **Why a constant:** every in-page offset fits in a `uint16`. The layout can be fixed at
  compile time. A compile-time check fails if `PageSize` ever outgrows `uint16`.
- **Addressing:** `PageID uint32`, byte offset = `PageID × 4096`. The ID is the location,
  so no mapping table is needed.

### 1.2 Page header (28 bytes, on every page)

```
offset size field
0      4    pageID        the page's own ID
4      1    type          unused / meta / leaf / internal / free
5      3    reserved
8      8    LSN           LSN of the last log record applied to this page
16     2    numSlots      number of slots
18     2    freeSpacePtr  start of the cell area (PageSize when empty)
20     4    rightSibling  next page on the same level / next free page
24     4    CRC32C        over every byte of the page except this field
```

- **pageID** in the header catches *misdirected writes* (a page written to the wrong offset).
- **LSN** is in the format from day one, so adding the WAL later doesn't change the page layout.
- **numSlots / freeSpacePtr** are in the common header because almost every page is a slotted
  page (B+ tree nodes).
- **rightSibling** links leaves for range scans. Free pages reuse the same field as the
  free-list "next" pointer (§1.6).
- **CRC32C** (Castagnoli) has hardware support on modern CPUs. It detects corruption and
  *torn writes* (a crash in the middle of a page write).
  - It is computed by the pager on a **private copy** at write time and verified at read time.
    It is never kept up to date in memory, which avoids races with concurrent readers.
  - An **all-zero page counts as valid**: it was allocated but never written.
- `InvalidPageID = 0` serves as the nil pointer. Page 0 is the meta page, which is never a
  child or sibling of another page, so a zeroed page has all its pointers nil.

### 1.3 Slotted page

```
0              28
┌──────────────┬────────────────────┬────────────┬─────────────────┐
│ header       │ slot array →       │ free space │ ← cells         │
│              │ [off u16][len u16] │            │                 │
└──────────────┴────────────────────┴────────────┴─────────────────┘
                                    ↑            ↑
                    28 + numSlots*4              freeSpacePtr
```

- Slots grow forward and cells grow backward from the end of the page.
- The slot order is the logical order (the B+ tree keeps slots sorted by key). Inserting
  at position *i* moves 4-byte slots, never the cell bodies. Binary search works directly on
  slot indexes.
- **Deletes leave holes.** `Compact()` runs only when an insert needs contiguous space and the
  total free space is enough. No fragmentation counter is needed: free space is computed
  from the slot lengths (`FreeSpace()`), and that calculation only happens when space is tight.
- API: `InsertCell(i, cell)`, `DeleteCell(i)`, `UpdateCell(i, cell)`, `Cell(i)`,
  `FreeSpace()`, `Fits(n)`, `Compact()`.

### 1.4 Cells and limits

```
cell = [keyLen u16][valueLen u16][key][value]
```

- `MaxKeySize = 256`, `MaxValueSize = 1024`. There are no overflow pages.
- **Why these limits:** the largest cell is 1284 bytes + a 4-byte slot. Three of them fit
  in 4068 usable bytes (`TestThreeMaxCellsFit`). So a node split always leaves both
  halves non-empty. Overflow pages for large values are a classic source of bugs, so they
  are not included.
- Internal B+ tree nodes can use the same encoding with the child page ID as the value.

### 1.5 Meta page (page 0)

```
offset size field
0      28   common header (type = meta)
28     8    magic             "BASEDB01"
36     4    version           1
40     4    pageSize          4096
44     4    rootPageID        InvalidPageID when the tree is empty
48     4    nextPageID        first page ID never handed out
52     4    freeListHead      InvalidPageID when the list is empty
56     8    lastAppliedIndex  last log entry applied to this store
```

- **magic, version and pageSize** are checked on `Open`, so a wrong file, an old format or a
  different build fails immediately instead of producing garbage.
- The meta page is **owned by the pager**. It is kept in memory (`Meta()`, `UpdateMeta()`) and
  written on every `Sync()`. The buffer pool never caches page 0: `ReadPage(0)` and
  `WritePage(0)` return `ErrMetaPage`. That way there is only one copy of the meta page,
  in one place.

### 1.6 Free list

```
meta.freeListHead ──► [type=free] ──rightSibling──► [type=free] ──► InvalidPageID
```

The list is linked through the free pages themselves, so it needs no extra space. Free:
push onto the head. Allocate: pop the head. Week 1 defined only the format (`InitFree`,
`NextFree`). *Since week 5 it is in use:* deletes free emptied pages and allocation takes
pages from it first (§5.3).

### 1.7 Pager

- `ReadPage` / `WritePage` use `ReadAt` / `WriteAt`. These don't share a file offset, so they
  are safe to call concurrently without a lock.
- **Checksums:** verified on every read (`ErrChecksum`). The page ID is checked against its
  location (`ErrMisdirected`).
- **Allocation:** `AllocatePage()` returns `nextPageID++`. Nothing is written yet. A page that
  was allocated but never written reads as zeros.
  - IDs that were never allocated are rejected (`ErrPageOutOfRange`). This catches dangling
    pointers early.
- **All fsync calls are in this package.** `Sync()` writes the meta page and then calls
  `fsync`. There is exactly one place that decides when data becomes durable, so the
  durability rules can be reviewed in a single file.
  - Since week 3 the WAL's file is here too (`pager.LogFile`, §3.1). The `wal` package decides
    what to write; the file I/O and its `fsync` stay in this package.
- `Open` on a new or empty file creates the meta page and syncs it. `Close` syncs.
- *Added in week 4, for recovery (§4.3):*
  - `OpenIgnoringMeta` opens a file whose meta page is torn.
  - `SetMeta` restores every meta field, including `NextPageID`.
  - `NumPages` returns the file size in pages.
  - `Abandon` (crash tests only) closes the file without writing anything.
- *Added in week 5:*
  - `UnallocatePage(id)` gives the newest page ID back, when the operation that allocated
    it is aborted (§5.3).
  - `ReplaceLogFile` / `RemoveStaleLogTemp` replace the log file atomically at a
    checkpoint: temp file, fsync, rename, directory fsync (§5.2).
- **No `O_DIRECT`:** Go has no portable way to do it. We use the OS page cache plus explicit
  `fsync`.

### 1.8 Buffer pool

```
pageTable: map[PageID]frame      frames: [ f0 | f1 | ... | fN-1 ]   (fixed, allocated once)
free:      never-used frames       each: page bytes, pinCount, dirty, RWMutex latch
replacer:  ARC (Adaptive Replacement Cache), evicts only frames with pinCount == 0
```

API: `Fetch(id)`, `NewPage()`, `Unpin(id, dirty)`, `FlushPage(id)`, `FlushAll()`.

| Decision | Why |
|---|---|
| Fixed number of frames | Predictable memory, no GC churn |
| **Pin count**: a pinned page is never evicted | A caller can safely hold `*Frame`. Every `Fetch`/`NewPage` needs exactly one `Unpin` |
| **Dirty flag**, sticky until written | `Unpin(id, false)` from one user never cancels another user's `Unpin(id, true)` |
| **ARC** replacement (changed from LRU, see §1.8.1) | Resists scans: pages used once can't push out pages used often. Adapts between recency and frequency by itself. Behind a `Replacer` interface |
| `NewPage()` allocates through the pager | Gets a frame first, then an ID, so a full pool doesn't waste page IDs |
| **Latches** (per-frame `RWMutex`), separate from locks | Latches protect bytes in memory for microseconds. Transaction locks (later) protect keys |
| One pool mutex; disk I/O happens while holding it | Simple (no "frame is loading" state). It costs throughput on many misses, which is acceptable here |
| `LogFlusher` hook (implemented by `wal.Log` since week 3) | The **WAL rule**: before writing a dirty page, the log must be durable up to its LSN. The hook existed from week 1, so the buffer pool didn't change when the WAL arrived |
| `FlushAll` = write dirty pages + `pager.Sync()` | The pool never calls fsync itself (§1.7) |

**Lock ordering (prevents deadlock):** frame latch → pool mutex. The pool never waits
for a latch while holding its mutex. Eviction doesn't need the latch, because pinCount == 0
means no one can hold it. `FlushPage` pins the page, releases the mutex, then takes the
read latch and copies the page. It clears the dirty flag *before* copying, so a concurrent
writer re-marks the page dirty.

#### 1.8.1 Replacement policy: ARC (changed from LRU)

```
        live pages (have a frame)               ghosts (page IDs only, no data)
mru  T1  pages used once recently          mruGhost  B1  recently evicted from mru
mfu  T2  pages used at least twice         mfuGhost  B2  recently evicted from mfu

p = target size of mru, 0 ≤ p ≤ c (c = number of frames)
```

| Event | What happens |
|---|---|
| hit (page is cached) | move it to the front of **mfu** |
| miss, page is in **mruGhost** | recency was evicted too early: **p grows** by max(1, \|B2\|/\|B1\|); page goes to mfu |
| miss, page is in **mfuGhost** | frequency was evicted too early: **p shrinks** by max(1, \|B1\|/\|B2\|); page goes to mfu |
| complete miss | trim the oldest ghost so the lists stay bounded, then put the page in **mru** |
| evict | if \|mru\| ≥ p, take the oldest *evictable* page of mru, else of mfu (fall back to the other list); its ID becomes a ghost |

Bounds: |mru| + |mruGhost| ≤ c, and all four lists together hold ≤ 2c entries.
`TestArcRandomInvariants` checks these after every step.

- **Why change from LRU:** a database does long sequential reads. Examples are
  `Scan(nil, nil)`, `Verify()`, and later recovery and replication catch-up. Under LRU, one
  scan larger than the pool evicts every hot page, such as the root and upper internal nodes
  of the B+ tree.
  - With ARC, scanned pages are used once and stay in mru. They are evicted first, while the
    pages used repeatedly sit in mfu (`TestScanResistance`: 2 hot pages survive a scan of
    200 pages through a 4-frame pool).
  - ARC also tunes itself through p. A fixed recency/frequency split like LRU-K would need a
    tuning parameter.
- **Cost:** ghost entries are a few bytes each (≤ c of them). All operations are O(1) except
  Evict, which skips pinned entries at the old end of a list.
- **Integration rules in the pool:**
  - `RecordAccess(frame, pageID)` is called on every real access (load, hit) and **before**
    `SetEvictable`, because ARC only knows a frame after its first access.
  - **Pins that are not accesses don't count.** `FlushPage` pins the page so it can't be
    evicted during the write, but flushing is not a use of the page
    (`TestFlushIsNotAnAccess`). Otherwise a checkpoint would wrongly promote every page to mfu.
  - If writing an evicted dirty page fails, the page stays cached. `Evict` has already turned
    it into a ghost, so the pool re-records the access to revive it. This error path also
    nudges p slightly, which is acceptable.
- **Differences from the base code you provided:**
  - Pool types (`page.PageID` uint32, `int` frames) instead of int32 types.
  - No internal mutex: the pool mutex already protects the replacer.
  - No `AccessType` parameter: ARC doesn't use it. It can be added back if access types are
    ever needed.
  - `>=` instead of `==` in the ghost-trimming bounds, which is more defensive.
  - `Remove` on a pinned frame panics instead of silently doing nothing.
  - A hit whose page ID doesn't match the frame's page panics. Both of those would be pool bugs.

### 1.9 Tests

| Required scenario | Test |
|---|---|
| Creating a new file | `TestNewFile`, `TestReopenKeepsPagesAndMeta`, `TestOpenRejectsNonDatabaseFile` |
| Detecting a corrupted checksum | `TestCorruptedChecksum`, `TestCorruptedMetaPage`, `TestChecksum` |
| A pinned page is not evicted | `TestPinnedPageIsNotEvicted` (also `ErrNoFreeFrame` when all frames are pinned) |
| Hundreds of pages cycling through 3 frames | `TestCyclingThroughThreeFrames` (500 pages, forward / backward / strided) |
| **Pass:** 10,000 pages with a small pool | `TestTenThousandPages` |

Also tested: a randomized slotted-page test against a slice model, cell limits, the meta
round trip, ARC (`arc_test.go`: eviction order, ghost hits, target adaptation, and a
50,000-step randomized invariant check), scan resistance, the WAL-rule hook order, and a concurrent-counter test
(8 goroutines, no lost updates). Run with `go test ./...`.

**Race detector:** `go test -race ./...` passes for the whole project, including the
transaction and lock manager tests of week 7 (last full run: 2026-10-29). On Windows it needs
cgo and gcc (installed through MSYS2), and it must be run from **PowerShell or cmd**. Started
from the Git Bash environment used during development, both gcc and ThreadSanitizer fail to
start: gcc exits silently, and TSan reports "failed to allocate … (error code: 87)". That
was an environment problem, not a race in the code. Between weeks 3 and 7 the race detector
was wrongly reported as broken on this machine; it never was.

### 1.10 Known limitations (to be addressed in later weeks)

1. ~~**Allocation is not crash-safe yet.**~~ *Solved in week 4:* every transaction that
   allocates logs the new `nextPageID` in its META record (§3.4). Recovery restores it as
   max(logged, on disk) (§4.3), so a committed page ID is never handed out twice.
   - *Week 5:* aborted mini-transactions no longer leak IDs; they give them back (§5.3).
2. ~~**Internal nodes need one more child pointer.**~~ Solved in week 2 (§2.1): cell 0 of
   an internal node has an empty key and points to the leftmost child.
3. ~~**Torn pages are detected, not repaired.**~~ *Solved in week 4* (§4.3) by replaying the
   page's whole history. *Week 5:* since checkpoints truncate the log, the first change after
   a checkpoint is logged as a full page image, and recovery rebuilds a torn page from that
   (§5.2).

### 1.11 Exercise: one Put, start to finish (to sketch on paper)

Before week 3, trace a single `Put(k, v)` through log record → page change → page LSN →
log flush → page flush → crash → recovery. Points to check in your sketch:
- When does the record get its LSN, and when is that LSN written into the page header?
- Why may the page reach disk only after the log is durable up to the page LSN?
  This is what the `LogFlusher` hook enforces.
- During recovery, how does comparing the record LSN with the page LSN decide
  "apply or skip"?

---

## Week 2 — B+ tree

**Goal:** a B+ tree with every kind of change (insert, update, split, delete), without WAL yet.

**Pass criterion:** the model test with 100,000 random operations passes (`TestModel`).
It reaches height 4 with about 1,150 leaves through a 32-frame pool, and the tree is checked
again after reopening the file.

**Why delete is in week 2:** the WAL in week 3 has to cover every write path. If delete came
later, the logging work would have to be done twice.

### Package layout

```
internal/btree/
  node.go      node formats, binary search, split point, page rebuild
  mutation.go  one Put/Delete = one mini-transaction (wraps mtr.Mtr since week 3)
  tree.go      Tree, Get, Put, Delete, split / root split
  iterator.go  Scan(start, end) iterator over the leaf chain
  verify.go    Verify(): structural invariants
```

### 2.1 Node formats

Both node types are slotted pages. Their cells use the week-1 format
`[keyLen][valueLen][key][value]` and are kept sorted by key.

```
Leaf      cell i = (key_i, value_i)       key_0 < key_1 < ...
          rightSibling = next leaf in key order (InvalidPageID for the last leaf)

Internal  cell 0 = ("",    child_0)       empty key = −∞
          cell i = (key_i, child_i)       child_i holds keys in [key_i, key_i+1)
```

- **Why the leftmost child is an ordinary cell with an empty key:** a node with *n* keys has
  *n+1* children, and the header has no field for the extra child. A cell whose key is never
  compared needs no format change. Leaves and internal nodes then share the same
  insert / split / rebuild code.
  - Empty user keys still work, because the search never looks at cell 0's key
    (`TestEmptyKeyAndValue`).
- Internal nodes leave `rightSibling` unused. Only leaves are chained, because only scans need it.
- The child pointer is the 4-byte value of an internal cell. Separator keys are copied from
  stored keys, so they always fit within `MaxKeySize`.

### 2.2 Search

- **Leaf:** binary search for the first key ≥ k (`leafSearch`).
- **Internal:** binary search for the last cell *i* where *i* = 0 or key_i ≤ k (`childIndex`).

### 2.3 Put = insert or update, with recursive splits

```
descend from the root to the leaf, remembering (node, child index) on the way
key exists?  UpdateCell   else  InsertCell
  fits → done
  ErrNoSpace → split the leaf; insert (separator, right) into the parent;
               the parent may split too ... up to the root
root split → new internal root with cells ("" → old root, sep → right)
```

- **Splits are by bytes, not by cell count** (`splitPoint`). The left node takes the smallest
  prefix of cells that holds at least half of the bytes. Both sides always get at least one
  cell. With the week-1 limits, both halves are guaranteed to fit:
  - The total is at most 4068 (a full page) + 1288 (the new cell) = 5356.
  - The left side is at most 5356/2 + 1288 = 3966 ≤ 4068. The right side is at most 2678.
- **Leaf split:** the separator is a copy of the right node's first key. The new leaf is
  linked into the chain: `right.rightSibling = left.rightSibling` and
  `left.rightSibling = right`.
- **Internal split:** the first right cell's key moves up to the parent as the separator.
  That cell stays in the right node as its −∞ cell.
- **An update that no longer fits** goes through the same split path, with the old cell
  replaced (`cellsWith(..., replace=true)`). There is no special code for it.
- The new page is allocated **before** the old node is changed. If allocation fails, that
  node is still intact.

### 2.4 Lazy delete

Delete removes the cell from its leaf and does nothing else: no merge, no redistribution, and
separators are not updated.

- **Why:** merging and redistribution are the most bug-prone parts of a B+ tree. Leaving them
  out costs only space.
- Empty leaves stay in the tree and in the leaf chain. Scans skip them
  (`TestScanAfterDelete` deletes every key and then inserts again).
- Separators stay valid even when the keys they were copied from are gone. A separator only
  has to *bound* the keys in a subtree, not exist in it.
- The tree never shrinks. Reusing empty pages through the free list is future work.
- *Week 4:* the slot-directory side of delete (removal, not tombstones) and free-space reuse
  are explained in §4.1–4.2. Crash safety of delete is covered in §4.6.

> **Week 5 update:** delete is no longer purely lazy. A leaf that becomes **empty** is
> removed from the tree and its page goes to the free list. Parents that become empty follow,
> and a root with one child is replaced by that child, so the tree shrinks (§5.3). Leaves that
> still hold records are still never merged or rebalanced, so the "why" above still holds.
> The empty-leaf points above no longer apply: `TestScanAfterDelete` now checks that
> deleting every key returns all pages to the free list.

### 2.5 Scan iterator

```go
it := tree.Scan(start, end)          // [start, end); nil = unbounded
for it.Next() { it.Key(); it.Value() }
it.Err()
```

- **No locks or pins are held between `Next` calls.** Each refill takes the tree read lock,
  copies the matching records of **one leaf**, and releases everything. So a forgotten
  iterator can't block writers or keep frames pinned.
- **The next batch is found by searching again** for the successor of the last returned key
  (that key + `0x00`), not by keeping a page pointer. This stays correct if writers split
  leaves between batches, and it will also stay correct once freed pages are reused.
  - Within one batch, the leaf chain is followed past empty leaves.
- **Guarantee:** keys come out in ascending order, and each batch is consistent. The scan as a
  whole is *not* a snapshot of the tree (that would need MVCC or locks).

### 2.6 Verify()

Checks, over the whole tree (`TestVerifyDetectsCorruption` breaks each one on purpose):

- every reachable page is a leaf or an internal node and is reached only once (no cycles or shared children)
- keys inside every node are strictly ascending
- **separators are valid:** every key in child *i* lies in [key_i, key_i+1), with the bounds
  inherited from all the ancestors, and cell 0 of an internal node has an empty key
- all leaves are at the same depth
- the `rightSibling` chain visits exactly the leaves, from left to right
- the root in the meta page equals the tree's root

Verify copies each page out before recursing, so it never pins more than one page at a time.

### 2.7 Pages go through one place: `mutation`

All page access goes through the buffer pool. A Put or Delete fetches pages only through its
`mutation`:

| | |
|---|---|
| `fetch(id)` / `create(type)` | pin + **write latch**, held until `finish` |
| `markDirty(p)` | the single record of which pages changed |
| `setRoot(id)` | the root change, applied to the meta page after `finish` |
| `finish()` | release the latches, unpin with the right dirty flags |

**Why this shape:** in week 3, `finish` becomes the MTR commit. It will build the WAL record
from the dirty pages, stamp their LSNs, and only then unpin them. Because every page stays
pinned until `finish`, no changed page can be evicted (and reach disk) before its log record
exists. `TestRootSplitCollectsPages` checks that a plain insert reports exactly 1 dirty page
and a root split exactly 3 (old leaf, new leaf, new root).

> **Week 3 update:** this happened as planned. `mutation` now wraps `mtr.Mtr` (§3.5).
> `fetch`/`create`/`markDirty`/`setRoot` keep their names, so `put`/`split`/`delete` didn't
> change. `finish` became `Commit` (logs + makes durable) or `Abort` (restores the
> before-images). `setRoot` now goes through `UpdateMeta`, so the root change is logged and
> reaches the meta page only after the commit is durable.

### 2.8 Concurrency

One `sync.RWMutex` for the whole tree: many readers (Get, Scan batches, Verify) or one writer
(Put, Delete). Page latches are still taken (write latches in `mutation`, read latches on
reads) so the pool's `FlushPage` always sees consistent pages.
`TestConcurrentReadersAndWriters` runs under `-race`. Latch crabbing is out of scope.

> **Week 7 note:** this tree lock is a *latch*: it protects the tree's pages for the moment
> one operation runs. *Locks* that protect keys for the length of a user transaction are a
> separate layer above it (`lockmgr`, §7). Since week 7, `Apply` runs a whole batch of puts
> and deletes as one mutation; the mutation tracks the current root, so a batch can split the
> root and keep going (§7.4).

### 2.9 Known limitations

1. ~~**A failed mutation can't be rolled back yet.**~~ *Solved in week 3:* a failed mutation
   is aborted and its pages are restored from their before-images (§3.5,
   `TestFailedPutIsRolledBack`). The tree is only marked **broken** if the commit itself
   can't be made durable (the log write or fsync fails).
2. **Pool size:** a mutation keeps its whole root-to-leaf path pinned, plus the new pages from
   splits, which is about 2 × height + 1 frames. 32 frames is plenty for height 4. If the pool
   is too small, the Put fails with `ErrNoFreeFrame` and is rolled back.
3. ~~**Durability.**~~ *Solved in weeks 3–4:* every Put/Delete is durable in the log when it
   returns, and recovery at startup brings the data file up to date (§4.3).

---

## Week 3 — WAL, LSN and the commit mechanism

**Goal:** a crash must never leave the data file in a state that a write only half produced.
Every change is described in the write-ahead log (WAL) before it can reach the data file. The
changes of one operation are grouped into one transaction. A transaction is durable once its
COMMIT record is synced.

**Delivered:**
- the WAL record format (txn ID, type, LSN, change data)
- append/flush with the right fsync order
- slotted-page changes logged as byte diffs, so header + slot directory + cells always stay
  consistent
- detection of corrupt and incomplete records
- the week-2 `mutation` turned into a mini-transaction

### Package layout

```
internal/pager/logfile.go  raw log file: ReadAt/WriteAt/Truncate/Sync (fsync stays in pager)
internal/wal/
  record.go   record format, encode/decode with validation
  diff.go     Diff(before, after) → runs; Apply(page, record) = redo of one record
  log.go      Log: Append, FlushUntil, Open (scan + cut torn tail), Transactions
internal/mtr/
  mtr.go      mini-transaction: Fetch/Create/MarkDirty/UpdateMeta, Commit, Abort
```

The dependency order stays downward:
`btree → mtr → (buffer, wal) → pager → page`. `buffer` doesn't import `wal`; it only sees
the `LogFlusher` interface from week 1.

### 3.1 The log file

```
[file header 16 B: "BASEWAL1" | version u32 | reserved][record][record]...
```

- **LSN = the byte offset of a record in the file.** "Is LSN x durable?" is a single
  comparison (`x < durable`), and a record can be found directly from its LSN.
- Because of the 16-byte header, **no record has LSN 0**. A page with pageLSN 0 was never
  logged, and `FlushUntil(0)` is a no-op.
- One log file. It is never truncated except to cut a torn tail (§3.6). Removing old log
  needs checkpoints (a later week).

> **Week 5 update:** the header is now 24 bytes (format version 2) and adds a `startLSN`.
> LSN = startLSN + (file offset − 24). A new log starts at LSN 24, so LSN still equals the
> file offset until the first checkpoint. A checkpoint replaces the log with an empty one
> that continues at the old end LSN, so LSNs never go backwards (§5.2).
- The file lives in `pager.LogFile`, keeping week 1's rule that every fsync is in the
  `pager` package.

### 3.2 Record format

```
offset size field
0      4    length    whole record, header included
4      4    CRC32C    over bytes [8, length)
8      8    LSN       = file offset of this record (self-check)
16     8    txnID
24     1    type      BEGIN | PAGE | META | COMMIT
25     ...  payload

PAGE    pageID u32 | flags u8 (bit 0 = init) | numRuns u16 | runs: (offset u16, len u16, bytes)
META    rootPageID u32 | nextPageID u32 | freeListHead u32 | lastAppliedIndex u64
BEGIN, COMMIT: no payload
```

| Field | Why |
|---|---|
| **length first** | A reader can find the end of a record without knowing its type, and can detect "the file ends before the record does" (§3.6) |
| **CRC32C over everything after itself** | Detects torn and corrupt records. The LSN, txnID, type and payload are all covered |
| **LSN stored in the record** | Catches records that are framed correctly but are in the wrong place (stale data, a copied segment). The checksum alone can't detect those |
| **txnID** | Groups records into transactions. IDs keep increasing across restarts: `Open` continues after the largest ID in the log |
| **PAGE = after-image of changed byte ranges** (physical redo) | Redo is "copy these bytes here". That is idempotent (applying a record twice is harmless) and needs no B+ tree logic during replay |
| **init flag** | For new pages: "start from zeros, then apply". A reused page ID may have old bytes on disk, and a diff against them would be wrong |
| **META = the full mutable meta snapshot** | Root changes and allocation (`nextPageID`) are logged with the pages that use them. A snapshot is simpler and safer than "field X changed" |
| `MaxRecordSize` = 32 KiB | A PAGE record is at most one page plus run headers (< 8 KiB). A larger length field is corruption, so it is rejected before allocating a buffer |

No undo information is logged, on purpose: §3.5 guarantees that uncommitted changes never
reach the data file, so there is never anything to undo.

### 3.3 Slotted-page changes: diffs of the whole page

`wal.Diff(before, after)` compares the two 4 KiB images byte by byte and returns the changed
ranges. Ranges closer than 8 bytes are merged, because each run costs a 4-byte header.

- **Why this keeps slotted pages consistent:** one B+ tree insert changes three areas of the
  page at once: the header (`numSlots`, `freeSpacePtr`), the slot directory (shifted slots),
  and the cell area (new cell bytes, or everything moved by `Compact`).
  - A diff of the whole page captures all three in **one PAGE record**, and one CRC protects
    it. Replay can't apply the slot directory without the cell it points to.
  - The B+ tree doesn't describe its changes (no "insert cell" log type per operation), so
    no change can be forgotten. Any future page operation is covered automatically.
- **Size:** a small insert logs a few dozen bytes, not the whole page
  (`TestDiffIsSmallForSmallChanges`). A compaction logs the moved region.
- `TestDiffApplySlottedPage` runs 3,000 random inserts, deletes, updates, compactions and
  sibling changes. It checks after every step that replaying the diffs gives exactly the same
  page, and that applying a record twice changes nothing.

### 3.4 One logical transaction per operation

One Put or Delete is one transaction, no matter how many pages it touches:

```
BEGIN(txn)
PAGE(txn, leaf)                 ← a plain insert stops here
PAGE(txn, new right leaf, init)
PAGE(txn, new root, init)       ← a root split adds these
META(txn, root=…, nextPageID=…)
COMMIT(txn)
```

- All records of a transaction are appended **in one `Append` call**, so they are contiguous
  in the log and get consecutive LSNs.
- `Transactions()` groups records by txnID and checks the structure: BEGIN first, PAGE/META
  only inside an open transaction, nothing after COMMIT, no reused IDs. A structure error is
  `ErrCorrupt`.
- **A transaction without a COMMIT record never happened.** Its records are ignored.
- `TestEveryWriteIsLogged` checks that a plain insert logs 1 PAGE record, a root split logs
  3 PAGE + 1 META, and a delete of a missing key logs nothing.

### 3.5 The mini-transaction and the commit order

`mtr.Mtr` replaces the week-2 `mutation` internals:

| | |
|---|---|
| `Fetch(id)` | pin + write latch until the end. Saves a **before-image** (4 KiB copy) |
| `Create(type)` | new page. Its before-image is "all zeros", and it is logged as an init record |
| `MarkDirty(p)` | p changed. Only marked pages are diffed |
| `UpdateMeta(fn)` | meta change. Logged now, applied to the pager after the commit is durable |
| `Commit()` | see below |
| `Abort()` | copy the before-images back, release. Nothing is logged |

**Commit order** (this order is what makes a crash safe at every point):

```
1. build records: BEGIN, PAGE per changed page (Diff vs before-image), META, COMMIT
2. log.Append(all)              → consecutive LSNs, still only in memory
3. stamp pageLSN = LSN of that page's PAGE record
4. log.FlushUntil(COMMIT LSN)   → write + fsync          ← the transaction is durable HERE
5. apply the meta change to the pager (in memory)
6. release latches and pins     → pages may now be evicted and written to the data file
```

| Crash during… | What survives | Why it is safe |
|---|---|---|
| 1–3 | nothing in the log, nothing in the data file | the pages are pinned, so they can't be written |
| 4 (the log write is torn) | a transaction without a valid COMMIT | it is ignored, and the data file never saw its pages |
| after 4 | the whole transaction is in the log | the data file may have none, some or all pages. The log has all of them |
| 6+, while a page is being written | the log + a torn page | the pool flushed the log first (WAL rule), so the record exists |

- **The WAL rule is enforced in two places.** (a) Pages stay pinned until step 6, so an
  uncommitted change can never be evicted (*no-steal*, `TestPagesStayPinnedUntilCommit`).
  (b) When the pool writes a dirty page, it first calls `log.FlushUntil(pageLSN)`, the
  week-1 hook.
- **The data file is not synced at commit** (*no-force*). The log already has everything, so
  pages reach the data file lazily (eviction, `FlushAll`). One commit costs one sequential
  log write + one fsync, not a random write per page.
- **Abort leaves the tree usable.** If anything fails before step 2 (e.g. `ErrNoFreeFrame`
  in the middle of a split), the before-images are copied back.
  - ~~Page IDs taken by `Create` are leaked.~~ *Since week 5* they are given back:
    pages that grew the file are dropped from the pool and their IDs returned, and pages
    taken from the free list get their before-image back (§5.3).
  - The tree is only marked broken if step 4 fails, because then memory is ahead of the log.

### 3.6 Detecting corrupt and incomplete records

`Open` scans the log from the start and validates every record:

| Check | Error |
|---|---|
| the file ends before the length field or the record ends | `ErrIncomplete`: a crash during a log write (normal) |
| length < header or > `MaxRecordSize` | `ErrCorrupt` |
| CRC mismatch | `ErrCorrupt` |
| stored LSN ≠ file offset | `ErrCorrupt` |
| unknown type or flags, a payload of the wrong size, a run outside the page, a PAGE record for page 0 | `ErrCorrupt` |
| bad grouping (§3.4) | `ErrCorrupt` (from `Transactions`) |

- **Policy: stop at the first invalid record, cut the file there, and continue.**
  `TailError()` reports what was cut.
  - Why not skip it? After a bad record, its length can't be trusted, so the next record
    can't be found.
  - Why cut it off? So new records are never written after garbage, where they would be
    unreachable.
  - A torn last record is the normal result of a crash, and it belongs to a transaction whose
    COMMIT never became durable, so nothing committed is lost.
- Tests: `TestTornTailIsCutOff` (crash in the middle of a COMMIT record: the transaction
  disappears and new appends work), `TestCorruptRecordStopsTheLog`,
  `TestDecodeRejectsCorruption` (7 kinds of damage), `TestBadGroupingIsCorrupt`,
  `TestUncommittedTxnIsIgnored`, and `TestBadHeader`.

### 3.7 Tests: the log alone is enough

The strongest check is to **rebuild the database from the log alone**:

- `TestLogRebuildsTree` runs 20,000 random Put/Delete operations without flushing data pages.
  It then replays every committed transaction (`wal.Apply`, META snapshots) onto an empty
  in-memory store and opens a B+ tree on it.
  - The rebuilt tree passes `Verify`, has exactly the model's contents, and has the same meta.
  - After `FlushAll`, every rebuilt page is **byte-identical** to the data file.
- `TestLogRebuildsPages` (mtr) does the same at the page level through a 4-frame pool, so
  pages are evicted all the time.
- `TestCommitLogsOneGroupedTransaction` checks one commit end to end: the record types,
  init flags, META contents, stamped pageLSNs, that the COMMIT is durable, and that the meta
  change was applied.
- `TestFailedPutIsRolledBack` uses a 4-frame pool, so a root split runs out of frames halfway.
  The tree still passes `Verify`, has all the earlier keys, and the log has exactly one
  transaction per successful Put.

**Test speed:** each commit is one fsync (≈ 0.9 ms here). The B+ tree tests open the log with
`wal.Options{NoSync: true}`: the same records and checksums, only the fsync is skipped. That
took the suite from 98 s to 8 s. The `wal` and `mtr` tests, which test durability, use real
fsync.

### 3.8 Known limitations / next steps

1. ~~**No replay at startup yet.**~~ *Done in week 4* (§4.3).
2. ~~**The log grows forever.**~~ *Solved in week 5:* checkpoints truncate it (§5.2).
3. ~~**One fsync per commit.**~~ *Solved in week 8:* group commit shares one fsync between
   concurrent commits (§8.3), giving up to 30 writes per fsync and 14× the throughput at 64
   clients.
4. ~~**Torn data pages.**~~ *Repaired in week 4* by replaying the page's full history, and
   since week 5 from a full page image logged on the first change after a checkpoint
   (Postgres's `full_page_writes`, §5.2).

---

## Week 4 — Crash recovery, delete and storage tests

**Goal:** after a restart, whether clean or after a crash, the data on disk is exactly the
result of the committed operations. The slotted-page storage engine supports CRUD and recovers
its data at startup.

### Package layout

```
internal/recovery/  Run: redo committed transactions onto the data file at startup
internal/engine/    DB: Open(dir) → recovery → Get/Put/Delete/Scan → Close
                    the one safe way to use the storage engine
```

Dependencies: `engine → (recovery, btree, buffer, wal, pager)`, `recovery → (wal, pager, page)`.
Recovery works directly on the pager, without the buffer pool or the B+ tree. It runs before
those exist.

### 4.1 Delete and the slot directory: remove, don't mark

Delete was implemented in week 2 (lazy delete, §2.4). This week fixes how it looks inside a
page. **A delete removes the slot from the slot directory.** The following slots shift left by
4 bytes. The cell bytes stay where they are and become a *hole* in the cell area.

| Option | Verdict |
|---|---|
| **Remove the slot** (chosen) | The slot directory stays dense and sorted, so binary search and split code need no special cases. `numSlots` is the real number of records. Nothing needs cleaning up later |
| Tombstone (keep the slot, flag it as deleted) | Every search, scan, split and `Verify` would have to skip tombstones, and something would have to clean them up eventually. Tombstones matter when slot numbers must stay stable, e.g. secondary indexes pointing at (page, slot) or MVCC. We have neither: the B+ tree finds records by key |

- **Crash safety needs no special handling.** The shifted slots, the new `numSlots` and the
  hole all change in one page, so one PAGE record covers them (§3.3). The crash matrix
  includes a delete (§4.6).
- `TestDeleteRemovesSlot` checks it: after deleting the middle of 5 records, the leaf has
  4 slots, still sorted.

### 4.2 Free space after deletes and updates; in-page compaction

Holes appear in two ways:
- **Delete:** the removed cell's bytes.
- **Update with a larger value:** the old cell's bytes. The new value is written elsewhere in
  the page. A smaller value is updated in place, and its unused tail is a hole.

Space accounting doesn't need a "fragmented bytes" field: `FreeSpace()` = page size − header −
slot directory − live cell bytes. That is computed from the slots, so it is always exact.

**Compaction runs only when it is needed:** when an insert or growing update finds that
`FreeSpace()` is enough but the *contiguous* gap between the slot directory and the cells is
not. `Compact()` then moves all live cells to the end of the page, in slot order.

- **Why lazy and not after every delete:**
  1. Most deletes are never followed by an insert into that page that needs the space.
  2. Compaction moves many bytes, and every moved byte is in the PAGE record's diff (the
     compaction scenario in §4.6 logs 813 bytes for one update). Compacting rarely keeps the
     log small.
- **Deleting the lowest cell** (the one at `freeSpacePtr`) gives its space back directly
  without compaction. **Deleting the last record** resets the page to empty.
- Tests:
  - `TestDeletedSpaceIsReused`: 50 rounds of deleting 10 records and inserting 10 new ones of
    the same size in a nearly full leaf, with **no split**.
  - `TestUpdateChurnDoesNotSplit`: 5,000 updates whose values grow and shrink between
    0 and 300 bytes, with no split.
  - The week-1 randomized slotted-page test still runs against a slice model.
- Empty *pages* are not reused yet (§2.4). That is page-level free space and needs the free list.
  *Done in week 5 (§5.3).*

### 4.3 Recovery algorithm (redo only)

```
engine.Open(dir):
 1. wal.Open        scan the log, cut off a torn/corrupt tail (§3.6)
 2. pager.Open      if the meta page fails its checksum (torn during Sync):
                    pager.OpenIgnoringMeta, metaDamaged = true
 3. recovery.Run:
    a. read the committed transactions (BEGIN…COMMIT); others are ignored
    b. META: the last logged META is the newest committed meta.
             NextPageID = max(logged, on disk). pager.SetMeta.
             (metaDamaged and no META in the log → ErrUnrecoverable)
    c. PAGES: for each page that appears in a committed PAGE record:
             read it; if its checksum fails (torn) → start from an all-zero page
             for each of its records, in LSN order:
                 if record.LSN > pageLSN → Apply (copy the runs, pageLSN = record.LSN)
             write the page if anything changed
    d. pager.Sync   pages + meta page durable, fsync
 4. build the buffer pool and the B+ tree on the recovered files
```

**Why redo alone is correct (no undo, no analysis pass):**

- **No uncommitted change is ever on disk.** A mini-transaction keeps its pages pinned until
  its COMMIT is durable (no-steal, §3.5). So the data file can be *behind* the log but never
  *ahead* of it, and there is nothing to undo.
- **Uncommitted transactions are skipped.** `Transactions()` returns only transactions with a
  COMMIT record. A transaction cut off by a crash, or one written without COMMIT, is ignored
  even though its BEGIN/PAGE records are still in the file (`TestUncommittedTxnIsNotApplied`
  writes one by hand that would wipe the root page).
- **The pageLSN check makes redo exact.** The pool writes whole pages, and only after the log
  is durable up to their pageLSN (the WAL rule). So a page on disk with pageLSN = L contains
  exactly the effects of its records with LSN ≤ L. Applying only the later records, in order,
  reproduces the page byte for byte.
  - Each PAGE record is a diff against the page *as it was just before that record*. That is
    why the order and the skip rule both matter.
- **Torn pages are rebuilt from zero.** The log is never truncated (§3.8), so a page's first
  record in the log is the init record that created it, and the whole history is there.
  Replaying from an all-zero page rebuilds it no matter what the torn write left
  (`TestTornDataPageIsRebuilt`).
  - *Week 5:* the log is now truncated at checkpoints. The page's first record in the log is
    then the **full page image** logged on its first change after the checkpoint (§5.2).
    The rule "start from zero, replay from the first init record" stays the same.
  - If the history doesn't start with an init record, recovery returns `ErrUnrecoverable`
    instead of guessing.
- **A torn meta page is rebuilt from the log.** The last META record has every field. The
  pager is opened without the damaged meta, and `SetMeta` restores it
  (`TestTornMetaPageIsRestored`). If the log has no META record either, `Open` fails with
  `ErrUnrecoverable` (`TestUnrecoverableIsReported`).
- **Recovery is idempotent.** It only writes pages to their correct state and ends with a
  sync. A crash during recovery just means it runs again, and the second run finds nothing to
  do (`TestRecoveryIsIdempotent` crashes right after recovery twice).

**Statistics** (`db.RecoveryStats()`): committed and uncommitted transactions, records applied
and skipped, pages written, torn pages, whether the meta page was restored, and what was cut
from the log tail. After a clean `Close` everything is skipped (`TestPutCloseOpenGet` checks
that `RecordsApplied` = 0).

### 4.4 Engine: one safe entry point

```go
db, err := engine.Open(dir, engine.Options{})   // recovery runs here
err = db.Put(key, value)                         // durable on return
v, ok, err := db.Get(key)
found, err := db.Delete(key)
it := db.Scan(start, end)
err = db.Close()
```

> **Week 7:** `db.Put`/`db.Delete` are now one-operation transactions (they respect other
> transactions' locks), `db.Get`/`db.Scan` are lock-free read-committed reads, and
> `db.Begin()` starts a multi-operation transaction (§7).

- **The startup order is fixed in one place:** log → data file → recovery → buffer pool and
  tree. Before week 4, every test wired these up by hand. Getting the order wrong (e.g.
  building the pool before recovery) would silently cache stale pages.
- **`Close` order:** `FlushAll` (dirty pages, each after its log records, then the meta page,
  then fsync) → close the log → close the data file. This fixes the gap noted in week 3, where
  `pager.Close()` alone could write a meta page pointing at pages that weren't on disk yet.
  - Through the engine that can't happen, and recovery would fix it anyway.
- Operations after `Close` return `ErrClosed`.
- `Options.PoolFrames` (default 256 = 1 MiB) and `Options.NoSync` (tests only).

### 4.5 Durability tests

| Test | Scenario |
|---|---|
| `TestPutCloseOpenGet` | Put → Close → Open → Get, with real fsync. Nothing to redo after a clean close |
| `TestDeleteAndUpdateSurviveRestart` | 1,000 puts, a third deleted, a third updated → Close → Open: each key in the right state |
| `TestManySessions` | 12 sessions of 500 random operations each, alternating clean `Close` and crash, compared against a model at the start of each session |
| `TestCrashRecoveryRedoesPages` | 3,000 operations, crash with dirty pages in the pool → all data back, recovery reports work done |
| `TestRecoveryIsIdempotent` | crash, recover, crash, recover: the second recovery does nothing |
| `TestTornCommitRecord` | crash in the middle of a COMMIT record: that transaction is lost, the one before survives, new writes continue |
| `TestUncommittedTxnIsNotApplied` | a hand-written transaction without COMMIT is ignored |
| `TestTornDataPageIsRebuilt`, `TestTornMetaPageIsRestored`, `TestUnrecoverableIsReported` | §4.3 |

A "crash" in these tests is `db.crash()`. It closes both files without writing anything
(`pager.Abandon`, `wal.Abandon`), so everything that was only in the buffer pool is lost, just
like when the process dies.

### 4.6 Controlled crash tests: every intermediate state of one operation

`TestCrashMatrix` takes one operation, saves the files before and after it, and **builds every
on-disk state a crash during that operation can leave**:

| Family | Data file | Log | Expected after recovery |
|---|---|---|---|
| A: commit not durable | unchanged (no-steal) | cut at every record boundary (−1, 0, +1, +2 bytes) and every 97 bytes inside the transaction | the operation is **not** visible |
| B: commit durable | **every subset** of the changed pages written (meta page included) | complete | the operation **is** visible |
| C: torn write | one changed page half new / half old, the rest all old or all new | complete | visible; the torn page is rebuilt |

Family B includes "meta page new, B+ tree pages old" and the reverse. After a power failure,
writes since the last fsync can survive in any combination, so recovery must handle all of
them.

| Operation | Changed pages | Crash states |
|---|---|---|
| root split (meta change) | meta, old leaf, new leaf, new root | 109 |
| leaf split under an existing root | meta, parent, old leaf, new leaf | 105 |
| slot update, same size (in place) | 1 leaf | 17 |
| slot update with in-page compaction | 1 leaf | 24 |
| delete (slot removal) | 1 leaf | 18 |

All 273 states recover to exactly "done" or "not done", and every recovered tree passes
`Verify`.

**The tests can fail:** with recovery's page writes disabled on purpose, all five scenarios
and `TestManySessions` fail immediately, with messages like "page 3: unexpected page type 0"
or a wrong value.

### 4.7 Real crash test: killing the process

`TestKillDuringWrites` starts a child process (the test binary itself, running
`TestKillChild`):
- The child writes to the database with **real fsync**: inserts, updates and deletes.
- After each operation returns, it prints `ack <i>`.
- The parent kills it (`TerminateProcess`) after a random number of acks or at a random time.
  The random time can hit the child during its own recovery.

After each kill, the parent opens the database and checks:
- every acknowledged operation is there
- the one operation that may have been in flight is either **fully there or fully absent**
- the tree passes `Verify`

Five rounds run on the same files, and the parent alternates between closing cleanly and
crashing right after recovery. It ran 10 × 5 rounds without a failure.

What it covers: process crashes, where the OS keeps written data. Power loss (writes lost
after the last fsync, torn pages) is covered by the crash matrix (§4.6), which builds those
states directly.

### 4.8 Known limitations

1. ~~**Recovery reads the whole log.**~~ *Solved in week 5:* the log is truncated at every
   checkpoint, so recovery reads at most about `CheckpointBytes` of log (§5.2).
2. ~~**Empty pages are never reused.**~~ *Solved in week 5* (§5.3).
3. **One writer at a time and one fsync per commit** (§3.8).
4. ~~**Race detector.**~~ Not a limitation: it works when run from PowerShell or cmd, and the
   whole suite passes under it (§1.9).

---

## Week 5 — Embedded hardening: lock file, checkpoints, page reuse

**Goal:** finish the database as an **embedded** store first (client/server comes later).
This step fixes the three problems that would hurt a real user:

1. two processes opening the same directory
2. a log that grows forever
3. a data file that never shrinks

Student-project level throughout: simple, correct, tested.

### Package layout

```
internal/lockfile/  Acquire/Release: OS-level exclusive lock on dir/LOCK
                    lockfile_windows.go (CreateFile, share mode 0), lockfile_unix.go (flock)
internal/btree/delete.go   delete that frees emptied pages
```

Changed: `engine` (lock, Checkpoint, auto-checkpoint), `wal` (start LSN, Reset), `mtr`
(free list allocation, Free, full page images, Abort gives pages back), `buffer`
(`DiscardNewPage`), `pager` (`UnallocatePage`, `ReplaceLogFile`), `btree/verify.go`
(free list and leak check).

### 5.1 Lock file

`engine.Open` takes an exclusive lock on `dir/LOCK` before touching anything. A second
`Open` of the same directory (from another process, or from the same process) fails with
`ErrLocked`. `Close` releases the lock.

- **Why an OS lock and not a "PID file":** the lock belongs to an open file handle, so the
  **OS releases it when the process dies**. A crash never leaves a stale lock that someone
  has to delete by hand.
- **Windows:** `CreateFile` with share mode 0, so no other handle can open the file.
  **Unix:** `flock(LOCK_EX | LOCK_NB)`. Standard library only (`syscall`), no dependencies.
  Both builds compile (`GOOS=linux/darwin go vet`).
- Tests:
  - `TestLockAcrossProcesses`: a child process holds the lock, so the parent gets
    `ErrLocked`. The child is killed, and then the parent gets the lock.
  - `TestSecondOpenIsLocked`: the lock is released by `Close` and by a crash.

### 5.2 Checkpoints and log truncation

```
db.Checkpoint():
 1. exclusive lock: no Put/Delete in progress
 2. pool.FlushAll: every dirty page (WAL rule first), then meta page, fsync data file
 3. log.Reset(meta): atomically replace the log with
        [header: startLSN = old end LSN][BEGIN, META(meta), COMMIT]
```

**When it runs:**
- automatically after a Put/Delete once the log reaches `Options.CheckpointBytes` (default
  4 MiB)
- at `Open` if recovery left a log above that size
- in `Close`
- manually with `db.Checkpoint()`

**No checkpoint storm.** With many concurrent writers, every commit that ends with the log
over the limit asks for a checkpoint. Before the fix each one ran: they queued on the
exclusive lock and each flushed the pool and replaced a log the first one had already
emptied (64 commits at once → 64 checkpoints). `maybeCheckpoint` now:
1. lets **one automatic checkpoint wait or run at a time** (an atomic flag); the other
   commits return at once, because that one truncates the log for them;
2. **checks the size again under the exclusive lock**, where no write can change it. A
   checkpoint that ran while this one waited (another automatic one, or a manual
   `Checkpoint`) has already done the work, so it does nothing.

A commit that skips the checkpoint is still durable: the checkpoint only shortens the log.
Tests: `TestNoCheckpointStorm` (64 commits over the limit at once → exactly 1 checkpoint)
and `TestCheckpointsUnderConcurrentWriters` (32 writers: at most one checkpoint per
`CheckpointBytes` of log, all data there after a crash). With the old code they fail
with 64 checkpoints, and 383 checkpoints where 114 at most were earned.

**Design decisions:**

| Decision | Why |
|---|---|
| **Sharp checkpoint** (no writes during it) | A fuzzy checkpoint needs a dirty-page table and recovery that starts from it. Pausing writes for one `FlushAll` is simple and correct. With the default size it happens every 4 MiB of log |
| **New log continues the LSN sequence** (`startLSN` in the header) | Every page on disk has pageLSN < the old end. Recovery's "apply if record LSN > pageLSN" stays correct only if new LSNs are larger. Restarting at 0 would make redo skip real changes |
| **Replace the whole file atomically** (temp + fsync + rename + dir fsync, in `pager`) | A crash leaves either the old log or the new one. The old one is harmless: recovery replays it onto pages that are already up to date and applies nothing (`TestCrashDuringCheckpoint`). A leftover `.tmp` is deleted at `Open`. No segment files needed |
| **The new log starts with a META transaction** | Recovery can rebuild a torn meta page only from a META record. Without this, a log after a checkpoint might have none |
| **Full page image on the first change after a checkpoint** | See below |

**Full page images (FPI).** Recovery rebuilds a torn page by starting from zeros and
replaying from the page's first *init* record (§4.3). Before week 5 that was always the
record that created the page, because the log was never truncated. After a truncation, a
page's creation is gone from the log.

So `mtr.Commit` now logs a page as a full image when **its before-image's pageLSN is below
the log's start LSN**, meaning it hasn't changed since the last checkpoint. Later changes
are byte diffs again.
- Diffs against zero cost only the non-zero bytes, so a mostly empty page is a small image.
- Cost: one full image per changed page per checkpoint interval. That is the same trade-off
  as Postgres's `full_page_writes`.
- `TestFullPageImageAfterCheckpoint` (mtr) checks first change = image, second = diff.
- `TestTornDataPageIsRebuilt` (engine) runs the whole story: checkpoint → change → page
  written → crash → torn page → rebuilt from the image.

**Bounds:**
- The log stays at about `CheckpointBytes` + one transaction. `TestCheckpointBoundsTheLog`
  uses 64 KiB and 6,000 operations, then crashes and recovers.
- After a clean `Close` the log is a few hundred bytes, and the next `Open` reads one META
  transaction (`TestCleanCloseLeavesAlmostNothing`).
- Recovery memory and time are bounded by the same size.

### 5.3 Page reuse: free list, freeing empty pages, no leaks

**Allocation moved into the mini-transaction.** `mtr.Create` first takes the head of the free
list (a logged page change plus a META change). Only if the list is empty does it grow the file
through `pool.NewPage` → `pager.AllocatePage`, as before. `mtr.Free(p)` formats p as a free
page pointing at the old head and makes it the new head.
- Both are logged as init records (small, because the pages are mostly zeros).
- The Mtr keeps a **working copy of the meta fields**, so a page freed earlier in the same
  operation can be reused later in it. The copy reaches the pager only after the commit is
  durable.
- `TestFreeAndReusePages` checks: freed in order a, b → reused b, a → then the file grows.

**Delete frees emptied pages** (`btree/delete.go`):

```
leaf becomes empty →
  1. its left neighbor (maybe under another parent: go up to the first ancestor where the
     path did not take child 0, then down the rightmost path) now points to its right neighbor
  2. free the leaf; remove its cell from the parent
       (if that was cell 0, the next cell becomes the −∞ cell)
  3. parent empty?  free it, remove it from its parent, repeat upward
  4. root left with one child?  that child becomes the root (repeat): the tree gets shorter
     root emptied?  the tree is empty (root = InvalidPageID)
```

- **Only empty pages are freed.** Leaves that still hold records are never merged or
  rebalanced (§2.4 still holds). That gives back all the space of a delete-heavy workload
  without the most bug-prone part of B+ trees.
- Removing child 0 and promoting the next cell to −∞ is safe. Its child's keys are ≥ the old
  separator ≥ the node's lower bound, so every key stays inside its new, wider range.

**Aborts give everything back.** If a mini-transaction is aborted:
- pages it took from the free list get their before-images back
- pages that grew the file are dropped without being written (`pool.DiscardNewPage`) and their
  IDs returned (`pager.UnallocatePage`)

This only needs "give back the newest ID": with one writer and IDs handed out in order, an
aborted operation's pages are always the newest. `TestAbortGivesPagesBack` checks it.

**`Verify` now checks for leaks.** The free list must hold only free pages, none of them also
in the tree, with no cycle. And **every allocated page (1 … NextPageID−1) must be either in
the tree or on the free list.** Every test that calls `Verify` now also proves nothing leaked,
including the 100,000-operation model test.

**Tests:**

| Test | Checks |
|---|---|
| `TestDeleteHeavyWorkloadDoesNotGrowFile` | 5 cycles of filling 3,000 keys and deleting them all: the file size is the same after every cycle |
| `TestModelWithRangeDeletes` | alternating fill/drain phases with range deletes, compared with a map, `Verify` every 500 steps; up to 136 pages on the free list at once |
| `TestShrinkTallTree` | a tree of height 4 emptied in random order shrinks 4 → 3 → 2 → 1 → 0, and all 550 pages end up on the free list |
| `TestScanAfterDelete` (updated) | deleting every key frees every page; refilling reuses them without growing the file |
| `TestFileStopsGrowingWithDeletes` (engine) | the same across restarts: the data file size is identical after each round |
| crash matrix (engine) | two new scenarios: **delete frees a leaf** (meta, parent, left neighbor, freed leaf) and **put reuses a free page**. All 7 scenarios now run on a checkpointed log, so their PAGE records are full images and the torn-page family is repaired from them. **545 crash states**, all recover to "done" or "not done" |

**The tests can fail.** With full page images turned off, the crash matrix and
`TestTornDataPageIsRebuilt` fail ("page 3 is damaged and its history does not start in the
log"). With the leaf-chain unlink removed, the delete tests fail in `Verify` ("leaf 35
rightSibling = 100, want 25").

### 5.4 Known limitations

1. **A checkpoint pauses all operations** while dirty pages are written (a sharp checkpoint).
   With a 1 MiB pool that is at most 256 page writes. A fuzzy checkpoint would avoid the
   pause.
2. **Underfull leaves stay underfull.** Only *empty* pages are reclaimed. A tree where every
   leaf keeps one record uses one page per record. Merging would fix that, at the cost of the
   complexity §2.4 avoids.
3. **The data file never gets shorter.** Freed pages are reused, so the file stops growing,
   but its size isn't returned to the OS. Truncating the file needs the free pages to be at
   the end, i.e. compaction.
4. **One writer, one fsync per commit** (§3.8).

---

## Week 6 — Terminal program

**Goal:** a database that runs in the terminal. `cmd/basedb` is a thin layer over
`engine`; the storage engine did not change, apart from a read-only `Stats()`.
How to build and use it is in the [README](../README.md).

### Package layout

```
cmd/basedb/
  main.go      flags, the three modes, Ctrl+C handling
  shell.go     the commands (one table), output formatting, friendly errors
  tokenize.go  command-line splitting: quotes and escapes; display/quote for output
```

> **Week 8 update:** the commands moved to `server.Session` and `tokenize.go` moved to
> `internal/protocol/quote.go`. `cmd/basedb` now only formats results, and it gained the
> server and client modes (§8.5). The transaction prompt is now `basedb(tx)>` (no write count:
> a remote client doesn't know it), and an open transaction at exit is reported as
> "open transaction rolled back, N writes discarded".

Added for it: `btree.Tree.Stats()` (shape of the tree and free list; it walks and
validates the tree like `Verify`) and `engine.DB.Stats()` (tree + page count + log size +
pool counters + recovery stats).

### 6.1 Three modes, one command layer

| Mode | When | Behavior |
|---|---|---|
| interactive | stdin is a terminal (or `-prompt`) | `basedb>` prompt; an error is printed and the session goes on |
| single command | arguments after the flags: `basedb -dir d get k` | runs one command, closes the database; exit code 0 or 1 |
| script | stdin is a file or pipe | no prompt; every line runs; exit code 1 if any line failed |

All three call the same `shell.exec(args)`, so a command behaves the same everywhere. The
tests drive the program through `run(args, stdin, stdout, stderr, terminal)`, which is the
whole program except the real OS handles.

**Commands:** `put`, `get`, `delete`, `scan [start] [end]`, `prefix`, `count`, plus the
admin commands `stats`, `verify` and `checkpoint`, and `help`, `exit`. They live in one
table: name, argument syntax, help text, argument count, function. That table drives
argument checking, the usage message and `help`.
- `prefix p` is `scan [p, next(p))`, where next(p) increments the last byte that isn't 0xff
  (`user:` → `user;`).

### 6.2 Keys and values on a command line

- **Splitting:** spaces separate arguments. Double quotes group, so spaces and empty values
  are possible. Inside quotes, `\" \\ \n \t \r \0 \xHH` work, so any byte value can be typed.
  An unterminated quote or an unknown escape is an error, not a guess.
- **Output uses the same notation.** Values are always printed quoted, and keys are printed
  as is when they are plain text. Printable UTF-8 (`Ayşe Yılmaz`, `İstanbul`) is shown as is;
  control characters and invalid UTF-8 bytes become `\xHH`.
  - `TestQuoteRoundTrip` checks with 500 random byte strings that whatever is printed reads
    back as the same bytes.

### 6.3 Clean exit and errors

- **Ctrl+C** (or SIGTERM): a goroutine catches the signal, calls `db.Close()` (checkpoint,
  so the next start has nothing to redo) and exits with code 130.
  - Even without this, no data would be lost: recovery replays the log.
  - `TestInterruptClosesCleanly` sends a simulated interrupt through `watchSignals`, then
    checks the database is closed, its data intact, and that reopening redoes nothing. A
    real console Ctrl+C can't be injected from the test environment on Windows.
- **`exit`, `quit`, end of input** (Ctrl+Z / Ctrl+D): `Close` the same way.
- **Errors the user can cause get plain messages** (`TestErrorsAreClear`):
  - unknown command (with a hint to use `help`), wrong number of arguments (with usage)
  - unterminated quote
  - key or value over the limit, with the actual size
  - "the database in … is already open in another basedb process" (the lock, §5.1)

### 6.4 Known limitations

1. **No command history or line editing** (up-arrow) at the prompt. That would need a
   terminal library. The standard library has none, and adding a dependency isn't worth it
   for a project terminal tool.
2. **Keys ≤ 256 bytes, values ≤ 1 KiB** (§1.4): there are no overflow pages yet.
3. ~~**No multi-command transactions.**~~ *Solved in week 7:* `begin` / `commit` /
   `rollback` (§7.6).

---

## Week 7 — Transactions: strict two-phase locking

**Goal:** complete ACID for groups of operations. Until week 6, each single `put`/`delete`
was atomic and durable, but there was no way to make several operations atomic together,
and nothing stopped concurrent users from seeing or overwriting each other's half-done work.
Week 7 adds **transactions with strict two-phase locking (2PL)**: serializable, phantom-free,
with deadlock detection.

### Package layout

```
internal/lockmgr/    the lock manager: modes, compatibility, FIFO queues, deadlock detection
internal/engine/tx.go   Tx: Begin, Get, GetForUpdate, Put, Delete, Scan, Commit, Rollback
internal/btree/      Apply(ops): many puts/deletes as one mini-transaction
cmd/basedb/          begin / commit / rollback
```

The storage stack below the B+ tree (`mtr`, `wal`, `recovery`, `buffer`, `pager`, `page`)
**did not change at all.** §7.3 explains why that was possible.

### 7.1 Strict 2PL

**Two-phase locking:** a transaction takes a lock before every read (shared) and every write
(exclusive), and **never takes a lock after it has released one**. This rule alone makes
every schedule *conflict-serializable*: the result equals some one-after-another order.

**Strict:** all locks are held until the transaction ends, and released only after the
commit is durable (or after the rollback). Without "strict", another transaction could read a
value written by a transaction that later aborts (a dirty read, and cascading aborts).

```
tx.Get(k)     IS(db) + S(k)    ─┐
tx.Put(k,v)   IX(db) + X(k)     │ growing phase: locks only taken
tx.Scan(a,b)  S(db)            ─┘
tx.Commit()   apply write set (one WAL transaction, fsync)  →  ReleaseAll
```

### 7.2 Lock manager (`lockmgr`)

**Two levels of locks (multi-granularity locking).** A key lock alone can't stop
**phantoms**: if transaction A scans `[a, b)` and B inserts a *new* key inside that range,
there was no key for A to lock. So there is also a lock on the whole database, with
intention modes:

|  | IS | IX | S | SIX | X |
|---|---|---|---|---|---|
| **IS** | ✓ | ✓ | ✓ | ✓ | – |
| **IX** | ✓ | ✓ | – | – | – |
| **S** | ✓ | – | ✓ | – | – |
| **SIX** | ✓ | – | – | – | – |
| **X** | – | – | – | – | – |

- Point readers take IS on the database and point writers IX. Many readers and writers of
  *different* keys run in parallel, and only key locks separate them.
- A **scan takes S on the database**. That conflicts with every writer's IX, so while A
  scans, no key can be inserted or deleted anywhere. A sees the same range twice
  (`TestNoPhantoms`).
- The price: a scanning transaction blocks all writers until it ends. That is coarser than
  next-key locking (locking only the scanned range), but much simpler, and still correct.
- **Lock upgrades** take the join of the two modes, e.g. a transaction that wrote (IX) and
  then scans (S) holds SIX.

**Waiting is first come, first served** per resource. A new request waits behind queued
ones even if it would fit next to the current holders (`TestFirstComeFirstServed`).
Otherwise a stream of readers could starve a waiting writer forever. One exception:
**upgrades go to the front of the queue**, because the requests queued behind may be
waiting for this very transaction.

**Deadlock detection, at the moment a transaction starts waiting.**
- The lock manager follows the **wait-for graph** from the new waiter: an edge T → U for
  every holder U whose mode conflicts with T's request, and for every conflicting request
  queued before T. If the path leads back to T, the request fails with `ErrDeadlock`, and T
  (the requester) is the victim.
- Checking only then is enough. Every new edge starts at the new waiter, or ends at it (when
  an upgrade jumps the queue), so every new cycle passes through it. A granted lock adds
  edges only to a running transaction, and a running transaction has no edge out.
- **Why detection and not wait-die / timeouts:** detection only aborts when there really is a
  cycle (`TestNoFalseDeadlock`). Wait-die also aborts on harmless waits, and timeouts are
  either slow or abort too early.
- Tests: two-transaction, three-transaction and upgrade deadlocks. A random stress test runs
  2,400 transactions over 8 goroutines and checks after **every grant** that no two
  transactions hold conflicting locks.

### 7.3 Deferred writes: why the storage stack didn't change

A transaction's writes go into its private **write set** (a map key → value/delete). They
reach the B+ tree only at **commit, all at once, as one mini-transaction**
(`btree.Apply`), which is one WAL transaction.

| Property | How it follows |
|---|---|
| **Atomicity** of a multi-key transaction | one WAL transaction: recovery applies all of it or none (§4.3). `TestCrashMatrix` "transaction of 40 writes": 17 changed pages, **575 crash states**, all "all or nothing". With more than 6 changed pages, 2^17 subsets would be too many, so family B tests a sample: none, all, each page alone, each page missing alone, and 64 random subsets. The whole matrix is now 1,120 states |
| **Rollback** | forget the write set and release the locks. Nothing to undo, because nothing reached a page |
| **Recovery stays redo-only** | an uncommitted transaction never touches a page or the log (`TestTxSurvivesCrash`: a crash with an open transaction leaves no trace) |
| **Reading your own writes** | `Get` checks the write set first. `Scan` merges the write set into the committed keys |

The alternative, writing in place during the transaction, would need **undo logging**,
because pages with uncommitted changes could reach disk. That means before-images in the log,
an undo pass in recovery, and compensation records. Deferred writes avoid all of it. The cost
is §7.7 item 2.

### 7.4 `btree.Apply` and the current root

`Apply(ops)` runs every op in one `mutate`. Within one mutation, the first ops can split the
root, so the later ops must see the *new* root.
- The mutation now carries the current root (`mutation.root`). `descend`, `put` (empty tree,
  root split) and `delete` use it instead of the committed `t.root`.
- The tree adopts it after the commit.
- `TestApplyIsOneTransaction` grows an empty tree to several levels and deletes a third of
  the keys in **one** batch: one WAL transaction. If `descend` reads `t.root` instead, it
  fails at once.
- `TestApplyIsAllOrNothing`: a batch that doesn't fit in the pool leaves no key, no log
  record and no leaked page.

### 7.5 Transactions and non-transactional calls together

| Call | Locks | Guarantee |
|---|---|---|
| `tx.Get`, `tx.Put`, `tx.Delete`, `tx.Scan` | S / X / database S, until the end | serializable |
| `db.Put`, `db.Delete` | **one-operation transaction** (IX + X) | waits for transactions that hold the key (`TestAutocommitWaitsForTransaction`) |
| `db.Get`, `db.Scan` | none | **read committed**: never sees uncommitted data (it is not in the tree), but two reads may differ if a commit happens in between |

`db.Put` *must* lock. Otherwise it could overwrite a key that a running transaction has read,
and the transaction's commit would then silently undo it (a lost update). Unlocked reads are
safe for the data, because transactions only write at commit. A user who needs repeatable
reads uses a transaction.

**`GetForUpdate`** (like SQL's `SELECT … FOR UPDATE`) reads and locks the key exclusively
right away. Read-then-write with plain `Get` + `Put` makes two transactions that read the same
key deadlock on the upgrade (S → X).

| Workload: 8 goroutines × 50 increments of one counter | Deadlock retries | Result |
|---|---|---|
| `Get` + `Put` | 1,432–1,769 | 400 ✓ |
| `GetForUpdate` + `Put` | **0** | 400 ✓ |

**Closing with open transactions:** `Close` first wakes every request waiting for a lock
(it fails with `ErrClosed`). That waiter holds no other engine lock, so `Close` can't hang
on it. Open transactions are simply dropped; they never wrote anything
(`TestCloseWithOpenTransactions`).

### 7.6 In the terminal program

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

- While a transaction is open, `put`, `get`, `delete`, `scan`, `prefix` and `count` go
  through it, and the prompt shows it.
- `rollback` discards the writes. `exit`, end of input or Ctrl+C with an open transaction
  rolls it back (and says so).
- `begin` is refused in single-command mode, where it would end immediately.
- `stats` shows the lock counters.

The terminal is one user, so it never waits for a lock. Locking pays off with concurrent
users: today goroutines in a Go program; later, the clients of the server.

### 7.7 Tests and their evidence

| Test | Shows |
|---|---|
| `TestNoLostUpdates` | concurrent read-modify-write of one counter: no increment lost |
| `TestBankTransfers` | 6 goroutines transfer money while an auditor sums all accounts in a transaction. Every audit sees the same total (250 audits) |
| `TestNoPhantoms` | an insert into a scanned range waits; the second scan sees the same keys |
| `TestDeadlockVictimIsRolledBack` | the victim gets `ErrDeadlock`, is rolled back, and its locks free the other transaction |
| `TestTxTooLarge` | over 1,000 keys, or a commit whose pages don't fit in the pool: `ErrTxTooLarge`, nothing applied |
| lockmgr tests | compatibility matrix, upgrades, FIFO, multi-granularity, 2- and 3-cycles, upgrade deadlock, no false deadlocks, Close wakes waiters, random stress |

**The tests can fail.** With plain reads taking no lock (one line changed), the isolation
tests fail exactly as the textbook predicts:
- **lost updates:** the counter ends at 51 instead of 400
- **inconsistent reads:** an audit sees 9,993 instead of 10,000
- **money created:** the final total is 11,614

### 7.8 Known limitations

1. **A scan locks the whole database.** That is correct and phantom-free, but it blocks all
   writers for the length of the scanning transaction. Next-key (range) locking would block
   only the scanned range.
2. **A transaction's commit must fit in the buffer pool at once.** The write set is capped
   at `MaxTxnWrites` = 1,000 keys, and a commit whose pages don't fit in the pool fails with
   `ErrTxTooLarge`. That is the price of deferred writes without undo logging (§7.3).
3. **One writer at a time underneath.** Transactions run in parallel up to their commit, but
   the commits themselves go through the tree's single writer lock. *Week 8:* group commit
   now shares one fsync between all commits waiting at that moment (§8.3).
4. ~~**No lock timeout.**~~ *Week 8:* the server disconnects a client that sends nothing for
   `IdleTimeout` (default 10 minutes), and its transaction and locks go with it (§8.4).
   Within one process, a transaction that is never ended still keeps its locks until `Close`.

---

## Week 8 — TCP server, client library, group commit

**Goal:** many clients using one database at the same time, over the network, and a
measurable answer to "what does a commit cost when many clients write at once?".

### Package layout

```
internal/protocol/   the wire format: Tokenize/Quote (moved from cmd/basedb), Response, Line
internal/server/     Session: the commands (moved from cmd/basedb); Server: TCP, one goroutine per client
client/              Go client library (public: other programs can import it)
internal/engine/commit.go   group commit
cmd/basedb/          the terminal program: local, server and client modes
cmd/basedb-bench/    load generator: throughput and latency, group commit on/off
bench/group_commit.csv      the measured results
```

Dependencies: `cmd → (server, client) → protocol`. `server → engine`. `client` doesn't
depend on the engine at all; it is only the wire format. The storage layers are unchanged,
and group commit lives entirely in `engine`.

### 8.1 The protocol

A **line-based text protocol**, like Redis' inline commands or memcached:

```
request:   put user:1 "Batuhan Deniz"\n          (the same syntax as the terminal program)

response:  ROW "key" "value"\n  ...              zero or more rows (scan, prefix, stats, help)
           OK [text]\n | VALUE "v"\n | NOT_FOUND\n | ERR <CODE> <message>\n    exactly one status line
```

| Decision | Why |
|---|---|
| **Text, not binary** | You can debug it by hand with `telnet` or `nc` and read it in a packet capture. Requests reuse the terminal program's syntax and quoting (`Tokenize`/`Quote`), so binary values work through `\xHH` escapes. The cost, a few escaped bytes and parsing, is nothing next to an fsync |
| **Rows, then exactly one status line** | The client always knows when a response ends, even for scans of any length, without a length prefix |
| **Keys and values always quoted on ROW/VALUE lines** | A value with a newline becomes `\n` and stays on its line, so a response line never needs special treatment |
| **Error codes** (`DEADLOCK`, `TXFAILED`, `TOOLARGE`, `BUSY`, …) | A program reacts to the code (`errors.Is(err, client.ErrDeadlock)` → retry). The message is for people |
| **Pipelining** | A client may send many requests without waiting. The server answers in order and flushes only when no request is buffered, so 1,000 pipelined requests cost a few network writes (`TestPipelining`) |
| `MaxLine` = 64 KiB | The largest legal request (a 256-byte key and a 1 KiB value, fully escaped) is far below it. A longer line is refused, and the connection closed, instead of buffering without limit |

### 8.2 One command layer for everyone

The commands moved from the terminal program into **`server.Session`**: one Session per client,
holding that client's open transaction. The TCP server gives each connection a Session, and
the terminal program uses a Session directly when it opens a database itself.

```
basedb -dir d           →  shell → Session → engine          (local)
basedb -connect a:p     →  shell → client → TCP → Server → Session → engine   (remote)
```

So local and remote can't behave differently. `TestLocalAndRemoteAreIdentical` runs the same
script both ways and compares the output byte for byte.

### 8.3 Group commit

**The problem:** a commit ends with an fsync of the log. This disk does about 1,000–1,200
fsyncs per second, so with one fsync per commit **the whole database is capped at about 1,150
writes/s, however many clients there are.** More clients only make the queue, and the
latency, longer.

**Group commit:** commits that arrive while an fsync is running share the *next* fsync.

```
committer A ──┐                ┌── A, B, C all durable after ONE fsync
committer B ──┼─► queue ─► leader: btree.Apply(A+B+C)  =  one mini-transaction, one WAL transaction
committer C ──┘                └── meanwhile D, E queue up → the next batch
```

- **How:** the first committer to find no leader becomes the **leader**. It takes everything
  queued, applies it as one `btree.Apply` (one WAL transaction, one fsync), wakes the
  committers in the batch, and repeats until the queue is empty. The batch size follows the
  load by itself (no timer, no tuning). The longer an fsync takes, the more commits wait for
  the next one.
- **Why merging transactions is correct:**
  1. Strict 2PL: every committer still holds X locks on all the keys it writes, so the
     transactions in a batch write **disjoint keys**, and their order inside the batch doesn't
     matter.
  2. One batch is one WAL transaction, so after a crash it is entirely there or entirely
     absent. That's stronger than needed, because no committer in the batch had been told
     "committed" yet. The crash tests run through this path.
  3. Each committer returns only after the batch's fsync, and releases its locks only after
     that. So **nothing is visible before it is durable**, as before.
- **This is commit batching at the transaction level, not at the log level.** The classic
  version (several COMMIT records, one fsync) would release the tree lock before the fsync.
  Readers that don't take locks could then see data that isn't durable yet. Batching whole
  transactions keeps the tree lock until the fsync, so that can't happen, and the WAL,
  mini-transaction and recovery code stay unchanged.
- **A batch too big for the buffer pool** (every page it touches stays pinned until the commit)
  is retried one transaction at a time. Batching can never make a commit fail that would
  succeed alone (`TestBatchTooBigFallsBack`). A batch is also capped at `PoolFrames/4` writes.
- `Options.NoGroupCommit` (flag `-group-commit=false`) turns it off, but only to measure the
  difference.

### 8.4 The server

- **One goroutine per connection.** All concurrency control is below it: the lock manager
  (isolation) and group commit (throughput).
- **A client that disconnects** in the middle of a transaction loses it. The Session rolls it
  back, and its locks are released at once (`TestDisconnectRollsBack`).
- **Idle timeout** (default 10 minutes, `-idle-timeout`): a client that sends nothing that long
  is disconnected, with the same rollback. An abandoned client can't hold locks forever
  (`TestIdleTimeout`). A write timeout does the same for a client that stops reading its
  responses.
- **Connection limit** (default 1,000, `-max-conns`): the next client gets `ERR BUSY` and is
  disconnected (`TestMaxConns`).
- **Shutdown** (Ctrl+C): stop accepting → disconnect every client (open transactions rolled
  back, lock waiters woken) → close the database cleanly (`TestCloseWithClients`,
  `TestServeMode`).
- **No authentication and no encryption.** That's why the example address is `127.0.0.1`.
  `0.0.0.0` opens the database to the whole network, and the flag's help says so.

### 8.5 The terminal program: kept, with three modes

```
basedb -dir ./mydb                          embedded, as before
basedb -dir ./mydb -serve 127.0.0.1:7070    the server
basedb -connect 127.0.0.1:7070              the same prompt and commands, over TCP
```

The CLI was not deleted. It became the server's front end (like `redis-server`/`redis-cli`,
or `postgres`/`psql`): one binary is the database, the server and the interactive client.
- The prompt shows where the commands go: `basedb>`, or `basedb@127.0.0.1:7070>`.
- The prompt shows whether a transaction is open: `basedb(tx)>`.

### 8.6 Benchmark: group commit off vs on

`go run ./cmd/basedb-bench -csv bench/group_commit.csv`:
- a fresh database per run, real fsync, a server on 127.0.0.1
- N clients, each putting distinct 100-byte values as fast as the server answers, for 2
  seconds per run

Measured again after the checkpoint storm fix (§5.2); the first measurement is in
`bench/group_commit_before.csv` and in the "before" column:

| clients | off: writes/s | off: p50 | **on: writes/s** | on: p50 | on: p99 | writes per fsync (on) | on, before the fix |
|---|---|---|---|---|---|---|---|
| 1 | 1,128 | 1.0 ms | 1,109 | 1.0 ms | 1.7 ms | 1.0 | 1,163 |
| 2 | 1,126 | 2.0 ms | 1,100 | 2.0 ms | 3.0 ms | 1.0 | 1,161 |
| 4 | 1,125 | 3.6 ms | 2,232 | 2.0 ms | 3.0 ms | 2.0 | 2,342 |
| 8 | 1,125 | 7.0 ms | 4,042 | 2.0 ms | 4.6 ms | 3.6 | 3,903 |
| 16 | 1,120 | 14.3 ms | 6,220 | 2.0 ms | 7.0 ms | 5.5 | 6,268 |
| 32 | 1,114 | 28.6 ms | 16,305 | 1.7 ms | 5.0 ms | 14.2 | 12,164 |
| 64 | 1,126 | 56.8 ms | **31,648** | 2.0 ms | 13.0 ms | **31.1** | 16,048 |

How to read it:
- **Without group commit the line is flat** at the disk's fsync rate, about 1,120 writes/s,
  whatever the number of clients. The median latency grows linearly, because clients just
  queue for the next fsync.
- **With group commit the fsync rate stays the same** (about 1,000 per second; it's the
  disk), but each fsync carries more writes. Up to 31 per fsync gives **28× the throughput at
  64 clients**, with the median latency staying around 2 ms.
- **Before the checkpoint storm fix it was only 14×.** At 64 clients the old code did 1,064
  commit fsyncs in 2 seconds instead of about 2,000: half the time, every writer was waiting
  for a queue of redundant checkpoints (§9.6).
- With 2 clients there is no gain. Each client's commit arrives while the other one's fsync is
  running, so they take turns instead of sharing. Sharing starts at about 4 clients.
- The numbers depend on the disk (fsync speed) and are from this machine. `bench/group_commit.csv`
  has them for graphs.

### 8.7 Tests

| Test | Shows |
|---|---|
| `TestRawProtocol` | the exact bytes a telnet user sees, including escapes, errors and `quit` |
| `TestManyClients` | 100 clients at once, 3,000 writes, the tree verified |
| `TestBankTransfersOverNetwork` | the week-7 isolation test across 8 connections: the total never changes |
| `TestDisconnectRollsBack`, `TestIdleTimeout` | a client that leaves or goes silent can't keep its locks |
| `TestDeadlockOverNetwork` | `ERR DEADLOCK` reaches the client, and the victim's transaction is gone |
| `TestPipelining`, `TestLineTooLong`, `TestMaxConns`, `TestCloseWithClients` | §8.1, §8.4 |
| `TestGroupCommitSharesFsyncs` | 16 writers with real fsync: 4.8 commits per fsync; group commit off: exactly 1 |
| `TestGroupCommitAfterCrash` | every acknowledged write of 16 concurrent writers survives a crash |
| `TestBatchTooBigFallsBack` | two transactions that don't fit in the pool together both commit |
| `TestLocalAndRemoteAreIdentical`, `TestServeMode`, … (cmd/basedb) | the three modes |

The whole suite passes under `go test -race ./...`.

**The tests can fail.** If a batch applies only its first transaction but reports success to
all of them, `TestGroupCommitSharesFsyncs` finds acknowledged keys missing, and
`TestGroupCommitAfterCrash` finds them missing after a crash.

### 8.8 Known limitations

1. **No authentication or encryption** (TLS). Bind to `127.0.0.1` or put it behind something
   that provides them.
2. **A scan is sent as one response.** The server builds the whole result in memory before
   sending it. That is fine for this project's data sizes; a large database would need
   streaming or a row limit.
3. **One goroutine per connection.** Simple, and Go goroutines are cheap. The 1,000-connection
   default limit keeps it bounded.
4. **Still one writer underneath** (§7.8): group commit shares the fsync, but batches are
   applied one after another.

---

## Week 9 — Comparison with other storage engines

**Goal:** put baseDB next to two production embedded stores, on the same machine and the
same workloads, and explain every difference from the design.

**Code:** `bench/compare/` (its own Go module, so baseDB keeps zero dependencies; method and
fairness rules in `bench/compare/README.md`). Results: `bench/compare1.txt`–`compare2.txt` (tables),
`bench/compare1.csv`–`compare2.csv` (every run); from §9.7 on, `compare3`.

### 9.1 The counterparts

| | baseDB | bbolt (etcd) | Pebble (CockroachDB) |
|---|---|---|---|
| Structure | B+ tree | B+ tree | LSM tree |
| Crash safety | WAL + redo recovery | copy-on-write pages + 2 meta pages, no log | WAL + immutable sorted files |
| Cache | own buffer pool (ARC) | OS page cache (mmap) | own block cache |
| Concurrent writers | many (2PL), group commit | one at a time (`Batch` groups) | many, commit pipeline |
| Isolation | serializable (2PL) | serializable (one writer) | snapshot |
| Space after deletes | reused (free list), file never shrinks | reused (free list), file never shrinks | returned by compaction |

bbolt is the closest design (same data structure, different way to survive a crash); Pebble
is the other big family (LSM).

### 9.2 Method

Every store fsyncs every commit; baseDB and Pebble get the same 64 MiB cache (bbolt uses the
OS cache); same keys (16 bytes), values (100 random bytes), operation order and machine; one
key per transaction outside the bulk loads; a new directory per run; median of 3 runs. One
trap found on the way: an update that writes the value already stored changes no bytes, so
baseDB logs nothing and skips the fsync (160,000 "updates"/s). Every update now writes a new
value, so baseDB pays like the others.

### 9.3 Results (100,000 keys, Windows 10, real fsync)

Second run, after the checkpoint storm fix (§5.2). The first run is in
`bench/compare1.txt`; its baseDB numbers are in the last column. §9.6 says which
differences come from the fix and which are noise.

| Workload | baseDB | bbolt | bbolt `Batch` | Pebble | baseDB, first run |
|---|---|---|---|---|---|
| bulk insert, sequential (keys/s) | 309,000 | 376,000 | 392,000 | **841,000** | 267,000 |
| bulk insert, random (keys/s) | 45,000 | 40,000 | 41,000 | **846,000** | 45,000 |
| point reads (ops/s) | **881,000** | 785,000 | 786,000 | 522,000 | 796,000 |
| scans of 10 keys (ops/s) | 246,000 | 723,000 | **724,000** | 353,000 | 208,000 |
| 95% reads / 5% updates (ops/s) | 22,400 | 10,600 | 1,460 | **22,600** | 22,000 |
| 50% reads / 50% updates (ops/s) | 2,240 | 1,050 | 150 | **2,250** | 2,210 |
| updates, 1 per transaction (ops/s) | **1,160** | 529 | 78 | 1,130 | 1,553 |
| 1 writer (inserts/s) | **1,154** | 538 | 78 | 1,137 | 1,169 |
| 8 writers | 3,479 | 544 | 618 | **4,561** | 3,467 |
| 64 writers | 34,899 (p99 3.0 ms) | 552 | 4,694 | **36,274** (p99 2.7 ms) | 14,746 (p99 26 ms) |
| on disk after loading | 17.6 MiB | 32.0 MiB | | **10.3 MiB** |
| after deleting the lower half | 17.6 MiB | 32.0 MiB | | **0.9 MiB** |
| after inserting 50,000 new keys | 20.5 MiB | 32.0 MiB | | **5.6 MiB** |

### 9.4 What the numbers say, and why

1. **One durable commit costs one fsync, and baseDB pays exactly one.** Single-key updates
   and single-writer inserts: baseDB ≈ Pebble ≈ 1,150/s, the disk's fsync rate. (The first
   run's 1,553 updates/s for baseDB was a fast spell of the disk: the A/B test in §9.6 gives
   1,100–1,160 for both the old and the new code.)
   bbolt is at half (539/s) because a copy-on-write commit syncs twice: the new pages, then
   the meta page that points to them. A log (baseDB, Pebble) needs one sync, the log append.
2. **Point reads: baseDB is the fastest of the three.** With the data in the cache, a lookup
   is a walk of 3 pages in the buffer pool. bbolt does the same walk through mmap but opens
   a read transaction for each Get. Pebble looks in the memtable and then in several sorted
   files (with bloom filters): the LSM read cost.
3. **Random inserts: Pebble is 18× faster than both B+ trees.** This is the textbook LSM
   trade-off. A B+ tree puts each key in its leaf, so a random insert touches a random page
   (and baseDB logs it); an LSM tree appends to a memtable and sorts later, in the
   background. The price is paid on reads (point 2) and in background compaction.
4. **Many writers: group commit puts baseDB level with Pebble.** bbolt `Update` stays at
   ~550/s at any writer count (one writer at a time, 2 fsyncs). bbolt `Batch` groups, but
   waits up to 10 ms to fill a batch, so it is slow with few writers. At 64 writers baseDB
   reaches 34,900/s (p99 3.0 ms) and Pebble 36,300/s (p99 2.7 ms). The first run had
   baseDB at 14,700/s with a p99 of 26 ms, and I explained the gap with Pebble's commit
   pipeline. That was wrong: the A/B test (§9.6) shows the gap was the checkpoint storm.
   Pebble is still ahead at 8 writers (4,561 vs 3,479), where its pipeline (write the next
   batch to the log while the previous one syncs) matters more than batch size.
5. **Scans: baseDB is the slowest.** The iterator copies keys and values out of the page
   and re-searches the tree for each batch of keys (§2, chosen so that no page stays pinned
   between calls). bbolt's cursor reads the mapped file directly with no copying.
6. **Space.**
   - baseDB uses less than bbolt after loading: bbolt grows its file in large steps (here
     to 32 MiB) for its memory map.
   - Pebble is smallest: sorted files are written full, and compaction drops deleted keys,
     so its directory shrinks after deletes (0.9 MiB). B+ trees (baseDB, bbolt) keep their
     file size and reuse freed pages instead (§5.3).
   - baseDB reuses the freed pages: deleting half and inserting as many keys grows the file
     by only 17%. That growth is the split rule, not a leak: a split divides a page in the
     middle, so increasing keys (like the refill here) leave every page half full, while
     random keys fill pages to ~69% on average (ln 2). The same effect shows in bulk loads:
     23.3 MiB for sequential keys against 17.6 MiB for random ones (1.32×, expected ≈1.39×).

### 9.5 Where baseDB could improve (measured, not done)

1. **Right-edge split:** when the new key goes to the end of the rightmost leaf, keep the
   left page full and start an empty right page (what many B-trees do). Sequential loads
   would fill pages to ~100% instead of 50%.
2. **Pipelined group commit:** let the next batch be applied while the previous one syncs
   (Pebble's design). After the checkpoint fix, this would help mostly at 4–16 writers,
   where batches are small.
3. **Cheaper scans:** return slices into the pinned page for a whole leaf at a time instead of
   copying and re-searching per batch.
4. **Readers wait for fsyncs:** the tree latch is held through the commit's fsync, so one
   writer cuts concurrent point reads from about 836,000/s to 4,600/s. Latch crabbing would
   fix it (measured with a temporary probe; not part of these benchmarks, which read with one
   thread and no writers).

### 9.6 Rerun after the checkpoint storm fix: what changed, and why

**Method.** A rerun on another day mixes the effect of a code change with the machine's mood
(disk, CPU frequency, background tasks). So besides rerunning everything, the fix was
measured directly: two binaries, one with the old `maybeCheckpoint` and one with the new,
run alternately (old, new, old, new) in one session. Results: `bench/checkpoint_ab.txt`.

**A/B: old vs new code, same session** (embedded = `bench/compare`, TCP = `cmd/basedb-bench`):

| | old, round 1 | new, round 1 | old, round 2 | new, round 2 |
|---|---|---|---|---|
| 1 writer, embedded | 1,155/s | 1,159/s | 1,106/s | 712/s ⚠ |
| 8 writers, embedded | 3,240/s | 3,463/s | 3,443/s | 3,111/s ⚠ |
| 32 writers, embedded (p99) | 12,436/s (18.6 ms) | 12,947/s (3.1 ms) | 12,152/s (19.0 ms) | 5,714/s ⚠ (7.5 ms) |
| 64 writers, embedded (p99) | 14,609/s (21 ms) | **31,503/s (3.0 ms)** | 14,790/s (22 ms) | 18,367/s ⚠ (12 ms) |
| 64 clients, TCP (p99) | 12,917/s (23 ms) | **32,276/s (3.6 ms)** | 12,040/s (32 ms) | 26,491/s ⚠ (5.0 ms) |
| commit fsyncs, 64 clients, 2 s | 897 | 2,081 | 880 | 1,756 |

⚠ The disk was slow during round 2 "new": a single writer did only 712 fsyncs/s, against
1,100–1,160 everywhere else, and a single writer cannot have a checkpoint storm. Even so, the
new code wins at 64 writers in that round too.

**What the fix changed:**
1. **64 writers: 2.2–2.5× more throughput, p99 down from 21–32 ms to 3–4 ms.** This is the
   storm in numbers. At 30,000 writes/s the log reaches 4 MiB every second or two. With the old
   code, every one of up to 64 commits that ended past the limit then ran its own
   checkpoint: pool flush, data file fsync, a new log file written, fsynced and renamed. All
   of that ran one after another under the exclusive lock, so every writer stood still. The
   commit fsync count shows it: 880–897 in 2 seconds before, about 2,000 after. The disk
   spent half its time on redundant checkpoints.
2. **32 writers: same throughput, p99 down 6×** (19 → 3 ms). Fewer commits pile up per
   storm, so the stall is shorter: rarer, but still the slowest 1% of commits.
3. **1 and 8 writers: no change.** With one writer, at most one commit can be past the limit
   at a time, so there was never a storm. With 8 the storm is at most 8 checkpoints deep and
   rare. The fix costs nothing here: one atomic compare-and-swap per checkpoint.
4. **Read and scan workloads: not affected.** They write nothing, so no checkpoints.

**What the rerun shows that is noise.** bbolt's code did not change, yet its scans went from
514,000 to 723,000/s (+41%) and its point reads +7%. baseDB's reads (+11%), scans (+18%) and
sequential bulk load (+16%) moved the same way, so these are the machine, not the fix. Rows
bounded by the CPU (reads, scans, bulk loads) vary by 10–40% between sessions; rows bounded
by fsync vary less, unless the disk has a slow spell. Two rules follow for the report:
compare stores **within one run**, never across runs; and measure a code change with an
interleaved A/B test, not with two runs on different days.

### 9.7 Six engines: baseDB, Pebble, SQLite, RocksDB, redb, WiredTiger (`compare4`)

bbolt was replaced by four engines that cover the main designs: RocksDB (the reference LSM
tree, C++), SQLite (B-tree, C), WiredTiger (MongoDB's B-tree with a log, C) and redb
(copy-on-write B-tree without a log, Rust). Setup, versions and fairness rules:
`bench/compare/README.md`. Every run is now saved as `bench/compareN.txt` / `.csv`, with the
date, machine, command and the exact version of every store in the file.

**Two measurement bugs, found on the way** (`compare3` is kept, marked as flawed):
1. **One value for all keys.** The values were random, but the same 100 bytes for every key,
   so compressing stores shrank them: RocksDB stored 100,000 keys in 1.6 MiB. Every key now
   gets its own random bytes. The space numbers of Pebble in `compare1`–`compare3` are
   affected too; baseDB's are not (it does not compress).
2. **Log files counted as data.** WiredTiger preallocates its logs (200 MiB of zeros) and
   Pebble keeps old logs to recycle (~10 MiB). After a clean Close no store's logs hold
   anything still needed, so `on disk` now leaves every store's log files out.

**WiredTiger on Windows with gcc.** It supports Windows only with MSVC. Six small patches
(`bench/compare/deps/patch_wiredtiger.py`) remove MSVC switches and intrinsics and keep its
Windows I/O code; the patched build ran every workload without an error.

**Results** (100,000 keys, fsync on every commit, 64 MiB cache each, median of 3 runs):

| Workload | baseDB | Pebble | RocksDB | SQLite | WiredTiger | redb |
|---|---|---|---|---|---|---|
| point reads (ops/s) | 644,000 | 376,000 | 477,000 | 319,000 | **795,000** | 507,000 |
| scans of 10 keys | 205,000 | 293,000 | 168,000 | 142,000 | **584,000** | 394,000 |
| bulk insert, random (keys/s) | 42,600 | **717,000** | 364,000 | 38,300 | 132,000 | 39,200 |
| bulk insert, sequential | 295,000 | 754,000 | **804,000** | 496,000 | 586,000 | 455,000 |
| updates, 1 per transaction | 1,135 | 1,158 | 1,131 | **1,696** | 1,175 | 903 |
| 1 writer (inserts/s) | 1,164 | 1,176 | 1,167 | 1,157 | **1,507** | 952 |
| 8 writers | 3,463 | 4,748 | **4,916** | 1,156 | 2,875 | 926 |
| 64 writers (p99) | 34,929 (4.2 ms) | **37,132** (2.9 ms) | 32,672 (12 ms) | 1,126 (118 ms) | 8,300 (45 ms) | 923 (260 ms) |
| on disk, loaded | 17.6 MiB | **10.9** | **10.9** | 13.4 | 13.0 | 32.1 |
| after deleting half | 17.6 | **5.5** | 11.3 | 13.4 | 13.0 | 32.1 |
| after refilling | 20.5 | **10.9** | 16.8 | 13.5 | 13.9 | 32.1 |

**What it says about baseDB:**
1. **Many writers: second of six.** Only the stores with group commit scale: Pebble 37,100,
   baseDB 34,900, RocksDB 32,700 writes/s at 64 writers. SQLite and redb allow one writer at a
   time and stay at their fsync rate (~1,100 and ~920/s) with p99 latencies of 118 and
   260 ms. WiredTiger groups log writes, but reaches only 8,300/s here.
2. **Point reads: second, after WiredTiger.** A cached lookup in baseDB is a walk of 3 pages
   in the buffer pool. WiredTiger is faster because its in-memory pages are not the disk
   format: it reads from structures built for lookups. The LSM trees (Pebble, RocksDB) pay
   for checking several levels; SQLite pays for its SQL layer (a prepared statement per
   lookup).
3. **One durable commit costs one fsync.** baseDB, Pebble, RocksDB, WiredTiger (updates):
   1,130–1,175/s, the disk. redb is about 20% slower: a copy-on-write commit writes new pages
   and then the header that points to them. SQLite (updates, 1,696/s) and WiredTiger (single
   inserts, 1,507/s) are *faster than the disk's usual fsync rate*. These measurements don't
   explain it. A plausible cause, to be tested: their log writes land in space that already
   exists (SQLite rewrites its WAL file from the start after each checkpoint; WiredTiger
   preallocates its log files), so the file size doesn't change and NTFS has less metadata
   to flush with each fsync. baseDB's log always grows, so every fsync also has to make the
   new file size durable. If that is the cause, preallocating baseDB's log would raise its
   commit rate.
4. **Random inserts: the B-trees are all alike, the LSM trees win.** baseDB (42,600/s), SQLite
   (38,300) and redb (39,200) are within 10% of each other; Pebble is 17× faster, RocksDB 9×.
   WiredTiger is the fastest B-tree (132,000) because it logs the *operation* (key and
   value), not the page: baseDB logs a byte diff or a full image of every page a transaction
   changes.
5. **Sequential inserts and space: the split rule costs baseDB.** baseDB is last in
   sequential inserts and uses the most space of the logging stores (17.6 MiB, against
   13.0–13.4 for SQLite and WiredTiger). SQLite spreads cells over neighbouring pages when it
   splits; WiredTiger rewrites pages full when it writes them out. baseDB splits in the
   middle, so increasing keys leave half-full pages (§9.4 item 6); fix: the right-edge split
   (§9.5 item 1).
6. **Space after deletes.** The B-trees (baseDB, SQLite, WiredTiger, redb) keep their file
   size and reuse freed pages: SQLite and WiredTiger fully, baseDB partly (the split rule
   again). Pebble compacts the deleted half away (5.5 MiB). RocksDB grows at first: a delete
   writes a tombstone, and compaction had not run yet.
7. **Scans: fourth.** WiredTiger and redb read the next keys from the page they are on;
   baseDB copies each leaf's records and searches the tree again for every leaf (§9.4
   item 5).

**New improvement ideas from this comparison** (not done):
1. **Preallocate the log** and reuse it after a checkpoint, then measure single-commit
   throughput with an A/B test (point 3 above).
2. **Log operations instead of page diffs** for leaf changes (logical redo, like WiredTiger):
   much less log per random insert. Recovery would then have to re-run operations on the
   tree, which is a bigger change to the recovery design (Weeks 3–4).
