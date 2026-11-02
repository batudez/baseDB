// Package buffer implements the buffer pool: a fixed set of in-memory frames
// that cache disk pages.
//
// Rules for callers (see docs/ARCHITECTURE.md, week 1):
//   - Every successful Fetch/NewPage must be matched by exactly one Unpin.
//     A pinned page is never evicted.
//   - Hold the frame's latch while reading (RLatch) or modifying (WLatch) the
//     page bytes, and only while the page is pinned.
//   - Lock order is frame latch -> pool mutex. The pool never waits for a
//     latch while holding its mutex, so callers may call into the pool while
//     holding a latch, except FlushPage on a page they have write-latched.
package buffer

import (
	"errors"
	"fmt"
	"sync"

	"baseDB/internal/page"
)

var (
	ErrNoFreeFrame     = errors.New("buffer: all frames are pinned")
	ErrPageNotResident = errors.New("buffer: page is not in the buffer pool")
	ErrNotPinned       = errors.New("buffer: page is not pinned")
)

// Disk is the storage the pool reads from and writes to (*pager.Pager).
type Disk interface {
	ReadPage(id page.PageID, dst *page.Page) error
	WritePage(id page.PageID, src *page.Page) error
	AllocatePage() (page.PageID, error)
	UnallocatePage(id page.PageID) error
	Sync() error
}

// LogFlusher enforces the write-ahead rule: before a dirty page is written,
// the log must be durable up to that page's LSN. Implemented by the WAL
// (week 3); until then the pool is created with a nil LogFlusher.
type LogFlusher interface {
	FlushUntil(lsn page.LSN) error
}

// Frame is one slot of the pool. A frame holds a page while it is pinned.
type Frame struct {
	latch sync.RWMutex
	page  page.Page

	// Protected by Pool.mu.
	id       page.PageID
	pinCount int
	dirty    bool
}

// Page returns the cached page. Access it only under the frame's latch.
func (f *Frame) Page() *page.Page { return &f.page }

// PageID returns the ID of the cached page. It is stable while the caller
// holds a pin.
func (f *Frame) PageID() page.PageID { return f.id }

func (f *Frame) RLatch()   { f.latch.RLock() }
func (f *Frame) RUnlatch() { f.latch.RUnlock() }
func (f *Frame) WLatch()   { f.latch.Lock() }
func (f *Frame) WUnlatch() { f.latch.Unlock() }

// Stats counts pool activity, for tests and benchmarks.
type Stats struct {
	Hits, Misses, Evictions, Writes uint64
}

// Pool caches pages of one Disk in a fixed number of frames.
type Pool struct {
	mu        sync.Mutex
	disk      Disk
	log       LogFlusher
	frames    []Frame
	pageTable map[page.PageID]int
	free      []int
	replacer  Replacer
	stats     Stats
}

// New creates a pool of numFrames frames over disk. log may be nil.
func New(disk Disk, numFrames int, log LogFlusher) *Pool {
	if numFrames <= 0 {
		panic("buffer: numFrames must be positive")
	}
	free := make([]int, numFrames)
	for i := range free {
		free[i] = numFrames - 1 - i // pop from the end -> frame 0 first
	}
	return &Pool{
		disk:      disk,
		log:       log,
		frames:    make([]Frame, numFrames),
		pageTable: make(map[page.PageID]int, numFrames),
		free:      free,
		replacer:  NewArcReplacer(numFrames),
	}
}

// Fetch returns the frame holding page id, reading it from disk if needed.
// The page is pinned; call Unpin when done.
func (b *Pool) Fetch(id page.PageID) (*Frame, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if fid, ok := b.pageTable[id]; ok {
		b.stats.Hits++
		b.pinLocked(fid, true)
		return &b.frames[fid], nil
	}
	b.stats.Misses++

	fid, err := b.victimLocked()
	if err != nil {
		return nil, err
	}
	f := &b.frames[fid]
	if err := b.disk.ReadPage(id, &f.page); err != nil {
		b.free = append(b.free, fid)
		return nil, err
	}
	b.installLocked(fid, id)
	return f, nil
}

// NewPage allocates a new page on disk and returns it pinned, initialized as
// an empty page of type TypeUnused (callers set the real type). The frame
// starts out dirty so the page reaches disk even if never modified.
func (b *Pool) NewPage() (*Frame, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Get a frame first so a failure doesn't waste a page ID.
	fid, err := b.victimLocked()
	if err != nil {
		return nil, err
	}
	id, err := b.disk.AllocatePage()
	if err != nil {
		b.free = append(b.free, fid)
		return nil, err
	}
	f := &b.frames[fid]
	f.page.Init(id, page.TypeUnused)
	b.installLocked(fid, id)
	f.dirty = true
	return f, nil
}

// DiscardNewPage undoes NewPage: the page must have been created by NewPage
// and be pinned exactly once (by the caller). Its frame is freed without
// writing anything, and its ID is given back to the disk. Used when the
// operation that created the page is aborted.
func (b *Pool) DiscardNewPage(id page.PageID) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	fid, ok := b.pageTable[id]
	if !ok {
		return fmt.Errorf("%w: page %d", ErrPageNotResident, id)
	}
	f := &b.frames[fid]
	if f.pinCount != 1 {
		return fmt.Errorf("buffer: discard page %d with pin count %d, want 1", id, f.pinCount)
	}
	f.pinCount, f.dirty = 0, false
	delete(b.pageTable, id)
	b.replacer.SetEvictable(fid, true)
	b.replacer.Remove(fid)
	b.free = append(b.free, fid)
	return b.disk.UnallocatePage(id)
}

// Unpin releases one pin on page id. dirty reports whether the caller
// modified the page; once dirty, a page stays dirty until written.
func (b *Pool) Unpin(id page.PageID, dirty bool) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	fid, ok := b.pageTable[id]
	if !ok {
		return fmt.Errorf("%w: page %d", ErrPageNotResident, id)
	}
	f := &b.frames[fid]
	if f.pinCount == 0 {
		return fmt.Errorf("%w: page %d", ErrNotPinned, id)
	}
	f.pinCount--
	if dirty {
		f.dirty = true
	}
	if f.pinCount == 0 {
		b.replacer.SetEvictable(fid, true)
	}
	return nil
}

// FlushPage writes page id to disk if it is resident and dirty. It does not
// sync. The caller must not hold the page's write latch.
func (b *Pool) FlushPage(id page.PageID) error {
	b.mu.Lock()
	fid, ok := b.pageTable[id]
	if !ok {
		b.mu.Unlock()
		return nil
	}
	// Keep the frame from being evicted while we work. Flushing is not a use
	// of the page, so it must not count as an access for the replacer.
	b.pinLocked(fid, false)
	b.mu.Unlock()
	f := &b.frames[fid]

	// Take a consistent snapshot under the read latch. The dirty flag is
	// cleared *before* copying: a writer that modifies the page after our
	// copy will set it again when it unpins, so no change is ever lost.
	f.RLatch()
	b.mu.Lock()
	wasDirty := f.dirty
	f.dirty = false
	b.mu.Unlock()
	var snapshot page.Page
	if wasDirty {
		snapshot = f.page
	}
	f.RUnlatch()

	var err error
	if wasDirty {
		err = b.writePage(id, &snapshot)
	}
	b.mu.Lock()
	if err != nil {
		f.dirty = true
	} else if wasDirty {
		b.stats.Writes++
	}
	b.mu.Unlock()

	if uerr := b.Unpin(id, false); err == nil {
		err = uerr
	}
	return err
}

// FlushAll writes every dirty resident page and syncs the disk, so all
// changes made so far are durable.
func (b *Pool) FlushAll() error {
	b.mu.Lock()
	ids := make([]page.PageID, 0, len(b.pageTable))
	for id := range b.pageTable {
		ids = append(ids, id)
	}
	b.mu.Unlock()

	for _, id := range ids {
		if err := b.FlushPage(id); err != nil {
			return err
		}
	}
	return b.disk.Sync()
}

// Stats returns a snapshot of the pool counters.
func (b *Pool) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.stats
}

// writePage enforces the WAL rule, then writes p.
func (b *Pool) writePage(id page.PageID, p *page.Page) error {
	if b.log != nil {
		if err := b.log.FlushUntil(p.LSN()); err != nil {
			return fmt.Errorf("buffer: flush log before page %d: %w", id, err)
		}
	}
	return b.disk.WritePage(id, p)
}

// victimLocked returns an empty frame, evicting a page if necessary.
func (b *Pool) victimLocked() (int, error) {
	if n := len(b.free); n > 0 {
		fid := b.free[n-1]
		b.free = b.free[:n-1]
		return fid, nil
	}

	fid, ok := b.replacer.Evict()
	if !ok {
		return 0, ErrNoFreeFrame
	}
	f := &b.frames[fid]
	// pinCount == 0, so nobody holds the latch: the page is safe to read.
	if f.dirty {
		if err := b.writePage(f.id, &f.page); err != nil {
			// The page stays cached (still dirty), so the replacer must track
			// it again. Evict already turned it into a ghost; re-recording the
			// access revives it. (Rare error path: the small ghost-hit
			// adjustment of ARC's target is acceptable here.)
			b.replacer.RecordAccess(fid, f.id)
			b.replacer.SetEvictable(fid, true)
			return 0, err
		}
		f.dirty = false
		b.stats.Writes++
	}
	delete(b.pageTable, f.id)
	b.stats.Evictions++
	return fid, nil
}

func (b *Pool) installLocked(fid int, id page.PageID) {
	f := &b.frames[fid]
	f.id = id
	f.pinCount = 0
	f.dirty = false
	b.pageTable[id] = fid
	b.pinLocked(fid, true)
}

// pinLocked adds a pin. access reports whether this is a real use of the
// page that the replacer should learn from. RecordAccess must come before
// SetEvictable, because ARC only knows a frame after its first access.
func (b *Pool) pinLocked(fid int, access bool) {
	f := &b.frames[fid]
	f.pinCount++
	if access {
		b.replacer.RecordAccess(fid, f.id)
	}
	b.replacer.SetEvictable(fid, false)
}
