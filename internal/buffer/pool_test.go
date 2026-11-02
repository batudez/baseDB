package buffer

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"baseDB/internal/page"
	"baseDB/internal/pager"
)

// memDisk is an in-memory Disk that also checks the write-ahead rule.
type memDisk struct {
	mu     sync.Mutex
	pages  map[page.PageID]page.Page
	next   page.PageID
	writes int
	log    *fakeLog
	walBug error // set when a page is written before its log record
}

func newMemDisk() *memDisk { return &memDisk{pages: map[page.PageID]page.Page{}, next: 1} }

func (d *memDisk) ReadPage(id page.PageID, dst *page.Page) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if id == 0 || id >= d.next {
		return pager.ErrPageOutOfRange
	}
	*dst = d.pages[id] // unwritten pages read as zero
	return nil
}

func (d *memDisk) WritePage(id page.PageID, src *page.Page) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.log != nil && d.log.flushedLSN() < src.LSN() {
		d.walBug = fmt.Errorf("page %d (LSN %d) written before log flushed", id, src.LSN())
	}
	d.pages[id] = *src
	d.writes++
	return nil
}

func (d *memDisk) AllocatePage() (page.PageID, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	id := d.next
	d.next++
	return id, nil
}

func (d *memDisk) UnallocatePage(id page.PageID) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if id+1 != d.next {
		return fmt.Errorf("unallocate %d, newest is %d", id, d.next-1)
	}
	d.next--
	return nil
}

func (d *memDisk) Sync() error { return nil }

type fakeLog struct {
	mu      sync.Mutex
	flushed page.LSN
}

func (l *fakeLog) FlushUntil(lsn page.LSN) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if lsn > l.flushed {
		l.flushed = lsn
	}
	return nil
}

func (l *fakeLog) flushedLSN() page.LSN {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.flushed
}

func openPager(t *testing.T) (*pager.Pager, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "data.db")
	pg, err := pager.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pg.Close() })
	return pg, path
}

func mustFetch(t *testing.T, b *Pool, id page.PageID) *Frame {
	t.Helper()
	f, err := b.Fetch(id)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func mustNew(t *testing.T, b *Pool) *Frame {
	t.Helper()
	f, err := b.NewPage()
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func mustUnpin(t *testing.T, b *Pool, id page.PageID, dirty bool) {
	t.Helper()
	if err := b.Unpin(id, dirty); err != nil {
		t.Fatal(err)
	}
}

// writeStamp stores a recognizable value derived from the page ID.
func writeStamp(f *Frame) {
	f.WLatch()
	f.Page().SetType(page.TypeLeaf)
	binary.LittleEndian.PutUint64(f.Page().Data()[page.HeaderSize:], uint64(f.PageID())*7919)
	f.WUnlatch()
}

func checkStamp(t *testing.T, f *Frame) {
	t.Helper()
	f.RLatch()
	defer f.RUnlatch()
	got := binary.LittleEndian.Uint64(f.Page().Data()[page.HeaderSize:])
	if want := uint64(f.PageID()) * 7919; got != want || f.Page().ID() != f.PageID() {
		t.Fatalf("page %d: stamp %d, want %d", f.PageID(), got, want)
	}
}

// TestScanResistance shows why the pool uses ARC: pages used repeatedly
// survive a long scan of pages used once. (Under LRU the scan would evict
// them.)
func TestScanResistance(t *testing.T) {
	disk := newMemDisk()
	disk.next = 1000 // pages 1..999 exist (read as zeros)
	b := New(disk, 4, nil)
	touch := func(id page.PageID) {
		mustFetch(t, b, id)
		mustUnpin(t, b, id, false)
	}

	for _, hot := range []page.PageID{1, 2} {
		touch(hot)
		touch(hot) // second use: the page becomes "frequent"
	}
	for id := page.PageID(100); id < 300; id++ {
		touch(id) // scan: each page used once
	}

	before := b.Stats()
	touch(1)
	touch(2)
	if after := b.Stats(); after.Misses != before.Misses {
		t.Fatalf("hot pages were evicted by the scan (misses %d -> %d)", before.Misses, after.Misses)
	}
}

// TestFlushIsNotAnAccess checks that FlushPage, which pins internally, does
// not promote a page from mru to mfu.
func TestFlushIsNotAnAccess(t *testing.T) {
	disk := newMemDisk()
	b := New(disk, 4, nil)
	f := mustNew(t, b)
	id := f.PageID()
	mustUnpin(t, b, id, true)

	if err := b.FlushPage(id); err != nil {
		t.Fatal(err)
	}
	arc := b.replacer.(*ArcReplacer)
	if e := arc.alive[b.pageTable[id]]; e.list != listMRU || !e.evictable {
		t.Fatalf("after flush: page in %v (evictable=%v), want evictable in mru", e.list, e.evictable)
	}

	mustFetch(t, b, id) // a real access does promote it
	if e := arc.alive[b.pageTable[id]]; e.list != listMFU {
		t.Fatalf("after fetch: page in %v, want mfu", e.list)
	}
	mustUnpin(t, b, id, false)
}

func TestNewPageAllocatesSequentialIDs(t *testing.T) {
	b := New(newMemDisk(), 4, nil)
	for want := page.PageID(1); want <= 3; want++ {
		f := mustNew(t, b)
		if f.PageID() != want || f.Page().ID() != want {
			t.Fatalf("NewPage id = %d, want %d", f.PageID(), want)
		}
		mustUnpin(t, b, want, false)
	}
}

func TestPinnedPageIsNotEvicted(t *testing.T) {
	b := New(newMemDisk(), 3, nil)
	pinned := mustNew(t, b)
	writeStamp(pinned)

	// Cycle many pages through the two remaining frames.
	for i := 0; i < 200; i++ {
		f := mustNew(t, b)
		writeStamp(f)
		mustUnpin(t, b, f.PageID(), true)
	}
	if again := mustFetch(t, b, pinned.PageID()); again != pinned {
		t.Fatal("pinned page moved to another frame")
	}
	checkStamp(t, pinned)

	// With every frame pinned, nothing can be loaded.
	mustFetch(t, b, 2)
	mustFetch(t, b, 3)
	if _, err := b.Fetch(4); !errors.Is(err, ErrNoFreeFrame) {
		t.Fatalf("err = %v, want ErrNoFreeFrame", err)
	}
	if _, err := b.NewPage(); !errors.Is(err, ErrNoFreeFrame) {
		t.Fatalf("err = %v, want ErrNoFreeFrame", err)
	}
	mustUnpin(t, b, 2, false)
	mustFetch(t, b, 4) // page 2 is evictable now
}

func TestPinRules(t *testing.T) {
	b := New(newMemDisk(), 2, nil)
	f := mustNew(t, b)
	id := f.PageID()
	mustFetch(t, b, id) // pinned twice, still one frame
	mustUnpin(t, b, id, false)
	mustUnpin(t, b, id, false)
	if err := b.Unpin(id, false); !errors.Is(err, ErrNotPinned) {
		t.Fatalf("err = %v, want ErrNotPinned", err)
	}
	if err := b.Unpin(99, false); !errors.Is(err, ErrPageNotResident) {
		t.Fatalf("err = %v, want ErrPageNotResident", err)
	}
	if _, err := b.Fetch(99); !errors.Is(err, pager.ErrPageOutOfRange) {
		t.Fatalf("err = %v, want ErrPageOutOfRange", err)
	}
}

func TestDirtyFlagSurvivesCleanUnpin(t *testing.T) {
	disk := newMemDisk()
	b := New(disk, 1, nil)
	f := mustNew(t, b)
	id := f.PageID()
	mustFetch(t, b, id)
	mustUnpin(t, b, id, true)
	mustUnpin(t, b, id, false) // must not clear the dirty flag
	mustNew(t, b)              // evicts the page
	if disk.writes != 1 {
		t.Fatalf("writes = %d, want 1", disk.writes)
	}
}

func TestFlushPage(t *testing.T) {
	disk := newMemDisk()
	b := New(disk, 4, nil)
	f := mustNew(t, b)
	writeStamp(f)
	mustUnpin(t, b, f.PageID(), true)

	if err := b.FlushPage(f.PageID()); err != nil {
		t.Fatal(err)
	}
	if err := b.FlushPage(f.PageID()); err != nil { // clean now: no second write
		t.Fatal(err)
	}
	if disk.writes != 1 {
		t.Fatalf("writes = %d, want 1", disk.writes)
	}
}

func TestWALRule(t *testing.T) {
	disk := newMemDisk()
	log := &fakeLog{}
	disk.log = log
	b := New(disk, 2, log)

	for i := 1; i <= 10; i++ {
		f := mustNew(t, b)
		f.WLatch()
		f.Page().SetLSN(page.LSN(i * 100))
		f.WUnlatch()
		mustUnpin(t, b, f.PageID(), true)
	}
	if err := b.FlushAll(); err != nil {
		t.Fatal(err)
	}
	if disk.walBug != nil {
		t.Fatal(disk.walBug)
	}
	if log.flushedLSN() != 1000 {
		t.Fatalf("log flushed to %d, want 1000", log.flushedLSN())
	}
}

// TestCyclingThroughThreeFrames pushes hundreds of pages through a 3-frame
// pool on a real file, in several access patterns.
func TestCyclingThroughThreeFrames(t *testing.T) {
	pg, _ := openPager(t)
	b := New(pg, 3, nil)
	const n = 500

	for i := 0; i < n; i++ {
		f := mustNew(t, b)
		writeStamp(f)
		mustUnpin(t, b, f.PageID(), true)
	}
	patterns := map[string]func(i int) page.PageID{
		"forward":  func(i int) page.PageID { return page.PageID(i + 1) },
		"backward": func(i int) page.PageID { return page.PageID(n - i) },
		"strided":  func(i int) page.PageID { return page.PageID(i*37%n + 1) },
	}
	for name, idAt := range patterns {
		for i := 0; i < n; i++ {
			f := mustFetch(t, b, idAt(i))
			checkStamp(t, f)
			mustUnpin(t, b, f.PageID(), false)
		}
		t.Logf("%s: %+v", name, b.Stats())
	}
	if s := b.Stats(); s.Evictions < 3*n {
		t.Fatalf("expected constant eviction, stats = %+v", s)
	}
}

// TestTenThousandPages is the week-1 pass criterion: write 10,000 pages
// through a small pool, then read them all back, both from the same pool and
// after reopening the file.
func TestTenThousandPages(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 40 MB")
	}
	const n, frames = 10000, 16
	pg, path := openPager(t)
	b := New(pg, frames, nil)

	for i := 0; i < n; i++ {
		f := mustNew(t, b)
		writeStamp(f)
		mustUnpin(t, b, f.PageID(), true)
	}
	for id := page.PageID(1); id <= n; id++ {
		f := mustFetch(t, b, id)
		checkStamp(t, f)
		mustUnpin(t, b, id, false)
	}
	if err := b.FlushAll(); err != nil {
		t.Fatal(err)
	}
	if err := pg.Close(); err != nil {
		t.Fatal(err)
	}

	pg2, err := pager.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer pg2.Close()
	if next := pg2.Meta().NextPageID; next != n+1 {
		t.Fatalf("nextPageID after reopen = %d, want %d", next, n+1)
	}
	b2 := New(pg2, frames, nil)
	for id := page.PageID(n); id >= 1; id-- {
		f := mustFetch(t, b2, id)
		checkStamp(t, f)
		mustUnpin(t, b2, id, false)
	}
}

// TestConcurrentCounters has goroutines increment counters stored in pages
// through a pool much smaller than the data, so pages constantly move
// between memory and disk. No increment may be lost.
func TestConcurrentCounters(t *testing.T) {
	pg, _ := openPager(t)
	const (
		numPages   = 32
		workers    = 8
		increments = 2000
	)
	b := New(pg, workers, nil) // each worker pins at most one page
	for i := 0; i < numPages; i++ {
		f := mustNew(t, b)
		mustUnpin(t, b, f.PageID(), true)
	}

	counter := func(p *page.Page) []byte { return p.Data()[page.HeaderSize : page.HeaderSize+8] }

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < increments; i++ {
				id := page.PageID((w*7+i*13)%numPages + 1)
				f, err := b.Fetch(id)
				if err != nil {
					errs <- err
					return
				}
				f.WLatch()
				c := counter(f.Page())
				binary.LittleEndian.PutUint64(c, binary.LittleEndian.Uint64(c)+1)
				f.WUnlatch()
				if err := b.Unpin(id, true); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	if err := b.FlushAll(); err != nil {
		t.Fatal(err)
	}
	// Read straight from disk, bypassing the pool.
	var total uint64
	for id := page.PageID(1); id <= numPages; id++ {
		var p page.Page
		if err := pg.ReadPage(id, &p); err != nil {
			t.Fatal(err)
		}
		total += binary.LittleEndian.Uint64(counter(&p))
	}
	if total != workers*increments {
		t.Fatalf("total = %d, want %d", total, workers*increments)
	}
}

func TestDiscardNewPage(t *testing.T) {
	disk := newMemDisk()
	b := New(disk, 2, nil)
	keep := mustNew(t, b)
	mustUnpin(t, b, keep.PageID(), true)
	f := mustNew(t, b)
	id := f.PageID()

	if err := b.DiscardNewPage(id); err != nil {
		t.Fatal(err)
	}
	if disk.next != id {
		t.Fatalf("page ID %d not given back (next = %d)", id, disk.next)
	}
	if _, err := b.Fetch(id); err == nil {
		t.Fatal("discarded page still readable")
	}
	// The frame is free again: two pages fit in the 2-frame pool without
	// evicting anything, and the ID is handed out again.
	again := mustNew(t, b)
	if again.PageID() != id || b.Stats().Evictions != 0 {
		t.Fatalf("new page %d (want %d), evictions %d", again.PageID(), id, b.Stats().Evictions)
	}
	if disk.writes != 0 {
		t.Fatal("discarded page was written")
	}
	// Only a page pinned exactly once (by its creator) can be discarded.
	mustFetch(t, b, id)
	if err := b.DiscardNewPage(id); err == nil {
		t.Fatal("discarded a page pinned twice")
	}
}
