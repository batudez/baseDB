package buffer

import (
	"container/list"
	"fmt"

	"baseDB/internal/page"
)

// ArcReplacer implements the Adaptive Replacement Cache (ARC) policy
// (Megiddo & Modha, 2003).
//
// ARC keeps four lists, each ordered most recent at the front:
//
//	mru       live pages accessed once recently            (T1, "recency")
//	mfu       live pages accessed at least twice           (T2, "frequency")
//	mruGhost  page IDs recently evicted from mru           (B1, no frame)
//	mfuGhost  page IDs recently evicted from mfu           (B2, no frame)
//
// The ghost lists remember only page IDs, never data. A miss on a page that
// is still in a ghost list shows the cache made the wrong choice, and ARC
// adapts the target size of mru (p, mruTarget) accordingly:
//
//	hit in mruGhost -> "recency was evicted too early"   -> grow p
//	hit in mfuGhost -> "frequency was evicted too early" -> shrink p
//
// Evict takes the oldest evictable page from mru while mru is at or above
// its target, otherwise from mfu. Pages seen only once (a large scan, a
// Verify pass) stay in mru and are evicted first, so they cannot push the
// frequently used pages out the way they would under LRU.
//
// Bounds (c = number of frames): |mru| + |mruGhost| <= c and the four lists
// together hold at most 2c entries.
//
// Not safe for concurrent use: the Pool calls it under its mutex.
type ArcReplacer struct {
	mru      *list.List
	mfu      *list.List
	mruGhost *list.List
	mfuGhost *list.List

	alive  map[int]*arcEntry         // frame -> entry in mru/mfu
	ghosts map[page.PageID]*arcEntry // page -> entry in mruGhost/mfuGhost

	size      int // evictable live frames
	mruTarget int // p, adapted on ghost hits, 0 <= p <= capacity
	capacity  int // c
}

type arcList uint8

const (
	listMRU arcList = iota
	listMFU
	listMRUGhost
	listMFUGhost
)

func (l arcList) String() string {
	return [...]string{"mru", "mfu", "mruGhost", "mfuGhost"}[l]
}

type arcEntry struct {
	pageID    page.PageID
	frame     int // -1 for ghosts
	evictable bool
	list      arcList
	elem      *list.Element
}

// NewArcReplacer returns a replacer for a pool of numFrames frames.
func NewArcReplacer(numFrames int) *ArcReplacer {
	return &ArcReplacer{
		mru:      list.New(),
		mfu:      list.New(),
		mruGhost: list.New(),
		mfuGhost: list.New(),
		alive:    make(map[int]*arcEntry),
		ghosts:   make(map[page.PageID]*arcEntry),
		capacity: numFrames,
	}
}

func (a *ArcReplacer) listOf(l arcList) *list.List {
	switch l {
	case listMRU:
		return a.mru
	case listMFU:
		return a.mfu
	case listMRUGhost:
		return a.mruGhost
	default:
		return a.mfuGhost
	}
}

// push puts e at the front of list l.
func (a *ArcReplacer) push(e *arcEntry, l arcList) {
	e.list = l
	e.elem = a.listOf(l).PushFront(e)
}

func (a *ArcReplacer) unlink(e *arcEntry) {
	a.listOf(e.list).Remove(e.elem)
	e.elem = nil
}

// Size returns the number of evictable frames.
func (a *ArcReplacer) Size() int { return a.size }

// SetEvictable marks whether a live frame may be evicted. Unknown frames are
// ignored.
func (a *ArcReplacer) SetEvictable(frame int, evictable bool) {
	e, ok := a.alive[frame]
	if !ok || e.evictable == evictable {
		return
	}
	e.evictable = evictable
	if evictable {
		a.size++
	} else {
		a.size--
	}
}

// RecordAccess is called when page id is accessed in frame. New entries
// start out non-evictable (the page is pinned).
func (a *ArcReplacer) RecordAccess(frame int, id page.PageID) {
	// Case 1: cache hit. Any repeated access makes the page "frequent".
	if e, ok := a.alive[frame]; ok {
		if e.pageID != id {
			panic(fmt.Sprintf("buffer: arc: frame %d holds page %d, accessed as page %d", frame, e.pageID, id))
		}
		a.unlink(e)
		a.push(e, listMFU)
		return
	}

	// Cases 2 and 3: ghost hit. The page was evicted recently: adapt the
	// target, then bring the page back as frequent.
	if g, ok := a.ghosts[id]; ok {
		b1, b2 := a.mruGhost.Len(), a.mfuGhost.Len()
		if g.list == listMRUGhost {
			delta := 1
			if b1 < b2 {
				delta = b2 / b1 // b1 >= 1: g is in mruGhost
			}
			a.mruTarget = min(a.mruTarget+delta, a.capacity)
		} else {
			delta := 1
			if b2 < b1 {
				delta = b1 / b2 // b2 >= 1: g is in mfuGhost
			}
			a.mruTarget = max(a.mruTarget-delta, 0)
		}
		a.unlink(g)
		delete(a.ghosts, id)
		a.addAlive(frame, id, listMFU)
		return
	}

	// Case 4: complete miss. Make room in the ghost lists, then add the page
	// to mru.
	if a.mru.Len()+a.mruGhost.Len() >= a.capacity {
		a.dropOldestGhost(a.mruGhost)
	} else if a.mru.Len()+a.mruGhost.Len()+a.mfu.Len()+a.mfuGhost.Len() >= 2*a.capacity {
		a.dropOldestGhost(a.mfuGhost)
	}
	a.addAlive(frame, id, listMRU)
}

func (a *ArcReplacer) addAlive(frame int, id page.PageID, l arcList) {
	e := &arcEntry{pageID: id, frame: frame}
	a.push(e, l)
	a.alive[frame] = e
}

func (a *ArcReplacer) dropOldestGhost(l *list.List) {
	if back := l.Back(); back != nil {
		g := l.Remove(back).(*arcEntry)
		delete(a.ghosts, g.pageID)
	}
}

// Evict chooses a victim: the oldest evictable page of mru while mru is at
// or above its target size, otherwise of mfu, falling back to the other list
// when the preferred one has no evictable page. The victim's page ID moves to
// the matching ghost list.
func (a *ArcReplacer) Evict() (int, bool) {
	if a.size == 0 {
		return -1, false
	}
	first, second := listMFU, listMRU
	if a.mru.Len() >= a.mruTarget {
		first, second = listMRU, listMFU
	}
	if frame, ok := a.evictFrom(first); ok {
		return frame, true
	}
	return a.evictFrom(second)
}

// evictFrom walks list l from its oldest end and evicts the first evictable
// entry, turning it into a ghost.
func (a *ArcReplacer) evictFrom(l arcList) (int, bool) {
	for elem := a.listOf(l).Back(); elem != nil; elem = elem.Prev() {
		e := elem.Value.(*arcEntry)
		if !e.evictable {
			continue
		}
		frame := e.frame
		a.unlink(e)
		delete(a.alive, frame)
		a.size--

		e.frame, e.evictable = -1, false
		if l == listMRU {
			a.push(e, listMRUGhost)
		} else {
			a.push(e, listMFUGhost)
		}
		a.ghosts[e.pageID] = e
		return frame, true
	}
	return -1, false
}

// Remove stops tracking an evictable frame without creating a ghost entry.
// Removing a pinned (non-evictable) frame is a caller bug and panics.
func (a *ArcReplacer) Remove(frame int) {
	e, ok := a.alive[frame]
	if !ok {
		return
	}
	if !e.evictable {
		panic(fmt.Sprintf("buffer: arc: removing non-evictable frame %d", frame))
	}
	a.unlink(e)
	delete(a.alive, frame)
	a.size--
}
