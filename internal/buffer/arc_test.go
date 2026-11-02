package buffer

import (
	"math/rand"
	"testing"

	"baseDB/internal/page"
)

// load records a first access of page id in frame and unpins it.
func load(a *ArcReplacer, frame int, id page.PageID) {
	a.RecordAccess(frame, id)
	a.SetEvictable(frame, true)
}

func mustEvict(t *testing.T, a *ArcReplacer, want int) {
	t.Helper()
	got, ok := a.Evict()
	if !ok || got != want {
		t.Fatalf("Evict = %d, %v; want frame %d", got, ok, want)
	}
}

func listPages(a *ArcReplacer, l arcList) []page.PageID {
	var ids []page.PageID
	for e := a.listOf(l).Front(); e != nil; e = e.Next() {
		ids = append(ids, e.Value.(*arcEntry).pageID)
	}
	return ids
}

func TestArcEvictsOldestFromMRU(t *testing.T) {
	a := NewArcReplacer(3)
	for f := 0; f < 3; f++ {
		load(a, f, page.PageID(10+f))
	}
	if a.Size() != 3 {
		t.Fatalf("Size = %d, want 3", a.Size())
	}
	mustEvict(t, a, 0)
	mustEvict(t, a, 1)
	mustEvict(t, a, 2)
	if _, ok := a.Evict(); ok || a.Size() != 0 {
		t.Fatal("nothing should be evictable")
	}
	if got := listPages(a, listMRUGhost); len(got) != 3 || got[0] != 12 || got[2] != 10 {
		t.Fatalf("mruGhost = %v, want [12 11 10]", got)
	}
}

func TestArcFrequentPageSurvives(t *testing.T) {
	a := NewArcReplacer(3)
	for f := 0; f < 3; f++ {
		load(a, f, page.PageID(10+f))
	}
	a.RecordAccess(0, 10) // second access: page 10 moves to mfu

	mustEvict(t, a, 1) // once-used pages go first...
	mustEvict(t, a, 2)
	mustEvict(t, a, 0) // ...mfu only when mru has nothing evictable
	if got := listPages(a, listMFUGhost); len(got) != 1 || got[0] != 10 {
		t.Fatalf("mfuGhost = %v, want [10]", got)
	}
}

func TestArcGhostHitsAdaptTarget(t *testing.T) {
	a := NewArcReplacer(2)
	load(a, 0, 10)
	load(a, 1, 11)

	// Page 10 is evicted from mru, then needed again: recency was evicted
	// too early, so the mru target grows.
	mustEvict(t, a, 0)
	a.RecordAccess(0, 10)
	if a.mruTarget != 1 {
		t.Fatalf("after mruGhost hit target = %d, want 1", a.mruTarget)
	}
	if e := a.alive[0]; e.list != listMFU || e.evictable {
		t.Fatalf("revived page in %v (evictable=%v), want pinned in mfu", e.list, e.evictable)
	}

	// Page 10 (now frequent) is evicted from mfu and needed again: the
	// target shrinks back.
	a.SetEvictable(0, true)
	a.SetEvictable(1, false)
	mustEvict(t, a, 0)
	a.RecordAccess(0, 10)
	if a.mruTarget != 0 {
		t.Fatalf("after mfuGhost hit target = %d, want 0", a.mruTarget)
	}
	if len(a.ghosts) != 0 {
		t.Fatalf("ghosts = %v, want none", a.ghosts)
	}
}

func TestArcTargetAffectsVictim(t *testing.T) {
	a := NewArcReplacer(3)
	load(a, 0, 10)
	load(a, 1, 11)
	load(a, 2, 12)
	a.RecordAccess(2, 12) // page 12 in mfu, pages 10, 11 in mru
	a.mruTarget = 3       // mru below target: prefer evicting from mfu

	mustEvict(t, a, 2)
}

func TestArcEvictableAccounting(t *testing.T) {
	a := NewArcReplacer(4)
	a.SetEvictable(3, true) // unknown frame: ignored
	if a.Size() != 0 {
		t.Fatalf("Size = %d, want 0", a.Size())
	}

	a.RecordAccess(0, 10) // new entries start pinned
	a.RecordAccess(1, 11)
	if a.Size() != 0 {
		t.Fatalf("Size = %d, want 0", a.Size())
	}
	if _, ok := a.Evict(); ok {
		t.Fatal("evicted a pinned frame")
	}

	a.SetEvictable(1, true)
	a.SetEvictable(1, true) // idempotent
	if a.Size() != 1 {
		t.Fatalf("Size = %d, want 1", a.Size())
	}
	mustEvict(t, a, 1) // frame 0 is older but pinned: skipped
}

func TestArcRemove(t *testing.T) {
	a := NewArcReplacer(4)
	load(a, 0, 10)
	load(a, 1, 11)
	a.Remove(0)
	a.Remove(7) // unknown frame: ignored
	if a.Size() != 1 || len(a.ghosts) != 0 {
		t.Fatalf("Size = %d, ghosts = %d; want 1, 0", a.Size(), len(a.ghosts))
	}
	mustEvict(t, a, 1)

	a.RecordAccess(2, 12)
	defer func() {
		if recover() == nil {
			t.Fatal("removing a pinned frame should panic")
		}
	}()
	a.Remove(2)
}

// TestArcRandomInvariants drives the replacer the way the pool does and
// checks ARC's invariants after every step.
func TestArcRandomInvariants(t *testing.T) {
	const capacity, pages = 8, 60
	rng := rand.New(rand.NewSource(1))
	a := NewArcReplacer(capacity)

	frameOf := map[page.PageID]int{} // resident pages
	pageOf := map[int]page.PageID{}
	pinned := map[int]bool{}
	var free []int
	for f := 0; f < capacity; f++ {
		free = append(free, f)
	}

	for step := 0; step < 50000; step++ {
		// Skewed access pattern: a few hot pages and a long cold tail.
		id := page.PageID(rng.Intn(pages))
		if rng.Intn(2) == 0 {
			id = page.PageID(rng.Intn(5))
		}

		if f, ok := frameOf[id]; ok {
			a.RecordAccess(f, id)
		} else {
			var f int
			if len(free) > 0 {
				f, free = free[len(free)-1], free[:len(free)-1]
			} else {
				var ok bool
				if f, ok = a.Evict(); !ok {
					continue // everything pinned
				}
				if pinned[f] {
					t.Fatalf("step %d: evicted pinned frame %d", step, f)
				}
				delete(frameOf, pageOf[f])
			}
			frameOf[id], pageOf[f] = f, id
			a.RecordAccess(f, id)
		}
		// Keep a page pinned now and then, release it later.
		f := frameOf[id]
		pinned[f] = rng.Intn(10) == 0
		a.SetEvictable(f, !pinned[f])

		checkArcInvariants(t, step, a, frameOf, pinned)
	}
}

func checkArcInvariants(t *testing.T, step int, a *ArcReplacer, frameOf map[page.PageID]int, pinned map[int]bool) {
	t.Helper()
	t1, t2, b1, b2 := a.mru.Len(), a.mfu.Len(), a.mruGhost.Len(), a.mfuGhost.Len()
	switch {
	case t1+b1 > a.capacity:
		t.Fatalf("step %d: |mru|+|mruGhost| = %d > %d", step, t1+b1, a.capacity)
	case t1+t2+b1+b2 > 2*a.capacity:
		t.Fatalf("step %d: total = %d > %d", step, t1+t2+b1+b2, 2*a.capacity)
	case a.mruTarget < 0 || a.mruTarget > a.capacity:
		t.Fatalf("step %d: target %d out of [0, %d]", step, a.mruTarget, a.capacity)
	case t1+t2 != len(frameOf) || len(a.alive) != len(frameOf):
		t.Fatalf("step %d: %d live entries, %d resident pages", step, t1+t2, len(frameOf))
	case b1+b2 != len(a.ghosts):
		t.Fatalf("step %d: ghost lists %d, ghost map %d", step, b1+b2, len(a.ghosts))
	}
	evictable := 0
	for id, f := range frameOf {
		e, ok := a.alive[f]
		if !ok || e.pageID != id {
			t.Fatalf("step %d: frame %d (page %d) not tracked", step, f, id)
		}
		if _, ghost := a.ghosts[id]; ghost {
			t.Fatalf("step %d: page %d is both resident and a ghost", step, id)
		}
		if e.evictable == pinned[f] {
			t.Fatalf("step %d: frame %d evictable=%v, pinned=%v", step, f, e.evictable, pinned[f])
		}
		if e.evictable {
			evictable++
		}
	}
	if a.Size() != evictable {
		t.Fatalf("step %d: Size = %d, want %d", step, a.Size(), evictable)
	}
}
