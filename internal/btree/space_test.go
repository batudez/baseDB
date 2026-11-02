package btree

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"

	"baseDB/internal/page"
)

// Free space after deletes and updates. Deleting a record removes its slot
// from the slot directory and leaves the cell bytes as a hole; updates that
// grow a value leave the old cell as a hole. The holes are reclaimed by
// in-page compaction when an insert or update needs contiguous space. These
// tests check that the space really is reused: the leaf does not split.

func leafStats(t *testing.T, e *env) (height int, nextPage page.PageID) {
	t.Helper()
	return mustVerify(t, e.tree).height, e.pg.Meta().NextPageID
}

func TestDeletedSpaceIsReused(t *testing.T) {
	e := newEnv(t)
	v := bytes.Repeat([]byte{'x'}, 200)
	for i := 0; i < 15; i++ { // ~3.2 KiB: one nearly full leaf
		mustPut(t, e.tree, key(i), v)
	}
	height, next := leafStats(t, e)
	if height != 1 {
		t.Fatalf("setup: height %d, want a single leaf", height)
	}

	for round := 0; round < 50; round++ {
		// Delete 10 records (holes all over the cell area), insert 10 new
		// ones of the same size: they only fit after compaction.
		for i := 0; i < 10; i++ {
			if found, err := e.tree.Delete(key(round*100 + i)); err != nil || (round == 0 && !found) {
				t.Fatalf("Delete: %v, %v", found, err)
			}
		}
		for i := 0; i < 10; i++ {
			mustPut(t, e.tree, key((round+1)*100+i), v)
		}
		// Keep the 5 records that were never deleted in the key range of
		// the next round's deletes.
		for i := 10; i < 15; i++ {
			mustGet(t, e.tree, key(i), v)
		}
	}
	if h, n := leafStats(t, e); h != 1 || n != next {
		t.Fatalf("leaf split although the data always fit: height %d, pages %d -> %d", h, next, n)
	}
}

func TestUpdateChurnDoesNotSplit(t *testing.T) {
	e := newEnv(t)
	rng := rand.New(rand.NewSource(8))
	model := map[string][]byte{}
	for i := 0; i < 10; i++ {
		v := bytes.Repeat([]byte{'a'}, 100)
		mustPut(t, e.tree, key(i), v)
		model[string(key(i))] = v
	}
	_, next := leafStats(t, e)

	// 10 records of at most 300 bytes always fit in one page, but values
	// grow and shrink constantly, leaving holes behind.
	for step := 0; step < 5000; step++ {
		k := key(rng.Intn(10))
		v := bytes.Repeat([]byte{byte('a' + step%26)}, rng.Intn(300))
		mustPut(t, e.tree, k, v)
		model[string(k)] = v
	}
	for k, v := range model {
		mustGet(t, e.tree, []byte(k), v)
	}
	if h, n := leafStats(t, e); h != 1 || n != next {
		t.Fatalf("updates caused a split: height %d, pages %d -> %d", h, next, n)
	}
}

// TestDeleteRemovesSlot checks how a delete looks inside the leaf: the slot
// directory shrinks by one entry and stays sorted; no tombstone remains.
func TestDeleteRemovesSlot(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 5; i++ {
		mustPut(t, e.tree, key(i), []byte(fmt.Sprint(i)))
	}
	if _, err := e.tree.Delete(key(2)); err != nil {
		t.Fatal(err)
	}
	f, err := e.pool.Fetch(e.tree.root)
	must(t, err)
	defer e.pool.Unpin(f.PageID(), false)
	p := f.Page()
	if p.NumSlots() != 4 {
		t.Fatalf("leaf has %d slots, want 4", p.NumSlots())
	}
	for i, want := range []int{0, 1, 3, 4} {
		if !bytes.Equal(cellKey(p, i), key(want)) {
			t.Fatalf("slot %d holds %q, want %q", i, cellKey(p, i), key(want))
		}
	}
}

// TestDeleteHeavyWorkloadDoesNotGrowFile fills the tree and empties it again
// and again. Emptied leaves (and internal nodes) go to the free list and are
// reused, so after the first cycle the file does not grow any more.
func TestDeleteHeavyWorkloadDoesNotGrowFile(t *testing.T) {
	e := newEnv(t)
	const n = 3000
	var sizeAfterFirst page.PageID
	for cycle := 0; cycle < 5; cycle++ {
		for i := 0; i < n; i++ {
			mustPut(t, e.tree, key(i), value(i))
		}
		if st := mustVerify(t, e.tree); st.height < 2 || st.keys != n {
			t.Fatalf("cycle %d: after fill %+v", cycle, st)
		}
		for i := 0; i < n; i++ {
			if found, err := e.tree.Delete(key((i * 7) % n)); err != nil || !found {
				t.Fatalf("cycle %d: Delete: %v, %v", cycle, found, err)
			}
		}
		st := mustVerify(t, e.tree)
		if st.keys != 0 || st.height != 0 {
			t.Fatalf("cycle %d: after emptying %+v", cycle, st)
		}
		next := e.pg.Meta().NextPageID
		if cycle == 0 {
			sizeAfterFirst = next
		} else if next != sizeAfterFirst {
			t.Fatalf("cycle %d: file grew from %d to %d pages", cycle, sizeAfterFirst, next)
		}
	}
}

// TestModelWithRangeDeletes is a model test whose deletes remove whole key
// ranges, so leaves empty out, internal nodes lose children and the tree
// shrinks in height. Verify (including the free-list leak check) runs every
// 500 steps.
func TestModelWithRangeDeletes(t *testing.T) {
	e := newEnv(t)
	rng := rand.New(rand.NewSource(11))
	model := map[string][]byte{}
	maxFree, minHeight, maxHeight := 0, 99, 0
	for step := 1; step <= 20000; step++ {
		// Alternate phases: filling (mostly puts) and draining (range and
		// single deletes), so the tree grows and shrinks in height.
		draining := (step/2500)%2 == 1
		putPct, rangePct := 85, 0
		if draining {
			putPct, rangePct = 20, 10
		}
		switch r := rng.Intn(100); {
		case r < rangePct: // delete a whole range of up to 300 keys
			start, n := rng.Intn(5000), rng.Intn(300)
			for i := start; i < start+n; i++ {
				_, err := e.tree.Delete(key(i))
				must(t, err)
				delete(model, string(key(i)))
			}
		case r < rangePct+putPct:
			k := key(rng.Intn(5000))
			v := bytes.Repeat([]byte{byte(step)}, rng.Intn(400))
			mustPut(t, e.tree, k, v)
			model[string(k)] = v
		default:
			k := key(rng.Intn(5000))
			_, err := e.tree.Delete(k)
			must(t, err)
			delete(model, string(k))
		}
		if step%500 == 0 {
			st := mustVerify(t, e.tree)
			if st.keys != len(model) {
				t.Fatalf("step %d: %d keys, model %d", step, st.keys, len(model))
			}
			maxFree = max(maxFree, st.free)
			minHeight, maxHeight = min(minHeight, st.height), max(maxHeight, st.height)
		}
	}
	for k, v := range model {
		mustGet(t, e.tree, []byte(k), v)
	}
	if maxFree == 0 {
		t.Fatal("no page was ever freed: the test did not exercise page reuse")
	}
	t.Logf("max free pages %d, height between %d and %d, file %d pages",
		maxFree, minHeight, maxHeight, e.pg.Meta().NextPageID-1)
}

// TestShrinkTallTree empties a tree of height >= 3 in random order: internal
// nodes below the root lose all their children and are freed, and the root
// shrinks level by level until the tree is empty.
func TestShrinkTallTree(t *testing.T) {
	e := newEnv(t)
	bigKey := func(i int) []byte { return []byte(fmt.Sprintf("%06d%s", i, bytes.Repeat([]byte{'k'}, 200))) }
	const n = 2000
	for i := 0; i < n; i++ {
		mustPut(t, e.tree, bigKey(i), bytes.Repeat([]byte{byte(i)}, 300))
	}
	st := mustVerify(t, e.tree)
	if st.height < 3 {
		t.Fatalf("setup: height %d, want >= 3", st.height)
	}
	allocated := int(e.pg.Meta().NextPageID) - 1
	var heights []int // every distinct height, in the order seen
	lastHeight := st.height

	// With random order, leaves only start to empty near the end, so the
	// last 600 deletes are verified one by one.
	for j, i := range rand.New(rand.NewSource(12)).Perm(n) {
		if found, err := e.tree.Delete(bigKey(i)); err != nil || !found {
			t.Fatalf("Delete(%d): %v, %v", i, found, err)
		}
		if j%100 == 0 || j >= n-600 {
			st := mustVerify(t, e.tree)
			if st.keys != n-j-1 {
				t.Fatalf("after %d deletes: %d keys", j+1, st.keys)
			}
			if st.height > lastHeight {
				t.Fatalf("after %d deletes: height grew from %d to %d", j+1, lastHeight, st.height)
			}
			if st.height != lastHeight || len(heights) == 0 {
				heights = append(heights, st.height)
			}
			lastHeight = st.height
		}
	}
	st = mustVerify(t, e.tree)
	if st.height != 0 || st.free != allocated {
		t.Fatalf("emptied tree: %+v, want height 0 and all %d pages free", st, allocated)
	}
	if len(heights) < 3 {
		t.Fatalf("heights seen %v: the root never shrank step by step", heights)
	}
	t.Logf("heights while shrinking %v, %d pages all back on the free list", heights, allocated)
}

// TestApplyIsOneTransaction: a batch that grows an empty tree to several
// levels (root splits inside the batch) and deletes again commits as one
// WAL transaction, and the tree follows every root change inside it.
func TestApplyIsOneTransaction(t *testing.T) {
	e := newEnv(t)
	var ops []Op
	for i := 0; i < 600; i++ {
		ops = append(ops, Op{Key: key(i), Value: value(i)})
	}
	for i := 0; i < 600; i += 3 {
		ops = append(ops, Op{Key: key(i), Delete: true})
	}
	ops = append(ops, Op{Key: key(9999), Delete: true}) // missing key: no error
	before, err := e.log.Transactions()
	must(t, err)
	must(t, e.tree.Apply(ops))

	after, err := e.log.Transactions()
	must(t, err)
	if len(after) != len(before)+1 {
		t.Fatalf("batch logged %d transactions, want 1", len(after)-len(before))
	}
	st := mustVerify(t, e.tree)
	if st.keys != 400 || st.height < 2 {
		t.Fatalf("stats = %+v", st)
	}
	for i := 0; i < 600; i++ {
		if i%3 == 0 {
			mustGet(t, e.tree, key(i), nil)
		} else {
			mustGet(t, e.tree, key(i), value(i))
		}
	}
}

// TestApplyIsAllOrNothing: a batch too big for the pool fails in the middle
// and leaves no trace (no key, no log record, no leaked page).
func TestApplyIsAllOrNothing(t *testing.T) {
	e := newEnvFrames(t, 8)
	for i := 0; i < 50; i++ {
		mustPut(t, e.tree, key(i), value(i))
	}
	before := mustVerify(t, e.tree)
	logBefore, err := e.log.Transactions()
	must(t, err)

	var ops []Op
	for i := 1000; i < 3000; i++ {
		ops = append(ops, Op{Key: key(i), Value: bytes.Repeat([]byte{'x'}, 200)})
	}
	if err := e.tree.Apply(ops); err == nil {
		t.Fatal("a batch larger than the pool succeeded")
	}
	if after := mustVerify(t, e.tree); after != before {
		t.Fatalf("failed batch changed the tree: %+v -> %+v", before, after)
	}
	mustGet(t, e.tree, key(1000), nil)
	if logAfter, _ := e.log.Transactions(); len(logAfter) != len(logBefore) {
		t.Fatal("failed batch wrote to the log")
	}
	// Invalid input is rejected before anything happens.
	if err := e.tree.Apply([]Op{{Key: key(1), Value: []byte("ok")}, {Key: make([]byte, 300)}}); err == nil {
		t.Fatal("oversized key accepted")
	}
	mustGet(t, e.tree, key(1), value(1))
}
