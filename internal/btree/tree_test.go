package btree

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	"baseDB/internal/buffer"
	"baseDB/internal/page"
	"baseDB/internal/pager"
	"baseDB/internal/wal"
)

type env struct {
	path    string
	walPath string
	pg      *pager.Pager
	log     *wal.Log
	pool    *buffer.Pool
	tree    *Tree
	frames  int
}

const testFrames = 32

func newEnv(t *testing.T) *env { return newEnvFrames(t, testFrames) }

// newEnvFrames opens a tree with a WAL. The log skips fsync: these tests
// check tree logic, and durability is tested in the wal and mtr packages.
func newEnvFrames(t *testing.T, frames int) *env {
	t.Helper()
	dir := t.TempDir()
	e := &env{path: filepath.Join(dir, "data.db"), walPath: filepath.Join(dir, "wal.log"), frames: frames}
	e.open(t)
	t.Cleanup(func() { e.log.Close(); e.pg.Close() })
	return e
}

func (e *env) open(t *testing.T) {
	t.Helper()
	pg, err := pager.Open(e.path)
	if err != nil {
		t.Fatal(err)
	}
	log, err := wal.OpenWithOptions(e.walPath, wal.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	e.pg, e.log = pg, log
	e.pool = buffer.New(pg, e.frames, log)
	e.tree = Open(e.pool, pg, log)
}

// reopen flushes everything, closes the files and opens them again.
func (e *env) reopen(t *testing.T) {
	t.Helper()
	if err := e.pool.FlushAll(); err != nil {
		t.Fatal(err)
	}
	if err := e.log.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.pg.Close(); err != nil {
		t.Fatal(err)
	}
	e.open(t)
}

func key(i int) []byte   { return []byte(fmt.Sprintf("key%06d", i)) }
func value(i int) []byte { return []byte(fmt.Sprintf("value-%d-%s", i, strings.Repeat("v", i%40))) }

func mustPut(t *testing.T, tr *Tree, k, v []byte) {
	t.Helper()
	if err := tr.Put(k, v); err != nil {
		t.Fatalf("Put(%q): %v", k, err)
	}
}

func mustGet(t *testing.T, tr *Tree, k, want []byte) {
	t.Helper()
	got, ok, err := tr.Get(k)
	if err != nil {
		t.Fatal(err)
	}
	if want == nil {
		if ok {
			t.Fatalf("Get(%q) = %q, want missing", k, got)
		}
		return
	}
	if !ok || !bytes.Equal(got, want) {
		t.Fatalf("Get(%q) = %q, %v; want %q", k, got, ok, want)
	}
}

func mustVerify(t *testing.T, tr *Tree) treeStats {
	t.Helper()
	st, err := tr.verify()
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func scanAll(t *testing.T, tr *Tree, start, end []byte) []entry {
	t.Helper()
	var out []entry
	it := tr.Scan(start, end)
	for it.Next() {
		out = append(out, entry{it.Key(), it.Value()})
	}
	if err := it.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestEmptyTree(t *testing.T) {
	e := newEnv(t)
	mustGet(t, e.tree, []byte("x"), nil)
	if found, err := e.tree.Delete([]byte("x")); found || err != nil {
		t.Fatalf("Delete = %v, %v", found, err)
	}
	if got := scanAll(t, e.tree, nil, nil); len(got) != 0 {
		t.Fatalf("scan of empty tree returned %d entries", len(got))
	}
	if st := mustVerify(t, e.tree); st.height != 0 {
		t.Fatalf("empty tree height = %d", st.height)
	}
}

func TestSingleNode(t *testing.T) {
	e := newEnv(t)
	for _, i := range []int{5, 1, 3, 2, 4} {
		mustPut(t, e.tree, key(i), value(i))
	}
	for i := 1; i <= 5; i++ {
		mustGet(t, e.tree, key(i), value(i))
	}
	mustGet(t, e.tree, key(6), nil)
	if st := mustVerify(t, e.tree); st.height != 1 || st.leaves != 1 || st.keys != 5 {
		t.Fatalf("stats = %+v", st)
	}
	got := scanAll(t, e.tree, nil, nil)
	for i, en := range got {
		if !bytes.Equal(en.key, key(i+1)) {
			t.Fatalf("scan[%d] = %q", i, en.key)
		}
	}
}

func TestEmptyKeyAndValue(t *testing.T) {
	e := newEnv(t)
	mustPut(t, e.tree, []byte{}, []byte("empty key"))
	mustPut(t, e.tree, []byte("k"), []byte{})
	for i := 0; i < 2000; i++ { // force splits with "" as smallest key
		mustPut(t, e.tree, key(i), value(i))
	}
	mustGet(t, e.tree, []byte{}, []byte("empty key"))
	mustGet(t, e.tree, []byte("k"), []byte{})
	mustVerify(t, e.tree)
}

func TestInsertOrders(t *testing.T) {
	const n = 5000
	orders := map[string][]int{
		"sorted":   make([]int, n),
		"reversed": make([]int, n),
		"random":   rand.New(rand.NewSource(7)).Perm(n),
	}
	for i := 0; i < n; i++ {
		orders["sorted"][i] = i
		orders["reversed"][i] = n - 1 - i
	}

	for name, order := range orders {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			for _, i := range order {
				mustPut(t, e.tree, key(i), value(i))
			}
			st := mustVerify(t, e.tree)
			if st.keys != n || st.height < 2 {
				t.Fatalf("stats = %+v", st)
			}
			for i := 0; i < n; i++ {
				mustGet(t, e.tree, key(i), value(i))
			}
			got := scanAll(t, e.tree, nil, nil)
			if len(got) != n {
				t.Fatalf("scan returned %d entries, want %d", len(got), n)
			}
			for i, en := range got {
				if !bytes.Equal(en.key, key(i)) || !bytes.Equal(en.value, value(i)) {
					t.Fatalf("scan[%d] = %q", i, en.key)
				}
			}
			t.Logf("%s: %+v", name, st)
		})
	}
}

// TestMultipleSplits uses large keys and values so that internal nodes split
// too and the tree grows to at least three levels.
func TestMultipleSplits(t *testing.T) {
	e := newEnv(t)
	bigKey := func(i int) []byte { return []byte(fmt.Sprintf("%06d%s", i, strings.Repeat("k", 200))) }
	bigValue := func(i int) []byte { return bytes.Repeat([]byte{byte(i)}, 300) }

	const n = 3000
	for _, i := range rand.New(rand.NewSource(3)).Perm(n) {
		mustPut(t, e.tree, bigKey(i), bigValue(i))
	}
	st := mustVerify(t, e.tree)
	if st.height < 3 {
		t.Fatalf("height = %d, want >= 3 (stats %+v)", st.height, st)
	}
	for i := 0; i < n; i++ {
		mustGet(t, e.tree, bigKey(i), bigValue(i))
	}
	t.Logf("%+v", st)
}

func TestMaxSizeRecords(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 300; i++ {
		k := append(key(i), bytes.Repeat([]byte{'k'}, page.MaxKeySize-len(key(i)))...)
		mustPut(t, e.tree, k, bytes.Repeat([]byte{byte(i)}, page.MaxValueSize))
	}
	mustVerify(t, e.tree)
}

func TestRootSplitCollectsPages(t *testing.T) {
	e := newEnv(t)
	var lastDirty []page.PageID
	e.tree.afterMutation = func(d []page.PageID) { lastDirty = d }

	mustPut(t, e.tree, key(0), value(0))
	if len(lastDirty) != 1 {
		t.Fatalf("first insert dirtied %v, want the new root leaf", lastDirty)
	}
	oldRoot := e.tree.root

	for i := 1; e.tree.root == oldRoot; i++ {
		mustPut(t, e.tree, key(i), value(i))
		if e.tree.root == oldRoot && len(lastDirty) != 1 {
			t.Fatalf("plain insert dirtied %v", lastDirty)
		}
	}
	// Root split: old leaf, new right leaf, new root.
	if len(lastDirty) != 3 || !slices.Contains(lastDirty, oldRoot) || !slices.Contains(lastDirty, e.tree.root) {
		t.Fatalf("root split dirtied %v (old root %d, new root %d)", lastDirty, oldRoot, e.tree.root)
	}
	if e.pg.Meta().RootPageID != e.tree.root {
		t.Fatal("meta root not updated")
	}
	mustVerify(t, e.tree)
}

func TestUpdates(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 500; i++ {
		mustPut(t, e.tree, key(i), []byte("small"))
	}
	// Grow every value: updates that no longer fit must split the leaf.
	for i := 0; i < 500; i++ {
		mustPut(t, e.tree, key(i), bytes.Repeat([]byte{byte(i)}, 1000))
	}
	st := mustVerify(t, e.tree)
	if st.keys != 500 {
		t.Fatalf("keys = %d, want 500 (updates must not duplicate)", st.keys)
	}
	// Shrink them again.
	for i := 0; i < 500; i++ {
		mustPut(t, e.tree, key(i), value(i))
	}
	for i := 0; i < 500; i++ {
		mustGet(t, e.tree, key(i), value(i))
	}
	mustVerify(t, e.tree)
}

func TestScanAfterDelete(t *testing.T) {
	e := newEnv(t)
	const n = 2000
	for i := 0; i < n; i++ {
		mustPut(t, e.tree, key(i), value(i))
	}
	for i := 0; i < n; i += 2 {
		if found, err := e.tree.Delete(key(i)); !found || err != nil {
			t.Fatalf("Delete(%d) = %v, %v", i, found, err)
		}
	}
	if found, _ := e.tree.Delete(key(0)); found {
		t.Fatal("second delete reported found")
	}

	got := scanAll(t, e.tree, key(100), key(500))
	if len(got) != 200 {
		t.Fatalf("scan returned %d entries, want 200", len(got))
	}
	for j, en := range got {
		if want := key(101 + 2*j); !bytes.Equal(en.key, want) {
			t.Fatalf("scan[%d] = %q, want %q", j, en.key, want)
		}
	}
	mustVerify(t, e.tree)

	// Delete everything: every leaf empties, so every page of the tree goes
	// to the free list and the tree becomes empty.
	allocated := int(e.pg.Meta().NextPageID) - 1
	for i := 1; i < n; i += 2 {
		e.tree.Delete(key(i))
	}
	if got := scanAll(t, e.tree, nil, nil); len(got) != 0 {
		t.Fatalf("scan after deleting all returned %d entries", len(got))
	}
	st := mustVerify(t, e.tree)
	if st.keys != 0 || st.height != 0 || st.free != allocated || e.tree.root != page.InvalidPageID {
		t.Fatalf("stats = %+v, root %d; want an empty tree with all %d pages free", st, e.tree.root, allocated)
	}
	// The tree is still usable, and new pages come from the free list.
	for i := 0; i < n; i++ {
		mustPut(t, e.tree, key(i), value(i))
	}
	mustGet(t, e.tree, key(7), value(7))
	if int(e.pg.Meta().NextPageID)-1 != allocated {
		t.Fatalf("refilling grew the file to %d pages instead of reusing the %d free ones",
			e.pg.Meta().NextPageID-1, allocated)
	}
	mustVerify(t, e.tree)
}

func TestScanBounds(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 1000; i += 10 {
		mustPut(t, e.tree, key(i), value(i))
	}
	count := func(start, end []byte) int { return len(scanAll(t, e.tree, start, end)) }

	cases := []struct {
		start, end []byte
		want       int
	}{
		{nil, nil, 100},
		{key(500), nil, 50},
		{nil, key(500), 50},          // end is exclusive
		{key(495), key(505), 1},      // bounds between keys
		{key(500), key(500), 0},      // empty range
		{key(600), key(500), 0},      // inverted range
		{[]byte("zzz"), nil, 0},      // past the last key
		{[]byte(""), []byte("a"), 0}, // before the first key
		{key(990), []byte("zzz"), 1}, // last key only
	}
	for _, c := range cases {
		if got := count(c.start, c.end); got != c.want {
			t.Errorf("Scan(%q, %q) = %d entries, want %d", c.start, c.end, got, c.want)
		}
	}
}

func TestLimits(t *testing.T) {
	e := newEnv(t)
	if err := e.tree.Put(make([]byte, page.MaxKeySize+1), nil); !errors.Is(err, page.ErrKeyTooLarge) {
		t.Fatalf("err = %v, want ErrKeyTooLarge", err)
	}
	if err := e.tree.Put([]byte("k"), make([]byte, page.MaxValueSize+1)); !errors.Is(err, page.ErrValueTooLarge) {
		t.Fatalf("err = %v, want ErrValueTooLarge", err)
	}
	mustPut(t, e.tree, []byte("k"), []byte("v")) // tree still usable
}

func TestPersistence(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 3000; i++ {
		mustPut(t, e.tree, key(i), value(i))
	}
	for i := 0; i < 3000; i += 3 {
		e.tree.Delete(key(i))
	}
	e.reopen(t)

	mustVerify(t, e.tree)
	for i := 0; i < 3000; i++ {
		if i%3 == 0 {
			mustGet(t, e.tree, key(i), nil)
		} else {
			mustGet(t, e.tree, key(i), value(i))
		}
	}
}

// TestModel is the week-2 pass criterion: 100,000 random Put/Get/Delete
// operations compared against a Go map, with Verify and scan checks every
// 1,000 steps and a final check after reopening the file.
func TestModel(t *testing.T) {
	steps := 100_000
	if testing.Short() {
		steps = 10_000
	}
	e := newEnv(t)
	rng := rand.New(rand.NewSource(42))
	model := map[string][]byte{}

	const keySpace = 20_000
	randKey := func() []byte {
		n := rng.Intn(keySpace)
		// Variable key lengths (8..200 bytes), deterministic per key.
		return []byte(fmt.Sprintf("%08d%s", n, strings.Repeat("k", n%193)))
	}
	randValue := func() []byte {
		size := rng.Intn(100)
		if rng.Intn(20) == 0 {
			size = rng.Intn(page.MaxValueSize + 1)
		}
		v := make([]byte, size)
		rng.Read(v)
		return v
	}

	checkScan := func(step int, start, end []byte) {
		var want []string
		for k := range model {
			if (start == nil || k >= string(start)) && (end == nil || k < string(end)) {
				want = append(want, k)
			}
		}
		sort.Strings(want)
		got := scanAll(t, e.tree, start, end)
		if len(got) != len(want) {
			t.Fatalf("step %d: Scan(%q, %q) returned %d entries, model has %d", step, start, end, len(got), len(want))
		}
		for i, en := range got {
			if string(en.key) != want[i] || !bytes.Equal(en.value, model[want[i]]) {
				t.Fatalf("step %d: scan entry %d = %q, want %q", step, i, en.key, want[i])
			}
		}
	}

	for step := 1; step <= steps; step++ {
		k := randKey()
		switch r := rng.Intn(10); {
		case r < 5:
			v := randValue()
			mustPut(t, e.tree, k, v)
			model[string(k)] = v
		case r < 7:
			found, err := e.tree.Delete(k)
			if err != nil {
				t.Fatal(err)
			}
			_, inModel := model[string(k)]
			if found != inModel {
				t.Fatalf("step %d: Delete(%q) found=%v, model=%v", step, k, found, inModel)
			}
			delete(model, string(k))
		default:
			got, ok, err := e.tree.Get(k)
			if err != nil {
				t.Fatal(err)
			}
			want, inModel := model[string(k)]
			if ok != inModel || !bytes.Equal(got, want) {
				t.Fatalf("step %d: Get(%q) = %v, model has %v", step, k, ok, inModel)
			}
		}

		if step%1000 == 0 {
			st := mustVerify(t, e.tree)
			if st.keys != len(model) {
				t.Fatalf("step %d: tree has %d keys, model %d", step, st.keys, len(model))
			}
			a, b := randKey(), randKey()
			if bytes.Compare(a, b) > 0 {
				a, b = b, a
			}
			checkScan(step, a, b)
		}
	}

	checkScan(steps, nil, nil)
	e.reopen(t)
	st := mustVerify(t, e.tree)
	checkScan(steps, nil, nil)
	t.Logf("final: %+v, pool %+v", st, e.pool.Stats())
}

// TestVerifyDetectsCorruption breaks a valid tree in several ways and checks
// that Verify reports each one.
func TestVerifyDetectsCorruption(t *testing.T) {
	corruptions := map[string]func(t *testing.T, e *env, leaf, root *page.Page){
		"broken sibling chain": func(t *testing.T, e *env, leaf, root *page.Page) {
			leaf.SetRightSibling(page.InvalidPageID)
		},
		"keys out of order": func(t *testing.T, e *env, leaf, root *page.Page) {
			c, _ := page.EncodeCell([]byte("zzz"), nil)
			must(t, leaf.UpdateCell(0, c))
		},
		"key outside separator range": func(t *testing.T, e *env, leaf, root *page.Page) {
			c, _ := page.EncodeCell(key(999999), nil)
			must(t, leaf.UpdateCell(leaf.NumSlots()-1, c))
		},
		"non-empty first separator": func(t *testing.T, e *env, leaf, root *page.Page) {
			must(t, root.UpdateCell(0, encodeInternal([]byte("x"), childAt(root, 0))))
		},
		"shared child": func(t *testing.T, e *env, leaf, root *page.Page) {
			must(t, root.UpdateCell(1, encodeInternal(cellKey(root, 1), childAt(root, 0))))
		},
		"wrong page type": func(t *testing.T, e *env, leaf, root *page.Page) {
			leaf.SetType(page.TypeFree)
		},
	}

	for name, corrupt := range corruptions {
		t.Run(name, func(t *testing.T) {
			e := newEnv(t)
			for i := 0; i < 2000; i++ {
				mustPut(t, e.tree, key(i), value(i))
			}
			mustVerify(t, e.tree)

			rootID := e.tree.root
			rf, err := e.pool.Fetch(rootID)
			must(t, err)
			leafID := childAt(rf.Page(), 0)
			lf, err := e.pool.Fetch(leafID)
			must(t, err)
			corrupt(t, e, lf.Page(), rf.Page())
			must(t, e.pool.Unpin(leafID, true))
			must(t, e.pool.Unpin(rootID, true))

			verr := e.tree.Verify()
			if !errors.Is(verr, ErrCorrupted) {
				t.Fatalf("Verify = %v, want ErrCorrupted", verr)
			}
			t.Log(verr)
		})
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// TestConcurrentReadersAndWriters is a smoke test for the tree lock (run it
// with -race): readers and scanners run while writers modify the tree.
func TestConcurrentReadersAndWriters(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 1000; i++ {
		mustPut(t, e.tree, key(i), value(i))
	}

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for w := 0; w < 2; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				k := 1000 + w*10000 + i
				if err := e.tree.Put(key(k), value(k)); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				if _, ok, err := e.tree.Get(key(i)); err != nil || !ok {
					errs <- fmt.Errorf("Get(%d) = %v, %v", i, ok, err)
					return
				}
				if i%100 == 0 {
					it := e.tree.Scan(nil, nil)
					var prev []byte
					for it.Next() {
						if prev != nil && bytes.Compare(prev, it.Key()) >= 0 {
							errs <- fmt.Errorf("scan out of order: %q then %q", prev, it.Key())
							return
						}
						prev = it.Key()
					}
					if it.Err() != nil {
						errs <- it.Err()
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if st := mustVerify(t, e.tree); st.keys != 5000 {
		t.Fatalf("keys = %d, want 5000", st.keys)
	}
}
