package engine

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"maps"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"baseDB/internal/page"
)

// Controlled crash tests.
//
// For one operation (a page split, a root split, a slot update, a delete)
// we capture the files before and after it, then build every state a crash
// in the middle of that operation can leave on disk, and check that recovery
// turns each one into either "operation done" or "operation not done",
// never anything in between:
//
//	A. the COMMIT never became durable: the log is cut at every interesting
//	   point inside the operation's transaction, the data file is unchanged
//	   (no-steal: uncommitted pages never reach it)
//	   → the operation must not be visible
//
//	B. the COMMIT is durable: the log is complete, and any subset of the
//	   changed pages (meta page included) reached the data file, because
//	   after a power failure the writes since the last fsync can survive in
//	   any combination
//	   → the operation must be visible
//
//	C. like B, but one changed page is torn (half new, half old bytes)
//	   → the page is rebuilt from the log; the operation must be visible

type kvOp struct {
	del  bool
	k, v []byte
}

func put(k, v []byte) kvOp { return kvOp{k: k, v: v} }
func del(k []byte) kvOp    { return kvOp{del: true, k: k} }

func (o kvOp) apply(t *testing.T, db *DB, model map[string]string) {
	t.Helper()
	if o.del {
		_, err := db.Delete(o.k)
		must(t, err)
		delete(model, string(o.k))
		return
	}
	must(t, db.Put(o.k, o.v))
	model[string(o.k)] = string(o.v)
}

type crashScenario struct {
	name string
	base []kvOp // state before the crash
	op   kvOp   // the operation the crash interrupts
	txn  []kvOp // or: a transaction of several writes, committed as one
	// pages the operation must change (0 = don't check); includes page 0
	// when the meta page changes
	wantChanged int
}

// mixedKey spreads inserts over the whole key space, so splits happen in
// the middle of the tree, not only at its right edge.
func mixedKey(i int) []byte { return key(i * 7919 % 10007) }

// firstPut returns the number of puts of mixedKey(0..) after which the next
// put satisfies cond on the meta page before and after it.
func firstPut(t *testing.T, cond func(before, after page.Meta) bool) int {
	t.Helper()
	db := mustOpen(t, t.TempDir(), fast)
	defer db.Close()
	for i := 0; i < 10000; i++ {
		before := db.pg.Meta()
		must(t, db.Put(mixedKey(i), val(i)))
		if cond(before, db.pg.Meta()) {
			return i
		}
	}
	t.Fatal("condition never met")
	return 0
}

// firstOp applies setup, then ops one by one, and returns the index of the
// first op after which cond holds on the meta page before and after it.
func firstOp(t *testing.T, setup, ops []kvOp, cond func(before, after page.Meta) bool) int {
	t.Helper()
	db := mustOpen(t, t.TempDir(), fast)
	defer db.Close()
	model := map[string]string{}
	for _, o := range setup {
		o.apply(t, db, model)
	}
	for i, o := range ops {
		before := db.pg.Meta()
		o.apply(t, db, model)
		if cond(before, db.pg.Meta()) {
			return i
		}
	}
	t.Fatal("condition never met")
	return 0
}

// txnOps: 30 inserts spread over the key space (several leaves, splits),
// 5 updates and 5 deletes, all in one transaction.
func txnOps() []kvOp {
	var ops []kvOp
	for i := 0; i < 30; i++ {
		ops = append(ops, put(mixedKey(5000+i), val(i)))
	}
	for i := 0; i < 5; i++ {
		ops = append(ops, put(mixedKey(i*40), []byte("updated in txn")), del(mixedKey(i*40+7)))
	}
	return ops
}

func (o kvOp) applyTx(t *testing.T, tx *Tx, model map[string]string) {
	t.Helper()
	if o.del {
		_, err := tx.Delete(o.k)
		must(t, err)
		delete(model, string(o.k))
		return
	}
	must(t, tx.Put(o.k, o.v))
	model[string(o.k)] = string(o.v)
}

func puts(n int) []kvOp {
	ops := make([]kvOp, n)
	for i := range ops {
		ops[i] = put(mixedKey(i), val(i))
	}
	return ops
}

func TestCrashMatrix(t *testing.T) {
	rootSplit := firstPut(t, func(b, a page.Meta) bool {
		return b.RootPageID != page.InvalidPageID && a.RootPageID != b.RootPageID
	})
	leafSplit := firstPut(t, func(b, a page.Meta) bool {
		return b.RootPageID != page.InvalidPageID && b.NextPageID > 3 && // height 2 already
			a.RootPageID == b.RootPageID && a.NextPageID > b.NextPageID
	})

	// A fragmented leaf: 20 cells, every other one deleted. Growing a value
	// then needs an in-page compaction (contiguous space is too small).
	var fragmented []kvOp
	for i := 0; i < 20; i++ {
		fragmented = append(fragmented, put(key(i), bytes.Repeat([]byte{'f'}, 150)))
	}
	for i := 0; i < 20; i += 2 {
		fragmented = append(fragmented, del(key(i)))
	}

	// A delete that empties a leaf (the page goes to the free list), and
	// then a put that takes that page back from the free list.
	var deletes []kvOp
	for i := 0; i < 400; i++ {
		deletes = append(deletes, del(mixedKey(i)))
	}
	grown := slices.Concat(puts(400), deletes)
	freeing := 400 + firstOp(t, puts(400), deletes, func(b, a page.Meta) bool {
		return a.FreeListHead != b.FreeListHead
	})
	afterFree := grown[:freeing+1]
	var refill []kvOp
	for i := 0; i < 400; i++ {
		refill = append(refill, put(mixedKey(1000+i), val(i)))
	}
	reusing := firstOp(t, afterFree, refill, func(b, a page.Meta) bool {
		return b.FreeListHead != page.InvalidPageID && a.FreeListHead != b.FreeListHead
	})

	scenarios := []crashScenario{
		{name: "delete frees a leaf", base: grown[:freeing], op: grown[freeing]},
		{name: "put reuses a free page", base: slices.Concat(afterFree, refill[:reusing]), op: refill[reusing]},
		{name: "root split", base: puts(rootSplit), op: put(mixedKey(rootSplit), val(rootSplit)), wantChanged: 4},
		{name: "leaf split", base: puts(leafSplit), op: put(mixedKey(leafSplit), val(leafSplit)), wantChanged: 4},
		{name: "slot update same size", base: puts(300), op: put(mixedKey(57), bytes.Repeat([]byte{'u'}, len(val(57)))), wantChanged: 1},
		{name: "slot update with compaction", base: fragmented, op: put(key(1), bytes.Repeat([]byte{'g'}, 700)), wantChanged: 1},
		{name: "delete", base: puts(300), op: del(mixedKey(100)), wantChanged: 1},
		{name: "transaction of 40 writes", base: puts(300), txn: txnOps()},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) { runCrashScenario(t, sc) })
	}
}

func runCrashScenario(t *testing.T, sc crashScenario) {
	// Capture the files before and after the operation.
	dir := t.TempDir()
	db := mustOpen(t, dir, fast)
	before := map[string]string{}
	for _, o := range sc.base {
		o.apply(t, db, before)
	}
	must(t, db.Close())
	dataBefore, logBefore := readFiles(t, dir)

	// "After" = the log right after the operation's commit (Close would
	// checkpoint and truncate it) and the data file with every page of the
	// operation written.
	db = mustOpen(t, dir, fast)
	after := maps.Clone(before)
	if sc.txn != nil {
		tx := mustBegin(t, db)
		for _, o := range sc.txn {
			o.applyTx(t, tx, after)
		}
		must(t, tx.Commit())
	} else {
		sc.op.apply(t, db, after)
	}
	_, logAfter := readFiles(t, dir)
	must(t, db.pool.FlushAll())
	dataAfter, _ := readFiles(t, dir)
	db.crash()

	if !bytes.HasPrefix(logAfter, logBefore) {
		t.Fatal("the operation rewrote existing log bytes")
	}
	txn := logAfter[len(logBefore):]
	changed := changedPages(dataBefore, dataAfter)
	if sc.wantChanged != 0 && len(changed) != sc.wantChanged {
		t.Fatalf("operation changed pages %v, want %d pages", changed, sc.wantChanged)
	}

	scratch := t.TempDir()
	runs := 0
	try := func(desc string, data, log []byte, want map[string]string) {
		t.Helper()
		writeFiles(t, scratch, data, log)
		db, err := Open(scratch, fast)
		if err != nil {
			t.Fatalf("%s: Open: %v", desc, err)
		}
		checkContents(t, db, want, desc)
		must(t, db.Close())
		runs++
	}

	// A. Commit not durable.
	for _, cut := range cutPoints(txn) {
		try(fmt.Sprintf("A: log cut at byte %d of %d", cut, len(txn)),
			dataBefore, concat(logBefore, txn[:cut]), before)
	}
	// B. Commit durable, any subset of changed pages written.
	for _, mask := range subsets(len(changed)) {
		try(fmt.Sprintf("B: pages written mask %b of %v", mask, changed),
			mix(dataBefore, dataAfter, changed, mask, -1), logAfter, after)
	}
	// C. Commit durable, one page torn, the others all old or all new.
	for torn := range changed {
		for _, mask := range []int{0, 1<<len(changed) - 1} {
			try(fmt.Sprintf("C: page %d torn, others mask %b", changed[torn], mask),
				mix(dataBefore, dataAfter, changed, mask, torn), logAfter, after)
		}
	}
	t.Logf("%d crash states recovered (txn %d bytes, changed pages %v)", runs, len(txn), changed)
}

// subsets returns the subsets of n changed pages to test, as bit masks: all
// 2^n of them for up to 6 pages; for more (a large transaction), a sample
// that still covers the extremes: none, all, each page alone, each page
// missing alone, and 64 random subsets.
func subsets(n int) []int {
	all := 1<<n - 1
	if n <= 6 {
		masks := make([]int, all+1)
		for i := range masks {
			masks[i] = i
		}
		return masks
	}
	masks := []int{0, all}
	for i := 0; i < n; i++ {
		masks = append(masks, 1<<i, all&^(1<<i))
	}
	rng := rand.New(rand.NewSource(int64(n)))
	for i := 0; i < 64; i++ {
		masks = append(masks, rng.Intn(all+1))
	}
	return masks
}

func readFiles(t *testing.T, dir string) (data, log []byte) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, DataFileName))
	must(t, err)
	log, err = os.ReadFile(filepath.Join(dir, LogFileName))
	must(t, err)
	return data, log
}

func writeFiles(t *testing.T, dir string, data, log []byte) {
	t.Helper()
	must(t, os.WriteFile(filepath.Join(dir, DataFileName), data, 0o644))
	must(t, os.WriteFile(filepath.Join(dir, LogFileName), log, 0o644))
}

func concat(a, b []byte) []byte { return append(bytes.Clone(a), b...) }

func pageAt(data []byte, id int) []byte {
	start := id * page.PageSize
	out := make([]byte, page.PageSize) // beyond the end of the file = zeros
	if start < len(data) {
		copy(out, data[start:])
	}
	return out
}

func changedPages(before, after []byte) []int {
	var ids []int
	for id := 0; id*page.PageSize < max(len(before), len(after)); id++ {
		if !bytes.Equal(pageAt(before, id), pageAt(after, id)) {
			ids = append(ids, id)
		}
	}
	return ids
}

// mix builds a data file from before, taking the pages in changed whose bit
// is set in mask from after. If torn >= 0, page changed[torn] gets the new
// first half and the old second half.
func mix(before, after []byte, changed []int, mask, torn int) []byte {
	out := make([]byte, max(len(before), len(after)))
	copy(out, before)
	for i, id := range changed {
		dst := out[id*page.PageSize : (id+1)*page.PageSize]
		switch {
		case i == torn:
			copy(dst[:page.PageSize/2], pageAt(after, id))
			copy(dst[page.PageSize/2:], pageAt(before, id)[page.PageSize/2:])
		case mask&(1<<i) != 0:
			copy(dst, pageAt(after, id))
		}
	}
	return out
}

// cutPoints returns log lengths to cut the transaction at: around every
// record boundary (inside the length field, right at the boundary, right
// after it) and every 97 bytes. Every cut is shorter than the whole
// transaction, so the COMMIT record is always incomplete or missing.
func cutPoints(txn []byte) []int {
	set := map[int]bool{0: true, len(txn) - 1: true}
	for off := 0; off+4 <= len(txn); {
		for _, d := range []int{-1, 0, 1, 2} {
			set[off+d] = true
		}
		off += int(binary.LittleEndian.Uint32(txn[off:]))
	}
	for c := 0; c < len(txn); c += 97 {
		set[c] = true
	}
	var cuts []int
	for c := range set {
		if c >= 0 && c < len(txn) {
			cuts = append(cuts, c)
		}
	}
	return cuts
}
