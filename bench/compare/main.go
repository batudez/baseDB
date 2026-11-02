// Command compare runs the same workloads against baseDB, Pebble, SQLite,
// RocksDB, redb and WiredTiger and prints the results side by side.
//
//	cd bench/compare
//	go run .                                   # everything, 3 runs, medians
//	go run . -dbs basedb,rocksdb -workloads read,writers -runs 1
//
// Every run is saved as ../compareN.txt (tables, with the machine and the
// store versions) and ../compareN.csv (every run), N = the next free number.
// It is a separate Go module, so baseDB itself keeps no dependencies. The
// C, C++ and Rust stores need cgo and deps/ (see README.md).
//
// Fairness rules (see README.md next to this file):
//   - every store fsyncs every commit (-sync=true), or none does
//   - every store gets the same cache size (-cache)
//   - same keys, values, operation order (fixed -seed) and machine
//   - one key per transaction, except in the bulk workloads
//   - one foreign call per operation for the stores not written in Go
//   - every run starts from a new directory; -runs repeats and the tables
//     show the median
package main

import (
	"encoding/binary"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type workload struct {
	name, desc string
	run        func(b *bench, db string) ([]result, error)
}

var workloads = []workload{
	{"bulk-seq", "insert all keys in increasing order, 1,000 keys per transaction", (*bench).bulkSeq},
	{"bulk-rand", "insert all keys in random order, 1,000 keys per transaction", (*bench).bulkRand},
	{"read", "100% random point reads (YCSB C)", func(b *bench, db string) ([]result, error) { return b.mix(db, "read", 100, 0) }},
	{"read-mostly", "95% reads, 5% updates (YCSB B)", func(b *bench, db string) ([]result, error) { return b.mix(db, "read-mostly", 95, 0) }},
	{"mixed", "50% reads, 50% updates (YCSB A)", func(b *bench, db string) ([]result, error) { return b.mix(db, "mixed", 50, 0) }},
	{"update", "100% random updates, one key per transaction", func(b *bench, db string) ([]result, error) { return b.mix(db, "update", 0, 0) }},
	{"scan", "short range scans from a random key (YCSB E, reads only)", func(b *bench, db string) ([]result, error) { return b.mix(db, "scan", 0, 100) }},
	{"writers", "N concurrent writers, one new key per transaction", (*bench).writers},
	{"space", "data on disk: after loading, after deleting the lower half of the keys, after inserting as many new keys", (*bench).space},
}

type result struct {
	workload string
	db       string
	threads  int
	run      int
	ops      int
	elapsed  time.Duration
	p50, p99 time.Duration
	timed    bool  // p50/p99 measured (per-operation workloads)
	size     int64 // directory size after Close, 0 = not measured
}

// avg is the exact mean latency: total time over operations, per thread.
// Unlike p50/p99 it does not depend on the clock's resolution.
func (r result) avg() time.Duration {
	if r.ops == 0 {
		return 0
	}
	return r.elapsed * time.Duration(r.threads) / time.Duration(r.ops)
}

func (r result) opsPerSec() float64 {
	if r.elapsed == 0 {
		return 0
	}
	return float64(r.ops) / r.elapsed.Seconds()
}

type bench struct {
	cfg          config
	keys         int
	valueSize    int
	pool         []byte // random bytes the values are cut from (valueFor)
	duration     time.Duration
	writerCounts []int
	scanLen      int
	seed         int64
	tmp          string
	run          int
	counter      uint64 // for newValue
}

func main() {
	dbsFlag := flag.String("dbs", "basedb,pebble,sqlite,rocksdb,redb,wiredtiger", "stores to test: "+strings.Join(storeNames(), ", "))
	wlFlag := flag.String("workloads", "all", "comma-separated workloads, or all")
	keys := flag.Int("keys", 100_000, "keys loaded before the read and update workloads")
	valueSize := flag.Int("value", 100, "value size in bytes")
	duration := flag.Duration("duration", 2*time.Second, "length of each timed workload")
	writersFlag := flag.String("writers", "1,8,64", "writer counts for the writers workload")
	cache := flag.Int("cache", 64, "cache size in MiB, for every store")
	syncFlag := flag.Bool("sync", true, "fsync every commit, in every store")
	runs := flag.Int("runs", 3, "repeat everything this many times; tables show the median")
	scanLen := flag.Int("scan-len", 10, "keys read per scan")
	seed := flag.Int64("seed", 1, "random seed: the same keys and operations for every store")
	resultsDir := flag.String("results", "..", "save the results here as compareN.txt and compareN.csv (N = next free number); empty = don't save")
	redbDLL := flag.String("redb-dll", filepath.Join("deps", "install", "redb", "baseredb.dll"), "path of the redb wrapper DLL")
	tmp := flag.String("tmp", "", "directory for the test databases (default: the system temp directory)")
	flag.Parse()

	var dbs []string
	for _, d := range strings.Split(*dbsFlag, ",") {
		d = strings.TrimSpace(d)
		if _, ok := stores[d]; !ok {
			fatalf("unknown store %q (available: %s; sqlite, rocksdb and wiredtiger need cgo)", d, strings.Join(storeNames(), ", "))
		}
		dbs = append(dbs, d)
	}
	var wls []workload
	if *wlFlag == "all" {
		wls = workloads
	} else {
		for _, name := range strings.Split(*wlFlag, ",") {
			i := slices.IndexFunc(workloads, func(w workload) bool { return w.name == strings.TrimSpace(name) })
			if i < 0 {
				fatalf("unknown workload %q", name)
			}
			wls = append(wls, workloads[i])
		}
	}
	var writers []int
	for _, s := range strings.Split(*writersFlag, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n < 1 {
			fatalf("bad -writers value %q", s)
		}
		writers = append(writers, n)
	}
	if *keys < 2 || *runs < 1 || *cache < 1 || *valueSize < 8 {
		fatalf("-keys, -runs and -cache must be positive, -value at least 8")
	}

	pool := make([]byte, 4<<20+*valueSize)
	rand.New(rand.NewSource(*seed)).Read(pool)
	b := &bench{
		cfg: config{sync: *syncFlag, cacheMiB: *cache, redbDLL: *redbDLL}, keys: *keys, valueSize: *valueSize, pool: pool,
		duration: *duration, writerCounts: writers, scanLen: *scanLen, seed: *seed, tmp: *tmp,
	}

	// Pick the result number first, so that a long run cannot collide
	// with one started after it.
	var txtPath, csvPath string
	if *resultsDir != "" {
		var err error
		if txtPath, csvPath, err = nextResultFiles(*resultsDir); err != nil {
			fatalf("results: %v", err)
		}
	}
	started := time.Now()

	var all []result
	for run := 1; run <= *runs; run++ {
		b.run = run
		for _, db := range dbs {
			for _, w := range wls {
				fmt.Fprintf(os.Stderr, "run %d/%d  %-12s %s\n", run, *runs, db, w.name)
				rs, err := w.run(b, db)
				if err != nil {
					fatalf("%s %s: %v", db, w.name, err)
				}
				all = append(all, rs...)
			}
		}
	}

	var out strings.Builder
	printHeader(&out, b, dbs, started)
	printTables(&out, b, dbs, wls, all, *runs)
	fmt.Print(out.String())
	if txtPath != "" {
		if err := os.WriteFile(txtPath, []byte(out.String()), 0o644); err != nil {
			fatalf("results: %v", err)
		}
		if err := writeCSV(csvPath, all); err != nil {
			fatalf("results: %v", err)
		}
		fmt.Printf("\nsaved %s and %s\n", txtPath, csvPath)
	}
}

// nextResultFiles returns dir/compareN.txt and .csv for the smallest N not
// used yet, and creates the .txt (empty) to reserve the number.
func nextResultFiles(dir string) (txt, csv string, err error) {
	for n := 1; ; n++ {
		txt = filepath.Join(dir, fmt.Sprintf("compare%d.txt", n))
		csv = filepath.Join(dir, fmt.Sprintf("compare%d.csv", n))
		if _, err := os.Stat(csv); err == nil {
			continue
		}
		f, err := os.OpenFile(txt, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
		if errors.Is(err, fs.ErrExist) {
			continue
		}
		if err != nil {
			return "", "", err
		}
		return txt, csv, f.Close()
	}
}

// printHeader records what is needed to compare this run with later ones:
// when, on what, with which versions and settings.
func printHeader(w io.Writer, b *bench, dbs []string, started time.Time) {
	host, _ := os.Hostname()
	fmt.Fprintf(w, "baseDB storage engine comparison, %s\n", started.Format("2006-01-02 15:04"))
	fmt.Fprintf(w, "machine: %s, %s/%s, %d CPUs, %s\n", host, runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.Version())
	fmt.Fprintf(w, "command: %s\n", strings.Join(os.Args[1:], " "))
	fmt.Fprintln(w, "stores:")
	for _, d := range dbs {
		fmt.Fprintf(w, "  %-12s %s\n", d, stores[d].version(b.cfg))
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "compare: "+format+"\n", args...)
	os.Exit(1)
}

func key(i int) []byte { return []byte(fmt.Sprintf("user%012d", i)) }

// valueFor returns the value of key i: its own random bytes, a window of
// the 4 MiB random pool. Every key must have a different value: with one
// value for all keys (compare3), a compressing store (RocksDB) shrank the
// data to almost nothing. The windows of nearby keys are 104,729 bytes
// apart, so no data block holds two overlapping ones. Cheap: no copy.
func (b *bench) valueFor(i int) []byte {
	off := i * 104_729 % (len(b.pool) - b.valueSize)
	return b.pool[off : off+b.valueSize : off+b.valueSize]
}

// newValue returns a value that differs from every earlier one. An update
// must really change the value: baseDB logs only the bytes that changed,
// so rewriting the same value would log nothing and skip the fsync, while
// the other stores rewrite it anyway.
func (b *bench) newValue() []byte {
	b.counter++
	v := slices.Clone(b.valueFor(int(b.counter)))
	binary.LittleEndian.PutUint64(v[:8], b.counter)
	return v
}

// ---- helpers for one store in one directory ----

func (b *bench) newDir() (string, func(), error) {
	dir, err := os.MkdirTemp(b.tmp, "compare-")
	return dir, func() { os.RemoveAll(dir) }, err
}

func (b *bench) open(db, dir string) (Store, error) { return stores[db].open(dir, b.cfg) }

// load inserts keys[i] for every i in order, 1,000 per transaction.
func (b *bench) load(s Store, order []int) error {
	for start := 0; start < len(order); start += 1000 {
		ops := make([]op, 0, 1000)
		for _, i := range order[start:min(start+1000, len(order))] {
			ops = append(ops, op{key: key(i), value: b.valueFor(i)})
		}
		if err := s.Batch(ops); err != nil {
			return err
		}
	}
	return nil
}

// dirSize is the size of a store's data after a clean Close: its directory
// without its write-ahead log files. After a clean Close every store has
// checkpointed or flushed, so its logs hold nothing that is still needed,
// but two stores keep log files as reserve: WiredTiger creates each log at
// full size (100 MiB) and prepares the next one in advance (200 MiB of
// zeros), and Pebble keeps old logs to recycle (about 10 MiB here). So no
// store's logs are counted: baseDB's wal.log, Pebble's and RocksDB's *.log
// (RocksDB's text info log LOG is counted), WiredTiger's log files. SQLite
// removes its WAL at Close; redb has no log.
func dirSize(dir string) int64 {
	var n int64
	filepath.WalkDir(dir, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil // d is nil when err is set
		}
		name := d.Name()
		if strings.HasSuffix(name, ".log") || strings.HasPrefix(name, "WiredTigerLog.") || strings.HasPrefix(name, "WiredTigerPreplog.") {
			return nil
		}
		if !d.IsDir() {
			if fi, err := d.Info(); err == nil {
				n += fi.Size()
			}
		}
		return nil
	})
	return n
}

func percentiles(lat []time.Duration) (p50, p99 time.Duration) {
	if len(lat) == 0 {
		return 0, 0
	}
	slices.Sort(lat)
	return lat[len(lat)*50/100], lat[min(len(lat)-1, len(lat)*99/100)]
}

// ---- workloads ----

func (b *bench) bulkSeq(db string) ([]result, error) {
	order := make([]int, b.keys)
	for i := range order {
		order[i] = i
	}
	return b.bulk(db, "bulk-seq", order)
}

func (b *bench) bulkRand(db string) ([]result, error) {
	return b.bulk(db, "bulk-rand", rand.New(rand.NewSource(b.seed)).Perm(b.keys))
}

func (b *bench) bulk(db, name string, order []int) ([]result, error) {
	dir, cleanup, err := b.newDir()
	if err != nil {
		return nil, err
	}
	defer cleanup()
	s, err := b.open(db, dir)
	if err != nil {
		return nil, err
	}
	start := time.Now()
	if err := b.load(s, order); err != nil {
		s.Close()
		return nil, err
	}
	elapsed := time.Since(start)
	if err := s.Close(); err != nil {
		return nil, err
	}
	return []result{{workload: name, db: db, threads: 1, run: b.run, ops: len(order), elapsed: elapsed, size: dirSize(dir)}}, nil
}

// mix loads the keys, reads them all once (so every store starts with a
// warm cache), then for the duration runs readPct% point reads, scanPct%
// scans and the rest updates of random existing keys, one thread.
func (b *bench) mix(db, name string, readPct, scanPct int) ([]result, error) {
	dir, cleanup, err := b.newDir()
	if err != nil {
		return nil, err
	}
	defer cleanup()
	s, err := b.open(db, dir)
	if err != nil {
		return nil, err
	}
	defer s.Close()
	if err := b.load(s, rand.New(rand.NewSource(b.seed)).Perm(b.keys)); err != nil {
		return nil, err
	}
	if n, err := s.Scan(nil, b.keys); err != nil || n != b.keys {
		return nil, fmt.Errorf("warm-up scan read %d of %d keys: %v", n, b.keys, err)
	}

	rng := rand.New(rand.NewSource(b.seed + 1))
	var lat []time.Duration
	start := time.Now()
	deadline := start.Add(b.duration)
	for time.Now().Before(deadline) {
		k := key(rng.Intn(b.keys))
		p := rng.Intn(100)
		t0 := time.Now()
		switch {
		case p < readPct:
			if _, ok, err := s.Get(k); err != nil || !ok {
				return nil, fmt.Errorf("get %s: found=%v err=%v", k, ok, err)
			}
		case p < readPct+scanPct:
			if _, err := s.Scan(k, b.scanLen); err != nil {
				return nil, err
			}
		default:
			if err := s.Put(k, b.newValue()); err != nil {
				return nil, err
			}
		}
		lat = append(lat, time.Since(t0))
	}
	elapsed := time.Since(start)
	p50, p99 := percentiles(lat)
	return []result{{workload: name, db: db, threads: 1, run: b.run, ops: len(lat), elapsed: elapsed, p50: p50, p99: p99, timed: true}}, nil
}

// writers: for each writer count, a new database and N goroutines that
// each put new keys, one per transaction, as fast as they can.
func (b *bench) writers(db string) ([]result, error) {
	var out []result
	for _, n := range b.writerCounts {
		r, err := b.writersOnce(db, n)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

func (b *bench) writersOnce(db string, n int) (result, error) {
	dir, cleanup, err := b.newDir()
	if err != nil {
		return result{}, err
	}
	defer cleanup()
	s, err := b.open(db, dir)
	if err != nil {
		return result{}, err
	}
	defer s.Close()

	var wg sync.WaitGroup
	lats := make([][]time.Duration, n)
	errs := make([]error, n)
	start := time.Now()
	deadline := start.Add(b.duration)
	for w := 0; w < n; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for j := 0; time.Now().Before(deadline); j++ {
				t0 := time.Now()
				if err := s.Put([]byte(fmt.Sprintf("w%03d-%010d", w, j)), b.valueFor(w<<20+j)); err != nil {
					errs[w] = err
					return
				}
				lats[w] = append(lats[w], time.Since(t0))
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	var all []time.Duration
	for w := range lats {
		if errs[w] != nil {
			return result{}, errs[w]
		}
		all = append(all, lats[w]...)
	}
	p50, p99 := percentiles(all)
	return result{workload: "writers", db: db, threads: n, run: b.run, ops: len(all), elapsed: elapsed, p50: p50, p99: p99, timed: true}, nil
}

// space measures the directory after Close at three points: all keys
// loaded; the lower half of the keys deleted (like expiring old records);
// as many new keys inserted as were deleted. A store that reuses freed
// space ends close to the first size.
func (b *bench) space(db string) ([]result, error) {
	dir, cleanup, err := b.newDir()
	if err != nil {
		return nil, err
	}
	defer cleanup()
	var out []result
	step := func(name string, f func(Store) error) error {
		s, err := b.open(db, dir)
		if err != nil {
			return err
		}
		if err := f(s); err != nil {
			s.Close()
			return err
		}
		if err := s.Close(); err != nil {
			return err
		}
		out = append(out, result{workload: name, db: db, threads: 1, run: b.run, size: dirSize(dir)})
		return nil
	}
	if err := step("space: loaded", func(s Store) error {
		return b.load(s, rand.New(rand.NewSource(b.seed)).Perm(b.keys))
	}); err != nil {
		return nil, err
	}
	if err := step("space: half deleted", func(s Store) error {
		for start := 0; start < b.keys/2; start += 1000 {
			var ops []op
			for i := start; i < min(start+1000, b.keys/2); i++ {
				ops = append(ops, op{key: key(i), del: true})
			}
			if err := s.Batch(ops); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if err := step("space: refilled", func(s Store) error {
		order := make([]int, b.keys/2)
		for i := range order {
			order[i] = b.keys + i
		}
		return b.load(s, order)
	}); err != nil {
		return nil, err
	}
	return out, nil
}

// ---- output ----

// median returns the middle value of the runs (the lower middle for an
// even count).
func median[T int64 | float64 | time.Duration](xs []T) T {
	if len(xs) == 0 {
		return 0
	}
	s := slices.Clone(xs)
	slices.Sort(s)
	return s[(len(s)-1)/2]
}

func printTables(w io.Writer, b *bench, dbs []string, wls []workload, all []result, runs int) {
	syncText := "fsync on every commit"
	if !b.cfg.sync {
		syncText = "NO fsync"
	}
	fmt.Fprintf(w, "\n%d keys, %d-byte values, %s, cache %d MiB per store, %v per timed run, median of %d run(s)\n",
		b.keys, b.valueSize, syncText, b.cfg.cacheMiB, b.duration, runs)

	// Group results in the order they were produced.
	type groupKey struct {
		workload, db string
		threads      int
	}
	groups := map[groupKey][]result{}
	var names []string
	for _, r := range all {
		k := groupKey{r.workload, r.db, r.threads}
		if _, ok := groups[k]; !ok && !slices.Contains(names, r.workload) {
			names = append(names, r.workload)
		}
		groups[k] = append(groups[k], r)
	}
	desc := map[string]string{}
	for _, wl := range wls {
		desc[wl.name] = wl.desc
	}

	for _, name := range names {
		isSpace := strings.HasPrefix(name, "space")
		if d, ok := desc[name]; ok {
			fmt.Fprintf(w, "\n%s: %s\n", name, d)
		} else {
			fmt.Fprintf(w, "\n%s\n", name)
		}
		if isSpace {
			fmt.Fprintf(w, "  %-12s %12s\n", "store", "on disk")
		} else {
			fmt.Fprintf(w, "  %-12s %7s %12s %10s %10s %10s %12s\n", "store", "threads", "ops/s", "avg", "p50", "p99", "on disk")
		}
		for _, db := range dbs {
			var threads []int
			for k := range groups {
				if k.workload == name && k.db == db && !slices.Contains(threads, k.threads) {
					threads = append(threads, k.threads)
				}
			}
			slices.Sort(threads)
			for _, t := range threads {
				rs := groups[groupKey{name, db, t}]
				var ops []float64
				var avgs, p50s, p99s []time.Duration
				var sizes []int64
				for _, r := range rs {
					ops = append(ops, r.opsPerSec())
					avgs = append(avgs, r.avg())
					p50s = append(p50s, r.p50)
					p99s = append(p99s, r.p99)
					sizes = append(sizes, r.size)
				}
				if isSpace {
					fmt.Fprintf(w, "  %-12s %12s\n", db, mib(median(sizes)))
					continue
				}
				timed := rs[0].timed
				fmt.Fprintf(w, "  %-12s %7d %12s %10s %10s %10s %12s\n", db, t, thousands(median(ops)),
					dur(median(avgs), true), dur(median(p50s), timed), dur(median(p99s), timed), mib(median(sizes)))
			}
		}
	}
	fmt.Fprintln(w, "\navg is exact (time / operations). p50 and p99 come from per-operation timings; ≈0 means below this")
	fmt.Fprintln(w, "machine's clock resolution (on Windows a few µs or more), so read those from avg instead.")
	fmt.Fprintln(w, "on disk = the data after a clean Close: the directory without write-ahead log files, which then")
	fmt.Fprintln(w, "hold nothing needed (Pebble keeps old ones to recycle, WiredTiger preallocates 200 MiB of them).")
}

func thousands(f float64) string {
	s := strconv.FormatInt(int64(f+0.5), 10)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func dur(d time.Duration, measured bool) string {
	switch {
	case !measured:
		return "-"
	case d == 0:
		return "≈0"
	case d < time.Millisecond:
		return fmt.Sprintf("%.1fµs", float64(d)/1e3)
	default:
		return fmt.Sprintf("%.2fms", float64(d)/1e6)
	}
}

func mib(n int64) string {
	if n == 0 {
		return "-"
	}
	return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
}

func writeCSV(path string, all []result) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	w.Write([]string{"workload", "db", "threads", "run", "ops", "seconds", "ops_per_sec", "avg_us", "p50_us", "p99_us", "size_bytes"})
	for _, r := range all {
		w.Write([]string{
			r.workload, r.db, strconv.Itoa(r.threads), strconv.Itoa(r.run), strconv.Itoa(r.ops),
			fmt.Sprintf("%.3f", r.elapsed.Seconds()), fmt.Sprintf("%.0f", r.opsPerSec()),
			fmt.Sprintf("%.2f", float64(r.avg())/1e3), fmt.Sprintf("%.1f", float64(r.p50)/1e3), fmt.Sprintf("%.1f", float64(r.p99)/1e3),
			strconv.FormatInt(r.size, 10),
		})
	}
	w.Flush()
	return w.Error()
}
