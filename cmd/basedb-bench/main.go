// Command basedb-bench measures write throughput and latency through the
// TCP server, with and without group commit, for a range of client counts.
//
//	go run ./cmd/basedb-bench                         # default matrix
//	go run ./cmd/basedb-bench -clients 1,8,64 -duration 3s -csv results.csv
//
// Every run uses a fresh database in a temporary directory, real fsync, a
// server on 127.0.0.1 and N clients that each put distinct keys as fast as
// the server answers. The result is a table on stdout and, with -csv, a
// file to draw graphs from.
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"baseDB/client"
	"baseDB/internal/engine"
	"baseDB/internal/server"
)

type result struct {
	groupCommit bool
	clients     int
	ops         int
	elapsed     time.Duration
	p50, p99    time.Duration
	fsyncs      uint64
}

func (r result) opsPerSec() float64 { return float64(r.ops) / r.elapsed.Seconds() }
func (r result) perFsync() float64  { return float64(r.ops) / float64(max(r.fsyncs, 1)) }

func main() {
	clientsFlag := flag.String("clients", "1,2,4,8,16,32,64", "comma-separated client counts")
	duration := flag.Duration("duration", 2*time.Second, "how long each run writes")
	valueSize := flag.Int("value", 100, "value size in bytes")
	csvPath := flag.String("csv", "", "also write the results to this CSV file")
	only := flag.String("group-commit", "both", "on, off or both")
	flag.Parse()

	var counts []int
	for _, s := range strings.Split(*clientsFlag, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n < 1 {
			fmt.Fprintf(os.Stderr, "bad -clients value %q\n", s)
			os.Exit(2)
		}
		counts = append(counts, n)
	}
	modes := map[string][]bool{"both": {false, true}, "on": {true}, "off": {false}}[*only]
	if modes == nil {
		fmt.Fprintln(os.Stderr, "-group-commit must be on, off or both")
		os.Exit(2)
	}

	fmt.Printf("put throughput through TCP, real fsync, %d-byte values, %v per run\n\n", *valueSize, *duration)
	fmt.Printf("%-13s %7s %10s %12s %9s %9s %8s %12s\n", "group commit", "clients", "ops", "ops/s", "p50", "p99", "fsyncs", "writes/fsync")
	var results []result
	for _, gc := range modes {
		for _, n := range counts {
			r, err := runOnce(gc, n, *duration, *valueSize)
			if err != nil {
				fmt.Fprintln(os.Stderr, "run failed:", err)
				os.Exit(1)
			}
			results = append(results, r)
			fmt.Printf("%-13s %7d %10d %12.0f %9s %9s %8d %12.1f\n", onOff(gc), n, r.ops, r.opsPerSec(),
				r.p50.Round(10*time.Microsecond), r.p99.Round(10*time.Microsecond), r.fsyncs, r.perFsync())
		}
	}
	if *csvPath != "" {
		if err := writeCSV(*csvPath, results); err != nil {
			fmt.Fprintln(os.Stderr, "csv:", err)
			os.Exit(1)
		}
		fmt.Printf("\nwrote %s\n", *csvPath)
	}
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

func runOnce(groupCommit bool, clients int, d time.Duration, valueSize int) (result, error) {
	dir, err := os.MkdirTemp("", "basedb-bench-")
	if err != nil {
		return result{}, err
	}
	defer os.RemoveAll(dir)
	db, err := engine.Open(dir, engine.Options{NoGroupCommit: !groupCommit}) // real fsync
	if err != nil {
		return result{}, err
	}
	srv := server.New(db, server.Options{MaxConns: clients + 10})
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		db.Close()
		return result{}, err
	}
	for srv.Addr() == nil {
		time.Sleep(time.Millisecond)
	}
	defer func() { srv.Close(); db.Close() }()
	addr := srv.Addr().String()

	conns := make([]*client.Client, clients)
	for i := range conns {
		if conns[i], err = client.Dial(addr); err != nil {
			return result{}, err
		}
		defer conns[i].Close()
	}
	value := []byte(strings.Repeat("v", valueSize))
	before, _ := db.Stats()

	var wg sync.WaitGroup
	latencies := make([][]time.Duration, clients)
	errs := make(chan error, clients)
	start := time.Now()
	deadline := start.Add(d)
	for i, c := range conns {
		wg.Add(1)
		go func(i int, c *client.Client) {
			defer wg.Done()
			for j := 0; time.Now().Before(deadline); j++ {
				t0 := time.Now()
				if err := c.Put([]byte(fmt.Sprintf("c%03d-%08d", i, j)), value); err != nil {
					errs <- err
					return
				}
				latencies[i] = append(latencies[i], time.Since(t0))
			}
		}(i, c)
	}
	wg.Wait()
	elapsed := time.Since(start)
	close(errs)
	if err := <-errs; err != nil {
		return result{}, err
	}

	var all []time.Duration
	for _, l := range latencies {
		all = append(all, l...)
	}
	slices.Sort(all)
	after, _ := db.Stats()
	r := result{groupCommit: groupCommit, clients: clients, ops: len(all), elapsed: elapsed,
		fsyncs: after.Commits.Batches - before.Commits.Batches}
	if len(all) > 0 {
		r.p50 = all[len(all)*50/100]
		r.p99 = all[min(len(all)-1, len(all)*99/100)]
	}
	return r, nil
}

func writeCSV(path string, results []result) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	w.Write([]string{"group_commit", "clients", "ops", "seconds", "ops_per_sec", "p50_ms", "p99_ms", "fsyncs", "writes_per_fsync"})
	for _, r := range results {
		w.Write([]string{
			onOff(r.groupCommit), strconv.Itoa(r.clients), strconv.Itoa(r.ops),
			fmt.Sprintf("%.3f", r.elapsed.Seconds()), fmt.Sprintf("%.0f", r.opsPerSec()),
			fmt.Sprintf("%.3f", float64(r.p50)/1e6), fmt.Sprintf("%.3f", float64(r.p99)/1e6),
			strconv.FormatUint(r.fsyncs, 10), fmt.Sprintf("%.2f", r.perFsync()),
		})
	}
	w.Flush()
	return w.Error()
}
