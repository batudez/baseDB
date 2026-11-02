package lockmgr

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	db Resource = "db"
	a  Resource = "a"
	b  Resource = "b"
	c  Resource = "c"
)

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// lockAsync starts a Lock in a goroutine and returns its result channel.
func lockAsync(m *Manager, txn TxnID, res Resource, mode Mode) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- m.Lock(txn, res, mode) }()
	return ch
}

// waitUntilWaiting blocks until n requests are queued in m.
func waitUntilWaiting(t *testing.T, m *Manager, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for m.Stats().Waiting < n {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %d waiting requests (have %d)", n, m.Stats().Waiting)
		}
		time.Sleep(time.Millisecond)
	}
}

func expectBlocked(t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("lock should be waiting, got %v", err)
	case <-time.After(50 * time.Millisecond):
	}
}

func expectGranted(t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case err := <-ch:
		must(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("lock never granted")
	}
}

func TestCompatibilityMatrix(t *testing.T) {
	modes := []Mode{IS, IX, S, SIX, X}
	want := map[[2]Mode]bool{
		{IS, IS}: true, {IS, IX}: true, {IS, S}: true, {IS, SIX}: true,
		{IX, IX}: true, {S, S}: true,
	}
	for _, x := range modes {
		for _, y := range modes {
			w := want[[2]Mode{x, y}] || want[[2]Mode{y, x}]
			if Compatible(x, y) != w {
				t.Errorf("Compatible(%v, %v) = %v, want %v", x, y, Compatible(x, y), w)
			}
		}
	}
}

func TestJoin(t *testing.T) {
	cases := []struct{ a, b, want Mode }{
		{IS, IS, IS}, {IS, IX, IX}, {IS, S, S}, {IX, S, SIX}, {S, IX, SIX},
		{SIX, IS, SIX}, {SIX, S, SIX}, {SIX, IX, SIX}, {S, X, X}, {IX, X, X}, {SIX, X, X},
	}
	for _, c := range cases {
		if got := join(c.a, c.b); got != c.want {
			t.Errorf("join(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
		}
	}
}

func TestSharedAndExclusive(t *testing.T) {
	m := New()
	must(t, m.Lock(1, a, S))
	must(t, m.Lock(2, a, S)) // S and S share

	x := lockAsync(m, 3, a, X)
	expectBlocked(t, x)
	m.ReleaseAll(1)
	expectBlocked(t, x) // txn 2 still reads
	m.ReleaseAll(2)
	expectGranted(t, x)

	s := lockAsync(m, 4, a, S)
	expectBlocked(t, s) // X excludes everything
	m.ReleaseAll(3)
	expectGranted(t, s)
}

func TestReentrantAndUpgrade(t *testing.T) {
	m := New()
	must(t, m.Lock(1, a, X))
	must(t, m.Lock(1, a, S)) // already covered by X: no-op
	if m.Holds(1, a) != X {
		t.Fatalf("holds %v, want X", m.Holds(1, a))
	}
	m.ReleaseAll(1)

	// Sole S holder upgrades at once.
	must(t, m.Lock(1, a, S))
	must(t, m.Lock(1, a, X))
	if m.Holds(1, a) != X {
		t.Fatalf("after upgrade holds %v, want X", m.Holds(1, a))
	}
	m.ReleaseAll(1)

	// With another reader, the upgrade waits for it.
	must(t, m.Lock(1, a, S))
	must(t, m.Lock(2, a, S))
	up := lockAsync(m, 1, a, X)
	expectBlocked(t, up)
	m.ReleaseAll(2)
	expectGranted(t, up)

	// IX + S on the database joins to SIX.
	must(t, m.Lock(3, db, IX))
	must(t, m.Lock(3, db, S))
	if m.Holds(3, db) != SIX {
		t.Fatalf("IX+S = %v, want SIX", m.Holds(3, db))
	}
}

// TestFirstComeFirstServed: a waiting writer is not overtaken by readers
// that arrive later, even though they would fit next to the current reader.
func TestFirstComeFirstServed(t *testing.T) {
	m := New()
	must(t, m.Lock(1, a, S))
	w := lockAsync(m, 2, a, X)
	waitUntilWaiting(t, m, 1)
	r := lockAsync(m, 3, a, S)
	expectBlocked(t, r) // would be compatible with txn 1, but queues behind txn 2
	m.ReleaseAll(1)
	expectGranted(t, w)
	expectBlocked(t, r)
	m.ReleaseAll(2)
	expectGranted(t, r)
}

func TestMultiGranularity(t *testing.T) {
	m := New()
	// Two writers of different keys share the database (IX + IX).
	must(t, m.Lock(1, db, IX))
	must(t, m.Lock(1, a, X))
	must(t, m.Lock(2, db, IX))
	must(t, m.Lock(2, b, X))
	// A reader of a third key fits too (IS).
	must(t, m.Lock(3, db, IS))
	must(t, m.Lock(3, c, S))
	// A full scan (S on the database) must wait for both writers.
	scan := lockAsync(m, 4, db, S)
	expectBlocked(t, scan)
	m.ReleaseAll(1)
	expectBlocked(t, scan)
	m.ReleaseAll(2)
	expectGranted(t, scan)
	// While the scan holds S, a new writer waits; a reader does not.
	must(t, m.Lock(5, db, IS))
	wr := lockAsync(m, 6, db, IX)
	expectBlocked(t, wr)
	m.ReleaseAll(4)
	expectGranted(t, wr)
}

func TestDeadlockTwoTransactions(t *testing.T) {
	m := New()
	must(t, m.Lock(1, a, X))
	must(t, m.Lock(2, b, X))
	first := lockAsync(m, 1, b, X) // 1 waits for 2
	waitUntilWaiting(t, m, 1)
	if err := m.Lock(2, a, X); !errors.Is(err, ErrDeadlock) { // 2 would wait for 1
		t.Fatalf("Lock = %v, want ErrDeadlock", err)
	}
	m.ReleaseAll(2) // the victim aborts
	expectGranted(t, first)
	if st := m.Stats(); st.Deadlocks != 1 {
		t.Fatalf("stats %+v", st)
	}
}

func TestDeadlockCycleOfThree(t *testing.T) {
	m := New()
	must(t, m.Lock(1, a, X))
	must(t, m.Lock(2, b, X))
	must(t, m.Lock(3, c, X))
	w1 := lockAsync(m, 1, b, X)
	waitUntilWaiting(t, m, 1)
	w2 := lockAsync(m, 2, c, X)
	waitUntilWaiting(t, m, 2)
	if err := m.Lock(3, a, X); !errors.Is(err, ErrDeadlock) {
		t.Fatalf("Lock = %v, want ErrDeadlock", err)
	}
	m.ReleaseAll(3)
	expectGranted(t, w2)
	m.ReleaseAll(2)
	expectGranted(t, w1)
}

// TestUpgradeDeadlock: two readers of the same key both try to upgrade to
// write it. Each waits for the other's S lock: the classic upgrade deadlock.
func TestUpgradeDeadlock(t *testing.T) {
	m := New()
	must(t, m.Lock(1, a, S))
	must(t, m.Lock(2, a, S))
	up1 := lockAsync(m, 1, a, X)
	waitUntilWaiting(t, m, 1)
	if err := m.Lock(2, a, X); !errors.Is(err, ErrDeadlock) {
		t.Fatalf("second upgrade = %v, want ErrDeadlock", err)
	}
	m.ReleaseAll(2)
	expectGranted(t, up1)
}

func TestNoFalseDeadlock(t *testing.T) {
	m := New()
	// A chain, not a cycle: 3 waits for 2, 2 waits for 1.
	must(t, m.Lock(1, a, X))
	must(t, m.Lock(2, b, X))
	w2 := lockAsync(m, 2, a, X)
	waitUntilWaiting(t, m, 1)
	w3 := lockAsync(m, 3, b, X)
	waitUntilWaiting(t, m, 2)
	if st := m.Stats(); st.Deadlocks != 0 {
		t.Fatalf("chain reported as deadlock: %+v", st)
	}
	m.ReleaseAll(1)
	expectGranted(t, w2)
	m.ReleaseAll(2)
	expectGranted(t, w3)
}

func TestCloseWakesWaiters(t *testing.T) {
	m := New()
	must(t, m.Lock(1, a, X))
	w := lockAsync(m, 2, a, X)
	waitUntilWaiting(t, m, 1)
	m.Close()
	select {
	case err := <-w:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("waiter got %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter not woken by Close")
	}
	if err := m.Lock(3, b, S); !errors.Is(err, ErrClosed) {
		t.Fatalf("Lock after Close = %v", err)
	}
}

// TestRandomStress runs many goroutines locking random keys in random modes
// (retrying deadlock victims) and checks after every grant that no two
// transactions ever hold conflicting locks on the same key.
func TestRandomStress(t *testing.T) {
	m := New()
	const workers, rounds, keys = 8, 300, 5
	type holderSet struct {
		mu sync.Mutex
		by map[TxnID]Mode
	}
	var held [keys]holderSet
	for i := range held {
		held[i].by = map[TxnID]Mode{}
	}
	var nextTxn atomic.Uint64
	var violations, deadlocks atomic.Int64
	modes := []Mode{S, X}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for r := 0; r < rounds; r++ {
				txn := TxnID(nextTxn.Add(1))
				var mine []int
				ok := true
				for i := 0; i < 1+rng.Intn(3) && ok; i++ {
					k, mode := rng.Intn(keys), modes[rng.Intn(2)]
					if err := m.Lock(txn, Resource(rune('a'+k)), mode); err != nil {
						if !errors.Is(err, ErrDeadlock) {
							t.Error(err)
						}
						deadlocks.Add(1)
						ok = false
						break
					}
					hs := &held[k]
					hs.mu.Lock()
					hs.by[txn] = m.Holds(txn, Resource(rune('a'+k)))
					for o, om := range hs.by {
						if o != txn && !Compatible(om, hs.by[txn]) {
							violations.Add(1)
						}
					}
					hs.mu.Unlock()
					mine = append(mine, k)
				}
				time.Sleep(time.Duration(rng.Intn(100)) * time.Microsecond)
				for _, k := range mine {
					held[k].mu.Lock()
					delete(held[k].by, txn)
					held[k].mu.Unlock()
				}
				m.ReleaseAll(txn)
			}
		}(int64(w))
	}
	wg.Wait()
	if violations.Load() != 0 {
		t.Fatalf("%d times two transactions held conflicting locks", violations.Load())
	}
	if st := m.Stats(); st.Held != 0 || st.Waiting != 0 {
		t.Fatalf("locks left behind: %+v", st)
	}
	t.Logf("%d transactions, %d deadlock victims, stats %+v", nextTxn.Load(), deadlocks.Load(), m.Stats())
}
