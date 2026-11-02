package engine

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func mustBegin(t *testing.T, db *DB) *Tx {
	t.Helper()
	tx, err := db.Begin()
	must(t, err)
	return tx
}

func getStr(t *testing.T, g interface {
	Get([]byte) ([]byte, bool, error)
}, k string) string {
	t.Helper()
	v, ok, err := g.Get([]byte(k))
	must(t, err)
	if !ok {
		return "<none>"
	}
	return string(v)
}

// run runs fn in a goroutine and returns a channel that receives its error.
func async(fn func() error) <-chan error {
	ch := make(chan error, 1)
	go func() { ch <- fn() }()
	return ch
}

func blocked(t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case err := <-ch:
		t.Fatalf("should be waiting for a lock, finished with %v", err)
	case <-time.After(100 * time.Millisecond):
	}
}

func finished(t *testing.T, ch <-chan error) {
	t.Helper()
	select {
	case err := <-ch:
		must(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("still waiting")
	}
}

func TestTxCommitAndRollback(t *testing.T) {
	db := mustOpen(t, t.TempDir(), fast)
	defer db.Close()
	must(t, db.Put([]byte("a"), []byte("old")))

	tx := mustBegin(t, db)
	must(t, tx.Put([]byte("a"), []byte("new")))
	must(t, tx.Put([]byte("b"), []byte("added")))
	// Not visible to others before commit (read committed reads).
	if getStr(t, db, "a") != "old" || getStr(t, db, "b") != "<none>" {
		t.Fatal("uncommitted writes are visible")
	}
	must(t, tx.Commit())
	if getStr(t, db, "a") != "new" || getStr(t, db, "b") != "added" {
		t.Fatal("committed writes not visible")
	}
	if err := tx.Put([]byte("c"), nil); !errors.Is(err, ErrTxDone) {
		t.Fatalf("use after commit = %v, want ErrTxDone", err)
	}

	tx = mustBegin(t, db)
	must(t, tx.Put([]byte("a"), []byte("discarded")))
	_, err := tx.Delete([]byte("b"))
	must(t, err)
	tx.Rollback()
	if getStr(t, db, "a") != "new" || getStr(t, db, "b") != "added" {
		t.Fatal("rolled-back writes were applied")
	}
	if st, _ := db.Stats(); st.Locks.Held != 0 {
		t.Fatalf("locks left after commit/rollback: %+v", st.Locks)
	}
}

func TestTxSeesItsOwnWrites(t *testing.T) {
	db := mustOpen(t, t.TempDir(), fast)
	defer db.Close()
	for _, k := range []string{"a", "c", "e", "g"} {
		must(t, db.Put([]byte(k), []byte("committed-"+k)))
	}
	tx := mustBegin(t, db)
	defer tx.Rollback()
	must(t, tx.Put([]byte("b"), []byte("own-b"))) // new key
	must(t, tx.Put([]byte("c"), []byte("own-c"))) // overwrite
	if found, err := tx.Delete([]byte("e")); !found || err != nil {
		t.Fatalf("Delete(e) = %v, %v", found, err)
	}
	if found, _ := tx.Delete([]byte("e")); found {
		t.Fatal("second delete in the same transaction found the key")
	}
	must(t, tx.Put([]byte("z"), []byte("own-z"))) // outside the scan range

	if getStr(t, tx, "c") != "own-c" || getStr(t, tx, "e") != "<none>" || getStr(t, tx, "a") != "committed-a" {
		t.Fatal("Get does not merge own writes")
	}
	var got []string
	it := tx.Scan([]byte("a"), []byte("h"))
	for it.Next() {
		got = append(got, string(it.Key())+"="+string(it.Value()))
	}
	must(t, it.Err())
	want := "[a=committed-a b=own-b c=own-c g=committed-g]"
	if fmt.Sprint(got) != want {
		t.Fatalf("scan = %v, want %s", got, want)
	}
}

// TestNoLostUpdates: many transactions increment one counter at the same
// time (read, then write). Without locking, increments would be lost.
// Under 2PL two readers that both want to write deadlock; one is rolled back
// and retries.
func TestNoLostUpdates(t *testing.T) {
	db := mustOpen(t, t.TempDir(), fast)
	defer db.Close()
	must(t, db.Put([]byte("counter"), []byte("0")))
	const workers, each = 8, 50
	var retries atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				for {
					err := increment(db)
					if errors.Is(err, ErrDeadlock) {
						retries.Add(1)
						continue
					}
					if err != nil {
						t.Error(err)
					}
					break
				}
			}
		}()
	}
	wg.Wait()
	if got := getStr(t, db, "counter"); got != strconv.Itoa(workers*each) {
		t.Fatalf("counter = %s, want %d (lost updates)", got, workers*each)
	}
	t.Logf("%d deadlock retries", retries.Load())
}

// TestGetForUpdateAvoidsUpgradeDeadlocks: the same counter workload, but
// reading with GetForUpdate. The second transaction waits instead of
// deadlocking, so there are no retries at all.
func TestGetForUpdateAvoidsUpgradeDeadlocks(t *testing.T) {
	db := mustOpen(t, t.TempDir(), fast)
	defer db.Close()
	must(t, db.Put([]byte("counter"), []byte("0")))
	const workers, each = 8, 50
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				if err := incrementWith(db, true); err != nil {
					t.Error(err) // a deadlock here would be an error
					return
				}
			}
		}()
	}
	wg.Wait()
	if got := getStr(t, db, "counter"); got != strconv.Itoa(workers*each) {
		t.Fatalf("counter = %s, want %d", got, workers*each)
	}
	if st, _ := db.Stats(); st.Locks.Deadlocks != 0 {
		t.Fatalf("%d deadlocks with GetForUpdate", st.Locks.Deadlocks)
	}
}

func increment(db *DB) error { return incrementWith(db, false) }

func incrementWith(db *DB, forUpdate bool) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	get := tx.Get
	if forUpdate {
		get = tx.GetForUpdate
	}
	v, _, err := get([]byte("counter"))
	if err != nil {
		return err // tx already rolled back
	}
	n, _ := strconv.Atoi(string(v))
	if err := tx.Put([]byte("counter"), []byte(strconv.Itoa(n+1))); err != nil {
		return err
	}
	return tx.Commit()
}

// TestBankTransfers is the classic isolation test: money moves between
// accounts in concurrent transactions, while auditors read every account in
// one transaction. Every audit must see the same total (no transfer seen
// half-done), and so must the final state.
func TestBankTransfers(t *testing.T) {
	db := mustOpen(t, t.TempDir(), fast)
	defer db.Close()
	const accounts, initial = 10, 1000
	for i := 0; i < accounts; i++ {
		must(t, db.Put(acct(i), []byte(strconv.Itoa(initial))))
	}
	var deadlocks, audits atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})

	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 100; i++ {
				from, to := rng.Intn(accounts), rng.Intn(accounts)
				for from == to {
					to = rng.Intn(accounts)
				}
				for {
					err := transfer(db, from, to, 1+rng.Intn(50))
					if errors.Is(err, ErrDeadlock) {
						deadlocks.Add(1)
						continue
					}
					if err != nil {
						t.Error(err)
					}
					break
				}
			}
		}(int64(w))
	}
	auditorDone := make(chan struct{})
	go func() {
		defer close(auditorDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			total, err := audit(db)
			if errors.Is(err, ErrDeadlock) {
				continue
			}
			if err != nil {
				t.Error(err)
				return
			}
			if total != accounts*initial {
				t.Errorf("audit saw total %d, want %d", total, accounts*initial)
				return
			}
			audits.Add(1)
		}
	}()
	wg.Wait()
	close(stop)
	<-auditorDone

	if total, err := audit(db); err != nil || total != accounts*initial {
		t.Fatalf("final total %d (%v), want %d", total, err, accounts*initial)
	}
	must(t, db.Verify())
	t.Logf("%d audits, %d deadlock retries", audits.Load(), deadlocks.Load())
}

func acct(i int) []byte { return []byte(fmt.Sprintf("acct%02d", i)) }

func transfer(db *DB, from, to, amount int) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	balance := func(i int) (int, error) {
		v, _, err := tx.Get(acct(i))
		n, _ := strconv.Atoi(string(v))
		return n, err
	}
	a, err := balance(from)
	if err != nil {
		return err
	}
	b, err := balance(to)
	if err != nil {
		return err
	}
	if err := tx.Put(acct(from), []byte(strconv.Itoa(a-amount))); err != nil {
		return err
	}
	if err := tx.Put(acct(to), []byte(strconv.Itoa(b+amount))); err != nil {
		return err
	}
	return tx.Commit()
}

func audit(db *DB) (int, error) {
	tx, err := db.Begin()
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	total := 0
	it := tx.Scan([]byte("acct"), []byte("acct~"))
	for it.Next() {
		n, _ := strconv.Atoi(string(it.Value()))
		total += n
	}
	return total, it.Err()
}

// TestNoPhantoms: a transaction scans a range twice. A concurrent insert
// into that range has to wait until the scanning transaction ends, so both
// scans see the same keys.
func TestNoPhantoms(t *testing.T) {
	db := mustOpen(t, t.TempDir(), fast)
	defer db.Close()
	for i := 0; i < 5; i++ {
		must(t, db.Put([]byte(fmt.Sprintf("item%d", i)), []byte("x")))
	}
	count := func(tx *Tx) int {
		n := 0
		it := tx.Scan([]byte("item"), []byte("item~"))
		for it.Next() {
			n++
		}
		must(t, it.Err())
		return n
	}

	reader := mustBegin(t, db)
	first := count(reader)
	insert := async(func() error { return db.Put([]byte("item9"), []byte("phantom?")) })
	blocked(t, insert) // the writer's IX lock waits for the scan's S lock
	if second := count(reader); second != first {
		t.Fatalf("phantom: first scan %d keys, second %d", first, second)
	}
	must(t, reader.Commit())
	finished(t, insert)
	if getStr(t, db, "item9") != "phantom?" {
		t.Fatal("insert lost")
	}
}

func TestDeadlockVictimIsRolledBack(t *testing.T) {
	db := mustOpen(t, t.TempDir(), fast)
	defer db.Close()
	tx1, tx2 := mustBegin(t, db), mustBegin(t, db)
	must(t, tx1.Put([]byte("a"), []byte("1")))
	must(t, tx2.Put([]byte("b"), []byte("2")))

	waiting := async(func() error { return tx1.Put([]byte("b"), []byte("1")) }) // tx1 waits for tx2
	blocked(t, waiting)
	if err := tx2.Put([]byte("a"), []byte("2")); !errors.Is(err, ErrDeadlock) { // cycle
		t.Fatalf("second cross lock = %v, want ErrDeadlock", err)
	}
	if err := tx2.Commit(); !errors.Is(err, ErrTxDone) {
		t.Fatalf("victim Commit = %v, want ErrTxDone (it was rolled back)", err)
	}
	finished(t, waiting) // the victim's locks were released
	must(t, tx1.Commit())
	if getStr(t, db, "a") != "1" || getStr(t, db, "b") != "1" {
		t.Fatal("survivor's writes missing, or victim's writes applied")
	}
}

// TestAutocommitWaitsForTransaction: a plain db.Put is a transaction too,
// so it waits for a transaction that holds the key.
func TestAutocommitWaitsForTransaction(t *testing.T) {
	db := mustOpen(t, t.TempDir(), fast)
	defer db.Close()
	tx := mustBegin(t, db)
	must(t, tx.Put([]byte("k"), []byte("from tx")))
	plain := async(func() error { return db.Put([]byte("k"), []byte("from db.Put")) })
	blocked(t, plain)
	must(t, tx.Commit())
	finished(t, plain)
	if getStr(t, db, "k") != "from db.Put" {
		t.Fatal("writes applied out of order")
	}
}

func TestCloseWithOpenTransactions(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, fast)
	holder := mustBegin(t, db)
	must(t, holder.Put([]byte("k"), []byte("never committed")))
	waiter := async(func() error { return db.Put([]byte("k"), []byte("waits")) })
	blocked(t, waiter)

	closed := async(db.Close)
	finished(t, closed) // Close must not hang on the waiting transaction
	select {
	case err := <-waiter:
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("waiter got %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("waiter still blocked after Close")
	}
	if err := holder.Commit(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Commit after Close = %v, want ErrClosed", err)
	}

	db = mustOpen(t, dir, fast)
	defer db.Close()
	if getStr(t, db, "k") != "<none>" {
		t.Fatal("an uncommitted transaction survived Close")
	}
}

func TestTxTooLarge(t *testing.T) {
	db := mustOpen(t, t.TempDir(), Options{NoSync: true, PoolFrames: 8})
	defer db.Close()
	tx := mustBegin(t, db)
	for i := 0; i < MaxTxnWrites; i++ {
		must(t, tx.Put(key(i), nil))
	}
	if err := tx.Put(key(MaxTxnWrites), nil); !errors.Is(err, ErrTxTooLarge) {
		t.Fatalf("write %d = %v, want ErrTxTooLarge", MaxTxnWrites+1, err)
	}
	must(t, tx.Put(key(0), []byte("rewrite is fine"))) // not a new key
	tx.Rollback()

	// Within the key limit, but with an 8-frame pool the commit's pages do
	// not fit: the commit fails as a whole.
	tx = mustBegin(t, db)
	for i := 0; i < 300; i++ {
		must(t, tx.Put(key(i), bytes.Repeat([]byte{'v'}, 500)))
	}
	if err := tx.Commit(); !errors.Is(err, ErrTxTooLarge) {
		t.Fatalf("Commit = %v, want ErrTxTooLarge", err)
	}
	if n := len(contents(t, db)); n != 0 {
		t.Fatalf("failed commit applied %d keys", n)
	}
	must(t, db.Verify())
}

// TestTxSurvivesCrash: a committed transaction is all there after a crash,
// an uncommitted one is not there at all.
func TestTxSurvivesCrash(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, fast)
	committed := mustBegin(t, db)
	for i := 0; i < 200; i++ {
		must(t, committed.Put(key(i), val(i)))
	}
	must(t, committed.Commit())
	open := mustBegin(t, db)
	for i := 200; i < 400; i++ {
		must(t, open.Put(key(i), val(i)))
	}
	db.crash() // the open transaction dies with the process

	db = mustOpen(t, dir, fast)
	defer db.Close()
	got := contents(t, db)
	if len(got) != 200 {
		t.Fatalf("%d keys after crash, want the 200 of the committed transaction", len(got))
	}
	must(t, db.Verify())
}
