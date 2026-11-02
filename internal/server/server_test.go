package server

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"baseDB/client"
	"baseDB/internal/engine"
	"baseDB/internal/protocol"
)

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// start opens a database and a server on a free local port.
func start(t *testing.T, opts Options) (*engine.DB, *Server, string) {
	t.Helper()
	db, err := engine.Open(t.TempDir(), engine.Options{NoSync: true})
	must(t, err)
	srv := New(db, opts)
	must(t, srv.Listen("127.0.0.1:0"))
	for srv.Addr() == nil {
		time.Sleep(time.Millisecond)
	}
	t.Cleanup(func() {
		srv.Close()
		db.Close()
	})
	return db, srv, srv.Addr().String()
}

func dial(t *testing.T, addr string) *client.Client {
	t.Helper()
	c, err := client.Dial(addr)
	must(t, err)
	t.Cleanup(func() { c.Close() })
	return c
}

// TestRawProtocol talks the protocol by hand, the way telnet or nc would.
func TestRawProtocol(t *testing.T) {
	_, _, addr := start(t, Options{})
	conn, err := net.Dial("tcp", addr)
	must(t, err)
	defer conn.Close()
	io.WriteString(conn, "put a 1\r\nput \"b c\" \"line1\\nline2\"\nget a\nget nope\nscan\ncount\nbogus\nput \"open\nping\nquit\n")
	got, err := io.ReadAll(conn) // the server closes after quit
	must(t, err)
	want := `OK
OK
VALUE "1"
NOT_FOUND
ROW "a" "1"
ROW "b c" "line1\nline2"
OK 2
OK 2
ERR UNKNOWN unknown command "bogus" (type help for the list)
ERR SYNTAX missing closing quote (")
OK PONG
OK bye
`
	if string(got) != want {
		t.Fatalf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestClientLibrary(t *testing.T) {
	_, _, addr := start(t, Options{})
	c := dial(t, addr)
	must(t, c.Put([]byte("k"), []byte("v\x00\xff\n")))
	v, ok, err := c.Get([]byte("k"))
	if err != nil || !ok || string(v) != "v\x00\xff\n" {
		t.Fatalf("Get = %q, %v, %v", v, ok, err)
	}
	if _, ok, _ := c.Get([]byte("nope")); ok {
		t.Fatal("missing key found")
	}
	must(t, c.Put([]byte("k2"), []byte("x")))
	kvs, err := c.Scan(nil, nil)
	if err != nil || len(kvs) != 2 || string(kvs[1].Key) != "k2" {
		t.Fatalf("Scan = %v, %v", kvs, err)
	}
	if n, err := c.Count([]byte("k2"), nil); err != nil || n != 1 {
		t.Fatalf("Count = %d, %v", n, err)
	}
	if found, err := c.Delete([]byte("k")); !found || err != nil {
		t.Fatalf("Delete = %v, %v", found, err)
	}
	if err := c.Put(make([]byte, 300), nil); !errors.Is(err, client.ErrTooLarge) {
		t.Fatalf("oversized key = %v, want ErrTooLarge", err)
	}
	if err := c.Commit(); !errors.Is(err, client.ErrNoTx) {
		t.Fatalf("Commit without Begin = %v, want ErrNoTx", err)
	}
}

// TestManyClients: 100 clients at once, each writing and reading its own
// keys.
func TestManyClients(t *testing.T) {
	db, srv, addr := start(t, Options{})
	const clients, each = 100, 30
	var wg sync.WaitGroup
	errs := make(chan error, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			c, err := client.Dial(addr)
			if err != nil {
				errs <- err
				return
			}
			defer c.Close()
			for j := 0; j < each; j++ {
				k := []byte(fmt.Sprintf("c%03d-%02d", i, j))
				if err := c.Put(k, k); err != nil {
					errs <- err
					return
				}
				if v, ok, err := c.Get(k); err != nil || !ok || string(v) != string(k) {
					errs <- fmt.Errorf("Get(%s) = %q, %v, %v", k, v, ok, err)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	st, _ := db.Stats()
	if st.Tree.Keys != clients*each {
		t.Fatalf("%d keys, want %d", st.Tree.Keys, clients*each)
	}
	if s := srv.Stats(); s.Accepted != clients {
		t.Fatalf("server stats %+v", s)
	}
	must(t, db.Verify())
}

// TestBankTransfersOverNetwork: the isolation test of week 7, with every
// transaction on its own connection.
func TestBankTransfersOverNetwork(t *testing.T) {
	_, _, addr := start(t, Options{})
	setup := dial(t, addr)
	const accounts, initial = 8, 1000
	for i := 0; i < accounts; i++ {
		must(t, setup.Put(acct(i), []byte(strconv.Itoa(initial))))
	}
	var retries atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			c, err := client.Dial(addr)
			if err != nil {
				t.Error(err)
				return
			}
			defer c.Close()
			rng := rand.New(rand.NewSource(seed))
			for i := 0; i < 40; i++ {
				from, to := rng.Intn(accounts), rng.Intn(accounts)
				if from == to {
					continue
				}
				for {
					err := transfer(c, from, to, 1+rng.Intn(20))
					if client.IsRetryable(err) {
						retries.Add(1)
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
	wg.Wait()
	total := 0
	kvs, err := setup.Scan([]byte("acct"), []byte("acct~"))
	must(t, err)
	for _, kv := range kvs {
		n, _ := strconv.Atoi(string(kv.Value))
		total += n
	}
	if total != accounts*initial {
		t.Fatalf("total %d, want %d", total, accounts*initial)
	}
	t.Logf("%d deadlock retries", retries.Load())
}

func acct(i int) []byte { return []byte(fmt.Sprintf("acct%02d", i)) }

func transfer(c *client.Client, from, to, amount int) error {
	if err := c.Begin(); err != nil {
		return err
	}
	read := func(i int) (int, error) {
		v, _, err := c.Get(acct(i))
		n, _ := strconv.Atoi(string(v))
		return n, err
	}
	a, err := read(from)
	if err != nil {
		return err // deadlock: the server already rolled back
	}
	b, err := read(to)
	if err != nil {
		return err
	}
	if err := c.Put(acct(from), []byte(strconv.Itoa(a-amount))); err != nil {
		return err
	}
	if err := c.Put(acct(to), []byte(strconv.Itoa(b+amount))); err != nil {
		return err
	}
	return c.Commit()
}

// TestDisconnectRollsBack: a client that disconnects in the middle of a
// transaction loses it, and its locks are released at once.
func TestDisconnectRollsBack(t *testing.T) {
	_, _, addr := start(t, Options{})
	a := dial(t, addr)
	must(t, a.Begin())
	must(t, a.Put([]byte("k"), []byte("from a")))

	b := dial(t, addr)
	done := make(chan error, 1)
	go func() { done <- b.Put([]byte("k"), []byte("from b")) }()
	select {
	case err := <-done:
		t.Fatalf("b did not wait for a's lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	a.Close() // a goes away without commit
	select {
	case err := <-done:
		must(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("a's locks were not released when it disconnected")
	}
	if v, _, _ := b.Get([]byte("k")); string(v) != "from b" {
		t.Fatalf("k = %q: a's uncommitted write survived its disconnect", v)
	}
}

// TestIdleTimeout: a client that stops talking in the middle of a
// transaction is disconnected, so it cannot block others forever.
func TestIdleTimeout(t *testing.T) {
	_, _, addr := start(t, Options{IdleTimeout: 200 * time.Millisecond})
	a := dial(t, addr)
	must(t, a.Begin())
	must(t, a.Put([]byte("k"), []byte("abandoned")))

	b := dial(t, addr)
	start := time.Now()
	must(t, b.Put([]byte("k"), []byte("b")))
	if waited := time.Since(start); waited < 100*time.Millisecond {
		t.Fatalf("b did not wait for the idle client's lock (%v)", waited)
	}
	if err := a.Commit(); err == nil {
		t.Fatal("the idle client's connection is still usable")
	}
}

func TestMaxConns(t *testing.T) {
	_, srv, addr := start(t, Options{MaxConns: 2})
	dial(t, addr).Ping()
	dial(t, addr).Ping()
	c := dial(t, addr)
	if err := c.Ping(); !errors.Is(err, client.ErrBusy) {
		t.Fatalf("third client = %v, want ErrBusy", err)
	}
	if s := srv.Stats(); s.Refused != 1 {
		t.Fatalf("stats %+v", s)
	}
}

// TestPipelining sends 500 requests without waiting, then reads 500
// responses, in order.
func TestPipelining(t *testing.T) {
	_, _, addr := start(t, Options{})
	conn, err := net.Dial("tcp", addr)
	must(t, err)
	defer conn.Close()
	var req strings.Builder
	for i := 0; i < 500; i++ {
		fmt.Fprintf(&req, "put k%03d %d\nget k%03d\n", i, i, i)
	}
	go io.WriteString(conn, req.String())
	r := bufio.NewReaderSize(conn, protocol.MaxLine)
	for i := 0; i < 500; i++ {
		put, err := protocol.ReadResponse(r)
		must(t, err)
		get, err := protocol.ReadResponse(r)
		must(t, err)
		if put.Status != protocol.StatusOK || string(get.Value) != strconv.Itoa(i) {
			t.Fatalf("request %d: %+v %+v", i, put, get)
		}
	}
}

func TestLineTooLong(t *testing.T) {
	_, _, addr := start(t, Options{})
	conn, err := net.Dial("tcp", addr)
	must(t, err)
	defer conn.Close()
	go io.WriteString(conn, "put k "+strings.Repeat("x", protocol.MaxLine+100)+"\n")
	got, _ := io.ReadAll(conn)
	if !strings.HasPrefix(string(got), "ERR SYNTAX line longer than") {
		t.Fatalf("got %q", got)
	}
}

func TestDeadlockOverNetwork(t *testing.T) {
	_, _, addr := start(t, Options{})
	a, b := dial(t, addr), dial(t, addr)
	must(t, a.Begin())
	must(t, b.Begin())
	must(t, a.Put([]byte("x"), []byte("a")))
	must(t, b.Put([]byte("y"), []byte("b")))
	aWait := make(chan error, 1)
	go func() { aWait <- a.Put([]byte("y"), []byte("a")) }() // a waits for b
	time.Sleep(100 * time.Millisecond)
	if err := b.Put([]byte("x"), []byte("b")); !errors.Is(err, client.ErrDeadlock) {
		t.Fatalf("b = %v, want ErrDeadlock", err)
	}
	must(t, <-aWait) // b was rolled back, so a got the lock
	must(t, a.Commit())
	if err := b.Commit(); !errors.Is(err, client.ErrNoTx) {
		t.Fatalf("the victim's transaction is gone: Commit = %v, want ErrNoTx", err)
	}
}

// TestCloseWithClients: shutting down with clients connected, one of them
// waiting for a lock, finishes promptly.
func TestCloseWithClients(t *testing.T) {
	db, srv, addr := start(t, Options{})
	a := dial(t, addr)
	must(t, a.Begin())
	must(t, a.Put([]byte("k"), []byte("a")))
	b := dial(t, addr)
	go b.Put([]byte("k"), []byte("b")) // waits for a
	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		srv.Close()
		db.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shutdown hangs")
	}
	if err := a.Ping(); err == nil {
		t.Fatal("client still connected after Close")
	}
}
