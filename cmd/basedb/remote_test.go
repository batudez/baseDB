package main

import (
	"bytes"
	"net"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"baseDB/internal/engine"
	"baseDB/internal/server"
)

// startServer runs a server on a free port for the remote-mode tests.
func startServer(t *testing.T) string {
	t.Helper()
	db, err := engine.Open(t.TempDir(), engine.Options{NoSync: true})
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(db, server.Options{})
	if err := srv.Listen("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	for srv.Addr() == nil {
		time.Sleep(time.Millisecond)
	}
	t.Cleanup(func() { srv.Close(); db.Close() })
	return srv.Addr().String()
}

func runRemote(t *testing.T, addr, script string, args ...string) (string, string, int) {
	t.Helper()
	var out, errOut bytes.Buffer
	code := run(append([]string{"-connect", addr}, args...), strings.NewReader(script), &out, &errOut, false)
	return out.String(), errOut.String(), code
}

// TestLocalAndRemoteAreIdentical runs the same script against a local
// database and against a server: the output must be the same, because both
// go through server.Session.
func TestLocalAndRemoteAreIdentical(t *testing.T) {
	script := `
put user:1 "Batuhan"
put user:2 "Ayşe Yılmaz"
put bin "\x00\xff\n"
get bin
scan
prefix user:
count user: user;
begin
put user:1 changed
delete user:2
rollback
begin
put t 1
commit
delete nope
get user:1
verify
`
	localOut, localErr, localCode := runScript(t, t.TempDir(), script)
	remoteOut, remoteErr, remoteCode := runRemote(t, startServer(t), script)
	if localCode != 0 || remoteCode != 0 || localErr != "" || remoteErr != "" {
		t.Fatalf("local exit %d %q, remote exit %d %q", localCode, localErr, remoteCode, remoteErr)
	}
	if localOut != remoteOut {
		t.Fatalf("local and remote differ\nlocal:\n%s\nremote:\n%s", localOut, remoteOut)
	}
}

func TestRemoteSingleCommandAndErrors(t *testing.T) {
	addr := startServer(t)
	if out, errOut, code := runRemote(t, addr, "", "put", "city", "İstanbul"); code != 0 || out != "OK\n" {
		t.Fatalf("put: exit %d, %q %q", code, out, errOut)
	}
	if out, _, code := runRemote(t, addr, "", "get", "city"); code != 0 || out != "\"İstanbul\"\n" {
		t.Fatalf("get: exit %d, %q", code, out)
	}
	if _, errOut, code := runRemote(t, addr, "", "bogus"); code != 1 || !strings.Contains(errOut, "unknown command") {
		t.Fatalf("bogus: exit %d, %q", code, errOut)
	}
	if _, errOut, code := runRemote(t, addr, "", "begin"); code != 1 || !strings.Contains(errOut, "use the prompt or a script") {
		t.Fatalf("begin: exit %d, %q", code, errOut)
	}
	// Nobody listening.
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	dead := ln.Addr().String()
	ln.Close()
	if _, errOut, code := runRemote(t, dead, "", "get", "x"); code != 1 || !strings.Contains(errOut, "cannot connect") {
		t.Fatalf("no server: exit %d, %q", code, errOut)
	}
	// -connect with -dir is a usage error.
	var out, errOut bytes.Buffer
	if code := run([]string{"-connect", addr, "-dir", "x"}, strings.NewReader(""), &out, &errOut, false); code != 2 {
		t.Fatalf("-connect -dir: exit %d", code)
	}
}

// TestRemoteTransactionLeftOpen: a client that exits with an open
// transaction rolls it back; nothing reaches the server's database.
func TestRemoteTransactionLeftOpen(t *testing.T) {
	addr := startServer(t)
	out, _, _ := runRemote(t, addr, "begin\nput left open\n")
	if !strings.Contains(out, "open transaction rolled back, 1 write discarded") {
		t.Fatalf("output %q", out)
	}
	if out, _, _ := runRemote(t, addr, "", "get", "left"); out != "(not found)\n" {
		t.Fatalf("left = %q", out)
	}
}

func TestRemotePrompt(t *testing.T) {
	addr := startServer(t)
	var out, errOut bytes.Buffer
	run([]string{"-connect", addr}, strings.NewReader("begin\nput a 1\ncommit\n"), &out, &errOut, true)
	for _, want := range []string{"connected to " + addr, "basedb@" + addr + "> ", "basedb@" + addr + "(tx)> "} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q in:\n%s", want, out.String())
		}
	}
}

// TestServeMode runs the program as a server, uses it, and stops it with a
// simulated Ctrl+C: the database is closed cleanly.
func TestServeMode(t *testing.T) {
	dir := t.TempDir()
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := ln.Addr().String()
	ln.Close()

	stop := make(chan os.Signal, 1)
	var out, errOut bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- serveMode(dir, addr, engine.Options{NoSync: true}, server.Options{Dir: dir}, &out, &errOut, stop)
	}()
	var err error
	for i := 0; i < 200; i++ { // wait until it listens
		var c net.Conn
		if c, err = net.Dial("tcp", addr); err == nil {
			c.Close()
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("server did not start: %v", err)
	}
	if o, e, code := runRemote(t, addr, "put k v\nget k\n"); code != 0 || o != "OK\n\"v\"\n" {
		t.Fatalf("client: exit %d %q %q", code, o, e)
	}

	stop <- syscall.SIGTERM
	select {
	case code := <-done:
		if code != 0 {
			t.Fatalf("serve exited %d: %s", code, errOut.String())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("server did not stop")
	}
	if !strings.Contains(out.String(), "listening on") || !strings.Contains(out.String(), "stopped") {
		t.Fatalf("server output %q", out.String())
	}
	db, err := engine.Open(dir, engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if st := db.RecoveryStats(); st.RecordsApplied != 0 {
		t.Fatal("server did not close the database cleanly")
	}
	if v, ok, _ := db.Get([]byte("k")); !ok || string(v) != "v" {
		t.Fatal("data written through the server is missing")
	}
}
