package main

import (
	"bytes"
	"os"
	"strings"
	"testing"
	"time"

	"baseDB/internal/engine"
)

// runScript runs basedb on dir with a script on stdin (no prompt).
func runScript(t *testing.T, dir, script string, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	var out, errOut bytes.Buffer
	code = run(append([]string{"-dir", dir}, args...), strings.NewReader(script), &out, &errOut, false)
	return out.String(), errOut.String(), code
}

func TestScriptSession(t *testing.T) {
	dir := t.TempDir()
	out, errOut, code := runScript(t, dir, `
# comments and blank lines are ignored
put user:1 "Batuhan"
put user:2 "Ayşe Yılmaz"
put order:7 "3 items"
get user:1
scan user: user;
prefix order:
count
delete user:1
get user:1
delete user:1
`)
	if code != 0 || errOut != "" {
		t.Fatalf("exit %d, stderr %q", code, errOut)
	}
	want := `OK
OK
OK
"Batuhan"
user:1 → "Batuhan"
user:2 → "Ayşe Yılmaz"
(2 keys)
order:7 → "3 items"
(1 key)
3
OK
(not found)
(not found)
`
	if out != want {
		t.Fatalf("output:\n%s\nwant:\n%s", out, want)
	}
}

func TestDataSurvivesBetweenRuns(t *testing.T) {
	dir := t.TempDir()
	if _, errOut, code := runScript(t, dir, "", "put", "city", "İstanbul"); code != 0 {
		t.Fatalf("put: exit %d: %s", code, errOut)
	}
	out, _, code := runScript(t, dir, "", "get", "city")
	if code != 0 || out != "\"İstanbul\"\n" {
		t.Fatalf("get after restart: exit %d, %q", code, out)
	}
}

func TestErrorsAreClear(t *testing.T) {
	dir := t.TempDir()
	long := strings.Repeat("x", 300)
	cases := []struct{ line, want string }{
		{"bogus", `unknown command "bogus"`},
		{"put onlykey", "usage: put <key> <value>"},
		{"get", "usage: get <key>"},
		{`put k "unterminated`, "missing closing quote"},
		{"put " + long + " v", "key is 300 bytes, the limit is 256"},
		{"put k " + strings.Repeat("v", 1500), "value is 1500 bytes, the limit is 1024"},
	}
	for _, c := range cases {
		_, errOut, code := runScript(t, dir, c.line+"\n")
		if code != 1 || !strings.Contains(errOut, c.want) {
			t.Errorf("%.30q: exit %d, stderr %q; want exit 1 and %q", c.line, code, errOut, c.want)
		}
	}
	// A failing command does not stop the script; later commands still run.
	out, _, code := runScript(t, dir, "bogus\nput a 1\nget a\n")
	if code != 1 || out != "OK\n\"1\"\n" {
		t.Fatalf("script after error: exit %d, %q", code, out)
	}
}

func TestSecondProcessIsRefused(t *testing.T) {
	dir := t.TempDir()
	db, err := engine.Open(dir, engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, errOut, code := runScript(t, dir, "", "get", "k")
	if code != 1 || !strings.Contains(errOut, "already open in another basedb process") {
		t.Fatalf("exit %d, stderr %q", code, errOut)
	}
}

func TestAdminCommands(t *testing.T) {
	dir := t.TempDir()
	var script strings.Builder
	for i := 0; i < 300; i++ {
		script.WriteString("put key" + strings.Repeat("0", 3) + string(rune('a'+i%26)) + string(rune('a'+i/26)) + " value\n")
	}
	script.WriteString("stats\nverify\ncheckpoint\nexit\nput never run\n")
	out, errOut, code := runScript(t, dir, script.String())
	if code != 0 || errOut != "" {
		t.Fatalf("exit %d, stderr %q", code, errOut)
	}
	for _, want := range []string{"keys            300", "tree            height 2", "buffer pool", "log ", "OK: tree and free list are consistent", "OK: log "} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
	// exit stopped the script: "put never run" did not happen.
	out, _, _ = runScript(t, dir, "", "get", "never")
	if out != "(not found)\n" {
		t.Fatalf("command after exit was run: %q", out)
	}
}

func TestHelpAndUsage(t *testing.T) {
	out, _, code := runScript(t, t.TempDir(), "", "help")
	if code != 0 || !strings.Contains(out, "put <key> <value>") || !strings.Contains(out, "checkpoint") {
		t.Fatalf("help: exit %d\n%s", code, out)
	}
	var errOut bytes.Buffer
	if code := run([]string{"-nosuchflag"}, strings.NewReader(""), &bytes.Buffer{}, &errOut, false); code != 2 {
		t.Fatalf("bad flag: exit %d", code)
	}
	if !strings.Contains(errOut.String(), "usage:") {
		t.Fatalf("bad flag shows no usage: %q", errOut.String())
	}
}

func TestPromptMode(t *testing.T) {
	var out, errOut bytes.Buffer
	code := run([]string{"-dir", t.TempDir()}, strings.NewReader("put a 1\nbogus\nexit\n"), &out, &errOut, true)
	// With a prompt, errors are shown but do not change the exit code.
	if code != 0 || strings.Count(out.String(), "basedb> ") != 3 || !strings.Contains(errOut.String(), "unknown command") {
		t.Fatalf("exit %d\nstdout %q\nstderr %q", code, out.String(), errOut.String())
	}
}

// TestInterruptClosesCleanly sends a simulated Ctrl+C: the database must be
// closed (so its directory can be opened again with nothing to recover) and
// the exit code must be 130.
func TestInterruptClosesCleanly(t *testing.T) {
	dir := t.TempDir()
	db, err := engine.Open(dir, engine.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	sigs := make(chan os.Signal, 1)
	exited := make(chan int, 1)
	var errOut bytes.Buffer
	stop := watchSignals(db.Close, &errOut, sigs, func(code int) { exited <- code })
	defer stop()

	sigs <- os.Interrupt
	select {
	case code := <-exited:
		if code != 130 {
			t.Fatalf("exit code %d, want 130", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no exit after the interrupt")
	}
	if err := db.Put([]byte("k2"), nil); err != engine.ErrClosed {
		t.Fatalf("database still open after interrupt: %v", err)
	}
	db2, err := engine.Open(dir, engine.Options{})
	if err != nil {
		t.Fatalf("reopen after interrupt: %v", err)
	}
	defer db2.Close()
	if st := db2.RecoveryStats(); st.RecordsApplied != 0 {
		t.Fatalf("interrupt did not close cleanly, recovery redid %d records", st.RecordsApplied)
	}
	if v, ok, _ := db2.Get([]byte("k")); !ok || string(v) != "v" {
		t.Fatal("data lost")
	}
}

func TestTransactionCommands(t *testing.T) {
	dir := t.TempDir()
	out, errOut, code := runScript(t, dir, `
put acct:1 100
put acct:2 100
begin
put acct:1 90
put acct:2 110
get acct:1
commit
begin
put acct:1 0
delete acct:2
rollback
scan acct: acct;
begin
put left open
`)
	if code != 0 || errOut != "" {
		t.Fatalf("exit %d, stderr %q", code, errOut)
	}
	want := `OK
OK
OK: transaction started
OK
OK
"90"
OK: committed 2 writes
OK: transaction started
OK
OK
OK: rolled back, 2 writes discarded
acct:1 → "90"
acct:2 → "110"
(2 keys)
OK: transaction started
OK
open transaction rolled back, 1 write discarded
`
	if out != want {
		t.Fatalf("output:\n%s\nwant:\n%s", out, want)
	}
	// The transaction left open at the end of the script was not committed.
	if out, _, _ := runScript(t, dir, "", "get", "left"); out != "(not found)\n" {
		t.Fatalf("open transaction was committed: %q", out)
	}
}

func TestTransactionErrors(t *testing.T) {
	dir := t.TempDir()
	cases := []struct{ script, want string }{
		{"commit\n", "no open transaction"},
		{"rollback\n", "no open transaction"},
		{"begin\nbegin\n", "already open"},
	}
	for _, c := range cases {
		_, errOut, code := runScript(t, dir, c.script)
		if code != 1 || !strings.Contains(errOut, c.want) {
			t.Errorf("%q: exit %d, stderr %q; want %q", c.script, code, errOut, c.want)
		}
	}
	_, errOut, code := runScript(t, dir, "", "begin")
	if code != 1 || !strings.Contains(errOut, "use the prompt or a script") {
		t.Fatalf("begin as a single command: exit %d, %q", code, errOut)
	}
}

func TestPromptShowsTransaction(t *testing.T) {
	var out, errOut bytes.Buffer
	run([]string{"-dir", t.TempDir()}, strings.NewReader("begin\nput a 1\ncommit\nexit\n"), &out, &errOut, true)
	for _, want := range []string{"basedb(tx)> ", "basedb> "} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("prompt %q missing in:\n%s", want, out.String())
		}
	}
}
