package lockfile

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestSecondAcquireFails(t *testing.T) {
	path := filepath.Join(t.TempDir(), "LOCK")
	l, err := Acquire(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(path); !errors.Is(err, ErrLocked) {
		t.Fatalf("second Acquire = %v, want ErrLocked", err)
	}
	if err := l.Release(); err != nil {
		t.Fatal(err)
	}
	l2, err := Acquire(path) // free again after Release
	if err != nil {
		t.Fatalf("Acquire after Release: %v", err)
	}
	l2.Release()
}

// TestLockChild holds the lock until it is killed.
func TestLockChild(t *testing.T) {
	path := os.Getenv("BASEDB_LOCK_PATH")
	if path == "" {
		t.Skip("helper process for TestLockAcrossProcesses")
	}
	if _, err := Acquire(path); err != nil {
		os.Stdout.WriteString("error " + err.Error() + "\n")
		os.Exit(2)
	}
	os.Stdout.WriteString("locked\n")
	time.Sleep(time.Minute)
}

// TestLockAcrossProcesses: while a child process holds the lock, the parent
// cannot take it; once the child is killed (as in a crash), the OS releases
// the lock and the parent can.
func TestLockAcrossProcesses(t *testing.T) {
	path := filepath.Join(t.TempDir(), "LOCK")
	cmd := exec.Command(os.Args[0], "-test.run=^TestLockChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), "BASEDB_LOCK_PATH="+path)
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, _ := out.Read(buf)
	if string(buf[:n]) != "locked\n" {
		cmd.Process.Kill()
		t.Fatalf("child: %q", buf[:n])
	}

	if _, err := Acquire(path); !errors.Is(err, ErrLocked) {
		cmd.Process.Kill()
		t.Fatalf("Acquire while the child holds the lock = %v, want ErrLocked", err)
	}
	cmd.Process.Kill()
	cmd.Wait()

	l, err := Acquire(path)
	if err != nil {
		t.Fatalf("lock not released when the child died: %v", err)
	}
	l.Release()
}
