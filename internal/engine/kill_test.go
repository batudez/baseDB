package engine

import (
	"bufio"
	"bytes"
	"fmt"
	"maps"
	"math/rand"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Real crash test: a child process writes to the database and prints
// "ack <i>" after operation i returned (it is then durable). The parent
// kills the child at a random moment, reopens the database and checks:
//
//   - every acknowledged operation is there
//   - the one operation that may have been in flight is either fully there
//     or not at all
//   - the tree is structurally valid
//
// The next round continues on the same files, so recovery also runs on a
// database that was itself recovered and crashed again.

const (
	envDir   = "BASEDB_KILL_DIR"
	envStart = "BASEDB_KILL_START"
)

// killOp is the i-th operation of the child: mostly inserts, plus updates
// and deletes of earlier keys, with values large enough to cause splits.
func killOp(i int) kvOp {
	switch {
	case i%7 == 6:
		return del(key(i - 3))
	case i%11 == 10:
		return put(key(i-5), bytes.Repeat([]byte{'u'}, 50+i%500))
	default:
		return put(key(i), val(i))
	}
}

// TestKillChild is the child process; it does nothing in a normal test run.
func TestKillChild(t *testing.T) {
	dir := os.Getenv(envDir)
	if dir == "" {
		t.Skip("helper process for TestKillDuringWrites")
	}
	start, _ := strconv.Atoi(os.Getenv(envStart))
	db, err := Open(dir, Options{PoolFrames: 16}) // real fsync
	if err != nil {
		fmt.Println("error", err)
		os.Exit(2)
	}
	for i := start; ; i++ {
		o := killOp(i)
		if o.del {
			_, err = db.Delete(o.k)
		} else {
			err = db.Put(o.k, o.v)
		}
		if err != nil {
			fmt.Println("error", err)
			os.Exit(2)
		}
		fmt.Printf("ack %d\n", i) // unbuffered: written before the next op starts
	}
}

func TestKillDuringWrites(t *testing.T) {
	if testing.Short() {
		t.Skip("starts child processes")
	}
	dir := t.TempDir()
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	model := map[string]string{}
	next := 0

	for round := 0; round < 5; round++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestKillChild$", "-test.count=1")
		cmd.Env = append(os.Environ(), envDir+"="+dir, envStart+"="+strconv.Itoa(next))
		out, err := cmd.StdoutPipe()
		must(t, err)
		must(t, cmd.Start())

		// Kill after a random number of acks, or after a random time (which
		// may hit the child while it is still recovering).
		target := next + 20 + rng.Intn(300)
		timer := time.AfterFunc(time.Duration(50+rng.Intn(1500))*time.Millisecond, func() { cmd.Process.Kill() })
		acked := next - 1
		sc := bufio.NewScanner(out)
		for sc.Scan() { // keep reading after the kill: acks already printed count
			line := sc.Text()
			if strings.HasPrefix(line, "error") {
				t.Fatalf("round %d: child: %s", round, line)
			}
			if n, ok := strings.CutPrefix(line, "ack "); ok {
				acked, _ = strconv.Atoi(n)
				if acked >= target {
					cmd.Process.Kill()
				}
			}
		}
		timer.Stop()
		cmd.Wait() // killed: the error is expected

		for i := next; i <= acked; i++ {
			applyToModel(killOp(i), model)
		}
		inFlight := maps.Clone(model)
		applyToModel(killOp(acked+1), inFlight)

		db := mustOpen(t, dir, fast)
		must(t, db.Verify())
		got := contents(t, db)
		if !maps.Equal(got, model) && !maps.Equal(got, inFlight) {
			t.Fatalf("round %d (killed after ack %d): database matches neither the acknowledged state nor the in-flight one (%d keys, model %d)",
				round, acked, len(got), len(model))
		}
		st := db.RecoveryStats()
		t.Logf("round %d: ops %d..%d acknowledged, recovery %+v", round, next, acked, st)

		// The child re-runs op acked+1, which is idempotent, so the model is
		// complete again after the next round. Alternate how the parent
		// leaves: clean close, or crash right after recovery.
		if round%2 == 0 {
			must(t, db.Close())
		} else {
			db.crash()
		}
		next = acked + 1
	}
}

func applyToModel(o kvOp, model map[string]string) {
	if o.del {
		delete(model, string(o.k))
	} else {
		model[string(o.k)] = string(o.v)
	}
}
