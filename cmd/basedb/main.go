// Command basedb is the terminal interface to baseDB, and its server.
//
// Local (the program opens the database itself):
//
//	basedb -dir ./mydb                     interactive prompt
//	basedb -dir ./mydb get user:1          one command
//	basedb -dir ./mydb < commands.txt      a script
//
// Server (many clients at once, over TCP):
//
//	basedb -dir ./mydb -serve 127.0.0.1:7070
//
// Client (the same prompt and commands, talking to a server):
//
//	basedb -connect 127.0.0.1:7070
//	basedb -connect 127.0.0.1:7070 get user:1
//
// The database directory is created if it does not exist. Type help for the
// list of commands.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"baseDB/client"
	"baseDB/internal/engine"
	"baseDB/internal/protocol"
	"baseDB/internal/server"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr, isTerminal(os.Stdin)))
}

// isTerminal reports whether f is a console (not a file or a pipe).
func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// run is the whole program; main only connects it to the real stdin/out.
// It returns the process exit code: 0 ok, 1 a command or the database
// failed, 2 bad command-line flags.
func run(args []string, stdin io.Reader, stdout, stderr io.Writer, terminal bool) int {
	fs := flag.NewFlagSet("basedb", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", "basedb-data", "database directory (created if missing)")
	pool := fs.Int("pool", engine.DefaultPoolFrames, "buffer pool size in pages of 4 KiB")
	prompt := fs.Bool("prompt", terminal, "show the basedb> prompt (on by default when stdin is a terminal)")
	serve := fs.String("serve", "", "run as a server on this address, e.g. 127.0.0.1:7070 (0.0.0.0:7070 is open to the network: there is no authentication)")
	connect := fs.String("connect", "", "connect to a basedb server at this address instead of opening -dir")
	maxConns := fs.Int("max-conns", server.DefaultMaxConns, "server: clients served at once")
	idle := fs.Duration("idle-timeout", server.DefaultIdleTimeout, "server: disconnect clients idle this long (their open transaction is rolled back)")
	groupCommit := fs.Bool("group-commit", true, "share one fsync between concurrent commits")
	fs.Usage = func() {
		fmt.Fprintf(stderr, `usage:
  basedb [flags]                              interactive prompt on -dir
  basedb [flags] <command> [args]             run one command
  basedb -dir <dir> -serve <host:port>        run as a server
  basedb -connect <host:port> [command]       talk to a server

flags:
`)
		fs.PrintDefaults()
		fmt.Fprintf(stderr, "\nType help at the prompt (or: basedb help) for the commands.\n")
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	set := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { set[f.Name] = true })
	if *connect != "" && (set["dir"] || *serve != "") {
		fmt.Fprintln(stderr, "basedb: -connect talks to a server, it cannot be combined with -dir or -serve")
		return 2
	}

	opts := engine.Options{PoolFrames: *pool, NoGroupCommit: !*groupCommit}
	if *serve != "" {
		if fs.NArg() > 0 {
			fmt.Fprintln(stderr, "basedb: -serve takes no command")
			return 2
		}
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(sigs)
		return serveMode(*dir, *serve, opts, server.Options{MaxConns: *maxConns, IdleTimeout: *idle, Dir: *dir}, stdout, stderr, sigs)
	}

	var b backend
	where := ""
	if *connect != "" {
		c, err := client.Dial(*connect)
		if err == nil {
			err = c.Ping() // a refused (BUSY) connection only shows on the first answer
		}
		if err != nil {
			fmt.Fprintf(stderr, "basedb: cannot connect to %s: %s\n", *connect, friendly(err, ""))
			return 1
		}
		b, where = &remote{c: c}, *connect
	} else {
		db, err := engine.Open(*dir, opts)
		if err != nil {
			fmt.Fprintf(stderr, "basedb: %s\n", friendly(err, *dir))
			return 1
		}
		b = &local{db: db, sess: server.NewSession(db, *dir)}
	}
	sh := &shell{b: b, out: stdout, where: where}

	if fs.NArg() > 0 { // single command
		sh.single = true
		err := sh.exec(fs.Args())
		cerr := b.Close()
		if err != nil && !errors.Is(err, errExit) {
			fmt.Fprintf(stderr, "error: %s\n", err)
			return 1
		}
		if cerr != nil && *connect == "" {
			fmt.Fprintf(stderr, "error while closing: %v\n", cerr)
			return 1
		}
		return 0
	}

	stop := closeOnInterrupt(b.Close, stderr)
	defer stop()
	if *prompt {
		if where != "" {
			fmt.Fprintf(stdout, "baseDB: connected to %s. Type help for commands, exit to quit.\n", where)
		} else {
			fmt.Fprintf(stdout, "baseDB: database in %s. Type help for commands, exit to quit.\n", *dir)
		}
	}
	code := repl(sh, stdin, stdout, stderr, *prompt)
	sh.finish() // a transaction left open is rolled back, never committed
	if err := b.Close(); err != nil && *connect == "" {
		fmt.Fprintf(stderr, "error while closing: %v\n", err)
		code = 1
	}
	return code
}

// serveMode runs the server until a signal arrives on stop, then shuts
// down: no new clients, every client disconnected (their open transactions
// rolled back), then the database closed cleanly.
func serveMode(dir, addr string, opts engine.Options, sopts server.Options, stdout, stderr io.Writer, stop <-chan os.Signal) int {
	db, err := engine.Open(dir, opts)
	if err != nil {
		fmt.Fprintf(stderr, "basedb: %s\n", friendly(err, dir))
		return 1
	}
	srv := server.New(db, sopts)
	if err := srv.Listen(addr); err != nil {
		db.Close()
		fmt.Fprintf(stderr, "basedb: %v\n", err)
		return 1
	}
	for srv.Addr() == nil {
		time.Sleep(time.Millisecond)
	}
	fmt.Fprintf(stdout, "baseDB server: database in %s, listening on %s (Ctrl+C to stop)\n", dir, srv.Addr())

	<-stop
	fmt.Fprintln(stdout, "shutting down: disconnecting clients, closing the database...")
	srv.Close()
	if err := db.Close(); err != nil {
		fmt.Fprintf(stderr, "error while closing: %v\n", err)
		return 1
	}
	st := srv.Stats()
	fmt.Fprintf(stdout, "stopped (%d connections served)\n", st.Accepted)
	return 0
}

// repl reads commands line by line until exit or end of input. With a
// prompt, errors are shown and the session goes on; without one (a script),
// the exit code is 1 if any command failed.
func repl(sh *shell, stdin io.Reader, stdout, stderr io.Writer, prompt bool) int {
	in := bufio.NewScanner(stdin)
	in.Buffer(make([]byte, 64<<10), 1<<20)
	failed := false
	for {
		if prompt {
			fmt.Fprint(stdout, sh.prompt())
		}
		if !in.Scan() {
			break
		}
		line := strings.TrimSpace(in.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		args, err := protocol.Tokenize(line)
		if err == nil {
			err = sh.exec(args)
		}
		if errors.Is(err, errExit) {
			return exitCode(failed, prompt)
		}
		if err != nil {
			fmt.Fprintf(stderr, "error: %s\n", err)
			failed = true
		}
	}
	if err := in.Err(); err != nil {
		fmt.Fprintf(stderr, "error reading input: %v\n", err)
		return 1
	}
	if prompt {
		fmt.Fprintln(stdout) // end of input (Ctrl+D / Ctrl+Z): finish the prompt line
	}
	return exitCode(failed, prompt)
}

func exitCode(failed, prompt bool) int {
	if failed && !prompt {
		return 1
	}
	return 0
}

// closeOnInterrupt closes cleanly when the user presses Ctrl+C (or the
// process is asked to stop), then exits. Locally that closes the database;
// without it the data would still be safe (recovery replays the log at the
// next start), but a clean Close leaves nothing to replay. The returned
// function stops watching.
func closeOnInterrupt(closer func() error, stderr io.Writer) (stop func()) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	stopWatching := watchSignals(closer, stderr, sigs, os.Exit)
	return func() {
		signal.Stop(sigs)
		stopWatching()
	}
}

// watchSignals waits for a signal on sigs, then calls closer and exit with
// 130 (the usual code for "stopped by Ctrl+C"). Separated from
// closeOnInterrupt so tests can send a signal without a real Ctrl+C.
func watchSignals(closer func() error, stderr io.Writer, sigs <-chan os.Signal, exit func(int)) (stop func()) {
	done := make(chan struct{})
	go func() {
		select {
		case <-sigs:
			fmt.Fprintln(stderr, "\ninterrupted: closing...")
			code := 130
			if err := closer(); err != nil && !errors.Is(err, engine.ErrClosed) {
				fmt.Fprintf(stderr, "error while closing: %v\n", err)
				code = 1
			}
			exit(code)
		case <-done:
		}
	}()
	return func() { close(done) }
}
