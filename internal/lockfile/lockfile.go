// Package lockfile gives one process exclusive use of a database directory.
//
// The lock is held by the operating system on an open file handle, not by
// the file's existence. When the process exits or crashes, the OS closes the
// handle and the lock disappears, so a crash never leaves a stale lock behind
// (unlike a "PID file" that would have to be cleaned up by hand).
//
//	Windows: the file is opened with share mode 0 (no other handle may open it)
//	Unix:    flock(LOCK_EX|LOCK_NB) on the file
//
// Both also exclude a second Open of the same directory inside one process.
package lockfile

import "errors"

// ErrLocked means another process (or another open DB in this process)
// already holds the lock.
var ErrLocked = errors.New("lockfile: database is in use by another process")

// Acquire takes the lock on the file at path, creating the file if needed.
// It does not wait: if the lock is taken it returns ErrLocked immediately.
func Acquire(path string) (*Lock, error) {
	return acquire(path)
}
