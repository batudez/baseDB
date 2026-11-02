//go:build unix

package lockfile

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// Lock is a held directory lock.
type Lock struct {
	f *os.File
}

func acquire(path string) (*Lock, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("lockfile: open %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w (%s)", ErrLocked, path)
		}
		return nil, fmt.Errorf("lockfile: lock %s: %w", path, err)
	}
	return &Lock{f: f}, nil
}

// Release gives the lock up (closing the file drops the flock). The lock
// file itself stays on disk.
func (l *Lock) Release() error {
	return l.f.Close()
}
