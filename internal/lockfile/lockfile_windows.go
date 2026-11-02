//go:build windows

package lockfile

import (
	"fmt"
	"syscall"
)

// errSharingViolation is ERROR_SHARING_VIOLATION: the file is open elsewhere
// with a share mode that excludes us.
const errSharingViolation syscall.Errno = 32

// Lock is a held directory lock.
type Lock struct {
	h syscall.Handle
}

func acquire(path string) (*Lock, error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	h, err := syscall.CreateFile(p,
		syscall.GENERIC_READ|syscall.GENERIC_WRITE,
		0, // share mode 0: nobody else may open the file while we hold it
		nil,
		syscall.OPEN_ALWAYS,
		syscall.FILE_ATTRIBUTE_NORMAL,
		0)
	if err == errSharingViolation {
		return nil, fmt.Errorf("%w (%s)", ErrLocked, path)
	}
	if err != nil {
		return nil, fmt.Errorf("lockfile: open %s: %w", path, err)
	}
	return &Lock{h: h}, nil
}

// Release gives the lock up. The lock file itself stays on disk.
func (l *Lock) Release() error {
	return syscall.CloseHandle(l.h)
}
