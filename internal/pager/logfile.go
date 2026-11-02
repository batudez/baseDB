package pager

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
)

// ReplaceLogFile atomically replaces the file at path with content:
//
//  1. write content to path+".tmp" and fsync it
//  2. rename it over path (atomic on Windows and POSIX)
//  3. fsync the directory, so the rename itself is durable (POSIX only;
//     NTFS journals the rename and Windows cannot fsync a directory)
//
// A crash at any point leaves either the old file or the new one, never a
// mix. A leftover .tmp file is removed by RemoveStaleLogTemp.
// The caller must have closed its handle on path (Windows cannot rename
// over an open file).
func ReplaceLogFile(path string, content []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("pager: create %s: %w", tmp, err)
	}
	if _, err := f.Write(content); err != nil {
		f.Close()
		return fmt.Errorf("pager: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("pager: sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("pager: rename %s: %w", tmp, err)
	}
	if runtime.GOOS == "windows" {
		return nil
	}
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("pager: sync directory: %w", err)
	}
	return nil
}

// RemoveStaleLogTemp deletes the temporary file a crash during
// ReplaceLogFile may have left behind.
func RemoveStaleLogTemp(path string) error {
	err := os.Remove(path + ".tmp")
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// LogFile is the raw file under the write-ahead log. The WAL package decides
// what to write; the file operations, and in particular fsync, live here so
// that every fsync in the engine stays in this package (see Pager.Sync).
type LogFile struct {
	f *os.File
}

// OpenLogFile opens (or creates) the log file at path.
func OpenLogFile(path string) (*LogFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("pager: open log %s: %w", path, err)
	}
	return &LogFile{f: f}, nil
}

// ReadAt reads len(b) bytes at offset off (see os.File.ReadAt).
func (l *LogFile) ReadAt(b []byte, off int64) (int, error) {
	return l.f.ReadAt(b, off)
}

// WriteAt writes b at offset off. The data is not durable until Sync.
func (l *LogFile) WriteAt(b []byte, off int64) error {
	if _, err := l.f.WriteAt(b, off); err != nil {
		return fmt.Errorf("pager: write log at %d: %w", off, err)
	}
	return nil
}

// Size returns the current file size.
func (l *LogFile) Size() (int64, error) {
	st, err := l.f.Stat()
	if err != nil {
		return 0, fmt.Errorf("pager: stat log: %w", err)
	}
	return st.Size(), nil
}

// Truncate cuts the file to size bytes (used to drop a torn log tail).
func (l *LogFile) Truncate(size int64) error {
	if err := l.f.Truncate(size); err != nil {
		return fmt.Errorf("pager: truncate log: %w", err)
	}
	return nil
}

// Sync flushes everything written so far to stable storage (fsync).
func (l *LogFile) Sync() error {
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("pager: sync log: %w", err)
	}
	return nil
}

// Close closes the file. It does not sync.
func (l *LogFile) Close() error {
	return l.f.Close()
}
