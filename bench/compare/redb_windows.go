package main

// redb (a Rust B-tree store) through baseredb.dll, a thin C ABI over it
// (deps/redb, built by deps/build-redb.ps1). Loaded with LoadLibrary, so it
// needs no cgo; every operation is one call into the DLL. redb's default
// durability (Immediate) fsyncs every commit. One writer at a time.

import (
	"errors"
	"fmt"
	"path/filepath"
	"syscall"
	"unsafe"
)

func init() {
	register("redb", storeInfo{open: openRedb, version: func(c config) string {
		d, err := loadRedb(c.redbDLL)
		if err != nil {
			return "redb (not loaded: " + err.Error() + ")"
		}
		buf := make([]byte, 64)
		n, _, _ := d.version.Call(uintptr(up(buf)), uintptr(len(buf)))
		return "redb " + string(buf[:n]) + " (Rust, through a C ABI DLL)"
	}})
}

type redbDLL struct {
	open, close, put, batch, get, scan, version *syscall.LazyProc
}

var redbLoaded *redbDLL

func loadRedb(path string) (*redbDLL, error) {
	if redbLoaded != nil {
		return redbLoaded, nil
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	dll := syscall.NewLazyDLL(abs)
	if err := dll.Load(); err != nil {
		return nil, fmt.Errorf("%s: %v (build it with deps/build-redb.ps1)", abs, err)
	}
	redbLoaded = &redbDLL{
		open: dll.NewProc("baseredb_open"), close: dll.NewProc("baseredb_close"),
		put: dll.NewProc("baseredb_put"), batch: dll.NewProc("baseredb_batch"),
		get: dll.NewProc("baseredb_get"), scan: dll.NewProc("baseredb_scan"),
		version: dll.NewProc("baseredb_version"),
	}
	return redbLoaded, nil
}

// up returns a pointer to b's bytes for the DLL (nil for an empty slice).
// Callers convert it with uintptr(up(b)) inside the Call argument list: only
// there does Go keep the memory alive and in place during the call.
func up(b []byte) unsafe.Pointer {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Pointer(&b[0])
}

type redbStore struct {
	d  *redbDLL
	db uintptr
}

var errRedb = errors.New("redb call failed (details on stderr)")

func openRedb(dir string, c config) (Store, error) {
	if !c.sync {
		return nil, errors.New("redb: -sync=false is not supported by this wrapper")
	}
	d, err := loadRedb(c.redbDLL)
	if err != nil {
		return nil, err
	}
	path := []byte(filepath.Join(dir, "kv.redb"))
	db, _, _ := d.open.Call(uintptr(up(path)), uintptr(len(path)), uintptr(c.cacheMiB)<<20)
	if db == 0 {
		return nil, errRedb
	}
	return &redbStore{d, db}, nil
}

func (s *redbStore) Put(k, v []byte) error {
	r, _, _ := s.d.put.Call(s.db, uintptr(up(k)), uintptr(len(k)), uintptr(up(v)), uintptr(len(v)))
	if int32(r) != 0 {
		return errRedb
	}
	return nil
}

func (s *redbStore) Batch(ops []op) error {
	buf := encodeOps(ops)
	r, _, _ := s.d.batch.Call(s.db, uintptr(up(buf)), uintptr(len(buf)))
	if int32(r) != 0 {
		return errRedb
	}
	return nil
}

func (s *redbStore) Get(k []byte) ([]byte, bool, error) {
	out := make([]byte, 256)
	for {
		var n uintptr
		r, _, _ := s.d.get.Call(s.db, uintptr(up(k)), uintptr(len(k)), uintptr(up(out)), uintptr(len(out)), uintptr(unsafe.Pointer(&n)))
		switch int32(r) {
		case 1:
			return out[:n], true, nil
		case 0:
			return nil, false, nil
		case -2:
			out = make([]byte, n)
		default:
			return nil, false, errRedb
		}
	}
}

func (s *redbStore) Scan(start []byte, n int) (int, error) {
	r, _, _ := s.d.scan.Call(s.db, uintptr(up(start)), uintptr(len(start)), uintptr(n))
	if int64(r) < 0 {
		return 0, errRedb
	}
	return int(r), nil
}

func (s *redbStore) Close() error {
	s.d.close.Call(s.db)
	return nil
}
