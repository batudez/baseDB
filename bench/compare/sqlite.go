//go:build cgo

package main

// SQLite through its C API (MSYS2 package mingw-w64-ucrt-x86_64-sqlite3),
// not database/sql: that layer costs about as much as a cached read, and
// here every operation is one cgo call into C, like for the other C stores.
//
// Settings for a key-value store with the same guarantees as the others:
// a WITHOUT ROWID table (the rows live in the primary-key B-tree itself),
// WAL journal, synchronous=FULL (fsync of the WAL on every commit), and a
// page cache of -cache MiB.

/*
#cgo LDFLAGS: -lsqlite3
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include "ops.h"

typedef struct {
	sqlite3 *db;
	sqlite3_stmt *put, *get, *del, *scan, *begin, *commit, *rollback;
} sq;

static int sq_prep(sqlite3 *db, const char *sql, sqlite3_stmt **st) {
	return sqlite3_prepare_v2(db, sql, -1, st, NULL);
}

static const char *sq_open(const char *path, int sync, int cache_kib, sq **out) {
	sq *s = calloc(1, sizeof(sq));
	char pragmas[256];
	int r = sqlite3_open_v2(path, &s->db, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX, NULL);
	if (r != SQLITE_OK) return sqlite3_errstr(r);
	snprintf(pragmas, sizeof pragmas,
		"PRAGMA journal_mode=WAL; PRAGMA synchronous=%s; PRAGMA cache_size=-%d;"
		"CREATE TABLE IF NOT EXISTS kv(k BLOB PRIMARY KEY, v BLOB) WITHOUT ROWID;",
		sync ? "FULL" : "OFF", cache_kib);
	if (sqlite3_exec(s->db, pragmas, NULL, NULL, NULL) != SQLITE_OK) return sqlite3_errmsg(s->db);
	if (sq_prep(s->db, "INSERT OR REPLACE INTO kv(k, v) VALUES(?, ?)", &s->put) ||
	    sq_prep(s->db, "SELECT v FROM kv WHERE k = ?", &s->get) ||
	    sq_prep(s->db, "DELETE FROM kv WHERE k = ?", &s->del) ||
	    sq_prep(s->db, "SELECT k, v FROM kv WHERE k >= ? ORDER BY k LIMIT ?", &s->scan) ||
	    sq_prep(s->db, "BEGIN IMMEDIATE", &s->begin) ||
	    sq_prep(s->db, "COMMIT", &s->commit) ||
	    sq_prep(s->db, "ROLLBACK", &s->rollback))
		return sqlite3_errmsg(s->db);
	*out = s;
	return NULL;
}

static int sq_step(sqlite3_stmt *st) {
	int r = sqlite3_step(st);
	sqlite3_reset(st);
	sqlite3_clear_bindings(st);
	return r == SQLITE_DONE ? SQLITE_OK : r;
}

static int sq_write(sq *s, int del, const void *k, int kl, const void *v, int vl) {
	sqlite3_stmt *st = del ? s->del : s->put;
	sqlite3_bind_blob(st, 1, k, kl, SQLITE_STATIC);
	if (!del) sqlite3_bind_blob(st, 2, v, vl, SQLITE_STATIC);
	return sq_step(st);
}

static const char *sq_put(sq *s, const void *k, int kl, const void *v, int vl) {
	return sq_write(s, 0, k, kl, v, vl) == SQLITE_OK ? NULL : sqlite3_errmsg(s->db);
}

static const char *sq_batch(sq *s, const char *buf, size_t len) {
	size_t pos = 0;
	bench_op o;
	if (sq_step(s->begin) != SQLITE_OK) return sqlite3_errmsg(s->db);
	while (bench_next_op(buf, len, &pos, &o)) {
		if (sq_write(s, o.del, o.k, (int)o.kl, o.v, (int)o.vl) != SQLITE_OK) {
			const char *e = sqlite3_errmsg(s->db);
			sq_step(s->rollback);
			return e;
		}
	}
	return sq_step(s->commit) == SQLITE_OK ? NULL : sqlite3_errmsg(s->db);
}

// sq_get copies the value into out. 1 found, 0 not found, -2 out too small
// (*vl = the size needed), -1 error.
static int sq_get(sq *s, const void *k, int kl, void *out, int cap, int *vl) {
	int r, ret;
	sqlite3_bind_blob(s->get, 1, k, kl, SQLITE_STATIC);
	r = sqlite3_step(s->get);
	if (r == SQLITE_ROW) {
		*vl = sqlite3_column_bytes(s->get, 0);
		if (*vl > cap) ret = -2;
		else {
			memcpy(out, sqlite3_column_blob(s->get, 0), *vl);
			ret = 1;
		}
	} else ret = r == SQLITE_DONE ? 0 : -1;
	sqlite3_reset(s->get);
	sqlite3_clear_bindings(s->get);
	return ret;
}

static int sq_scan(sq *s, const void *start, int sl, int n) {
	int count = 0, r;
	// No start: an empty blob, which sorts before every key. (A NULL
	// pointer would bind SQL NULL, and k >= NULL matches nothing.)
	if (sl == 0) sqlite3_bind_zeroblob(s->scan, 1, 0);
	else sqlite3_bind_blob(s->scan, 1, start, sl, SQLITE_STATIC);
	sqlite3_bind_int(s->scan, 2, n);
	while ((r = sqlite3_step(s->scan)) == SQLITE_ROW) {
		(void)sqlite3_column_blob(s->scan, 0);
		(void)sqlite3_column_blob(s->scan, 1);
		count++;
	}
	sqlite3_reset(s->scan);
	sqlite3_clear_bindings(s->scan);
	return r == SQLITE_DONE ? count : -1;
}

static int sq_close(sq *s) {
	sqlite3_finalize(s->put); sqlite3_finalize(s->get); sqlite3_finalize(s->del);
	sqlite3_finalize(s->scan); sqlite3_finalize(s->begin); sqlite3_finalize(s->commit);
	sqlite3_finalize(s->rollback);
	int r = sqlite3_close(s->db);
	free(s);
	return r;
}

static const char *sq_errmsg(sq *s) { return sqlite3_errmsg(s->db); }
*/
import "C"

import (
	"errors"
	"path/filepath"
	"sync"
	"unsafe"
)

func init() {
	register("sqlite", storeInfo{
		open:    openSQLite,
		version: func(config) string { return "SQLite " + C.GoString(C.sqlite3_libversion()) + " (C API, MSYS2 package)" },
	})
}

// sqliteStore serializes calls with a mutex: one connection and its
// prepared statements are used by one thread at a time. SQLite allows one
// writer at a time anyway.
type sqliteStore struct {
	mu sync.Mutex
	s  *C.sq
}

// cptr returns a pointer to b's bytes for C (nil for an empty slice).
func cptr(b []byte) unsafe.Pointer {
	if len(b) == 0 {
		return nil
	}
	return unsafe.Pointer(&b[0])
}

func openSQLite(dir string, c config) (Store, error) {
	path := C.CString(filepath.Join(dir, "kv.sqlite"))
	defer C.free(unsafe.Pointer(path))
	fsync := C.int(0)
	if c.sync {
		fsync = 1
	}
	var s *C.sq
	if e := C.sq_open(path, fsync, C.int(c.cacheMiB*1024), &s); e != nil {
		return nil, errors.New("sqlite: " + C.GoString(e))
	}
	return &sqliteStore{s: s}, nil
}

func (s *sqliteStore) Put(k, v []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := C.sq_put(s.s, cptr(k), C.int(len(k)), cptr(v), C.int(len(v))); e != nil {
		return errors.New("sqlite put: " + C.GoString(e))
	}
	return nil
}

func (s *sqliteStore) Batch(ops []op) error {
	buf := encodeOps(ops)
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := C.sq_batch(s.s, (*C.char)(cptr(buf)), C.size_t(len(buf))); e != nil {
		return errors.New("sqlite batch: " + C.GoString(e))
	}
	return nil
}

func (s *sqliteStore) Get(k []byte) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]byte, 256)
	for {
		var n C.int
		switch r := C.sq_get(s.s, cptr(k), C.int(len(k)), cptr(out), C.int(len(out)), &n); r {
		case 1:
			return out[:n], true, nil
		case 0:
			return nil, false, nil
		case -2:
			out = make([]byte, n)
		default:
			return nil, false, errors.New("sqlite get: " + C.GoString(C.sq_errmsg(s.s)))
		}
	}
}

func (s *sqliteStore) Scan(start []byte, n int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r := C.sq_scan(s.s, cptr(start), C.int(len(start)), C.int(n))
	if r < 0 {
		return 0, errors.New("sqlite scan: " + C.GoString(C.sq_errmsg(s.s)))
	}
	return int(r), nil
}

func (s *sqliteStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r := C.sq_close(s.s); r != C.SQLITE_OK {
		return errors.New("sqlite close: " + C.GoString(C.sqlite3_errstr(r)))
	}
	return nil
}
