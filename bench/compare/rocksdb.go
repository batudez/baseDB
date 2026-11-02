//go:build cgo

package main

// RocksDB through its C API (MSYS2 package mingw-w64-ucrt-x86_64-rocksdb).
// Default options, except: a block cache of -cache MiB, and sync=true on
// every write (fsync of the write-ahead log before the write returns).
// Every operation is one cgo call; scans and copies happen in C.

/*
#cgo LDFLAGS: -lrocksdb
#include <rocksdb/c.h>
#include <stdlib.h>
#include <string.h>
#include "ops.h"

typedef struct {
	rocksdb_t *db;
	rocksdb_options_t *opts;
	rocksdb_block_based_table_options_t *table;
	rocksdb_cache_t *cache;
	rocksdb_writeoptions_t *w;
	rocksdb_readoptions_t *r;
} rdb;

static char *rdb_open(const char *dir, int sync, size_t cache_bytes, rdb **out) {
	char *err = NULL;
	rdb *s = calloc(1, sizeof(rdb));
	s->opts = rocksdb_options_create();
	rocksdb_options_set_create_if_missing(s->opts, 1);
	s->cache = rocksdb_cache_create_lru(cache_bytes);
	s->table = rocksdb_block_based_options_create();
	rocksdb_block_based_options_set_block_cache(s->table, s->cache);
	rocksdb_options_set_block_based_table_factory(s->opts, s->table);
	s->w = rocksdb_writeoptions_create();
	rocksdb_writeoptions_set_sync(s->w, sync);
	s->r = rocksdb_readoptions_create();
	s->db = rocksdb_open(s->opts, dir, &err);
	if (err != NULL) return err;
	*out = s;
	return NULL;
}

static char *rdb_put(rdb *s, const char *k, size_t kl, const char *v, size_t vl) {
	char *err = NULL;
	rocksdb_put(s->db, s->w, k, kl, v, vl, &err);
	return err;
}

static char *rdb_batch(rdb *s, const char *buf, size_t len) {
	char *err = NULL;
	size_t pos = 0;
	bench_op o;
	rocksdb_writebatch_t *b = rocksdb_writebatch_create();
	while (bench_next_op(buf, len, &pos, &o)) {
		if (o.del) rocksdb_writebatch_delete(b, o.k, o.kl);
		else rocksdb_writebatch_put(b, o.k, o.kl, o.v, o.vl);
	}
	rocksdb_write(s->db, s->w, b, &err);
	rocksdb_writebatch_destroy(b);
	return err;
}

// rdb_get: 1 found (copied into out), 0 not found, -2 out too small
// (*vl = size needed), -1 error (*err set).
static int rdb_get(rdb *s, const char *k, size_t kl, char *out, size_t cap, size_t *vl, char **err) {
	char *v = rocksdb_get(s->db, s->r, k, kl, vl, err);
	if (*err != NULL) return -1;
	if (v == NULL) return 0;
	int ret = 1;
	if (*vl > cap) ret = -2;
	else memcpy(out, v, *vl);
	rocksdb_free(v);
	return ret;
}

static long rdb_scan(rdb *s, const char *start, size_t sl, long n, char **err) {
	long count = 0;
	size_t len;
	rocksdb_iterator_t *it = rocksdb_create_iterator(s->db, s->r);
	if (sl == 0) rocksdb_iter_seek_to_first(it);
	else rocksdb_iter_seek(it, start, sl);
	for (; count < n && rocksdb_iter_valid(it); rocksdb_iter_next(it)) {
		(void)rocksdb_iter_key(it, &len);
		(void)rocksdb_iter_value(it, &len);
		count++;
	}
	rocksdb_iter_get_error(it, err);
	rocksdb_iter_destroy(it);
	return count;
}

// rdb_close flushes the memtable (like Pebble's Close here), then closes.
static char *rdb_close(rdb *s) {
	char *err = NULL;
	rocksdb_flushoptions_t *f = rocksdb_flushoptions_create();
	rocksdb_flushoptions_set_wait(f, 1);
	rocksdb_flush(s->db, f, &err);
	rocksdb_flushoptions_destroy(f);
	rocksdb_close(s->db);
	rocksdb_writeoptions_destroy(s->w);
	rocksdb_readoptions_destroy(s->r);
	rocksdb_options_destroy(s->opts);
	rocksdb_block_based_options_destroy(s->table);
	rocksdb_cache_destroy(s->cache);
	free(s);
	return err;
}
*/
import "C"

import (
	"errors"
	"unsafe"
)

// rocksdbVersion is the MSYS2 package the benchmark was built against (the
// C API has no version function).
const rocksdbVersion = "RocksDB 11.1.1 (C API, MSYS2 package)"

func init() {
	register("rocksdb", storeInfo{open: openRocksDB, version: func(config) string { return rocksdbVersion }})
}

type rocksStore struct{ s *C.rdb }

// rerr turns a RocksDB error string (malloc'ed) into a Go error.
func rerr(what string, e *C.char) error {
	if e == nil {
		return nil
	}
	defer C.rocksdb_free(unsafe.Pointer(e))
	return errors.New("rocksdb " + what + ": " + C.GoString(e))
}

func openRocksDB(dir string, c config) (Store, error) {
	cdir := C.CString(dir)
	defer C.free(unsafe.Pointer(cdir))
	sync := C.int(0)
	if c.sync {
		sync = 1
	}
	var s *C.rdb
	if err := rerr("open", C.rdb_open(cdir, sync, C.size_t(c.cacheMiB)<<20, &s)); err != nil {
		return nil, err
	}
	return &rocksStore{s}, nil
}

func (s *rocksStore) Put(k, v []byte) error {
	return rerr("put", C.rdb_put(s.s, (*C.char)(cptr(k)), C.size_t(len(k)), (*C.char)(cptr(v)), C.size_t(len(v))))
}

func (s *rocksStore) Batch(ops []op) error {
	buf := encodeOps(ops)
	return rerr("batch", C.rdb_batch(s.s, (*C.char)(cptr(buf)), C.size_t(len(buf))))
}

func (s *rocksStore) Get(k []byte) ([]byte, bool, error) {
	out := make([]byte, 256)
	for {
		var n C.size_t
		var e *C.char
		switch r := C.rdb_get(s.s, (*C.char)(cptr(k)), C.size_t(len(k)), (*C.char)(cptr(out)), C.size_t(len(out)), &n, &e); r {
		case 1:
			return out[:n], true, nil
		case 0:
			return nil, false, nil
		case -2:
			out = make([]byte, n)
		default:
			return nil, false, rerr("get", e)
		}
	}
}

func (s *rocksStore) Scan(start []byte, n int) (int, error) {
	var e *C.char
	count := C.rdb_scan(s.s, (*C.char)(cptr(start)), C.size_t(len(start)), C.long(n), &e)
	return int(count), rerr("scan", e)
}

func (s *rocksStore) Close() error { return rerr("close", C.rdb_close(s.s)) }
