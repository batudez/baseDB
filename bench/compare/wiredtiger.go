//go:build cgo

package main

// WiredTiger (MongoDB's storage engine) through its C API, built from
// source by deps/build-wiredtiger.ps1 (WiredTiger has no MinGW package).
// Settings: a cache of -cache MiB, the write-ahead log on, and every
// transaction committed with sync=on (log fsynced before commit returns).
// A WiredTiger session is single-threaded, so each goroutine takes one
// (with its cursor) from a pool. Every operation is one cgo call.

/*
#cgo CFLAGS: -I${SRCDIR}/deps/install/wiredtiger/include
#cgo LDFLAGS: ${SRCDIR}/deps/install/wiredtiger/lib/libwiredtiger.a -lsynchronization -lws2_32
#include <wiredtiger.h>
#include <stdlib.h>
#include <string.h>
#include "ops.h"

typedef struct {
	WT_SESSION *s;
	WT_CURSOR *c;
	const char *commit; // "sync=on" or "sync=off"
} wtx;

static int wt_open(const char *dir, const char *cfg, WT_CONNECTION **conn) {
	WT_SESSION *s;
	int r = wiredtiger_open(dir, NULL, cfg, conn);
	if (r != 0) return r;
	if ((r = (*conn)->open_session(*conn, NULL, NULL, &s)) != 0) return r;
	r = s->create(s, "table:kv", "key_format=u,value_format=u");
	s->close(s, NULL);
	return r;
}

static int wt_session(WT_CONNECTION *conn, int sync, wtx **out) {
	wtx *x = calloc(1, sizeof(wtx));
	int r = conn->open_session(conn, NULL, NULL, &x->s);
	if (r != 0) return r;
	if ((r = x->s->open_cursor(x->s, "table:kv", NULL, NULL, &x->c)) != 0) return r;
	x->commit = sync ? "sync=on" : "sync=off";
	*out = x;
	return 0;
}

static int wt_write(wtx *x, int del, const void *k, size_t kl, const void *v, size_t vl) {
	WT_ITEM ki = {0}, vi = {0};
	ki.data = k; ki.size = kl;
	x->c->set_key(x->c, &ki);
	if (del) {
		int r = x->c->remove(x->c);
		return r == WT_NOTFOUND ? 0 : r;
	}
	vi.data = v; vi.size = vl;
	x->c->set_value(x->c, &vi);
	return x->c->insert(x->c);
}

static int wt_tx(wtx *x, const char *buf, size_t len) {
	size_t pos = 0;
	bench_op o;
	int r = x->s->begin_transaction(x->s, NULL);
	if (r != 0) return r;
	while (bench_next_op(buf, len, &pos, &o)) {
		if ((r = wt_write(x, o.del, o.k, o.kl, o.v, o.vl)) != 0) {
			x->s->rollback_transaction(x->s, NULL);
			return r;
		}
	}
	return x->s->commit_transaction(x->s, x->commit);
}

static int wt_put(wtx *x, const void *k, size_t kl, const void *v, size_t vl) {
	int r = x->s->begin_transaction(x->s, NULL);
	if (r != 0) return r;
	if ((r = wt_write(x, 0, k, kl, v, vl)) != 0) {
		x->s->rollback_transaction(x->s, NULL);
		return r;
	}
	return x->s->commit_transaction(x->s, x->commit);
}

// wt_get: 1 found (copied into out), 0 not found, -2 out too small
// (*vl = size needed), -1 a WiredTiger error.
static int wt_get(wtx *x, const void *k, size_t kl, void *out, size_t cap, size_t *vl) {
	WT_ITEM ki = {0}, vi = {0};
	int r, ret;
	ki.data = k; ki.size = kl;
	x->c->set_key(x->c, &ki);
	r = x->c->search(x->c);
	if (r == WT_NOTFOUND) ret = 0;
	else if (r != 0) ret = -1;
	else if (x->c->get_value(x->c, &vi) != 0) ret = -1;
	else {
		*vl = vi.size;
		if (vi.size > cap) ret = -2;
		else { memcpy(out, vi.data, vi.size); ret = 1; }
	}
	x->c->reset(x->c);
	return ret;
}

static long wt_scan(wtx *x, const void *start, size_t sl, long n, int *err) {
	WT_ITEM ki = {0}, vi = {0};
	int exact, r;
	long count = 0;
	*err = 0;
	if (sl == 0) r = x->c->next(x->c);
	else {
		ki.data = start; ki.size = sl;
		x->c->set_key(x->c, &ki);
		r = x->c->search_near(x->c, &exact);
		if (r == 0 && exact < 0) r = x->c->next(x->c);
	}
	for (; r == 0 && count < n; r = x->c->next(x->c)) {
		x->c->get_key(x->c, &ki);
		x->c->get_value(x->c, &vi);
		count++;
	}
	if (r != 0 && r != WT_NOTFOUND) *err = r;
	x->c->reset(x->c);
	return count;
}

static void wt_session_close(wtx *x) {
	x->s->close(x->s, NULL);
	free(x);
}

static int wt_close(WT_CONNECTION *conn) { return conn->close(conn, NULL); }
*/
import "C"

import (
	"errors"
	"fmt"
	"sync"
	"unsafe"
)

func init() {
	register("wiredtiger", storeInfo{
		open: openWiredTiger,
		version: func(config) string {
			return C.GoString(C.wiredtiger_version(nil, nil, nil)) + " (C API, built from source with MinGW)"
		},
	})
}

type wtStore struct {
	conn *C.WT_CONNECTION
	sync C.int

	mu   sync.Mutex
	free []*C.wtx // idle sessions
	all  []*C.wtx
}

func wterr(what string, r C.int) error {
	if r == 0 {
		return nil
	}
	return fmt.Errorf("wiredtiger %s: %s", what, C.GoString(C.wiredtiger_strerror(r)))
}

func openWiredTiger(dir string, c config) (Store, error) {
	cfg := fmt.Sprintf("create,cache_size=%dMB,log=(enabled=true),session_max=1000,statistics=(none)", c.cacheMiB)
	cdir, ccfg := C.CString(dir), C.CString(cfg)
	defer C.free(unsafe.Pointer(cdir))
	defer C.free(unsafe.Pointer(ccfg))
	s := &wtStore{}
	if c.sync {
		s.sync = 1
	}
	if err := wterr("open", C.wt_open(cdir, ccfg, &s.conn)); err != nil {
		return nil, err
	}
	return s, nil
}

// session takes an idle session, or opens one.
func (s *wtStore) session() (*C.wtx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n := len(s.free); n > 0 {
		x := s.free[n-1]
		s.free = s.free[:n-1]
		return x, nil
	}
	var x *C.wtx
	if err := wterr("session", C.wt_session(s.conn, s.sync, &x)); err != nil {
		return nil, err
	}
	s.all = append(s.all, x)
	return x, nil
}

func (s *wtStore) release(x *C.wtx) {
	s.mu.Lock()
	s.free = append(s.free, x)
	s.mu.Unlock()
}

func (s *wtStore) Put(k, v []byte) error {
	x, err := s.session()
	if err != nil {
		return err
	}
	defer s.release(x)
	return wterr("put", C.wt_put(x, cptr(k), C.size_t(len(k)), cptr(v), C.size_t(len(v))))
}

func (s *wtStore) Batch(ops []op) error {
	x, err := s.session()
	if err != nil {
		return err
	}
	defer s.release(x)
	buf := encodeOps(ops)
	return wterr("batch", C.wt_tx(x, (*C.char)(cptr(buf)), C.size_t(len(buf))))
}

func (s *wtStore) Get(k []byte) ([]byte, bool, error) {
	x, err := s.session()
	if err != nil {
		return nil, false, err
	}
	defer s.release(x)
	out := make([]byte, 256)
	for {
		var n C.size_t
		switch r := C.wt_get(x, cptr(k), C.size_t(len(k)), cptr(out), C.size_t(len(out)), &n); r {
		case 1:
			return out[:n], true, nil
		case 0:
			return nil, false, nil
		case -2:
			out = make([]byte, n)
		default:
			return nil, false, errors.New("wiredtiger get failed")
		}
	}
}

func (s *wtStore) Scan(start []byte, n int) (int, error) {
	x, err := s.session()
	if err != nil {
		return 0, err
	}
	defer s.release(x)
	var e C.int
	count := C.wt_scan(x, cptr(start), C.size_t(len(start)), C.long(n), &e)
	return int(count), wterr("scan", e)
}

// Close closes the sessions, then the connection (which checkpoints).
func (s *wtStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, x := range s.all {
		C.wt_session_close(x)
	}
	s.all, s.free = nil, nil
	return wterr("close", C.wt_close(s.conn))
}
