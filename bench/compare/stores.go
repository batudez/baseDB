package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	"github.com/cockroachdb/pebble"

	"baseDB/internal/engine"
)

// Store is what every database under test is reduced to. Every write
// method is one transaction: durable when it returns (with -sync on).
type Store interface {
	Put(key, value []byte) error           // one key, one transaction
	Batch(ops []op) error                  // many keys, one transaction
	Get(key []byte) ([]byte, bool, error)  // the value is a copy
	Scan(start []byte, n int) (int, error) // read up to n keys from start; returns how many
	Close() error
}

type op struct {
	key, value []byte
	del        bool
}

// config is the same for every store, so that they get equal resources.
type config struct {
	sync     bool   // fsync every commit
	cacheMiB int    // page / block cache size
	redbDLL  string // path of baseredb.dll (redb_windows.go)
}

type storeInfo struct {
	open    func(dir string, c config) (Store, error)
	version func(c config) string // for the results header
}

// stores lists the databases this program can open, by -dbs name. The
// stores written in C, C++ or Rust add themselves in their own files
// (sqlite.go, rocksdb.go, wiredtiger.go: need cgo; redb_windows.go).
var stores = map[string]storeInfo{
	"basedb": {
		open:    func(d string, c config) (Store, error) { return openBaseDB(d, c, true) },
		version: func(config) string { return "this repository (working tree)" },
	},
	"basedb-nogc": {
		open:    func(d string, c config) (Store, error) { return openBaseDB(d, c, false) },
		version: func(config) string { return "this repository, group commit off" },
	},
	"pebble": {open: openPebble, version: func(config) string { return "github.com/cockroachdb/pebble v1.1.2" }},
}

func register(name string, info storeInfo) { stores[name] = info }

func storeNames() []string {
	var names []string
	for n := range stores {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// encodeOps packs a batch for the C and Rust stores, so that one batch is
// one foreign call: [kind u8 (0 put, 1 delete)][key len u32][value len u32][key][value]...
func encodeOps(ops []op) []byte {
	n := 0
	for _, o := range ops {
		n += 9 + len(o.key) + len(o.value)
	}
	buf := make([]byte, 0, n)
	for _, o := range ops {
		kind := byte(0)
		if o.del {
			kind = 1
		}
		buf = append(buf, kind)
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(o.key)))
		buf = binary.LittleEndian.AppendUint32(buf, uint32(len(o.value)))
		buf = append(buf, o.key...)
		buf = append(buf, o.value...)
	}
	return buf
}

// ---- baseDB ----

type baseStore struct{ db *engine.DB }

func openBaseDB(dir string, c config, groupCommit bool) (Store, error) {
	db, err := engine.Open(dir, engine.Options{
		PoolFrames:    c.cacheMiB << 20 / 4096,
		NoSync:        !c.sync,
		NoGroupCommit: !groupCommit,
	})
	if err != nil {
		return nil, err
	}
	return &baseStore{db}, nil
}

func (s *baseStore) Put(k, v []byte) error { return s.db.Put(k, v) }

func (s *baseStore) Batch(ops []op) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	for _, o := range ops {
		if o.del {
			_, err = tx.Delete(o.key)
		} else {
			err = tx.Put(o.key, o.value)
		}
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (s *baseStore) Get(k []byte) ([]byte, bool, error) { return s.db.Get(k) }

func (s *baseStore) Scan(start []byte, n int) (int, error) {
	it := s.db.Scan(start, nil)
	i := 0
	for i < n && it.Next() {
		_, _ = it.Key(), it.Value()
		i++
	}
	return i, it.Err()
}

func (s *baseStore) Close() error { return s.db.Close() }

// ---- Pebble ----

type pebbleStore struct {
	db    *pebble.DB
	write *pebble.WriteOptions
}

type quietLogger struct{}

func (quietLogger) Infof(string, ...interface{})  {}
func (quietLogger) Errorf(string, ...interface{}) {}
func (quietLogger) Fatalf(format string, args ...interface{}) {
	panic(fmt.Sprintf(format, args...))
}

func openPebble(dir string, c config) (Store, error) {
	cache := pebble.NewCache(int64(c.cacheMiB) << 20)
	defer cache.Unref()
	db, err := pebble.Open(dir, &pebble.Options{Cache: cache, Logger: quietLogger{}})
	if err != nil {
		return nil, err
	}
	w := pebble.Sync
	if !c.sync {
		w = pebble.NoSync
	}
	return &pebbleStore{db, w}, nil
}

func (s *pebbleStore) Put(k, v []byte) error { return s.db.Set(k, v, s.write) }

func (s *pebbleStore) Batch(ops []op) error {
	b := s.db.NewBatch()
	defer b.Close()
	for _, o := range ops {
		var err error
		if o.del {
			err = b.Delete(o.key, nil)
		} else {
			err = b.Set(o.key, o.value, nil)
		}
		if err != nil {
			return err
		}
	}
	return b.Commit(s.write)
}

func (s *pebbleStore) Get(k []byte) ([]byte, bool, error) {
	v, closer, err := s.db.Get(k)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	defer closer.Close()
	return append([]byte(nil), v...), true, nil
}

func (s *pebbleStore) Scan(start []byte, n int) (int, error) {
	it, err := s.db.NewIter(&pebble.IterOptions{LowerBound: start})
	if err != nil {
		return 0, err
	}
	i := 0
	for ok := it.First(); ok && i < n; ok = it.Next() {
		_, _ = it.Key(), it.Value()
		i++
	}
	if err := it.Error(); err != nil {
		it.Close()
		return i, err
	}
	return i, it.Close()
}

// Close flushes the memtable first, so the directory size measured after
// Close is the data in sorted files, like the other stores after theirs.
func (s *pebbleStore) Close() error {
	if err := s.db.Flush(); err != nil {
		s.db.Close()
		return err
	}
	return s.db.Close()
}
