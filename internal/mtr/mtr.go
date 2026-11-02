// Package mtr implements mini-transactions: a group of page changes that is
// logged and becomes durable as one unit.
//
// A data structure (the B+ tree) fetches pages through an Mtr, changes them
// freely in the buffer pool, and calls Commit. Commit compares every changed
// page with its image from before the transaction, logs the differences as
// one WAL transaction (BEGIN, PAGE..., META, COMMIT) and makes it durable.
// The data structure itself contains no logging code.
//
// The Mtr also owns page allocation: Create takes a page from the free list
// (or grows the file), Free puts a page on the free list. Both are ordinary
// logged page and meta changes.
//
// See docs/ARCHITECTURE.md (weeks 3 and 5).
package mtr

import (
	"fmt"

	"baseDB/internal/buffer"
	"baseDB/internal/page"
	"baseDB/internal/wal"
)

// MetaStore holds the meta page fields (*pager.Pager).
type MetaStore interface {
	Meta() page.Meta
	UpdateMeta(fn func(m *page.Meta))
}

// Mtr is one mini-transaction. Every page it touches stays pinned and
// write-latched until Commit or Abort, so no other code can see or flush a
// half-done change, and no changed page can reach disk before its log
// record is durable (the pages cannot be evicted while pinned).
//
// An Mtr is used by one goroutine, must end with exactly one Commit or
// Abort, and only one Mtr may run at a time (the caller serializes writers).
type Mtr struct {
	pool *buffer.Pool
	log  *wal.Log
	meta MetaStore

	pages []*entry // in order of first access
	index map[page.PageID]*entry

	// working is the meta as this Mtr sees it: allocations and frees change
	// FreeListHead here first, so a page freed earlier in the same Mtr can be
	// reused by a later Create. It reaches the MetaStore after the commit.
	working     page.Meta
	metaChanged bool
	done        bool
}

type entry struct {
	frame    *buffer.Frame
	before   page.Page // image at first access (all zeros for extended pages)
	extended bool      // allocated by growing the file; given back on Abort
	init     bool      // (re)formatted by this Mtr: logged as a full image
	dirty    bool
}

// Begin starts a mini-transaction.
func Begin(pool *buffer.Pool, log *wal.Log, meta MetaStore) *Mtr {
	return &Mtr{
		pool:    pool,
		log:     log,
		meta:    meta,
		index:   make(map[page.PageID]*entry),
		working: meta.Meta(),
	}
}

func (m *Mtr) track(f *buffer.Frame, extended bool) *entry {
	f.WLatch()
	e := &entry{frame: f, extended: extended}
	if !extended {
		e.before = *f.Page()
	}
	m.index[f.PageID()] = e
	m.pages = append(m.pages, e)
	return e
}

// Fetch returns page id, pinned and write-latched until the end of the Mtr.
func (m *Mtr) Fetch(id page.PageID) (*page.Page, error) {
	if e, ok := m.index[id]; ok {
		return e.frame.Page(), nil
	}
	f, err := m.pool.Fetch(id)
	if err != nil {
		return nil, err
	}
	return m.track(f, false).frame.Page(), nil
}

// Meta returns the meta fields as this Mtr sees them (including its own
// allocations, frees and UpdateMeta changes).
func (m *Mtr) Meta() page.Meta { return m.working }

// Create returns a new, empty page of type t. It reuses the first page of
// the free list if there is one, and grows the file otherwise. Either way the
// page is logged as a full image (an "init" record), since its previous
// contents are meaningless.
func (m *Mtr) Create(t page.Type) (*page.Page, error) {
	if head := m.working.FreeListHead; head != page.InvalidPageID {
		p, err := m.Fetch(head)
		if err != nil {
			return nil, err
		}
		if p.Type() != page.TypeFree {
			return nil, fmt.Errorf("mtr: free list head %d has type %d, not free", head, p.Type())
		}
		m.working.FreeListHead = p.NextFree()
		m.metaChanged = true
		p.Init(head, t)
		e := m.index[head]
		e.init, e.dirty = true, true
		return p, nil
	}

	f, err := m.pool.NewPage()
	if err != nil {
		return nil, err
	}
	e := m.track(f, true)
	e.init, e.dirty = true, true
	f.Page().SetType(t)
	return f.Page(), nil
}

// Free puts p, obtained from this Mtr, on the free list. The page must no
// longer be referenced by the data structure.
func (m *Mtr) Free(p *page.Page) {
	e := m.entryOf(p)
	p.InitFree(p.ID(), m.working.FreeListHead)
	m.working.FreeListHead = p.ID()
	m.metaChanged = true
	e.init, e.dirty = true, true
}

func (m *Mtr) entryOf(p *page.Page) *entry {
	e, ok := m.index[p.ID()]
	if !ok {
		panic(fmt.Sprintf("mtr: page %d was not obtained from this mini-transaction", p.ID()))
	}
	return e
}

// MarkDirty records that p, obtained from this Mtr, was changed.
func (m *Mtr) MarkDirty(p *page.Page) { m.entryOf(p).dirty = true }

// UpdateMeta changes the meta fields. The change is visible through Meta at
// once, logged with the transaction, and applied to the MetaStore only once
// the commit is durable.
func (m *Mtr) UpdateMeta(fn func(meta *page.Meta)) {
	fn(&m.working)
	m.metaChanged = true
}

// DirtyPages lists the pages marked dirty, in order of first access.
func (m *Mtr) DirtyPages() []page.PageID {
	var ids []page.PageID
	for _, e := range m.pages {
		if e.dirty {
			ids = append(ids, e.frame.PageID())
		}
	}
	return ids
}

// Commit logs the changes and makes them durable, in this order:
//
//  1. build the records: BEGIN, one PAGE record per changed page, META if the
//     meta fields or the page count changed, COMMIT.
//     A PAGE record is a full image ("init") if the page was created or freed
//     here, or if this is its first change since the last checkpoint
//     (pageLSN < log start); otherwise it is the byte diff against the
//     before-image. The full image on the first change after a checkpoint is
//     what lets recovery rebuild a torn page once the older log is gone.
//  2. append them to the log in one piece (they get consecutive LSNs)
//  3. stamp every changed page with the LSN of its PAGE record (pageLSN)
//  4. flush the log up to the COMMIT record (write + fsync): from here on
//     the transaction survives a crash
//  5. apply the meta change in memory
//  6. release latches and pins; changed pages may now be written to the
//     data file at any time, the buffer pool's WAL check always passes
//     because their records are already durable
//
// It returns the COMMIT record's LSN (0 if nothing changed). If the flush
// fails the changes are in memory but not durable; the caller must stop
// using the structure.
func (m *Mtr) Commit() (page.LSN, error) {
	m.finishOnce()

	type stamp struct {
		p   *page.Page
		rec *wal.Record
	}
	var stamps []stamp
	var recs []*wal.Record
	extended := false
	checkpoint := m.log.StartLSN()
	for _, e := range m.pages {
		if !e.dirty {
			continue
		}
		after := e.frame.Page()
		init := e.init
		var runs []wal.Run
		if !init {
			if runs = wal.Diff(&e.before, after); len(runs) == 0 {
				e.dirty = false // marked dirty, but nothing changed
				continue
			}
			init = e.before.LSN() < checkpoint // first change since the checkpoint
		}
		if init {
			runs = wal.Diff(&page.Page{}, after)
		}
		extended = extended || e.extended
		rec := &wal.Record{Type: wal.TypePage, PageID: after.ID(), Init: init, Runs: runs}
		recs = append(recs, rec)
		stamps = append(stamps, stamp{after, rec})
	}

	// Growing the file changed nextPageID in the pager, so it is logged too.
	if m.metaChanged || extended {
		snapshot := m.working
		snapshot.NextPageID = m.meta.Meta().NextPageID
		recs = append(recs, &wal.Record{Type: wal.TypeMeta, Meta: snapshot})
	}
	if len(recs) == 0 {
		return 0, m.release(false)
	}

	txn := m.log.BeginTxn()
	commit := &wal.Record{Type: wal.TypeCommit}
	recs = append([]*wal.Record{{Type: wal.TypeBegin}}, recs...)
	recs = append(recs, commit)
	for _, r := range recs {
		r.TxnID = txn
	}
	if err := m.log.Append(recs...); err != nil {
		m.restore()
		m.release(true)
		return 0, err
	}

	for _, s := range stamps {
		s.p.SetLSN(s.rec.LSN)
	}
	err := m.log.FlushUntil(commit.LSN)
	if err == nil && m.metaChanged {
		working := m.working
		m.meta.UpdateMeta(func(meta *page.Meta) { *meta = working }) // keeps NextPageID
	}
	if rerr := m.release(false); err == nil {
		err = rerr
	}
	return commit.LSN, err
}

// Abort undoes everything the Mtr did: changed pages get their
// before-images back, pages allocated by growing the file are dropped and
// their IDs given back, and the meta changes are forgotten. Nothing is
// logged.
func (m *Mtr) Abort() {
	m.finishOnce()
	m.restore()
	m.release(true)
}

func (m *Mtr) finishOnce() {
	if m.done {
		panic("mtr: Commit or Abort called twice")
	}
	m.done = true
}

func (m *Mtr) restore() {
	for _, e := range m.pages {
		if !e.extended {
			*e.frame.Page() = e.before
			e.dirty = false
		}
	}
}

// release unlatches and unpins every page, newest first. On abort, pages
// that grew the file are discarded instead (newest first, as the pager can
// only give back its newest page ID).
func (m *Mtr) release(abort bool) error {
	var firstErr error
	for i := len(m.pages) - 1; i >= 0; i-- {
		e := m.pages[i]
		id := e.frame.PageID()
		e.frame.WUnlatch()
		var err error
		if abort && e.extended {
			err = m.pool.DiscardNewPage(id)
		} else {
			err = m.pool.Unpin(id, e.dirty)
		}
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	m.pages, m.index = nil, nil
	return firstErr
}
