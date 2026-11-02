// Package pager owns the data file: it reads and writes pages, verifies
// checksums, allocates page IDs and keeps the meta page (page 0).
//
// Every fsync in the storage engine happens in this package (Sync), so there
// is exactly one place that decides when data becomes durable.
package pager

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"baseDB/internal/page"
)

var (
	// ErrChecksum means the bytes read from disk do not match their checksum:
	// the page is corrupted or was torn by a crash during a write.
	ErrChecksum = errors.New("pager: page checksum mismatch")

	// ErrMisdirected means a page was found at (or sent to) the wrong offset.
	ErrMisdirected = errors.New("pager: page ID does not match its location")

	// ErrPageOutOfRange means the page ID was never allocated.
	ErrPageOutOfRange = errors.New("pager: page not allocated")

	// ErrMetaPage means a caller tried to read or write page 0 as a normal
	// page. The meta page is only accessed through Meta/UpdateMeta.
	ErrMetaPage = errors.New("pager: page 0 is the meta page")
)

// Pager manages one data file. It is safe for concurrent use: os.File.ReadAt
// and WriteAt do not share a file offset, and mu protects the meta page.
type Pager struct {
	file *os.File

	mu   sync.Mutex
	meta page.Meta
}

// Open opens the data file at path, creating and initializing it if it does
// not exist or is empty. An existing file must have a valid meta page.
func Open(path string) (*Pager, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("pager: open %s: %w", path, err)
	}
	pg := &Pager{file: f}
	if err := pg.init(); err != nil {
		f.Close()
		return nil, err
	}
	return pg, nil
}

// OpenIgnoringMeta opens a data file whose meta page is damaged (Open
// returned ErrChecksum for page 0, e.g. after a torn write of the meta page).
// The pager starts with an empty meta whose NextPageID covers every page in
// the file; recovery must then restore the real meta from the log with
// SetMeta before the pager is used.
func OpenIgnoringMeta(path string) (*Pager, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("pager: open %s: %w", path, err)
	}
	pg := &Pager{file: f, meta: page.NewMeta()}
	n, err := pg.NumPages()
	if err != nil {
		f.Close()
		return nil, err
	}
	pg.meta.NextPageID = max(pg.meta.NextPageID, page.PageID(n))
	return pg, nil
}

func (pg *Pager) init() error {
	st, err := pg.file.Stat()
	if err != nil {
		return fmt.Errorf("pager: stat: %w", err)
	}

	if st.Size() == 0 {
		pg.meta = page.NewMeta()
		return pg.Sync()
	}

	var p page.Page
	if err := pg.readAt(page.MetaPageID, &p); err != nil {
		return err
	}
	m, err := p.ReadMeta()
	if err != nil {
		return fmt.Errorf("pager: %w", err)
	}
	pg.meta = m
	return nil
}

func offset(id page.PageID) int64 { return int64(id) * page.PageSize }

// checkID rejects page 0 and IDs that were never allocated.
func (pg *Pager) checkID(id page.PageID) error {
	if id == page.MetaPageID {
		return ErrMetaPage
	}
	pg.mu.Lock()
	next := pg.meta.NextPageID
	pg.mu.Unlock()
	if id >= next {
		return fmt.Errorf("%w: page %d (next is %d)", ErrPageOutOfRange, id, next)
	}
	return nil
}

// ReadPage reads page id into dst and verifies its checksum. A page that was
// allocated but never written reads as all zeros.
func (pg *Pager) ReadPage(id page.PageID, dst *page.Page) error {
	if err := pg.checkID(id); err != nil {
		return err
	}
	return pg.readAt(id, dst)
}

func (pg *Pager) readAt(id page.PageID, dst *page.Page) error {
	buf := dst.Data()
	n, err := pg.file.ReadAt(buf, offset(id))
	if err == io.EOF {
		clear(buf[n:])
	} else if err != nil {
		return fmt.Errorf("pager: read page %d: %w", id, err)
	}

	if !dst.VerifyChecksum() {
		return fmt.Errorf("%w: page %d", ErrChecksum, id)
	}
	if !dst.IsZero() && dst.ID() != id {
		return fmt.Errorf("%w: read page %d, header says %d", ErrMisdirected, id, dst.ID())
	}
	return nil
}

// WritePage writes src to the location of page id. The checksum is computed
// on a private copy, so src is never modified. The write is not durable
// until Sync is called.
func (pg *Pager) WritePage(id page.PageID, src *page.Page) error {
	if err := pg.checkID(id); err != nil {
		return err
	}
	if src.ID() != id {
		return fmt.Errorf("%w: writing page %d, header says %d", ErrMisdirected, id, src.ID())
	}
	return pg.writeAt(id, src)
}

func (pg *Pager) writeAt(id page.PageID, src *page.Page) error {
	buf := *src
	buf.UpdateChecksum()
	if _, err := pg.file.WriteAt(buf.Data(), offset(id)); err != nil {
		return fmt.Errorf("pager: write page %d: %w", id, err)
	}
	return nil
}

// AllocatePage hands out a new page ID. The page is not written; until it
// is, it reads as all zeros. The new nextPageID becomes durable with the
// next Sync. (Reuse of pages from the free list comes later.)
func (pg *Pager) AllocatePage() (page.PageID, error) {
	pg.mu.Lock()
	defer pg.mu.Unlock()
	id := pg.meta.NextPageID
	if id == ^page.PageID(0) {
		return page.InvalidPageID, errors.New("pager: data file is full")
	}
	pg.meta.NextPageID++
	return id, nil
}

// UnallocatePage gives back the most recently allocated page ID. It is used
// when the operation that allocated the page is aborted, so its ID is not
// leaked. Only the newest ID can be given back (IDs are handed out in order
// and there is one writer, so an aborted operation's pages are always the
// newest ones).
func (pg *Pager) UnallocatePage(id page.PageID) error {
	pg.mu.Lock()
	defer pg.mu.Unlock()
	if id+1 != pg.meta.NextPageID {
		return fmt.Errorf("pager: cannot unallocate page %d, newest page is %d", id, pg.meta.NextPageID-1)
	}
	pg.meta.NextPageID--
	return nil
}

// Meta returns a copy of the current meta page fields.
func (pg *Pager) Meta() page.Meta {
	pg.mu.Lock()
	defer pg.mu.Unlock()
	return pg.meta
}

// UpdateMeta changes the meta page fields in memory. The change becomes
// durable with the next Sync. fn must not change NextPageID.
func (pg *Pager) UpdateMeta(fn func(m *page.Meta)) {
	pg.mu.Lock()
	defer pg.mu.Unlock()
	next := pg.meta.NextPageID
	fn(&pg.meta)
	pg.meta.NextPageID = next
}

// SetMeta replaces all meta fields, NextPageID included. Only crash recovery
// uses it, to restore the meta page from the log; normal code goes through
// AllocatePage and UpdateMeta.
func (pg *Pager) SetMeta(m page.Meta) {
	pg.mu.Lock()
	defer pg.mu.Unlock()
	pg.meta = m
}

// NumPages returns the number of pages the file currently spans, rounding a
// partially written last page up.
func (pg *Pager) NumPages() (int, error) {
	st, err := pg.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("pager: stat: %w", err)
	}
	return int((st.Size() + page.PageSize - 1) / page.PageSize), nil
}

// Sync writes the meta page and flushes everything written so far to stable
// storage (fsync). This is the only place in the engine that calls fsync.
func (pg *Pager) Sync() error {
	var p page.Page
	pg.mu.Lock()
	p.WriteMeta(pg.meta)
	pg.mu.Unlock()

	if err := pg.writeAt(page.MetaPageID, &p); err != nil {
		return err
	}
	if err := pg.file.Sync(); err != nil {
		return fmt.Errorf("pager: sync: %w", err)
	}
	return nil
}

// Close syncs and closes the data file.
func (pg *Pager) Close() error {
	err := pg.Sync()
	if cerr := pg.file.Close(); err == nil {
		err = cerr
	}
	return err
}

// Abandon closes the file without writing or syncing anything, as if the
// process had died at this moment. For crash tests only.
func (pg *Pager) Abandon() error {
	return pg.file.Close()
}
