// Package recovery brings the data file up to date with the write-ahead log
// at startup (redo-only crash recovery).
//
// Why redo is enough: a mini-transaction keeps its pages pinned until its
// COMMIT record is durable (no-steal), so the data file never contains a
// change of an uncommitted transaction. After a crash the data file can only
// be *behind* the log, never ahead of it. Recovery therefore replays the
// committed transactions and ignores everything else.
//
// See docs/ARCHITECTURE.md (week 4).
package recovery

import (
	"errors"
	"fmt"
	"slices"

	"baseDB/internal/page"
	"baseDB/internal/pager"
	"baseDB/internal/wal"
)

// ErrUnrecoverable means the data file is damaged in a way the log cannot
// repair (for example a torn page whose history is not fully in the log).
var ErrUnrecoverable = errors.New("recovery: data cannot be recovered")

// Stats describes what a recovery run did.
type Stats struct {
	Transactions   int   // committed transactions in the log
	Uncommitted    int   // transactions in the log without COMMIT (ignored)
	RecordsApplied int   // PAGE records redone
	RecordsSkipped int   // PAGE records already reflected on disk
	PagesWritten   int   // pages rewritten in the data file
	TornPages      int   // pages that failed their checksum and were rebuilt
	MetaRestored   bool  // the meta page was replaced by the log's last META
	LogTail        error // what the log open cut off (torn or corrupt tail)
}

// Run replays the committed transactions of log onto the data file of pg:
//
//  1. Meta: the last META record in the log is the newest committed meta
//     (root, free list, ...). It replaces the meta page; NextPageID is the
//     larger of the logged and the on-disk value. If the meta page itself was
//     damaged (metaDamaged), the log must contain a META record.
//  2. Pages: for every page that appears in a committed PAGE record, read it
//     and apply, in LSN order, each record whose LSN is greater than the
//     page's pageLSN. Records at or below pageLSN are already on disk.
//     A page that fails its checksum (torn write) is rebuilt from zero; this
//     is possible because its first record in the log is an init record.
//  3. Sync: write the changed pages and the meta page, then fsync, so the
//     next start finds them in place.
//
// Run is idempotent: if the process crashes during recovery, running it
// again gives the same result.
func Run(pg *pager.Pager, log *wal.Log, metaDamaged bool) (Stats, error) {
	st := Stats{LogTail: log.TailError()}
	txns, uncommitted, err := log.TransactionsWithOpen()
	if err != nil {
		return st, fmt.Errorf("recovery: read log: %w", err)
	}
	st.Transactions, st.Uncommitted = len(txns), uncommitted

	var lastMeta *page.Meta
	byPage := map[page.PageID][]*wal.Record{}
	for i := range txns {
		for j := range txns[i].Records {
			r := &txns[i].Records[j]
			switch r.Type {
			case wal.TypePage:
				byPage[r.PageID] = append(byPage[r.PageID], r)
			case wal.TypeMeta:
				lastMeta = &r.Meta
			}
		}
	}

	// 1. Meta first: page IDs above the on-disk NextPageID must be readable.
	switch {
	case lastMeta != nil:
		m := *lastMeta
		m.NextPageID = max(m.NextPageID, pg.Meta().NextPageID)
		if m != pg.Meta() {
			pg.SetMeta(m)
			st.MetaRestored = true
		}
	case metaDamaged:
		return st, fmt.Errorf("%w: meta page damaged and the log has no META record", ErrUnrecoverable)
	}
	next := pg.Meta().NextPageID

	// 2. Pages, in page order (the order does not matter for correctness:
	// each page is replayed independently).
	ids := make([]page.PageID, 0, len(byPage))
	for id := range byPage {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		if id >= next {
			return st, fmt.Errorf("%w: log changes page %d but NextPageID is %d", ErrUnrecoverable, id, next)
		}
		if err := redoPage(pg, id, byPage[id], &st); err != nil {
			return st, err
		}
	}

	// 3. Make the result durable before new transactions build on it.
	if err := pg.Sync(); err != nil {
		return st, fmt.Errorf("recovery: %w", err)
	}
	return st, nil
}

func redoPage(pg *pager.Pager, id page.PageID, recs []*wal.Record, st *Stats) error {
	var p page.Page
	err := pg.ReadPage(id, &p)
	switch {
	case errors.Is(err, pager.ErrChecksum) || errors.Is(err, pager.ErrMisdirected):
		// Torn or misplaced write. The log holds the page's whole history
		// (it is never truncated), starting with the init record that
		// created it, so the page can be rebuilt from nothing.
		if !recs[0].Init {
			return fmt.Errorf("%w: page %d is damaged (%v) and its history does not start in the log",
				ErrUnrecoverable, id, err)
		}
		p = page.Page{}
		st.TornPages++
	case err != nil:
		return fmt.Errorf("recovery: read page %d: %w", id, err)
	}

	changed := false
	for _, r := range recs {
		if r.LSN <= p.LSN() {
			st.RecordsSkipped++ // already on disk
			continue
		}
		wal.Apply(&p, r)
		st.RecordsApplied++
		changed = true
	}
	if !changed {
		return nil
	}
	if err := pg.WritePage(id, &p); err != nil {
		return fmt.Errorf("recovery: write page %d: %w", id, err)
	}
	st.PagesWritten++
	return nil
}
