package wal

import (
	"bytes"

	"baseDB/internal/page"
)

// mergeGap: two changed ranges closer than this are logged as one run. Each
// run costs 4 header bytes, so logging a few unchanged bytes is cheaper than
// starting a new run.
const mergeGap = 8

// Diff returns the byte ranges in which after differs from before. Applying
// the runs to a copy of before (see Apply) reproduces after exactly.
//
// Diff works on raw bytes, so it covers every part of a slotted page at once:
// header fields (numSlots, freeSpacePtr, rightSibling), the slot directory and
// the cell data. Callers never need to describe what they changed.
func Diff(before, after *page.Page) []Run {
	a, b := before.Data(), after.Data()
	var runs []Run
	for i := 0; i < page.PageSize; {
		if a[i] == b[i] {
			i++
			continue
		}
		start, end := i, i+1 // end is exclusive
		for j := end; j < page.PageSize && j-end < mergeGap; j++ {
			if a[j] != b[j] {
				end = j + 1
			}
		}
		runs = append(runs, Run{Off: start, Data: bytes.Clone(b[start:end])})
		i = end
	}
	return runs
}

// Apply redoes a PAGE record on p: it zeroes the page if the record is an
// init record, copies the runs in, and sets the page LSN to the record's
// LSN. Applying the same record twice gives the same result (idempotent).
func Apply(p *page.Page, r *Record) {
	if r.Type != TypePage {
		panic("wal: Apply on a " + r.Type.String() + " record")
	}
	if r.Init {
		*p = page.Page{}
	}
	for _, run := range r.Runs {
		copy(p.Data()[run.Off:], run.Data)
	}
	p.SetLSN(r.LSN)
}
