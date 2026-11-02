package page

// Free list structure (only the format is defined for now; allocation does
// not reuse free pages yet).
//
// Freed pages form a singly linked list threaded through the pages
// themselves, so it costs no extra space:
//
//	meta.FreeListHead ──► [TypeFree page] ──rightSibling──► [TypeFree page] ──► InvalidPageID
//
// Freeing page X:    X.rightSibling = meta.FreeListHead; meta.FreeListHead = X
// Allocating:        X = meta.FreeListHead; meta.FreeListHead = X.rightSibling

// InitFree formats p as a free-list page pointing at next.
func (p *Page) InitFree(id, next PageID) {
	p.Init(id, TypeFree)
	p.SetRightSibling(next)
}

// NextFree returns the next page of the free list.
func (p *Page) NextFree() PageID { return p.RightSibling() }
