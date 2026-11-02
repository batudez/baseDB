package btree

import (
	"baseDB/internal/mtr"
	"baseDB/internal/page"
)

// mutation is the mini-transaction of one Put, Delete or Apply, plus the
// tree's root as seen inside it. Every page access of a write goes through
// it; the mtr package collects the changed pages, logs them and makes them
// durable (docs/ARCHITECTURE.md, week 3).
//
// root starts as the committed root and follows every root change made in
// the mutation, so that several operations in one mutation (Apply) each see
// the tree as the previous ones left it. The tree adopts it after commit.
type mutation struct {
	*mtr.Mtr
	root        page.PageID
	rootChanged bool
}

func (m *mutation) fetch(id page.PageID) (*page.Page, error) { return m.Fetch(id) }
func (m *mutation) create(t page.Type) (*page.Page, error)   { return m.Create(t) }
func (m *mutation) markDirty(p *page.Page)                   { m.MarkDirty(p) }
func (m *mutation) free(p *page.Page)                        { m.Free(p) }

// setRoot records a new root. It is logged in the META record of the
// transaction and reaches the meta page only after the commit is durable.
func (m *mutation) setRoot(id page.PageID) {
	m.rootChanged = true
	m.root = id
	m.UpdateMeta(func(meta *page.Meta) { meta.RootPageID = id })
}
