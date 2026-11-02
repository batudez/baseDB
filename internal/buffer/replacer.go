package buffer

import "baseDB/internal/page"

// Replacer chooses which unpinned frame to evict when the pool is full.
//
// Implementations are not safe for concurrent use; the Pool calls them only
// while holding its mutex.
type Replacer interface {
	// RecordAccess notes that page id, held in frame, was just used. It is
	// called once when a page is loaded into a frame and again on every hit.
	RecordAccess(frame int, id page.PageID)
	// SetEvictable marks whether the frame may be chosen as a victim
	// (true exactly when its pin count is zero).
	SetEvictable(frame int, evictable bool)
	// Evict picks a victim among evictable frames and stops tracking it.
	// It returns false if no frame is evictable.
	Evict() (frame int, ok bool)
	// Remove stops tracking an evictable frame without making it a victim
	// (e.g. when its page is dropped).
	Remove(frame int)
	// Size returns the number of evictable frames.
	Size() int
}
