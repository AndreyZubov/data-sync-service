package syncer

import (
	"sync"
	"time"

	"github.com/AndreyZubov/data-sync-service/internal/store"
)

// checkpointer tracks an export's progress through its result pages and
// saves a checkpoint whenever more pages are fully committed. Pages are
// parsed in order, but their batches are written by concurrent workers in
// any order, so a page only counts once all of its batches and every page
// before it are written. It is safe for concurrent use.
type checkpointer struct {
	save func(store.ExportCheckpoint) error

	mu sync.Mutex
	cp store.ExportCheckpoint
	// pages are the pages opened but not yet committed, oldest first.
	pages []*pageProgress
}

// pageProgress is the state of one result page.
type pageProgress struct {
	unwritten int // batches sent to workers but not yet written
	closed    bool
	next      string
	records   int64
	watermark *time.Time
}

// newCheckpointer starts tracking from cp and saves progress with save.
func newCheckpointer(cp store.ExportCheckpoint, save func(store.ExportCheckpoint) error) *checkpointer {
	return &checkpointer{cp: cp, save: save}
}

// open starts tracking the next page.
func (c *checkpointer) open() *pageProgress {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := &pageProgress{}
	c.pages = append(c.pages, p)
	return p
}

// sent counts a batch of p handed to the workers. It must be called before
// the batch is sent, so the page cannot look written in the meantime.
func (c *checkpointer) sent(p *pageProgress) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p.unwritten++
}

// close records that every batch of p has been sent, along with the locator
// of the page after it ("" after the last page) and the page's totals.
func (c *checkpointer) close(p *pageProgress, next string, records int64, watermark *time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p.closed, p.next, p.records, p.watermark = true, next, records, watermark
	return c.advance()
}

// written records that a batch of p is committed.
func (c *checkpointer) written(p *pageProgress) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	p.unwritten--
	return c.advance()
}

// watermark returns the highest SystemModstamp of the committed pages.
func (c *checkpointer) watermark() *time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cp.Watermark
}

// advance commits the leading pages that are complete and saves the new
// checkpoint. Saving under the lock keeps checkpoints in order; it happens
// once per page, so the workers it holds up wait rarely.
func (c *checkpointer) advance() error {
	advanced := false
	for len(c.pages) > 0 && c.pages[0].closed && c.pages[0].unwritten == 0 {
		p := c.pages[0]
		c.pages = c.pages[1:]
		c.cp.Locator = p.next
		c.cp.Records += p.records
		c.cp.Watermark = later(c.cp.Watermark, p.watermark)
		advanced = true
	}
	// Once the last page is in, the run is about to complete, and a
	// checkpoint without a locator would mean starting from the first page.
	if !advanced || c.cp.Locator == "" {
		return nil
	}
	return c.save(c.cp)
}
