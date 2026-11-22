package goncordia

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kirimatt/goncordia/v2/driver"
)

// maxCompleteBatch caps how many completions share one query.
const maxCompleteBatch = 500

// batchCompleter coalesces concurrent successful completions into one
// JobCompleteMany call (group commit). It adds no waiting time: a job that
// completes while no flush is running starts one at once; jobs that complete
// during a flush are written together by the next one. Every caller blocks
// until its own job is written, so callers observe the same ordering as with
// JobSetStateIfRunning: when complete returns, the row is completed.
type batchCompleter struct {
	exec driver.JobBatchCompleter

	mu      sync.Mutex
	pending []*completeRequest
	leading bool
}

type completeRequest struct {
	params driver.JobSetStateParams
	done   chan completeResult // buffered (1): a lead hand-off or the result
}

type completeResult struct {
	lead bool
	err  error
}

func newBatchCompleter(exec driver.JobBatchCompleter) *batchCompleter {
	return &batchCompleter{exec: exec}
}

// complete marks one running job completed. It returns driver.ErrStaleClaim
// when the claim no longer matches, like JobSetStateIfRunning.
func (c *batchCompleter) complete(params driver.JobSetStateParams) error {
	req := &completeRequest{params: params, done: make(chan completeResult, 1)}
	c.mu.Lock()
	c.pending = append(c.pending, req)
	if c.leading {
		c.mu.Unlock()
		res := <-req.done
		if !res.lead {
			return res.err
		}
	} else {
		c.leading = true
		c.mu.Unlock()
	}
	return c.lead(req)
}

// lead flushes pending batches until self has been written, then hands the
// leader role to the oldest waiting request, if any.
func (c *batchCompleter) lead(self *completeRequest) error {
	for {
		c.mu.Lock()
		n := min(len(c.pending), maxCompleteBatch)
		batch := make([]*completeRequest, n)
		copy(batch, c.pending[:n])
		c.pending = append(c.pending[:0], c.pending[n:]...)
		c.mu.Unlock()

		c.flush(batch)

		select {
		case res := <-self.done:
			c.mu.Lock()
			if len(c.pending) > 0 {
				next := c.pending[0]
				c.mu.Unlock()
				next.done <- completeResult{lead: true}
			} else {
				c.leading = false
				c.mu.Unlock()
			}
			return res.err
		default:
		}
	}
}

func (c *batchCompleter) flush(batch []*completeRequest) {
	if len(batch) == 0 {
		return
	}
	params := make([]driver.JobSetStateParams, len(batch))
	for i, req := range batch {
		params[i] = req.params
	}
	// The callers' job contexts may already be done; the write must still
	// happen, bounded like setState's fallback.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	completed, err := c.exec.JobCompleteMany(ctx, params)
	if err == nil && len(completed) != len(batch) {
		err = fmt.Errorf("complete batch: driver returned %d results for %d jobs", len(completed), len(batch))
	}
	for i, req := range batch {
		switch {
		case err != nil:
			req.done <- completeResult{err: err}
		case !completed[i]:
			req.done <- completeResult{err: fmt.Errorf("%w: job %q", driver.ErrStaleClaim, req.params.ID)}
		default:
			req.done <- completeResult{}
		}
	}
}
