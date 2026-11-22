package goncordia

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/kirimatt/goncordia/v2/driver"
)

// maxCompleteBatch caps how many requests share one driver call.
const maxCompleteBatch = 500

// groupCommit coalesces concurrent per-job writes into one batched driver
// call. It adds no waiting time: a request that arrives while no flush is
// running starts one at once; requests that arrive during a flush are written
// together by the next one. Every caller blocks until its own entry is
// written, so callers observe the same ordering as with a per-job call.
type groupCommit[P any] struct {
	write func(context.Context, []P) ([]bool, error)

	mu      sync.Mutex
	pending []*groupRequest[P]
	leading bool
}

type groupRequest[P any] struct {
	params P
	done   chan groupResult // buffered (1): a lead hand-off or the result
}

type groupResult struct {
	lead bool
	ok   bool
	err  error
}

// do writes one entry and reports the driver's per-entry result.
func (g *groupCommit[P]) do(params P) (bool, error) {
	req := &groupRequest[P]{params: params, done: make(chan groupResult, 1)}
	g.mu.Lock()
	g.pending = append(g.pending, req)
	if g.leading {
		g.mu.Unlock()
		res := <-req.done
		if !res.lead {
			return res.ok, res.err
		}
	} else {
		g.leading = true
		g.mu.Unlock()
	}
	return g.lead(req)
}

// lead flushes pending batches until self has been written, then hands the
// leader role to the oldest waiting request, if any.
func (g *groupCommit[P]) lead(self *groupRequest[P]) (bool, error) {
	for {
		g.mu.Lock()
		n := min(len(g.pending), maxCompleteBatch)
		batch := make([]*groupRequest[P], n)
		copy(batch, g.pending[:n])
		g.pending = append(g.pending[:0], g.pending[n:]...)
		g.mu.Unlock()

		g.flush(batch)

		select {
		case res := <-self.done:
			g.mu.Lock()
			if len(g.pending) > 0 {
				next := g.pending[0]
				g.mu.Unlock()
				next.done <- groupResult{lead: true}
			} else {
				g.leading = false
				g.mu.Unlock()
			}
			return res.ok, res.err
		default:
		}
	}
}

func (g *groupCommit[P]) flush(batch []*groupRequest[P]) {
	if len(batch) == 0 {
		return
	}
	params := make([]P, len(batch))
	for i, req := range batch {
		params[i] = req.params
	}
	// The callers' job contexts may already be done; the write must still
	// happen, bounded like setState's fallback.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	results, err := g.write(ctx, params)
	if err == nil && len(results) != len(batch) {
		err = fmt.Errorf("batch write: driver returned %d results for %d jobs", len(results), len(batch))
	}
	for i, req := range batch {
		if err != nil {
			req.done <- groupResult{err: err}
			continue
		}
		req.done <- groupResult{ok: results[i]}
	}
}

// batchCompleter coalesces concurrent successful completions into one
// JobCompleteMany call.
type batchCompleter struct {
	g groupCommit[driver.JobSetStateParams]
}

func newBatchCompleter(exec driver.JobBatchCompleter) *batchCompleter {
	return &batchCompleter{g: groupCommit[driver.JobSetStateParams]{write: exec.JobCompleteMany}}
}

// complete marks one running job completed. It returns driver.ErrStaleClaim
// when the claim no longer matches, like JobSetStateIfRunning.
func (c *batchCompleter) complete(params driver.JobSetStateParams) error {
	ok, err := c.g.do(params)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%w: job %q", driver.ErrStaleClaim, params.ID)
	}
	return nil
}

// batchStartMarker coalesces concurrent JobMarkStarted writes into one
// JobMarkStartedMany call.
type batchStartMarker struct {
	g groupCommit[driver.JobMarkStartedParams]
}

func newBatchStartMarker(exec driver.JobBatchStartMarker) *batchStartMarker {
	return &batchStartMarker{g: groupCommit[driver.JobMarkStartedParams]{write: exec.JobMarkStartedMany}}
}

// mark persists the start instant of one running job; false means the claim
// no longer matches, like JobMarkStarted.
func (m *batchStartMarker) mark(params driver.JobMarkStartedParams) (bool, error) {
	return m.g.do(params)
}
