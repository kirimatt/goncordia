package goncordia

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/v2/driver"
)

type fakeBatchExec struct {
	mu      sync.Mutex
	batches [][]string
	gate    chan struct{} // when set, the first call waits on it
	stale   map[string]bool
	err     error
}

func (f *fakeBatchExec) JobCompleteMany(_ context.Context, params []driver.JobSetStateParams) ([]bool, error) {
	f.mu.Lock()
	first := len(f.batches) == 0
	ids := make([]string, len(params))
	for i, p := range params {
		ids[i] = p.ID
	}
	f.batches = append(f.batches, ids)
	gate := f.gate
	f.mu.Unlock()
	if first && gate != nil {
		<-gate
	}
	if f.err != nil {
		return nil, f.err
	}
	out := make([]bool, len(params))
	for i, p := range params {
		out[i] = !f.stale[p.ID]
	}
	return out, nil
}

func (f *fakeBatchExec) snapshot() [][]string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]string(nil), f.batches...)
}

func waitUntil(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestBatchCompleterSingleCallFlushesAtOnce(t *testing.T) {
	f := &fakeBatchExec{}
	c := newBatchCompleter(f)
	if err := c.complete(driver.JobSetStateParams{ID: "1"}); err != nil {
		t.Fatal(err)
	}
	if got := f.snapshot(); len(got) != 1 || len(got[0]) != 1 {
		t.Fatalf("batches=%v", got)
	}
}

func TestBatchCompleterCoalescesDuringFlush(t *testing.T) {
	f := &fakeBatchExec{gate: make(chan struct{})}
	c := newBatchCompleter(f)
	errs := make(chan error, 11)
	go func() { errs <- c.complete(driver.JobSetStateParams{ID: "first"}) }()
	waitUntil(t, func() bool { return len(f.snapshot()) == 1 })
	for i := range 10 {
		go func() { errs <- c.complete(driver.JobSetStateParams{ID: strconv.Itoa(i)}) }()
	}
	waitUntil(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.pending) == 10
	})
	close(f.gate)
	for range 11 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	got := f.snapshot()
	if len(got) != 2 || len(got[1]) != 10 {
		t.Fatalf("want 2 batches (1 then 10), got %v", got)
	}
}

func TestBatchCompleterReportsStaleAndErrors(t *testing.T) {
	f := &fakeBatchExec{stale: map[string]bool{"old": true}}
	c := newBatchCompleter(f)
	if err := c.complete(driver.JobSetStateParams{ID: "old"}); !errors.Is(err, driver.ErrStaleClaim) {
		t.Fatalf("stale: err=%v", err)
	}
	if err := c.complete(driver.JobSetStateParams{ID: "ok"}); err != nil {
		t.Fatalf("ok: err=%v", err)
	}
	f.err = errors.New("db down")
	if err := c.complete(driver.JobSetStateParams{ID: "x"}); err == nil || errors.Is(err, driver.ErrStaleClaim) {
		t.Fatalf("db error: err=%v", err)
	}
}

func TestBatchCompleterConcurrentCallersAllServed(t *testing.T) {
	f := &fakeBatchExec{stale: map[string]bool{}}
	for i := 0; i < 2000; i += 7 {
		f.stale[strconv.Itoa(i)] = true
	}
	c := newBatchCompleter(f)
	var wg sync.WaitGroup
	for i := range 2000 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := c.complete(driver.JobSetStateParams{ID: strconv.Itoa(i)})
			if f.stale[strconv.Itoa(i)] != errors.Is(err, driver.ErrStaleClaim) {
				t.Errorf("job %d: err=%v", i, err)
			}
		}()
	}
	wg.Wait()
	seen := map[string]int{}
	for _, b := range f.snapshot() {
		if len(b) > maxCompleteBatch {
			t.Fatalf("batch of %d exceeds cap", len(b))
		}
		for _, id := range b {
			seen[id]++
		}
	}
	if len(seen) != 2000 {
		t.Fatalf("written %d distinct jobs, want 2000", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %s written %d times", id, n)
		}
	}
	if c.leading || len(c.pending) != 0 {
		t.Fatalf("completer not idle: leading=%v pending=%d", c.leading, len(c.pending))
	}
}

type shortResultExec struct{}

func (shortResultExec) JobCompleteMany(context.Context, []driver.JobSetStateParams) ([]bool, error) {
	return nil, nil
}

func TestBatchCompleterRejectsShortDriverResult(t *testing.T) {
	c := newBatchCompleter(shortResultExec{})
	err := c.complete(driver.JobSetStateParams{ID: "1"})
	if err == nil || errors.Is(err, driver.ErrStaleClaim) {
		t.Fatalf("err=%v, want a driver contract error", err)
	}
	c.flush(nil) // an empty batch is a no-op
}
