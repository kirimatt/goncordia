// Package gontest provides test helpers for goncordia.
//
// It covers three scenarios:
//
//   - Asserting that business logic enqueued the right jobs (via Tracker).
//   - Unit-testing a worker function in isolation (via WorkerHelper).
//   - Controlling time in tests that involve scheduled or delayed jobs (via MockClock).
//
// # Quick start
//
//	func TestOrderConfirmation(t *testing.T) {
//	    client, tracker := gontest.NewClient(t)
//	    _ = PlaceOrder(ctx, client, orderID)   // calls client.Enqueue internally
//
//	    jobs := gontest.RequireEnqueued[SendEmailArgs](t, tracker, 1)
//	    if jobs[0].Args.OrderID != orderID {
//	        t.Errorf("wrong order ID: %s", jobs[0].Args.OrderID)
//	    }
//	}
//
//	func TestEmailWorker(t *testing.T) {
//	    h := gontest.NewWorkerHelper[SendEmailArgs](myEmailWorker)
//	    if err := h.Work(ctx, SendEmailArgs{To: "user@example.com"}); err != nil {
//	        t.Fatal(err)
//	    }
//	}
package gontest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	goncordia "github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver"
	"github.com/kirimatt/goncordia/driver/memory"
)

// NoTx is the transaction type for the test client.
// In-memory operations require no real transactions.
type NoTx = memory.NoTx

// Client is a goncordia Client backed by the in-memory driver.
type Client = goncordia.Client[memory.NoTx]

// WorkerPool is a WorkerPool backed by the in-memory driver.
type WorkerPool = goncordia.WorkerPool[memory.NoTx]

// MockClock is a controllable fake clock for deterministic time in tests.
// Use Advance to move time forward; inject into workers via WorkerConfig.Clock.
type MockClock = clock.Mock
type ManualClock = clock.Manual

// NewMockClock returns a MockClock set to 2024-01-01 00:00:00 UTC.
func NewMockClock() *MockClock {
	return clock.NewMock(time.Time{})
}

// Tracker wraps the in-memory driver and provides typed inspection and assertion
// helpers over the job store. It is intentionally tied to the memory driver —
// for integration tests against a real backend use that driver's test helpers.
type Tracker struct {
	d   *memory.Driver
	clk clock.Clock
}

// NewClient creates an in-memory test Client and Tracker that share the same job
// store. Call Enqueue on the client, then assert via the tracker.
// Cleanup (driver.Close) is registered via t.Cleanup.
func NewClient(t testing.TB) (*Client, *Tracker) {
	t.Helper()
	d := memory.New()
	t.Cleanup(func() { d.Close() })
	return goncordia.NewClient[memory.NoTx](d, goncordia.ClientConfig{}), &Tracker{d: d}
}

// NewClientWithClock is like NewClient but uses the given clock, allowing you to
// control time for scheduled-job tests.
//
//	clk := gontest.NewMockClock()
//	client, tracker := gontest.NewClientWithClock(t, clk)
//	client.Enqueue(ctx, job, &core.InsertOpts{RunAt: clk.Now().Add(time.Hour)})
//	clk.Advance(time.Hour)
//	// job is now available
func NewClientWithClock(t testing.TB, clk *MockClock) (*Client, *Tracker) {
	t.Helper()
	d := memory.New(memory.WithClock(clk))
	t.Cleanup(func() { d.Close() })
	return goncordia.NewClient[memory.NoTx](d, goncordia.ClientConfig{Clock: clk}), &Tracker{d: d, clk: clk}
}

// NewWorkerPool creates a WorkerPool backed by the same store as this Tracker.
// The pool and tracker share state, so jobs enqueued via the paired Client are
// visible to workers started via this pool.
func (tr *Tracker) NewWorkerPool(registry *core.Registry, cfg goncordia.WorkerConfig) *WorkerPool {
	if cfg.Clock == nil && tr.clk != nil {
		cfg.Clock = tr.clk
	}
	return goncordia.NewWorkerPool[memory.NoTx](tr.d, registry, cfg)
}

// Driver returns the underlying memory driver for advanced inspection
// (e.g. checking job state, errors, or worker ID).
func (tr *Tracker) Driver() *memory.Driver { return tr.d }

// Jobs returns all jobs of kind T currently in the store, regardless of state.
// Args are deserialized into T; rows that fail to deserialize are silently skipped.
func Jobs[T core.JobArgs](tr *Tracker) []*core.Job[T] {
	var zero T
	kind := zero.Kind()
	var result []*core.Job[T]
	for _, row := range tr.d.AllJobs() {
		if row.Kind != kind {
			continue
		}
		var args T
		if err := json.Unmarshal(row.Args, &args); err != nil {
			continue
		}
		result = append(result, &core.Job[T]{
			ID:         row.ID,
			Queue:      row.Queue,
			Args:       args,
			AttemptNum: row.AttemptNum,
			MaxRetry:   row.MaxRetry,
			CreatedAt:  row.CreatedAt,
			WorkerID:   row.WorkerID,
			Tags:       row.Tags,
		})
	}
	return result
}

// RequireEnqueued asserts that exactly n jobs of kind T are in the store.
// It returns the matching jobs so callers can inspect their args.
// Calls t.Fatal if the count does not match.
func RequireEnqueued[T core.JobArgs](t testing.TB, tr *Tracker, n int) []*core.Job[T] {
	t.Helper()
	jobs := Jobs[T](tr)
	if len(jobs) != n {
		var zero T
		t.Fatalf("gontest: expected %d enqueued %q job(s), got %d", n, zero.Kind(), len(jobs))
	}
	return jobs
}

// RequireNoEnqueued asserts that no jobs of kind T are in the store.
// Calls t.Fatal if any are found.
func RequireNoEnqueued[T core.JobArgs](t testing.TB, tr *Tracker) {
	t.Helper()
	RequireEnqueued[T](t, tr, 0)
}

// WorkerHelper lets you invoke a worker function directly without a WorkerPool
// or database. Use it to unit-test the logic of a single worker in isolation.
//
//	h := gontest.NewWorkerHelper[SendEmailArgs](emailWorker)
//	if err := h.Work(ctx, SendEmailArgs{To: "user@example.com"}); err != nil {
//	    t.Fatal(err)
//	}
type WorkerHelper[T core.JobArgs] struct {
	worker core.Worker[T]
}

// NewWorkerHelper creates a WorkerHelper for the given worker.
func NewWorkerHelper[T core.JobArgs](w core.Worker[T]) *WorkerHelper[T] {
	return &WorkerHelper[T]{worker: w}
}

// Work calls the worker with args wrapped in a minimal Job (ID="test", AttemptNum=1).
func (h *WorkerHelper[T]) Work(ctx context.Context, args T) error {
	return h.worker.Process(ctx, &core.Job[T]{
		ID:         "test",
		Queue:      "default",
		Args:       args,
		AttemptNum: 1,
		MaxRetry:   1,
	})
}

// WorkJob calls the worker with the given fully-specified Job.
// Use this when AttemptNum, MaxRetry, Tags, or other fields matter to your worker.
func (h *WorkerHelper[T]) WorkJob(ctx context.Context, job *core.Job[T]) error {
	return h.worker.Process(ctx, job)
}

// WorkerFuncHelper is a convenience constructor for workers defined as plain functions.
//
//	h := gontest.WorkerFuncHelper[SendEmailArgs](func(ctx context.Context, job *core.Job[SendEmailArgs]) error {
//	    return sendEmail(job.Args.To, job.Args.Subject)
//	})
func WorkerFuncHelper[T core.JobArgs](fn func(context.Context, *core.Job[T]) error) *WorkerHelper[T] {
	return NewWorkerHelper[T](core.WorkerFunc[T](fn))
}

// RequireWork runs the worker directly on args and calls t.Fatal if it returns an error.
// It is a one-liner alternative to creating a WorkerHelper when you only need to
// assert the happy path.
func RequireWork[T core.JobArgs](t testing.TB, ctx context.Context, w core.Worker[T], args T) {
	t.Helper()
	h := NewWorkerHelper[T](w)
	if err := h.Work(ctx, args); err != nil {
		var zero T
		t.Fatalf("gontest: worker %q returned error: %v", zero.Kind(), err)
	}
}

// MustEnqueue enqueues a single job and calls t.Fatal if it fails.
func MustEnqueue[T core.JobArgs](t testing.TB, ctx context.Context, c *Client, args T, opts *core.InsertOpts) *driver.JobInsertResult {
	t.Helper()
	result, err := c.Enqueue(ctx, args, opts)
	if err != nil {
		var zero T
		t.Fatalf("gontest: enqueue %q: %v", zero.Kind(), err)
	}
	return result
}

// FormatJobList formats a slice of jobs for readable test failure messages.
func FormatJobList[T core.JobArgs](jobs []*core.Job[T]) string {
	if len(jobs) == 0 {
		return "(none)"
	}
	var zero T
	out := fmt.Sprintf("%d %q job(s):\n", len(jobs), zero.Kind())
	for i, j := range jobs {
		b, _ := json.Marshal(j.Args)
		out += fmt.Sprintf("  [%d] id=%s queue=%s attempt=%d args=%s\n",
			i, j.ID, j.Queue, j.AttemptNum, b)
	}
	return out
}
