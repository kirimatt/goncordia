package goncordia_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

// batchMemoryDriver adds driver.JobBatchCompleter to the memory driver.
type batchMemoryDriver struct {
	*memory.Driver
	calls atomic.Int64
	jobs  atomic.Int64
	fail  atomic.Bool

	startJobs atomic.Int64
	failStart atomic.Bool
}

type batchMemoryExecutor struct {
	driver.Executor
	d *batchMemoryDriver
}

func (d *batchMemoryDriver) Executor() driver.Executor {
	return batchMemoryExecutor{Executor: d.Driver.Executor(), d: d}
}

func (e batchMemoryExecutor) JobCompleteMany(ctx context.Context, params []driver.JobSetStateParams) ([]bool, error) {
	e.d.calls.Add(1)
	e.d.jobs.Add(int64(len(params)))
	if e.d.fail.Load() {
		return nil, errors.New("batch write failed")
	}
	out := make([]bool, len(params))
	for i, p := range params {
		p.State = driver.JobStateCompleted
		err := e.Executor.JobSetStateIfRunning(ctx, p)
		if err != nil && !errors.Is(err, driver.ErrStaleClaim) {
			return nil, err
		}
		out[i] = err == nil
	}
	return out, nil
}

func (e batchMemoryExecutor) JobMarkStartedMany(ctx context.Context, params []driver.JobMarkStartedParams) ([]bool, error) {
	e.d.startJobs.Add(int64(len(params)))
	if e.d.failStart.Load() {
		return nil, errors.New("start batch failed")
	}
	marker := e.Executor.(driver.JobStartMarker)
	out := make([]bool, len(params))
	for i, p := range params {
		ok, err := marker.JobMarkStarted(ctx, p)
		if err != nil {
			return nil, err
		}
		out[i] = ok
	}
	return out, nil
}

var (
	_ driver.JobBatchCompleter   = batchMemoryExecutor{}
	_ driver.JobBatchStartMarker = batchMemoryExecutor{}
)

func runBatchPool(t *testing.T, d *batchMemoryDriver, jobs int, onErr func(error)) {
	t.Helper()
	client := goncordia.NewClient[memory.NoTx](d, goncordia.ClientConfig{})
	registry := core.NewRegistry()
	var done atomic.Int64
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(context.Context, *core.Job[featureArgs]) error {
		done.Add(1)
		return nil
	}), core.WorkerOpts{})
	for i := range jobs {
		if _, err := client.Enqueue(context.Background(), featureArgs{N: i}, nil); err != nil {
			t.Fatal(err)
		}
	}
	pool := goncordia.NewWorkerPool[memory.NoTx](d, registry, goncordia.WorkerConfig{
		Concurrency: 8, PollInterval: 5 * time.Millisecond, ErrorHandler: onErr,
	})
	go pool.Start(context.Background()) //nolint:errcheck
	waitForCondition(t, 2*time.Second, func() bool { return done.Load() == int64(jobs) }, "jobs processed")
	_ = pool.Shutdown(context.Background())
}

func TestWorkerPoolUsesBatchCompleter(t *testing.T) {
	d := &batchMemoryDriver{Driver: memory.New()}
	runBatchPool(t, d, 40, nil)
	if d.jobs.Load() != 40 {
		t.Fatalf("batch completer wrote %d jobs, want 40", d.jobs.Load())
	}
	if d.startJobs.Load() != 40 {
		t.Fatalf("batch start marker wrote %d jobs, want 40", d.startJobs.Load())
	}
	stats, err := d.Driver.Executor().(driver.AdminExecutor).QueueStats(context.Background(), "default")
	if err != nil {
		t.Fatal(err)
	}
	if got := stats.States[driver.JobStateCompleted]; got != 40 {
		t.Fatalf("completed jobs=%d, want 40 (stats %v)", got, stats.States)
	}
}

func TestWorkerPoolReportsBatchCompleteError(t *testing.T) {
	d := &batchMemoryDriver{Driver: memory.New()}
	d.fail.Store(true)
	d.failStart.Store(true)
	var mu sync.Mutex
	var errs []error
	runBatchPool(t, d, 3, func(err error) {
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	})
	mu.Lock()
	defer mu.Unlock()
	found, foundStart := false, false
	for _, err := range errs {
		if strings.Contains(err.Error(), "batch write failed") {
			found = true
		}
		if strings.Contains(err.Error(), "start batch failed") {
			foundStart = true
		}
	}
	if !found || !foundStart {
		t.Fatalf("errors=%v, want the batch failure reported", errs)
	}
}

// A pool must keep fetching while the queue has a backlog; PollInterval only
// paces polls of an empty queue.
func TestWorkerPoolDrainsBacklogWithoutWaitingForPoll(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient[memory.NoTx](d, goncordia.ClientConfig{})
	registry := core.NewRegistry()
	var done atomic.Int64
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(context.Context, *core.Job[featureArgs]) error {
		done.Add(1)
		return nil
	}), core.WorkerOpts{})
	const jobs = 400
	args := make([]core.JobArgs, jobs)
	for i := range args {
		args[i] = featureArgs{N: i}
	}
	if _, err := client.EnqueueMany(context.Background(), args, nil); err != nil {
		t.Fatal(err)
	}
	pool := goncordia.NewWorkerPool[memory.NoTx](d, registry, goncordia.WorkerConfig{
		Concurrency: 4, PollInterval: time.Hour,
	})
	go pool.Start(context.Background()) //nolint:errcheck
	defer func() { _ = pool.Shutdown(context.Background()) }()
	waitForCondition(t, 5*time.Second, func() bool { return done.Load() == jobs }, "backlog drained without poll ticks")
}
