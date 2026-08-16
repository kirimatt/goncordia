package goncordia_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver"
	"github.com/kirimatt/goncordia/driver/memory"
)

type featureArgs struct {
	N int `json:"n"`
}

func TestNoRetryDiscardsAfterFirstFailure(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	registry := core.NewRegistry()
	var attempts atomic.Int64
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(context.Context, *core.Job[featureArgs]) error {
		attempts.Add(1)
		return errors.New("permanent")
	}), core.WorkerOpts{})
	if _, err := client.Enqueue(context.Background(), featureArgs{}, nil); err != nil {
		t.Fatal(err)
	}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		PollInterval: 5 * time.Millisecond, RetryPolicy: core.NoRetry{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.Start(ctx) //nolint:errcheck
	waitForCondition(t, time.Second, func() bool {
		jobs := d.AllJobs()
		return len(jobs) == 1 && jobs[0].State == driver.JobStateDiscarded
	}, "NoRetry discard")
	if got := attempts.Load(); got != 1 {
		t.Fatalf("attempts=%d, want 1", got)
	}
	pool.Stop()
}

func TestWorkerPersistsPanicStackTrace(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(context.Context, *core.Job[featureArgs]) error {
		panic("boom")
	}), core.WorkerOpts{})
	if _, err := client.Enqueue(context.Background(), featureArgs{}, nil); err != nil {
		t.Fatal(err)
	}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		PollInterval: 5 * time.Millisecond, RetryPolicy: core.NoRetry{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.Start(ctx) //nolint:errcheck
	waitForCondition(t, time.Second, func() bool {
		jobs := d.AllJobs()
		return len(jobs) == 1 && jobs[0].State == driver.JobStateDiscarded
	}, "panic discard")
	pool.Stop()
	job := d.AllJobs()[0]
	if len(job.Errors) != 1 || job.Errors[0].Error != "panic: boom" {
		t.Fatalf("panic errors=%+v", job.Errors)
	}
	if !strings.Contains(job.Errors[0].Trace, "TestWorkerPersistsPanicStackTrace") ||
		!strings.Contains(job.Errors[0].Trace, "runtime/debug.Stack") {
		t.Fatalf("panic trace missing call stack: %q", job.Errors[0].Trace)
	}
}

func TestWorkerHonorsRetryDirectives(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(_ context.Context, job *core.Job[featureArgs]) error {
		if job.Args.N == 1 {
			return core.Discard(errors.New("permanent"))
		}
		return core.RetryAfter(time.Hour, errors.New("rate limited"))
	}), core.WorkerOpts{MaxRetry: 10})
	for _, n := range []int{1, 2} {
		if _, err := client.Enqueue(context.Background(), featureArgs{N: n}, nil); err != nil {
			t.Fatal(err)
		}
	}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Concurrency: 2, PollInterval: time.Hour, Clock: clk,
	})
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.Start(runCtx) //nolint:errcheck
	waitForCondition(t, time.Second, func() bool {
		jobs := d.AllJobs()
		if len(jobs) != 2 {
			return false
		}
		states := map[int]driver.JobRow{}
		for _, job := range jobs {
			var args featureArgs
			if err := json.Unmarshal(job.Args, &args); err == nil {
				states[args.N] = job
			}
		}
		return states[1].State == driver.JobStateDiscarded &&
			states[2].State == driver.JobStateAvailable &&
			states[2].RunAt.Equal(clk.Now().Add(time.Hour))
	}, "retry directives")
	pool.Stop()
}

func (featureArgs) Kind() string { return "worker_feature" }

func TestWorkerPassesMetadataAndAppliesTimeout(t *testing.T) {
	clk := clock.NewMock(time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	registry := core.NewRegistry()
	seen := make(chan *core.Job[featureArgs], 1)
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(ctx context.Context, job *core.Job[featureArgs]) error {
		seen <- job
		<-ctx.Done()
		return ctx.Err()
	}), core.WorkerOpts{MaxRetry: 1, Timeout: 25 * time.Millisecond})

	if _, err := client.Enqueue(context.Background(), featureArgs{N: 1}, nil); err != nil {
		t.Fatal(err)
	}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{PollInterval: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.Start(ctx) //nolint:errcheck

	select {
	case job := <-seen:
		if !job.CreatedAt.Equal(clk.Now()) {
			t.Fatalf("CreatedAt=%v, want %v", job.CreatedAt, clk.Now())
		}
		if job.WorkerID == "" {
			t.Fatal("WorkerID was not propagated")
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	clk.Advance(25 * time.Millisecond)

	waitForCondition(t, time.Second, func() bool {
		jobs := d.AllJobs()
		return len(jobs) == 1 && jobs[0].State == driver.JobStateDiscarded
	}, "timed-out job to be discarded")
	jobs := d.AllJobs()
	if len(jobs[0].Errors) != 1 || jobs[0].Errors[0].Error != context.DeadlineExceeded.Error() {
		t.Fatalf("expected deadline error, got %+v", jobs[0].Errors)
	}
	pool.Stop()
}

func TestWorkerHeartbeatsLongRunningJob(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	registry := core.NewRegistry()
	started := make(chan struct{})
	release := make(chan struct{})
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(context.Context, *core.Job[featureArgs]) error {
		close(started)
		<-release
		return nil
	}), core.WorkerOpts{})
	if _, err := client.Enqueue(context.Background(), featureArgs{}, nil); err != nil {
		t.Fatal(err)
	}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Concurrency: 1, PollInterval: time.Hour,
		StuckJobTimeout: 90 * time.Second, HeartbeatInterval: 30 * time.Second,
		Clock: clk,
	})
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.Start(runCtx) //nolint:errcheck
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("job did not start")
	}

	claimedAt := d.AllJobs()[0].AttemptedAt
	clk.Advance(30 * time.Second)
	waitForCondition(t, time.Second, func() bool {
		jobs := d.AllJobs()
		return len(jobs) == 1 && jobs[0].AttemptedAt != nil && jobs[0].AttemptedAt.After(*claimedAt)
	}, "worker heartbeat")
	for range 4 {
		clk.Advance(30 * time.Second)
		waitForCondition(t, time.Second, func() bool {
			jobs := d.AllJobs()
			return len(jobs) == 1 && jobs[0].AttemptedAt != nil && jobs[0].AttemptedAt.Equal(clk.Now())
		}, "renewed heartbeat after long execution")
	}
	rescued, err := d.Executor().(driver.StuckJobRescuer).JobRescueStuck(context.Background(), driver.JobRescueParams{
		Queue: "default", Before: clk.Now().Add(-90 * time.Second),
	})
	if err != nil || rescued != 0 {
		t.Fatalf("healthy job was rescued: rescued=%d err=%v", rescued, err)
	}
	close(release)
	waitForCondition(t, time.Second, func() bool {
		jobs := d.AllJobs()
		return len(jobs) == 1 && jobs[0].State == driver.JobStateCompleted
	}, "heartbeating job completion")
	pool.Stop()
}

func TestWorkerCancellationYieldsWithoutConsumingAttempt(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	registry := core.NewRegistry()
	started := make(chan struct{})
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(ctx context.Context, _ *core.Job[featureArgs]) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}), core.WorkerOpts{})
	if _, err := client.Enqueue(context.Background(), featureArgs{}, nil); err != nil {
		t.Fatal(err)
	}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Concurrency: 1, PollInterval: time.Hour,
	})
	runCtx, cancel := context.WithCancel(context.Background())
	go pool.Start(runCtx) //nolint:errcheck
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("job did not start")
	}
	cancel()
	waitForCondition(t, time.Second, func() bool {
		jobs := d.AllJobs()
		return len(jobs) == 1 && jobs[0].State == driver.JobStateAvailable && jobs[0].AttemptNum == 0 && len(jobs[0].Errors) == 0
	}, "cancelled worker to yield claim")
	pool.Stop()
}

func TestWorkerOptsConcurrencyIsEnforced(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	registry := core.NewRegistry()
	var active atomic.Int64
	var maximum atomic.Int64
	var completed atomic.Int64
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(context.Context, *core.Job[featureArgs]) error {
		current := active.Add(1)
		for {
			old := maximum.Load()
			if current <= old || maximum.CompareAndSwap(old, current) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		active.Add(-1)
		completed.Add(1)
		return nil
	}), core.WorkerOpts{Concurrency: 1})
	for i := range 6 {
		if _, err := client.Enqueue(context.Background(), featureArgs{N: i}, nil); err != nil {
			t.Fatal(err)
		}
	}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{Concurrency: 6, PollInterval: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.Start(ctx) //nolint:errcheck
	waitForCondition(t, 2*time.Second, func() bool { return completed.Load() == 6 }, "all jobs to complete")
	if got := maximum.Load(); got != 1 {
		t.Fatalf("max kind concurrency=%d, want 1", got)
	}
	pool.Stop()
}

func TestPipelineWaitDoesNotConsumeGlobalConcurrency(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	registry := core.NewRegistry()
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	unrelatedRan := make(chan struct{}, 1)
	var firstOnce sync.Once
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(_ context.Context, job *core.Job[featureArgs]) error {
		switch job.Args.N {
		case 1:
			firstOnce.Do(func() { close(firstStarted) })
			<-releaseFirst
		case 99:
			unrelatedRan <- struct{}{}
		}
		return nil
	}), core.WorkerOpts{})

	ctx := context.Background()
	for _, request := range []struct {
		n        int
		pipeline string
	}{{1, "account-1"}, {2, "account-1"}, {99, ""}} {
		if _, err := client.Enqueue(ctx, featureArgs{N: request.n}, &core.InsertOpts{PipelineID: request.pipeline}); err != nil {
			t.Fatal(err)
		}
	}

	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Concurrency: 2, MaxPending: 3, PollInterval: 5 * time.Millisecond,
	})
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go pool.Start(runCtx) //nolint:errcheck
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("first pipeline job did not start")
	}
	select {
	case <-unrelatedRan:
		// The second pipeline job is waiting without occupying the other slot.
	case <-time.After(250 * time.Millisecond):
		close(releaseFirst)
		pool.Stop()
		t.Fatal("pipeline waiter consumed global concurrency and starved unrelated work")
	}
	close(releaseFirst)
	pool.Stop()
}

func TestPipelineSerializesInQueueOrder(t *testing.T) {
	clk := clock.NewMock(time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	registry := core.NewRegistry()
	var mu sync.Mutex
	var order []int
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(_ context.Context, job *core.Job[featureArgs]) error {
		mu.Lock()
		order = append(order, job.Args.N)
		mu.Unlock()
		time.Sleep(5 * time.Millisecond)
		return nil
	}), core.WorkerOpts{})
	for i := range 5 {
		if _, err := client.Enqueue(context.Background(), featureArgs{N: i}, &core.InsertOpts{PipelineID: "account-1"}); err != nil {
			t.Fatal(err)
		}
	}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{Concurrency: 5, PollInterval: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.Start(ctx) //nolint:errcheck
	waitForCondition(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 5
	}, "pipeline jobs")
	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(order, []int{0, 1, 2, 3, 4}) {
		t.Fatalf("pipeline order=%v", order)
	}
	pool.Stop()
}
