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

type limitedCapabilityDriver struct {
	*memory.Driver
	capabilities driver.Capabilities
}

func (d *limitedCapabilityDriver) Name() string { return "limited" }
func (d *limitedCapabilityDriver) Capabilities() driver.Capabilities {
	return d.capabilities
}

type claimRecorder struct {
	mu     sync.Mutex
	queues []string
}

func (r *claimRecorder) JobClaimed(_ context.Context, row driver.JobRow) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.queues = append(r.queues, row.Queue)
}
func (*claimRecorder) JobHeartbeat(context.Context, goncordia.HeartbeatEvent) {}
func (*claimRecorder) JobsRescued(context.Context, goncordia.RescueEvent)     {}
func (r *claimRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.queues...)
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
	initialLease := d.AllJobs()[0].LeaseExpiresAt
	if claimedAt == nil || initialLease == nil {
		t.Fatalf("claim timestamps missing: %+v", d.AllJobs()[0])
	}
	clk.Advance(30 * time.Second)
	waitForCondition(t, time.Second, func() bool {
		jobs := d.AllJobs()
		return len(jobs) == 1 && jobs[0].LeaseExpiresAt != nil && jobs[0].LeaseExpiresAt.After(*initialLease)
	}, "worker heartbeat")
	for range 4 {
		clk.Advance(30 * time.Second)
		waitForCondition(t, time.Second, func() bool {
			jobs := d.AllJobs()
			return len(jobs) == 1 && jobs[0].LeaseExpiresAt != nil &&
				jobs[0].LeaseExpiresAt.Equal(clk.Now().Add(90*time.Second)) &&
				jobs[0].AttemptedAt != nil && jobs[0].AttemptedAt.Equal(*claimedAt)
		}, "renewed heartbeat after long execution")
	}
	rescued, err := d.Executor().(driver.StuckJobRescuer).JobRescueStuck(context.Background(), driver.JobRescueParams{
		Queue: "default", At: clk.Now(), Before: clk.Now().Add(-90 * time.Second),
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

func TestWorkerShutdownReportsTimeoutAndKeepsLeaseAlive(t *testing.T) {
	start := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	clk := clock.NewManual(start)
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
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
		Concurrency: 1, PollInterval: time.Hour, Clock: clk,
		StuckJobTimeout: 90 * time.Second, HeartbeatInterval: 30 * time.Second,
	})
	go pool.Start(context.Background()) //nolint:errcheck
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("job did not start")
	}

	initialLease := d.AllJobs()[0].LeaseExpiresAt
	if initialLease == nil {
		t.Fatal("initial lease missing")
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	shutdownResult := make(chan error, 1)
	go func() { shutdownResult <- pool.Shutdown(shutdownCtx) }()
	clk.Advance(30 * time.Second)
	waitForCondition(t, time.Second, func() bool {
		lease := d.AllJobs()[0].LeaseExpiresAt
		return lease != nil && lease.After(*initialLease)
	}, "heartbeat while shutdown drains")
	if err := <-shutdownResult; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown error=%v, want context deadline exceeded", err)
	}

	close(release)
	if err := pool.Shutdown(context.Background()); err != nil {
		t.Fatalf("finish shutdown: %v", err)
	}
	if state := d.AllJobs()[0].State; state != driver.JobStateCompleted {
		t.Fatalf("job state=%s, want completed", state)
	}
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

func TestWeightedQueueFairnessAndConcurrencyLimit(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	registry := core.NewRegistry()
	release := make(chan struct{})
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(context.Context, *core.Job[featureArgs]) error {
		<-release
		return nil
	}), core.WorkerOpts{})
	for i := range 8 {
		for _, queue := range []string{"critical", "bulk"} {
			if _, err := client.Enqueue(context.Background(), featureArgs{N: i}, &core.InsertOpts{Queue: queue}); err != nil {
				t.Fatal(err)
			}
		}
	}
	recorder := &claimRecorder{}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues: []string{"critical", "bulk"}, Concurrency: 6, MaxPending: 6,
		PollInterval: time.Hour, Observer: recorder,
		QueuePolicies: map[string]goncordia.QueuePolicy{
			"critical": {Weight: 2},
			"bulk":     {Weight: 1, Concurrency: 1},
		},
	})
	go pool.Start(context.Background()) //nolint:errcheck
	waitForCondition(t, time.Second, func() bool { return len(recorder.snapshot()) == 6 }, "weighted claims")
	got := recorder.snapshot()
	if !slices.Equal(got, []string{"critical", "critical", "bulk", "critical", "critical", "critical"}) {
		t.Fatalf("claim order=%v", got)
	}
	close(release)
	pool.Stop()
}

func TestQueueRateLimitUsesInjectedClock(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
	registry := core.NewRegistry()
	release := make(chan struct{})
	var started atomic.Int64
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(context.Context, *core.Job[featureArgs]) error {
		started.Add(1)
		<-release
		return nil
	}), core.WorkerOpts{})
	for i := range 4 {
		if _, err := client.Enqueue(context.Background(), featureArgs{N: i}, nil); err != nil {
			t.Fatal(err)
		}
	}
	recorder := &claimRecorder{}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Concurrency: 4, MaxPending: 4, PollInterval: time.Second, Clock: clk, Observer: recorder,
		QueuePolicies: map[string]goncordia.QueuePolicy{
			"default": {RateLimit: 2, RatePeriod: time.Minute},
		},
	})
	go pool.Start(context.Background()) //nolint:errcheck
	waitForCondition(t, time.Second, func() bool { return len(recorder.snapshot()) == 4 }, "prefetched claims")
	waitForCondition(t, time.Second, func() bool { return started.Load() == 2 }, "initial rate-limited starts")
	clk.Advance(29 * time.Second)
	time.Sleep(10 * time.Millisecond)
	if got := started.Load(); got != 2 {
		t.Fatalf("starts before token refill=%d, want 2", got)
	}
	clk.Advance(time.Second)
	waitForCondition(t, time.Second, func() bool { return started.Load() == 3 }, "start after first token refill")
	clk.Advance(30 * time.Second)
	waitForCondition(t, time.Second, func() bool { return started.Load() == 4 }, "start after second token refill")
	close(release)
	pool.Stop()
}

func TestQueueRateLimitsCombineSecondAndMinuteWindows(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
	registry := core.NewRegistry()
	release := make(chan struct{})
	var started atomic.Int64
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(context.Context, *core.Job[featureArgs]) error {
		started.Add(1)
		<-release
		return nil
	}), core.WorkerOpts{})
	for i := range 4 {
		if _, err := client.Enqueue(context.Background(), featureArgs{N: i}, nil); err != nil {
			t.Fatal(err)
		}
	}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Concurrency: 4, MaxPending: 4, PollInterval: time.Hour, Clock: clk,
		QueuePolicies: map[string]goncordia.QueuePolicy{
			"default": {RateLimits: []goncordia.QueueRateLimit{
				{Limit: 2, Period: time.Second, Burst: 2},
				{Limit: 3, Period: time.Minute, Burst: 3},
			}},
		},
	})
	go pool.Start(context.Background()) //nolint:errcheck
	waitForCondition(t, time.Second, func() bool { return started.Load() == 2 }, "initial second burst")
	clk.Advance(500 * time.Millisecond)
	waitForCondition(t, time.Second, func() bool { return started.Load() == 3 }, "third start after second window")
	clk.Advance(19*time.Second + 499*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	if got := started.Load(); got != 3 {
		t.Fatalf("starts before minute window=%d, want 3", got)
	}
	clk.Advance(time.Millisecond)
	waitForCondition(t, time.Second, func() bool { return started.Load() == 4 }, "fourth start after minute window")
	close(release)
	pool.Stop()
}

func TestQueueRateLimitDefaultBurstSpacesStarts(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
	registry := core.NewRegistry()
	release := make(chan struct{})
	var started atomic.Int64
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(context.Context, *core.Job[featureArgs]) error {
		started.Add(1)
		<-release
		return nil
	}), core.WorkerOpts{})
	for i := range 2 {
		if _, err := client.Enqueue(context.Background(), featureArgs{N: i}, nil); err != nil {
			t.Fatal(err)
		}
	}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Concurrency: 2, MaxPending: 2, PollInterval: time.Hour, Clock: clk,
		QueuePolicies: map[string]goncordia.QueuePolicy{
			"default": {RateLimits: []goncordia.QueueRateLimit{{Limit: 2, Period: time.Minute}}},
		},
	})
	go pool.Start(context.Background()) //nolint:errcheck
	waitForCondition(t, time.Second, func() bool { return started.Load() == 1 }, "first smoothed start")
	clk.Advance(29 * time.Second)
	time.Sleep(10 * time.Millisecond)
	if got := started.Load(); got != 1 {
		t.Fatalf("starts before default burst cooldown=%d, want 1", got)
	}
	clk.Advance(time.Second)
	waitForCondition(t, time.Second, func() bool { return started.Load() == 2 }, "second smoothed start")
	close(release)
	pool.Stop()
}

func TestKeyedTagRateLimitHasIndependentBudgets(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 8, 23, 12, 0, 30, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
	registry := core.NewRegistry()
	started := make(chan string, 3)
	release := make(chan struct{})
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(_ context.Context, job *core.Job[featureArgs]) error {
		started <- job.Tags[0]
		<-release
		return nil
	}), core.WorkerOpts{})
	for index, tag := range []string{"tenant:a", "tenant:a", "tenant:b"} {
		if _, err := client.Enqueue(context.Background(), featureArgs{N: index}, &core.InsertOpts{Tags: []string{tag}}); err != nil {
			t.Fatal(err)
		}
	}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Concurrency: 3, MaxPending: 3, PollInterval: time.Hour, Clock: clk,
		QueuePolicies: map[string]goncordia.QueuePolicy{"default": {RateLimits: []goncordia.QueueRateLimit{{
			Limit: 1, Period: time.Minute, Key: goncordia.RateLimitKeyTag, TagPrefix: "tenant:",
		}}}},
	})
	go pool.Start(context.Background()) //nolint:errcheck
	first := <-started
	second := <-started
	if first == second {
		t.Fatalf("independent tags did not receive independent permits: %q, %q", first, second)
	}
	select {
	case third := <-started:
		t.Fatalf("same-tag job started early: %q", third)
	case <-time.After(20 * time.Millisecond):
	}
	clk.Advance(time.Minute)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("same-tag job did not start after refill")
	}
	close(release)
	pool.Stop()
}

func TestKeyedTagConcurrencyLimit(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	registry := core.NewRegistry()
	started := make(chan string, 3)
	releaseA := make(chan struct{})
	releaseB := make(chan struct{})
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(_ context.Context, job *core.Job[featureArgs]) error {
		tag := job.Tags[0]
		started <- tag
		if tag == "tenant:a" {
			<-releaseA
		} else {
			<-releaseB
		}
		return nil
	}), core.WorkerOpts{})
	for index, tag := range []string{"tenant:a", "tenant:a", "tenant:b"} {
		if _, err := client.Enqueue(context.Background(), featureArgs{N: index}, &core.InsertOpts{Tags: []string{tag}}); err != nil {
			t.Fatal(err)
		}
	}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Concurrency: 3, MaxPending: 3, PollInterval: time.Hour,
		QueuePolicies: map[string]goncordia.QueuePolicy{"default": {KeyConcurrency: []goncordia.KeyConcurrencyLimit{{
			Key: goncordia.RateLimitKeyTag, TagPrefix: "tenant:", Limit: 1,
		}}}},
	})
	go pool.Start(context.Background()) //nolint:errcheck
	first, second := <-started, <-started
	if first == second {
		t.Fatalf("expected one start per tenant, got %q and %q", first, second)
	}
	select {
	case third := <-started:
		t.Fatalf("second tenant:a job exceeded concurrency: %q", third)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseA)
	select {
	case tag := <-started:
		if tag != "tenant:a" {
			t.Fatalf("unexpected released tag %q", tag)
		}
	case <-time.After(time.Second):
		t.Fatal("waiting tenant:a job did not start")
	}
	close(releaseB)
	pool.Stop()
}

func TestUpdateQueuePolicyWakesRateLimitedJobs(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
	registry := core.NewRegistry()
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(context.Context, *core.Job[featureArgs]) error {
		started <- struct{}{}
		<-release
		return nil
	}), core.WorkerOpts{})
	for index := range 2 {
		if _, err := client.Enqueue(context.Background(), featureArgs{N: index}, nil); err != nil {
			t.Fatal(err)
		}
	}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Concurrency: 2, MaxPending: 2, PollInterval: time.Hour, Clock: clk,
		QueuePolicies: map[string]goncordia.QueuePolicy{"default": {RateLimits: []goncordia.QueueRateLimit{{
			Limit: 1, Period: time.Hour,
		}}}},
	})
	go pool.Start(context.Background()) //nolint:errcheck
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first job did not start")
	}
	time.Sleep(20 * time.Millisecond)
	if err := pool.UpdateQueuePolicy("default", goncordia.QueuePolicy{}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("policy reload did not wake waiting job")
	}
	close(release)
	pool.Stop()
}

func TestWorkerConfigValidationFailsBeforeStart(t *testing.T) {
	base := memory.New()
	d := &limitedCapabilityDriver{Driver: base, capabilities: driver.Capabilities{}}
	registry := core.NewRegistry()
	invalid := goncordia.WorkerConfig{
		Queues: []string{"default"}, DistributedPipelines: true,
		QueuePolicies: map[string]goncordia.QueuePolicy{
			"missing": {},
			"default": {RateLimits: []goncordia.QueueRateLimit{
				{Limit: 2, Burst: 3},
				{Limit: 1, Scope: goncordia.RateLimitScopeGlobal},
			}},
		},
	}
	if _, err := goncordia.NewWorkerPoolChecked(d, registry, invalid); err == nil ||
		!strings.Contains(err.Error(), "not listed in Queues") ||
		!strings.Contains(err.Error(), "Burst must not exceed Limit") ||
		!strings.Contains(err.Error(), "linearizable CAS") ||
		!strings.Contains(err.Error(), "linearizable leases") {
		t.Fatalf("checked config error=%v", err)
	}
	pool := goncordia.NewWorkerPool(d, registry, invalid)
	if err := pool.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "invalid worker config") {
		t.Fatalf("Start error=%v, want invalid worker config", err)
	}
}

func TestGlobalQueueRateLimitCoordinatesWorkerPools(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
	registry := core.NewRegistry()
	release := make(chan struct{})
	started := make(chan int, 2)
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(_ context.Context, job *core.Job[featureArgs]) error {
		started <- job.Args.N
		<-release
		return nil
	}), core.WorkerOpts{})
	for i := range 2 {
		if _, err := client.Enqueue(context.Background(), featureArgs{N: i}, nil); err != nil {
			t.Fatal(err)
		}
	}
	config := func(workerID string) goncordia.WorkerConfig {
		return goncordia.WorkerConfig{
			WorkerID: workerID, Concurrency: 1, MaxPending: 1, PollInterval: time.Hour,
			Clock: clk, RateLimitPollInterval: time.Second,
			QueuePolicies: map[string]goncordia.QueuePolicy{
				"default": {RateLimits: []goncordia.QueueRateLimit{{
					Limit: 1, Period: time.Minute, Scope: goncordia.RateLimitScopeGlobal,
				}}},
			},
		}
	}
	poolA := goncordia.NewWorkerPool(d, registry, config("rate-worker-a"))
	poolB := goncordia.NewWorkerPool(d, registry, config("rate-worker-b"))
	go poolA.Start(context.Background()) //nolint:errcheck
	go poolB.Start(context.Background()) //nolint:errcheck
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first globally rate-limited handler did not start")
	}
	select {
	case value := <-started:
		t.Fatalf("second handler started before global permit expiry: %d", value)
	case <-time.After(20 * time.Millisecond):
	}
	clk.Advance(59 * time.Second)
	time.Sleep(10 * time.Millisecond)
	select {
	case value := <-started:
		t.Fatalf("second handler started at 59 seconds: %d", value)
	default:
	}
	clk.Advance(time.Second)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("second handler did not start after global permit expiry")
	}
	close(release)
	waitForCondition(t, time.Second, func() bool {
		jobs := d.AllJobs()
		return len(jobs) == 2 && jobs[0].State == driver.JobStateCompleted && jobs[1].State == driver.JobStateCompleted
	}, "globally rate-limited jobs")
	poolA.Stop()
	poolB.Stop()
}

func TestShutdownYieldsJobWaitingForRatePermit(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
	registry := core.NewRegistry()
	release := make(chan struct{})
	var started atomic.Int64
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(context.Context, *core.Job[featureArgs]) error {
		started.Add(1)
		<-release
		return nil
	}), core.WorkerOpts{})
	for i := range 2 {
		if _, err := client.Enqueue(context.Background(), featureArgs{N: i}, nil); err != nil {
			t.Fatal(err)
		}
	}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Concurrency: 2, MaxPending: 2, PollInterval: time.Hour, Clock: clk,
		QueuePolicies: map[string]goncordia.QueuePolicy{
			"default": {RateLimits: []goncordia.QueueRateLimit{{Limit: 1, Period: time.Hour}}},
		},
	})
	go pool.Start(context.Background()) //nolint:errcheck
	waitForCondition(t, time.Second, func() bool { return started.Load() == 1 }, "first rate-limited start")
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- pool.Shutdown(context.Background()) }()
	close(release)
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown waited for a blocked rate permit")
	}
	if got := started.Load(); got != 1 {
		t.Fatalf("handler starts during shutdown=%d, want 1", got)
	}
	jobs := d.AllJobs()
	states := map[driver.JobState]int{}
	for _, job := range jobs {
		states[job.State]++
	}
	if states[driver.JobStateCompleted] != 1 || states[driver.JobStateAvailable] != 1 {
		t.Fatalf("states after shutdown=%v, want one completed and one available", states)
	}
}

func TestDistributedPipelinesSerializeAcrossPools(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	registry := core.NewRegistry()
	release := make(chan struct{})
	var active atomic.Int32
	var maxActive atomic.Int32
	entered := make(chan struct{}, 2)
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(context.Context, *core.Job[featureArgs]) error {
		current := active.Add(1)
		for {
			previous := maxActive.Load()
			if current <= previous || maxActive.CompareAndSwap(previous, current) {
				break
			}
		}
		entered <- struct{}{}
		<-release
		active.Add(-1)
		return nil
	}), core.WorkerOpts{})
	for i := range 2 {
		if _, err := client.Enqueue(context.Background(), featureArgs{N: i}, &core.InsertOpts{PipelineID: "account-1"}); err != nil {
			t.Fatal(err)
		}
	}
	recorder := &claimRecorder{}
	config := goncordia.WorkerConfig{
		Concurrency: 1, MaxPending: 1, PollInterval: 2 * time.Millisecond,
		DistributedPipelines: true, PipelineLeaseDuration: time.Second,
		PipelinePollInterval: 2 * time.Millisecond, Observer: recorder,
	}
	poolA := goncordia.NewWorkerPool(d, registry, config)
	config.WorkerID = "pool-b"
	poolB := goncordia.NewWorkerPool(d, registry, config)
	go poolA.Start(context.Background()) //nolint:errcheck
	go poolB.Start(context.Background()) //nolint:errcheck
	waitForCondition(t, time.Second, func() bool { return len(recorder.snapshot()) == 2 }, "claims across pools")
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("first pipeline job did not enter")
	}
	time.Sleep(20 * time.Millisecond)
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("max distributed pipeline concurrency=%d, want 1", got)
	}
	close(release)
	waitForCondition(t, time.Second, func() bool {
		jobs := d.AllJobs()
		return len(jobs) == 2 && jobs[0].State == driver.JobStateCompleted && jobs[1].State == driver.JobStateCompleted
	}, "distributed pipeline completion")
	poolA.Stop()
	poolB.Stop()
}
