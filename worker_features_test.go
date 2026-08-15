package goncordia_test

import (
	"context"
	"errors"
	"slices"
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
