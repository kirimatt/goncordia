package goncordia_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

// pollingDriver has no Listener, like stdlib, Cassandra, ClickHouse,
// DynamoDB, and Firestore, so the pool polls.
type pollingDriver struct{ *memory.Driver }

func (pollingDriver) Listener() driver.Listener { return nil }

// TestWorkerPoolPollsWithoutNotifications covers drivers without push
// notifications: a job enqueued after start is fetched on the next poll tick.
func TestWorkerPoolPollsWithoutNotifications(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC))
	d := pollingDriver{memory.New(memory.WithClock(clk))}
	registry := core.NewRegistry()
	var processed atomic.Int64
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(context.Context, *core.Job[featureArgs]) error {
		processed.Add(1)
		return nil
	}), core.WorkerOpts{})
	pool := goncordia.NewWorkerPool[memory.NoTx](d, registry, goncordia.WorkerConfig{
		PollInterval: time.Second, Clock: clk, StuckJobTimeout: time.Minute, RescueInterval: time.Minute,
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- pool.Start(ctx) }()
	waitForCondition(t, time.Second, func() bool { return clk.ActiveTimers() > 0 }, "poll ticker")

	client := goncordia.NewClient[memory.NoTx](d, goncordia.ClientConfig{Clock: clk})
	if _, err := client.Enqueue(ctx, featureArgs{N: 1}, nil); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, 2*time.Second, func() bool {
		if processed.Load() == 1 {
			return true
		}
		clk.Advance(time.Second)
		return false
	}, "job fetched by polling")
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Start returned %v, want context.Canceled", err)
	}
}

func TestWorkerRecordsNonStringPanicValues(t *testing.T) {
	for name, value := range map[string]any{
		"panic: wrapped failure":     errors.New("wrapped failure"),
		"panic: unknown panic value": 42,
	} {
		t.Run(name, func(t *testing.T) {
			d := memory.New()
			client := goncordia.NewClient(d, goncordia.ClientConfig{})
			registry := core.NewRegistry()
			core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(context.Context, *core.Job[featureArgs]) error {
				panic(value)
			}), core.WorkerOpts{})
			if _, err := client.Enqueue(context.Background(), featureArgs{}, nil); err != nil {
				t.Fatal(err)
			}
			pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
				PollInterval: 5 * time.Millisecond, RetryPolicy: core.NoRetry{},
			})
			go pool.Start(context.Background()) //nolint:errcheck
			waitForCondition(t, time.Second, func() bool {
				jobs := d.AllJobs()
				return len(jobs) == 1 && jobs[0].State == driver.JobStateDiscarded
			}, "panic discard")
			_ = pool.Shutdown(context.Background())
			if got := d.AllJobs()[0].Errors[0].Error; got != name {
				t.Fatalf("recorded error %q, want %q", got, name)
			}
		})
	}
}

func TestNewCronSchedulerChecked(t *testing.T) {
	d := memory.New()
	if _, err := goncordia.NewCronSchedulerChecked(d, []goncordia.PeriodicJob{{Args: featureArgs{}}}, goncordia.CronConfig{}); err == nil {
		t.Fatal("accepted a periodic job without a schedule")
	}
	scheduler, err := goncordia.NewCronSchedulerChecked(d, []goncordia.PeriodicJob{
		{Schedule: core.Every(time.Hour), Args: featureArgs{N: 1}},
	}, goncordia.CronConfig{})
	if err != nil || scheduler == nil {
		t.Fatalf("valid scheduler = %v, %v", scheduler, err)
	}
}

func TestRateLimitScopeString(t *testing.T) {
	if goncordia.RateLimitScopeLocal.String() != "local" || goncordia.RateLimitScopeGlobal.String() != "global" {
		t.Fatalf("scopes = %q, %q", goncordia.RateLimitScopeLocal, goncordia.RateLimitScopeGlobal)
	}
}

func TestNopWorkerObserverAcceptsEveryEvent(t *testing.T) {
	ctx := context.Background()
	var observer goncordia.WorkerObserver = goncordia.NopWorkerObserver{}
	observer.JobClaimed(ctx, driver.JobRow{})
	observer.JobStarted(ctx, goncordia.JobStartedEvent{})
	observer.JobFinished(ctx, goncordia.JobFinishedEvent{})
	observer.JobRateLimited(ctx, goncordia.RateLimitWaitEvent{})
	observer.JobHeartbeat(ctx, goncordia.HeartbeatEvent{})
	observer.JobsRescued(ctx, goncordia.RescueEvent{})
}
