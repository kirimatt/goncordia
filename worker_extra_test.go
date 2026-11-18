package goncordia_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

func TestWorkerMiddlewareWrapsHandlersInOrder(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	var mu sync.Mutex
	var calls []string
	record := func(s string) { mu.Lock(); calls = append(calls, s); mu.Unlock() }
	layer := func(name string) goncordia.JobMiddleware {
		return func(ctx context.Context, job *core.RawJob, next func(context.Context, *core.RawJob) error) error {
			record(name + " before")
			err := next(ctx, job)
			record(name + " after")
			return err
		}
	}
	done := make(chan struct{})
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(context.Context, *core.Job[featureArgs]) error {
		record("handler")
		return nil
	}), core.WorkerOpts{})
	observer := &completionObserver{done: done}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		PollInterval: time.Hour, RetentionInterval: -1, Observer: observer,
		Middleware: []goncordia.JobMiddleware{layer("outer"), layer("inner")},
	})
	go pool.Start(context.Background()) //nolint:errcheck
	defer pool.Shutdown(context.Background()) //nolint:errcheck
	if _, err := client.Enqueue(context.Background(), featureArgs{N: 1}, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("job did not finish")
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"outer before", "inner before", "handler", "inner after", "outer after"}
	if len(calls) != len(want) {
		t.Fatalf("calls = %v, want %v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("calls = %v, want %v", calls, want)
		}
	}
}

// completionObserver closes done when the first job finishes.
type completionObserver struct {
	goncordia.NopWorkerObserver
	done chan struct{}
	once sync.Once
}

func (o *completionObserver) JobFinished(context.Context, goncordia.JobFinishedEvent) {
	o.once.Do(func() { close(o.done) })
}

// heartbeatScript answers heartbeats from a fixed script of outcomes.
type heartbeatScript struct {
	driver.Executor
	mu      sync.Mutex
	answers []heartbeatAnswer
}

type heartbeatAnswer struct {
	renewed bool
	err     error
}

func (e *heartbeatScript) JobHeartbeat(ctx context.Context, p driver.JobHeartbeatParams) (bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.answers) == 0 {
		return e.Executor.(driver.JobHeartbeater).JobHeartbeat(ctx, p)
	}
	answer := e.answers[0]
	e.answers = e.answers[1:]
	return answer.renewed, answer.err
}

type heartbeatRecorder struct {
	goncordia.NopWorkerObserver
	events chan goncordia.HeartbeatEvent
}

func (o *heartbeatRecorder) JobHeartbeat(_ context.Context, event goncordia.HeartbeatEvent) {
	o.events <- event
}

// TestHeartbeatErrorThenLostLeaseCancelsHandler checks that a failed renewal
// is reported and retried, and a refused one cancels the running handler.
func TestHeartbeatErrorThenLostLeaseCancelsHandler(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC))
	mem := memory.New(memory.WithClock(clk))
	boom := errors.New("lease store down")
	exec := &heartbeatScript{Executor: mem.Executor(), answers: []heartbeatAnswer{{err: boom}, {renewed: false}}}
	d := &execDriver{Driver: mem, exec: exec}
	registry := core.NewRegistry()
	handlerErr := make(chan error, 1)
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(ctx context.Context, _ *core.Job[featureArgs]) error {
		<-ctx.Done()
		handlerErr <- ctx.Err()
		return ctx.Err()
	}), core.WorkerOpts{})
	reported := make(chan error, 8)
	recorder := &heartbeatRecorder{events: make(chan goncordia.HeartbeatEvent, 8)}
	pool := goncordia.NewWorkerPool[memory.NoTx](d, registry, goncordia.WorkerConfig{
		Clock: clk, PollInterval: time.Hour, RetentionInterval: -1,
		StuckJobTimeout: 30 * time.Second, HeartbeatInterval: time.Second, RescueInterval: time.Hour,
		Observer: recorder, ErrorHandler: func(err error) { reported <- err },
	})
	go pool.Start(context.Background()) //nolint:errcheck
	defer pool.Shutdown(context.Background()) //nolint:errcheck
	client := goncordia.NewClient[memory.NoTx](d, goncordia.ClientConfig{Clock: clk})
	if _, err := client.Enqueue(context.Background(), featureArgs{N: 1}, nil); err != nil {
		t.Fatal(err)
	}
	waitForCondition(t, 2*time.Second, func() bool {
		for _, job := range mem.AllJobs() {
			if job.State == driver.JobStateRunning {
				return true
			}
		}
		return false
	}, "job running")

	for i, want := range []heartbeatAnswer{{err: boom}, {renewed: false}} {
		var event goncordia.HeartbeatEvent
		waitForCondition(t, 2*time.Second, func() bool {
			select {
			case event = <-recorder.events:
				return true
			default:
				clk.Advance(time.Second)
				return false
			}
		}, "heartbeat")
		if !errors.Is(event.Err, want.err) || event.Renewed != want.renewed {
			t.Fatalf("heartbeat %d = %+v, want %+v", i, event, want)
		}
	}
	select {
	case err := <-handlerErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("handler ctx ended with %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("lost lease did not cancel the handler")
	}
	select {
	case err := <-reported:
		if !errors.Is(err, boom) {
			t.Fatalf("reported %v, want the heartbeat error", err)
		}
	case <-time.After(time.Second):
		t.Fatal("heartbeat error was not reported")
	}
}

// TestGlobalRefusalRollsBackLocalReservation checks that a start refused by
// a global rule returns the local budget it had reserved.
func TestGlobalRefusalRollsBackLocalReservation(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
	registry := core.NewRegistry()
	started := make(chan string, 8)
	release := make(chan struct{})
	close(release)
	startRecorder(registry, started, release)
	for i := range 3 {
		if _, err := client.Enqueue(context.Background(), featureArgs{N: i}, nil); err != nil {
			t.Fatal(err)
		}
	}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Concurrency: 3, MaxPending: 3, PollInterval: time.Hour, Clock: clk, RetentionInterval: -1,
		QueuePolicies: map[string]goncordia.QueuePolicy{"default": {RateLimits: []goncordia.QueueRateLimit{
			{Limit: 2, Period: time.Hour, Mode: goncordia.RateLimitModeFixedWindow},
			{Limit: 1, Period: time.Minute, Scope: goncordia.RateLimitScopeGlobal},
		}}},
	})
	go pool.Start(context.Background()) //nolint:errcheck
	defer pool.Shutdown(context.Background()) //nolint:errcheck

	expectStarts(t, started, 1, "12:00")
	// The second job reserved hourly budget at 12:00 before the global rule
	// refused it. Had that reservation stuck, the hourly window would be full
	// now and the second job would wait until 13:00.
	clk.Advance(time.Minute)
	expectStarts(t, started, 1, "12:01")
	clk.Advance(time.Minute)
	expectStarts(t, started, 0, "12:02, hourly window full")
	clk.Advance(58 * time.Minute)
	expectStarts(t, started, 1, "13:00")
}
