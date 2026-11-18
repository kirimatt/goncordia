package goncordia_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

// blockingListExecutor makes a retention pass hang in JobList until its
// context ends, then fail with the context's error, as a database driver
// does when the pool stops mid-pass.
type blockingListExecutor struct {
	driver.Executor
	admin   driver.AdminExecutor
	listing chan struct{}
	once    sync.Once
}

func (e *blockingListExecutor) JobList(ctx context.Context, p driver.JobListParams) ([]driver.JobRow, error) {
	if p.State == "" {
		return e.admin.JobList(ctx, p)
	}
	e.once.Do(func() { close(e.listing) })
	<-ctx.Done()
	return nil, ctx.Err()
}

func (e *blockingListExecutor) QueueStats(ctx context.Context, queue string) (driver.QueueStats, error) {
	return e.admin.QueueStats(ctx, queue)
}

type blockingListDriver struct {
	*memory.Driver
	exec *blockingListExecutor
}

func (d *blockingListDriver) Executor() driver.Executor { return d.exec }

func newBlockingListDriver(clk clock.Clock) *blockingListDriver {
	mem := memory.New(memory.WithClock(clk))
	return &blockingListDriver{Driver: mem, exec: &blockingListExecutor{
		Executor: mem.Executor(), admin: mem.Executor().(driver.AdminExecutor), listing: make(chan struct{}),
	}}
}

type passRecorder struct {
	goncordia.NopWorkerObserver
	mu     sync.Mutex
	events []goncordia.RetentionPassEvent
}

func (r *passRecorder) RetentionPassed(_ context.Context, event goncordia.RetentionPassEvent) {
	r.mu.Lock()
	r.events = append(r.events, event)
	r.mu.Unlock()
}

func TestRunRetentionStaysQuietWhenStoppedMidPass(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC))
	d := newBlockingListDriver(clk)
	m := goncordia.NewMaintenance[memory.NoTx](d, goncordia.MaintenanceConfig{Clock: clk})
	var mu sync.Mutex
	var reported []error
	observer := &passRecorder{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- m.RunRetention(ctx, goncordia.RetentionRunConfig{
			Policy:   goncordia.RetentionPolicy{Completed: time.Hour},
			Observer: observer,
			OnPrune: func(_ goncordia.PruneResult, err error) {
				mu.Lock()
				reported = append(reported, err)
				mu.Unlock()
			},
		})
	}()
	select {
	case <-d.exec.listing:
	case <-time.After(2 * time.Second):
		t.Fatal("retention pass did not start")
	}
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if len(reported) != 0 {
		t.Fatalf("a pass stopped by cancellation reported %v", reported)
	}
	if len(observer.events) != 0 {
		t.Fatalf("observer saw a cancelled pass: %+v", observer.events)
	}
}

func TestWorkerPoolShutdownReportsNoRetentionError(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC))
	d := newBlockingListDriver(clk)
	var mu sync.Mutex
	var reported []error
	pool := goncordia.NewWorkerPool[memory.NoTx](d, core.NewRegistry(), goncordia.WorkerConfig{
		Clock: clk, PollInterval: time.Hour, RescueInterval: time.Hour,
		ErrorHandler: func(err error) { mu.Lock(); reported = append(reported, err); mu.Unlock() },
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- pool.Start(ctx) }()
	select {
	case <-d.exec.listing:
	case <-time.After(2 * time.Second):
		t.Fatal("retention pass did not start")
	}
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	if len(reported) != 0 {
		t.Fatalf("graceful shutdown reported %v", reported)
	}
}
