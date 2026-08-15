package goncordia_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver"
	memdriver "github.com/kirimatt/goncordia/driver/memory"
)

// TestScheduledJobNotVisibleUntilRunAt verifies that a job with a future RunAt
// is not picked up until the clock advances past that time — no real sleeping needed.
func TestScheduledJobNotVisibleUntilRunAt(t *testing.T) {
	base := time.Date(2024, 6, 1, 12, 0, 0, 0, time.UTC)
	clk := clock.NewMock(base)

	d := memdriver.New(memdriver.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	exec := d.Executor()
	ctx := context.Background()

	// Schedule job 1 hour in the future
	_, err := client.Enqueue(ctx, EmailArgs{To: "future@test.com"}, &core.InsertOpts{
		RunAt: base.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	fetch := func() []driver.JobRow {
		rows, _ := exec.JobFetchBatch(ctx, driver.FetchParams{Queue: "default", Limit: 10})
		return rows
	}

	// t=0: not yet visible
	if rows := fetch(); len(rows) != 0 {
		t.Fatalf("expected 0 jobs at t=0, got %d", len(rows))
	}

	// t=30min: still not visible
	clk.Advance(30 * time.Minute)
	if rows := fetch(); len(rows) != 0 {
		t.Fatalf("expected 0 jobs at t=30min, got %d", len(rows))
	}

	// t=61min: RunAt passed, job is now available
	clk.Advance(31 * time.Minute)
	if rows := fetch(); len(rows) != 1 {
		t.Fatalf("expected 1 job at t=61min, got %d", len(rows))
	}
}

// TestRetryRescheduledWithMockClock verifies that a failed job is rescheduled
// according to the retry policy, and only becomes visible after the clock advances.
func TestRetryRescheduledWithMockClock(t *testing.T) {
	base := time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewMock(base)

	d := memdriver.New(memdriver.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{})

	var attempts atomic.Int64
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[EmailArgs](func(_ context.Context, _ *core.Job[EmailArgs]) error {
		attempts.Add(1)
		return errors.New("transient error")
	}), core.WorkerOpts{Queue: "default", MaxRetry: 3})

	ctx := context.Background()
	if _, err := client.Enqueue(ctx, EmailArgs{To: "retry@test.com"}, nil); err != nil {
		t.Fatal(err)
	}

	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  1,
		PollInterval: 50 * time.Millisecond,
		RetryPolicy:  core.FixedRetry{Delay: 10 * time.Minute},
		Clock:        clk,
	})

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	go pool.Start(runCtx) //nolint:errcheck

	// First attempt happens immediately
	waitForCondition(t, 2*time.Second, func() bool { return attempts.Load() >= 1 }, "first attempt")

	// Wait until processRow finishes: job must leave Running state before we advance the clock.
	// If we advance while processRow is still in NextRetryAt(clk), retryAt shifts forward.
	waitForCondition(t, 2*time.Second, func() bool {
		for _, j := range d.AllJobs() {
			if j.State == driver.JobStateRunning {
				return false
			}
		}
		return true
	}, "job leaves Running state")

	// 5 minutes in: retryAt = base+10min, job must not be visible yet
	clk.Advance(5 * time.Minute)
	snap := attempts.Load()
	time.Sleep(200 * time.Millisecond)
	if attempts.Load() != snap {
		t.Fatal("job was processed before retry delay elapsed")
	}

	// Advance past retry delay — job becomes visible
	clk.Advance(6 * time.Minute) // total: 11min > retryAt (10min)
	waitForCondition(t, 2*time.Second, func() bool { return attempts.Load() >= 2 }, "second attempt after clock advance")

	pool.Stop()
}

func waitForCondition(t *testing.T, timeout time.Duration, cond func() bool, label string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for: %s", label)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
