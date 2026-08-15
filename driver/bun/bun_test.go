package bundriver_test

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	_ "modernc.org/sqlite"

	"github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	bundriver "github.com/kirimatt/goncordia/driver/bun"
	"github.com/kirimatt/goncordia/driver/drivertest"
)

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func (EmailJob) Kind() string { return "email" }

func newBunDB(t *testing.T) *bun.DB {
	t.Helper()
	sqlDB, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	sqlDB.SetMaxOpenConns(2) // tx + concurrent reads
	db := bun.NewDB(sqlDB, sqlitedialect.New())
	t.Cleanup(func() { db.Close() })
	return db
}

func newDriver(t *testing.T, opts ...bundriver.Option) (*bundriver.Driver, *bun.DB) {
	t.Helper()
	db := newBunDB(t)
	d := bundriver.New(db, opts...)
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return d, db
}

func TestBun_EnqueueAndProcess(t *testing.T) {
	d, _ := newDriver(t)
	drivertest.Run(t, d.Executor())
	ctx := context.Background()

	var processed atomic.Int64
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		processed.Add(1)
		return nil
	}), core.WorkerOpts{Queue: "default", MaxRetry: 3})

	client := bundriver.NewClient(d, goncordia.ClientConfig{})
	for i := 0; i < 3; i++ {
		if _, err := client.Enqueue(ctx, EmailJob{To: "a@b.com", Subject: "hi"}, nil); err != nil {
			t.Fatal(err)
		}
	}

	wp := bundriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  3,
		PollInterval: 50 * time.Millisecond,
	})
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go wp.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(10 * time.Second)
	for processed.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: processed %d/3", processed.Load())
		}
		time.Sleep(30 * time.Millisecond)
	}
	wp.Stop()
}

func TestBun_EnqueueTx(t *testing.T) {
	d, db := newDriver(t)
	ctx := context.Background()
	client := bundriver.NewClient(d, goncordia.ClientConfig{})

	// Commit path: job must appear.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	result, err := client.EnqueueTx(ctx, tx, EmailJob{To: "commit@test.com"}, nil)
	if err != nil {
		tx.Rollback() //nolint:errcheck
		t.Fatal(err)
	}
	if result.Job == nil {
		t.Fatal("expected non-nil job")
	}
	tx.Commit() //nolint:errcheck

	rows, err := d.Executor().JobFetchBatch(ctx, bundriver.FetchParams("default", 10))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 job after commit, got %d", len(rows))
	}

	// Rollback path: job must not appear.
	tx2, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	_, err = client.EnqueueTx(ctx, tx2, EmailJob{To: "rollback@test.com"}, nil)
	if err != nil {
		tx2.Rollback() //nolint:errcheck
		t.Fatal(err)
	}
	tx2.Rollback() //nolint:errcheck

	// First job is now running; no new jobs from the rolled-back tx.
	rows2, err := d.Executor().JobFetchBatch(ctx, bundriver.FetchParams("default", 10))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows2) != 0 {
		t.Fatalf("expected 0 jobs after rollback, got %d", len(rows2))
	}
}

func TestBun_UniqueJobs(t *testing.T) {
	d, _ := newDriver(t)
	ctx := context.Background()
	client := bundriver.NewClient(d, goncordia.ClientConfig{})
	opts := &core.InsertOpts{UniqueOpts: &core.UniqueOpts{ByArgs: true, ByQueue: true}}

	r1, err := client.Enqueue(ctx, EmailJob{To: "dup@test.com", Subject: "Hello"}, opts)
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if r1.UniqueSkip {
		t.Fatal("first insert should not be duplicate")
	}

	r2, err := client.Enqueue(ctx, EmailJob{To: "dup@test.com", Subject: "Hello"}, opts)
	if err != nil {
		t.Fatalf("second insert: %v", err)
	}
	if !r2.UniqueSkip {
		t.Fatal("expected second insert to be duplicate")
	}
}

func TestBun_ScheduledJob(t *testing.T) {
	clk := clock.NewMock(time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))
	d, _ := newDriver(t, bundriver.WithClock(clk))
	ctx := context.Background()
	client := bundriver.NewClient(d, goncordia.ClientConfig{})

	_, err := client.Enqueue(ctx, EmailJob{To: "scheduled@test.com"}, &core.InsertOpts{
		RunAt: clk.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	rows, _ := d.Executor().JobFetchBatch(ctx, bundriver.FetchParams("default", 10))
	if len(rows) != 0 {
		t.Fatalf("expected 0 before RunAt, got %d", len(rows))
	}

	clk.Advance(2 * time.Hour)
	rows, _ = d.Executor().JobFetchBatch(ctx, bundriver.FetchParams("default", 10))
	if len(rows) != 1 {
		t.Fatalf("expected 1 after RunAt, got %d", len(rows))
	}
}

func TestBun_RetryAndDiscard(t *testing.T) {
	clk := clock.NewMock(time.Now())
	d, _ := newDriver(t, bundriver.WithClock(clk))
	ctx := context.Background()

	var attempts atomic.Int64
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		attempts.Add(1)
		return errors.New("always fails")
	}), core.WorkerOpts{Queue: "default", MaxRetry: 2})

	client := bundriver.NewClient(d, goncordia.ClientConfig{})
	if _, err := client.Enqueue(ctx, EmailJob{To: "fail@test.com"}, nil); err != nil {
		t.Fatal(err)
	}

	wp := bundriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  1,
		PollInterval: 50 * time.Millisecond,
		RetryPolicy:  core.FixedRetry{Delay: 50 * time.Millisecond},
		Clock:        clk,
	})
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go wp.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(15 * time.Second)
	for attempts.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d attempts", attempts.Load())
		}
		clk.Advance(100 * time.Millisecond)
		time.Sleep(60 * time.Millisecond)
	}
	wp.Stop()
}
