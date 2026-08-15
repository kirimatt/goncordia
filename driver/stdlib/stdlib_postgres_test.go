package stdlib_test

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver/drivertest"
	stdlibdriver "github.com/kirimatt/goncordia/driver/stdlib"
)

func newPostgresDriver(t *testing.T, opts ...stdlibdriver.Option) *stdlibdriver.Driver {
	t.Helper()
	ctx := context.Background()

	db, err := sql.Open("pgx", postgresDSN)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}

	// Drop and recreate tables for test isolation (shared container).
	if _, err := db.ExecContext(ctx,
		"DROP TABLE IF EXISTS goncordia_jobs; DROP TABLE IF EXISTS goncordia_queues; DROP TABLE IF EXISTS goncordia_leaders; DROP TABLE IF EXISTS goncordia_schema_migrations",
	); err != nil {
		db.Close()
		t.Fatalf("drop tables: %v", err)
	}

	d := stdlibdriver.New(db, stdlibdriver.Postgres, opts...)
	if err := d.Migrate(ctx); err != nil {
		db.Close()
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return d
}

func TestStdlibPostgres_EnqueueAndProcess(t *testing.T) {
	d := newPostgresDriver(t)
	drivertest.Run(t, d.Executor())
	ctx := context.Background()

	var processed atomic.Int64
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		processed.Add(1)
		return nil
	}), core.WorkerOpts{Queue: "default", MaxRetry: 3})

	client := stdlibdriver.NewClient(d, goncordia.ClientConfig{})
	for i := 0; i < 5; i++ {
		if _, err := client.Enqueue(ctx, EmailJob{To: "a@b.com", Subject: "hi"}, nil); err != nil {
			t.Fatal(err)
		}
	}

	wp := stdlibdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  5,
		PollInterval: 100 * time.Millisecond,
	})
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go wp.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(15 * time.Second)
	for processed.Load() < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: processed %d/5", processed.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
	wp.Stop()
}

func TestStdlibPostgres_EnqueueTx(t *testing.T) {
	d := newPostgresDriver(t)
	ctx := context.Background()
	client := stdlibdriver.NewClient(d, goncordia.ClientConfig{})

	tx, err := d.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.EnqueueTx(ctx, tx, EmailJob{To: "tx@test.com", Subject: "tx"}, nil)
	if err != nil {
		tx.Rollback() //nolint:errcheck
		t.Fatal(err)
	}
	if result.Job == nil {
		t.Fatal("expected non-nil job")
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	rows, err := d.Executor().JobFetchBatch(ctx, stdlibdriver.FetchParams("default", 10))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected 0 jobs after rollback, got %d", len(rows))
	}
}

func TestStdlibPostgres_UniqueJobs(t *testing.T) {
	d := newPostgresDriver(t)
	ctx := context.Background()
	client := stdlibdriver.NewClient(d, goncordia.ClientConfig{})
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

func TestStdlibPostgres_ScheduledJob(t *testing.T) {
	clk := clock.NewMock(time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))
	d := newPostgresDriver(t, stdlibdriver.WithClock(clk))
	ctx := context.Background()
	client := stdlibdriver.NewClient(d, goncordia.ClientConfig{})

	_, err := client.Enqueue(ctx, EmailJob{To: "scheduled@test.com"}, &core.InsertOpts{
		RunAt: clk.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	rows, _ := d.Executor().JobFetchBatch(ctx, stdlibdriver.FetchParams("default", 10))
	if len(rows) != 0 {
		t.Fatalf("expected 0 before RunAt, got %d", len(rows))
	}

	clk.Advance(2 * time.Hour)
	rows, _ = d.Executor().JobFetchBatch(ctx, stdlibdriver.FetchParams("default", 10))
	if len(rows) != 1 {
		t.Fatalf("expected 1 after RunAt, got %d", len(rows))
	}
}

func TestStdlibPostgres_RetryAndDiscard(t *testing.T) {
	clk := clock.NewMock(time.Now())
	d := newPostgresDriver(t, stdlibdriver.WithClock(clk))
	ctx := context.Background()

	var attempts atomic.Int64
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		attempts.Add(1)
		return errors.New("always fails")
	}), core.WorkerOpts{Queue: "default", MaxRetry: 2})

	client := stdlibdriver.NewClient(d, goncordia.ClientConfig{})
	if _, err := client.Enqueue(ctx, EmailJob{To: "fail@test.com"}, nil); err != nil {
		t.Fatal(err)
	}

	wp := stdlibdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  1,
		PollInterval: 100 * time.Millisecond,
		RetryPolicy:  core.FixedRetry{Delay: 100 * time.Millisecond},
		Clock:        clk,
	})
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go wp.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(30 * time.Second)
	for attempts.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d attempts", attempts.Load())
		}
		clk.Advance(200 * time.Millisecond)
		time.Sleep(100 * time.Millisecond)
	}
	wp.Stop()
}

func TestStdlibPostgres_SkipLocked(t *testing.T) {
	// Verify that concurrent fetchers don't claim the same job (SKIP LOCKED).
	d := newPostgresDriver(t)
	ctx := context.Background()
	client := stdlibdriver.NewClient(d, goncordia.ClientConfig{})

	for i := 0; i < 10; i++ {
		if _, err := client.Enqueue(ctx, EmailJob{To: "skip@test.com"}, nil); err != nil {
			t.Fatal(err)
		}
	}

	var totalClaimed atomic.Int64
	fetch := func() {
		rows, err := d.Executor().JobFetchBatch(ctx, stdlibdriver.FetchParams("default", 5))
		if err != nil {
			t.Errorf("fetch: %v", err)
			return
		}
		totalClaimed.Add(int64(len(rows)))
	}

	// Two concurrent fetchers — combined should claim exactly 10 jobs, not more.
	done := make(chan struct{})
	go func() { fetch(); close(done) }()
	fetch()
	<-done

	if got := totalClaimed.Load(); got != 10 {
		t.Errorf("SKIP LOCKED: expected 10 claimed total, got %d", got)
	}
}
