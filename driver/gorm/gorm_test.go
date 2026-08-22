package gormdriver_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	gormpkg "gorm.io/gorm"

	"github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver/drivertest"
	gormdriver "github.com/kirimatt/goncordia/driver/gorm"
)

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func (EmailJob) Kind() string { return "email" }

func newGormDB(t *testing.T) *gormpkg.DB {
	t.Helper()
	db, err := gormpkg.Open(sqlite.Open("file::memory:?cache=shared"), &gormpkg.Config{})
	if err != nil {
		t.Fatalf("open gorm db: %v", err)
	}
	sqlDB, _ := db.DB()
	sqlDB.SetMaxOpenConns(2) // tx + concurrent reads
	t.Cleanup(func() { sqlDB.Close() })
	return db
}

func newDriver(t *testing.T, opts ...gormdriver.Option) (*gormdriver.Driver, *gormpkg.DB) {
	t.Helper()
	gdb := newGormDB(t)
	d, err := gormdriver.New(gdb, opts...)
	if err != nil {
		t.Fatalf("new driver: %v", err)
	}
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return d, gdb
}

func TestGorm_EnqueueAndProcess(t *testing.T) {
	d, _ := newDriver(t)
	drivertest.Run(t, d.Executor())
	ctx := context.Background()

	var processed atomic.Int64
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		processed.Add(1)
		return nil
	}), core.WorkerOpts{Queue: "default", MaxRetry: 3})

	client := gormdriver.NewClient(d, goncordia.ClientConfig{})
	for i := 0; i < 3; i++ {
		if _, err := client.Enqueue(ctx, EmailJob{To: "a@b.com", Subject: "hi"}, nil); err != nil {
			t.Fatal(err)
		}
	}

	wp := gormdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
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

func TestGorm_EnqueueTx(t *testing.T) {
	d, gdb := newDriver(t)
	ctx := context.Background()
	client := gormdriver.NewClient(d, goncordia.ClientConfig{})

	// Commit path: job must appear.
	tx := gdb.Begin()
	if tx.Error != nil {
		t.Fatalf("begin: %v", tx.Error)
	}
	result, err := client.EnqueueTx(ctx, tx, EmailJob{To: "commit@test.com"}, nil)
	if err != nil {
		tx.Rollback() //nolint:errcheck
		t.Fatal(err)
	}
	if result.Job == nil {
		t.Fatal("expected non-nil job")
	}
	tx.Commit()

	rows, err := d.Executor().JobFetchBatch(ctx, gormdriver.FetchParams("default", 10))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 job after commit, got %d", len(rows))
	}

	// Rollback path: job must not appear.
	tx2 := gdb.Begin()
	if tx2.Error != nil {
		t.Fatalf("begin tx2: %v", tx2.Error)
	}
	_, err = client.EnqueueTx(ctx, tx2, EmailJob{To: "rollback@test.com"}, nil)
	if err != nil {
		tx2.Rollback() //nolint:errcheck
		t.Fatal(err)
	}
	tx2.Rollback()

	// The first job is now in 'running' state (fetched above); no new jobs from rollback.
	rows2, err := d.Executor().JobFetchBatch(ctx, gormdriver.FetchParams("default", 10))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows2) != 0 {
		t.Fatalf("expected 0 jobs after rollback, got %d", len(rows2))
	}
}

func TestGorm_UniqueJobs(t *testing.T) {
	d, _ := newDriver(t)
	ctx := context.Background()
	client := gormdriver.NewClient(d, goncordia.ClientConfig{})
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

func TestGorm_ScheduledJob(t *testing.T) {
	clk := clock.NewMock(time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))
	d, _ := newDriver(t, gormdriver.WithClock(clk))
	ctx := context.Background()
	client := gormdriver.NewClient(d, goncordia.ClientConfig{})

	_, err := client.Enqueue(ctx, EmailJob{To: "scheduled@test.com"}, &core.InsertOpts{
		RunAt: clk.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	rows, _ := d.Executor().JobFetchBatch(ctx, gormdriver.FetchParams("default", 10))
	if len(rows) != 0 {
		t.Fatalf("expected 0 before RunAt, got %d", len(rows))
	}

	clk.Advance(2 * time.Hour)
	rows, _ = d.Executor().JobFetchBatch(ctx, gormdriver.FetchParams("default", 10))
	if len(rows) != 1 {
		t.Fatalf("expected 1 after RunAt, got %d", len(rows))
	}
}

func TestGorm_RetryAndDiscard(t *testing.T) {
	clk := clock.NewMock(time.Now())
	d, _ := newDriver(t, gormdriver.WithClock(clk))
	ctx := context.Background()

	var attempts atomic.Int64
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		attempts.Add(1)
		return errors.New("always fails")
	}), core.WorkerOpts{Queue: "default", MaxRetry: 2})

	client := gormdriver.NewClient(d, goncordia.ClientConfig{})
	if _, err := client.Enqueue(ctx, EmailJob{To: "fail@test.com"}, nil); err != nil {
		t.Fatal(err)
	}

	wp := gormdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
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
