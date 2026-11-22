package pgxv5_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	pgxdriver "github.com/kirimatt/goncordia/driver/pgxv5/v2"
	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver/drivertest"
)

func TestPgxv5_ConcurrentMigrateAndOwnership(t *testing.T) {
	skipIfNoDocker(t)
	pool, cleanup := newTestPool(t)
	defer cleanup()
	drivers := []*pgxdriver.Driver{pgxdriver.New(pool), pgxdriver.New(pool)}
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- drivers[i%len(drivers)].Migrate(context.Background())
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Errorf("concurrent migrate: %v", err)
		}
	}
	if err := drivers[0].Close(); err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(context.Background()); err != nil {
		t.Fatalf("driver closed caller-owned pool: %v", err)
	}
}

// skipIfNoDocker skips the test if Docker is not available.
func skipIfNoDocker(t *testing.T) {
	t.Helper()
	if os.Getenv("SKIP_INTEGRATION") != "" {
		t.Skip("SKIP_INTEGRATION set")
	}
}

// testDBSeq numbers the databases created on an external server.
var testDBSeq atomic.Int64

// newExternalTestPool creates a fresh database for one test on the server in
// GONCORDIA_TEST_PG_DSN, for running the suite where Docker is unavailable.
func newExternalTestPool(t *testing.T, adminDSN string) (*pgxpool.Pool, func()) {
	t.Helper()
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatalf("connect admin: %v", err)
	}
	defer admin.Close(ctx) //nolint:errcheck
	name := fmt.Sprintf("goncordia_t_%d_%d", os.Getpid(), testDBSeq.Add(1))
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatalf("create database: %v", err)
	}
	cfg, err := pgxpool.ParseConfig(adminDSN)
	if err != nil {
		t.Fatalf("parse dsn: %v", err)
	}
	cfg.ConnConfig.Database = name
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	return pool, func() {
		pool.Close()
		if c, err := pgx.Connect(ctx, adminDSN); err == nil {
			_, _ = c.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
			_ = c.Close(ctx)
		}
	}
}

func newTestPool(t *testing.T) (*pgxpool.Pool, func()) {
	t.Helper()
	ctx := context.Background()
	if dsn := os.Getenv("GONCORDIA_TEST_PG_DSN"); dsn != "" {
		return newExternalTestPool(t, dsn)
	}

	ctr, err := tcpostgres.Run(ctx,
		"postgres:17",
		tcpostgres.WithDatabase("goncordia_test"),
		tcpostgres.WithUsername("goncordia"),
		tcpostgres.WithPassword("goncordia"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(30*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}

	dsn, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		t.Fatalf("get connection string: %v", err)
	}

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		t.Fatalf("create pool: %v", err)
	}

	return pool, func() {
		pool.Close()
		ctr.Terminate(ctx) //nolint:errcheck
	}
}

// --- job types for tests ---

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func (EmailJob) Kind() string { return "email" }

// --- tests ---

func TestPgxv5_EnqueueAndProcess(t *testing.T) {
	skipIfNoDocker(t)
	pool, cleanup := newTestPool(t)
	defer cleanup()

	ctx := context.Background()
	d := pgxdriver.New(pool)
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	drivertest.Run(t, d.Executor())
	drivertest.RunResults(t, d.Executor())
	drivertest.RunRetention(t, d)

	var processed atomic.Int64
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		processed.Add(1)
		return nil
	}), core.WorkerOpts{MaxRetry: 3})

	client := goncordia.NewClient(d, goncordia.ClientConfig{})

	for i := 0; i < 5; i++ {
		if _, err := client.Enqueue(ctx, EmailJob{To: "a@b.com", Subject: "hi"}, nil); err != nil {
			t.Fatal(err)
		}
	}

	pool2 := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  5,
		PollInterval: 100 * time.Millisecond,
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go pool2.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(10 * time.Second)
	for processed.Load() < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: processed %d/5", processed.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = pool2.Shutdown(context.Background())
}

func TestPgxv5_EnqueueTx(t *testing.T) {
	skipIfNoDocker(t)
	pool, cleanup := newTestPool(t)
	defer cleanup()

	ctx := context.Background()
	d := pgxdriver.New(pool)
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	client := goncordia.NewClient(d, goncordia.ClientConfig{})

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}

	result, err := client.EnqueueTx(ctx, tx, EmailJob{To: "tx@test.com", Subject: "tx"}, nil)
	if err != nil {
		tx.Rollback(ctx) //nolint:errcheck
		t.Fatal(err)
	}
	if result.Job == nil {
		t.Fatal("expected non-nil job")
	}

	// Rollback — job should NOT appear
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}

	rows, err := d.Executor().JobFetchBatch(ctx, pgxdriver.FetchParams("default", 10))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected 0 jobs after rollback, got %d", len(rows))
	}
}

func TestPgxv5_UniqueJobs(t *testing.T) {
	skipIfNoDocker(t)
	pool, cleanup := newTestPool(t)
	defer cleanup()

	ctx := context.Background()
	d := pgxdriver.New(pool)
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	client := goncordia.NewClient(d, goncordia.ClientConfig{})

	opts := &core.InsertOpts{UniqueOpts: &core.UniqueOpts{ByArgs: true, ByQueue: true}}

	r1, err := client.Enqueue(ctx, EmailJob{To: "dup@test.com", Subject: "Hello"}, opts)
	if err != nil {
		t.Fatalf("first insert failed: %v", err)
	}
	if r1.UniqueSkip {
		t.Fatal("first insert should not be a duplicate")
	}

	r2, err := client.Enqueue(ctx, EmailJob{To: "dup@test.com", Subject: "Hello"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !r2.UniqueSkip {
		t.Fatal("expected second insert to be a duplicate")
	}
}

func TestPgxv5_ScheduledJob(t *testing.T) {
	skipIfNoDocker(t)
	pool, cleanup := newTestPool(t)
	defer cleanup()

	ctx := context.Background()
	clk := clock.NewManual(time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))
	d := pgxdriver.New(pool, pgxdriver.WithClock(clk))
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	client := goncordia.NewClient(d, goncordia.ClientConfig{})

	_, err := client.Enqueue(ctx, EmailJob{To: "scheduled@test.com"}, &core.InsertOpts{
		RunAt: clk.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Not visible yet
	rows, _ := d.Executor().JobFetchBatch(ctx, pgxdriver.FetchParams("default", 10))
	if len(rows) != 0 {
		t.Fatalf("expected 0 jobs before RunAt, got %d", len(rows))
	}

	// Advance clock past RunAt
	clk.Advance(2 * time.Hour)
	rows, _ = d.Executor().JobFetchBatch(ctx, pgxdriver.FetchParams("default", 10))
	if len(rows) != 1 {
		t.Fatalf("expected 1 job after RunAt, got %d", len(rows))
	}
}

func TestPgxv5_RetryAndDiscard(t *testing.T) {
	skipIfNoDocker(t)
	pool, cleanup := newTestPool(t)
	defer cleanup()

	ctx := context.Background()
	clk := clock.NewManual(time.Now())
	d := pgxdriver.New(pool, pgxdriver.WithClock(clk))
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	client := goncordia.NewClient(d, goncordia.ClientConfig{})

	var attempts atomic.Int64
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		attempts.Add(1)
		return errors.New("always fails")
	}), core.WorkerOpts{MaxRetry: 2})

	if _, err := client.Enqueue(ctx, EmailJob{To: "fail@test.com"}, nil); err != nil {
		t.Fatal(err)
	}

	workerPool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  1,
		PollInterval: 50 * time.Millisecond,
		RetryPolicy:  core.FixedRetry{Delay: 100 * time.Millisecond},
		Clock:        clk,
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go workerPool.Start(runCtx) //nolint:errcheck

	// Wait for attempts to hit MaxRetry
	deadline := time.Now().Add(15 * time.Second)
	for attempts.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d attempts", attempts.Load())
		}
		clk.Advance(200 * time.Millisecond)
		time.Sleep(50 * time.Millisecond)
	}

	_ = workerPool.Shutdown(context.Background())
	if got := attempts.Load(); got < 2 {
		t.Errorf("expected >= 2 attempts, got %d", got)
	}
}
