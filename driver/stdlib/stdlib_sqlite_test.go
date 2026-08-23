package stdlib_test

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver/drivertest"
	stdlibdriver "github.com/kirimatt/goncordia/driver/stdlib"
)

func TestStdlibSQLite_ConcurrentMigrateAndOwnership(t *testing.T) {
	path := filepath.Join(t.TempDir(), "goncordia.db")
	db1, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db1.Close()
	db2, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	drivers := []*stdlibdriver.Driver{
		stdlibdriver.New(db1, stdlibdriver.SQLite),
		stdlibdriver.New(db2, stdlibdriver.SQLite),
	}
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
			t.Fatalf("concurrent migrate: %v", err)
		}
	}

	var migrations int
	if err := db1.QueryRow(`SELECT COUNT(*) FROM goncordia_schema_migrations`).Scan(&migrations); err != nil {
		t.Fatal(err)
	}
	if migrations != 7 {
		t.Fatalf("migration count=%d, want 7", migrations)
	}
	if err := drivers[0].Close(); err != nil {
		t.Fatal(err)
	}
	if err := db1.Ping(); err != nil {
		t.Fatalf("driver closed caller-owned database: %v", err)
	}
}

func newSQLiteDriver(t *testing.T, opts ...stdlibdriver.Option) (*stdlibdriver.Driver, *sql.DB) {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1) // SQLite: single writer
	t.Cleanup(func() { db.Close() })

	d := stdlibdriver.New(db, stdlibdriver.SQLite, opts...)
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return d, db
}

func TestStdlibSQLite_MigratesExistingSchema(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	// This is the complete pre-pipeline schema. Existing installations have no
	// migration journal, so 001 must remain safe before 002 adds pipeline_id.
	if _, err := db.Exec(`CREATE TABLE goncordia_jobs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		queue TEXT NOT NULL,
		kind TEXT NOT NULL,
		args TEXT NOT NULL DEFAULT '{}',
		state TEXT NOT NULL DEFAULT 'available',
		priority INTEGER NOT NULL DEFAULT 0,
		run_at DATETIME NOT NULL,
		created_at DATETIME NOT NULL,
		attempted_at DATETIME,
		finalized_at DATETIME,
		attempt_num INTEGER NOT NULL DEFAULT 0,
		max_retry INTEGER NOT NULL DEFAULT 0,
		timeout_ms INTEGER NOT NULL DEFAULT 0,
		unique_key TEXT,
		worker_id TEXT,
		tags TEXT NOT NULL DEFAULT '[]',
		errors TEXT NOT NULL DEFAULT '[]'
	)`); err != nil {
		t.Fatal(err)
	}
	d := stdlibdriver.New(db, stdlibdriver.SQLite)
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatalf("first migrate: %v", err)
	}
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatalf("idempotent migrate: %v", err)
	}
	rows, err := db.Query(`PRAGMA table_info(goncordia_jobs)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	found := make(map[string]bool)
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull, primaryKey int
		var defaultValue any
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		found[name] = true
	}
	if !found["pipeline_id"] {
		t.Fatal("pipeline_id was not added to existing schema")
	}
	if !found["started_at"] {
		t.Fatal("started_at was not added to existing schema")
	}
}

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func (EmailJob) Kind() string { return "email" }

func TestStdlibSQLite_EnqueueAndProcess(t *testing.T) {
	d, _ := newSQLiteDriver(t)
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
		PollInterval: 50 * time.Millisecond,
	})
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go wp.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(10 * time.Second)
	for processed.Load() < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: processed %d/5", processed.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}
	wp.Stop()
}

func TestStdlibSQLite_EnqueueTx(t *testing.T) {
	d, db := newSQLiteDriver(t)
	ctx := context.Background()
	client := stdlibdriver.NewClient(d, goncordia.ClientConfig{})

	tx, err := db.BeginTx(ctx, nil)
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

	// Nothing visible after rollback
	rows, err := d.Executor().JobFetchBatch(ctx, stdlibdriver.FetchParams("default", 10))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected 0 jobs after rollback, got %d", len(rows))
	}
}

func TestStdlibSQLite_UniqueJobs(t *testing.T) {
	d, _ := newSQLiteDriver(t)
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

func TestStdlibSQLite_ScheduledJob(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock(time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))
	d, _ := newSQLiteDriver(t, stdlibdriver.WithClock(clk))
	client := stdlibdriver.NewClient(d, goncordia.ClientConfig{})

	_, err := client.Enqueue(ctx, EmailJob{To: "scheduled@test.com"}, &core.InsertOpts{
		RunAt: clk.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	// Not visible before RunAt
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

func TestStdlibSQLite_RetryAndDiscard(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock(time.Now())
	d, _ := newSQLiteDriver(t, stdlibdriver.WithClock(clk))

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
		PollInterval: 50 * time.Millisecond,
		RetryPolicy:  core.FixedRetry{Delay: 100 * time.Millisecond},
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
		clk.Advance(200 * time.Millisecond)
		time.Sleep(50 * time.Millisecond)
	}
	wp.Stop()
}
