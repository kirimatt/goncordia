package clickhousedriver_test

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"

	"github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/core"
	clickhousedriver "github.com/kirimatt/goncordia/driver/clickhouse"
	"github.com/kirimatt/goncordia/driver/drivertest"
)

func skipIfNoDocker(t *testing.T) {
	t.Helper()
	if os.Getenv("SKIP_INTEGRATION") != "" {
		t.Skip("SKIP_INTEGRATION set")
	}
}

func newTestConn(t *testing.T) (clickhouse.Conn, func()) {
	t.Helper()
	ctx := context.Background()

	ctr, err := tcclickhouse.Run(ctx, "clickhouse/clickhouse-server:24.3-alpine")
	if err != nil {
		t.Fatalf("start clickhouse container: %v", err)
	}

	dsn, err := ctr.ConnectionString(ctx)
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		t.Fatalf("get connection string: %v", err)
	}

	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		t.Fatalf("parse dsn: %v", err)
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		t.Fatalf("open clickhouse: %v", err)
	}
	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		ctr.Terminate(ctx) //nolint:errcheck
		t.Fatalf("ping clickhouse: %v", err)
	}

	return conn, func() {
		conn.Close()
		ctr.Terminate(ctx) //nolint:errcheck
	}
}

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func (EmailJob) Kind() string { return "email" }

// --- tests ---

func TestClickHouse_EnqueueAndProcess(t *testing.T) {
	skipIfNoDocker(t)
	conn, cleanup := newTestConn(t)
	defer cleanup()

	ctx := context.Background()
	d := clickhousedriver.New(conn)
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	drivertest.Run(t, d.Executor())

	var processed atomic.Int64
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		processed.Add(1)
		return nil
	}), core.WorkerOpts{Queue: "default", MaxRetry: 3})

	client := clickhousedriver.NewClient(d, goncordia.ClientConfig{})
	for i := 0; i < 5; i++ {
		if _, err := client.Enqueue(ctx, EmailJob{To: "a@b.com", Subject: "hi"}, nil); err != nil {
			t.Fatal(err)
		}
	}

	pool := clickhousedriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  5,
		PollInterval: 200 * time.Millisecond,
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go pool.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(20 * time.Second)
	for processed.Load() < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: processed %d/5", processed.Load())
		}
		time.Sleep(100 * time.Millisecond)
	}
	pool.Stop()
}

func TestClickHouse_UniqueJobs(t *testing.T) {
	skipIfNoDocker(t)
	conn, cleanup := newTestConn(t)
	defer cleanup()

	ctx := context.Background()
	d := clickhousedriver.New(conn)
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	client := clickhousedriver.NewClient(d, goncordia.ClientConfig{})

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

func TestClickHouse_RetryAndDiscard(t *testing.T) {
	skipIfNoDocker(t)
	conn, cleanup := newTestConn(t)
	defer cleanup()

	ctx := context.Background()
	d := clickhousedriver.New(conn)
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	var attempts atomic.Int64
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		attempts.Add(1)
		return errors.New("always fails")
	}), core.WorkerOpts{Queue: "default", MaxRetry: 2})

	client := clickhousedriver.NewClient(d, goncordia.ClientConfig{})
	if _, err := client.Enqueue(ctx, EmailJob{To: "fail@test.com"}, nil); err != nil {
		t.Fatal(err)
	}

	pool := clickhousedriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  1,
		PollInterval: 100 * time.Millisecond,
		RetryPolicy:  core.FixedRetry{Delay: 200 * time.Millisecond},
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go pool.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(20 * time.Second)
	for attempts.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d attempts", attempts.Load())
		}
		time.Sleep(100 * time.Millisecond)
	}
	pool.Stop()
	if got := attempts.Load(); got < 2 {
		t.Errorf("expected >= 2 attempts, got %d", got)
	}
}
