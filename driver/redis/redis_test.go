package redisdriver_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"

	"github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver/drivertest"
	redisdriver "github.com/kirimatt/goncordia/driver/redis"
)

var redisAddr string

func TestMain(m *testing.M) {
	ctx := context.Background()
	code := 0
	defer func() { os.Exit(code) }()

	ctr, err := tcredis.Run(ctx, "redis:7")
	if err != nil {
		fmt.Fprintf(os.Stderr, "start redis container: %v\n", err)
		code = 1
		return
	}
	defer ctr.Terminate(ctx) //nolint:errcheck

	addr, err := ctr.Endpoint(ctx, "")
	if err != nil {
		fmt.Fprintf(os.Stderr, "redis endpoint: %v\n", err)
		code = 1
		return
	}
	redisAddr = addr

	code = m.Run()
}

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func (EmailJob) Kind() string { return "email" }

func newDriver(t *testing.T, opts ...redisdriver.Option) (*redisdriver.Driver, *redis.Client) {
	t.Helper()
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	t.Cleanup(func() { rdb.Close() })

	d := redisdriver.New(rdb, opts...)
	if err := d.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	// Flush all keys so each test starts clean.
	if err := rdb.FlushAll(ctx).Err(); err != nil {
		t.Fatalf("flushall: %v", err)
	}
	return d, rdb
}

func TestRedis_EnqueueAndProcess(t *testing.T) {
	d, _ := newDriver(t)
	drivertest.Run(t, d.Executor())
	ctx := context.Background()

	var processed atomic.Int64
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		processed.Add(1)
		return nil
	}), core.WorkerOpts{Queue: "default", MaxRetry: 3})

	client := redisdriver.NewClient(d, goncordia.ClientConfig{})
	for i := range 5 {
		if _, err := client.Enqueue(ctx, EmailJob{To: fmt.Sprintf("u%d@b.com", i), Subject: "hi"}, nil); err != nil {
			t.Fatal(err)
		}
	}

	wp := redisdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  5,
		PollInterval: 50 * time.Millisecond,
	})
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go wp.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(15 * time.Second)
	for processed.Load() < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: processed %d/5", processed.Load())
		}
		time.Sleep(30 * time.Millisecond)
	}
	wp.Stop()
}

// TestRedis_EnqueueTxUnsupported verifies that EnqueueTx is correctly
// rejected with a descriptive error. Redis does not provide rollback semantics,
// so transactional inserts are intentionally unsupported. Use Enqueue instead.
func TestRedis_EnqueueTxUnsupported(t *testing.T) {
	d, _ := newDriver(t)
	ctx := context.Background()
	client := redisdriver.NewClient(d, goncordia.ClientConfig{})

	_, err := client.EnqueueTx(ctx, redisdriver.NoTx{}, EmailJob{To: "tx@test.com"}, nil)
	if err == nil {
		t.Fatal("expected error for unsupported EnqueueTx on Redis driver")
	}
}

func TestRedis_UniqueJobs(t *testing.T) {
	d, _ := newDriver(t)
	ctx := context.Background()
	client := redisdriver.NewClient(d, goncordia.ClientConfig{})
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

func TestRedis_ConcurrentUniqueInsertIsAtomic(t *testing.T) {
	d, _ := newDriver(t)
	ctx := context.Background()
	client := redisdriver.NewClient(d, goncordia.ClientConfig{})
	opts := &core.InsertOpts{UniqueOpts: &core.UniqueOpts{ByArgs: true}}

	const inserts = 32
	results := make(chan bool, inserts)
	errs := make(chan error, inserts)
	var wg sync.WaitGroup
	for range inserts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := client.Enqueue(ctx, EmailJob{To: "atomic@test.com"}, opts)
			if err != nil {
				errs <- err
				return
			}
			results <- result.UniqueSkip
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent insert: %v", err)
	}

	inserted := 0
	skipped := 0
	for skip := range results {
		if skip {
			skipped++
		} else {
			inserted++
		}
	}
	if inserted != 1 || skipped != inserts-1 {
		t.Fatalf("inserted=%d skipped=%d, want 1/%d", inserted, skipped, inserts-1)
	}
}

func TestRedis_ScheduledJob(t *testing.T) {
	clk := clock.NewMock(time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))
	d, _ := newDriver(t, redisdriver.WithClock(clk))
	ctx := context.Background()
	client := redisdriver.NewClient(d, goncordia.ClientConfig{})

	_, err := client.Enqueue(ctx, EmailJob{To: "scheduled@test.com"}, &core.InsertOpts{
		RunAt: clk.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	rows, _ := d.Executor().JobFetchBatch(ctx, redisdriver.FetchParams("default", 10))
	if len(rows) != 0 {
		t.Fatalf("expected 0 before RunAt, got %d", len(rows))
	}

	clk.Advance(2 * time.Hour)
	rows, _ = d.Executor().JobFetchBatch(ctx, redisdriver.FetchParams("default", 10))
	if len(rows) != 1 {
		t.Fatalf("expected 1 after RunAt, got %d", len(rows))
	}
}

func TestRedis_RetryAndDiscard(t *testing.T) {
	clk := clock.NewMock(time.Now())
	d, _ := newDriver(t, redisdriver.WithClock(clk))
	ctx := context.Background()

	var attempts atomic.Int64
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		attempts.Add(1)
		return errors.New("always fails")
	}), core.WorkerOpts{Queue: "default", MaxRetry: 2})

	client := redisdriver.NewClient(d, goncordia.ClientConfig{})
	if _, err := client.Enqueue(ctx, EmailJob{To: "fail@test.com"}, nil); err != nil {
		t.Fatal(err)
	}

	wp := redisdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  1,
		PollInterval: 50 * time.Millisecond,
		RetryPolicy:  core.FixedRetry{Delay: 50 * time.Millisecond},
		Clock:        clk,
	})
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go wp.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(20 * time.Second)
	for attempts.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d attempts", attempts.Load())
		}
		clk.Advance(100 * time.Millisecond)
		time.Sleep(60 * time.Millisecond)
	}
	wp.Stop()
}

func TestRedis_QueuePauseResume(t *testing.T) {
	d, _ := newDriver(t)
	ctx := context.Background()
	client := redisdriver.NewClient(d, goncordia.ClientConfig{})

	for range 3 {
		if _, err := client.Enqueue(ctx, EmailJob{To: "pause@test.com"}, nil); err != nil {
			t.Fatal(err)
		}
	}

	exec := d.Executor()
	if err := exec.QueuePause(ctx, "default"); err != nil {
		t.Fatal(err)
	}

	rows, _ := exec.JobFetchBatch(ctx, redisdriver.FetchParams("default", 10))
	if len(rows) != 0 {
		t.Errorf("expected 0 jobs from paused queue, got %d", len(rows))
	}

	if err := exec.QueueResume(ctx, "default"); err != nil {
		t.Fatal(err)
	}

	rows, _ = exec.JobFetchBatch(ctx, redisdriver.FetchParams("default", 10))
	if len(rows) != 3 {
		t.Errorf("expected 3 jobs after resume, got %d", len(rows))
	}
}
