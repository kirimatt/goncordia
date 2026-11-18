package redisdriver_test

import (
	"context"
	"errors"
	"testing"
	"time"

	redisdriver "github.com/kirimatt/goncordia/driver/redis/v2"
	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
	"github.com/kirimatt/goncordia/v2/future"
)

type squareArgs struct{ N int }

func (squareArgs) Kind() string { return "square" }

var squareTask = future.Define[squareArgs, int]()

// TestRedis_FutureEndToEnd runs worker and producer with separate Managers, as
// in two processes, so the producer learns results by polling Redis.
func TestRedis_FutureEndToEnd(t *testing.T) {
	d, rdb := newDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	workerSide, err := future.New(d, future.Config{})
	if err != nil {
		t.Fatal(err)
	}
	producerSide, err := future.New(d, future.Config{PollInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	registry := core.NewRegistry()
	squareTask.Register(registry, workerSide, func(_ context.Context, job *core.Job[squareArgs]) (int, error) {
		if job.Args.N < 0 {
			return 0, core.Discard(errors.New("negative input"))
		}
		return job.Args.N * job.Args.N, nil
	}, core.WorkerOpts{})
	wp := redisdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues: []string{"default"}, PollInterval: 20 * time.Millisecond, Observer: workerSide,
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = wp.Start(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()

	square := future.BindTx(producerSide, redisdriver.NewClient(d, goncordia.ClientConfig{}), squareTask)

	if got, err := square.Call(ctx, squareArgs{N: 7}); err != nil || got != 49 {
		t.Fatalf("Call = %d, %v", got, err)
	}
	if _, err := square.Call(ctx, squareArgs{N: -1}); !errors.Is(err, future.ErrDiscarded) {
		t.Fatalf("discarded Call err = %v", err)
	}

	f, err := square.Enqueue(ctx, squareArgs{N: 3})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := f.Await(ctx); err != nil || got != 9 {
		t.Fatalf("Await = %d, %v", got, err)
	}
	if n, err := rdb.Exists(ctx, "goncordia:result:"+f.ID()).Result(); err != nil || n != 1 {
		t.Fatalf("result key exists = %d, %v", n, err)
	}
	if err := d.Executor().JobDelete(ctx, f.ID()); err != nil {
		t.Fatal(err)
	}
	if n, _ := rdb.Exists(ctx, "goncordia:result:"+f.ID()).Result(); n != 0 {
		t.Fatal("JobDelete left the result key behind")
	}
	if _, err := square.Of(f.ID()).Await(ctx); !errors.Is(err, future.ErrNotFound) {
		t.Fatalf("Await of a deleted job err = %v, want ErrNotFound", err)
	}

	// Redis has no transactions: EnqueueTx is rejected, Enqueue/Call work.
	if _, err := square.EnqueueTx(ctx, redisdriver.NoTx{}, squareArgs{N: 2}); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("EnqueueTx err = %v, want ErrUnsupported", err)
	}
}

// TestRedis_FutureReportsLostResult simulates eviction of a result key and
// checks that Await reports ErrResultLost, while a deliberate Forget does not.
func TestRedis_FutureReportsLostResult(t *testing.T) {
	d, rdb := newDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	m, err := future.New(d, future.Config{PollInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	registry := core.NewRegistry()
	squareTask.Register(registry, m, func(_ context.Context, job *core.Job[squareArgs]) (int, error) {
		return job.Args.N * job.Args.N, nil
	}, core.WorkerOpts{})
	wp := redisdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues: []string{"default"}, PollInterval: 20 * time.Millisecond, Observer: m,
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = wp.Start(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()
	square := future.Bind(m, redisdriver.NewClient(d, goncordia.ClientConfig{}), squareTask)

	evicted, err := square.Enqueue(ctx, squareArgs{N: 4})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := evicted.Await(ctx); err != nil || got != 16 {
		t.Fatalf("Await = %d, %v", got, err)
	}
	if err := rdb.Del(ctx, "goncordia:result:"+evicted.ID()).Err(); err != nil { // as eviction would
		t.Fatal(err)
	}
	_, err = square.Of(evicted.ID()).Await(ctx)
	if !errors.Is(err, future.ErrResultLost) || errors.Is(err, future.ErrNoResult) {
		t.Fatalf("Await after eviction err = %v, want ErrResultLost", err)
	}

	forgotten, err := square.Enqueue(ctx, squareArgs{N: 5})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := forgotten.Await(ctx); err != nil {
		t.Fatal(err)
	}
	if err := m.Forget(ctx, forgotten.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err := square.Of(forgotten.ID()).Await(ctx); !errors.Is(err, future.ErrNoResult) {
		t.Fatalf("Await after Forget err = %v, want ErrNoResult", err)
	}
	// The job document keeps decoding after the scripts rewrote it.
	if row, err := d.Executor().JobGetByID(ctx, forgotten.ID()); err != nil || row.State != driver.JobStateCompleted {
		t.Fatalf("job after Forget = %+v, %v", row, err)
	}
}

func TestRedis_MigrateRejectsAllkeysEviction(t *testing.T) {
	d, rdb := newDriver(t)
	ctx := context.Background()
	t.Cleanup(func() { _ = rdb.ConfigSet(context.Background(), "maxmemory-policy", "noeviction").Err() })

	for _, policy := range []string{"volatile-lru", "noeviction"} {
		if err := rdb.ConfigSet(ctx, "maxmemory-policy", policy).Err(); err != nil {
			t.Fatal(err)
		}
		if err := d.Migrate(ctx); err != nil {
			t.Fatalf("Migrate with %s: %v", policy, err)
		}
	}
	if err := rdb.ConfigSet(ctx, "maxmemory-policy", "allkeys-lru").Err(); err != nil {
		t.Fatal(err)
	}
	if err := d.Migrate(ctx); !errors.Is(err, redisdriver.ErrEvictionPolicy) {
		t.Fatalf("Migrate with allkeys-lru err = %v, want ErrEvictionPolicy", err)
	}
	allowed := redisdriver.New(rdb, redisdriver.WithAllowEviction())
	if err := allowed.Migrate(ctx); err != nil {
		t.Fatalf("Migrate with WithAllowEviction: %v", err)
	}
}
