// Package bench provides throughput benchmarks for goncordia drivers.
//
// Run all benchmarks:
//
//	go test ./bench/... -bench=. -benchmem -benchtime=5s
//
// Memory driver shows framework overhead without storage I/O.
// SQLite driver shows realistic single-process performance.
package bench_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	stdlibdriver "github.com/kirimatt/goncordia/driver/stdlib/v2"
	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

type benchJob struct{ N int }

func (benchJob) Kind() string { return "bench" }

// ---- driver factories ----

func newMemoryDriver(_ testing.TB) (driver.Driver[memory.NoTx], func()) {
	d := memory.New()
	return d, func() {}
}

func newSQLiteDriver(b testing.TB) (driver.Driver[*sql.Tx], func()) {
	b.Helper()
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=memory&cache=shared&_journal=WAL", b.Name()))
	if err != nil {
		b.Fatalf("open sqlite: %v", err)
	}
	db.SetMaxOpenConns(1) // SQLite: single writer
	d := stdlibdriver.New(db, stdlibdriver.SQLite)
	if err := d.Migrate(context.Background()); err != nil {
		b.Fatalf("migrate: %v", err)
	}
	return d, func() { db.Close() }
}

// ---- Enqueue benchmarks ----

func benchmarkEnqueue[TTx any](b *testing.B, d driver.Driver[TTx]) {
	b.Helper()
	ctx := context.Background()
	client := goncordia.NewClient[TTx](d, goncordia.ClientConfig{})

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		if _, err := client.Enqueue(ctx, benchJob{N: i}, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEnqueue_Memory(b *testing.B) {
	d, cleanup := newMemoryDriver(b)
	defer cleanup()
	benchmarkEnqueue(b, d)
}

func BenchmarkEnqueue_SQLite(b *testing.B) {
	d, cleanup := newSQLiteDriver(b)
	defer cleanup()
	benchmarkEnqueue(b, d)
}

// ---- EnqueueMany benchmarks (batch of 100) ----

func benchmarkEnqueueBatch[TTx any](b *testing.B, d driver.Driver[TTx], batchSize int) {
	b.Helper()
	ctx := context.Background()
	client := goncordia.NewClient[TTx](d, goncordia.ClientConfig{})
	args := make([]core.JobArgs, batchSize)
	for i := range args {
		args[i] = benchJob{N: i}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := client.EnqueueMany(ctx, args, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEnqueueBatch100_Memory(b *testing.B) {
	d, cleanup := newMemoryDriver(b)
	defer cleanup()
	benchmarkEnqueueBatch(b, d, 100)
}

func BenchmarkEnqueueBatch100_SQLite(b *testing.B) {
	d, cleanup := newSQLiteDriver(b)
	defer cleanup()
	benchmarkEnqueueBatch(b, d, 100)
}

// ---- FetchAndComplete: the hot worker loop path ----
//
// Pre-populates b.N jobs, then measures fetch-one + mark-complete per iteration.

func benchmarkFetchAndComplete[TTx any](b *testing.B, d driver.Driver[TTx]) {
	b.Helper()
	ctx := context.Background()
	exec := d.Executor()

	params := make([]driver.JobInsertParams, b.N)
	for i := range params {
		params[i] = driver.JobInsertParams{
			Queue:    "default",
			Kind:     "bench",
			Args:     []byte(`{"n":0}`),
			Priority: 0,
		}
	}
	if _, err := exec.JobInsertMany(ctx, params); err != nil {
		b.Fatalf("pre-populate: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rows, err := exec.JobFetchBatch(ctx, driver.FetchParams{Queue: "default", Limit: 1})
		if err != nil {
			b.Fatal(err)
		}
		if len(rows) == 0 {
			b.Fatal("queue exhausted before b.N iterations")
		}
		if err := exec.JobSetStateIfRunning(ctx, driver.JobSetStateParams{
			ID:    rows[0].ID,
			State: driver.JobStateCompleted,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFetchAndComplete_Memory(b *testing.B) {
	d, cleanup := newMemoryDriver(b)
	defer cleanup()
	benchmarkFetchAndComplete(b, d)
}

func BenchmarkFetchAndComplete_SQLite(b *testing.B) {
	d, cleanup := newSQLiteDriver(b)
	defer cleanup()
	benchmarkFetchAndComplete(b, d)
}

// ---- End-to-end: full WorkerPool with N=fixed workload ----
//
// These benchmarks use a fixed workload (not b.N jobs) so that the pool
// startup cost is amortised. b.N controls how many times the full workload runs.

const e2eWorkload = 1000

func benchmarkEndToEndN[TTx any](b *testing.B, d driver.Driver[TTx], concurrency, workload int) {
	b.Helper()
	ctx := context.Background()

	registry := core.NewRegistry()
	var processed atomic.Int64
	core.RegisterWorker(registry, core.WorkerFunc[benchJob](func(_ context.Context, _ *core.Job[benchJob]) error {
		processed.Add(1)
		return nil
	}), core.WorkerOpts{})

	client := goncordia.NewClient[TTx](d, goncordia.ClientConfig{})
	wp := goncordia.NewWorkerPool[TTx](d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  concurrency,
		PollInterval: 5 * time.Millisecond,
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go wp.Start(runCtx) //nolint:errcheck

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		processed.Store(0)
		args := make([]core.JobArgs, workload)
		for i := range args {
			args[i] = benchJob{N: i}
		}
		if _, err := client.EnqueueMany(ctx, args, nil); err != nil {
			b.Fatal(err)
		}
		deadline := time.Now().Add(60 * time.Second)
		for processed.Load() < int64(workload) {
			if time.Now().After(deadline) {
				b.Fatalf("timeout: only %d/%d processed", processed.Load(), workload)
			}
			time.Sleep(time.Millisecond)
		}
	}

	_ = wp.Shutdown(context.Background())
	b.ReportMetric(float64(workload*b.N)/b.Elapsed().Seconds(), "jobs/s")
}

func benchmarkEndToEnd[TTx any](b *testing.B, d driver.Driver[TTx], concurrency int) {
	b.Helper()
	ctx := context.Background()

	registry := core.NewRegistry()
	var processed atomic.Int64
	core.RegisterWorker(registry, core.WorkerFunc[benchJob](func(_ context.Context, _ *core.Job[benchJob]) error {
		processed.Add(1)
		return nil
	}), core.WorkerOpts{})

	client := goncordia.NewClient[TTx](d, goncordia.ClientConfig{})
	wp := goncordia.NewWorkerPool[TTx](d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  concurrency,
		PollInterval: 5 * time.Millisecond,
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go wp.Start(runCtx) //nolint:errcheck

	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		processed.Store(0)
		args := make([]core.JobArgs, e2eWorkload)
		for i := range args {
			args[i] = benchJob{N: i}
		}
		if _, err := client.EnqueueMany(ctx, args, nil); err != nil {
			b.Fatal(err)
		}
		deadline := time.Now().Add(30 * time.Second)
		for processed.Load() < e2eWorkload {
			if time.Now().After(deadline) {
				b.Fatalf("timeout: only %d/%d processed", processed.Load(), e2eWorkload)
			}
			time.Sleep(time.Millisecond)
		}
	}

	_ = wp.Shutdown(context.Background())
	b.ReportMetric(float64(e2eWorkload*b.N)/b.Elapsed().Seconds(), "jobs/s")
}

func BenchmarkEndToEnd_Memory_c1(b *testing.B) {
	d, cleanup := newMemoryDriver(b)
	defer cleanup()
	benchmarkEndToEnd(b, d, 1)
}

func BenchmarkEndToEnd_Memory_c10(b *testing.B) {
	d, cleanup := newMemoryDriver(b)
	defer cleanup()
	benchmarkEndToEnd(b, d, 10)
}

func BenchmarkEndToEnd_SQLite_c1(b *testing.B) {
	d, cleanup := newSQLiteDriver(b)
	defer cleanup()
	benchmarkEndToEnd(b, d, 1)
}

func BenchmarkEndToEnd_SQLite_c4(b *testing.B) {
	d, cleanup := newSQLiteDriver(b)
	defer cleanup()
	// SQLite single-writer: concurrency > 1 helps with the processing side
	// but write throughput is still serialised by MaxOpenConns(1).
	benchmarkEndToEnd(b, d, 4)
}
