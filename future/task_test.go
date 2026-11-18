package future

import (
	"context"
	"errors"
	"testing"

	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
)

var sumTask = Define[sumArgs, int]()

func sumHandler(_ context.Context, job *core.Job[sumArgs]) (int, error) {
	if job.Args.A < 0 {
		return 0, core.Discard(errors.New("negative"))
	}
	return job.Args.A + job.Args.B, nil
}

func TestBoundTaskCallReturnsHandlerValue(t *testing.T) {
	fx := newFixture(t)
	registry := core.NewRegistry()
	sumTask.Register(registry, fx.m, sumHandler, core.WorkerOpts{})
	fx.startPool(t, registry, fx.m)
	sum := Bind(fx.m, fx.client, sumTask)
	ctx := context.Background()

	got, err := sum.Call(ctx, sumArgs{A: 2, B: 3})
	if err != nil || got != 5 {
		t.Fatalf("Call = %d, %v", got, err)
	}

	f, err := sum.Enqueue(ctx, sumArgs{A: 10, B: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := f.Await(ctx); err != nil || got != 11 {
		t.Fatalf("Await = %d, %v", got, err)
	}
	if got, err := sum.Of(f.ID()).Await(ctx); err != nil || got != 11 {
		t.Fatalf("Of = %d, %v", got, err)
	}
	if got, err := sumTask.Of(fx.m, f.ID()).Await(ctx); err != nil || got != 11 {
		t.Fatalf("Task.Of = %d, %v", got, err)
	}

	if _, err := sum.Call(ctx, sumArgs{A: -1}); !errors.Is(err, ErrDiscarded) {
		t.Fatalf("discarded Call err = %v", err)
	}
}

func TestBoundTaskWithAppliesInsertOpts(t *testing.T) {
	fx := newFixture(t)
	sum := Bind(fx.m, fx.client, sumTask)
	ctx := context.Background()

	critical := sum.With(&core.InsertOpts{Queue: "critical", Priority: 7})
	f, err := critical.Enqueue(ctx, sumArgs{A: 1})
	if err != nil {
		t.Fatal(err)
	}
	row, err := fx.d.Executor().JobGetByID(ctx, f.ID())
	if err != nil {
		t.Fatal(err)
	}
	if row.Queue != "critical" || row.Priority != 7 {
		t.Fatalf("queue=%q priority=%d", row.Queue, row.Priority)
	}

	plain, err := sum.Enqueue(ctx, sumArgs{A: 1})
	if err != nil {
		t.Fatal(err)
	}
	if row, _ := fx.d.Executor().JobGetByID(ctx, plain.ID()); row.Queue != "default" {
		t.Fatalf("With changed the original Func: queue=%q", row.Queue)
	}
}

func TestBoundTaskCallStopsWithContext(t *testing.T) {
	fx := newFixture(t) // no worker pool: the job stays pending
	sum := Bind(fx.m, fx.client, sumTask)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := sum.Call(ctx, sumArgs{A: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestBindTxReportsDriverWithoutTransactions(t *testing.T) {
	fx := newFixture(t)
	sum := BindTx(fx.m, fx.client, sumTask).With(&core.InsertOpts{Queue: "critical"})
	_, err := sum.EnqueueTx(context.Background(), struct{}{}, sumArgs{A: 1})
	if !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("err = %v, want ErrUnsupported from the memory driver", err)
	}
	if _, err := sum.Enqueue(context.Background(), sumArgs{A: 1}); err != nil {
		t.Fatalf("non-transactional Enqueue through TxFunc: %v", err)
	}
}

func TestBindRequiresManagerAndClient(t *testing.T) {
	fx := newFixture(t)
	defer func() {
		if recover() == nil {
			t.Fatal("Bind accepted a nil Manager")
		}
	}()
	Bind(nil, fx.client, sumTask)
}

func jobCount(t *testing.T, fx *fixture, state driver.JobState) int {
	t.Helper()
	rows, err := fx.d.Executor().(driver.AdminExecutor).JobList(context.Background(), driver.JobListParams{State: state, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	return len(rows)
}

func TestCallDeletesItsJobAfterSuccess(t *testing.T) {
	fx := newFixture(t)
	registry := core.NewRegistry()
	sumTask.Register(registry, fx.m, sumHandler, core.WorkerOpts{})
	fx.startPool(t, registry, fx.m)
	sum := Bind(fx.m, fx.client, sumTask)
	ctx := context.Background()

	if got, err := sum.Call(ctx, sumArgs{A: 1, B: 1}); err != nil || got != 2 {
		t.Fatalf("Call = %d, %v", got, err)
	}
	if n := jobCount(t, fx, driver.JobStateCompleted); n != 0 {
		t.Fatalf("completed jobs after Call = %d, want 0", n)
	}
	// A unique job may be awaited by other callers, so it stays.
	unique := sum.With(&core.InsertOpts{UniqueOpts: &core.UniqueOpts{ByArgs: true}})
	if _, err := unique.Call(ctx, sumArgs{A: 2}); err != nil {
		t.Fatal(err)
	}
	if n := jobCount(t, fx, driver.JobStateCompleted); n != 1 {
		t.Fatalf("completed jobs after unique Call = %d, want 1", n)
	}
	// A failed job stays for dead-letter inspection.
	if _, err := sum.Call(ctx, sumArgs{A: -1}); !errors.Is(err, ErrDiscarded) {
		t.Fatal(err)
	}
	if n := jobCount(t, fx, driver.JobStateDiscarded); n != 1 {
		t.Fatalf("discarded jobs = %d, want 1", n)
	}
	// Enqueue exposes the ID, so its job is kept.
	f, err := sum.Enqueue(ctx, sumArgs{A: 3})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Await(ctx); err != nil {
		t.Fatal(err)
	}
	if n := jobCount(t, fx, driver.JobStateCompleted); n != 2 {
		t.Fatalf("completed jobs after Enqueue+Await = %d, want 2", n)
	}
}

func TestCallKeepsJobsWhenConfigured(t *testing.T) {
	fx := newFixture(t)
	m, err := New(fx.d, Config{Clock: fx.clk, KeepCalledJobs: true})
	if err != nil {
		t.Fatal(err)
	}
	registry := core.NewRegistry()
	sumTask.Register(registry, m, sumHandler, core.WorkerOpts{})
	fx.startPool(t, registry, m)
	if _, err := Bind(m, fx.client, sumTask).Call(context.Background(), sumArgs{A: 1}); err != nil {
		t.Fatal(err)
	}
	if n := jobCount(t, fx, driver.JobStateCompleted); n != 1 {
		t.Fatalf("completed jobs = %d, want 1", n)
	}
}
