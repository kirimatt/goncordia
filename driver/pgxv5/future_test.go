package pgxv5_test

import (
	"context"
	"errors"
	"testing"
	"time"

	pgxdriver "github.com/kirimatt/goncordia/driver/pgxv5/v2"
	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/future"
)

type doubleArgs struct{ N int }

func (doubleArgs) Kind() string { return "double" }

// TestPgxv5_FutureAcrossManagers runs the worker and the producer with separate
// Managers, as in two processes: the producer learns the result by polling.
func TestPgxv5_FutureAcrossManagers(t *testing.T) {
	skipIfNoDocker(t)
	pool, cleanup := newTestPool(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	d := pgxdriver.New(pool)
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	workerSide, err := future.New(d, future.Config{})
	if err != nil {
		t.Fatal(err)
	}
	producerSide, err := future.New(d, future.Config{PollInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	registry := core.NewRegistry()
	future.RegisterWorker(registry, workerSide, func(_ context.Context, job *core.Job[doubleArgs]) (int, error) {
		if job.Args.N < 0 {
			return 0, core.Discard(errors.New("negative input"))
		}
		return job.Args.N * 2, nil
	}, core.WorkerOpts{})
	wp := pgxdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
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

	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	ok, err := future.Enqueue[int](ctx, producerSide, client, doubleArgs{N: 21}, nil)
	if err != nil {
		t.Fatal(err)
	}
	bad, err := future.Enqueue[int](ctx, producerSide, client, doubleArgs{N: -1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := ok.Await(ctx); err != nil || got != 42 {
		t.Fatalf("Await = %d, %v", got, err)
	}
	if _, err := bad.Await(ctx); !errors.Is(err, future.ErrDiscarded) {
		t.Fatalf("discarded Await err = %v", err)
	}
	if err := d.Executor().JobDelete(ctx, ok.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err := future.Of[int](producerSide, ok.ID()).Await(ctx); err == nil {
		t.Fatal("Await of a deleted job succeeded")
	}
}
