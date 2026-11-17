package future_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver/memory"
	"github.com/kirimatt/goncordia/v2/future"
)

type SumArgs struct {
	A int `json:"a"`
	B int `json:"b"`
}

func (SumArgs) Kind() string { return "sum" }

// Example runs a future end to end on the in-memory driver: the worker returns
// a value, the producer awaits it, and a discarded job surfaces as
// future.ErrDiscarded.
func Example() {
	ctx := context.Background()
	clk := clock.NewManual(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))

	// The Manager stores results with the driver and wakes local waiters.
	m, err := future.New(d, future.Config{Clock: clk})
	if err != nil {
		panic(err)
	}

	registry := core.NewRegistry()
	future.RegisterWorker(registry, m, func(_ context.Context, job *core.Job[SumArgs]) (int, error) {
		if job.Args.A < 0 {
			return 0, core.Discard(errors.New("negative input"))
		}
		return job.Args.A + job.Args.B, nil
	}, core.WorkerOpts{})

	// Passing the Manager as the observer wakes Await as soon as the job
	// finishes in this process; no polling interval has to elapse.
	wp := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Clock: clk, PollInterval: time.Hour, Observer: m,
	})
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = wp.Start(runCtx)
	}()
	defer func() {
		stop()
		<-done
	}()

	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})

	f, err := future.Enqueue[int](ctx, m, client, SumArgs{A: 2, B: 3}, nil)
	if err != nil {
		panic(err)
	}
	sum, err := f.Await(ctx)
	fmt.Println("sum:", sum, err)

	bad, err := future.Enqueue[int](ctx, m, client, SumArgs{A: -1}, nil)
	if err != nil {
		panic(err)
	}
	_, err = bad.Await(ctx)
	fmt.Println("discarded:", errors.Is(err, future.ErrDiscarded))

	// Output:
	// sum: 5 <nil>
	// discarded: true
}
