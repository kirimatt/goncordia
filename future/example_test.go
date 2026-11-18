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

// --- Contract: shared by producers and workers. ---

type SumArgs struct {
	A int `json:"a"`
	B int `json:"b"`
}

func (SumArgs) Kind() string { return "sum" }

// Sum declares that SumArgs jobs produce an int.
var Sum = future.Define[SumArgs, int]()

// --- Worker code. ---

func sumHandler(_ context.Context, job *core.Job[SumArgs]) (int, error) {
	if job.Args.A < 0 {
		return 0, core.Discard(errors.New("negative input"))
	}
	return job.Args.A + job.Args.B, nil
}

// --- Caller code: knows only the bound task, not the queue. ---

type Billing struct {
	sum *future.Func[SumArgs, int]
}

func (b *Billing) Total(ctx context.Context, a, c int) (int, error) {
	return b.sum.Call(ctx, SumArgs{A: a, B: c})
}

// Example wires the in-memory driver in main and calls the task from ordinary
// application code.
func Example() {
	ctx := context.Background()
	clk := clock.NewManual(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))

	m, err := future.New(d, future.Config{Clock: clk})
	if err != nil {
		panic(err)
	}
	registry := core.NewRegistry()
	Sum.Register(registry, m, sumHandler, core.WorkerOpts{})

	// The Manager as observer wakes Await the moment a job finishes.
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
	billing := &Billing{sum: future.Bind(m, client, Sum)}

	total, err := billing.Total(ctx, 2, 3)
	fmt.Println("total:", total, err)

	_, err = billing.Total(ctx, -1, 0)
	fmt.Println("discarded:", errors.Is(err, future.ErrDiscarded))

	// Output:
	// total: 5 <nil>
	// discarded: true
}
