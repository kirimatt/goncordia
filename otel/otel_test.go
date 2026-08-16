package otelgoncordia_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.opentelemetry.io/otel/metric/noop"
	nooptrace "go.opentelemetry.io/otel/trace/noop"

	"github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver/memory"
	otelgoncordia "github.com/kirimatt/goncordia/otel"
)

type emailJob struct{ To string }

func (emailJob) Kind() string { return "email" }

func newPool(t *testing.T, worker core.Worker[emailJob], opts core.WorkerOpts, mwOpts ...otelgoncordia.Option) (*goncordia.WorkerPool[memory.NoTx], *memory.Driver) {
	t.Helper()
	d := memory.New()
	registry := core.NewRegistry()
	core.RegisterWorker(registry, worker, opts)

	wp := goncordia.NewWorkerPool[memory.NoTx](d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  5,
		PollInterval: 20 * time.Millisecond,
		Middleware:   []goncordia.JobMiddleware{otelgoncordia.NewMiddleware(mwOpts...)},
	})
	return wp, d
}

func TestMiddleware_SuccessfulJob(t *testing.T) {
	var ran atomic.Bool
	wp, d := newPool(t,
		core.WorkerFunc[emailJob](func(_ context.Context, _ *core.Job[emailJob]) error {
			ran.Store(true)
			return nil
		}),
		core.WorkerOpts{},
		otelgoncordia.WithTracerProvider(nooptrace.NewTracerProvider()),
		otelgoncordia.WithMeterProvider(noop.NewMeterProvider()),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go wp.Start(ctx) //nolint:errcheck

	client := goncordia.NewClient[memory.NoTx](d, goncordia.ClientConfig{})
	if _, err := client.Enqueue(ctx, emailJob{To: "x@test.com"}, nil); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for !ran.Load() {
		if time.Now().After(deadline) {
			t.Fatal("job never ran")
		}
		time.Sleep(10 * time.Millisecond)
	}
	wp.Stop()
}

func TestInstrumentation_ClientAndWorkerObservers(t *testing.T) {
	instrumentation := otelgoncordia.NewInstrumentation(
		otelgoncordia.WithTracerProvider(nooptrace.NewTracerProvider()),
		otelgoncordia.WithMeterProvider(noop.NewMeterProvider()),
	)
	d := memory.New()
	registry := core.NewRegistry()
	ran := make(chan struct{}, 1)
	core.RegisterWorker(registry, core.WorkerFunc[emailJob](func(_ context.Context, _ *core.Job[emailJob]) error {
		ran <- struct{}{}
		return nil
	}), core.WorkerOpts{})
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		PollInterval: 5 * time.Millisecond,
		Observer:     instrumentation,
		Middleware:   []goncordia.JobMiddleware{otelgoncordia.NewMiddleware()},
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pool.Start(ctx) //nolint:errcheck
	client := goncordia.NewClient(d, goncordia.ClientConfig{Observer: instrumentation})
	if _, err := client.Enqueue(ctx, emailJob{To: "observed@test.com"}, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case <-ran:
	case <-time.After(time.Second):
		t.Fatal("instrumented job did not run")
	}
	pool.Stop()
}

func TestMiddleware_FailingJob(t *testing.T) {
	var attempts atomic.Int64
	wp, d := newPool(t,
		core.WorkerFunc[emailJob](func(_ context.Context, _ *core.Job[emailJob]) error {
			attempts.Add(1)
			return errors.New("oops")
		}),
		core.WorkerOpts{MaxRetry: 1},
		otelgoncordia.WithTracerProvider(nooptrace.NewTracerProvider()),
		otelgoncordia.WithMeterProvider(noop.NewMeterProvider()),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go wp.Start(ctx) //nolint:errcheck

	client := goncordia.NewClient[memory.NoTx](d, goncordia.ClientConfig{})
	if _, err := client.Enqueue(ctx, emailJob{To: "y@test.com"}, nil); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for attempts.Load() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("job never ran")
		}
		time.Sleep(10 * time.Millisecond)
	}
	wp.Stop()
}

func TestMiddleware_PanicJob(t *testing.T) {
	var ran atomic.Bool
	wp, d := newPool(t,
		core.WorkerFunc[emailJob](func(_ context.Context, _ *core.Job[emailJob]) error {
			ran.Store(true)
			panic("something went very wrong")
		}),
		core.WorkerOpts{MaxRetry: 1},
		otelgoncordia.WithTracerProvider(nooptrace.NewTracerProvider()),
		otelgoncordia.WithMeterProvider(noop.NewMeterProvider()),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go wp.Start(ctx) //nolint:errcheck

	client := goncordia.NewClient[memory.NoTx](d, goncordia.ClientConfig{})
	if _, err := client.Enqueue(ctx, emailJob{To: "z@test.com"}, nil); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for !ran.Load() {
		if time.Now().After(deadline) {
			t.Fatal("job never ran")
		}
		time.Sleep(10 * time.Millisecond)
	}
	// Panic was recovered — pool should still be alive.
	wp.Stop()
}
