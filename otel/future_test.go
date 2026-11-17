package otelgoncordia_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	otelgoncordia "github.com/kirimatt/goncordia/otel/v2"
	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
	"github.com/kirimatt/goncordia/v2/driver/memory"
	"github.com/kirimatt/goncordia/v2/future"
)

type squareArgs struct{ N int }

func (squareArgs) Kind() string { return "square" }

type telemetry struct {
	spans  *tracetest.SpanRecorder
	reader *sdkmetric.ManualReader
	opts   []otelgoncordia.Option
}

func newTelemetry(clk clock.Clock) *telemetry {
	spans := tracetest.NewSpanRecorder()
	reader := sdkmetric.NewManualReader()
	return &telemetry{
		spans:  spans,
		reader: reader,
		opts: []otelgoncordia.Option{
			otelgoncordia.WithTracerProvider(sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(spans))),
			otelgoncordia.WithMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))),
			otelgoncordia.WithClock(clk),
		},
	}
}

func (tm *telemetry) span(t *testing.T, name string) sdktrace.ReadOnlySpan {
	t.Helper()
	for _, span := range tm.spans.Ended() {
		if span.Name() == name {
			return span
		}
	}
	t.Fatalf("no ended span %q", name)
	return nil
}

func (tm *telemetry) metric(t *testing.T, name string) metricdata.Aggregation {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := tm.reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == name {
				return m.Data
			}
		}
	}
	t.Fatalf("no metric %q", name)
	return nil
}

func attrValue(attrs []attribute.KeyValue, key string) (attribute.Value, bool) {
	for _, kv := range attrs {
		if string(kv.Key) == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

func TestFutureInstrumentationTracesAndMeasuresCompletedJob(t *testing.T) {
	clk := clock.NewManual(time.Time{})
	tm := newTelemetry(clk)
	d := memory.New(memory.WithClock(clk))
	m, err := future.New(d, future.Config{Clock: clk, Observer: otelgoncordia.NewFutureInstrumentation(tm.opts...)})
	if err != nil {
		t.Fatal(err)
	}
	registry := core.NewRegistry()
	future.RegisterWorker(registry, m, func(_ context.Context, job *core.Job[squareArgs]) (int, error) {
		return job.Args.N * job.Args.N, nil
	}, core.WorkerOpts{})

	ctx, cancel := context.WithCancel(t.Context())
	wp := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Clock: clk, PollInterval: time.Hour,
		Observer:   goncordia.WorkerObservers(otelgoncordia.NewInstrumentation(tm.opts...), m),
		Middleware: []goncordia.JobMiddleware{otelgoncordia.NewMiddleware(tm.opts...)},
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

	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
	f, err := future.Enqueue[int](t.Context(), m, client, squareArgs{N: 7}, nil)
	if err != nil {
		t.Fatal(err)
	}
	got, err := f.Await(t.Context())
	if err != nil || got != 49 {
		t.Fatalf("Await = %d, %v", got, err)
	}

	await := tm.span(t, "goncordia.future.await")
	if await.Status().Code != codes.Ok {
		t.Fatalf("await status = %+v", await.Status())
	}
	for key, want := range map[string]string{
		"goncordia.job.id": f.ID(), "goncordia.job.kind": "square",
		"goncordia.future.outcome": "completed", "goncordia.job.state": "completed",
	} {
		if v, ok := attrValue(await.Attributes(), key); !ok || v.AsString() != want {
			t.Errorf("await attr %s = %v, want %s", key, v.String(), want)
		}
	}
	if v, _ := attrValue(await.Attributes(), "goncordia.future.result.bytes"); v.AsInt64() != 2 {
		t.Errorf("result bytes = %d", v.AsInt64())
	}

	store := tm.span(t, "goncordia.future.store")
	process := tm.span(t, "goncordia.process")
	if store.Parent().SpanID() != process.SpanContext().SpanID() {
		t.Error("store span is not a child of the job process span")
	}

	hist, ok := tm.metric(t, "goncordia.future.await.duration").(metricdata.Histogram[float64])
	if !ok || len(hist.DataPoints) != 1 || hist.DataPoints[0].Count != 1 {
		t.Fatalf("await duration = %+v", hist)
	}
	if v, _ := hist.DataPoints[0].Attributes.Value("goncordia.future.outcome"); v.AsString() != "completed" {
		t.Errorf("await duration outcome = %s", v.AsString())
	}
	size, ok := tm.metric(t, "goncordia.future.result.size").(metricdata.Histogram[int64])
	if !ok || len(size.DataPoints) != 1 || size.DataPoints[0].Sum != 2 {
		t.Fatalf("result size = %+v", size)
	}
	polls, ok := tm.metric(t, "goncordia.future.await.polls").(metricdata.Sum[int64])
	if !ok || len(polls.DataPoints) != 1 || polls.DataPoints[0].Value < 1 {
		t.Fatalf("polls = %+v", polls)
	}
	if _, ok := tm.metric(t, "goncordia.future.store.duration").(metricdata.Histogram[float64]); !ok {
		t.Fatal("store duration missing")
	}
}

func TestFutureInstrumentationMarksFailures(t *testing.T) {
	clk := clock.NewManual(time.Time{})
	tm := newTelemetry(clk)
	inst := otelgoncordia.NewFutureInstrumentation(tm.opts...)

	_, finish := inst.StartAwait(t.Context(), future.AwaitStart{JobID: "j1", Blocking: true})
	clk.Advance(3 * time.Second)
	finish(future.AwaitFinish{
		Job:     &driver.JobRow{ID: "j1", Kind: "square", Queue: "default", State: driver.JobStateDiscarded, AttemptNum: 3},
		Outcome: future.OutcomeDiscarded, Polls: 4, Err: errors.New("boom"),
	})
	_, finishStore := inst.StartStore(t.Context(), future.StoreStart{JobID: "j2", Kind: "square", Bytes: 10})
	finishStore(future.StoreFinish{Err: errors.New("disk full")})

	await := tm.span(t, "goncordia.future.await")
	if await.Status().Code != codes.Error || len(await.Events()) == 0 {
		t.Fatalf("await status = %+v events=%d", await.Status(), len(await.Events()))
	}
	if v, _ := attrValue(await.Attributes(), "goncordia.job.kind"); v.AsString() != "square" {
		t.Errorf("kind learned from row = %q", v.AsString())
	}
	if tm.span(t, "goncordia.future.store").Status().Code != codes.Error {
		t.Error("store span not marked as error")
	}
	hist := tm.metric(t, "goncordia.future.await.duration").(metricdata.Histogram[float64])
	if len(hist.DataPoints) != 1 || hist.DataPoints[0].Sum != 3 {
		t.Fatalf("await duration = %+v, want 3s from the injected clock", hist.DataPoints)
	}
	store := tm.metric(t, "goncordia.future.store.duration").(metricdata.Histogram[float64])
	if v, _ := store.DataPoints[0].Attributes.Value("status"); v.AsString() != "error" {
		t.Errorf("store status = %s", v.AsString())
	}
	var rm metricdata.ResourceMetrics
	_ = tm.reader.Collect(context.Background(), &rm)
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == "goncordia.future.result.size" {
				if h := m.Data.(metricdata.Histogram[int64]); len(h.DataPoints) != 0 {
					t.Error("failed write recorded a result size")
				}
			}
		}
	}
}
