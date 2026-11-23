package otelgoncordia_test

import (
	"context"
	"strings"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	otelgoncordia "github.com/kirimatt/goncordia/otel/v2"
	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

func TestTraceContextTagsRoundTrip(t *testing.T) {
	if tags := otelgoncordia.TraceContextTags(context.Background()); tags != nil {
		t.Fatalf("tags without a span = %v, want nil", tags)
	}

	tp := sdktrace.NewTracerProvider()
	ctx, span := tp.Tracer("test").Start(context.Background(), "request")
	defer span.End()

	tags := otelgoncordia.TraceContextTags(ctx)
	if len(tags) != 1 || !strings.HasPrefix(tags[0], "traceparent=00-"+span.SpanContext().TraceID().String()) {
		t.Fatalf("tags = %v", tags)
	}

	got := trace.SpanContextFromContext(otelgoncordia.ExtractTraceContext(context.Background(), append([]string{"user-tag"}, tags...)))
	if !got.IsRemote() || got.TraceID() != span.SpanContext().TraceID() || got.SpanID() != span.SpanContext().SpanID() {
		t.Fatalf("extracted %+v, want span %+v", got, span.SpanContext())
	}

	plain := context.Background()
	if otelgoncordia.ExtractTraceContext(plain, []string{"user-tag", "traceparent=garbage"}) != plain {
		t.Fatal("ExtractTraceContext changed ctx without a valid traceparent")
	}
}

// One trace from the request that enqueues the job to the worker that runs it.
func TestTraceContinuesFromEnqueueToWorker(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	d := memory.New()
	inst := otelgoncordia.NewInstrumentation(otelgoncordia.WithTracerProvider(tp))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Observer: inst, ContextTags: otelgoncordia.TraceContextTags})

	ctx, request := tp.Tracer("app").Start(context.Background(), "POST /orders")
	res, err := client.Enqueue(ctx, emailJob{To: "a@example.com"}, nil)
	request.End()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(strings.Join(res.Job.Tags, ","), "traceparent=") {
		t.Fatalf("job tags = %v", res.Job.Tags)
	}

	done := make(chan struct{})
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[emailJob](func(context.Context, *core.Job[emailJob]) error {
		close(done)
		return nil
	}), core.WorkerOpts{})
	wp := goncordia.NewWorkerPool[memory.NoTx](d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		PollInterval: 10 * time.Millisecond,
		Middleware:   []goncordia.JobMiddleware{otelgoncordia.NewMiddleware(otelgoncordia.WithTracerProvider(tp))},
	})
	runCtx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() { defer close(stopped); _ = wp.Start(runCtx) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("job did not run")
	}
	cancel()
	<-stopped

	spans := map[string]sdktrace.ReadOnlySpan{}
	for _, s := range recorder.Ended() {
		spans[s.Name()] = s
	}
	enqueue, process := spans["goncordia.enqueue"], spans["goncordia.process"]
	if enqueue == nil || process == nil {
		t.Fatalf("spans = %v", keys(spans))
	}
	traceID := request.SpanContext().TraceID()
	if enqueue.SpanContext().TraceID() != traceID || process.SpanContext().TraceID() != traceID {
		t.Fatalf("trace ids: request %s, enqueue %s, process %s", traceID, enqueue.SpanContext().TraceID(), process.SpanContext().TraceID())
	}
	if process.Parent().SpanID() != enqueue.SpanContext().SpanID() {
		t.Fatalf("process parent = %s, want enqueue span %s", process.Parent().SpanID(), enqueue.SpanContext().SpanID())
	}
}

func keys(m map[string]sdktrace.ReadOnlySpan) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}
