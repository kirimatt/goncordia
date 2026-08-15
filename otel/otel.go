// Package otelgoncordia provides OpenTelemetry instrumentation for goncordia.
//
// Add the middleware to WorkerConfig to get a span and metrics for every job:
//
//	import otelgoncordia "github.com/kirimatt/goncordia/otel"
//
//	wp := pgxdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
//	    Queues:     []string{"default"},
//	    Middleware: []goncordia.JobMiddleware{
//	        otelgoncordia.NewMiddleware(),
//	    },
//	})
//
// By default the package uses the global TracerProvider and MeterProvider.
// Supply your own with WithTracerProvider / WithMeterProvider.
package otelgoncordia

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	goncordia "github.com/kirimatt/goncordia"
	gonclock "github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
)

const (
	instrName    = "github.com/kirimatt/goncordia"
	spanName     = "goncordia.process"
	attrKind     = "goncordia.job.kind"
	attrQueue    = "goncordia.job.queue"
	attrID       = "goncordia.job.id"
	attrAttempt  = "goncordia.job.attempt"
	attrPipeline = "goncordia.job.pipeline_id"
	attrWorker   = "goncordia.worker.id"
	attrStatus   = "status"
	statusOK     = "ok"
	statusError  = "error"
)

// Option configures the OTel middleware.
type Option func(*mwOptions)

// WithTracerProvider sets the TracerProvider used to create job spans.
// Default: otel.GetTracerProvider() (the global provider).
func WithTracerProvider(tp trace.TracerProvider) Option {
	return func(o *mwOptions) { o.tracerProvider = tp }
}

// WithMeterProvider sets the MeterProvider used to record job metrics.
// Default: otel.GetMeterProvider() (the global provider).
func WithMeterProvider(mp metric.MeterProvider) Option {
	return func(o *mwOptions) { o.meterProvider = mp }
}

// WithClock sets the time source used for duration and queue-time metrics.
func WithClock(clk gonclock.Clock) Option {
	return func(o *mwOptions) { o.clock = clk }
}

type mwOptions struct {
	tracerProvider trace.TracerProvider
	meterProvider  metric.MeterProvider
	clock          gonclock.Clock
}

// NewMiddleware returns a JobMiddleware that:
//   - creates a span for each job execution named "goncordia.process"
//   - records goncordia.job.duration (histogram, seconds) and
//     goncordia.job.count (counter) with attributes kind, queue, status
func NewMiddleware(opts ...Option) goncordia.JobMiddleware {
	o := &mwOptions{}
	for _, opt := range opts {
		opt(o)
	}

	tp := o.tracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	mp := o.meterProvider
	if mp == nil {
		mp = otel.GetMeterProvider()
	}
	clk := o.clock
	if clk == nil {
		clk = gonclock.Real{}
	}

	tracer := tp.Tracer(instrName)
	meter := mp.Meter(instrName)

	duration, _ := meter.Float64Histogram(
		"goncordia.job.duration",
		metric.WithDescription("Job execution duration in seconds"),
		metric.WithUnit("s"),
	)
	queueTime, _ := meter.Float64Histogram(
		"goncordia.job.queue_time",
		metric.WithDescription("Time from enqueue to worker execution in seconds"),
		metric.WithUnit("s"),
	)
	count, _ := meter.Int64Counter(
		"goncordia.job.count",
		metric.WithDescription("Number of jobs processed"),
	)

	return func(ctx context.Context, job *core.RawJob, next func(context.Context, *core.RawJob) error) error {
		attrs := []attribute.KeyValue{
			attribute.String(attrKind, job.Kind),
			attribute.String(attrQueue, job.Queue),
			attribute.String(attrID, job.ID),
			attribute.Int(attrAttempt, job.AttemptNum),
		}
		if job.PipelineID != "" {
			attrs = append(attrs, attribute.String(attrPipeline, job.PipelineID))
		}
		if job.WorkerID != "" {
			attrs = append(attrs, attribute.String(attrWorker, job.WorkerID))
		}

		ctx, span := tracer.Start(ctx, spanName,
			trace.WithAttributes(attrs...),
			trace.WithSpanKind(trace.SpanKindConsumer),
		)

		start := clk.Now()
		if !job.CreatedAt.IsZero() {
			queueTime.Record(ctx, clk.Since(job.CreatedAt).Seconds(), metric.WithAttributes(
				attribute.String(attrKind, job.Kind),
				attribute.String(attrQueue, job.Queue),
			))
		}
		err := next(ctx, job)
		elapsed := clk.Since(start).Seconds()

		status := statusOK
		if err != nil {
			status = statusError
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		} else {
			span.SetStatus(codes.Ok, "")
		}
		span.End()

		metricAttrs := metric.WithAttributes(
			attribute.String(attrKind, job.Kind),
			attribute.String(attrQueue, job.Queue),
			attribute.String(attrStatus, status),
		)
		duration.Record(ctx, elapsed, metricAttrs)
		count.Add(ctx, 1, metricAttrs)

		return err
	}
}
