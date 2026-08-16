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
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	goncordia "github.com/kirimatt/goncordia"
	gonclock "github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver"
)

const (
	instrName    = "github.com/kirimatt/goncordia"
	spanName     = "goncordia.process"
	enqueueSpan  = "goncordia.enqueue"
	attrKind     = "goncordia.job.kind"
	attrQueue    = "goncordia.job.queue"
	attrID       = "goncordia.job.id"
	attrAttempt  = "goncordia.job.attempt"
	attrPipeline = "goncordia.job.pipeline_id"
	attrWorker   = "goncordia.worker.id"
	attrStatus   = "status"
	attrDriver   = "goncordia.driver"
	attrCount    = "goncordia.job.batch_size"
	attrTx       = "goncordia.enqueue.transactional"
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

// Instrumentation implements goncordia.ClientObserver and
// goncordia.WorkerObserver with OpenTelemetry spans and metrics.
type Instrumentation struct {
	tracer       trace.Tracer
	clock        gonclock.Clock
	enqueueTime  metric.Float64Histogram
	scheduleLag  metric.Float64Histogram
	heartbeats   metric.Int64Counter
	leaseRescues metric.Int64Counter
}

// NewInstrumentation creates enqueue tracing plus claim, heartbeat, and lease
// metrics. Pass it to ClientConfig.Observer and WorkerConfig.Observer.
func NewInstrumentation(opts ...Option) *Instrumentation {
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
	meter := mp.Meter(instrName)
	enqueueTime, _ := meter.Float64Histogram("goncordia.enqueue.duration",
		metric.WithDescription("Job enqueue storage duration in seconds"), metric.WithUnit("s"))
	scheduleLag, _ := meter.Float64Histogram("goncordia.job.schedule_lag",
		metric.WithDescription("Delay from run_at eligibility to worker claim in seconds"), metric.WithUnit("s"))
	heartbeats, _ := meter.Int64Counter("goncordia.job.heartbeat.count",
		metric.WithDescription("Number of claim heartbeat attempts"))
	leaseRescues, _ := meter.Int64Counter("goncordia.job.lease_rescued",
		metric.WithDescription("Number of expired claims returned to the queue"))
	return &Instrumentation{
		tracer: tp.Tracer(instrName), clock: clk, enqueueTime: enqueueTime,
		scheduleLag: scheduleLag, heartbeats: heartbeats, leaseRescues: leaseRescues,
	}
}

// StartEnqueue starts a producer span and returns its completion callback.
func (i *Instrumentation) StartEnqueue(ctx context.Context, event goncordia.EnqueueStart) (context.Context, func(goncordia.EnqueueFinish)) {
	attrs := []attribute.KeyValue{
		attribute.String(attrDriver, event.Driver),
		attribute.Int(attrCount, event.Count),
		attribute.Bool(attrTx, event.Transactional),
	}
	if event.Queue != "" {
		attrs = append(attrs, attribute.String(attrQueue, event.Queue))
	}
	if event.Kind != "" {
		attrs = append(attrs, attribute.String(attrKind, event.Kind))
	}
	started := i.clock.Now()
	ctx, span := i.tracer.Start(ctx, enqueueSpan, trace.WithSpanKind(trace.SpanKindProducer), trace.WithAttributes(attrs...))
	return ctx, func(result goncordia.EnqueueFinish) {
		status := statusOK
		if result.Err != nil {
			status = statusError
			span.RecordError(result.Err)
			span.SetStatus(codes.Error, result.Err.Error())
		} else {
			span.SetStatus(codes.Ok, "")
		}
		span.SetAttributes(
			attribute.Int("goncordia.enqueue.inserted", result.Inserted),
			attribute.Int("goncordia.enqueue.unique_skipped", result.UniqueSkipped),
		)
		span.End()
		i.enqueueTime.Record(ctx, nonNegativeSeconds(i.clock.Since(started)), metric.WithAttributes(
			attribute.String(attrDriver, event.Driver), attribute.String(attrStatus, status),
		))
	}
}

// JobClaimed records delay from eligibility to claim. Scheduled time does not
// count as queue wait.
func (i *Instrumentation) JobClaimed(ctx context.Context, job driver.JobRow) {
	if job.RunAt.IsZero() || !job.RunAt.After(job.CreatedAt) {
		return
	}
	i.scheduleLag.Record(ctx, nonNegativeSeconds(i.clock.Now().Sub(job.RunAt)), metric.WithAttributes(
		attribute.String(attrKind, job.Kind), attribute.String(attrQueue, job.Queue),
	))
}

// JobHeartbeat records successful, stale, and failed heartbeat attempts.
func (i *Instrumentation) JobHeartbeat(ctx context.Context, event goncordia.HeartbeatEvent) {
	status := statusOK
	if event.Err != nil {
		status = statusError
	} else if !event.Renewed {
		status = "stale"
	}
	i.heartbeats.Add(ctx, 1, metric.WithAttributes(
		attribute.String(attrKind, event.Job.Kind), attribute.String(attrQueue, event.Job.Queue),
		attribute.String(attrStatus, status),
	))
}

// JobsRescued records lease rescue counts and failures.
func (i *Instrumentation) JobsRescued(ctx context.Context, event goncordia.RescueEvent) {
	status := statusOK
	if event.Err != nil {
		status = statusError
	}
	i.leaseRescues.Add(ctx, event.Rescued, metric.WithAttributes(
		attribute.String(attrQueue, event.Queue), attribute.String(attrStatus, status),
	))
}

func nonNegativeSeconds(duration time.Duration) float64 {
	if duration < 0 {
		return 0
	}
	return duration.Seconds()
}

var _ goncordia.ClientObserver = (*Instrumentation)(nil)
var _ goncordia.WorkerObserver = (*Instrumentation)(nil)

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
		eligibleAt := job.CreatedAt
		if job.RunAt.After(eligibleAt) {
			eligibleAt = job.RunAt
		}
		if !eligibleAt.IsZero() {
			queueTime.Record(ctx, nonNegativeSeconds(clk.Now().Sub(eligibleAt)), metric.WithAttributes(
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
