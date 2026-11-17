package otelgoncordia

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"

	gonclock "github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/future"
)

const (
	futureAwaitSpan = "goncordia.future.await"
	futureStoreSpan = "goncordia.future.store"
	attrOutcome     = "goncordia.future.outcome"
	attrBlocking    = "goncordia.future.blocking"
	attrPolls       = "goncordia.future.polls"
	attrWakeups     = "goncordia.future.wakeups"
	attrResultBytes = "goncordia.future.result.bytes"
	attrState       = "goncordia.job.state"
)

// FutureInstrumentation implements future.Observer with OpenTelemetry spans
// and metrics. Pass it to future.Config.Observer.
//
// Spans:
//   - goncordia.future.await: one Await or TryGet call, with the job ID, kind,
//     outcome, state reads, local wakeups, and result size.
//   - goncordia.future.store: one result write. Install NewMiddleware on the
//     worker pool to make it a child of the goncordia.process span.
//
// Metrics:
//   - goncordia.future.await.duration (s): Await/TryGet latency by kind,
//     outcome, and blocking.
//   - goncordia.future.await.polls: job-state reads by kind.
//   - goncordia.future.await.wakeups: local completion signals by kind. A low
//     wakeup share means producers and workers run in different processes and
//     latency is bounded by future.Config.PollInterval.
//   - goncordia.future.result.size (By): stored result size by kind.
//   - goncordia.future.store.duration (s): result write latency by kind and status.
type FutureInstrumentation struct {
	tracer        trace.Tracer
	clock         gonclock.Clock
	awaitDuration metric.Float64Histogram
	awaitPolls    metric.Int64Counter
	awaitWakeups  metric.Int64Counter
	resultSize    metric.Int64Histogram
	storeDuration metric.Float64Histogram
}

// NewFutureInstrumentation creates future instrumentation. It accepts the same
// options as NewInstrumentation.
func NewFutureInstrumentation(opts ...Option) *FutureInstrumentation {
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
	awaitDuration, _ := meter.Float64Histogram("goncordia.future.await.duration",
		metric.WithDescription("Duration of future Await and TryGet calls in seconds"), metric.WithUnit("s"))
	awaitPolls, _ := meter.Int64Counter("goncordia.future.await.polls",
		metric.WithDescription("Number of job-state reads made by futures"))
	awaitWakeups, _ := meter.Int64Counter("goncordia.future.await.wakeups",
		metric.WithDescription("Number of local completion signals received by futures"))
	resultSize, _ := meter.Int64Histogram("goncordia.future.result.size",
		metric.WithDescription("Size of stored future results in bytes"), metric.WithUnit("By"))
	storeDuration, _ := meter.Float64Histogram("goncordia.future.store.duration",
		metric.WithDescription("Duration of future result writes in seconds"), metric.WithUnit("s"))
	return &FutureInstrumentation{
		tracer: tp.Tracer(instrName), clock: clk,
		awaitDuration: awaitDuration, awaitPolls: awaitPolls, awaitWakeups: awaitWakeups,
		resultSize: resultSize, storeDuration: storeDuration,
	}
}

// StartAwait starts a goncordia.future.await span.
func (i *FutureInstrumentation) StartAwait(ctx context.Context, start future.AwaitStart) (context.Context, func(future.AwaitFinish)) {
	attrs := []attribute.KeyValue{
		attribute.String(attrID, start.JobID),
		attribute.Bool(attrBlocking, start.Blocking),
	}
	if start.Kind != "" {
		attrs = append(attrs, attribute.String(attrKind, start.Kind))
	}
	began := i.clock.Now()
	ctx, span := i.tracer.Start(ctx, futureAwaitSpan, trace.WithSpanKind(trace.SpanKindInternal), trace.WithAttributes(attrs...))
	return ctx, func(finish future.AwaitFinish) {
		kind := start.Kind
		if kind == "" && finish.Job != nil {
			kind = finish.Job.Kind
			span.SetAttributes(attribute.String(attrKind, kind))
		}
		if finish.Job != nil {
			span.SetAttributes(
				attribute.String(attrQueue, finish.Job.Queue),
				attribute.String(attrState, string(finish.Job.State)),
				attribute.Int(attrAttempt, finish.Job.AttemptNum),
			)
		}
		span.SetAttributes(
			attribute.String(attrOutcome, string(finish.Outcome)),
			attribute.Int(attrPolls, finish.Polls),
			attribute.Int(attrWakeups, finish.Wakeups),
			attribute.Int(attrResultBytes, finish.ResultBytes),
		)
		switch {
		case finish.Outcome == future.OutcomeCompleted || finish.Outcome == future.OutcomePending:
			span.SetStatus(codes.Ok, "")
		case finish.Err != nil:
			span.RecordError(finish.Err)
			span.SetStatus(codes.Error, finish.Err.Error())
		}
		span.End()

		kindAttr := attribute.String(attrKind, kind)
		i.awaitDuration.Record(ctx, nonNegativeSeconds(i.clock.Since(began)), metric.WithAttributes(
			kindAttr, attribute.String(attrOutcome, string(finish.Outcome)), attribute.Bool(attrBlocking, start.Blocking),
		))
		i.awaitPolls.Add(ctx, int64(finish.Polls), metric.WithAttributes(kindAttr))
		if finish.Wakeups > 0 {
			i.awaitWakeups.Add(ctx, int64(finish.Wakeups), metric.WithAttributes(kindAttr))
		}
	}
}

// StartStore starts a goncordia.future.store span.
func (i *FutureInstrumentation) StartStore(ctx context.Context, start future.StoreStart) (context.Context, func(future.StoreFinish)) {
	began := i.clock.Now()
	ctx, span := i.tracer.Start(ctx, futureStoreSpan, trace.WithSpanKind(trace.SpanKindInternal), trace.WithAttributes(
		attribute.String(attrID, start.JobID),
		attribute.String(attrKind, start.Kind),
		attribute.String(attrQueue, start.Queue),
		attribute.Int(attrAttempt, start.Attempt),
		attribute.Int(attrResultBytes, start.Bytes),
	))
	return ctx, func(finish future.StoreFinish) {
		status := statusOK
		if finish.Err != nil {
			status = statusError
			span.RecordError(finish.Err)
			span.SetStatus(codes.Error, finish.Err.Error())
		} else {
			span.SetStatus(codes.Ok, "")
		}
		span.End()
		kindAttr := attribute.String(attrKind, start.Kind)
		i.storeDuration.Record(ctx, nonNegativeSeconds(i.clock.Since(began)), metric.WithAttributes(
			kindAttr, attribute.String(attrStatus, status),
		))
		if finish.Err == nil {
			i.resultSize.Record(ctx, int64(start.Bytes), metric.WithAttributes(kindAttr))
		}
	}
}

var _ future.Observer = (*FutureInstrumentation)(nil)
