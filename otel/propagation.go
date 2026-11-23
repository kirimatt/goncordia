package otelgoncordia

import (
	"context"
	"strings"

	"go.opentelemetry.io/otel/propagation"
)

// Tag names that carry the W3C trace context on a job. A job tag has the form
// "traceparent=00-<trace id>-<span id>-<flags>".
const (
	TraceParentTag = "traceparent"
	TraceStateTag  = "tracestate"
)

var traceContext = propagation.TraceContext{}

// TraceContextTags returns the W3C trace context of ctx as job tags, or nil
// when ctx carries no valid span. Set it as goncordia.ClientConfig.ContextTags
// together with the Instrumentation as Observer; NewMiddleware then continues
// the trace in the worker, so one trace covers the request that enqueued the
// job, the enqueue and every attempt:
//
//	inst := otelgoncordia.NewInstrumentation()
//	client := goncordia.NewClient(d, goncordia.ClientConfig{
//	    Observer:    inst,
//	    ContextTags: otelgoncordia.TraceContextTags,
//	})
func TraceContextTags(ctx context.Context) []string {
	carrier := propagation.MapCarrier{}
	traceContext.Inject(ctx, carrier)
	parent := carrier.Get(TraceParentTag)
	if parent == "" {
		return nil
	}
	tags := []string{TraceParentTag + "=" + parent}
	if state := carrier.Get(TraceStateTag); state != "" {
		tags = append(tags, TraceStateTag+"="+state)
	}
	return tags
}

// ExtractTraceContext returns ctx with the remote span context carried by the
// job tags written by TraceContextTags. ctx is returned unchanged when the
// tags carry none.
func ExtractTraceContext(ctx context.Context, tags []string) context.Context {
	carrier := propagation.MapCarrier{}
	for _, tag := range tags {
		name, value, ok := strings.Cut(tag, "=")
		if ok && (name == TraceParentTag || name == TraceStateTag) {
			carrier.Set(name, value)
		}
	}
	if carrier.Get(TraceParentTag) == "" {
		return ctx
	}
	return traceContext.Extract(ctx, carrier)
}
