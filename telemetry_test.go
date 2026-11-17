package goncordia_test

import (
	"context"
	"testing"

	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/driver"
)

type countingObserver struct {
	claimed, started, finished, limited, heartbeats, rescued int
}

func (o *countingObserver) JobClaimed(context.Context, driver.JobRow)               { o.claimed++ }
func (o *countingObserver) JobStarted(context.Context, goncordia.JobStartedEvent)   { o.started++ }
func (o *countingObserver) JobFinished(context.Context, goncordia.JobFinishedEvent) { o.finished++ }
func (o *countingObserver) JobRateLimited(context.Context, goncordia.RateLimitWaitEvent) {
	o.limited++
}
func (o *countingObserver) JobHeartbeat(context.Context, goncordia.HeartbeatEvent) { o.heartbeats++ }
func (o *countingObserver) JobsRescued(context.Context, goncordia.RescueEvent)     { o.rescued++ }

// finishedOnly handles one event and inherits no-ops for the rest.
type finishedOnly struct {
	goncordia.NopWorkerObserver
	finished int
}

func (o *finishedOnly) JobFinished(context.Context, goncordia.JobFinishedEvent) { o.finished++ }

func TestWorkerObserversFanOut(t *testing.T) {
	all := &countingObserver{}
	partial := &finishedOnly{}
	combined := goncordia.WorkerObservers(all, nil, partial)
	ctx := context.Background()

	combined.JobClaimed(ctx, driver.JobRow{})
	combined.JobStarted(ctx, goncordia.JobStartedEvent{})
	combined.JobFinished(ctx, goncordia.JobFinishedEvent{})
	combined.JobRateLimited(ctx, goncordia.RateLimitWaitEvent{})
	combined.JobHeartbeat(ctx, goncordia.HeartbeatEvent{})
	combined.JobsRescued(ctx, goncordia.RescueEvent{})

	if *all != (countingObserver{1, 1, 1, 1, 1, 1}) {
		t.Fatalf("observer = %+v", *all)
	}
	if partial.finished != 1 {
		t.Fatalf("partial observer finished = %d", partial.finished)
	}
}
