package goncordia_test

import (
	"context"
	"testing"

	"github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/driver"
)

type countingObserver struct{ claimed, heartbeats, rescued int }

func (o *countingObserver) JobClaimed(context.Context, driver.JobRow)              { o.claimed++ }
func (o *countingObserver) JobHeartbeat(context.Context, goncordia.HeartbeatEvent) { o.heartbeats++ }
func (o *countingObserver) JobsRescued(context.Context, goncordia.RescueEvent)     { o.rescued++ }

type countingLifecycleObserver struct {
	countingObserver
	started, finished, limited int
}

func (o *countingLifecycleObserver) JobStarted(context.Context, goncordia.JobStartedEvent) {
	o.started++
}
func (o *countingLifecycleObserver) JobFinished(context.Context, goncordia.JobFinishedEvent) {
	o.finished++
}
func (o *countingLifecycleObserver) JobRateLimited(context.Context, goncordia.RateLimitWaitEvent) {
	o.limited++
}

func TestWorkerObserversFanOut(t *testing.T) {
	plain := &countingObserver{}
	lifecycle := &countingLifecycleObserver{}
	combined := goncordia.WorkerObservers(plain, nil, lifecycle)
	ctx := context.Background()

	combined.JobClaimed(ctx, driver.JobRow{})
	combined.JobHeartbeat(ctx, goncordia.HeartbeatEvent{})
	combined.JobsRescued(ctx, goncordia.RescueEvent{})
	events, ok := combined.(goncordia.WorkerLifecycleObserver)
	if !ok {
		t.Fatal("combined observer must implement WorkerLifecycleObserver")
	}
	events.JobStarted(ctx, goncordia.JobStartedEvent{})
	events.JobFinished(ctx, goncordia.JobFinishedEvent{})
	events.JobRateLimited(ctx, goncordia.RateLimitWaitEvent{})

	if *plain != (countingObserver{1, 1, 1}) {
		t.Fatalf("plain observer = %+v", *plain)
	}
	if lifecycle.countingObserver != (countingObserver{1, 1, 1}) ||
		lifecycle.started != 1 || lifecycle.finished != 1 || lifecycle.limited != 1 {
		t.Fatalf("lifecycle observer = %+v", *lifecycle)
	}
}
