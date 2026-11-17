package goncordia

import (
	"context"
	"time"

	"github.com/kirimatt/goncordia/driver"
)

// EnqueueStart describes an enqueue operation before storage access.
type EnqueueStart struct {
	Driver        string
	Count         int
	Transactional bool
	Queue         string
	Kind          string
}

// EnqueueFinish describes the outcome of an enqueue operation.
type EnqueueFinish struct {
	Inserted      int
	UniqueSkipped int
	Err           error
}

// ClientObserver instruments enqueue operations. StartEnqueue may enrich the
// returned context and must return a completion callback.
type ClientObserver interface {
	StartEnqueue(context.Context, EnqueueStart) (context.Context, func(EnqueueFinish))
}

// HeartbeatEvent describes one attempt to renew a running claim.
type HeartbeatEvent struct {
	Job     driver.JobRow
	Renewed bool
	Err     error
}

// RescueEvent describes one abandoned-claim rescue pass.
type RescueEvent struct {
	Queue   string
	Before  time.Time
	Rescued int64
	Err     error
}

// JobStartedEvent describes the instant a claimed job enters its handler.
type JobStartedEvent struct {
	Job       driver.JobRow
	StartedAt time.Time
	ClaimWait time.Duration
}

// JobFinishedEvent describes the handler outcome selected by the worker.
type JobFinishedEvent struct {
	Job        driver.JobRow
	State      driver.JobState
	Err        error
	StartedAt  time.Time
	FinishedAt time.Time
}

// RateLimitWaitEvent describes a job delayed by local or global rate policy.
type RateLimitWaitEvent struct {
	Job     driver.JobRow
	Scope   RateLimitScope
	RetryAt time.Time
	Err     error
}

// WorkerObserver receives claim, heartbeat, and rescue lifecycle events.
type WorkerObserver interface {
	JobClaimed(context.Context, driver.JobRow)
	JobHeartbeat(context.Context, HeartbeatEvent)
	JobsRescued(context.Context, RescueEvent)
}

// WorkerLifecycleObserver is an optional extension implemented by observers
// that need execution and rate-limit lifecycle events. WorkerObserver remains
// source-compatible for existing instrumentation.
type WorkerLifecycleObserver interface {
	JobStarted(context.Context, JobStartedEvent)
	JobFinished(context.Context, JobFinishedEvent)
	JobRateLimited(context.Context, RateLimitWaitEvent)
}

// WorkerObservers fans worker events out to several observers in order. Nil
// observers are skipped. Lifecycle events reach only observers that implement
// WorkerLifecycleObserver. Use it to combine, for example, OpenTelemetry
// instrumentation with a future.Manager.
func WorkerObservers(observers ...WorkerObserver) WorkerObserver {
	list := make(workerObserverList, 0, len(observers))
	for _, observer := range observers {
		if observer != nil {
			list = append(list, observer)
		}
	}
	return list
}

type workerObserverList []WorkerObserver

func (l workerObserverList) JobClaimed(ctx context.Context, job driver.JobRow) {
	for _, observer := range l {
		observer.JobClaimed(ctx, job)
	}
}

func (l workerObserverList) JobHeartbeat(ctx context.Context, event HeartbeatEvent) {
	for _, observer := range l {
		observer.JobHeartbeat(ctx, event)
	}
}

func (l workerObserverList) JobsRescued(ctx context.Context, event RescueEvent) {
	for _, observer := range l {
		observer.JobsRescued(ctx, event)
	}
}

func (l workerObserverList) JobStarted(ctx context.Context, event JobStartedEvent) {
	for _, observer := range l {
		if lifecycle, ok := observer.(WorkerLifecycleObserver); ok {
			lifecycle.JobStarted(ctx, event)
		}
	}
}

func (l workerObserverList) JobFinished(ctx context.Context, event JobFinishedEvent) {
	for _, observer := range l {
		if lifecycle, ok := observer.(WorkerLifecycleObserver); ok {
			lifecycle.JobFinished(ctx, event)
		}
	}
}

func (l workerObserverList) JobRateLimited(ctx context.Context, event RateLimitWaitEvent) {
	for _, observer := range l {
		if lifecycle, ok := observer.(WorkerLifecycleObserver); ok {
			lifecycle.JobRateLimited(ctx, event)
		}
	}
}

var _ WorkerLifecycleObserver = workerObserverList(nil)
