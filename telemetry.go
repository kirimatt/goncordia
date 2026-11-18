package goncordia

import (
	"context"
	"time"

	"github.com/kirimatt/goncordia/v2/driver"
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

// WorkerObserver receives worker lifecycle events: claims, handler start and
// finish, rate-limit waits, heartbeats, and rescues. Embed NopWorkerObserver
// to implement only the events you need.
type WorkerObserver interface {
	JobClaimed(context.Context, driver.JobRow)
	JobStarted(context.Context, JobStartedEvent)
	JobFinished(context.Context, JobFinishedEvent)
	JobRateLimited(context.Context, RateLimitWaitEvent)
	JobHeartbeat(context.Context, HeartbeatEvent)
	JobsRescued(context.Context, RescueEvent)
}

// NopWorkerObserver ignores every event. Embed it in observers that handle
// only some events.
type NopWorkerObserver struct{}

func (NopWorkerObserver) JobClaimed(context.Context, driver.JobRow)          {}
func (NopWorkerObserver) JobStarted(context.Context, JobStartedEvent)        {}
func (NopWorkerObserver) JobFinished(context.Context, JobFinishedEvent)      {}
func (NopWorkerObserver) JobRateLimited(context.Context, RateLimitWaitEvent) {}
func (NopWorkerObserver) JobHeartbeat(context.Context, HeartbeatEvent)       {}
func (NopWorkerObserver) JobsRescued(context.Context, RescueEvent)           {}

// WorkerObservers fans worker events out to several observers in order. Nil
// observers are skipped. Use it to combine, for example, OpenTelemetry
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

func (l workerObserverList) JobStarted(ctx context.Context, event JobStartedEvent) {
	for _, observer := range l {
		observer.JobStarted(ctx, event)
	}
}

func (l workerObserverList) JobFinished(ctx context.Context, event JobFinishedEvent) {
	for _, observer := range l {
		observer.JobFinished(ctx, event)
	}
}

func (l workerObserverList) JobRateLimited(ctx context.Context, event RateLimitWaitEvent) {
	for _, observer := range l {
		observer.JobRateLimited(ctx, event)
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

var (
	_ WorkerObserver = NopWorkerObserver{}
	_ WorkerObserver = workerObserverList(nil)
)

// RetentionPassEvent describes one retention pass that this instance ran as
// the leader.
type RetentionPassEvent struct {
	Result     PruneResult
	Err        error
	StartedAt  time.Time
	FinishedAt time.Time
}

// RetentionObserver is an optional extension of WorkerObserver. A worker
// pool, or RunRetention with RetentionRunConfig.Observer, calls it after each
// retention pass this instance runs as the leader. WorkerObservers forwards
// it to the observers that implement it.
type RetentionObserver interface {
	RetentionPassed(context.Context, RetentionPassEvent)
}

func (l workerObserverList) RetentionPassed(ctx context.Context, event RetentionPassEvent) {
	for _, observer := range l {
		if o, ok := observer.(RetentionObserver); ok {
			o.RetentionPassed(ctx, event)
		}
	}
}
