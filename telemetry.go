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

// WorkerObserver receives claim, heartbeat, and rescue lifecycle events.
type WorkerObserver interface {
	JobClaimed(context.Context, driver.JobRow)
	JobHeartbeat(context.Context, HeartbeatEvent)
	JobsRescued(context.Context, RescueEvent)
}
