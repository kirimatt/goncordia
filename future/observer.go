package future

import (
	"context"
	"errors"
	"fmt"

	"github.com/kirimatt/goncordia/v2/driver"
)

// Errors returned by Await. Use errors.Is to classify them.
var (
	// ErrDiscarded matches a *JobError for a job that exhausted its attempts
	// or was discarded by its handler.
	ErrDiscarded = errors.New("goncordia/future: job discarded")
	// ErrCancelled matches a *JobError for a cancelled job.
	ErrCancelled = errors.New("goncordia/future: job cancelled")
	// ErrNoResult reports a completed job without a stored value, for example
	// one processed by a plain core.Worker instead of future.RegisterWorker.
	ErrNoResult = errors.New("goncordia/future: job completed without a stored result")
)

// JobError describes a job that ended without a value.
type JobError struct {
	JobID   string
	Kind    string
	State   driver.JobState
	Attempt int
	// Message is the last recorded handler error, if any.
	Message string
}

func (e *JobError) Error() string {
	if e.Message == "" {
		return fmt.Sprintf("goncordia/future: job %s (%s) %s", e.JobID, e.Kind, e.State)
	}
	return fmt.Sprintf("goncordia/future: job %s (%s) %s after attempt %d: %s", e.JobID, e.Kind, e.State, e.Attempt, e.Message)
}

// Is matches ErrDiscarded or ErrCancelled according to State.
func (e *JobError) Is(target error) bool {
	switch target {
	case ErrDiscarded:
		return e.State == driver.JobStateDiscarded
	case ErrCancelled:
		return e.State == driver.JobStateCancelled
	}
	return false
}

func newJobError(row *driver.JobRow) *JobError {
	err := &JobError{JobID: row.ID, Kind: row.Kind, State: row.State, Attempt: row.AttemptNum}
	if n := len(row.Errors); n > 0 {
		last := row.Errors[n-1]
		err.Message = last.Error
		err.Attempt = last.Attempt
	}
	return err
}

// Outcome classifies how an Await or TryGet call ended.
type Outcome string

const (
	OutcomeCompleted Outcome = "completed"
	OutcomeDiscarded Outcome = "discarded"
	OutcomeCancelled Outcome = "cancelled"
	OutcomeNoResult  Outcome = "no_result"
	OutcomeNotFound  Outcome = "not_found"
	// OutcomePending is reported by TryGet while the job is still running.
	OutcomePending Outcome = "pending"
	// OutcomeAbandoned means the caller's context ended before the job did.
	OutcomeAbandoned Outcome = "abandoned"
	// OutcomeError covers storage and decoding failures.
	OutcomeError Outcome = "error"
)

// AwaitStart describes an Await or TryGet call before its first storage read.
// Kind is empty for futures created with Of until the job row is read.
type AwaitStart struct {
	JobID string
	Kind  string
	// Blocking is true for Await and false for TryGet.
	Blocking bool
}

// AwaitFinish describes how an Await or TryGet call ended.
type AwaitFinish struct {
	// Job is the last row read, or nil if no row was read.
	Job     *driver.JobRow
	Outcome Outcome
	// Polls counts job-state reads; Wakeups counts local completion signals.
	Polls       int
	Wakeups     int
	ResultBytes int
	Err         error
}

// StoreStart describes a result write after a successful handler run.
type StoreStart struct {
	JobID   string
	Kind    string
	Queue   string
	Attempt int
	Bytes   int
}

// StoreFinish describes the outcome of a result write.
type StoreFinish struct {
	Err error
}

// Observer instruments futures. Each Start method may enrich the returned
// context and must return a non-nil completion callback.
type Observer interface {
	StartAwait(context.Context, AwaitStart) (context.Context, func(AwaitFinish))
	StartStore(context.Context, StoreStart) (context.Context, func(StoreFinish))
}

func (m *Manager) startAwait(ctx context.Context, start AwaitStart) (context.Context, func(AwaitFinish)) {
	if m.observer == nil {
		return ctx, func(AwaitFinish) {}
	}
	return m.observer.StartAwait(ctx, start)
}

func (m *Manager) startStore(ctx context.Context, event StoreStart) (context.Context, func(StoreFinish)) {
	if m.observer == nil {
		return ctx, func(StoreFinish) {}
	}
	return m.observer.StartStore(ctx, event)
}
