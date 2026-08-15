// Package core contains the backend-agnostic engine logic:
// job/worker registration, retry policies, scheduling, and middleware.
package core

import (
	"context"
	"time"
)

// JobArgs is implemented by structs that represent job arguments.
// Kind() returns a unique identifier used to route jobs to the right worker.
type JobArgs interface {
	Kind() string
}

// Worker processes jobs of a specific type.
// T must implement JobArgs.
type Worker[T JobArgs] interface {
	// Process handles a single job. Return a non-nil error to trigger retry logic.
	Process(ctx context.Context, job *Job[T]) error
}

// WorkerFunc is an adapter to allow plain functions to be used as Workers.
type WorkerFunc[T JobArgs] func(ctx context.Context, job *Job[T]) error

func (f WorkerFunc[T]) Process(ctx context.Context, job *Job[T]) error { return f(ctx, job) }

// Job is the typed job instance passed to a Worker.
// It carries the deserialized arguments and execution metadata.
type Job[T JobArgs] struct {
	// ID is the unique job identifier (backend-specific format).
	ID string
	// Queue is the queue this job belongs to.
	Queue string
	// Args contains the deserialized job arguments.
	Args T
	// AttemptNum is the current attempt number (1-based).
	AttemptNum int
	// MaxRetry is the maximum number of attempts before the job is discarded.
	MaxRetry int
	// CreatedAt is when the job was first enqueued.
	CreatedAt time.Time
	// WorkerID identifies the worker pool that claimed this attempt.
	WorkerID string
	// Tags are optional labels attached to the job at enqueue time.
	Tags []string
	// PipelineID groups jobs that must not run concurrently. Jobs sharing the same
	// non-empty PipelineID are serialized within a single WorkerPool process.
	PipelineID string
}

// InsertOpts controls optional parameters when enqueueing a job.
type InsertOpts struct {
	// Queue overrides the default queue name for this job.
	Queue string
	// Priority sets the job priority (higher = processed first). Default: 0.
	Priority int
	// RunAt schedules the job for future execution. Zero means immediately.
	RunAt time.Time
	// UniqueOpts prevents duplicate jobs from being inserted.
	UniqueOpts *UniqueOpts
	// MaxRetry overrides the worker's default max retry count.
	MaxRetry *int
	// Timeout overrides the worker's default job execution timeout.
	Timeout *time.Duration
	// Tags attaches arbitrary labels to the job for filtering/observability.
	Tags []string
	// PipelineID groups jobs that must not run concurrently within the same
	// WorkerPool process. Jobs sharing the same non-empty PipelineID run
	// sequentially in claim order. Empty string disables this behaviour.
	// Note: this guarantee is per-process only — it does not coordinate across
	// multiple worker pool instances.
	PipelineID string
}

// UniqueOpts controls deduplication behavior.
type UniqueOpts struct {
	// Completed, discarded, and cancelled jobs never block a new insert.
	// This rule is identical across all drivers.
	// ByArgs deduplicates based on the job arguments (JSON equality).
	ByArgs bool
	// ByQueue includes the queue name in the uniqueness key.
	ByQueue bool
	// ByPeriod deduplicates within a rolling time window.
	ByPeriod time.Duration
}

// WorkerOpts configures default behavior for a Worker registration.
type WorkerOpts struct {
	// Queue is retained for source compatibility. Queue selection belongs to
	// InsertOpts or ClientConfig and this field has no effect.
	// Deprecated: use InsertOpts.Queue or ClientConfig.DefaultQueue.
	Queue string
	// MaxRetry is the maximum number of attempts. Default: 3.
	MaxRetry int
	// Timeout is the maximum duration for a single job execution. 0 = no timeout.
	Timeout time.Duration
	// Concurrency limits how many of this job type run simultaneously.
	// 0 means inherit from the global pool limit.
	Concurrency int
}
