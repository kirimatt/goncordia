package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// workerEntry is the type-erased wrapper stored in the registry.
type workerEntry struct {
	opts    WorkerOpts
	process func(ctx context.Context, rawJob *RawJob) error
}

// RawJob is an untyped job as returned from the storage layer,
// before its Args are deserialized into the concrete T type.
type RawJob struct {
	ID         string
	Queue      string
	Kind       string
	Args       json.RawMessage
	AttemptNum int
	MaxRetry   int
	CreatedAt  time.Time
	RunAt      time.Time
	WorkerID   string
	Tags       []string
	PipelineID string
}

// Registry maps job kinds to their type-erased worker implementations.
// It is built by calling RegisterWorker and consumed by the engine.
type Registry struct {
	workers map[string]workerEntry
}

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
	return &Registry{workers: make(map[string]workerEntry)}
}

// RegisterWorker registers a typed Worker for the given job args type T.
// Kind is determined by calling T{}.Kind() via the zero value.
func RegisterWorker[T JobArgs](r *Registry, w Worker[T], opts WorkerOpts) {
	var zero T
	kind := zero.Kind()
	r.workers[kind] = workerEntry{
		opts: opts,
		process: func(ctx context.Context, rawJob *RawJob) error {
			var args T
			if err := json.Unmarshal(rawJob.Args, &args); err != nil {
				return fmt.Errorf("unmarshal job args for kind %q: %w", kind, err)
			}
			typedJob := &Job[T]{
				ID:         rawJob.ID,
				Queue:      rawJob.Queue,
				Args:       args,
				AttemptNum: rawJob.AttemptNum,
				MaxRetry:   rawJob.MaxRetry,
				CreatedAt:  rawJob.CreatedAt,
				RunAt:      rawJob.RunAt,
				WorkerID:   rawJob.WorkerID,
				Tags:       rawJob.Tags,
				PipelineID: rawJob.PipelineID,
			}
			return w.Process(ctx, typedJob)
		},
	}
}

// Process dispatches a raw job to the correct worker based on its Kind.
func (r *Registry) Process(ctx context.Context, rawJob *RawJob) error {
	entry, ok := r.workers[rawJob.Kind]
	if !ok {
		return fmt.Errorf("no worker registered for job kind %q", rawJob.Kind)
	}
	return entry.process(ctx, rawJob)
}

// Opts returns the WorkerOpts for a given job kind.
func (r *Registry) Opts(kind string) (WorkerOpts, bool) {
	entry, ok := r.workers[kind]
	if !ok {
		return WorkerOpts{}, false
	}
	return entry.opts, true
}

// Kinds returns all registered job kinds.
func (r *Registry) Kinds() []string {
	kinds := make([]string, 0, len(r.workers))
	for k := range r.workers {
		kinds = append(kinds, k)
	}
	return kinds
}
