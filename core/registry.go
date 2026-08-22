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
	ID             string
	Queue          string
	Kind           string
	Args           json.RawMessage
	AttemptNum     int
	MaxRetry       int
	CreatedAt      time.Time
	RunAt          time.Time
	WorkerID       string
	Tags           []string
	PipelineID     string
	PayloadVersion int
}

type payloadEnvelope struct {
	Version *int            `json:"_goncordia_payload_version"`
	Payload json.RawMessage `json:"payload"`
}

// EncodePayload wraps payloads newer than version 1 while leaving legacy
// version-1 JSON byte-for-byte compatible.
func EncodePayload(payload json.RawMessage, version int) (json.RawMessage, error) {
	if version <= 0 {
		return nil, fmt.Errorf("payload version must be positive")
	}
	if version == 1 {
		return payload, nil
	}
	return json.Marshal(struct {
		Version int             `json:"_goncordia_payload_version"`
		Payload json.RawMessage `json:"payload"`
	}{Version: version, Payload: payload})
}

func decodePayload(payload json.RawMessage) (int, json.RawMessage, error) {
	var envelope payloadEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return 0, nil, err
	}
	if envelope.Version == nil {
		return 1, payload, nil
	}
	if *envelope.Version <= 0 || len(envelope.Payload) == 0 {
		return 0, nil, fmt.Errorf("invalid versioned payload envelope")
	}
	return *envelope.Version, envelope.Payload, nil
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
			version, payload, err := decodePayload(rawJob.Args)
			if err != nil {
				return Discard(fmt.Errorf("decode job payload for kind %q: %w", kind, err))
			}
			currentVersion := opts.PayloadVersion
			if currentVersion <= 0 {
				currentVersion = 1
			}
			if version > currentVersion {
				return Discard(fmt.Errorf("job payload for kind %q has version %d, worker supports %d", kind, version, currentVersion))
			}
			for version < currentVersion {
				upcast := opts.Upcasters[version]
				if upcast == nil {
					return Discard(fmt.Errorf("missing payload upcaster for kind %q from version %d", kind, version))
				}
				payload, err = upcast(payload)
				if err != nil {
					return Discard(fmt.Errorf("upcast job payload for kind %q from version %d: %w", kind, version, err))
				}
				version++
			}
			rawJob.PayloadVersion = currentVersion
			var args T
			if err := json.Unmarshal(payload, &args); err != nil {
				return Discard(fmt.Errorf("unmarshal job args for kind %q: %w", kind, err))
			}
			typedJob := &Job[T]{
				ID:             rawJob.ID,
				Queue:          rawJob.Queue,
				Args:           args,
				AttemptNum:     rawJob.AttemptNum,
				MaxRetry:       rawJob.MaxRetry,
				CreatedAt:      rawJob.CreatedAt,
				RunAt:          rawJob.RunAt,
				WorkerID:       rawJob.WorkerID,
				Tags:           rawJob.Tags,
				PipelineID:     rawJob.PipelineID,
				PayloadVersion: currentVersion,
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
