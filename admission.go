package goncordia

import (
	"context"
	"errors"
	"fmt"

	"github.com/kirimatt/goncordia/driver"
)

// ErrAdmissionRejected is returned when an enqueue admission policy refuses
// new work. Callers can use errors.Is without depending on policy details.
var ErrAdmissionRejected = errors.New("goncordia: enqueue admission rejected")

// AdmissionRequest describes a storage write before it happens.
type AdmissionRequest struct {
	Jobs          []driver.JobInsertParams
	Transactional bool
}

// AdmissionController decides whether an enqueue operation may proceed.
// Implementations must be safe for concurrent use.
type AdmissionController interface {
	Admit(context.Context, AdmissionRequest) error
}

// QueueFullError reports the queue and configured active-job ceiling that
// rejected an enqueue request.
type QueueFullError struct {
	Queue     string
	Active    int64
	Incoming  int
	MaxActive int64
}

func (e *QueueFullError) Error() string {
	return fmt.Sprintf("%v: queue %q has %d active jobs; adding %d exceeds limit %d",
		ErrAdmissionRejected, e.Queue, e.Active, e.Incoming, e.MaxActive)
}

func (e *QueueFullError) Unwrap() error { return ErrAdmissionRejected }

// QueueDepthAdmission rejects writes that would exceed a queue's active-job
// ceiling. The check is deliberately advisory: portable drivers cannot make a
// stats read and a later insert atomic. Use it for backpressure, not billing or
// security quotas.
type QueueDepthAdmission struct {
	exec      driver.AdminExecutor
	MaxActive map[string]int64
}

// NewQueueDepthAdmission creates a portable queue-depth controller. A missing
// queue or a non-positive limit is unlimited.
func NewQueueDepthAdmission[TTx any](d driver.Driver[TTx], limits map[string]int64) (*QueueDepthAdmission, error) {
	exec, ok := d.Executor().(driver.AdminExecutor)
	if !ok {
		return nil, fmt.Errorf("%w: driver %q does not expose queue statistics", driver.ErrUnsupported, d.Name())
	}
	return &QueueDepthAdmission{exec: exec, MaxActive: limits}, nil
}

// Admit implements AdmissionController.
func (a *QueueDepthAdmission) Admit(ctx context.Context, request AdmissionRequest) error {
	incoming := make(map[string]int)
	for _, job := range request.Jobs {
		incoming[job.Queue]++
	}
	for queue, count := range incoming {
		limit := a.MaxActive[queue]
		if limit <= 0 {
			continue
		}
		stats, err := a.exec.QueueStats(ctx, queue)
		if err != nil {
			return fmt.Errorf("admission stats for queue %q: %w", queue, err)
		}
		active := stats.States[driver.JobStateAvailable] +
			stats.States[driver.JobStateScheduled] +
			stats.States[driver.JobStateRetryable] +
			stats.States[driver.JobStateRunning]
		if active+int64(count) > limit {
			return &QueueFullError{Queue: queue, Active: active, Incoming: count, MaxActive: limit}
		}
	}
	return nil
}

var _ AdmissionController = (*QueueDepthAdmission)(nil)
