package future

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/kirimatt/goncordia/core"
)

// Handler processes a job and returns the value delivered to its Future.
type Handler[A core.JobArgs, R any] func(ctx context.Context, job *core.Job[A]) (R, error)

// RegisterWorker registers h for jobs of type A. On success the returned value
// is JSON-encoded and stored before the job is marked completed, so a Future
// never observes a completed job whose value is still being written.
//
// A handler error follows the normal retry policy and stores nothing. A value
// that cannot be encoded discards the job, because a retry would fail the same
// way. A failed result write is returned as a retryable error, so the handler
// runs again; handlers must be idempotent as with any at-least-once job.
func RegisterWorker[A core.JobArgs, R any](registry *core.Registry, m *Manager, h Handler[A, R], opts core.WorkerOpts) {
	core.RegisterWorker(registry, core.WorkerFunc[A](func(ctx context.Context, job *core.Job[A]) error {
		value, err := h(ctx, job)
		if err != nil {
			return err
		}
		raw, err := json.Marshal(value)
		if err != nil {
			return core.Discard(fmt.Errorf("goncordia/future: encode result of job %s: %w", job.ID, err))
		}
		storeCtx, finish := m.startStore(ctx, StoreStart{
			JobID: job.ID, Kind: job.Args.Kind(), Queue: job.Queue, Attempt: job.AttemptNum, Bytes: len(raw),
		})
		err = m.store.JobResultPut(storeCtx, job.ID, raw)
		finish(StoreFinish{Err: err})
		if err != nil {
			return fmt.Errorf("goncordia/future: store result of job %s: %w", job.ID, err)
		}
		return nil
	}), opts)
}
