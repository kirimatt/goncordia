// Package future lets a producer enqueue a job and wait for the value its
// handler returns.
//
// Declare a Task once, next to the job arguments, to tie them to the result
// type. Workers register a handler for it; application code receives a bound
// Func and calls it without type parameters:
//
//	// contract, shared by producers and workers
//	var Resize = future.Define[ResizeArgs, Thumbnail]()
//
//	// main
//	m, err := future.New(d, future.Config{})
//	Resize.Register(registry, m, resizeHandler, core.WorkerOpts{})
//	wp := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{Observer: m})
//	gallery := &Gallery{resize: future.Bind(m, client, Resize)}
//
//	// application code (method of Gallery)
//	thumb, err := g.resize.Call(ctx, ResizeArgs{ImageID: id})
//
// The handler's value is JSON-encoded and written to a result store before the
// job is marked completed. Use BindTx to also enqueue inside a transaction, and
// the lower-level Enqueue, EnqueueTx, RegisterWorker, and Of functions when a
// Task is not convenient.
//
// Futures work with every driver. Job state is read through
// Executor.JobGetByID and results are stored by the driver, or by Config.Store
// when it is set. When the producer and the worker pool share a Manager in one
// process, and the Manager is installed as (or inside goncordia.WorkerObservers
// for) WorkerConfig.Observer, Await wakes as soon as the job finishes.
// Otherwise it polls storage every Config.PollInterval on the injected clock.
//
// Delivery is at-least-once. A handler may run again after a crash or a failed
// result write, and the last stored value wins.
package future

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
)

// DefaultPollInterval is used when Config.PollInterval is zero.
const DefaultPollInterval = time.Second

// Config configures a Manager.
type Config struct {
	// Store persists handler results. Nil stores them with the driver.
	Store driver.JobResultStore
	// PollInterval bounds how long Await sleeps between storage reads when no
	// local completion signal arrives. Default: DefaultPollInterval.
	PollInterval time.Duration
	// Clock drives poll timers. Default: clock.Real{}.
	Clock clock.Clock
	// Observer receives await and result-store events for metrics and traces.
	Observer Observer
	// KeepCalledJobs keeps the job and result after Func.Call returns a
	// value. By default Call deletes them: its job ID never reaches the
	// caller, so nobody can await it again.
	KeepCalledJobs bool
}

// jobReader is the part of driver.Executor that futures need.
type jobReader interface {
	JobGetByID(ctx context.Context, id string) (*driver.JobRow, error)
	JobDelete(ctx context.Context, id string) error
}

// Manager connects futures, result storage, and local completion signals. One
// Manager is safe for concurrent use and should be shared by the producers and
// worker pools of a process.
type Manager struct {
	goncordia.NopWorkerObserver

	jobs     jobReader
	store    driver.JobResultStore
	clock    clock.Clock
	poll     time.Duration
	observer Observer
	keepCall bool

	mu      sync.Mutex
	waiters map[string]map[chan struct{}]struct{}
}

// New creates a Manager for d. Results are stored by the driver unless
// Config.Store is set.
func New[TTx any](d driver.Driver[TTx], cfg Config) (*Manager, error) {
	if d == nil {
		return nil, errors.New("goncordia/future: nil driver")
	}
	exec := d.Executor()
	store := cfg.Store
	if store == nil {
		store = exec
	}
	if cfg.PollInterval < 0 {
		return nil, fmt.Errorf("goncordia/future: negative PollInterval %s", cfg.PollInterval)
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = DefaultPollInterval
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	return &Manager{
		jobs:     exec,
		store:    store,
		clock:    cfg.Clock,
		poll:     cfg.PollInterval,
		observer: cfg.Observer,
		keepCall: cfg.KeepCalledJobs,
		waiters:  make(map[string]map[chan struct{}]struct{}),
	}, nil
}

// Forget deletes the stored result of jobID. Use it once a result has been
// consumed if the job row itself is retained.
func (m *Manager) Forget(ctx context.Context, jobID string) error {
	return m.store.JobResultDelete(ctx, jobID)
}

// release deletes the job and result behind a consumed Call. A unique job may
// be followed by other callers, so it stays for retention. Failures are
// ignored: retention removes the job later.
func (m *Manager) release(ctx context.Context, jobID string, unique bool) {
	if m.keepCall || unique {
		return
	}
	_ = m.store.JobResultDelete(ctx, jobID)
	_ = m.jobs.JobDelete(ctx, jobID)
}

// Enqueue inserts args through client and returns a Future for its result.
// When unique options match an existing job, the Future follows that job.
func Enqueue[R any, TTx any](ctx context.Context, m *Manager, client *goncordia.Client[TTx], args core.JobArgs, opts *core.InsertOpts) (*Future[R], error) {
	result, err := client.Enqueue(ctx, args, opts)
	if err != nil {
		return nil, err
	}
	return attach[R](m, result, args)
}

// EnqueueTx inserts args inside tx and returns a Future for its result. The
// job is invisible until tx commits, so call Await only after the commit.
func EnqueueTx[R any, TTx any](ctx context.Context, m *Manager, client *goncordia.Client[TTx], tx TTx, args core.JobArgs, opts *core.InsertOpts) (*Future[R], error) {
	result, err := client.EnqueueTx(ctx, tx, args, opts)
	if err != nil {
		return nil, err
	}
	return attach[R](m, result, args)
}

// Of returns a Future for an existing job, for example one enqueued by another
// process whose ID was passed along.
func Of[R any](m *Manager, jobID string) *Future[R] {
	return &Future[R]{m: m, id: jobID}
}

func attach[R any](m *Manager, result *driver.JobInsertResult, args core.JobArgs) (*Future[R], error) {
	if result == nil || result.Job == nil || result.Job.ID == "" {
		return nil, errors.New("goncordia/future: driver returned no job for the insert")
	}
	return &Future[R]{m: m, id: result.Job.ID, kind: args.Kind(), unique: result.Job.UniqueKey != ""}, nil
}

// Future is the pending result of one job. Terminal outcomes are cached, so
// repeated Await calls after resolution do not read storage again.
type Future[R any] struct {
	m    *Manager
	id   string
	kind string
	// unique jobs may be shared with other callers, so Call keeps them.
	unique bool

	mu       sync.Mutex
	resolved bool
	value    R
	err      error
}

// ID returns the job ID the Future follows.
func (f *Future[R]) ID() string { return f.id }

// Await blocks until the job reaches a terminal state or ctx ends.
//
// It returns the handler value for a completed job, a *JobError matching
// ErrDiscarded or ErrCancelled for a failed job, ErrNoResult for a job that
// completed without a stored value, ErrResultLost for a job whose stored
// value disappeared (for example evicted from Redis), and ErrNotFound (which also matches
// driver.ErrNotFound) when the job does not exist. Await does not wait for a
// missing job to appear: a job whose transaction rolled back or has not
// committed yet, or that was deleted, returns ErrNotFound immediately; the
// three cases look the same in storage. Not-found, context, and storage
// errors are not cached, so a later Await reads storage again.
func (f *Future[R]) Await(ctx context.Context) (R, error) {
	if value, err, ok := f.cached(); ok {
		return value, err
	}
	ctx, finish := f.m.startAwait(ctx, AwaitStart{JobID: f.id, Kind: f.jobKind(), Blocking: true})
	wake := f.m.subscribe(f.id)
	defer f.m.unsubscribe(f.id, wake)

	var stats AwaitFinish
	var ticker clock.Ticker
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()
	for {
		value, done, err := f.check(ctx, &stats)
		if done || err != nil {
			stats.Err = err
			finish(stats)
			return value, err
		}
		if ticker == nil {
			ticker = f.m.clock.NewTicker(f.m.poll)
		}
		select {
		case <-ctx.Done():
			var zero R
			stats.Outcome = OutcomeAbandoned
			stats.Err = ctx.Err()
			finish(stats)
			return zero, ctx.Err()
		case <-wake:
			stats.Wakeups++
		case <-ticker.C():
		}
	}
}

// TryGet reads the job state once without waiting. done is false while the
// job has not reached a terminal state.
func (f *Future[R]) TryGet(ctx context.Context) (value R, done bool, err error) {
	if value, err, ok := f.cached(); ok {
		return value, true, err
	}
	ctx, finish := f.m.startAwait(ctx, AwaitStart{JobID: f.id, Kind: f.jobKind()})
	var stats AwaitFinish
	value, done, err = f.check(ctx, &stats)
	if !done && err == nil {
		stats.Outcome = OutcomePending
	}
	stats.Err = err
	finish(stats)
	return value, done, err
}

func (f *Future[R]) jobKind() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.kind
}

func (f *Future[R]) cached() (R, error, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.value, f.err, f.resolved
}

func (f *Future[R]) resolve(value R, err error) (R, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.resolved {
		f.value, f.err, f.resolved = value, err, true
	}
	return f.value, true, f.err
}

// check performs one storage read. It returns done=true for cached terminal
// outcomes and records the read in stats.
func (f *Future[R]) check(ctx context.Context, stats *AwaitFinish) (R, bool, error) {
	var zero R
	stats.Polls++
	row, err := f.m.jobs.JobGetByID(ctx, f.id)
	if err == nil && row == nil {
		// Drivers before v2.5.1 could report a missing job as (nil, nil).
		err = fmt.Errorf("%w: job %q", driver.ErrNotFound, f.id)
	}
	if err != nil {
		if errors.Is(err, driver.ErrNotFound) {
			stats.Outcome = OutcomeNotFound
			return zero, false, fmt.Errorf("%w: job %s: %w", ErrNotFound, f.id, err)
		}
		stats.Outcome = OutcomeError
		return zero, false, fmt.Errorf("goncordia/future: read job %s: %w", f.id, err)
	}
	stats.Job = row
	f.mu.Lock()
	if f.kind == "" {
		f.kind = row.Kind
	}
	f.mu.Unlock()
	switch row.State {
	case driver.JobStateCompleted:
		raw, err := f.m.store.JobResultGet(ctx, f.id)
		if errors.Is(err, driver.ErrResultLost) {
			stats.Outcome = OutcomeResultLost
			return f.resolve(zero, fmt.Errorf("%w: job %s: %w", ErrResultLost, f.id, err))
		}
		if errors.Is(err, driver.ErrNotFound) {
			stats.Outcome = OutcomeNoResult
			return f.resolve(zero, fmt.Errorf("%w: job %s", ErrNoResult, f.id))
		}
		if err != nil {
			stats.Outcome = OutcomeError
			return zero, false, fmt.Errorf("goncordia/future: read result of job %s: %w", f.id, err)
		}
		stats.ResultBytes = len(raw)
		var value R
		if err := json.Unmarshal(raw, &value); err != nil {
			stats.Outcome = OutcomeError
			return f.resolve(zero, fmt.Errorf("goncordia/future: decode result of job %s: %w", f.id, err))
		}
		stats.Outcome = OutcomeCompleted
		return f.resolve(value, nil)
	case driver.JobStateDiscarded, driver.JobStateCancelled:
		jobErr := newJobError(row)
		if row.State == driver.JobStateDiscarded {
			stats.Outcome = OutcomeDiscarded
		} else {
			stats.Outcome = OutcomeCancelled
		}
		return f.resolve(zero, jobErr)
	default:
		return zero, false, nil
	}
}

func (m *Manager) subscribe(jobID string) chan struct{} {
	ch := make(chan struct{}, 1)
	m.mu.Lock()
	defer m.mu.Unlock()
	set := m.waiters[jobID]
	if set == nil {
		set = make(map[chan struct{}]struct{})
		m.waiters[jobID] = set
	}
	set[ch] = struct{}{}
	return ch
}

func (m *Manager) unsubscribe(jobID string, ch chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	set := m.waiters[jobID]
	delete(set, ch)
	if len(set) == 0 {
		delete(m.waiters, jobID)
	}
}

func (m *Manager) notify(jobID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for ch := range m.waiters[jobID] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// waiting reports how many Await calls currently wait on jobID. Tests use it to
// synchronize with a manual clock.
func (m *Manager) waiting(jobID string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.waiters[jobID])
}

// JobFinished wakes local Await calls once the worker has persisted a
// terminal state. Retryable outcomes keep waiters asleep.
func (m *Manager) JobFinished(_ context.Context, event goncordia.JobFinishedEvent) {
	switch event.State {
	case driver.JobStateCompleted, driver.JobStateDiscarded, driver.JobStateCancelled:
		m.notify(event.Job.ID)
	}
}

var _ goncordia.WorkerObserver = (*Manager)(nil)
