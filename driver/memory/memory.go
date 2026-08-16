// Package memory provides an in-memory driver for testing and development.
// It has no persistence — all jobs are lost on process restart.
// TTx is NoTx; the driver does not claim rollback semantics.
package memory

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/driver"
)

// NoTx is the transaction type for the memory driver.
// In-memory operations are individually synchronized but do not provide rollback.
type NoTx struct{}

// Driver implements driver.Driver[NoTx] using in-memory maps.
type Driver struct {
	mu      sync.Mutex
	jobs    map[string]*driver.JobRow
	queues  map[string]*driver.QueueRow
	seq     uint64
	notify  map[string][]chan driver.Notification
	leaders map[string]memoryLeader
	clk     clock.Clock
}

type memoryLeader struct {
	workerID  string
	expiresAt time.Time
}

// Option configures a memory Driver.
type Option func(*Driver)

// WithClock injects a custom Clock (e.g. clock.NewMock() in tests).
func WithClock(c clock.Clock) Option {
	return func(d *Driver) { d.clk = c }
}

// New creates a new in-memory Driver.
func New(opts ...Option) *Driver {
	d := &Driver{
		jobs:    make(map[string]*driver.JobRow),
		queues:  make(map[string]*driver.QueueRow),
		notify:  make(map[string][]chan driver.Notification),
		leaders: make(map[string]memoryLeader),
		clk:     clock.Real{},
	}
	for _, o := range opts {
		o(d)
	}
	return d
}

func (d *Driver) Name() string { return "memory" }

func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		NativeTx:      false,
		SkipLocked:    true,
		UniqueJobs:    true,
		ListenNotify:  true,
		AdvisoryLocks: false,
	}
}

func (d *Driver) Executor() driver.Executor         { return &executor{d: d} }
func (d *Driver) UnwrapTx(_ NoTx) driver.ExecutorTx { return &txExecutor{executor: executor{d: d}} }
func (d *Driver) Listener() driver.Listener {
	return &listener{d: d, subs: make(map[string]chan driver.Notification)}
}
func (d *Driver) Close() error { return nil }

// --- executor ---

type executor struct{ d *Driver }

func (e *executor) Begin(_ context.Context) (driver.ExecutorTx, error) {
	return nil, fmt.Errorf("%w: memory transactions", driver.ErrUnsupported)
}

func (e *executor) JobInsertMany(_ context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()

	results := make([]driver.JobInsertResult, 0, len(params))
	for _, p := range params {
		if p.UniqueKey != "" {
			if dup := e.d.findUniqueJob(p.UniqueKey); dup != nil {
				results = append(results, driver.JobInsertResult{Job: dup, UniqueSkip: true})
				continue
			}
		}

		e.d.seq++
		id := "mem_" + strconv.FormatUint(e.d.seq, 10)
		now := e.d.clk.Now()
		runAt := p.RunAt
		if runAt.IsZero() {
			runAt = now
		}
		state := driver.JobStateAvailable
		if runAt.After(now) {
			state = driver.JobStateScheduled
		}
		row := &driver.JobRow{
			ID:         id,
			Queue:      p.Queue,
			Kind:       p.Kind,
			Args:       p.Args,
			State:      state,
			Priority:   p.Priority,
			RunAt:      runAt,
			CreatedAt:  now,
			MaxRetry:   p.MaxRetry,
			Timeout:    p.Timeout,
			Tags:       p.Tags,
			UniqueKey:  p.UniqueKey,
			PipelineID: p.PipelineID,
		}
		e.d.jobs[id] = row
		e.d.ensureQueue(p.Queue, now)
		e.d.broadcastNotify(p.Queue)

		results = append(results, driver.JobInsertResult{Job: row})
	}
	return results, nil
}

func (e *executor) JobGetByID(_ context.Context, id string) (*driver.JobRow, error) {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	row, ok := e.d.jobs[id]
	if !ok {
		return nil, fmt.Errorf("job %q not found", id)
	}
	cp := *row
	return &cp, nil
}

func (e *executor) JobList(_ context.Context, params driver.JobListParams) ([]driver.JobRow, error) {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	rows := make([]driver.JobRow, 0, len(e.d.jobs))
	for _, row := range e.d.jobs {
		if params.Queue != "" && row.Queue != params.Queue || params.State != "" && row.State != params.State || params.Kind != "" && row.Kind != params.Kind || params.Cursor != "" && row.ID >= params.Cursor {
			continue
		}
		rows = append(rows, *row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].CreatedAt.Equal(rows[j].CreatedAt) {
			return rows[i].CreatedAt.After(rows[j].CreatedAt)
		}
		return rows[i].ID > rows[j].ID
	})
	limit := params.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

func (e *executor) QueueStats(_ context.Context, queue string) (driver.QueueStats, error) {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	stats := driver.QueueStats{Queue: queue, States: make(map[driver.JobState]int64)}
	for _, row := range e.d.jobs {
		if row.Queue == queue {
			stats.States[row.State]++
			stats.Total++
		}
	}
	return stats, nil
}

func (e *executor) JobFetchBatch(_ context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()

	q, ok := e.d.queues[params.Queue]
	if !ok || (q != nil && q.Paused) {
		return nil, nil
	}

	now := e.d.clk.Now()
	var candidates []*driver.JobRow
	for _, j := range e.d.jobs {
		if j.Queue != params.Queue {
			continue
		}
		// Promote scheduled jobs whose RunAt has passed
		if j.State == driver.JobStateScheduled && !j.RunAt.After(now) {
			j.State = driver.JobStateAvailable
		}
		if j.State == driver.JobStateAvailable && !j.RunAt.After(now) {
			candidates = append(candidates, j)
		}
	}

	sort.Slice(candidates, func(i, k int) bool {
		if candidates[i].Priority != candidates[k].Priority {
			return candidates[i].Priority > candidates[k].Priority
		}
		if !candidates[i].RunAt.Equal(candidates[k].RunAt) {
			return candidates[i].RunAt.Before(candidates[k].RunAt)
		}
		if !candidates[i].CreatedAt.Equal(candidates[k].CreatedAt) {
			return candidates[i].CreatedAt.Before(candidates[k].CreatedAt)
		}
		return candidates[i].ID < candidates[k].ID
	})

	limit := params.Limit
	if limit <= 0 || limit > len(candidates) {
		limit = len(candidates)
	}

	rows := make([]driver.JobRow, 0, limit)
	for _, j := range candidates[:limit] {
		t := now
		j.State = driver.JobStateRunning
		j.AttemptedAt = &t
		j.AttemptNum++
		j.WorkerID = params.WorkerID
		cp := *j
		rows = append(rows, cp)
	}
	return rows, nil
}

func (e *executor) JobRescueStuck(_ context.Context, params driver.JobRescueParams) (int64, error) {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	var rescued int64
	for _, row := range e.d.jobs {
		if row.Queue != params.Queue || row.State != driver.JobStateRunning || row.AttemptedAt == nil || row.AttemptedAt.After(params.Before) {
			continue
		}
		row.State = driver.JobStateAvailable
		row.AttemptedAt = nil
		row.WorkerID = ""
		rescued++
	}
	return rescued, nil
}

func (e *executor) JobHeartbeat(_ context.Context, params driver.JobHeartbeatParams) (bool, error) {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	row, ok := e.d.jobs[params.ID]
	if !ok || row.State != driver.JobStateRunning || row.WorkerID != params.WorkerID || row.AttemptNum != params.Attempt {
		return false, nil
	}
	at := params.At.UTC()
	row.AttemptedAt = &at
	return true, nil
}

func (e *executor) JobSetStateIfRunning(_ context.Context, params driver.JobSetStateParams) error {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	row, ok := e.d.jobs[params.ID]
	if !ok || row.State != driver.JobStateRunning || !params.MatchesClaim(row.WorkerID, row.AttemptNum) {
		return nil
	}
	if params.Yield {
		row.State = driver.JobStateAvailable
		row.AttemptNum--
		row.AttemptedAt = nil
		row.WorkerID = ""
		return nil
	}
	row.State = params.State
	row.WorkerID = ""
	if params.Err != nil {
		row.Errors = append(row.Errors, driver.AttemptError{
			At:      e.d.clk.Now(),
			Attempt: row.AttemptNum,
			Error:   *params.Err,
		})
	}
	if params.State == driver.JobStateRetryable && !params.RetryAt.IsZero() {
		row.RunAt = params.RetryAt
		row.State = driver.JobStateAvailable
	}
	if params.State == driver.JobStateCompleted ||
		params.State == driver.JobStateDiscarded ||
		params.State == driver.JobStateCancelled {
		now := e.d.clk.Now()
		row.FinalizedAt = &now
	}
	return nil
}

func (e *executor) JobCancel(_ context.Context, id string) error {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	row, ok := e.d.jobs[id]
	if !ok {
		return fmt.Errorf("job %q not found", id)
	}
	if row.State != driver.JobStateAvailable && row.State != driver.JobStateScheduled {
		return fmt.Errorf("job %q is in state %s, can only cancel available/scheduled", id, row.State)
	}
	row.State = driver.JobStateCancelled
	now := e.d.clk.Now()
	row.FinalizedAt = &now
	return nil
}

func (e *executor) JobDelete(_ context.Context, id string) error {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	delete(e.d.jobs, id)
	return nil
}

func (e *executor) JobReschedule(_ context.Context, params driver.RescheduleParams) error {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	row, ok := e.d.jobs[params.ID]
	if !ok {
		return fmt.Errorf("job %q not found", params.ID)
	}
	row.RunAt = params.RunAt
	row.State = driver.JobStateScheduled
	return nil
}

func (e *executor) QueueGet(_ context.Context, name string) (*driver.QueueRow, error) {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	q, ok := e.d.queues[name]
	if !ok {
		return nil, fmt.Errorf("queue %q not found", name)
	}
	cp := *q
	return &cp, nil
}

func (e *executor) QueuePause(_ context.Context, name string) error {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	e.d.ensureQueue(name, e.d.clk.Now())
	e.d.queues[name].Paused = true
	return nil
}

func (e *executor) QueueResume(_ context.Context, name string) error {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	e.d.ensureQueue(name, e.d.clk.Now())
	e.d.queues[name].Paused = false
	return nil
}

func (e *executor) QueueList(_ context.Context, _ driver.QueueListParams) ([]*driver.QueueRow, error) {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	rows := make([]*driver.QueueRow, 0, len(e.d.queues))
	for _, q := range e.d.queues {
		cp := *q
		rows = append(rows, &cp)
	}
	return rows, nil
}

func (e *executor) LeaderAttemptElect(_ context.Context, params driver.LeaderElectParams) (bool, error) {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	now := e.d.clk.Now()
	current, ok := e.d.leaders[params.Name]
	if ok && current.expiresAt.After(now) && current.workerID != params.WorkerID {
		return false, nil
	}
	e.d.leaders[params.Name] = memoryLeader{workerID: params.WorkerID, expiresAt: now.Add(params.TTL)}
	return true, nil
}

func (e *executor) LeaderResign(_ context.Context, params driver.LeaderResignParams) error {
	e.d.mu.Lock()
	defer e.d.mu.Unlock()
	if current, ok := e.d.leaders[params.Name]; ok && current.workerID == params.WorkerID {
		delete(e.d.leaders, params.Name)
	}
	return nil
}

// --- txExecutor ---

type txExecutor struct{ executor }

func (t *txExecutor) Commit(_ context.Context) error   { return nil }
func (t *txExecutor) Rollback(_ context.Context) error { return nil }

// --- listener ---

type listener struct {
	d    *Driver
	subs map[string]chan driver.Notification
}

func (l *listener) Listen(_ context.Context, queue string) (<-chan driver.Notification, error) {
	l.d.mu.Lock()
	defer l.d.mu.Unlock()
	ch := make(chan driver.Notification, 16)
	l.d.notify[queue] = append(l.d.notify[queue], ch)
	l.subs[queue] = ch
	return ch, nil
}

func (l *listener) Unlisten(_ context.Context, queue string) error {
	l.d.mu.Lock()
	defer l.d.mu.Unlock()
	l.remove(queue)
	return nil
}

func (l *listener) Close() error {
	l.d.mu.Lock()
	defer l.d.mu.Unlock()
	for queue := range l.subs {
		l.remove(queue)
	}
	return nil
}

// remove is called with d.mu held.
func (l *listener) remove(queue string) {
	owned := l.subs[queue]
	if owned == nil {
		return
	}
	channels := l.d.notify[queue]
	for i, ch := range channels {
		if ch == owned {
			channels = append(channels[:i], channels[i+1:]...)
			break
		}
	}
	if len(channels) == 0 {
		delete(l.d.notify, queue)
	} else {
		l.d.notify[queue] = channels
	}
	delete(l.subs, queue)
	close(owned)
}

// --- helpers ---

func (d *Driver) ensureQueue(name string, now time.Time) {
	if _, ok := d.queues[name]; !ok {
		d.queues[name] = &driver.QueueRow{Name: name, CreatedAt: now, UpdatedAt: now}
	}
}

func (d *Driver) broadcastNotify(queue string) {
	for _, ch := range d.notify[queue] {
		select {
		case ch <- driver.Notification{Queue: queue}:
		default:
		}
	}
}

func (d *Driver) findUniqueJob(uniqueKey string) *driver.JobRow {
	for _, j := range d.jobs {
		if j.UniqueKey == uniqueKey &&
			(driver.IsPermanentUniqueKey(uniqueKey) ||
				j.State != driver.JobStateCompleted &&
					j.State != driver.JobStateDiscarded &&
					j.State != driver.JobStateCancelled) {
			return j
		}
	}
	return nil
}

// AllJobs returns a snapshot of all jobs for test inspection.
func (d *Driver) AllJobs() []driver.JobRow {
	d.mu.Lock()
	defer d.mu.Unlock()
	rows := make([]driver.JobRow, 0, len(d.jobs))
	for _, j := range d.jobs {
		rows = append(rows, *j)
	}
	return rows
}

// compile-time interface checks
var _ driver.Driver[NoTx] = (*Driver)(nil)
var _ driver.Executor = (*executor)(nil)
