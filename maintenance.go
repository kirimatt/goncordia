package goncordia

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/driver"
)

const maintenancePageSize = 500

// Maintenance provides portable retention, bulk-action, and dead-letter APIs
// using the common driver contract.
type Maintenance[TTx any] struct {
	driver driver.Driver[TTx]
	clock  clock.Clock
}

// MaintenanceConfig configures maintenance operations.
type MaintenanceConfig struct {
	Clock clock.Clock
}

// KeepForever as a RetentionPolicy duration keeps that state forever. In
// WorkerConfig.Retention, where zero means the default, it is the way to turn
// a state off.
const KeepForever time.Duration = -1

// DefaultRetentionPolicy is what worker pools apply unless WorkerConfig
// overrides it: a day of completed and cancelled jobs, a week of discarded
// ones for dead-letter inspection, and an hour of future results.
var DefaultRetentionPolicy = RetentionPolicy{
	Completed: 24 * time.Hour,
	Discarded: 7 * 24 * time.Hour,
	Cancelled: 24 * time.Hour,
	Results:   time.Hour,
}

// RetentionPolicy configures how long terminal jobs remain available. A zero
// duration or KeepForever disables pruning for that state.
type RetentionPolicy struct {
	Completed time.Duration
	Discarded time.Duration
	Cancelled time.Duration
	// Results deletes the stored future results of completed jobs finalized
	// this long ago, while the job rows stay until Completed. Use it to free
	// result storage early, including results nobody awaited. It has no
	// effect when it is not shorter than Completed.
	Results time.Duration
}

// withDefaults fills zero fields from defaults.
func (p RetentionPolicy) withDefaults(defaults RetentionPolicy) RetentionPolicy {
	fill := func(v *time.Duration, d time.Duration) {
		if *v == 0 {
			*v = d
		}
	}
	fill(&p.Completed, defaults.Completed)
	fill(&p.Discarded, defaults.Discarded)
	fill(&p.Cancelled, defaults.Cancelled)
	fill(&p.Results, defaults.Results)
	return p
}

// normalize validates the policy and maps KeepForever to zero.
func (p RetentionPolicy) normalize() (RetentionPolicy, error) {
	for _, v := range []*time.Duration{&p.Completed, &p.Discarded, &p.Cancelled, &p.Results} {
		switch {
		case *v == KeepForever:
			*v = 0
		case *v < 0:
			return p, fmt.Errorf("retention duration must not be negative (use KeepForever)")
		}
	}
	return p, nil
}

// PruneResult summarizes a retention pass.
type PruneResult struct {
	Scanned        int `json:"scanned"`
	Deleted        int `json:"deleted"`
	ResultsDeleted int `json:"results_deleted"`
}

// BulkFailure describes one item that could not be changed.
type BulkFailure struct {
	ID    string `json:"id"`
	Error string `json:"error"`
}

// BulkResult preserves partial progress from a bulk operation.
type BulkResult struct {
	Succeeded []string      `json:"succeeded"`
	Failed    []BulkFailure `json:"failed"`
}

// NewMaintenance creates a maintenance service for d.
func NewMaintenance[TTx any](d driver.Driver[TTx], cfg MaintenanceConfig) *Maintenance[TTx] {
	clk := cfg.Clock
	if clk == nil {
		clk = clock.Real{}
	}
	return &Maintenance[TTx]{driver: d, clock: clk}
}

// Prune deletes terminal jobs finalized on or before each configured cutoff,
// and the stored results of completed jobs older than policy.Results.
func (m *Maintenance[TTx]) Prune(ctx context.Context, policy RetentionPolicy) (PruneResult, error) {
	policy, err := policy.normalize()
	if err != nil {
		return PruneResult{}, err
	}
	if pruner, ok := m.driver.Executor().(driver.JobPruner); ok {
		return m.pruneIndexed(ctx, pruner, policy)
	}
	admin, ok := m.driver.Executor().(driver.AdminExecutor)
	if !ok {
		return PruneResult{}, fmt.Errorf("%w: driver %q does not support job listing", driver.ErrUnsupported, m.driver.Name())
	}

	exec := m.driver.Executor()
	now := m.clock.Now()
	var result PruneResult
	var errs []error
	for _, item := range []struct {
		state    driver.JobState
		duration time.Duration
		results  time.Duration
	}{
		{driver.JobStateCompleted, policy.Completed, policy.Results},
		{driver.JobStateDiscarded, policy.Discarded, 0},
		{driver.JobStateCancelled, policy.Cancelled, 0},
	} {
		if item.duration == 0 && item.results == 0 {
			continue
		}
		cursor := ""
		for {
			rows, err := admin.JobList(ctx, driver.JobListParams{State: item.state, Cursor: cursor, Limit: maintenancePageSize})
			if err != nil {
				return result, fmt.Errorf("list %s jobs for retention: %w", item.state, err)
			}
			if len(rows) == 0 {
				break
			}
			for _, row := range rows {
				result.Scanned++
				if row.FinalizedAt == nil {
					continue
				}
				age := now.Sub(*row.FinalizedAt)
				switch {
				case item.duration > 0 && age >= item.duration:
					if err := exec.JobDelete(ctx, row.ID); err != nil {
						errs = append(errs, fmt.Errorf("delete job %s: %w", row.ID, err))
						continue
					}
					result.Deleted++
				case item.results > 0 && age >= item.results:
					deleted, err := deleteResult(ctx, exec, row.ID)
					if err != nil {
						errs = append(errs, fmt.Errorf("delete result of job %s: %w", row.ID, err))
						continue
					}
					if deleted {
						result.ResultsDeleted++
					}
				}
			}
			if len(rows) < maintenancePageSize {
				break
			}
			cursor = driver.EncodeJobCursor(rows[len(rows)-1])
		}
	}
	return result, errors.Join(errs...)
}

// pruneIndexed applies policy through a driver index, page by page, so the
// cost follows the number of expired jobs rather than all retained ones.
func (m *Maintenance[TTx]) pruneIndexed(ctx context.Context, pruner driver.JobPruner, policy RetentionPolicy) (PruneResult, error) {
	now := m.clock.Now()
	var result PruneResult
	drain := func(prune func(context.Context, driver.JobPruneParams) (int, error), params driver.JobPruneParams, counter *int) error {
		params.Limit = maintenancePageSize
		for {
			n, err := prune(ctx, params)
			*counter += n
			result.Scanned += n
			if err != nil {
				return err
			}
			if n < params.Limit {
				return nil
			}
		}
	}
	for _, item := range []struct {
		state    driver.JobState
		duration time.Duration
	}{
		{driver.JobStateCompleted, policy.Completed},
		{driver.JobStateDiscarded, policy.Discarded},
		{driver.JobStateCancelled, policy.Cancelled},
	} {
		if item.duration == 0 {
			continue
		}
		params := driver.JobPruneParams{State: item.state, Before: now.Add(-item.duration)}
		if err := drain(pruner.JobPruneFinalized, params, &result.Deleted); err != nil {
			return result, fmt.Errorf("prune %s jobs: %w", item.state, err)
		}
	}
	if policy.Results > 0 {
		params := driver.JobPruneParams{State: driver.JobStateCompleted, Before: now.Add(-policy.Results)}
		if err := drain(pruner.JobResultPrune, params, &result.ResultsDeleted); err != nil {
			return result, fmt.Errorf("prune results: %w", err)
		}
	}
	return result, nil
}

// deleteResult removes a stored result and reports whether one existed, so
// repeated passes do not count results that are already gone.
func deleteResult(ctx context.Context, store driver.JobResultStore, jobID string) (bool, error) {
	if _, err := store.JobResultGet(ctx, jobID); err != nil {
		if errors.Is(err, driver.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, store.JobResultDelete(ctx, jobID)
}

// RetentionRunConfig configures Maintenance.RunRetention.
type RetentionRunConfig struct {
	// Policy is applied on every pass.
	Policy RetentionPolicy
	// Interval between passes. Default: one hour.
	Interval time.Duration
	// LeaderName identifies the retention lease. Default: "goncordia-retention".
	LeaderName string
	// WorkerID identifies this instance in the lease. Default: random.
	WorkerID string
	// LeaderTTL bounds how long a crashed leader blocks others. Default: twice
	// Interval, so a live leader renews before the lease expires.
	LeaderTTL time.Duration
	// OnPrune receives the outcome of every pass this instance runs as
	// leader, except a pass cut short because ctx ended, which is neither
	// reported here nor to Observer. Default: log errors with slog.
	OnPrune func(PruneResult, error)
	// Observer also receives every pass, with its timing, for metrics such
	// as RetentionMetrics.
	Observer RetentionObserver
}

// RunRetention prunes on a schedule until ctx ends. Run it in every instance:
// each pass first takes a leader lease, so only one instance prunes at a
// time, and another takes over if the leader stops. The first pass runs
// immediately. It returns ctx's error, or a configuration error at once.
func (m *Maintenance[TTx]) RunRetention(ctx context.Context, cfg RetentionRunConfig) error {
	if _, err := cfg.Policy.normalize(); err != nil {
		return err
	}
	if cfg.Interval < 0 || cfg.LeaderTTL < 0 {
		return fmt.Errorf("retention interval and leader TTL must not be negative")
	}
	if cfg.Interval == 0 {
		cfg.Interval = time.Hour
	}
	if cfg.LeaderName == "" {
		cfg.LeaderName = "goncordia-retention"
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = newWorkerID()
	}
	if cfg.LeaderTTL == 0 {
		cfg.LeaderTTL = 2 * cfg.Interval
	}
	if cfg.OnPrune == nil {
		cfg.OnPrune = func(_ PruneResult, err error) {
			if err != nil {
				slog.Error("goncordia retention error", "error", err)
			}
		}
	}
	exec := m.driver.Executor()
	defer func() {
		resignCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = exec.LeaderResign(resignCtx, driver.LeaderResignParams{Name: cfg.LeaderName, WorkerID: cfg.WorkerID})
	}()

	pass := func() {
		elected, err := exec.LeaderAttemptElect(ctx, driver.LeaderElectParams{
			Name: cfg.LeaderName, WorkerID: cfg.WorkerID, TTL: cfg.LeaderTTL,
		})
		if err != nil {
			if ctx.Err() == nil {
				cfg.OnPrune(PruneResult{}, fmt.Errorf("retention leader election: %w", err))
			}
			return
		}
		if !elected {
			return
		}
		started := m.clock.Now()
		result, err := m.Prune(ctx, cfg.Policy)
		if err != nil && ctx.Err() != nil {
			// Stopped mid-pass: not a failure, and the next leader resumes.
			return
		}
		cfg.OnPrune(result, err)
		if cfg.Observer != nil {
			cfg.Observer.RetentionPassed(ctx, RetentionPassEvent{
				Result: result, Err: err, StartedAt: started, FinishedAt: m.clock.Now(),
			})
		}
	}
	ticker := m.clock.NewTicker(cfg.Interval)
	defer ticker.Stop()
	pass()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C():
			pass()
		}
	}
}

// BulkRetry reschedules jobs at runAt. A zero runAt uses the injected clock.
func (m *Maintenance[TTx]) BulkRetry(ctx context.Context, ids []string, runAt time.Time) (BulkResult, error) {
	if runAt.IsZero() {
		runAt = m.clock.Now()
	}
	return m.bulk(ctx, ids, func(ctx context.Context, id string) error {
		return m.driver.Executor().JobReschedule(ctx, driver.RescheduleParams{ID: id, RunAt: runAt})
	})
}

// BulkCancel cancels available or scheduled jobs.
func (m *Maintenance[TTx]) BulkCancel(ctx context.Context, ids []string) (BulkResult, error) {
	return m.bulk(ctx, ids, m.driver.Executor().JobCancel)
}

// BulkDelete permanently removes jobs and releases durable uniqueness keys.
func (m *Maintenance[TTx]) BulkDelete(ctx context.Context, ids []string) (BulkResult, error) {
	return m.bulk(ctx, ids, m.driver.Executor().JobDelete)
}

// DeadLetterList returns discarded jobs using the standard opaque cursor.
func (m *Maintenance[TTx]) DeadLetterList(ctx context.Context, params driver.JobListParams) (driver.JobPage, error) {
	admin, ok := m.driver.Executor().(driver.AdminExecutor)
	if !ok {
		return driver.JobPage{}, fmt.Errorf("%w: driver %q does not support job listing", driver.ErrUnsupported, m.driver.Name())
	}
	params.State = driver.JobStateDiscarded
	limit := params.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	params.Limit = limit
	rows, err := admin.JobList(ctx, params)
	if err != nil {
		return driver.JobPage{}, err
	}
	if rows == nil {
		rows = []driver.JobRow{}
	}
	page := driver.JobPage{Items: rows}
	if len(rows) == limit && len(rows) > 0 {
		candidate := driver.EncodeJobCursor(rows[len(rows)-1])
		params.Cursor, params.Limit = candidate, 1
		following, probeErr := admin.JobList(ctx, params)
		if probeErr != nil {
			return driver.JobPage{}, probeErr
		}
		if len(following) > 0 {
			page.NextCursor, page.HasMore = candidate, true
		}
	}
	return page, nil
}

// DeadLetterReplay reschedules discarded jobs after verifying their state.
func (m *Maintenance[TTx]) DeadLetterReplay(ctx context.Context, ids []string, runAt time.Time) (BulkResult, error) {
	if runAt.IsZero() {
		runAt = m.clock.Now()
	}
	return m.bulk(ctx, ids, func(ctx context.Context, id string) error {
		row, err := m.driver.Executor().JobGetByID(ctx, id)
		if err != nil {
			return err
		}
		if row == nil {
			return fmt.Errorf("%w: job %q", driver.ErrNotFound, id)
		}
		if row.State != driver.JobStateDiscarded {
			return fmt.Errorf("%w: job %q is %s, not discarded", driver.ErrConflict, id, row.State)
		}
		return m.driver.Executor().JobReschedule(ctx, driver.RescheduleParams{ID: id, RunAt: runAt})
	})
}

func (m *Maintenance[TTx]) bulk(ctx context.Context, ids []string, action func(context.Context, string) error) (BulkResult, error) {
	result := BulkResult{Succeeded: make([]string, 0, len(ids)), Failed: []BulkFailure{}}
	seen := make(map[string]struct{}, len(ids))
	var errs []error
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return result, errors.Join(append(errs, err)...)
		}
		if id == "" {
			err := fmt.Errorf("job id must not be empty")
			result.Failed = append(result.Failed, BulkFailure{Error: err.Error()})
			errs = append(errs, err)
			continue
		}
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		if err := action(ctx, id); err != nil {
			result.Failed = append(result.Failed, BulkFailure{ID: id, Error: err.Error()})
			errs = append(errs, fmt.Errorf("job %s: %w", id, err))
			continue
		}
		result.Succeeded = append(result.Succeeded, id)
	}
	return result, errors.Join(errs...)
}
