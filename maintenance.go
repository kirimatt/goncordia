package goncordia

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/driver"
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

// RetentionPolicy configures how long terminal jobs remain available. A zero
// duration disables pruning for that state.
type RetentionPolicy struct {
	Completed time.Duration
	Discarded time.Duration
	Cancelled time.Duration
}

// PruneResult summarizes a retention pass.
type PruneResult struct {
	Scanned int `json:"scanned"`
	Deleted int `json:"deleted"`
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

// Prune deletes terminal jobs finalized on or before each configured cutoff.
func (m *Maintenance[TTx]) Prune(ctx context.Context, policy RetentionPolicy) (PruneResult, error) {
	for _, duration := range []time.Duration{policy.Completed, policy.Discarded, policy.Cancelled} {
		if duration < 0 {
			return PruneResult{}, fmt.Errorf("retention duration must not be negative")
		}
	}
	admin, ok := m.driver.Executor().(driver.AdminExecutor)
	if !ok {
		return PruneResult{}, fmt.Errorf("%w: driver %q does not support job listing", driver.ErrUnsupported, m.driver.Name())
	}

	var result PruneResult
	var errs []error
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
		cutoff := m.clock.Now().Add(-item.duration)
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
				if row.FinalizedAt == nil || row.FinalizedAt.After(cutoff) {
					continue
				}
				if err := m.driver.Executor().JobDelete(ctx, row.ID); err != nil {
					errs = append(errs, fmt.Errorf("delete job %s: %w", row.ID, err))
					continue
				}
				result.Deleted++
			}
			if len(rows) < maintenancePageSize {
				break
			}
			cursor = driver.EncodeJobCursor(rows[len(rows)-1])
		}
	}
	return result, errors.Join(errs...)
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
