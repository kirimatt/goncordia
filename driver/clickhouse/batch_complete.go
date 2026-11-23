package clickhousedriver

import (
	"context"
	"fmt"
	"strings"

	"github.com/kirimatt/goncordia/v2/driver"
)

// queryJobsByID reads the current version of many jobs with one FINAL query.
func (e *executor) queryJobsByID(ctx context.Context, ids []string) (map[string]chJob, error) {
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		placeholders[i] = "?"
		args[i] = id
	}
	rows, err := e.conn.Query(ctx,
		`SELECT `+selectJobCols+` FROM goncordia_jobs FINAL WHERE id IN (`+strings.Join(placeholders, ",")+`)`,
		args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make(map[string]chJob, len(ids))
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out[j.ID] = j
	}
	return out, rows.Err()
}

// JobCompleteMany completes many running jobs with one read and one
// multi-row INSERT (one new part instead of one per job); see
// driver.JobBatchCompleter. Each job keeps the fencing of
// JobSetStateIfRunning. A job listed twice is completed once.
func (e *executor) JobCompleteMany(ctx context.Context, params []driver.JobSetStateParams) ([]bool, error) {
	ids := make([]string, len(params))
	for i, p := range params {
		ids[i] = p.ID
	}
	current, err := e.queryJobsByID(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("complete jobs: %w", err)
	}
	now := e.clk.Now()
	ok := make([]bool, len(params))
	var writes []chJob
	for i, p := range params {
		j, found := current[p.ID]
		if !found || j.State != string(driver.JobStateRunning) || !p.MatchesClaim(j.WorkerID, int(j.AttemptNum)) {
			continue
		}
		j.State = string(driver.JobStateCompleted)
		j.FinalizedAt = &now
		j.WorkerID = ""
		j.LeaseExpiresAt = nil
		j.Version++
		current[p.ID] = j // a duplicate entry now sees a completed job
		writes = append(writes, j)
		ok[i] = true
	}
	if len(writes) > 0 {
		if err := insertJobRows(ctx, e.conn, writes); err != nil {
			return nil, fmt.Errorf("complete jobs: %w", err)
		}
	}
	return ok, nil
}

// JobMarkStartedMany persists the start instant of many running jobs with
// one read and one multi-row INSERT; see driver.JobBatchStartMarker.
func (e *executor) JobMarkStartedMany(ctx context.Context, params []driver.JobMarkStartedParams) ([]bool, error) {
	ids := make([]string, len(params))
	for i, p := range params {
		ids[i] = p.ID
	}
	current, err := e.queryJobsByID(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("mark jobs started: %w", err)
	}
	ok := make([]bool, len(params))
	var writes []chJob
	for i, p := range params {
		j, found := current[p.ID]
		if !found || j.State != string(driver.JobStateRunning) || j.WorkerID != p.WorkerID || int(j.AttemptNum) != p.Attempt {
			continue
		}
		at := p.At.UTC()
		j.StartedAt = &at
		j.Version++
		current[p.ID] = j
		writes = append(writes, j)
		ok[i] = true
	}
	if len(writes) > 0 {
		if err := insertJobRows(ctx, e.conn, writes); err != nil {
			return nil, fmt.Errorf("mark jobs started: %w", err)
		}
	}
	return ok, nil
}

var (
	_ driver.JobBatchCompleter   = (*executor)(nil)
	_ driver.JobBatchStartMarker = (*executor)(nil)
)
