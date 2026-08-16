package pgxv5

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/driver"
)

// querier is the common interface satisfied by both *pgxpool.Pool and pgx.Tx.
type querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn interface{ RowsAffected() int64 }, err error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// poolQuerier wraps *pgxpool.Pool to satisfy querier.
type poolQuerier struct{ p *pgxpool.Pool }

func (q poolQuerier) Exec(ctx context.Context, sql string, args ...any) (interface{ RowsAffected() int64 }, error) {
	tag, err := q.p.Exec(ctx, sql, args...)
	return tag, err
}
func (q poolQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return q.p.Query(ctx, sql, args...)
}
func (q poolQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return q.p.QueryRow(ctx, sql, args...)
}

// txQuerier wraps pgx.Tx to satisfy querier.
type txQuerier struct{ tx pgx.Tx }

func (q txQuerier) Exec(ctx context.Context, sql string, args ...any) (interface{ RowsAffected() int64 }, error) {
	tag, err := q.tx.Exec(ctx, sql, args...)
	return tag, err
}
func (q txQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return q.tx.Query(ctx, sql, args...)
}
func (q txQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return q.tx.QueryRow(ctx, sql, args...)
}

// executor is the non-transactional executor (uses pool).
type executor struct {
	pool *pgxpool.Pool
	clk  clock.Clock
}

func (e *executor) Begin(ctx context.Context) (driver.ExecutorTx, error) {
	tx, err := e.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &txExecutor{querier: tx, clk: e.clk}, nil
}

func (e *executor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertMany(ctx, poolQuerier{e.pool}, e.clk, params)
}
func (e *executor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	return jobGetByID(ctx, poolQuerier{e.pool}, id)
}
func (e *executor) JobList(ctx context.Context, params driver.JobListParams) ([]driver.JobRow, error) {
	return jobList(ctx, poolQuerier{e.pool}, params)
}
func (e *executor) QueueStats(ctx context.Context, queue string) (driver.QueueStats, error) {
	return queueStats(ctx, poolQuerier{e.pool}, queue)
}
func (e *executor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	return jobFetchBatch(ctx, poolQuerier{e.pool}, e.clk, params)
}
func (e *executor) JobRescueStuck(ctx context.Context, params driver.JobRescueParams) (int64, error) {
	return jobRescueStuck(ctx, poolQuerier{e.pool}, params)
}
func (e *executor) JobHeartbeat(ctx context.Context, params driver.JobHeartbeatParams) (bool, error) {
	return jobHeartbeat(ctx, poolQuerier{e.pool}, params)
}
func (e *executor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(ctx, poolQuerier{e.pool}, e.clk, params)
}
func (e *executor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(ctx, poolQuerier{e.pool}, e.clk, id)
}
func (e *executor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(ctx, poolQuerier{e.pool}, id)
}
func (e *executor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(ctx, poolQuerier{e.pool}, params)
}
func (e *executor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(ctx, poolQuerier{e.pool}, name)
}
func (e *executor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(ctx, poolQuerier{e.pool}, e.clk, name, true)
}
func (e *executor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(ctx, poolQuerier{e.pool}, e.clk, name, false)
}
func (e *executor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(ctx, poolQuerier{e.pool}, params)
}
func (e *executor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(ctx, poolQuerier{e.pool}, e.clk, params)
}
func (e *executor) LeaderResign(ctx context.Context, params driver.LeaderResignParams) error {
	return leaderResign(ctx, poolQuerier{e.pool}, params)
}
func (e *executor) ScheduleCursorGetOrCreate(ctx context.Context, params driver.ScheduleCursorCreateParams) (driver.ScheduleCursorResult, error) {
	return scheduleCursorGetOrCreate(ctx, poolQuerier{e.pool}, params)
}
func (e *executor) ScheduleCursorAdvance(ctx context.Context, params driver.ScheduleCursorAdvanceParams) (bool, error) {
	return scheduleCursorAdvance(ctx, poolQuerier{e.pool}, params)
}

// txExecutor is the transactional executor (wraps pgx.Tx).
type txExecutor struct {
	querier pgx.Tx
	clk     clock.Clock
}

func (t *txExecutor) Commit(ctx context.Context) error   { return t.querier.Commit(ctx) }
func (t *txExecutor) Rollback(ctx context.Context) error { return t.querier.Rollback(ctx) }

func (t *txExecutor) Begin(ctx context.Context) (driver.ExecutorTx, error) {
	// Nested tx via savepoint
	tx, err := t.querier.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &txExecutor{querier: tx, clk: t.clk}, nil
}

func (t *txExecutor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertMany(ctx, txQuerier{t.querier}, t.clk, params)
}
func (t *txExecutor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	return jobGetByID(ctx, txQuerier{t.querier}, id)
}
func (t *txExecutor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	return jobFetchBatch(ctx, txQuerier{t.querier}, t.clk, params)
}
func (t *txExecutor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(ctx, txQuerier{t.querier}, t.clk, params)
}
func (t *txExecutor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(ctx, txQuerier{t.querier}, t.clk, id)
}
func (t *txExecutor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(ctx, txQuerier{t.querier}, id)
}
func (t *txExecutor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(ctx, txQuerier{t.querier}, params)
}
func (t *txExecutor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(ctx, txQuerier{t.querier}, name)
}
func (t *txExecutor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(ctx, txQuerier{t.querier}, t.clk, name, true)
}
func (t *txExecutor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(ctx, txQuerier{t.querier}, t.clk, name, false)
}
func (t *txExecutor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(ctx, txQuerier{t.querier}, params)
}
func (t *txExecutor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(ctx, txQuerier{t.querier}, t.clk, params)
}
func (t *txExecutor) LeaderResign(ctx context.Context, params driver.LeaderResignParams) error {
	return leaderResign(ctx, txQuerier{t.querier}, params)
}
func (t *txExecutor) ScheduleCursorGetOrCreate(ctx context.Context, params driver.ScheduleCursorCreateParams) (driver.ScheduleCursorResult, error) {
	return scheduleCursorGetOrCreate(ctx, txQuerier{t.querier}, params)
}
func (t *txExecutor) ScheduleCursorAdvance(ctx context.Context, params driver.ScheduleCursorAdvanceParams) (bool, error) {
	return scheduleCursorAdvance(ctx, txQuerier{t.querier}, params)
}

// --- SQL implementations ---

func jobInsertMany(ctx context.Context, q querier, clk clock.Clock, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	results := make([]driver.JobInsertResult, 0, len(params))
	now := clk.Now()

	for _, p := range params {
		runAt := p.RunAt
		if runAt.IsZero() {
			runAt = now
		}
		state := driver.JobStateAvailable
		if runAt.After(now) {
			state = driver.JobStateScheduled
		}

		tags := p.Tags
		if tags == nil {
			tags = []string{}
		}

		var uniqueKey *string
		if p.UniqueKey != "" {
			uniqueKey = &p.UniqueKey
		}

		const insertSQL = `
INSERT INTO goncordia_jobs
    (queue, kind, args, state, priority, run_at, created_at, max_retry, timeout_ms, unique_key, tags, pipeline_id)
VALUES
    ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL
DO NOTHING
RETURNING id, queue, kind, args, state, priority, run_at, created_at,
          attempted_at, finalized_at, attempt_num, max_retry, timeout_ms,
          unique_key, worker_id, tags, errors, pipeline_id`

		row := q.QueryRow(ctx, insertSQL,
			p.Queue, p.Kind, p.Args, string(state), p.Priority, runAt, now,
			p.MaxRetry, p.Timeout.Milliseconds(),
			uniqueKey, tags, p.PipelineID,
		)

		jobRow, err := scanJobRow(row)
		if err != nil {
			if isNoRows(err) {
				// Duplicate — find the existing job
				existing, ferr := findUniqueJob(ctx, q, p.UniqueKey)
				if ferr != nil {
					return nil, ferr
				}
				results = append(results, driver.JobInsertResult{Job: existing, UniqueSkip: true})
				continue
			}
			return nil, fmt.Errorf("insert job: %w", err)
		}
		results = append(results, driver.JobInsertResult{Job: jobRow})
	}
	return results, nil
}

func findUniqueJob(ctx context.Context, q querier, uniqueKey string) (*driver.JobRow, error) {
	const sql = `
SELECT id, queue, kind, args, state, priority, run_at, created_at,
       attempted_at, finalized_at, attempt_num, max_retry, timeout_ms,
       unique_key, worker_id, tags, errors, pipeline_id
FROM goncordia_jobs
WHERE unique_key = $1
LIMIT 1`
	return scanJobRow(q.QueryRow(ctx, sql, uniqueKey))
}

func jobGetByID(ctx context.Context, q querier, id string) (*driver.JobRow, error) {
	idInt, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid job id %q: %w", id, err)
	}
	const sql = `
SELECT id, queue, kind, args, state, priority, run_at, created_at,
       attempted_at, finalized_at, attempt_num, max_retry, timeout_ms,
       unique_key, worker_id, tags, errors, pipeline_id
FROM goncordia_jobs WHERE id = $1`
	row, err := scanJobRow(q.QueryRow(ctx, sql, idInt))
	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("%w: job %q", driver.ErrNotFound, id)
	}
	return row, err
}

func jobList(ctx context.Context, q querier, params driver.JobListParams) ([]driver.JobRow, error) {
	query := `SELECT id, queue, kind, args, state, priority, run_at, created_at,
       attempted_at, finalized_at, attempt_num, max_retry, timeout_ms,
       unique_key, worker_id, tags, errors, pipeline_id
FROM goncordia_jobs WHERE TRUE`
	args := make([]any, 0, 5)
	add := func(clause string, value any) {
		args = append(args, value)
		query += fmt.Sprintf(clause, len(args))
	}
	if params.Queue != "" {
		add(" AND queue=$%d", params.Queue)
	}
	if params.State != "" {
		add(" AND state=$%d", string(params.State))
	}
	if params.Kind != "" {
		add(" AND kind=$%d", params.Kind)
	}
	if params.Cursor != "" {
		cursorAt, cursorID, err := driver.DecodeJobCursor(params.Cursor)
		if err != nil {
			return nil, err
		}
		id, err := strconv.ParseInt(cursorID, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid postgres job id", driver.ErrInvalidCursor)
		}
		args = append(args, cursorAt, id)
		query += fmt.Sprintf(" AND (created_at<$%d OR (created_at=$%d AND id<$%d))", len(args)-1, len(args)-1, len(args))
	}
	limit := params.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	args = append(args, limit)
	query += fmt.Sprintf(" ORDER BY created_at DESC, id DESC LIMIT $%d", len(args))
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobRows(rows)
}

func queueStats(ctx context.Context, q querier, queue string) (driver.QueueStats, error) {
	rows, err := q.Query(ctx, `SELECT state, COUNT(*) FROM goncordia_jobs WHERE queue=$1 GROUP BY state`, queue)
	if err != nil {
		return driver.QueueStats{}, err
	}
	defer rows.Close()
	stats := driver.QueueStats{Queue: queue, States: make(map[driver.JobState]int64)}
	for rows.Next() {
		var state driver.JobState
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			return driver.QueueStats{}, err
		}
		stats.States[state] = count
		stats.Total += count
	}
	return stats, rows.Err()
}

func jobFetchBatch(ctx context.Context, q querier, clk clock.Clock, params driver.FetchParams) ([]driver.JobRow, error) {
	if params.Limit <= 0 {
		params.Limit = 1
	}
	const sql = `
WITH fetched AS (
    SELECT id FROM goncordia_jobs
    WHERE queue = $1
      AND state IN ('available', 'scheduled')
      AND run_at <= $2
    ORDER BY priority DESC, run_at, created_at, id
    LIMIT $3
    FOR UPDATE SKIP LOCKED
)
UPDATE goncordia_jobs j
SET state       = 'running',
    attempted_at = $4,
    attempt_num  = attempt_num + 1,
    worker_id    = $5
FROM fetched
WHERE j.id = fetched.id
RETURNING j.id, j.queue, j.kind, j.args, j.state, j.priority, j.run_at,
          j.created_at, j.attempted_at, j.finalized_at, j.attempt_num,
          j.max_retry, j.timeout_ms, j.unique_key, j.worker_id, j.tags, j.errors, j.pipeline_id`

	now := clk.Now()
	rows, err := q.Query(ctx, sql, params.Queue, now, params.Limit, now, params.WorkerID)
	if err != nil {
		return nil, fmt.Errorf("fetch batch: %w", err)
	}
	defer rows.Close()
	return scanJobRows(rows)
}

func jobRescueStuck(ctx context.Context, q querier, params driver.JobRescueParams) (int64, error) {
	const sql = `
UPDATE goncordia_jobs SET
    state = 'available', attempted_at = NULL, worker_id = NULL
WHERE queue = $1 AND state = 'running' AND attempted_at <= $2`
	tag, err := q.Exec(ctx, sql, params.Queue, params.Before)
	if err != nil {
		return 0, fmt.Errorf("rescue stuck jobs: %w", err)
	}
	return tag.RowsAffected(), nil
}

func jobHeartbeat(ctx context.Context, q querier, params driver.JobHeartbeatParams) (bool, error) {
	tag, err := q.Exec(ctx, `UPDATE goncordia_jobs SET attempted_at = $1
WHERE id = $2 AND state = 'running' AND worker_id = $3 AND attempt_num = $4`,
		params.At.UTC(), params.ID, params.WorkerID, params.Attempt)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func jobSetStateIfRunning(ctx context.Context, q querier, clk clock.Clock, params driver.JobSetStateParams) error {
	idInt, err := strconv.ParseInt(params.ID, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid job id %q: %w", params.ID, err)
	}

	if params.Yield {
		const yieldSQL = `
UPDATE goncordia_jobs SET
    state        = 'available',
    attempt_num  = attempt_num - 1,
    attempted_at = NULL,
    worker_id    = NULL
WHERE id = $1 AND state = 'running'
  AND ($2 = '' OR worker_id = $2)
  AND ($3 = 0 OR attempt_num = $3)`
		tag, err := q.Exec(ctx, yieldSQL, idInt, params.ExpectedWorkerID, params.ExpectedAttempt)
		if err == nil && tag.RowsAffected() == 0 {
			return fmt.Errorf("%w: job %q", driver.ErrStaleClaim, params.ID)
		}
		return err
	}

	errJSON := encodeError(params.Err, params.Trace, params.Attempt, clk)

	var finalizedAt *time.Time
	terminal := false
	switch params.State {
	case driver.JobStateCompleted, driver.JobStateDiscarded, driver.JobStateCancelled:
		t := clk.Now()
		finalizedAt = &t
		terminal = true
	}

	var retryAt *time.Time
	if params.State == driver.JobStateRetryable && !params.RetryAt.IsZero() {
		retryAt = &params.RetryAt
	}

	// For retryable → flip back to available with future run_at
	targetState := string(params.State)
	if params.State == driver.JobStateRetryable {
		targetState = string(driver.JobStateAvailable)
	}

	const sql = `
UPDATE goncordia_jobs SET
    state        = $2,
    worker_id    = NULL,
    finalized_at = COALESCE($3, finalized_at),
    run_at       = COALESCE($4, run_at),
    unique_key   = CASE WHEN $8 AND unique_key NOT LIKE 'uf2_%' THEN NULL ELSE unique_key END,
    errors       = CASE WHEN $5::jsonb IS NOT NULL
                        THEN errors || $5::jsonb
                        ELSE errors END
WHERE id = $1 AND state = 'running'
  AND ($6 = '' OR worker_id = $6)
  AND ($7 = 0 OR attempt_num = $7)`

	tag, err := q.Exec(ctx, sql, idInt, targetState, finalizedAt, retryAt, errJSON, params.ExpectedWorkerID, params.ExpectedAttempt, terminal)
	if err == nil && tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: job %q", driver.ErrStaleClaim, params.ID)
	}
	return err
}

func jobCancel(ctx context.Context, q querier, clk clock.Clock, id string) error {
	idInt, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid job id %q: %w", id, err)
	}
	now := clk.Now()
	const sql = `
UPDATE goncordia_jobs
SET state = 'cancelled', finalized_at = $2,
    unique_key = CASE WHEN unique_key LIKE 'uf2_%' THEN unique_key ELSE NULL END
WHERE id = $1 AND state IN ('available', 'scheduled')`
	tag, err := q.Exec(ctx, sql, idInt, now)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		if _, getErr := jobGetByID(ctx, q, id); getErr != nil {
			return getErr
		}
		return fmt.Errorf("%w: job %q cannot be cancelled in its current state", driver.ErrConflict, id)
	}
	return nil
}

func jobDelete(ctx context.Context, q querier, id string) error {
	idInt, err := strconv.ParseInt(id, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid job id %q: %w", id, err)
	}
	tag, err := q.Exec(ctx, `DELETE FROM goncordia_jobs WHERE id = $1`, idInt)
	if err == nil && tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: job %q", driver.ErrNotFound, id)
	}
	return err
}

func jobReschedule(ctx context.Context, q querier, params driver.RescheduleParams) error {
	idInt, err := strconv.ParseInt(params.ID, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid job id %q: %w", params.ID, err)
	}
	const sql = `UPDATE goncordia_jobs SET state = 'scheduled', run_at = $2 WHERE id = $1`
	tag, err := q.Exec(ctx, sql, idInt, params.RunAt)
	if err == nil && tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: job %q", driver.ErrNotFound, params.ID)
	}
	return err
}

func queueGet(ctx context.Context, q querier, name string) (*driver.QueueRow, error) {
	const sql = `SELECT name, paused, created_at, updated_at FROM goncordia_queues WHERE name = $1`
	row, err := scanQueueRow(q.QueryRow(ctx, sql, name))
	if err == pgx.ErrNoRows {
		return nil, fmt.Errorf("%w: queue %q", driver.ErrNotFound, name)
	}
	return row, err
}

func queueSetPaused(ctx context.Context, q querier, clk clock.Clock, name string, paused bool) error {
	const sql = `
INSERT INTO goncordia_queues (name, paused, created_at, updated_at)
VALUES ($1, $2, $3, $3)
ON CONFLICT (name) DO UPDATE SET paused = EXCLUDED.paused, updated_at = EXCLUDED.updated_at`
	_, err := q.Exec(ctx, sql, name, paused, clk.Now())
	return err
}

func queueList(ctx context.Context, q querier, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	limit := params.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	query := `SELECT name, paused, created_at, updated_at FROM goncordia_queues`
	var args []any
	if params.Cursor != "" {
		cursor, err := driver.DecodeQueueCursor(params.Cursor)
		if err != nil {
			return nil, err
		}
		query += ` WHERE name>$1`
		args = append(args, cursor)
	}
	query += fmt.Sprintf(` ORDER BY name LIMIT $%d`, len(args)+1)
	args = append(args, limit)
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*driver.QueueRow
	for rows.Next() {
		r, err := scanQueueRow(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func leaderAttemptElect(ctx context.Context, q querier, clk clock.Clock, params driver.LeaderElectParams) (bool, error) {
	const sql = `
INSERT INTO goncordia_leaders (name, worker_id, expires_at)
VALUES ($1, $2, $3)
ON CONFLICT (name) DO UPDATE SET
    worker_id = EXCLUDED.worker_id,
    expires_at = EXCLUDED.expires_at
WHERE goncordia_leaders.expires_at <= $4
   OR goncordia_leaders.worker_id = EXCLUDED.worker_id`
	now := clk.Now()
	tag, err := q.Exec(ctx, sql, params.Name, params.WorkerID, now.Add(params.TTL), now)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

func leaderResign(ctx context.Context, q querier, params driver.LeaderResignParams) error {
	_, err := q.Exec(ctx, `DELETE FROM goncordia_leaders WHERE name=$1 AND worker_id=$2`, params.Name, params.WorkerID)
	return err
}

func scheduleCursorGetOrCreate(ctx context.Context, q querier, params driver.ScheduleCursorCreateParams) (driver.ScheduleCursorResult, error) {
	const insertSQL = `INSERT INTO goncordia_schedule_cursors (id, cursor_at) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING`
	tag, err := q.Exec(ctx, insertSQL, params.ID, params.InitialAt.UTC())
	if err != nil {
		return driver.ScheduleCursorResult{}, err
	}
	var at time.Time
	if err := q.QueryRow(ctx, `SELECT cursor_at FROM goncordia_schedule_cursors WHERE id=$1`, params.ID).Scan(&at); err != nil {
		return driver.ScheduleCursorResult{}, err
	}
	return driver.ScheduleCursorResult{At: at.UTC(), Created: tag.RowsAffected() == 1}, nil
}

func scheduleCursorAdvance(ctx context.Context, q querier, params driver.ScheduleCursorAdvanceParams) (bool, error) {
	tag, err := q.Exec(ctx, `UPDATE goncordia_schedule_cursors SET cursor_at=$1 WHERE id=$2 AND cursor_at=$3`,
		params.Next.UTC(), params.ID, params.Expected.UTC())
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// --- scan helpers ---

type scanner interface {
	Scan(dest ...any) error
}

func scanJobRow(s scanner) (*driver.JobRow, error) {
	var (
		r          driver.JobRow
		id         int64
		state      string
		timeoutMS  int64
		uniqueKey  *string
		workerID   *string
		tags       []string
		errorsJSON []byte
	)
	err := s.Scan(
		&id, &r.Queue, &r.Kind, &r.Args, &state, &r.Priority, &r.RunAt,
		&r.CreatedAt, &r.AttemptedAt, &r.FinalizedAt, &r.AttemptNum,
		&r.MaxRetry, &timeoutMS, &uniqueKey, &workerID, &tags, &errorsJSON, &r.PipelineID,
	)
	if err != nil {
		return nil, err
	}
	r.ID = strconv.FormatInt(id, 10)
	r.State = driver.JobState(state)
	r.Timeout = time.Duration(timeoutMS) * time.Millisecond
	if uniqueKey != nil {
		r.UniqueKey = *uniqueKey
	}
	if workerID != nil {
		r.WorkerID = *workerID
	}
	r.Tags = tags
	if len(errorsJSON) > 0 && string(errorsJSON) != "[]" {
		_ = json.Unmarshal(errorsJSON, &r.Errors)
	}
	return &r, nil
}

func scanJobRows(rows pgx.Rows) ([]driver.JobRow, error) {
	var result []driver.JobRow
	for rows.Next() {
		r, err := scanJobRow(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *r)
	}
	return result, rows.Err()
}

func scanQueueRow(s scanner) (*driver.QueueRow, error) {
	var r driver.QueueRow
	err := s.Scan(&r.Name, &r.Paused, &r.CreatedAt, &r.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// encodeError serialises a single error into a JSONB array element for appending.
func encodeError(errStr, trace *string, attempt int, clk clock.Clock) []byte {
	if errStr == nil {
		return nil
	}
	entry := driver.AttemptError{At: clk.Now(), Attempt: attempt, Error: *errStr}
	if trace != nil {
		entry.Trace = *trace
	}
	b, _ := json.Marshal([]driver.AttemptError{entry})
	return b
}

func isNoRows(err error) bool {
	return err != nil && err.Error() == "no rows in result set"
}
