package stdlib

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/driver"
)

// executor is the non-transactional executor (uses *sql.DB).
type executor struct {
	db      *sql.DB
	dialect Dialect
	clk     clock.Clock
}

func (e *executor) Begin(ctx context.Context) (driver.ExecutorTx, error) {
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &txExecutor{tx: tx, dialect: e.dialect, clk: e.clk}, nil
}

func (e *executor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertMany(ctx, e.db, e.dialect, e.clk, params)
}
func (e *executor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	return jobGetByID(ctx, e.db, e.dialect, id)
}
func (e *executor) JobList(ctx context.Context, params driver.JobListParams) ([]driver.JobRow, error) {
	return jobList(ctx, e.db, e.dialect, params)
}
func (e *executor) QueueStats(ctx context.Context, queue string) (driver.QueueStats, error) {
	return queueStats(ctx, e.db, e.dialect, queue)
}
func (e *executor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	return jobFetchBatch(ctx, e.db, e.dialect, e.clk, params)
}
func (e *executor) JobRescueStuck(ctx context.Context, params driver.JobRescueParams) (int64, error) {
	return jobRescueStuck(ctx, e.db, e.dialect, params)
}
func (e *executor) JobHeartbeat(ctx context.Context, params driver.JobHeartbeatParams) (bool, error) {
	return jobHeartbeat(ctx, e.db, e.dialect, params)
}
func (e *executor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(ctx, e.db, e.dialect, e.clk, params)
}
func (e *executor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(ctx, e.db, e.dialect, e.clk, id)
}
func (e *executor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(ctx, e.db, e.dialect, id)
}
func (e *executor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(ctx, e.db, e.dialect, params)
}
func (e *executor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(ctx, e.db, e.dialect, name)
}
func (e *executor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(ctx, e.db, e.dialect, e.clk, name, true)
}
func (e *executor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(ctx, e.db, e.dialect, e.clk, name, false)
}
func (e *executor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(ctx, e.db, e.dialect, params)
}
func (e *executor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(ctx, e.db, e.dialect, e.clk, params)
}
func (e *executor) LeaderResign(ctx context.Context, params driver.LeaderResignParams) error {
	return leaderResign(ctx, e.db, e.dialect, params)
}
func (e *executor) ScheduleCursorGetOrCreate(ctx context.Context, params driver.ScheduleCursorCreateParams) (driver.ScheduleCursorResult, error) {
	return scheduleCursorGetOrCreate(ctx, e.db, e.dialect, params)
}
func (e *executor) ScheduleCursorAdvance(ctx context.Context, params driver.ScheduleCursorAdvanceParams) (bool, error) {
	return scheduleCursorAdvance(ctx, e.db, e.dialect, params)
}

// txExecutor wraps *sql.Tx.
type txExecutor struct {
	tx      *sql.Tx
	dialect Dialect
	clk     clock.Clock
}

func (t *txExecutor) Commit(ctx context.Context) error   { return t.tx.Commit() }
func (t *txExecutor) Rollback(ctx context.Context) error { return t.tx.Rollback() }

func (t *txExecutor) Begin(ctx context.Context) (driver.ExecutorTx, error) {
	if t.dialect == Postgres {
		if _, err := t.tx.ExecContext(ctx, "SAVEPOINT goncordia_sp"); err != nil {
			return nil, err
		}
		return &savepointExecutor{txExecutor: t, ctx: ctx}, nil
	}
	return t, nil
}

func (t *txExecutor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertMany(ctx, t.tx, t.dialect, t.clk, params)
}
func (t *txExecutor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	return jobGetByID(ctx, t.tx, t.dialect, id)
}
func (t *txExecutor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	return jobFetchBatch(ctx, t.tx, t.dialect, t.clk, params)
}
func (t *txExecutor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(ctx, t.tx, t.dialect, t.clk, params)
}
func (t *txExecutor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(ctx, t.tx, t.dialect, t.clk, id)
}
func (t *txExecutor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(ctx, t.tx, t.dialect, id)
}
func (t *txExecutor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(ctx, t.tx, t.dialect, params)
}
func (t *txExecutor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(ctx, t.tx, t.dialect, name)
}
func (t *txExecutor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(ctx, t.tx, t.dialect, t.clk, name, true)
}
func (t *txExecutor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(ctx, t.tx, t.dialect, t.clk, name, false)
}
func (t *txExecutor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(ctx, t.tx, t.dialect, params)
}
func (t *txExecutor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(ctx, t.tx, t.dialect, t.clk, params)
}
func (t *txExecutor) LeaderResign(ctx context.Context, params driver.LeaderResignParams) error {
	return leaderResign(ctx, t.tx, t.dialect, params)
}
func (t *txExecutor) ScheduleCursorGetOrCreate(ctx context.Context, params driver.ScheduleCursorCreateParams) (driver.ScheduleCursorResult, error) {
	return scheduleCursorGetOrCreate(ctx, t.tx, t.dialect, params)
}
func (t *txExecutor) ScheduleCursorAdvance(ctx context.Context, params driver.ScheduleCursorAdvanceParams) (bool, error) {
	return scheduleCursorAdvance(ctx, t.tx, t.dialect, params)
}

// savepointExecutor implements nested tx via SAVEPOINT for Postgres.
type savepointExecutor struct {
	*txExecutor
	ctx context.Context
}

func (s *savepointExecutor) Commit(_ context.Context) error {
	_, err := s.txExecutor.tx.ExecContext(s.ctx, "RELEASE SAVEPOINT goncordia_sp")
	return err
}
func (s *savepointExecutor) Rollback(_ context.Context) error {
	_, err := s.txExecutor.tx.ExecContext(s.ctx, "ROLLBACK TO SAVEPOINT goncordia_sp")
	return err
}

// querier is satisfied by *sql.DB, *sql.Tx, and savepointExecutor.
type querier interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// --- static SQL ---

const selectJobCols = `id, queue, kind, args, state, priority, run_at, created_at,
    attempted_at, lease_expires_at, finalized_at, attempt_num, max_retry, timeout_ms,
    unique_key, worker_id, tags, errors, pipeline_id`

const insertJobCols = `queue, kind, args, state, priority, run_at, created_at,
    max_retry, timeout_ms, unique_key, tags, errors, pipeline_id`

const insertJobVals = `?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?`

const sqlYield = `UPDATE goncordia_jobs
SET state = 'available', attempted_at = NULL, lease_expires_at = NULL, worker_id = NULL, attempt_num = attempt_num - 1
WHERE id = ? AND state = 'running'
  AND (? = '' OR worker_id = ?)
  AND (? = 0 OR attempt_num = ?)`

// Postgres uses COALESCE + JSONB concat in one statement.
const sqlSetStatePostgres = `UPDATE goncordia_jobs
SET state        = $2,
    worker_id    = NULL,
    lease_expires_at = NULL,
    finalized_at = COALESCE($3, finalized_at),
    run_at       = COALESCE($4, run_at),
    unique_key   = CASE WHEN $8 AND unique_key NOT LIKE 'uf2_%' THEN NULL ELSE unique_key END,
    errors       = CASE WHEN $5::jsonb IS NOT NULL THEN errors || $5::jsonb ELSE errors END
WHERE id = $1 AND state = 'running'
  AND ($6 = '' OR worker_id = $6)
  AND ($7 = 0 OR attempt_num = $7)`

// MySQL and SQLite share the same main-update SQL; error append differs.
const sqlSetStateMain = `UPDATE goncordia_jobs
SET state        = ?,
    worker_id    = NULL,
    lease_expires_at = NULL,
    finalized_at = COALESCE(?, finalized_at),
    run_at       = COALESCE(?, run_at),
    unique_key   = CASE WHEN ? AND unique_key NOT LIKE 'uf2_%' THEN NULL ELSE unique_key END
WHERE id = ? AND state = 'running'
  AND (? = '' OR worker_id = ?)
  AND (? = 0 OR attempt_num = ?)`

const sqlAppendErrorMySQL = `UPDATE goncordia_jobs
SET errors = JSON_ARRAY_APPEND(errors, '$', CAST(? AS JSON))
WHERE id = ? AND state = 'running'
  AND (? = '' OR worker_id = ?)
  AND (? = 0 OR attempt_num = ?)`

const sqlAppendErrorSQLite = `UPDATE goncordia_jobs
SET errors = json_insert(errors, '$[#]', json(?))
WHERE id = ? AND state = 'running'
  AND (? = '' OR worker_id = ?)
  AND (? = 0 OR attempt_num = ?)`

const sqlJobCancel = `UPDATE goncordia_jobs
SET state = 'cancelled', finalized_at = ?,
    unique_key = CASE WHEN unique_key LIKE 'uf2_%' THEN unique_key ELSE NULL END
WHERE id = ? AND state IN ('available', 'scheduled')`

const sqlJobDelete = `DELETE FROM goncordia_jobs WHERE id = ?`

const sqlJobReschedule = `UPDATE goncordia_jobs SET state = 'scheduled', run_at = ? WHERE id = ?`

const sqlQueueGet = `SELECT name, paused, created_at, updated_at FROM goncordia_queues WHERE name = ?`

const sqlQueueUpsertPostgres = `INSERT INTO goncordia_queues (name, paused, created_at, updated_at)
VALUES ($1, $2, $3, $3)
ON CONFLICT (name) DO UPDATE SET paused = EXCLUDED.paused, updated_at = EXCLUDED.updated_at`

const sqlQueueUpsertMySQL = `INSERT INTO goncordia_queues (name, paused, created_at, updated_at)
VALUES (?, ?, ?, ?)
ON DUPLICATE KEY UPDATE paused = VALUES(paused), updated_at = VALUES(updated_at)`

const sqlQueueUpsertSQLite = `INSERT INTO goncordia_queues (name, paused, created_at, updated_at)
VALUES (?, ?, ?, ?)
ON CONFLICT(name) DO UPDATE SET paused = excluded.paused, updated_at = excluded.updated_at`

// repeatIN builds a comma-separated list of count ? placeholders for IN clauses.
func repeatIN(count int) string {
	if count == 1 {
		return "?"
	}
	return "?" + strings.Repeat(", ?", count-1)
}

// --- SQL implementations ---

func jobInsertMany(ctx context.Context, q querier, d Dialect, clk clock.Clock, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
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
		tagsJSON, _ := json.Marshal(tags)

		var uniqueKey *string
		if p.UniqueKey != "" {
			uniqueKey = &p.UniqueKey
		}

		if uniqueKey != nil {
			existing, err := findUniqueJob(ctx, q, d, *uniqueKey)
			if err != nil {
				return nil, err
			}
			if existing != nil {
				results = append(results, driver.JobInsertResult{Job: existing, UniqueSkip: true})
				continue
			}
		}

		args := p.Args
		if args == nil {
			args = []byte("{}")
		}

		// database/sql sends []byte as bytea for Postgres jsonb — pass as string instead.
		var argsArg, tagsArg, errorsArg any
		if d == Postgres {
			argsArg = string(args)
			tagsArg = string(tagsJSON)
			errorsArg = "[]"
		} else {
			argsArg = args
			tagsArg = tagsJSON
			errorsArg = []byte("[]")
		}

		sqlArgs := []any{
			p.Queue, p.Kind, argsArg, string(state), p.Priority,
			runAt, now, p.MaxRetry, p.Timeout.Milliseconds(),
			uniqueKey, tagsArg, errorsArg, p.PipelineID,
		}

		var (
			row    *driver.JobRow
			rowErr error
		)
		if d == Postgres {
			insertSQL := d.q(`INSERT INTO goncordia_jobs (` + insertJobCols + `)
VALUES (` + insertJobVals + `)
ON CONFLICT (unique_key) WHERE unique_key IS NOT NULL DO NOTHING
RETURNING id`)
			var insertedID string
			if err := q.QueryRowContext(ctx, insertSQL, sqlArgs...).Scan(&insertedID); err == sql.ErrNoRows && uniqueKey != nil {
				existing, findErr := findUniqueJob(ctx, q, d, *uniqueKey)
				if findErr != nil {
					return nil, findErr
				}
				results = append(results, driver.JobInsertResult{Job: existing, UniqueSkip: true})
				continue
			} else if err != nil {
				return nil, fmt.Errorf("insert job: %w", err)
			}
			row, rowErr = jobGetByID(ctx, q, d, insertedID)
		} else {
			insertSQL := `INSERT INTO goncordia_jobs (` + insertJobCols + `) VALUES (` + insertJobVals + `)`
			res, err := q.ExecContext(ctx, insertSQL, sqlArgs...)
			if err != nil {
				if uniqueKey != nil {
					existing, findErr := findUniqueJob(ctx, q, d, *uniqueKey)
					if findErr == nil && existing != nil {
						results = append(results, driver.JobInsertResult{Job: existing, UniqueSkip: true})
						continue
					}
				}
				return nil, fmt.Errorf("insert job: %w", err)
			}
			id, err := res.LastInsertId()
			if err != nil {
				return nil, fmt.Errorf("get last insert id: %w", err)
			}
			row, rowErr = jobGetByID(ctx, q, d, strconv.FormatInt(id, 10))
		}
		if rowErr != nil {
			return nil, rowErr
		}
		results = append(results, driver.JobInsertResult{Job: row})
	}
	return results, nil
}

func findUniqueJob(ctx context.Context, q querier, d Dialect, uniqueKey string) (*driver.JobRow, error) {
	query := d.q(`SELECT ` + selectJobCols + ` FROM goncordia_jobs
WHERE unique_key = ?
LIMIT 1`)
	j, err := scanJobRow(d, q.QueryRowContext(ctx, query, uniqueKey))
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return j, err
}

func jobGetByID(ctx context.Context, q querier, d Dialect, id string) (*driver.JobRow, error) {
	query := d.q(`SELECT ` + selectJobCols + ` FROM goncordia_jobs WHERE id = ?`)
	row, err := scanJobRow(d, q.QueryRowContext(ctx, query, id))
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%w: job %q", driver.ErrNotFound, id)
	}
	return row, err
}

func jobList(ctx context.Context, q querier, d Dialect, params driver.JobListParams) ([]driver.JobRow, error) {
	query := `SELECT ` + selectJobCols + ` FROM goncordia_jobs WHERE 1=1`
	args := make([]any, 0, 5)
	if params.Queue != "" {
		query += ` AND queue=?`
		args = append(args, params.Queue)
	}
	if params.State != "" {
		query += ` AND state=?`
		args = append(args, string(params.State))
	}
	if params.Kind != "" {
		query += ` AND kind=?`
		args = append(args, params.Kind)
	}
	if params.Cursor != "" {
		cursorAt, cursorID, err := driver.DecodeJobCursor(params.Cursor)
		if err != nil {
			return nil, err
		}
		query += ` AND (created_at<? OR (created_at=? AND id<?))`
		args = append(args, cursorAt, cursorAt, cursorID)
	}
	limit := params.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	query += ` ORDER BY created_at DESC, id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := q.QueryContext(ctx, d.q(query), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobRows(d, rows)
}

func queueStats(ctx context.Context, q querier, d Dialect, queue string) (driver.QueueStats, error) {
	rows, err := q.QueryContext(ctx, d.q(`SELECT state, COUNT(*) FROM goncordia_jobs WHERE queue=? GROUP BY state`), queue)
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

func jobFetchBatch(ctx context.Context, q querier, d Dialect, clk clock.Clock, params driver.FetchParams) ([]driver.JobRow, error) {
	if params.Limit <= 0 {
		params.Limit = 1
	}
	now := clk.Now()
	if d.supportsSkipLocked() {
		return jobFetchSkipLocked(ctx, q, d, now, params)
	}
	return jobFetchSQLite(ctx, q, d, now, params)
}

func jobRescueStuck(ctx context.Context, q querier, d Dialect, params driver.JobRescueParams) (int64, error) {
	query := `UPDATE goncordia_jobs
SET state = 'available', attempted_at = NULL, lease_expires_at = NULL, worker_id = NULL
WHERE queue = ? AND state = 'running'
  AND ((lease_expires_at IS NOT NULL AND lease_expires_at <= ?)
       OR (lease_expires_at IS NULL AND attempted_at <= ?))`
	result, err := q.ExecContext(ctx, d.q(query), params.Queue, params.At, params.Before)
	if err != nil {
		return 0, fmt.Errorf("rescue stuck jobs: %w", err)
	}
	return result.RowsAffected()
}

func jobHeartbeat(ctx context.Context, q querier, d Dialect, params driver.JobHeartbeatParams) (bool, error) {
	leaseExpiresAt := params.LeaseExpiresAt
	if leaseExpiresAt.IsZero() {
		leaseExpiresAt = params.At
	}
	result, err := q.ExecContext(ctx, d.q(`UPDATE goncordia_jobs SET lease_expires_at = ?
WHERE id = ? AND state = 'running' AND worker_id = ? AND attempt_num = ?`),
		leaseExpiresAt.UTC(), params.ID, params.WorkerID, params.Attempt)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	return rows > 0, err
}

func jobFetchSkipLocked(ctx context.Context, q querier, d Dialect, now time.Time, params driver.FetchParams) ([]driver.JobRow, error) {
	selectSQL := d.q(`SELECT id FROM goncordia_jobs
WHERE queue = ?
  AND state IN ('available', 'scheduled')
  AND run_at <= ?
ORDER BY priority DESC, run_at, created_at, id
LIMIT ?
FOR UPDATE SKIP LOCKED`)
	rows, err := q.QueryContext(ctx, selectSQL, params.Queue, now, params.Limit)
	if err != nil {
		return nil, fmt.Errorf("fetch ids: %w", err)
	}
	ids, err := scanIDs(rows)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return claimJobs(ctx, q, d, now, ids, params.WorkerID, params.LeaseDuration)
}

func jobFetchSQLite(ctx context.Context, q querier, d Dialect, now time.Time, params driver.FetchParams) ([]driver.JobRow, error) {
	rows, err := q.QueryContext(ctx,
		`SELECT id FROM goncordia_jobs
WHERE queue = ?
  AND state IN ('available', 'scheduled')
  AND run_at <= ?
ORDER BY priority DESC, run_at, created_at, id
LIMIT ?`,
		params.Queue, now, params.Limit)
	if err != nil {
		return nil, err
	}
	ids, err := scanIDs(rows)
	if err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return claimJobs(ctx, q, d, now, ids, params.WorkerID, params.LeaseDuration)
}

func scanIDs(rows *sql.Rows) ([]int64, error) {
	defer rows.Close()
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func claimJobs(ctx context.Context, q querier, d Dialect, now time.Time, ids []int64, workerID string, leaseDuration time.Duration) ([]driver.JobRow, error) {
	inList := repeatIN(len(ids))
	var leaseExpiresAt *time.Time
	if leaseDuration > 0 {
		expires := now.Add(leaseDuration)
		leaseExpiresAt = &expires
	}

	updateArgs := make([]any, 0, 4+len(ids))
	updateArgs = append(updateArgs, "running", now, leaseExpiresAt, workerID)
	for _, id := range ids {
		updateArgs = append(updateArgs, id)
	}
	if _, err := q.ExecContext(ctx,
		d.q(`UPDATE goncordia_jobs
SET state = ?, attempted_at = ?, lease_expires_at = ?, attempt_num = attempt_num + 1, worker_id = ?
WHERE id IN (`+inList+`)`),
		updateArgs...,
	); err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}

	selectArgs := make([]any, len(ids))
	for i, id := range ids {
		selectArgs[i] = id
	}
	rows, err := q.QueryContext(ctx,
		d.q(`SELECT `+selectJobCols+` FROM goncordia_jobs WHERE id IN (`+inList+`)`),
		selectArgs...,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobRows(d, rows)
}

func jobSetStateIfRunning(ctx context.Context, q querier, d Dialect, clk clock.Clock, params driver.JobSetStateParams) error {
	if params.Yield {
		result, err := q.ExecContext(ctx, d.q(sqlYield), params.ID,
			params.ExpectedWorkerID, params.ExpectedWorkerID,
			params.ExpectedAttempt, params.ExpectedAttempt)
		return staleClaimIfNoRows(result, err, params.ID)
	}

	var finalizedAt *time.Time
	terminal := false
	switch params.State {
	case driver.JobStateCompleted, driver.JobStateDiscarded, driver.JobStateCancelled:
		t := clk.Now()
		finalizedAt = &t
		terminal = true
	}

	targetState := string(params.State)
	var retryAt *time.Time
	if params.State == driver.JobStateRetryable {
		targetState = string(driver.JobStateAvailable)
		if !params.RetryAt.IsZero() {
			retryAt = &params.RetryAt
		}
	}

	var errJSON *string
	if params.Err != nil {
		entry := driver.AttemptError{At: clk.Now(), Attempt: params.Attempt, Error: *params.Err}
		if params.Trace != nil {
			entry.Trace = *params.Trace
		}
		b, _ := json.Marshal(entry)
		s := string(b)
		if d == Postgres {
			s = "[" + s + "]"
		}
		errJSON = &s
	}

	if d == Postgres {
		result, err := q.ExecContext(ctx, sqlSetStatePostgres, params.ID, targetState, finalizedAt, retryAt, errJSON,
			params.ExpectedWorkerID, params.ExpectedAttempt, terminal)
		return staleClaimIfNoRows(result, err, params.ID)
	}

	if errJSON != nil {
		appendSQL := sqlAppendErrorSQLite
		if d == MySQL {
			appendSQL = sqlAppendErrorMySQL
		}
		if _, err := q.ExecContext(ctx, appendSQL, *errJSON, params.ID,
			params.ExpectedWorkerID, params.ExpectedWorkerID,
			params.ExpectedAttempt, params.ExpectedAttempt); err != nil {
			return err
		}
	}
	result, err := q.ExecContext(ctx, sqlSetStateMain, targetState, finalizedAt, retryAt, terminal, params.ID,
		params.ExpectedWorkerID, params.ExpectedWorkerID,
		params.ExpectedAttempt, params.ExpectedAttempt)
	return staleClaimIfNoRows(result, err, params.ID)
}

func staleClaimIfNoRows(result sql.Result, err error, id string) error {
	if err != nil {
		return err
	}
	if rows, rowsErr := result.RowsAffected(); rowsErr == nil && rows == 0 {
		return fmt.Errorf("%w: job %q", driver.ErrStaleClaim, id)
	}
	return nil
}

func jobCancel(ctx context.Context, q querier, d Dialect, clk clock.Clock, id string) error {
	result, err := q.ExecContext(ctx, d.q(sqlJobCancel), clk.Now(), id)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		if _, getErr := jobGetByID(ctx, q, d, id); getErr != nil {
			return getErr
		}
		return fmt.Errorf("%w: job %q cannot be cancelled in its current state", driver.ErrConflict, id)
	}
	return nil
}

func jobDelete(ctx context.Context, q querier, d Dialect, id string) error {
	result, err := q.ExecContext(ctx, d.q(sqlJobDelete), id)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		return fmt.Errorf("%w: job %q", driver.ErrNotFound, id)
	}
	return nil
}

func jobReschedule(ctx context.Context, q querier, d Dialect, params driver.RescheduleParams) error {
	result, err := q.ExecContext(ctx, d.q(sqlJobReschedule), params.RunAt, params.ID)
	if err != nil {
		return err
	}
	if rows, _ := result.RowsAffected(); rows == 0 {
		return fmt.Errorf("%w: job %q", driver.ErrNotFound, params.ID)
	}
	return nil
}

func queueGet(ctx context.Context, q querier, d Dialect, name string) (*driver.QueueRow, error) {
	row, err := scanQueueRow(q.QueryRowContext(ctx, d.q(sqlQueueGet), name))
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%w: queue %q", driver.ErrNotFound, name)
	}
	return row, err
}

func queueSetPaused(ctx context.Context, q querier, d Dialect, clk clock.Clock, name string, paused bool) error {
	now := clk.Now()
	switch d {
	case Postgres:
		_, err := q.ExecContext(ctx, sqlQueueUpsertPostgres, name, paused, now)
		return err
	case MySQL:
		_, err := q.ExecContext(ctx, sqlQueueUpsertMySQL, name, paused, now, now)
		return err
	default:
		_, err := q.ExecContext(ctx, sqlQueueUpsertSQLite, name, paused, now, now)
		return err
	}
}

func queueList(ctx context.Context, q querier, d Dialect, params driver.QueueListParams) ([]*driver.QueueRow, error) {
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
		query += ` WHERE name>?`
		args = append(args, cursor)
	}
	query += ` ORDER BY name LIMIT ?`
	args = append(args, limit)
	rows, err := q.QueryContext(ctx, d.q(query), args...)
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

func leaderAttemptElect(ctx context.Context, q querier, d Dialect, clk clock.Clock, params driver.LeaderElectParams) (bool, error) {
	now := clk.Now()
	expiresAt := now.Add(params.TTL)
	result, err := q.ExecContext(ctx, d.q(`UPDATE goncordia_leaders
SET worker_id=?, expires_at=?
WHERE name=? AND (expires_at<=? OR worker_id=?)`), params.WorkerID, expiresAt, params.Name, now, params.WorkerID)
	if err != nil {
		return false, err
	}
	if n, _ := result.RowsAffected(); n > 0 {
		return true, nil
	}
	if _, err := q.ExecContext(ctx, d.q(`INSERT INTO goncordia_leaders (name, worker_id, expires_at) VALUES (?, ?, ?)`), params.Name, params.WorkerID, expiresAt); err == nil {
		return true, nil
	} else {
		var currentWorker string
		var currentExpiry time.Time
		if scanErr := q.QueryRowContext(ctx, d.q(`SELECT worker_id, expires_at FROM goncordia_leaders WHERE name=?`), params.Name).Scan(&currentWorker, &currentExpiry); scanErr != nil {
			return false, err
		}
	}
	return false, nil
}

func leaderResign(ctx context.Context, q querier, d Dialect, params driver.LeaderResignParams) error {
	_, err := q.ExecContext(ctx, d.q(`DELETE FROM goncordia_leaders WHERE name=? AND worker_id=?`), params.Name, params.WorkerID)
	return err
}

func scheduleCursorGetOrCreate(ctx context.Context, q querier, d Dialect, params driver.ScheduleCursorCreateParams) (driver.ScheduleCursorResult, error) {
	initial := params.InitialAt.UTC()
	var result sql.Result
	var err error
	switch d {
	case Postgres:
		result, err = q.ExecContext(ctx, `INSERT INTO goncordia_schedule_cursors (id, cursor_at) VALUES ($1,$2) ON CONFLICT (id) DO NOTHING`, params.ID, initial)
	case MySQL:
		result, err = q.ExecContext(ctx, `INSERT IGNORE INTO goncordia_schedule_cursors (id, cursor_at) VALUES (?,?)`, params.ID, initial)
	default:
		result, err = q.ExecContext(ctx, `INSERT OR IGNORE INTO goncordia_schedule_cursors (id, cursor_at) VALUES (?,?)`, params.ID, initial)
	}
	if err != nil {
		return driver.ScheduleCursorResult{}, err
	}
	created, _ := result.RowsAffected()
	var at time.Time
	if err := q.QueryRowContext(ctx, d.q(`SELECT cursor_at FROM goncordia_schedule_cursors WHERE id=?`), params.ID).Scan(&at); err != nil {
		return driver.ScheduleCursorResult{}, err
	}
	return driver.ScheduleCursorResult{At: at.UTC(), Created: created == 1}, nil
}

func scheduleCursorAdvance(ctx context.Context, q querier, d Dialect, params driver.ScheduleCursorAdvanceParams) (bool, error) {
	result, err := q.ExecContext(ctx, d.q(`UPDATE goncordia_schedule_cursors SET cursor_at=? WHERE id=? AND cursor_at=?`),
		params.Next.UTC(), params.ID, params.Expected.UTC())
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	return rows == 1, err
}

// --- scan helpers ---

type rowScanner interface {
	Scan(dest ...any) error
}

func scanJobRow(d Dialect, s rowScanner) (*driver.JobRow, error) {
	var (
		r         driver.JobRow
		idStr     string
		state     string
		timeoutMS int64
		uniqueKey sql.NullString
		workerID  sql.NullString
		tagsRaw   []byte
		errorsRaw []byte
	)
	err := s.Scan(
		&idStr, &r.Queue, &r.Kind, &r.Args, &state, &r.Priority, &r.RunAt,
		&r.CreatedAt, &r.AttemptedAt, &r.LeaseExpiresAt, &r.FinalizedAt, &r.AttemptNum,
		&r.MaxRetry, &timeoutMS, &uniqueKey, &workerID, &tagsRaw, &errorsRaw, &r.PipelineID,
	)
	if err != nil {
		return nil, err
	}
	r.ID = idStr
	r.State = driver.JobState(state)
	r.Timeout = time.Duration(timeoutMS) * time.Millisecond
	if uniqueKey.Valid {
		r.UniqueKey = uniqueKey.String
	}
	if workerID.Valid {
		r.WorkerID = workerID.String
	}
	if len(tagsRaw) > 0 {
		_ = json.Unmarshal(tagsRaw, &r.Tags)
	}
	if len(errorsRaw) > 0 {
		_ = json.Unmarshal(errorsRaw, &r.Errors)
	}
	return &r, nil
}

func scanJobRows(d Dialect, rows *sql.Rows) ([]driver.JobRow, error) {
	var result []driver.JobRow
	for rows.Next() {
		r, err := scanJobRow(d, rows)
		if err != nil {
			return nil, err
		}
		result = append(result, *r)
	}
	return result, rows.Err()
}

func scanQueueRow(s rowScanner) (*driver.QueueRow, error) {
	var r driver.QueueRow
	var paused any
	if err := s.Scan(&r.Name, &paused, &r.CreatedAt, &r.UpdatedAt); err != nil {
		return nil, err
	}
	switch v := paused.(type) {
	case bool:
		r.Paused = v
	case int64:
		r.Paused = v != 0
	case []byte:
		r.Paused = len(v) > 0 && v[0] == 1
	}
	return &r, nil
}
