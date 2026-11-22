package stdlib

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/kirimatt/goncordia/v2/driver"
)

// batchExecutor adds JobCompleteMany and JobMarkStartedMany. Postgres and
// SQLite (3.35+) use one UPDATE ... RETURNING; MySQL, which has no RETURNING,
// locks the rows in a transaction, checks the claims, and updates the ones
// that still match.
type batchExecutor struct {
	*executor
}

// JobCompleteMany completes many running jobs in one UPDATE. The per-row
// fencing and the unique-key release match jobSetStateIfRunning for
// JobStateCompleted.
func (e *batchExecutor) JobCompleteMany(ctx context.Context, params []driver.JobSetStateParams) ([]bool, error) {
	if len(params) == 0 {
		return []bool{}, nil
	}
	if e.dialect == MySQL {
		claims := make([]batchClaim, len(params))
		for i, p := range params {
			claims[i] = batchClaim{id: p.ID, workerID: p.ExpectedWorkerID, attempt: p.ExpectedAttempt}
		}
		return e.mysqlBatch(ctx, claims, func(tx *sql.Tx, ids []any) error {
			args := append([]any{e.clk.Now()}, ids...)
			_, err := tx.ExecContext(ctx, `UPDATE goncordia_jobs SET
    state            = 'completed',
    worker_id        = NULL,
    lease_expires_at = NULL,
    finalized_at     = ?,
    unique_key       = CASE WHEN unique_key NOT LIKE 'uf2_%' THEN NULL ELSE unique_key END
WHERE id IN (`+repeatIN(len(ids))+`)`, args...)
			return err
		}, "complete jobs")
	}
	ids := make([]int64, len(params))
	args := make([]any, 0, 3*len(params)+1)
	var values strings.Builder
	for i, p := range params {
		id, err := strconv.ParseInt(p.ID, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid job id %q: %w", p.ID, err)
		}
		ids[i] = id
		if i > 0 {
			values.WriteString(", ")
		}
		values.WriteString("(CAST(? AS BIGINT), CAST(? AS TEXT), CAST(? AS INTEGER))")
		args = append(args, id, p.ExpectedWorkerID, p.ExpectedAttempt)
	}
	args = append(args, e.clk.Now())
	query := `WITH c(id, worker_id, attempt) AS (VALUES ` + values.String() + `)
UPDATE goncordia_jobs SET
    state            = 'completed',
    worker_id        = NULL,
    lease_expires_at = NULL,
    finalized_at     = ?,
    unique_key       = CASE WHEN unique_key NOT LIKE 'uf2_%' THEN NULL ELSE unique_key END
FROM c
WHERE goncordia_jobs.id = c.id AND goncordia_jobs.state = 'running'
  AND (c.worker_id = '' OR goncordia_jobs.worker_id = c.worker_id)
  AND (c.attempt <= 0 OR goncordia_jobs.attempt_num = c.attempt)
RETURNING goncordia_jobs.id`
	rows, err := e.db.QueryContext(ctx, e.dialect.q(query), args...)
	if err != nil {
		return nil, fmt.Errorf("complete jobs: %w", err)
	}
	return matchReturnedIDs(rows, ids, "complete jobs")
}

// JobMarkStartedMany persists the start instant of many running jobs; see
// driver.JobBatchStartMarker.
func (e *batchExecutor) JobMarkStartedMany(ctx context.Context, params []driver.JobMarkStartedParams) ([]bool, error) {
	if len(params) == 0 {
		return []bool{}, nil
	}
	if e.dialect == MySQL {
		claims := make([]batchClaim, len(params))
		at := make(map[int64]time.Time, len(params))
		for i, p := range params {
			claims[i] = batchClaim{id: p.ID, workerID: p.WorkerID, attempt: p.Attempt, strict: true}
			if id, err := strconv.ParseInt(p.ID, 10, 64); err == nil {
				at[id] = p.At.UTC()
			}
		}
		return e.mysqlBatch(ctx, claims, func(tx *sql.Tx, ids []any) error {
			var cases strings.Builder
			args := make([]any, 0, 3*len(ids))
			for _, id := range ids {
				cases.WriteString(" WHEN ? THEN ?")
				args = append(args, id, at[id.(int64)])
			}
			args = append(args, ids...)
			_, err := tx.ExecContext(ctx, `UPDATE goncordia_jobs SET started_at = CASE id`+cases.String()+` END
WHERE id IN (`+repeatIN(len(ids))+`)`, args...)
			return err
		}, "mark jobs started")
	}
	ids := make([]int64, len(params))
	args := make([]any, 0, 4*len(params))
	atCast := "CAST(? AS TIMESTAMPTZ)"
	if e.dialect == SQLite {
		atCast = "?" // keep the driver's own time encoding
	}
	var values strings.Builder
	for i, p := range params {
		id, err := strconv.ParseInt(p.ID, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid job id %q: %w", p.ID, err)
		}
		ids[i] = id
		if i > 0 {
			values.WriteString(", ")
		}
		values.WriteString("(CAST(? AS BIGINT), CAST(? AS TEXT), CAST(? AS INTEGER), " + atCast + ")")
		args = append(args, id, p.WorkerID, p.Attempt, p.At.UTC())
	}
	query := `WITH c(id, worker_id, attempt, at) AS (VALUES ` + values.String() + `)
UPDATE goncordia_jobs SET started_at = c.at
FROM c
WHERE goncordia_jobs.id = c.id AND goncordia_jobs.state = 'running'
  AND goncordia_jobs.worker_id = c.worker_id AND goncordia_jobs.attempt_num = c.attempt
RETURNING goncordia_jobs.id`
	rows, err := e.db.QueryContext(ctx, e.dialect.q(query), args...)
	if err != nil {
		return nil, fmt.Errorf("mark jobs started: %w", err)
	}
	return matchReturnedIDs(rows, ids, "mark jobs started")
}

// batchClaim is one fencing precondition. Without strict, an empty workerID
// or a non-positive attempt matches any value, like JobSetStateIfRunning.
type batchClaim struct {
	id       string
	workerID string
	attempt  int
	strict   bool
}

// mysqlBatch locks the claimed rows, keeps those whose claim still matches,
// and lets update write them, all in one transaction.
func (e *batchExecutor) mysqlBatch(ctx context.Context, claims []batchClaim, update func(*sql.Tx, []any) error, op string) ([]bool, error) {
	ids := make([]any, len(claims))
	for i, c := range claims {
		id, err := strconv.ParseInt(c.id, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid job id %q: %w", c.id, err)
		}
		ids[i] = id
	}
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	defer func() { _ = tx.Rollback() }()
	rows, err := tx.QueryContext(ctx, `SELECT id, COALESCE(worker_id, ''), attempt_num FROM goncordia_jobs
WHERE id IN (`+repeatIN(len(ids))+`) AND state = 'running' FOR UPDATE`, ids...)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	type current struct {
		workerID string
		attempt  int
	}
	running := make(map[int64]current, len(ids))
	for rows.Next() {
		var id int64
		var c current
		if err := rows.Scan(&id, &c.workerID, &c.attempt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		running[id] = c
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	out := make([]bool, len(claims))
	var matched []any
	for i, c := range claims {
		cur, ok := running[ids[i].(int64)]
		if !ok {
			continue
		}
		workerOK := cur.workerID == c.workerID || (!c.strict && c.workerID == "")
		attemptOK := cur.attempt == c.attempt || (!c.strict && c.attempt <= 0)
		if workerOK && attemptOK {
			out[i] = true
			matched = append(matched, ids[i])
		}
	}
	if len(matched) > 0 {
		if err := update(tx, matched); err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	return out, nil
}

// matchReturnedIDs maps the ids an UPDATE ... RETURNING id produced back onto
// the request order.
func matchReturnedIDs(rows *sql.Rows, ids []int64, op string) ([]bool, error) {
	defer rows.Close()
	done := make(map[int64]bool, len(ids))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		done[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}
	out := make([]bool, len(ids))
	for i, id := range ids {
		out[i] = done[id]
	}
	return out, nil
}
