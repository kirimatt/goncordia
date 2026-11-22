package stdlib

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/kirimatt/goncordia/v2/driver"
)

// batchExecutor adds JobCompleteMany for dialects whose UPDATE supports
// RETURNING (Postgres, SQLite 3.35+).
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
	defer rows.Close()
	done := make(map[int64]bool, len(params))
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("complete jobs: %w", err)
		}
		done[id] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("complete jobs: %w", err)
	}
	out := make([]bool, len(params))
	for i, id := range ids {
		out[i] = done[id]
	}
	return out, nil
}
