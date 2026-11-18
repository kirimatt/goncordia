package clickhousedriver

import (
	"context"
	"fmt"
	"strings"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/kirimatt/goncordia/v2/driver"
)

// Retention reads candidates through data-skipping indexes (minmax on
// finalized_at and updated_at, set on state, bloom filter on id) without
// FINAL, confirms each candidate's newest version with argMax, and removes
// every version with a lightweight DELETE. A job revived between the check
// and the DELETE is lost; like the rest of this driver, writers to one job
// are last-writer-wins.

func pruneLimit(limit int) int {
	if limit <= 0 {
		return 500
	}
	return limit
}

func inList(n int) string {
	return "(" + strings.TrimSuffix(strings.Repeat("?,", n), ",") + ")"
}

func stringArgs(ids []string) []interface{} {
	args := make([]interface{}, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	return args
}

func scanIDs(ctx context.Context, conn chdriver.Conn, query string, args ...interface{}) ([]string, error) {
	rows, err := conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// JobPruneFinalized deletes jobs of params.State finalized at or before
// params.Before. It also removes rows of jobs deleted with JobDelete, which
// only writes a "deleted" version; those are not counted.
func (e *executor) JobPruneFinalized(ctx context.Context, params driver.JobPruneParams) (int, error) {
	limit := pruneLimit(params.Limit)
	candidates, err := scanIDs(ctx, e.conn,
		`SELECT DISTINCT id FROM goncordia_jobs
		 WHERE (state = ? AND finalized_at <= ?) OR state = ?
		 LIMIT ?`,
		string(params.State), params.Before, jobStateDeleted, limit)
	if err != nil || len(candidates) == 0 {
		return 0, err
	}
	rows, err := e.conn.Query(ctx,
		`SELECT id, argMax(state, version), argMax(finalized_at, version) FROM goncordia_jobs
		 WHERE id IN `+inList(len(candidates))+` GROUP BY id`,
		stringArgs(candidates)...)
	if err != nil {
		return 0, err
	}
	var expired []string
	deleted := 0
	for rows.Next() {
		var (
			id, state   string
			finalizedAt *time.Time
		)
		if err := rows.Scan(&id, &state, &finalizedAt); err != nil {
			rows.Close()
			return 0, err
		}
		switch {
		case state == jobStateDeleted:
			expired = append(expired, id)
		case state == string(params.State) && finalizedAt != nil && !finalizedAt.After(params.Before):
			expired = append(expired, id)
			deleted++
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(expired) == 0 {
		return 0, nil
	}
	args := stringArgs(expired)
	if err := e.conn.Exec(ctx, `DELETE FROM goncordia_job_results WHERE job_id IN `+inList(len(expired)), args...); err != nil {
		return 0, fmt.Errorf("delete results: %w", err)
	}
	if err := e.conn.Exec(ctx, `DELETE FROM goncordia_jobs WHERE id IN `+inList(len(expired)), args...); err != nil {
		return 0, fmt.Errorf("delete jobs: %w", err)
	}
	return deleted, nil
}

// JobResultPrune deletes results written at or before params.Before,
// including the tombstones JobResultDelete leaves. Only live results are
// counted.
func (e *executor) JobResultPrune(ctx context.Context, params driver.JobPruneParams) (int, error) {
	candidates, err := scanIDs(ctx, e.conn,
		`SELECT DISTINCT job_id FROM goncordia_job_results WHERE updated_at <= ? LIMIT ?`,
		params.Before, pruneLimit(params.Limit))
	if err != nil || len(candidates) == 0 {
		return 0, err
	}
	rows, err := e.conn.Query(ctx,
		`SELECT job_id, argMax(deleted, version), argMax(updated_at, version) FROM goncordia_job_results
		 WHERE job_id IN `+inList(len(candidates))+` GROUP BY job_id`,
		stringArgs(candidates)...)
	if err != nil {
		return 0, err
	}
	var expired []string
	deleted := 0
	for rows.Next() {
		var (
			id        string
			tombstone uint8
			updatedAt time.Time
		)
		if err := rows.Scan(&id, &tombstone, &updatedAt); err != nil {
			rows.Close()
			return 0, err
		}
		if updatedAt.After(params.Before) {
			continue
		}
		expired = append(expired, id)
		if tombstone == 0 {
			deleted++
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(expired) == 0 {
		return 0, nil
	}
	if err := e.conn.Exec(ctx, `DELETE FROM goncordia_job_results WHERE job_id IN `+inList(len(expired)), stringArgs(expired)...); err != nil {
		return 0, fmt.Errorf("delete results: %w", err)
	}
	return deleted, nil
}

var _ driver.JobPruner = (*executor)(nil)
