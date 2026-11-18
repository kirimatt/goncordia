package cassandradriver

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/gocql/gocql"

	"github.com/kirimatt/goncordia/v2/driver"
)

// Retention lookup tables.
//
// goncordia_jobs_finalized and goncordia_results_written index terminal jobs
// and results by time, partitioned into retentionShards per state so writes
// spread across nodes. Pruning walks a partition in clustering order from a
// stored watermark: deleted rows leave tombstones, and starting at the
// watermark keeps reads from scanning them. Entries written with a timestamp
// older than the watermark minus retentionRescan (a clock skew larger than
// that) are not pruned.
const (
	retentionShards = 16
	retentionRescan = time.Minute
)

var retentionTables = []string{
	`CREATE TABLE IF NOT EXISTS goncordia_jobs_finalized (
		state        text,
		shard        int,
		finalized_at timestamp,
		id           text,
		PRIMARY KEY ((state, shard), finalized_at, id)
	) WITH CLUSTERING ORDER BY (finalized_at ASC, id ASC)`,
	`CREATE TABLE IF NOT EXISTS goncordia_results_written (
		shard      int,
		updated_at timestamp,
		job_id     text,
		PRIMARY KEY ((shard), updated_at, job_id)
	) WITH CLUSTERING ORDER BY (updated_at ASC, job_id ASC)`,
	`CREATE TABLE IF NOT EXISTS goncordia_retention_marks (
		name text PRIMARY KEY,
		at   timestamp
	)`,
}

func retentionShard(id string) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	return int(h.Sum32() % retentionShards)
}

func recordFinalized(ctx context.Context, session *gocql.Session, state driver.JobState, at time.Time, id string) error {
	return session.Query(
		`INSERT INTO goncordia_jobs_finalized (state, shard, finalized_at, id) VALUES (?, ?, ?, ?)`,
		string(state), retentionShard(id), at, id,
	).WithContext(ctx).Exec()
}

func recordResult(ctx context.Context, session *gocql.Session, at time.Time, id string) error {
	return session.Query(
		`INSERT INTO goncordia_results_written (shard, updated_at, job_id) VALUES (?, ?, ?)`,
		retentionShard(id), at, id,
	).WithContext(ctx).Exec()
}

func (e *executor) mark(ctx context.Context, name string) (time.Time, error) {
	var at time.Time
	err := e.session.Query(`SELECT at FROM goncordia_retention_marks WHERE name=?`, name).WithContext(ctx).Scan(&at)
	if errors.Is(err, gocql.ErrNotFound) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	return at.Add(-retentionRescan), nil
}

func (e *executor) setMark(ctx context.Context, name string, at time.Time) error {
	return e.session.Query(`INSERT INTO goncordia_retention_marks (name, at) VALUES (?, ?)`, name, at).WithContext(ctx).Exec()
}

func pruneLimit(limit int) int {
	if limit <= 0 {
		return 500
	}
	return limit
}

type retentionEntry struct {
	at time.Time
	id string
}

// expired reads up to limit entries of one shard, oldest first, written at or
// before before and not before the shard's watermark.
func (e *executor) expired(ctx context.Context, query, mark string, before time.Time, limit int, key ...interface{}) ([]retentionEntry, error) {
	from, err := e.mark(ctx, mark)
	if err != nil {
		return nil, err
	}
	args := append(key, from, before, limit)
	iter := e.session.Query(query, args...).WithContext(ctx).Iter()
	var (
		entries []retentionEntry
		entry   retentionEntry
	)
	for iter.Scan(&entry.at, &entry.id) {
		entries = append(entries, entry)
	}
	if err := iter.Close(); err != nil {
		return nil, err
	}
	return entries, nil
}

// JobPruneFinalized deletes jobs of params.State finalized at or before
// params.Before through goncordia_jobs_finalized. An entry whose job has
// since left that state (a retried discarded job) is dropped without
// deleting the job.
func (e *executor) JobPruneFinalized(ctx context.Context, params driver.JobPruneParams) (int, error) {
	limit := pruneLimit(params.Limit)
	deleted := 0
	for shard := 0; shard < retentionShards && deleted < limit; shard++ {
		mark := fmt.Sprintf("jobs:%s:%d", params.State, shard)
		entries, err := e.expired(ctx,
			`SELECT finalized_at, id FROM goncordia_jobs_finalized
			 WHERE state=? AND shard=? AND finalized_at>=? AND finalized_at<=? LIMIT ?`,
			mark, params.Before, limit-deleted, string(params.State), shard)
		if err != nil {
			return deleted, fmt.Errorf("read expired jobs: %w", err)
		}
		for _, entry := range entries {
			j, err := selectJob(ctx, e.session, entry.id)
			switch {
			case errors.Is(err, gocql.ErrNotFound):
			case err != nil:
				return deleted, err
			case j.State == string(params.State) && j.FinalizedAt.Equal(entry.at):
				if err := jobDelete(ctx, e.session, entry.id); err != nil && !errors.Is(err, driver.ErrNotFound) {
					return deleted, err
				}
				deleted++
			}
			if err := e.session.Query(
				`DELETE FROM goncordia_jobs_finalized WHERE state=? AND shard=? AND finalized_at=? AND id=?`,
				string(params.State), shard, entry.at, entry.id,
			).WithContext(ctx).Exec(); err != nil {
				return deleted, err
			}
		}
		if len(entries) > 0 {
			if err := e.setMark(ctx, mark, entries[len(entries)-1].at); err != nil {
				return deleted, err
			}
		}
	}
	return deleted, nil
}

// JobResultPrune deletes results written at or before params.Before through
// goncordia_results_written. An entry for a result rewritten since is
// dropped without deleting the newer result.
func (e *executor) JobResultPrune(ctx context.Context, params driver.JobPruneParams) (int, error) {
	limit := pruneLimit(params.Limit)
	deleted := 0
	for shard := 0; shard < retentionShards && deleted < limit; shard++ {
		mark := fmt.Sprintf("results:%d", shard)
		entries, err := e.expired(ctx,
			`SELECT updated_at, job_id FROM goncordia_results_written
			 WHERE shard=? AND updated_at>=? AND updated_at<=? LIMIT ?`,
			mark, params.Before, limit-deleted, shard)
		if err != nil {
			return deleted, fmt.Errorf("read expired results: %w", err)
		}
		for _, entry := range entries {
			var updatedAt time.Time
			err := e.session.Query(`SELECT updated_at FROM goncordia_job_results WHERE job_id=?`, entry.id).
				WithContext(ctx).Scan(&updatedAt)
			switch {
			case errors.Is(err, gocql.ErrNotFound):
			case err != nil:
				return deleted, err
			case updatedAt.Equal(entry.at):
				if err := e.session.Query(`DELETE FROM goncordia_job_results WHERE job_id=?`, entry.id).
					WithContext(ctx).Exec(); err != nil {
					return deleted, err
				}
				deleted++
			}
			if err := e.session.Query(
				`DELETE FROM goncordia_results_written WHERE shard=? AND updated_at=? AND job_id=?`,
				shard, entry.at, entry.id,
			).WithContext(ctx).Exec(); err != nil {
				return deleted, err
			}
		}
		if len(entries) > 0 {
			if err := e.setMark(ctx, mark, entries[len(entries)-1].at); err != nil {
				return deleted, err
			}
		}
	}
	return deleted, nil
}

var _ driver.JobPruner = (*executor)(nil)
