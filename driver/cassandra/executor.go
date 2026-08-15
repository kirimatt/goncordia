package cassandradriver

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"

	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/driver"
)

// ---- executor ----

type executor struct {
	session *gocql.Session
	clk     clock.Clock
}

func (e *executor) Begin(_ context.Context) (driver.ExecutorTx, error) {
	return nil, fmt.Errorf("%w: cassandra transactions", driver.ErrUnsupported)
}

func (e *executor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertMany(ctx, e.session, e.clk, params)
}
func (e *executor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	return jobGetByID(ctx, e.session, id)
}
func (e *executor) JobList(ctx context.Context, params driver.JobListParams) ([]driver.JobRow, error) {
	return jobList(ctx, e.session, params)
}
func (e *executor) QueueStats(ctx context.Context, queue string) (driver.QueueStats, error) {
	return queueStats(ctx, e.session, queue)
}
func (e *executor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	return jobFetchBatch(ctx, e.session, e.clk, params)
}
func (e *executor) JobRescueStuck(ctx context.Context, params driver.JobRescueParams) (int64, error) {
	return jobRescueStuck(ctx, e.session, params)
}
func (e *executor) JobHeartbeat(ctx context.Context, params driver.JobHeartbeatParams) (bool, error) {
	return jobHeartbeat(ctx, e.session, params)
}
func (e *executor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(ctx, e.session, e.clk, params)
}
func (e *executor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(ctx, e.session, e.clk, id)
}
func (e *executor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(ctx, e.session, id)
}
func (e *executor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(ctx, e.session, params)
}
func (e *executor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(ctx, e.session, name)
}
func (e *executor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(ctx, e.session, e.clk, name, true)
}
func (e *executor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(ctx, e.session, e.clk, name, false)
}
func (e *executor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(ctx, e.session, params)
}
func (e *executor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(ctx, e.session, e.clk, params)
}
func (e *executor) LeaderResign(ctx context.Context, name string) error {
	return leaderResign(ctx, e.session, name)
}

// ---- txExecutor (no-op tx — Cassandra has no real transactions) ----

type txExecutor struct{ executor }

func (t *txExecutor) Commit(_ context.Context) error   { return nil }
func (t *txExecutor) Rollback(_ context.Context) error { return nil }
func (t *txExecutor) Begin(_ context.Context) (driver.ExecutorTx, error) {
	return nil, fmt.Errorf("nested transactions not supported")
}
func (t *txExecutor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertMany(ctx, t.session, t.clk, params)
}
func (t *txExecutor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	return jobGetByID(ctx, t.session, id)
}
func (t *txExecutor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	return jobFetchBatch(ctx, t.session, t.clk, params)
}
func (t *txExecutor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(ctx, t.session, t.clk, params)
}
func (t *txExecutor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(ctx, t.session, t.clk, id)
}
func (t *txExecutor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(ctx, t.session, id)
}
func (t *txExecutor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(ctx, t.session, params)
}
func (t *txExecutor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(ctx, t.session, name)
}
func (t *txExecutor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(ctx, t.session, t.clk, name, true)
}
func (t *txExecutor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(ctx, t.session, t.clk, name, false)
}
func (t *txExecutor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(ctx, t.session, params)
}
func (t *txExecutor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(ctx, t.session, t.clk, params)
}
func (t *txExecutor) LeaderResign(ctx context.Context, name string) error {
	return leaderResign(ctx, t.session, name)
}

// ---- job row ----

type cassandraJob struct {
	ID          string
	Queue       string
	Kind        string
	Args        []byte
	State       string
	Priority    int
	RunAt       time.Time
	CreatedAt   time.Time
	AttemptedAt time.Time
	FinalizedAt time.Time
	AttemptNum  int
	MaxRetry    int
	TimeoutMs   int64
	UniqueKey   string
	WorkerID    string
	Tags        []string
	ErrorsJSON  string
	Version     int64
	PipelineID  string
}

type storedError struct {
	At      int64  `json:"at_ms"`
	Attempt int    `json:"attempt"`
	Error   string `json:"error"`
}

func marshalErrors(errs []driver.AttemptError) string {
	if len(errs) == 0 {
		return "[]"
	}
	out := make([]storedError, len(errs))
	for i, e := range errs {
		out[i] = storedError{At: e.At.UnixMilli(), Attempt: e.Attempt, Error: e.Error}
	}
	b, _ := json.Marshal(out)
	return string(b)
}

func unmarshalErrors(s string) []driver.AttemptError {
	if s == "" || s == "[]" {
		return nil
	}
	var stored []storedError
	if err := json.Unmarshal([]byte(s), &stored); err != nil {
		return nil
	}
	out := make([]driver.AttemptError, len(stored))
	for i, e := range stored {
		out[i] = driver.AttemptError{At: time.UnixMilli(e.At).UTC(), Attempt: e.Attempt, Error: e.Error}
	}
	return out
}

func jobToRow(j cassandraJob) *driver.JobRow {
	row := &driver.JobRow{
		ID:         j.ID,
		Queue:      j.Queue,
		Kind:       j.Kind,
		Args:       j.Args,
		State:      driver.JobState(j.State),
		Priority:   j.Priority,
		RunAt:      j.RunAt.UTC(),
		CreatedAt:  j.CreatedAt.UTC(),
		AttemptNum: j.AttemptNum,
		MaxRetry:   j.MaxRetry,
		Timeout:    time.Duration(j.TimeoutMs) * time.Millisecond,
		UniqueKey:  j.UniqueKey,
		WorkerID:   j.WorkerID,
		Tags:       j.Tags,
		Errors:     unmarshalErrors(j.ErrorsJSON),
		PipelineID: j.PipelineID,
	}
	if !j.AttemptedAt.IsZero() {
		t := j.AttemptedAt.UTC()
		row.AttemptedAt = &t
	}
	if !j.FinalizedAt.IsZero() {
		t := j.FinalizedAt.UTC()
		row.FinalizedAt = &t
	}
	return row
}

func selectJob(ctx context.Context, session *gocql.Session, id string) (cassandraJob, error) {
	var j cassandraJob
	err := session.Query(`SELECT id,queue,kind,args,state,priority,run_at,created_at,
		attempted_at,finalized_at,attempt_num,max_retry,timeout_ms,
		unique_key,worker_id,tags,errors_json,version,pipeline_id
		FROM goncordia_jobs WHERE id = ?`, id).
		WithContext(ctx).
		Scan(&j.ID, &j.Queue, &j.Kind, &j.Args, &j.State, &j.Priority,
			&j.RunAt, &j.CreatedAt, &j.AttemptedAt, &j.FinalizedAt,
			&j.AttemptNum, &j.MaxRetry, &j.TimeoutMs,
			&j.UniqueKey, &j.WorkerID, &j.Tags, &j.ErrorsJSON, &j.Version, &j.PipelineID)
	return j, err
}

func jobList(ctx context.Context, session *gocql.Session, params driver.JobListParams) ([]driver.JobRow, error) {
	iter := session.Query(`SELECT id,queue,kind,args,state,priority,run_at,created_at,
		attempted_at,finalized_at,attempt_num,max_retry,timeout_ms,
		unique_key,worker_id,tags,errors_json,version,pipeline_id FROM goncordia_jobs`).WithContext(ctx).Iter()
	var rows []driver.JobRow
	for {
		var j cassandraJob
		if !iter.Scan(&j.ID, &j.Queue, &j.Kind, &j.Args, &j.State, &j.Priority,
			&j.RunAt, &j.CreatedAt, &j.AttemptedAt, &j.FinalizedAt,
			&j.AttemptNum, &j.MaxRetry, &j.TimeoutMs,
			&j.UniqueKey, &j.WorkerID, &j.Tags, &j.ErrorsJSON, &j.Version, &j.PipelineID) {
			break
		}
		if params.Queue != "" && j.Queue != params.Queue || params.State != "" && driver.JobState(j.State) != params.State || params.Kind != "" && j.Kind != params.Kind || params.Cursor != "" && j.ID >= params.Cursor {
			continue
		}
		rows = append(rows, *jobToRow(j))
	}
	if err := iter.Close(); err != nil {
		return nil, err
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

func queueStats(ctx context.Context, session *gocql.Session, queue string) (driver.QueueStats, error) {
	iter := session.Query(`SELECT queue,state FROM goncordia_jobs`).WithContext(ctx).Iter()
	stats := driver.QueueStats{Queue: queue, States: make(map[driver.JobState]int64)}
	var jobQueue, state string
	for iter.Scan(&jobQueue, &state) {
		if jobQueue == queue {
			stats.States[driver.JobState(state)]++
			stats.Total++
		}
	}
	return stats, iter.Close()
}

// ---- JobInsertMany ----

func jobInsertMany(ctx context.Context, session *gocql.Session, clk clock.Clock, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	now := clk.Now()
	results := make([]driver.JobInsertResult, len(params))

	for i, p := range params {
		runAt := p.RunAt
		if runAt.IsZero() {
			runAt = now
		}
		state := driver.JobStateAvailable
		if runAt.After(now) {
			state = driver.JobStateScheduled
		}

		id := uuid.New().String()
		tags := p.Tags
		if tags == nil {
			tags = []string{}
		}

		// Unique-key check: INSERT IF NOT EXISTS into the uniq table.
		if p.UniqueKey != "" {
			m := map[string]interface{}{}
			applied, err := session.Query(
				`INSERT INTO goncordia_uniq (queue, ukey, job_id) VALUES (?, ?, ?) IF NOT EXISTS`,
				p.Queue, p.UniqueKey, id,
			).WithContext(ctx).MapScanCAS(m)
			if err != nil {
				return nil, fmt.Errorf("unique key check: %w", err)
			}
			if !applied {
				results[i] = driver.JobInsertResult{UniqueSkip: true}
				continue
			}
		}

		j := cassandraJob{
			ID:         id,
			Queue:      p.Queue,
			Kind:       p.Kind,
			Args:       p.Args,
			State:      string(state),
			Priority:   p.Priority,
			RunAt:      runAt,
			CreatedAt:  now,
			MaxRetry:   p.MaxRetry,
			TimeoutMs:  p.Timeout.Milliseconds(),
			UniqueKey:  p.UniqueKey,
			Tags:       tags,
			ErrorsJSON: "[]",
			Version:    1,
			PipelineID: p.PipelineID,
		}

		if err := session.Query(
			`INSERT INTO goncordia_jobs
			(id,queue,kind,args,state,priority,run_at,created_at,
			 attempt_num,max_retry,timeout_ms,unique_key,worker_id,tags,errors_json,version,pipeline_id)
			VALUES (?,?,?,?,?,?,?,?,0,?,?,?,?,?,?,1,?)`,
			j.ID, j.Queue, j.Kind, j.Args, j.State, j.Priority, j.RunAt, j.CreatedAt,
			j.MaxRetry, j.TimeoutMs, j.UniqueKey, j.WorkerID, j.Tags, j.ErrorsJSON, j.PipelineID,
		).WithContext(ctx).Exec(); err != nil {
			return nil, fmt.Errorf("insert job: %w", err)
		}

		// The lookup contains both immediately available and scheduled jobs;
		// run_at <= now makes scheduled rows visible without a promotion scan.
		if state == driver.JobStateAvailable || state == driver.JobStateScheduled {
			if err := session.Query(
				`INSERT INTO goncordia_jobs_avail (queue, run_at, priority, id) VALUES (?, ?, ?, ?)`,
				j.Queue, j.RunAt, j.Priority, j.ID,
			).WithContext(ctx).Exec(); err != nil {
				return nil, fmt.Errorf("insert avail: %w", err)
			}
		}

		// Ensure queue metadata exists.
		if err := session.Query(
			`INSERT INTO goncordia_queues (name, paused, created_at, updated_at) VALUES (?, false, ?, ?) IF NOT EXISTS`,
			p.Queue, now, now,
		).WithContext(ctx).Exec(); err != nil {
			return nil, fmt.Errorf("upsert queue: %w", err)
		}

		results[i] = driver.JobInsertResult{Job: jobToRow(j)}
	}
	return results, nil
}

// ---- JobGetByID ----

func jobGetByID(ctx context.Context, session *gocql.Session, id string) (*driver.JobRow, error) {
	j, err := selectJob(ctx, session, id)
	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return jobToRow(j), nil
}

// ---- JobFetchBatch ----

// JobFetchBatch claims up to params.Limit due available or scheduled jobs from the given queue.
// It uses Cassandra lightweight transactions to ensure
// each job is claimed by exactly one worker.
func jobFetchBatch(ctx context.Context, session *gocql.Session, clk clock.Clock, params driver.FetchParams) ([]driver.JobRow, error) {
	paused, err := isQueuePaused(ctx, session, params.Queue)
	if err != nil {
		return nil, err
	}
	if paused {
		return nil, nil
	}

	now := clk.Now()

	// Query the avail lookup table for candidate jobs.
	iter := session.Query(
		`SELECT id, run_at, priority FROM goncordia_jobs_avail
		 WHERE queue = ? AND run_at <= ?`,
		params.Queue, now,
	).WithContext(ctx).Iter()

	type candidate struct {
		id       string
		runAt    time.Time
		priority int
	}
	var candidates []candidate
	var cid string
	var cRunAt time.Time
	var cPriority int
	for iter.Scan(&cid, &cRunAt, &cPriority) {
		candidates = append(candidates, candidate{id: cid, runAt: cRunAt, priority: cPriority})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("scan avail: %w", err)
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].priority != candidates[j].priority {
			return candidates[i].priority > candidates[j].priority
		}
		if !candidates[i].runAt.Equal(candidates[j].runAt) {
			return candidates[i].runAt.Before(candidates[j].runAt)
		}
		return candidates[i].id < candidates[j].id
	})

	var claimed []driver.JobRow
	for _, c := range candidates {
		if len(claimed) >= params.Limit {
			break
		}

		j, err := selectJob(ctx, session, c.id)
		if err == gocql.ErrNotFound {
			// Stale avail entry — clean up.
			_ = session.Query(`DELETE FROM goncordia_jobs_avail WHERE queue=? AND run_at=? AND priority=? AND id=?`,
				params.Queue, c.runAt, c.priority, c.id).WithContext(ctx).Exec()
			continue
		}
		if err != nil {
			return nil, err
		}
		if j.State != string(driver.JobStateAvailable) && j.State != string(driver.JobStateScheduled) {
			// Already claimed by another worker.
			_ = session.Query(`DELETE FROM goncordia_jobs_avail WHERE queue=? AND run_at=? AND priority=? AND id=?`,
				params.Queue, c.runAt, c.priority, c.id).WithContext(ctx).Exec()
			continue
		}

		// Atomically claim with LWT.
		newVersion := j.Version + 1
		applied, lwtErr := session.Query(
			`UPDATE goncordia_jobs SET state=?, worker_id=?, attempted_at=?, attempt_num=?, version=?
			 WHERE id=? IF state IN (?,?) AND version=?`,
			string(driver.JobStateRunning), params.WorkerID, now, j.AttemptNum+1, newVersion,
			c.id, string(driver.JobStateAvailable), string(driver.JobStateScheduled), j.Version,
		).WithContext(ctx).MapScanCAS(map[string]interface{}{})
		if lwtErr != nil {
			return nil, fmt.Errorf("claim LWT: %w", lwtErr)
		}
		if !applied {
			continue // another worker claimed it first
		}
		if err := session.Query(
			`INSERT INTO goncordia_jobs_running (queue, attempted_at, id) VALUES (?, ?, ?)`,
			params.Queue, now, c.id,
		).WithContext(ctx).Exec(); err != nil {
			return nil, fmt.Errorf("track running job: %w", err)
		}

		// Remove from avail lookup.
		_ = session.Query(`DELETE FROM goncordia_jobs_avail WHERE queue=? AND run_at=? AND priority=? AND id=?`,
			params.Queue, c.runAt, c.priority, c.id).WithContext(ctx).Exec()

		j.State = string(driver.JobStateRunning)
		j.WorkerID = params.WorkerID
		j.AttemptedAt = now
		j.AttemptNum++
		j.Version = newVersion
		claimed = append(claimed, *jobToRow(j))
	}
	return claimed, nil
}

func jobRescueStuck(ctx context.Context, session *gocql.Session, params driver.JobRescueParams) (int64, error) {
	iter := session.Query(
		`SELECT attempted_at, id FROM goncordia_jobs_running WHERE queue=? AND attempted_at<=?`,
		params.Queue, params.Before,
	).WithContext(ctx).Iter()
	type candidate struct {
		attemptedAt time.Time
		id          string
	}
	var candidates []candidate
	var attemptedAt time.Time
	var id string
	for iter.Scan(&attemptedAt, &id) {
		candidates = append(candidates, candidate{attemptedAt: attemptedAt, id: id})
	}
	if err := iter.Close(); err != nil {
		return 0, err
	}
	var rescued int64
	for _, candidate := range candidates {
		job, err := selectJob(ctx, session, candidate.id)
		if err == gocql.ErrNotFound || (err == nil && job.State != string(driver.JobStateRunning)) {
			_ = session.Query(`DELETE FROM goncordia_jobs_running WHERE queue=? AND attempted_at=? AND id=?`, params.Queue, candidate.attemptedAt, candidate.id).WithContext(ctx).Exec()
			continue
		}
		if err != nil {
			return rescued, err
		}
		applied, err := session.Query(
			`UPDATE goncordia_jobs SET state=?, worker_id='', attempted_at=?, version=version+1 WHERE id=? IF state=? AND attempted_at=?`,
			string(driver.JobStateAvailable), time.Time{}, candidate.id, string(driver.JobStateRunning), candidate.attemptedAt,
		).WithContext(ctx).MapScanCAS(map[string]interface{}{})
		if err != nil {
			return rescued, err
		}
		if applied {
			if err := session.Query(`INSERT INTO goncordia_jobs_avail (queue, run_at, priority, id) VALUES (?, ?, ?, ?)`, job.Queue, job.RunAt, job.Priority, job.ID).WithContext(ctx).Exec(); err != nil {
				return rescued, err
			}
			rescued++
		}
		if err := session.Query(`DELETE FROM goncordia_jobs_running WHERE queue=? AND attempted_at=? AND id=?`, params.Queue, candidate.attemptedAt, candidate.id).WithContext(ctx).Exec(); err != nil {
			return rescued, err
		}
	}
	return rescued, nil
}

func jobHeartbeat(ctx context.Context, session *gocql.Session, params driver.JobHeartbeatParams) (bool, error) {
	job, err := selectJob(ctx, session, params.ID)
	if err == gocql.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if job.State != string(driver.JobStateRunning) || job.WorkerID != params.WorkerID || job.AttemptNum != params.Attempt {
		return false, nil
	}
	at := params.At.UTC()
	if err := session.Query(
		`INSERT INTO goncordia_jobs_running (queue, attempted_at, id) VALUES (?, ?, ?)`,
		job.Queue, at, job.ID,
	).WithContext(ctx).Exec(); err != nil {
		return false, err
	}
	applied, err := session.Query(
		`UPDATE goncordia_jobs SET attempted_at=?, version=version+1 WHERE id=?
		 IF state=? AND worker_id=? AND attempt_num=? AND version=?`,
		at, job.ID, string(driver.JobStateRunning), params.WorkerID, params.Attempt, job.Version,
	).WithContext(ctx).MapScanCAS(map[string]interface{}{})
	if err != nil || !applied {
		_ = session.Query(
			`DELETE FROM goncordia_jobs_running WHERE queue=? AND attempted_at=? AND id=?`,
			job.Queue, at, job.ID,
		).WithContext(ctx).Exec()
		return false, err
	}
	if err := session.Query(
		`DELETE FROM goncordia_jobs_running WHERE queue=? AND attempted_at=? AND id=?`,
		job.Queue, job.AttemptedAt, job.ID,
	).WithContext(ctx).Exec(); err != nil {
		return false, err
	}
	return true, nil
}

// ---- JobSetStateIfRunning ----

func jobSetStateIfRunning(ctx context.Context, session *gocql.Session, clk clock.Clock, params driver.JobSetStateParams) error {
	j, err := selectJob(ctx, session, params.ID)
	if err == gocql.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}
	if j.State != string(driver.JobStateRunning) || !params.MatchesClaim(j.WorkerID, j.AttemptNum) {
		return nil
	}

	if params.Yield {
		applied, err := session.Query(
			`UPDATE goncordia_jobs SET state=?, worker_id='', attempted_at=?, attempt_num=attempt_num-1, version=version+1
			 WHERE id=? IF state=? AND version=?`,
			string(driver.JobStateAvailable), time.Time{}, params.ID, string(driver.JobStateRunning), j.Version,
		).WithContext(ctx).MapScanCAS(map[string]interface{}{})
		if err != nil || !applied {
			return err
		}
		if err := deleteRunningLookup(ctx, session, j); err != nil {
			return err
		}
		return session.Query(
			`INSERT INTO goncordia_jobs_avail (queue, run_at, priority, id) VALUES (?, ?, ?, ?)`,
			j.Queue, j.RunAt, j.Priority, params.ID,
		).WithContext(ctx).Exec()
	}

	now := clk.Now()
	newErrors := unmarshalErrors(j.ErrorsJSON)
	if params.Err != nil {
		newErrors = append(newErrors, driver.AttemptError{
			At:      now,
			Attempt: j.AttemptNum,
			Error:   *params.Err,
		})
	}
	errJSON := marshalErrors(newErrors)

	if params.State == driver.JobStateRetryable {
		retryAt := params.RetryAt
		if retryAt.IsZero() {
			retryAt = now
		}
		applied, err := session.Query(
			`UPDATE goncordia_jobs SET state=?, run_at=?, worker_id='', errors_json=?, version=version+1
			 WHERE id=? IF state=? AND version=?`,
			string(driver.JobStateAvailable), retryAt, errJSON, params.ID, string(driver.JobStateRunning), j.Version,
		).WithContext(ctx).MapScanCAS(map[string]interface{}{})
		if err != nil {
			return err
		}
		if !applied {
			return nil
		}
		if err := deleteRunningLookup(ctx, session, j); err != nil {
			return err
		}
		// Re-add to avail lookup at the retry time.
		return session.Query(
			`INSERT INTO goncordia_jobs_avail (queue, run_at, priority, id) VALUES (?, ?, ?, ?)`,
			j.Queue, retryAt, j.Priority, params.ID,
		).WithContext(ctx).Exec()
	}

	// Terminal state.
	applied, stateErr := session.Query(
		`UPDATE goncordia_jobs SET state=?, finalized_at=?, worker_id='', errors_json=?, version=version+1
		 WHERE id=? IF state=? AND version=?`,
		string(params.State), now, errJSON, params.ID, string(driver.JobStateRunning), j.Version,
	).WithContext(ctx).MapScanCAS(map[string]interface{}{})
	if stateErr != nil || !applied {
		return stateErr
	}
	if err := deleteRunningLookup(ctx, session, j); err != nil {
		return err
	}
	if j.UniqueKey != "" {
		return session.Query(`DELETE FROM goncordia_uniq WHERE queue=? AND ukey=?`,
			j.Queue, j.UniqueKey).WithContext(ctx).Exec()
	}
	return nil
}

func deleteRunningLookup(ctx context.Context, session *gocql.Session, job cassandraJob) error {
	return session.Query(
		`DELETE FROM goncordia_jobs_running WHERE queue=? AND attempted_at=? AND id=?`,
		job.Queue, job.AttemptedAt, job.ID,
	).WithContext(ctx).Exec()
}

// ---- JobCancel ----

func jobCancel(ctx context.Context, session *gocql.Session, clk clock.Clock, id string) error {
	j, err := selectJob(ctx, session, id)
	if err == gocql.ErrNotFound {
		return fmt.Errorf("job %q not found", id)
	}
	if err != nil {
		return err
	}
	if j.State != string(driver.JobStateAvailable) && j.State != string(driver.JobStateScheduled) {
		return fmt.Errorf("job %q is in state %s, can only cancel available/scheduled", id, j.State)
	}

	now := clk.Now()
	if _, cancelErr := session.Query(
		`UPDATE goncordia_jobs SET state=?, finalized_at=?, version=version+1
		 WHERE id=? IF state IN ('available','scheduled')`,
		string(driver.JobStateCancelled), now, id,
	).WithContext(ctx).MapScanCAS(map[string]interface{}{}); cancelErr != nil {
		return cancelErr
	}

	// Clean up avail lookup if it was available (not scheduled).
	if j.State == string(driver.JobStateAvailable) {
		_ = session.Query(`DELETE FROM goncordia_jobs_avail WHERE queue=? AND run_at=? AND priority=? AND id=?`,
			j.Queue, j.RunAt, j.Priority, id).WithContext(ctx).Exec()
	}

	if j.UniqueKey != "" {
		_ = session.Query(`DELETE FROM goncordia_uniq WHERE queue=? AND ukey=?`,
			j.Queue, j.UniqueKey).WithContext(ctx).Exec()
	}
	return nil
}

// ---- JobDelete ----

func jobDelete(ctx context.Context, session *gocql.Session, id string) error {
	j, err := selectJob(ctx, session, id)
	if err == gocql.ErrNotFound {
		return nil
	}
	if err != nil {
		return err
	}

	if err := session.Query(`DELETE FROM goncordia_jobs WHERE id=?`, id).WithContext(ctx).Exec(); err != nil {
		return err
	}
	if j.State == string(driver.JobStateAvailable) {
		_ = session.Query(`DELETE FROM goncordia_jobs_avail WHERE queue=? AND run_at=? AND priority=? AND id=?`,
			j.Queue, j.RunAt, j.Priority, id).WithContext(ctx).Exec()
	}
	if j.UniqueKey != "" {
		_ = session.Query(`DELETE FROM goncordia_uniq WHERE queue=? AND ukey=?`,
			j.Queue, j.UniqueKey).WithContext(ctx).Exec()
	}
	return nil
}

// ---- JobReschedule ----

func jobReschedule(ctx context.Context, session *gocql.Session, params driver.RescheduleParams) error {
	j, err := selectJob(ctx, session, params.ID)
	if err == gocql.ErrNotFound {
		return fmt.Errorf("job %q not found", params.ID)
	}
	if err != nil {
		return err
	}

	oldState := j.State
	oldRunAt := j.RunAt

	if err := session.Query(
		`UPDATE goncordia_jobs SET state=?, run_at=?, version=version+1 WHERE id=?`,
		string(driver.JobStateScheduled), params.RunAt, params.ID,
	).WithContext(ctx).Exec(); err != nil {
		return err
	}

	if oldState == string(driver.JobStateAvailable) {
		_ = session.Query(`DELETE FROM goncordia_jobs_avail WHERE queue=? AND run_at=? AND priority=? AND id=?`,
			j.Queue, oldRunAt, j.Priority, params.ID).WithContext(ctx).Exec()
	}
	return nil
}

// ---- Queue ----

func isQueuePaused(ctx context.Context, session *gocql.Session, name string) (bool, error) {
	var paused bool
	err := session.Query(`SELECT paused FROM goncordia_queues WHERE name=?`, name).
		WithContext(ctx).Scan(&paused)
	if err == gocql.ErrNotFound {
		return false, nil
	}
	return paused, err
}

func queueGet(ctx context.Context, session *gocql.Session, name string) (*driver.QueueRow, error) {
	var row driver.QueueRow
	var createdAt, updatedAt time.Time
	err := session.Query(`SELECT name, paused, created_at, updated_at FROM goncordia_queues WHERE name=?`, name).
		WithContext(ctx).Scan(&row.Name, &row.Paused, &createdAt, &updatedAt)
	if err == gocql.ErrNotFound {
		return nil, fmt.Errorf("queue %q not found", name)
	}
	if err != nil {
		return nil, err
	}
	row.CreatedAt = createdAt.UTC()
	row.UpdatedAt = updatedAt.UTC()
	return &row, nil
}

func queueSetPaused(ctx context.Context, session *gocql.Session, clk clock.Clock, name string, paused bool) error {
	now := clk.Now()
	return session.Query(
		`UPDATE goncordia_queues SET paused=?, updated_at=? WHERE name=?`,
		paused, now, name,
	).WithContext(ctx).Exec()
}

func queueList(ctx context.Context, session *gocql.Session, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	limit := params.Limit
	if limit <= 0 {
		limit = 100
	}
	iter := session.Query(`SELECT name, paused, created_at, updated_at FROM goncordia_queues LIMIT ?`, limit).
		WithContext(ctx).Iter()
	var rows []*driver.QueueRow
	var name string
	var paused bool
	var createdAt, updatedAt time.Time
	for iter.Scan(&name, &paused, &createdAt, &updatedAt) {
		rows = append(rows, &driver.QueueRow{
			Name:      name,
			Paused:    paused,
			CreatedAt: createdAt.UTC(),
			UpdatedAt: updatedAt.UTC(),
		})
	}
	return rows, iter.Close()
}

// ---- Leader election ----

func leaderAttemptElect(ctx context.Context, session *gocql.Session, clk clock.Clock, params driver.LeaderElectParams) (bool, error) {
	now := clk.Now()
	expiresAt := now.Add(params.TTL)

	// Check if key exists and is expired.
	var currentWorker string
	var currentExpiry time.Time
	err := session.Query(`SELECT worker_id, expires_at FROM goncordia_leaders WHERE name=?`, params.Name).
		WithContext(ctx).Scan(&currentWorker, &currentExpiry)

	if err == gocql.ErrNotFound || now.After(currentExpiry) {
		// No leader or expired — try to become leader with LWT.
		applied, lwtErr := session.Query(
			`INSERT INTO goncordia_leaders (name, worker_id, expires_at) VALUES (?, ?, ?) IF NOT EXISTS`,
			params.Name, params.WorkerID, expiresAt,
		).WithContext(ctx).MapScanCAS(map[string]interface{}{})
		if lwtErr != nil {
			return false, lwtErr
		}
		if applied {
			return true, nil
		}
		// Race — someone else got in. Try UPDATE instead (handles expired case).
		applied, lwtErr = session.Query(
			`UPDATE goncordia_leaders SET worker_id=?, expires_at=? WHERE name=? IF expires_at<?`,
			params.WorkerID, expiresAt, params.Name, now,
		).WithContext(ctx).MapScanCAS(map[string]interface{}{})
		if lwtErr != nil {
			return false, lwtErr
		}
		return applied, nil
	}
	if err != nil {
		return false, err
	}

	// Existing unexpired leader — renew if it's us.
	if currentWorker == params.WorkerID {
		return true, session.Query(
			`UPDATE goncordia_leaders SET expires_at=? WHERE name=? IF worker_id=?`,
			expiresAt, params.Name, params.WorkerID,
		).WithContext(ctx).Exec()
	}
	return false, nil
}

func leaderResign(ctx context.Context, session *gocql.Session, name string) error {
	return session.Query(`DELETE FROM goncordia_leaders WHERE name=?`, name).WithContext(ctx).Exec()
}

// compile-time checks
var _ driver.Executor = (*executor)(nil)
var _ driver.ExecutorTx = (*txExecutor)(nil)
