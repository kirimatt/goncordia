package firestoredriver

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/driver"
)

// errJobGone signals that a candidate job was already claimed by another worker.
var errJobGone = errors.New("job no longer available")

// ---- document types ----

type jobDoc struct {
	ID          string     `firestore:"id"`
	Queue       string     `firestore:"queue"`
	Kind        string     `firestore:"kind"`
	Args        string     `firestore:"args"` // JSON string
	State       string     `firestore:"state"`
	Priority    int        `firestore:"priority"`
	RunAt       time.Time  `firestore:"run_at"`
	CreatedAt   time.Time  `firestore:"created_at"`
	AttemptedAt time.Time  `firestore:"attempted_at"` // zero if not yet attempted
	FinalizedAt time.Time  `firestore:"finalized_at"` // zero if not finalized
	AttemptNum  int        `firestore:"attempt_num"`
	MaxRetry    int        `firestore:"max_retry"`
	TimeoutMs   int64      `firestore:"timeout_ms"`
	UniqueKey   string     `firestore:"unique_key"`
	WorkerID    string     `firestore:"worker_id"`
	Tags        []string   `firestore:"tags"`
	Errors      []jobError `firestore:"errors"`
	Version     int64      `firestore:"version"`
	PipelineID  string     `firestore:"pipeline_id"`
}

type jobError struct {
	At      time.Time `firestore:"at"`
	Attempt int       `firestore:"attempt"`
	Message string    `firestore:"message"`
}

type queueDoc struct {
	Name      string    `firestore:"name"`
	Paused    bool      `firestore:"paused"`
	CreatedAt time.Time `firestore:"created_at"`
	UpdatedAt time.Time `firestore:"updated_at"`
}

type leaderDoc struct {
	Name      string    `firestore:"name"`
	WorkerID  string    `firestore:"worker_id"`
	ExpiresAt time.Time `firestore:"expires_at"`
}

// ---- executor (non-transactional) ----

type executor struct {
	client *firestore.Client
	clk    clock.Clock
}

func (e *executor) Begin(_ context.Context) (driver.ExecutorTx, error) {
	// Firestore transactions must be started via RunTransaction.
	// The engine never calls Begin; return an error to surface misuse early.
	return nil, fmt.Errorf("%w: use firestore.Client.RunTransaction with EnqueueTx", driver.ErrUnsupported)
}

func (e *executor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertMany(ctx, e.client, e.clk, params)
}
func (e *executor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	return jobGetByID(ctx, e.client, id)
}
func (e *executor) JobList(ctx context.Context, params driver.JobListParams) ([]driver.JobRow, error) {
	return jobList(ctx, e.client, params)
}
func (e *executor) QueueStats(ctx context.Context, queue string) (driver.QueueStats, error) {
	return queueStats(ctx, e.client, queue)
}
func (e *executor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	return jobFetchBatch(ctx, e.client, e.clk, params)
}
func (e *executor) JobRescueStuck(ctx context.Context, params driver.JobRescueParams) (int64, error) {
	return jobRescueStuck(ctx, e.client, params)
}
func (e *executor) JobHeartbeat(ctx context.Context, params driver.JobHeartbeatParams) (bool, error) {
	return jobHeartbeat(ctx, e.client, params)
}
func (e *executor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(ctx, e.client, e.clk, params)
}
func (e *executor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(ctx, e.client, e.clk, id)
}
func (e *executor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(ctx, e.client, id)
}
func (e *executor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(ctx, e.client, params)
}
func (e *executor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(ctx, e.client, e.clk, name)
}
func (e *executor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(ctx, e.client, e.clk, name, true)
}
func (e *executor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(ctx, e.client, e.clk, name, false)
}
func (e *executor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(ctx, e.client, params)
}
func (e *executor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(ctx, e.client, e.clk, params)
}
func (e *executor) LeaderResign(ctx context.Context, params driver.LeaderResignParams) error {
	return leaderResign(ctx, e.client, params)
}
func (e *executor) ScheduleCursorGetOrCreate(ctx context.Context, params driver.ScheduleCursorCreateParams) (driver.ScheduleCursorResult, error) {
	return scheduleCursorGetOrCreate(ctx, e.client, params)
}
func (e *executor) ScheduleCursorAdvance(ctx context.Context, params driver.ScheduleCursorAdvanceParams) (bool, error) {
	return scheduleCursorAdvance(ctx, e.client, params)
}

// ---- txExecutor (wraps *firestore.Transaction from user's RunTransaction callback) ----

type txExecutor struct {
	client *firestore.Client
	tx     *firestore.Transaction
	clk    clock.Clock
}

func (t *txExecutor) Commit(_ context.Context) error   { return nil } // managed by RunTransaction
func (t *txExecutor) Rollback(_ context.Context) error { return nil } // managed by RunTransaction
func (t *txExecutor) Begin(_ context.Context) (driver.ExecutorTx, error) {
	return nil, fmt.Errorf("nested transactions not supported")
}

func (t *txExecutor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertManyTx(ctx, t.client, t.tx, t.clk, params)
}
func (t *txExecutor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	snap, err := t.tx.Get(t.client.Collection(colJobs).Doc(id))
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, nil
		}
		return nil, err
	}
	var j jobDoc
	if err := snap.DataTo(&j); err != nil {
		return nil, err
	}
	return docToRow(j), nil
}
func (t *txExecutor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	// Not typically called in a user transaction; fall through to non-tx path.
	return jobFetchBatch(ctx, t.client, t.clk, params)
}
func (t *txExecutor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(ctx, t.client, t.clk, params)
}
func (t *txExecutor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(ctx, t.client, t.clk, id)
}
func (t *txExecutor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(ctx, t.client, id)
}
func (t *txExecutor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(ctx, t.client, params)
}
func (t *txExecutor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(ctx, t.client, t.clk, name)
}
func (t *txExecutor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(ctx, t.client, t.clk, name, true)
}
func (t *txExecutor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(ctx, t.client, t.clk, name, false)
}
func (t *txExecutor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(ctx, t.client, params)
}
func (t *txExecutor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(ctx, t.client, t.clk, params)
}
func (t *txExecutor) LeaderResign(ctx context.Context, params driver.LeaderResignParams) error {
	return leaderResign(ctx, t.client, params)
}
func (t *txExecutor) ScheduleCursorGetOrCreate(ctx context.Context, params driver.ScheduleCursorCreateParams) (driver.ScheduleCursorResult, error) {
	return scheduleCursorGetOrCreate(ctx, t.client, params)
}
func (t *txExecutor) ScheduleCursorAdvance(ctx context.Context, params driver.ScheduleCursorAdvanceParams) (bool, error) {
	return scheduleCursorAdvance(ctx, t.client, params)
}

// ---- JobInsertMany (non-tx) ----

func jobInsertMany(ctx context.Context, client *firestore.Client, clk clock.Clock, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
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

		doc := jobDoc{
			ID:         id,
			Queue:      p.Queue,
			Kind:       p.Kind,
			Args:       string(p.Args),
			State:      string(state),
			Priority:   p.Priority,
			RunAt:      runAt.UTC(),
			CreatedAt:  now.UTC(),
			AttemptNum: 0,
			MaxRetry:   p.MaxRetry,
			TimeoutMs:  p.Timeout.Milliseconds(),
			UniqueKey:  p.UniqueKey,
			Tags:       tags,
			Errors:     []jobError{},
			Version:    1,
			PipelineID: p.PipelineID,
		}

		jobRef := client.Collection(colJobs).Doc(id)

		if p.UniqueKey != "" {
			uniqRef := client.Collection(colUniq).Doc(p.UniqueKey)
			var skip bool
			if err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
				snap, err := tx.Get(uniqRef)
				if err == nil && snap.Exists() {
					skip = true
					return nil
				}
				if err != nil && status.Code(err) != codes.NotFound {
					return err
				}
				if err := tx.Create(jobRef, doc); err != nil {
					return err
				}
				return tx.Create(uniqRef, map[string]interface{}{"job_id": id})
			}); err != nil {
				return nil, fmt.Errorf("insert job %d: %w", i, err)
			}
			if skip {
				results[i] = driver.JobInsertResult{UniqueSkip: true}
				continue
			}
		} else {
			if _, err := jobRef.Create(ctx, doc); err != nil {
				return nil, fmt.Errorf("insert job %d: %w", i, err)
			}
		}

		// Ensure queue metadata row exists (ignore if already created).
		qRef := client.Collection(colQueues).Doc(p.Queue)
		if _, err := qRef.Create(ctx, queueDoc{
			Name:      p.Queue,
			Paused:    false,
			CreatedAt: now.UTC(),
			UpdatedAt: now.UTC(),
		}); err != nil && status.Code(err) != codes.AlreadyExists {
			return nil, fmt.Errorf("upsert queue: %w", err)
		}

		results[i] = driver.JobInsertResult{Job: docToRow(doc)}
	}
	return results, nil
}

// ---- JobInsertMany (transactional — all reads before all writes) ----

func jobInsertManyTx(ctx context.Context, client *firestore.Client, tx *firestore.Transaction, clk clock.Clock, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	now := clk.Now()
	results := make([]driver.JobInsertResult, len(params))

	type entry struct {
		jobRef  *firestore.DocumentRef
		uniqRef *firestore.DocumentRef
		doc     jobDoc
		skip    bool
	}
	entries := make([]entry, len(params))

	// Phase 1 — all reads.
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
		doc := jobDoc{
			ID:         id,
			Queue:      p.Queue,
			Kind:       p.Kind,
			Args:       string(p.Args),
			State:      string(state),
			Priority:   p.Priority,
			RunAt:      runAt.UTC(),
			CreatedAt:  now.UTC(),
			AttemptNum: 0,
			MaxRetry:   p.MaxRetry,
			TimeoutMs:  p.Timeout.Milliseconds(),
			UniqueKey:  p.UniqueKey,
			Tags:       tags,
			Errors:     []jobError{},
			Version:    1,
			PipelineID: p.PipelineID,
		}
		entries[i] = entry{
			jobRef: client.Collection(colJobs).Doc(id),
			doc:    doc,
		}

		if p.UniqueKey != "" {
			uniqRef := client.Collection(colUniq).Doc(p.UniqueKey)
			entries[i].uniqRef = uniqRef
			snap, err := tx.Get(uniqRef)
			if err != nil && status.Code(err) != codes.NotFound {
				return nil, err
			}
			if err == nil && snap.Exists() {
				entries[i].skip = true
				results[i] = driver.JobInsertResult{UniqueSkip: true}
			}
		}
	}

	// Phase 2 — all writes.
	for i, e := range entries {
		if e.skip {
			continue
		}
		if err := tx.Create(e.jobRef, e.doc); err != nil {
			return nil, err
		}
		if e.uniqRef != nil {
			if err := tx.Create(e.uniqRef, map[string]interface{}{"job_id": e.doc.ID}); err != nil {
				return nil, err
			}
		}
		results[i] = driver.JobInsertResult{Job: docToRow(e.doc)}
	}
	return results, nil
}

// ---- JobGetByID ----

func jobGetByID(ctx context.Context, client *firestore.Client, id string) (*driver.JobRow, error) {
	snap, err := client.Collection(colJobs).Doc(id).Get(ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, nil
		}
		return nil, err
	}
	var j jobDoc
	if err := snap.DataTo(&j); err != nil {
		return nil, err
	}
	return docToRow(j), nil
}

func jobList(ctx context.Context, client *firestore.Client, params driver.JobListParams) ([]driver.JobRow, error) {
	query := client.Collection(colJobs).Query
	if params.Queue != "" {
		query = query.Where("queue", "==", params.Queue)
	}
	if params.State != "" {
		query = query.Where("state", "==", string(params.State))
	}
	if params.Kind != "" {
		query = query.Where("kind", "==", params.Kind)
	}
	snaps, err := query.Documents(ctx).GetAll()
	if err != nil {
		return nil, err
	}
	rows := make([]driver.JobRow, 0, len(snaps))
	for _, snap := range snaps {
		var job jobDoc
		if snap.DataTo(&job) != nil || params.Cursor != "" && job.ID >= params.Cursor {
			continue
		}
		rows = append(rows, *docToRow(job))
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

func queueStats(ctx context.Context, client *firestore.Client, queue string) (driver.QueueStats, error) {
	snaps, err := client.Collection(colJobs).Where("queue", "==", queue).Documents(ctx).GetAll()
	if err != nil {
		return driver.QueueStats{}, err
	}
	stats := driver.QueueStats{Queue: queue, States: make(map[driver.JobState]int64)}
	for _, snap := range snaps {
		var job jobDoc
		if snap.DataTo(&job) == nil {
			stats.States[driver.JobState(job.State)]++
			stats.Total++
		}
	}
	return stats, nil
}

// ---- JobFetchBatch ----

// jobFetchBatch queries candidates with a snapshot read, then claims each one
// via a per-job transaction. Firestore's optimistic concurrency ensures only
// one worker wins per job.
func jobFetchBatch(ctx context.Context, client *firestore.Client, clk clock.Clock, params driver.FetchParams) ([]driver.JobRow, error) {
	// Check if queue is paused.
	qSnap, err := client.Collection(colQueues).Doc(params.Queue).Get(ctx)
	if err == nil && qSnap.Exists() {
		var q queueDoc
		qSnap.DataTo(&q) //nolint:errcheck
		if q.Paused {
			return nil, nil
		}
	}

	now := clk.Now()

	snaps, err := client.Collection(colJobs).
		Where("queue", "==", params.Queue).
		Where("state", "in", []string{string(driver.JobStateAvailable), string(driver.JobStateScheduled)}).
		Where("run_at", "<=", now).
		Documents(ctx).GetAll()
	if err != nil {
		return nil, fmt.Errorf("query available jobs: %w", err)
	}

	// Decode and sort: highest priority first, then earliest run_at.
	type candidate struct {
		ref *firestore.DocumentRef
		j   jobDoc
	}
	candidates := make([]candidate, 0, len(snaps))
	for _, s := range snaps {
		var j jobDoc
		if err := s.DataTo(&j); err != nil {
			continue
		}
		candidates = append(candidates, candidate{ref: s.Ref, j: j})
	}
	sort.Slice(candidates, func(i, k int) bool {
		if candidates[i].j.Priority != candidates[k].j.Priority {
			return candidates[i].j.Priority > candidates[k].j.Priority
		}
		if !candidates[i].j.RunAt.Equal(candidates[k].j.RunAt) {
			return candidates[i].j.RunAt.Before(candidates[k].j.RunAt)
		}
		if !candidates[i].j.CreatedAt.Equal(candidates[k].j.CreatedAt) {
			return candidates[i].j.CreatedAt.Before(candidates[k].j.CreatedAt)
		}
		return candidates[i].j.ID < candidates[k].j.ID
	})

	var claimed []driver.JobRow
	for _, c := range candidates {
		if len(claimed) >= params.Limit {
			break
		}

		claimTime := clk.Now()
		err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
			snap, err := tx.Get(c.ref)
			if err != nil {
				return err
			}
			var cur jobDoc
			if err := snap.DataTo(&cur); err != nil {
				return err
			}
			if cur.State != string(driver.JobStateAvailable) && cur.State != string(driver.JobStateScheduled) {
				return errJobGone
			}
			return tx.Update(c.ref, []firestore.Update{
				{Path: "state", Value: string(driver.JobStateRunning)},
				{Path: "worker_id", Value: params.WorkerID},
				{Path: "attempted_at", Value: claimTime.UTC()},
				{Path: "attempt_num", Value: firestore.Increment(1)},
				{Path: "version", Value: firestore.Increment(1)},
			})
		})
		if errors.Is(err, errJobGone) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("claim job: %w", err)
		}

		j := c.j
		row := driver.JobRow{
			ID:          j.ID,
			Queue:       j.Queue,
			Kind:        j.Kind,
			Args:        []byte(j.Args),
			State:       driver.JobStateRunning,
			Priority:    j.Priority,
			RunAt:       j.RunAt,
			CreatedAt:   j.CreatedAt,
			AttemptedAt: &claimTime,
			AttemptNum:  j.AttemptNum + 1,
			MaxRetry:    j.MaxRetry,
			Timeout:     time.Duration(j.TimeoutMs) * time.Millisecond,
			Tags:        j.Tags,
			Errors:      jobErrorsToRow(j.Errors),
			UniqueKey:   j.UniqueKey,
			WorkerID:    params.WorkerID,
			PipelineID:  j.PipelineID,
		}
		claimed = append(claimed, row)
	}
	return claimed, nil
}

func jobRescueStuck(ctx context.Context, client *firestore.Client, params driver.JobRescueParams) (int64, error) {
	snaps, err := client.Collection(colJobs).
		Where("queue", "==", params.Queue).
		Where("state", "==", string(driver.JobStateRunning)).
		Where("attempted_at", "<=", params.Before).
		Documents(ctx).GetAll()
	if err != nil {
		return 0, err
	}
	var rescued int64
	for _, snap := range snaps {
		err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
			current, err := tx.Get(snap.Ref)
			if err != nil {
				return err
			}
			var job jobDoc
			if err := current.DataTo(&job); err != nil {
				return err
			}
			if job.State != string(driver.JobStateRunning) || job.AttemptedAt.After(params.Before) {
				return errJobGone
			}
			return tx.Update(snap.Ref, []firestore.Update{
				{Path: "state", Value: string(driver.JobStateAvailable)},
				{Path: "worker_id", Value: ""},
				{Path: "attempted_at", Value: time.Time{}},
				{Path: "version", Value: firestore.Increment(1)},
			})
		})
		if errors.Is(err, errJobGone) {
			continue
		}
		if err != nil {
			return rescued, err
		}
		rescued++
	}
	return rescued, nil
}

func jobHeartbeat(ctx context.Context, client *firestore.Client, params driver.JobHeartbeatParams) (bool, error) {
	jobRef := client.Collection(colJobs).Doc(params.ID)
	renewed := false
	err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		snap, err := tx.Get(jobRef)
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return nil
			}
			return err
		}
		var job jobDoc
		if err := snap.DataTo(&job); err != nil {
			return err
		}
		if job.State != string(driver.JobStateRunning) || job.WorkerID != params.WorkerID || job.AttemptNum != params.Attempt {
			return nil
		}
		renewed = true
		return tx.Update(jobRef, []firestore.Update{{Path: "attempted_at", Value: params.At.UTC()}})
	})
	return renewed && err == nil, err
}

// ---- JobSetStateIfRunning ----

func jobSetStateIfRunning(ctx context.Context, client *firestore.Client, clk clock.Clock, params driver.JobSetStateParams) error {
	jobRef := client.Collection(colJobs).Doc(params.ID)
	return client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		snap, err := tx.Get(jobRef)
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return nil
			}
			return err
		}
		var j jobDoc
		if err := snap.DataTo(&j); err != nil {
			return err
		}
		if j.State != string(driver.JobStateRunning) || !params.MatchesClaim(j.WorkerID, j.AttemptNum) {
			return nil
		}

		now := clk.Now()

		if params.Yield {
			return tx.Update(jobRef, []firestore.Update{
				{Path: "state", Value: string(driver.JobStateAvailable)},
				{Path: "worker_id", Value: ""},
				{Path: "attempted_at", Value: time.Time{}},
				{Path: "attempt_num", Value: firestore.Increment(-1)},
				{Path: "version", Value: firestore.Increment(1)},
			})
		}

		updates := []firestore.Update{
			{Path: "worker_id", Value: ""},
			{Path: "version", Value: firestore.Increment(1)},
		}

		if params.Err != nil {
			newErrors := append(j.Errors, jobError{
				At:      now,
				Attempt: j.AttemptNum,
				Message: *params.Err,
			})
			updates = append(updates, firestore.Update{Path: "errors", Value: newErrors})
		}

		switch params.State {
		case driver.JobStateRetryable:
			retryAt := params.RetryAt
			if retryAt.IsZero() {
				retryAt = now
			}
			updates = append(updates,
				firestore.Update{Path: "state", Value: string(driver.JobStateAvailable)},
				firestore.Update{Path: "run_at", Value: retryAt.UTC()},
			)
		default:
			updates = append(updates,
				firestore.Update{Path: "state", Value: string(params.State)},
				firestore.Update{Path: "finalized_at", Value: now.UTC()},
			)
			if j.UniqueKey != "" && !driver.IsPermanentUniqueKey(j.UniqueKey) {
				tx.Delete(client.Collection(colUniq).Doc(j.UniqueKey)) //nolint:errcheck
			}
		}

		return tx.Update(jobRef, updates)
	})
}

// ---- JobCancel ----

func jobCancel(ctx context.Context, client *firestore.Client, clk clock.Clock, id string) error {
	jobRef := client.Collection(colJobs).Doc(id)
	return client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		snap, err := tx.Get(jobRef)
		if err != nil {
			if status.Code(err) == codes.NotFound {
				return fmt.Errorf("job %q not found", id)
			}
			return err
		}
		var j jobDoc
		if err := snap.DataTo(&j); err != nil {
			return err
		}
		if j.State != string(driver.JobStateAvailable) && j.State != string(driver.JobStateScheduled) {
			return fmt.Errorf("job %q is in state %s, can only cancel available/scheduled", id, j.State)
		}
		now := clk.Now()
		if err := tx.Update(jobRef, []firestore.Update{
			{Path: "state", Value: string(driver.JobStateCancelled)},
			{Path: "finalized_at", Value: now.UTC()},
			{Path: "version", Value: firestore.Increment(1)},
		}); err != nil {
			return err
		}
		if j.UniqueKey != "" && !driver.IsPermanentUniqueKey(j.UniqueKey) {
			tx.Delete(client.Collection(colUniq).Doc(j.UniqueKey)) //nolint:errcheck
		}
		return nil
	})
}

// ---- JobDelete ----

func jobDelete(ctx context.Context, client *firestore.Client, id string) error {
	jobRef := client.Collection(colJobs).Doc(id)
	snap, err := client.Collection(colJobs).Doc(id).Get(ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil
		}
		return err
	}
	var j jobDoc
	if err := snap.DataTo(&j); err != nil {
		return err
	}
	if _, err := jobRef.Delete(ctx); err != nil {
		return err
	}
	if j.UniqueKey != "" {
		client.Collection(colUniq).Doc(j.UniqueKey).Delete(ctx) //nolint:errcheck
	}
	return nil
}

// ---- JobReschedule ----

func jobReschedule(ctx context.Context, client *firestore.Client, params driver.RescheduleParams) error {
	_, err := client.Collection(colJobs).Doc(params.ID).Update(ctx, []firestore.Update{
		{Path: "state", Value: string(driver.JobStateScheduled)},
		{Path: "run_at", Value: params.RunAt.UTC()},
		{Path: "version", Value: firestore.Increment(1)},
	})
	return err
}

// ---- Queue ----

func queueGet(ctx context.Context, client *firestore.Client, clk clock.Clock, name string) (*driver.QueueRow, error) {
	snap, err := client.Collection(colQueues).Doc(name).Get(ctx)
	if err != nil {
		if status.Code(err) == codes.NotFound {
			// Auto-create on first access.
			now := clk.Now()
			doc := queueDoc{Name: name, Paused: false, CreatedAt: now.UTC(), UpdatedAt: now.UTC()}
			if _, cerr := client.Collection(colQueues).Doc(name).Create(ctx, doc); cerr != nil && status.Code(cerr) != codes.AlreadyExists {
				return nil, cerr
			}
			return &driver.QueueRow{Name: name, CreatedAt: now, UpdatedAt: now}, nil
		}
		return nil, err
	}
	var q queueDoc
	if err := snap.DataTo(&q); err != nil {
		return nil, err
	}
	return &driver.QueueRow{
		Name:      q.Name,
		Paused:    q.Paused,
		CreatedAt: q.CreatedAt,
		UpdatedAt: q.UpdatedAt,
	}, nil
}

func queueSetPaused(ctx context.Context, client *firestore.Client, clk clock.Clock, name string, paused bool) error {
	now := clk.Now().UTC()
	_, err := client.Collection(colQueues).Doc(name).Set(ctx, map[string]any{
		"name": name, "paused": paused, "created_at": now, "updated_at": now,
	}, firestore.MergeAll)
	return err
}

func queueList(ctx context.Context, client *firestore.Client, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	limit := params.Limit
	if limit <= 0 {
		limit = 100
	}
	snaps, err := client.Collection(colQueues).Limit(limit).Documents(ctx).GetAll()
	if err != nil {
		return nil, err
	}
	rows := make([]*driver.QueueRow, 0, len(snaps))
	for _, snap := range snaps {
		var q queueDoc
		if err := snap.DataTo(&q); err != nil {
			continue
		}
		rows = append(rows, &driver.QueueRow{
			Name:      q.Name,
			Paused:    q.Paused,
			CreatedAt: q.CreatedAt,
			UpdatedAt: q.UpdatedAt,
		})
	}
	return rows, nil
}

// ---- Leader election ----

// leaderAttemptElect claims or renews leadership using a conditional transaction.
func leaderAttemptElect(ctx context.Context, client *firestore.Client, clk clock.Clock, params driver.LeaderElectParams) (bool, error) {
	ref := client.Collection(colLeaders).Doc(params.Name)
	var elected bool
	err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		elected = false
		now := clk.Now()
		snap, err := tx.Get(ref)
		if err != nil && status.Code(err) != codes.NotFound {
			return err
		}

		if err != nil || !snap.Exists() {
			// No leader — claim it.
			elected = true
			return tx.Create(ref, leaderDoc{
				Name:      params.Name,
				WorkerID:  params.WorkerID,
				ExpiresAt: now.Add(params.TTL).UTC(),
			})
		}

		var cur leaderDoc
		if err := snap.DataTo(&cur); err != nil {
			return err
		}
		// Allow claim if expired or we are already the leader.
		if now.Before(cur.ExpiresAt) && cur.WorkerID != params.WorkerID {
			return nil // another worker holds the lease
		}
		elected = true
		return tx.Update(ref, []firestore.Update{
			{Path: "worker_id", Value: params.WorkerID},
			{Path: "expires_at", Value: now.Add(params.TTL).UTC()},
		})
	})
	return elected, err
}

func leaderResign(ctx context.Context, client *firestore.Client, params driver.LeaderResignParams) error {
	ref := client.Collection(colLeaders).Doc(params.Name)
	return client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		snap, err := tx.Get(ref)
		if status.Code(err) == codes.NotFound {
			return nil
		}
		if err != nil {
			return err
		}
		var current leaderDoc
		if err := snap.DataTo(&current); err != nil {
			return err
		}
		if current.WorkerID != params.WorkerID {
			return nil
		}
		return tx.Delete(ref)
	})
}

type scheduleCursorDoc struct {
	At time.Time `firestore:"cursor_at"`
}

func scheduleCursorGetOrCreate(ctx context.Context, client *firestore.Client, params driver.ScheduleCursorCreateParams) (driver.ScheduleCursorResult, error) {
	ref := client.Collection(colCursors).Doc(params.ID)
	var result driver.ScheduleCursorResult
	err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		snap, err := tx.Get(ref)
		if status.Code(err) == codes.NotFound {
			result = driver.ScheduleCursorResult{At: params.InitialAt.UTC(), Created: true}
			return tx.Create(ref, scheduleCursorDoc{At: result.At})
		}
		if err != nil {
			return err
		}
		var doc scheduleCursorDoc
		if err := snap.DataTo(&doc); err != nil {
			return err
		}
		result.At = doc.At.UTC()
		return nil
	})
	return result, err
}

func scheduleCursorAdvance(ctx context.Context, client *firestore.Client, params driver.ScheduleCursorAdvanceParams) (bool, error) {
	ref := client.Collection(colCursors).Doc(params.ID)
	advanced := false
	err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		snap, err := tx.Get(ref)
		if err != nil {
			return err
		}
		var doc scheduleCursorDoc
		if err := snap.DataTo(&doc); err != nil {
			return err
		}
		if !doc.At.Equal(params.Expected) {
			return nil
		}
		advanced = true
		return tx.Update(ref, []firestore.Update{{Path: "cursor_at", Value: params.Next.UTC()}})
	})
	return advanced, err
}

// ---- helpers ----

func docToRow(j jobDoc) *driver.JobRow {
	row := &driver.JobRow{
		ID:         j.ID,
		Queue:      j.Queue,
		Kind:       j.Kind,
		Args:       []byte(j.Args),
		State:      driver.JobState(j.State),
		Priority:   j.Priority,
		RunAt:      j.RunAt,
		CreatedAt:  j.CreatedAt,
		AttemptNum: j.AttemptNum,
		MaxRetry:   j.MaxRetry,
		Timeout:    time.Duration(j.TimeoutMs) * time.Millisecond,
		UniqueKey:  j.UniqueKey,
		WorkerID:   j.WorkerID,
		Tags:       j.Tags,
		Errors:     jobErrorsToRow(j.Errors),
		PipelineID: j.PipelineID,
	}
	if !j.AttemptedAt.IsZero() {
		t := j.AttemptedAt
		row.AttemptedAt = &t
	}
	if !j.FinalizedAt.IsZero() {
		t := j.FinalizedAt
		row.FinalizedAt = &t
	}
	if row.Tags == nil {
		row.Tags = []string{}
	}
	if len(row.Args) == 0 {
		row.Args = []byte("{}")
	}
	return row
}

func jobErrorsToRow(errs []jobError) []driver.AttemptError {
	if len(errs) == 0 {
		return nil
	}
	out := make([]driver.AttemptError, len(errs))
	for i, e := range errs {
		out[i] = driver.AttemptError{At: e.At, Attempt: e.Attempt, Error: e.Message}
	}
	return out
}

// compile-time checks
var _ driver.Executor = (*executor)(nil)
var _ driver.ExecutorTx = (*txExecutor)(nil)
