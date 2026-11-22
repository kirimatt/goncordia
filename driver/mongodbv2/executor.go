package mongodbv2

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/driver"
)

// ---- BSON document types ----

type jobDoc struct {
	ID             bson.ObjectID  `bson:"_id,omitempty"`
	Queue          string         `bson:"queue"`
	Kind           string         `bson:"kind"`
	Args           string         `bson:"args"` // JSON string
	State          string         `bson:"state"`
	Priority       int            `bson:"priority"`
	RunAt          time.Time      `bson:"run_at"`
	CreatedAt      time.Time      `bson:"created_at"`
	AttemptedAt    *time.Time     `bson:"attempted_at,omitempty"`
	StartedAt      *time.Time     `bson:"started_at,omitempty"`
	LeaseExpiresAt *time.Time     `bson:"lease_expires_at,omitempty"`
	FinalizedAt    *time.Time     `bson:"finalized_at,omitempty"`
	AttemptNum     int            `bson:"attempt_num"`
	MaxRetry       int            `bson:"max_retry"`
	TimeoutMs      int64          `bson:"timeout_ms"`
	UniqueKey      string         `bson:"unique_key,omitempty"`
	WorkerID       string         `bson:"worker_id,omitempty"`
	Tags           []string       `bson:"tags"`
	Errors         []attemptError `bson:"errors"`
	PipelineID     string         `bson:"pipeline_id,omitempty"`
}

type attemptError struct {
	At      time.Time `bson:"at"`
	Attempt int       `bson:"attempt"`
	Message string    `bson:"message"`
	Trace   string    `bson:"trace,omitempty"`
}

type queueDoc struct {
	Name      string    `bson:"name"`
	Paused    bool      `bson:"paused"`
	CreatedAt time.Time `bson:"created_at"`
	UpdatedAt time.Time `bson:"updated_at"`
}

type leaderDoc struct {
	ID        string    `bson:"_id"`
	WorkerID  string    `bson:"worker_id"`
	ElectedAt time.Time `bson:"elected_at"`
	ExpiresAt time.Time `bson:"expires_at"`
}

// ---- executor (non-transactional) ----

type executor struct {
	client *mongo.Client
	db     *mongo.Database
	clk    clock.Clock
}

func (e *executor) Begin(ctx context.Context) (driver.ExecutorTx, error) {
	session, err := e.client.StartSession()
	if err != nil {
		return nil, fmt.Errorf("start session: %w", err)
	}
	if err := session.StartTransaction(); err != nil {
		session.EndSession(ctx)
		return nil, fmt.Errorf("start transaction: %w", err)
	}
	return &txExecutor{db: e.db, session: session, owned: true, clk: e.clk}, nil
}

func (e *executor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertMany(ctx, e.db, e.clk, params)
}
func (e *executor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	return jobGetByID(ctx, e.db, id)
}
func (e *executor) JobList(ctx context.Context, params driver.JobListParams) ([]driver.JobRow, error) {
	return jobList(ctx, e.db, params)
}
func (e *executor) QueueStats(ctx context.Context, queue string) (driver.QueueStats, error) {
	return queueStats(ctx, e.db, queue)
}
func (e *executor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	return jobFetchBatch(ctx, e.db, e.clk, params)
}
func (e *executor) JobRescueStuck(ctx context.Context, params driver.JobRescueParams) (int64, error) {
	return jobRescueStuck(ctx, e.db, params)
}
func (e *executor) JobHeartbeat(ctx context.Context, params driver.JobHeartbeatParams) (bool, error) {
	return jobHeartbeat(ctx, e.db, params)
}
func (e *executor) JobMarkStarted(ctx context.Context, params driver.JobMarkStartedParams) (bool, error) {
	return jobMarkStarted(ctx, e.db, params)
}
func (e *executor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(ctx, e.db, e.clk, params)
}
func (e *executor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(ctx, e.db, e.clk, id)
}
func (e *executor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(ctx, e.db, id)
}
func (e *executor) JobResultPut(ctx context.Context, jobID string, value []byte) error {
	return jobResultPut(ctx, e.db, e.clk, jobID, value)
}
func (e *executor) JobResultGet(ctx context.Context, jobID string) ([]byte, error) {
	return jobResultGet(ctx, e.db, jobID)
}
func (e *executor) JobResultDelete(ctx context.Context, jobID string) error {
	return jobResultDelete(ctx, e.db, jobID)
}
func (e *executor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(ctx, e.db, params)
}
func (e *executor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(ctx, e.db, e.clk, name)
}
func (e *executor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(ctx, e.db, e.clk, name, true)
}
func (e *executor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(ctx, e.db, e.clk, name, false)
}
func (e *executor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(ctx, e.db, params)
}
func (e *executor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(ctx, e.db, e.clk, params)
}
func (e *executor) LeaderResign(ctx context.Context, params driver.LeaderResignParams) error {
	return leaderResign(ctx, e.db, params)
}
func (e *executor) ScheduleCursorGetOrCreate(ctx context.Context, params driver.ScheduleCursorCreateParams) (driver.ScheduleCursorResult, error) {
	return scheduleCursorGetOrCreate(ctx, e.db, params)
}
func (e *executor) ScheduleCursorAdvance(ctx context.Context, params driver.ScheduleCursorAdvanceParams) (bool, error) {
	return scheduleCursorAdvance(ctx, e.db, params)
}

// ---- txExecutor (transactional) ----

type txExecutor struct {
	db      *mongo.Database
	session *mongo.Session
	owned   bool // the session was started by Begin and is ended on commit/rollback
	clk     clock.Clock
}

// in binds ctx to the session so MongoDB routes the operation through the
// transaction.
func (t *txExecutor) in(ctx context.Context) context.Context {
	return mongo.NewSessionContext(ctx, t.session)
}

func (t *txExecutor) Commit(ctx context.Context) error {
	if t.owned {
		defer t.session.EndSession(ctx)
	}
	return t.session.CommitTransaction(ctx)
}

func (t *txExecutor) Rollback(ctx context.Context) error {
	if t.owned {
		defer t.session.EndSession(ctx)
	}
	return t.session.AbortTransaction(ctx)
}

// All executor methods run inside the session through t.in(ctx).

func (t *txExecutor) Begin(ctx context.Context) (driver.ExecutorTx, error) {
	return nil, fmt.Errorf("nested transactions not supported")
}
func (t *txExecutor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertMany(t.in(ctx), t.db, t.clk, params)
}
func (t *txExecutor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	return jobGetByID(t.in(ctx), t.db, id)
}
func (t *txExecutor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	return jobFetchBatch(t.in(ctx), t.db, t.clk, params)
}
func (t *txExecutor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(t.in(ctx), t.db, t.clk, params)
}
func (t *txExecutor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(t.in(ctx), t.db, t.clk, id)
}
func (t *txExecutor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(t.in(ctx), t.db, id)
}
func (t *txExecutor) JobResultPut(ctx context.Context, jobID string, value []byte) error {
	return jobResultPut(t.in(ctx), t.db, t.clk, jobID, value)
}
func (t *txExecutor) JobResultGet(ctx context.Context, jobID string) ([]byte, error) {
	return jobResultGet(t.in(ctx), t.db, jobID)
}
func (t *txExecutor) JobResultDelete(ctx context.Context, jobID string) error {
	return jobResultDelete(t.in(ctx), t.db, jobID)
}
func (t *txExecutor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(t.in(ctx), t.db, params)
}
func (t *txExecutor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(t.in(ctx), t.db, t.clk, name)
}
func (t *txExecutor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(t.in(ctx), t.db, t.clk, name, true)
}
func (t *txExecutor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(t.in(ctx), t.db, t.clk, name, false)
}
func (t *txExecutor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(t.in(ctx), t.db, params)
}
func (t *txExecutor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(t.in(ctx), t.db, t.clk, params)
}
func (t *txExecutor) LeaderResign(ctx context.Context, params driver.LeaderResignParams) error {
	return leaderResign(t.in(ctx), t.db, params)
}
func (t *txExecutor) ScheduleCursorGetOrCreate(ctx context.Context, params driver.ScheduleCursorCreateParams) (driver.ScheduleCursorResult, error) {
	return scheduleCursorGetOrCreate(t.in(ctx), t.db, params)
}
func (t *txExecutor) ScheduleCursorAdvance(ctx context.Context, params driver.ScheduleCursorAdvanceParams) (bool, error) {
	return scheduleCursorAdvance(t.in(ctx), t.db, params)
}

// ---- core functions ----

func jobInsertMany(ctx context.Context, db *mongo.Database, clk clock.Clock, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	col := db.Collection(jobsCollection)
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

		doc := jobDoc{
			Queue:      p.Queue,
			Kind:       p.Kind,
			Args:       string(p.Args),
			State:      string(state),
			Priority:   p.Priority,
			RunAt:      runAt,
			CreatedAt:  now,
			MaxRetry:   p.MaxRetry,
			TimeoutMs:  p.Timeout.Milliseconds(),
			UniqueKey:  p.UniqueKey,
			Tags:       p.Tags,
			Errors:     []attemptError{},
			PipelineID: p.PipelineID,
		}
		if doc.Tags == nil {
			doc.Tags = []string{}
		}

		if p.UniqueKey != "" {
			// Atomic upsert: the canonical key already captures the requested scope.
			filter := bson.M{"unique_key": p.UniqueKey}
			update := bson.M{"$setOnInsert": doc}
			res, err := col.UpdateOne(ctx, filter, update, options.UpdateOne().SetUpsert(true))
			if err != nil {
				if mongo.IsDuplicateKeyError(err) {
					results[i] = driver.JobInsertResult{UniqueSkip: true}
					continue
				}
				return nil, fmt.Errorf("insert job %d: %w", i, err)
			}
			if res.UpsertedCount == 0 {
				results[i] = driver.JobInsertResult{UniqueSkip: true}
				continue
			}
			oid := res.UpsertedID.(bson.ObjectID)
			doc.ID = oid
		} else {
			res, err := col.InsertOne(ctx, doc)
			if err != nil {
				return nil, fmt.Errorf("insert job %d: %w", i, err)
			}
			doc.ID = res.InsertedID.(bson.ObjectID)
		}

		results[i] = driver.JobInsertResult{Job: docToRow(doc)}
	}
	return results, nil
}

func jobGetByID(ctx context.Context, db *mongo.Database, id string) (*driver.JobRow, error) {
	oid, err := bson.ObjectIDFromHex(id)
	if err != nil {
		return nil, fmt.Errorf("invalid job id %q: %w", id, err)
	}
	var doc jobDoc
	if err := db.Collection(jobsCollection).FindOne(ctx, bson.M{"_id": oid}).Decode(&doc); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("%w: job %q", driver.ErrNotFound, id)
		}
		return nil, err
	}
	row := docToRow(doc)
	return row, nil
}

func jobList(ctx context.Context, db *mongo.Database, params driver.JobListParams) ([]driver.JobRow, error) {
	filter := bson.M{}
	if params.Queue != "" {
		filter["queue"] = params.Queue
	}
	if params.State != "" {
		filter["state"] = string(params.State)
	}
	if params.Kind != "" {
		filter["kind"] = params.Kind
	}
	if params.Cursor != "" {
		cursorAt, cursorID, err := driver.DecodeJobCursor(params.Cursor)
		if err != nil {
			return nil, err
		}
		cursorOID, err := bson.ObjectIDFromHex(cursorID)
		if err != nil {
			return nil, fmt.Errorf("%w: invalid mongodb job id", driver.ErrInvalidCursor)
		}
		filter["$or"] = bson.A{
			bson.M{"created_at": bson.M{"$lt": cursorAt}},
			bson.M{"created_at": cursorAt, "_id": bson.M{"$lt": cursorOID}},
		}
	}
	limit := params.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	cursor, err := db.Collection(jobsCollection).Find(ctx, filter,
		options.Find().SetSort(bson.D{{Key: "created_at", Value: -1}, {Key: "_id", Value: -1}}).SetLimit(int64(limit)),
	)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(ctx)
	var docs []jobDoc
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, err
	}
	rows := make([]driver.JobRow, 0, len(docs))
	for _, doc := range docs {
		rows = append(rows, *docToRow(doc))
	}
	return rows, nil
}

func queueStats(ctx context.Context, db *mongo.Database, queue string) (driver.QueueStats, error) {
	pipeline := mongo.Pipeline{
		{{Key: "$match", Value: bson.D{{Key: "queue", Value: queue}}}},
		{{Key: "$group", Value: bson.D{{Key: "_id", Value: "$state"}, {Key: "count", Value: bson.D{{Key: "$sum", Value: 1}}}}}},
	}
	cursor, err := db.Collection(jobsCollection).Aggregate(ctx, pipeline)
	if err != nil {
		return driver.QueueStats{}, err
	}
	defer cursor.Close(ctx)
	stats := driver.QueueStats{Queue: queue, States: make(map[driver.JobState]int64)}
	for cursor.Next(ctx) {
		var row struct {
			State string `bson:"_id"`
			Count int64  `bson:"count"`
		}
		if err := cursor.Decode(&row); err != nil {
			return driver.QueueStats{}, err
		}
		stats.States[driver.JobState(row.State)] = row.Count
		stats.Total += row.Count
	}
	return stats, cursor.Err()
}

func jobFetchBatch(ctx context.Context, db *mongo.Database, clk clock.Clock, params driver.FetchParams) ([]driver.JobRow, error) {
	// Check if queue is paused.
	var q queueDoc
	err := db.Collection(queuesCollection).FindOne(ctx, bson.M{"name": params.Queue}).Decode(&q)
	if err == nil && q.Paused {
		return nil, nil
	}

	col := db.Collection(jobsCollection)
	now := clk.Now()

	filter := bson.M{
		"queue":  params.Queue,
		"state":  bson.M{"$in": bson.A{string(driver.JobStateAvailable), string(driver.JobStateScheduled)}},
		"run_at": bson.M{"$lte": now},
	}
	set := bson.M{
		"state":        string(driver.JobStateRunning),
		"attempted_at": now,
		"worker_id":    params.WorkerID,
	}
	if params.LeaseDuration > 0 {
		set["lease_expires_at"] = now.Add(params.LeaseDuration)
	}
	update := bson.M{
		"$set":   set,
		"$inc":   bson.M{"attempt_num": 1},
		"$unset": bson.M{"started_at": ""},
	}
	if params.LeaseDuration <= 0 {
		update["$unset"].(bson.M)["lease_expires_at"] = ""
	}
	findOpts := options.FindOneAndUpdate().
		SetSort(bson.D{{Key: "priority", Value: -1}, {Key: "run_at", Value: 1}, {Key: "created_at", Value: 1}, {Key: "_id", Value: 1}}).
		SetReturnDocument(options.After)

	if params.Limit > 1 {
		return fetchMany(ctx, col, filter, update, params.Limit)
	}
	var doc jobDoc
	if err := col.FindOneAndUpdate(ctx, filter, update, findOpts).Decode(&doc); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, nil
		}
		return nil, err
	}
	return []driver.JobRow{*docToRow(doc)}, nil
}

// fetchMany claims up to limit jobs in three round trips instead of one
// findOneAndUpdate per job: read candidate ids in fetch order, claim them
// with one updateMany that re-checks the ready filter (each document update
// is atomic, so a job goes to exactly one fetcher) and stamps a per-call
// claim token, then read back the documents carrying that token.
func fetchMany(ctx context.Context, col *mongo.Collection, filter, update bson.M, limit int) ([]driver.JobRow, error) {
	cur, err := col.Find(ctx, filter, options.Find().
		SetSort(bson.D{{Key: "priority", Value: -1}, {Key: "run_at", Value: 1}, {Key: "created_at", Value: 1}, {Key: "_id", Value: 1}}).
		SetLimit(int64(limit)).
		SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return nil, fmt.Errorf("find ready jobs: %w", err)
	}
	var candidates []struct {
		ID bson.ObjectID `bson:"_id"`
	}
	if err := cur.All(ctx, &candidates); err != nil {
		return nil, fmt.Errorf("find ready jobs: %w", err)
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	ids := make(bson.A, len(candidates))
	for i, c := range candidates {
		ids[i] = c.ID
	}
	token := bson.NewObjectID().Hex()
	claimFilter := bson.M{"_id": bson.M{"$in": ids}}
	for k, v := range filter {
		claimFilter[k] = v
	}
	claimUpdate := bson.M{}
	for k, v := range update {
		claimUpdate[k] = v
	}
	set := bson.M{"claim_token": token}
	for k, v := range update["$set"].(bson.M) {
		set[k] = v
	}
	claimUpdate["$set"] = set
	if _, err := col.UpdateMany(ctx, claimFilter, claimUpdate); err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	cur, err = col.Find(ctx, bson.M{"_id": bson.M{"$in": ids}, "claim_token": token, "state": string(driver.JobStateRunning)},
		options.Find().SetSort(bson.D{{Key: "priority", Value: -1}, {Key: "run_at", Value: 1}, {Key: "created_at", Value: 1}, {Key: "_id", Value: 1}}))
	if err != nil {
		return nil, fmt.Errorf("read claimed jobs: %w", err)
	}
	var docs []jobDoc
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("read claimed jobs: %w", err)
	}
	rows := make([]driver.JobRow, len(docs))
	for i, doc := range docs {
		rows[i] = *docToRow(doc)
	}
	return rows, nil
}

func jobRescueStuck(ctx context.Context, db *mongo.Database, params driver.JobRescueParams) (int64, error) {
	result, err := db.Collection(jobsCollection).UpdateMany(ctx,
		bson.M{
			"queue": params.Queue,
			"state": string(driver.JobStateRunning),
			"$or": bson.A{
				bson.M{"lease_expires_at": bson.M{"$lte": params.At}},
				bson.M{"lease_expires_at": nil, "attempted_at": bson.M{"$lte": params.Before}},
			},
		},
		bson.M{
			"$set":   bson.M{"state": string(driver.JobStateAvailable), "attempted_at": nil},
			"$unset": bson.M{"worker_id": "", "lease_expires_at": ""},
		},
	)
	if err != nil {
		return 0, err
	}
	return result.ModifiedCount, nil
}

func jobHeartbeat(ctx context.Context, db *mongo.Database, params driver.JobHeartbeatParams) (bool, error) {
	oid, err := bson.ObjectIDFromHex(params.ID)
	if err != nil {
		return false, fmt.Errorf("invalid job id %q: %w", params.ID, err)
	}
	leaseExpiresAt := params.LeaseExpiresAt
	if leaseExpiresAt.IsZero() {
		leaseExpiresAt = params.At
	}
	result, err := db.Collection(jobsCollection).UpdateOne(ctx, bson.M{
		"_id": oid, "state": string(driver.JobStateRunning),
		"worker_id": params.WorkerID, "attempt_num": params.Attempt,
	}, bson.M{"$set": bson.M{"lease_expires_at": leaseExpiresAt.UTC()}})
	if err != nil {
		return false, err
	}
	return result.ModifiedCount == 1, nil
}

func jobMarkStarted(ctx context.Context, db *mongo.Database, params driver.JobMarkStartedParams) (bool, error) {
	oid, err := bson.ObjectIDFromHex(params.ID)
	if err != nil {
		return false, fmt.Errorf("invalid job id %q: %w", params.ID, err)
	}
	result, err := db.Collection(jobsCollection).UpdateOne(ctx, bson.M{
		"_id": oid, "state": string(driver.JobStateRunning),
		"worker_id": params.WorkerID, "attempt_num": params.Attempt,
	}, bson.M{"$set": bson.M{"started_at": params.At.UTC()}})
	if err != nil {
		return false, err
	}
	return result.MatchedCount == 1, nil
}

func jobSetStateIfRunning(ctx context.Context, db *mongo.Database, clk clock.Clock, params driver.JobSetStateParams) error {
	oid, err := bson.ObjectIDFromHex(params.ID)
	if err != nil {
		return fmt.Errorf("invalid job id %q: %w", params.ID, err)
	}
	filter := bson.M{"_id": oid, "state": string(driver.JobStateRunning)}
	if params.ExpectedWorkerID != "" {
		filter["worker_id"] = params.ExpectedWorkerID
	}
	if params.ExpectedAttempt > 0 {
		filter["attempt_num"] = params.ExpectedAttempt
	}

	if params.Yield {
		result, updateErr := db.Collection(jobsCollection).UpdateOne(ctx,
			filter,
			bson.M{
				"$set":   bson.M{"state": string(driver.JobStateAvailable), "attempted_at": nil},
				"$inc":   bson.M{"attempt_num": -1},
				"$unset": bson.M{"worker_id": "", "lease_expires_at": ""},
			},
		)
		if updateErr == nil && result.ModifiedCount == 0 {
			return fmt.Errorf("%w: job %q", driver.ErrStaleClaim, params.ID)
		}
		return updateErr
	}

	now := clk.Now()
	set := bson.M{"state": string(params.State)}
	unset := bson.M{"worker_id": "", "lease_expires_at": ""}
	push := bson.M{}

	switch params.State {
	case driver.JobStateCompleted, driver.JobStateDiscarded, driver.JobStateCancelled:
		set["finalized_at"] = now
		var current struct {
			UniqueKey string `bson:"unique_key"`
		}
		if err := db.Collection(jobsCollection).FindOne(ctx, filter,
			options.FindOne().SetProjection(bson.M{"unique_key": 1})).Decode(&current); err == mongo.ErrNoDocuments {
			return fmt.Errorf("%w: job %q", driver.ErrStaleClaim, params.ID)
		} else if err != nil {
			return err
		}
		if !driver.IsPermanentUniqueKey(current.UniqueKey) {
			unset["unique_key"] = "" // free uniqueness slot
		}
	case driver.JobStateRetryable:
		if !params.RetryAt.IsZero() {
			set["run_at"] = params.RetryAt
		}
		set["state"] = string(driver.JobStateAvailable) // retryable → available with new run_at
	}

	if params.Err != nil {
		stored := attemptError{
			At:      now,
			Attempt: params.Attempt,
			Message: *params.Err,
		}
		if params.Trace != nil {
			stored.Trace = *params.Trace
		}
		push["errors"] = stored
	}

	update := bson.M{"$set": set}
	if len(unset) > 0 {
		update["$unset"] = unset
	}
	if len(push) > 0 {
		update["$push"] = push
	}

	result, err := db.Collection(jobsCollection).UpdateOne(ctx,
		filter,
		update,
	)
	if err == nil && result.ModifiedCount == 0 {
		return fmt.Errorf("%w: job %q", driver.ErrStaleClaim, params.ID)
	}
	return err
}

func jobCancel(ctx context.Context, db *mongo.Database, clk clock.Clock, id string) error {
	oid, err := bson.ObjectIDFromHex(id)
	if err != nil {
		return fmt.Errorf("invalid job id %q: %w", id, err)
	}
	filter := bson.M{
		"_id": oid, "state": bson.M{"$in": bson.A{string(driver.JobStateAvailable), string(driver.JobStateScheduled)}},
	}
	var current struct {
		UniqueKey string `bson:"unique_key"`
	}
	if err := db.Collection(jobsCollection).FindOne(ctx, filter,
		options.FindOne().SetProjection(bson.M{"unique_key": 1})).Decode(&current); err == mongo.ErrNoDocuments {
		if _, getErr := jobGetByID(ctx, db, id); getErr != nil {
			return getErr
		}
		return fmt.Errorf("%w: job %q cannot be cancelled in its current state", driver.ErrConflict, id)
	} else if err != nil {
		return err
	}
	update := bson.M{"$set": bson.M{"state": string(driver.JobStateCancelled), "finalized_at": clk.Now()}}
	if !driver.IsPermanentUniqueKey(current.UniqueKey) {
		update["$unset"] = bson.M{"unique_key": ""}
	}
	_, err = db.Collection(jobsCollection).UpdateOne(ctx, filter, update)
	return err
}

func jobDelete(ctx context.Context, db *mongo.Database, id string) error {
	oid, err := bson.ObjectIDFromHex(id)
	if err != nil {
		return fmt.Errorf("invalid job id %q: %w", id, err)
	}
	if _, err := db.Collection(resultsCollection).DeleteOne(ctx, bson.M{"_id": oid}); err != nil {
		return err
	}
	result, err := db.Collection(jobsCollection).DeleteOne(ctx, bson.M{"_id": oid})
	if err == nil && result.DeletedCount == 0 {
		return fmt.Errorf("%w: job %q", driver.ErrNotFound, id)
	}
	return err
}

type resultDoc struct {
	ID        bson.ObjectID `bson:"_id"`
	Value     []byte        `bson:"value"`
	UpdatedAt time.Time     `bson:"updated_at"`
}

// jobResultPut upserts a result for an existing job. Results share the job's
// ObjectID and are removed by jobDelete.
func jobResultPut(ctx context.Context, db *mongo.Database, clk clock.Clock, jobID string, value []byte) error {
	oid, err := bson.ObjectIDFromHex(jobID)
	if err != nil {
		return fmt.Errorf("invalid job id %q: %w", jobID, err)
	}
	n, err := db.Collection(jobsCollection).CountDocuments(ctx, bson.M{"_id": oid}, options.Count().SetLimit(1))
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("%w: job %q", driver.ErrNotFound, jobID)
	}
	if value == nil {
		value = []byte{}
	}
	_, err = db.Collection(resultsCollection).ReplaceOne(ctx, bson.M{"_id": oid},
		resultDoc{ID: oid, Value: value, UpdatedAt: clk.Now()}, options.Replace().SetUpsert(true))
	return err
}

func jobResultGet(ctx context.Context, db *mongo.Database, jobID string) ([]byte, error) {
	oid, err := bson.ObjectIDFromHex(jobID)
	if err != nil {
		return nil, fmt.Errorf("invalid job id %q: %w", jobID, err)
	}
	var doc resultDoc
	err = db.Collection(resultsCollection).FindOne(ctx, bson.M{"_id": oid}).Decode(&doc)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("%w: result for job %q", driver.ErrNotFound, jobID)
	}
	if err != nil {
		return nil, err
	}
	return doc.Value, nil
}

func jobResultDelete(ctx context.Context, db *mongo.Database, jobID string) error {
	oid, err := bson.ObjectIDFromHex(jobID)
	if err != nil {
		return fmt.Errorf("invalid job id %q: %w", jobID, err)
	}
	_, err = db.Collection(resultsCollection).DeleteOne(ctx, bson.M{"_id": oid})
	return err
}

func jobReschedule(ctx context.Context, db *mongo.Database, params driver.RescheduleParams) error {
	oid, err := bson.ObjectIDFromHex(params.ID)
	if err != nil {
		return fmt.Errorf("invalid job id %q: %w", params.ID, err)
	}
	result, err := db.Collection(jobsCollection).UpdateOne(ctx,
		bson.M{"_id": oid},
		bson.M{"$set": bson.M{"run_at": params.RunAt, "state": string(driver.JobStateScheduled)}},
	)
	if err == nil && result.MatchedCount == 0 {
		return fmt.Errorf("%w: job %q", driver.ErrNotFound, params.ID)
	}
	return err
}

func queueGet(ctx context.Context, db *mongo.Database, clk clock.Clock, name string) (*driver.QueueRow, error) {
	now := clk.Now()
	col := db.Collection(queuesCollection)

	// Upsert: create if not exists.
	filter := bson.M{"name": name}
	update := bson.M{
		"$setOnInsert": queueDoc{Name: name, Paused: false, CreatedAt: now, UpdatedAt: now},
	}
	var doc queueDoc
	after := options.After
	err := col.FindOneAndUpdate(ctx, filter, update,
		options.FindOneAndUpdate().SetUpsert(true).SetReturnDocument(after),
	).Decode(&doc)
	if err != nil {
		return nil, err
	}
	return &driver.QueueRow{
		Name:      doc.Name,
		Paused:    doc.Paused,
		CreatedAt: doc.CreatedAt,
		UpdatedAt: doc.UpdatedAt,
	}, nil
}

func queueSetPaused(ctx context.Context, db *mongo.Database, clk clock.Clock, name string, paused bool) error {
	now := clk.Now()
	col := db.Collection(queuesCollection)
	_, err := col.UpdateOne(ctx,
		bson.M{"name": name},
		bson.M{"$set": bson.M{"paused": paused, "updated_at": now}},
		options.UpdateOne().SetUpsert(true),
	)
	return err
}

func queueList(ctx context.Context, db *mongo.Database, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	limit := int64(params.Limit)
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	filter := bson.M{}
	if params.Cursor != "" {
		cursor, err := driver.DecodeQueueCursor(params.Cursor)
		if err != nil {
			return nil, err
		}
		filter["name"] = bson.M{"$gt": cursor}
	}
	cur, err := db.Collection(queuesCollection).Find(ctx,
		filter,
		options.Find().SetLimit(limit).SetSort(bson.D{{Key: "name", Value: 1}}),
	)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)

	var rows []*driver.QueueRow
	for cur.Next(ctx) {
		var doc queueDoc
		if err := cur.Decode(&doc); err != nil {
			return nil, err
		}
		rows = append(rows, &driver.QueueRow{
			Name:      doc.Name,
			Paused:    doc.Paused,
			CreatedAt: doc.CreatedAt,
			UpdatedAt: doc.UpdatedAt,
		})
	}
	return rows, cur.Err()
}

func leaderAttemptElect(ctx context.Context, db *mongo.Database, clk clock.Clock, params driver.LeaderElectParams) (bool, error) {
	now := clk.Now()
	newExpiry := now.Add(params.TTL)
	col := db.Collection(leadersCollection)

	// Update existing record if it's expired or belongs to us.
	res, err := col.UpdateOne(ctx,
		bson.M{
			"_id": params.Name,
			"$or": bson.A{
				bson.M{"expires_at": bson.M{"$lte": now}},
				bson.M{"worker_id": params.WorkerID},
			},
		},
		bson.M{"$set": bson.M{"worker_id": params.WorkerID, "elected_at": now, "expires_at": newExpiry}},
	)
	if err != nil {
		return false, err
	}
	if res.MatchedCount > 0 {
		return true, nil
	}

	// No existing record — try to create one.
	_, err = col.InsertOne(ctx, leaderDoc{
		ID: params.Name, WorkerID: params.WorkerID, ElectedAt: now, ExpiresAt: newExpiry,
	})
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return false, nil // another worker won the race
		}
		return false, err
	}
	return true, nil
}

func leaderResign(ctx context.Context, db *mongo.Database, params driver.LeaderResignParams) error {
	_, err := db.Collection(leadersCollection).DeleteOne(ctx, bson.M{"_id": params.Name, "worker_id": params.WorkerID})
	return err
}

type scheduleCursorDoc struct {
	ID string    `bson:"_id"`
	At time.Time `bson:"cursor_at"`
}

func scheduleCursorGetOrCreate(ctx context.Context, db *mongo.Database, params driver.ScheduleCursorCreateParams) (driver.ScheduleCursorResult, error) {
	col := db.Collection(cursorsCollection)
	initial := params.InitialAt.UTC()
	created := true
	_, err := col.InsertOne(ctx, scheduleCursorDoc{ID: params.ID, At: initial})
	if err != nil {
		if !mongo.IsDuplicateKeyError(err) {
			return driver.ScheduleCursorResult{}, err
		}
		created = false
	}
	var doc scheduleCursorDoc
	if err := col.FindOne(ctx, bson.M{"_id": params.ID}).Decode(&doc); err != nil {
		return driver.ScheduleCursorResult{}, err
	}
	return driver.ScheduleCursorResult{At: doc.At.UTC(), Created: created}, nil
}

func scheduleCursorAdvance(ctx context.Context, db *mongo.Database, params driver.ScheduleCursorAdvanceParams) (bool, error) {
	result, err := db.Collection(cursorsCollection).UpdateOne(ctx,
		bson.M{"_id": params.ID, "cursor_at": params.Expected.UTC()},
		bson.M{"$set": bson.M{"cursor_at": params.Next.UTC()}})
	if err != nil {
		return false, err
	}
	return result.ModifiedCount == 1, nil
}

// ---- helpers ----

func docToRow(doc jobDoc) *driver.JobRow {
	var errs []driver.AttemptError
	for _, e := range doc.Errors {
		errs = append(errs, driver.AttemptError{At: e.At, Attempt: e.Attempt, Error: e.Message, Trace: e.Trace})
	}

	var timeout time.Duration
	if doc.TimeoutMs > 0 {
		timeout = time.Duration(doc.TimeoutMs) * time.Millisecond
	}

	// Normalise state: "retryable" stored as "available" (we convert at write time).
	state := driver.JobState(doc.State)

	// Parse tags — guard against nil stored by older docs.
	tags := doc.Tags
	if tags == nil {
		tags = []string{}
	}

	// Args must be valid JSON; fall back to empty object if blank.
	args := []byte(doc.Args)
	if len(args) == 0 {
		args = []byte("{}")
	}

	return &driver.JobRow{
		ID:             doc.ID.Hex(),
		Queue:          doc.Queue,
		Kind:           doc.Kind,
		Args:           args,
		State:          state,
		Priority:       doc.Priority,
		RunAt:          doc.RunAt,
		CreatedAt:      doc.CreatedAt,
		AttemptedAt:    doc.AttemptedAt,
		StartedAt:      doc.StartedAt,
		LeaseExpiresAt: doc.LeaseExpiresAt,
		FinalizedAt:    doc.FinalizedAt,
		AttemptNum:     doc.AttemptNum,
		MaxRetry:       doc.MaxRetry,
		Timeout:        timeout,
		UniqueKey:      doc.UniqueKey,
		WorkerID:       doc.WorkerID,
		Tags:           tags,
		Errors:         errs,
		PipelineID:     doc.PipelineID,
	}
}

var (
	_ driver.JobResultStore = (*executor)(nil)
	_ driver.JobResultStore = (*txExecutor)(nil)
)

// JobPruneFinalized deletes up to params.Limit jobs of params.State finalized
// at or before params.Before, through the goncordia_jobs_finalized index.
func (e *executor) JobPruneFinalized(ctx context.Context, params driver.JobPruneParams) (int, error) {
	filter := bson.M{"state": string(params.State), "finalized_at": bson.M{"$lte": params.Before}}
	ids, err := expiredIDs(ctx, e.db.Collection(jobsCollection), filter, "finalized_at", params.Limit)
	if err != nil || len(ids) == 0 {
		return 0, err
	}
	// The filter is repeated so jobs revived since the lookup stay.
	filter["_id"] = bson.M{"$in": ids}
	deleted, err := e.db.Collection(jobsCollection).DeleteMany(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("prune jobs: %w", err)
	}
	// Results share the job's _id. A job revived in between loses only a
	// result it will write again when it runs.
	if _, err := e.db.Collection(resultsCollection).DeleteMany(ctx, bson.M{"_id": bson.M{"$in": ids}}); err != nil {
		return int(deleted.DeletedCount), fmt.Errorf("prune results of deleted jobs: %w", err)
	}
	return int(deleted.DeletedCount), nil
}

// JobResultPrune deletes up to params.Limit results written at or before
// params.Before, through the goncordia_job_results_updated index. A result is
// written moments before its job is finalized.
func (e *executor) JobResultPrune(ctx context.Context, params driver.JobPruneParams) (int, error) {
	results := e.db.Collection(resultsCollection)
	ids, err := expiredIDs(ctx, results, bson.M{"updated_at": bson.M{"$lte": params.Before}}, "updated_at", params.Limit)
	if err != nil || len(ids) == 0 {
		return 0, err
	}
	deleted, err := results.DeleteMany(ctx, bson.M{"_id": bson.M{"$in": ids}})
	if err != nil {
		return 0, fmt.Errorf("prune results: %w", err)
	}
	return int(deleted.DeletedCount), nil
}

func expiredIDs(ctx context.Context, coll *mongo.Collection, filter bson.M, sortKey string, limit int) ([]bson.ObjectID, error) {
	if limit <= 0 {
		limit = 500
	}
	cursor, err := coll.Find(ctx, filter, options.Find().
		SetSort(bson.D{{Key: sortKey, Value: 1}}).
		SetLimit(int64(limit)).
		SetProjection(bson.M{"_id": 1}))
	if err != nil {
		return nil, fmt.Errorf("select expired documents: %w", err)
	}
	var docs []struct {
		ID bson.ObjectID `bson:"_id"`
	}
	if err := cursor.All(ctx, &docs); err != nil {
		return nil, err
	}
	ids := make([]bson.ObjectID, len(docs))
	for i, doc := range docs {
		ids[i] = doc.ID
	}
	return ids, nil
}

var _ driver.JobPruner = (*executor)(nil)
