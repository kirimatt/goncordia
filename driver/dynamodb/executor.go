package dynamodbdriver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"

	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/driver"
)

// ---- executor ----

type executor struct {
	svc *dynamodb.Client
	clk clock.Clock
}

func (e *executor) Begin(_ context.Context) (driver.ExecutorTx, error) {
	return nil, fmt.Errorf("%w: dynamodb transactions", driver.ErrUnsupported)
}

func (e *executor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertMany(ctx, e.svc, e.clk, params)
}
func (e *executor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	return jobGetByID(ctx, e.svc, id)
}
func (e *executor) JobList(ctx context.Context, params driver.JobListParams) ([]driver.JobRow, error) {
	return jobList(ctx, e.svc, params)
}
func (e *executor) QueueStats(ctx context.Context, queue string) (driver.QueueStats, error) {
	return queueStats(ctx, e.svc, queue)
}
func (e *executor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	return jobFetchBatch(ctx, e.svc, e.clk, params)
}
func (e *executor) JobRescueStuck(ctx context.Context, params driver.JobRescueParams) (int64, error) {
	return jobRescueStuck(ctx, e.svc, params)
}
func (e *executor) JobHeartbeat(ctx context.Context, params driver.JobHeartbeatParams) (bool, error) {
	return jobHeartbeat(ctx, e.svc, params)
}
func (e *executor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(ctx, e.svc, e.clk, params)
}
func (e *executor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(ctx, e.svc, e.clk, id)
}
func (e *executor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(ctx, e.svc, id)
}
func (e *executor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(ctx, e.svc, params)
}
func (e *executor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(ctx, e.svc, name)
}
func (e *executor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(ctx, e.svc, e.clk, name, true)
}
func (e *executor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(ctx, e.svc, e.clk, name, false)
}
func (e *executor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(ctx, e.svc, params)
}
func (e *executor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(ctx, e.svc, e.clk, params)
}
func (e *executor) LeaderResign(ctx context.Context, params driver.LeaderResignParams) error {
	return leaderResign(ctx, e.svc, params)
}
func (e *executor) ScheduleCursorGetOrCreate(ctx context.Context, params driver.ScheduleCursorCreateParams) (driver.ScheduleCursorResult, error) {
	return scheduleCursorGetOrCreate(ctx, e.svc, params)
}
func (e *executor) ScheduleCursorAdvance(ctx context.Context, params driver.ScheduleCursorAdvanceParams) (bool, error) {
	return scheduleCursorAdvance(ctx, e.svc, params)
}

// ---- txExecutor (no-op tx — DynamoDB has no cross-table transactions) ----

type txExecutor struct{ executor }

func (t *txExecutor) Commit(_ context.Context) error   { return nil }
func (t *txExecutor) Rollback(_ context.Context) error { return nil }
func (t *txExecutor) Begin(_ context.Context) (driver.ExecutorTx, error) {
	return nil, fmt.Errorf("nested transactions not supported")
}
func (t *txExecutor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertMany(ctx, t.svc, t.clk, params)
}
func (t *txExecutor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	return jobGetByID(ctx, t.svc, id)
}
func (t *txExecutor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	return jobFetchBatch(ctx, t.svc, t.clk, params)
}
func (t *txExecutor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(ctx, t.svc, t.clk, params)
}
func (t *txExecutor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(ctx, t.svc, t.clk, id)
}
func (t *txExecutor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(ctx, t.svc, id)
}
func (t *txExecutor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(ctx, t.svc, params)
}
func (t *txExecutor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(ctx, t.svc, name)
}
func (t *txExecutor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(ctx, t.svc, t.clk, name, true)
}
func (t *txExecutor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(ctx, t.svc, t.clk, name, false)
}
func (t *txExecutor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(ctx, t.svc, params)
}
func (t *txExecutor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(ctx, t.svc, t.clk, params)
}
func (t *txExecutor) LeaderResign(ctx context.Context, params driver.LeaderResignParams) error {
	return leaderResign(ctx, t.svc, params)
}
func (t *txExecutor) ScheduleCursorGetOrCreate(ctx context.Context, params driver.ScheduleCursorCreateParams) (driver.ScheduleCursorResult, error) {
	return scheduleCursorGetOrCreate(ctx, t.svc, params)
}
func (t *txExecutor) ScheduleCursorAdvance(ctx context.Context, params driver.ScheduleCursorAdvanceParams) (bool, error) {
	return scheduleCursorAdvance(ctx, t.svc, params)
}

// ---- job row ----

type dynamoJob struct {
	ID             string   `dynamodbav:"id"`
	Queue          string   `dynamodbav:"queue"`
	Kind           string   `dynamodbav:"kind"`
	Args           []byte   `dynamodbav:"args"`
	State          string   `dynamodbav:"state"`
	QueueState     string   `dynamodbav:"queue_state"` // "{queue}#{state}" — GSI PK
	Priority       int      `dynamodbav:"priority"`
	RunAt          string   `dynamodbav:"run_at"` // RFC3339Nano — GSI SK
	CreatedAt      string   `dynamodbav:"created_at"`
	AttemptedAt    string   `dynamodbav:"attempted_at"`
	LeaseExpiresAt string   `dynamodbav:"lease_expires_at,omitempty"`
	FinalizedAt    string   `dynamodbav:"finalized_at"`
	AttemptNum     int      `dynamodbav:"attempt_num"`
	MaxRetry       int      `dynamodbav:"max_retry"`
	TimeoutMs      int64    `dynamodbav:"timeout_ms"`
	UniqueKey      string   `dynamodbav:"unique_key"`
	WorkerID       string   `dynamodbav:"worker_id"`
	Tags           []string `dynamodbav:"tags"`
	ErrorsJSON     string   `dynamodbav:"errors_json"`
	Version        int64    `dynamodbav:"version"`
	PipelineID     string   `dynamodbav:"pipeline_id"`
}

const timeFmt = time.RFC3339Nano

func qsKey(queue, state string) string { return queue + "#" + state }

func parseTime(s string) time.Time {
	t, _ := time.Parse(timeFmt, s)
	return t.UTC()
}

func jobFromDynamo(j dynamoJob) *driver.JobRow {
	row := &driver.JobRow{
		ID:         j.ID,
		Queue:      j.Queue,
		Kind:       j.Kind,
		Args:       j.Args,
		State:      driver.JobState(j.State),
		Priority:   j.Priority,
		RunAt:      parseTime(j.RunAt),
		CreatedAt:  parseTime(j.CreatedAt),
		AttemptNum: j.AttemptNum,
		MaxRetry:   j.MaxRetry,
		Timeout:    time.Duration(j.TimeoutMs) * time.Millisecond,
		UniqueKey:  j.UniqueKey,
		WorkerID:   j.WorkerID,
		Tags:       j.Tags,
		Errors:     unmarshalErrors(j.ErrorsJSON),
		PipelineID: j.PipelineID,
	}
	if j.AttemptedAt != "" {
		t := parseTime(j.AttemptedAt)
		row.AttemptedAt = &t
	}
	if j.LeaseExpiresAt != "" {
		t := parseTime(j.LeaseExpiresAt)
		row.LeaseExpiresAt = &t
	}
	if j.FinalizedAt != "" {
		t := parseTime(j.FinalizedAt)
		row.FinalizedAt = &t
	}
	return row
}

// ---- error serialization ----

type storedError struct {
	At      int64  `json:"at_ms"`
	Attempt int    `json:"attempt"`
	Error   string `json:"error"`
	Trace   string `json:"trace,omitempty"`
}

func marshalErrors(errs []driver.AttemptError) string {
	if len(errs) == 0 {
		return "[]"
	}
	out := make([]storedError, len(errs))
	for i, e := range errs {
		out[i] = storedError{At: e.At.UnixMilli(), Attempt: e.Attempt, Error: e.Error, Trace: e.Trace}
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
		out[i] = driver.AttemptError{At: time.UnixMilli(e.At).UTC(), Attempt: e.Attempt, Error: e.Error, Trace: e.Trace}
	}
	return out
}

// ---- selectJob ----

func selectJob(ctx context.Context, svc *dynamodb.Client, id string) (*dynamoJob, error) {
	out, err := svc.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableJobs),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: id},
		},
	})
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
		return nil, nil
	}
	var j dynamoJob
	if err := attributevalue.UnmarshalMap(out.Item, &j); err != nil {
		return nil, err
	}
	return &j, nil
}

// ---- JobInsertMany ----

func jobInsertMany(ctx context.Context, svc *dynamodb.Client, clk clock.Clock, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
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

		j := dynamoJob{
			ID:         id,
			Queue:      p.Queue,
			Kind:       p.Kind,
			Args:       p.Args,
			State:      string(state),
			QueueState: qsKey(p.Queue, string(state)),
			Priority:   p.Priority,
			RunAt:      runAt.UTC().Format(timeFmt),
			CreatedAt:  now.UTC().Format(timeFmt),
			AttemptNum: 0,
			MaxRetry:   p.MaxRetry,
			TimeoutMs:  p.Timeout.Milliseconds(),
			UniqueKey:  p.UniqueKey,
			Tags:       tags,
			ErrorsJSON: "[]",
			Version:    1,
			PipelineID: p.PipelineID,
		}

		item, err := attributevalue.MarshalMap(j)
		if err != nil {
			return nil, err
		}
		if p.UniqueKey != "" {
			_, err = svc.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
				TransactItems: []types.TransactWriteItem{
					{Put: &types.Put{TableName: aws.String(tableJobs), Item: item}},
					{Put: &types.Put{
						TableName: aws.String(tableUniq),
						Item: map[string]types.AttributeValue{
							"pk": &types.AttributeValueMemberS{Value: p.UniqueKey}, "job_id": &types.AttributeValueMemberS{Value: id},
						},
						ConditionExpression:      aws.String("attribute_not_exists(#pk)"),
						ExpressionAttributeNames: map[string]string{"#pk": "pk"},
					}},
				},
			})
			if err != nil {
				var cancelled *types.TransactionCanceledException
				if errors.As(err, &cancelled) {
					results[i] = driver.JobInsertResult{UniqueSkip: true}
					continue
				}
				return nil, fmt.Errorf("insert unique job: %w", err)
			}
		} else if _, err := svc.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableJobs), Item: item,
		}); err != nil {
			return nil, fmt.Errorf("insert job: %w", err)
		}

		// Ensure queue metadata row exists.
		nowStr := now.UTC().Format(timeFmt)
		if _, err := svc.PutItem(ctx, &dynamodb.PutItemInput{
			TableName: aws.String(tableQueues),
			Item: map[string]types.AttributeValue{
				"name":       &types.AttributeValueMemberS{Value: p.Queue},
				"paused":     &types.AttributeValueMemberBOOL{Value: false},
				"created_at": &types.AttributeValueMemberS{Value: nowStr},
				"updated_at": &types.AttributeValueMemberS{Value: nowStr},
			},
			ConditionExpression:      aws.String("attribute_not_exists(#n)"),
			ExpressionAttributeNames: map[string]string{"#n": "name"},
		}); err != nil {
			var cce *types.ConditionalCheckFailedException
			if !errors.As(err, &cce) {
				return nil, fmt.Errorf("upsert queue: %w", err)
			}
		}

		results[i] = driver.JobInsertResult{Job: jobFromDynamo(j)}
	}
	return results, nil
}

// ---- JobGetByID ----

func jobGetByID(ctx context.Context, svc *dynamodb.Client, id string) (*driver.JobRow, error) {
	j, err := selectJob(ctx, svc, id)
	if err != nil {
		return nil, err
	}
	if j == nil {
		return nil, fmt.Errorf("%w: job %q", driver.ErrNotFound, id)
	}
	return jobFromDynamo(*j), nil
}

func scanJobs(ctx context.Context, svc *dynamodb.Client, params driver.JobListParams) ([]driver.JobRow, error) {
	var cursorAt time.Time
	var cursorID string
	if params.Cursor != "" {
		var err error
		cursorAt, cursorID, err = driver.DecodeJobCursor(params.Cursor)
		if err != nil {
			return nil, err
		}
	}
	var rows []driver.JobRow
	consume := func(items []map[string]types.AttributeValue) {
		for _, item := range items {
			var job dynamoJob
			if attributevalue.UnmarshalMap(item, &job) != nil {
				continue
			}
			row := jobFromDynamo(job)
			if params.Queue != "" && job.Queue != params.Queue || params.State != "" && driver.JobState(job.State) != params.State || params.Kind != "" && job.Kind != params.Kind || params.Cursor != "" && !driver.JobFollowsCursor(*row, cursorAt, cursorID) {
				continue
			}
			rows = append(rows, *row)
		}
	}
	if params.Queue != "" {
		states := []driver.JobState{params.State}
		if params.State == "" {
			states = []driver.JobState{
				driver.JobStateAvailable, driver.JobStateScheduled, driver.JobStateRunning,
				driver.JobStateCompleted, driver.JobStateDiscarded, driver.JobStateCancelled,
			}
		}
		for _, state := range states {
			var startKey map[string]types.AttributeValue
			for {
				out, err := svc.Query(ctx, &dynamodb.QueryInput{
					TableName: aws.String(tableJobs), IndexName: aws.String(gsiQueueState),
					KeyConditionExpression:   aws.String("#qs = :qs"),
					ExpressionAttributeNames: map[string]string{"#qs": "queue_state"},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":qs": &types.AttributeValueMemberS{Value: qsKey(params.Queue, string(state))},
					},
					ExclusiveStartKey: startKey,
				})
				if err != nil {
					return nil, err
				}
				consume(out.Items)
				if len(out.LastEvaluatedKey) == 0 {
					break
				}
				startKey = out.LastEvaluatedKey
			}
		}
	} else {
		var startKey map[string]types.AttributeValue
		for {
			out, err := svc.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(tableJobs), ExclusiveStartKey: startKey})
			if err != nil {
				return nil, err
			}
			consume(out.Items)
			if len(out.LastEvaluatedKey) == 0 {
				break
			}
			startKey = out.LastEvaluatedKey
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].CreatedAt.Equal(rows[j].CreatedAt) {
			return rows[i].CreatedAt.After(rows[j].CreatedAt)
		}
		return rows[i].ID > rows[j].ID
	})
	return rows, nil
}

func jobList(ctx context.Context, svc *dynamodb.Client, params driver.JobListParams) ([]driver.JobRow, error) {
	rows, err := scanJobs(ctx, svc, params)
	if err != nil {
		return nil, err
	}
	limit := params.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

func queueStats(ctx context.Context, svc *dynamodb.Client, queue string) (driver.QueueStats, error) {
	stats := driver.QueueStats{Queue: queue, States: make(map[driver.JobState]int64)}
	states := []driver.JobState{
		driver.JobStateAvailable, driver.JobStateScheduled, driver.JobStateRunning,
		driver.JobStateCompleted, driver.JobStateDiscarded, driver.JobStateCancelled,
	}
	for _, state := range states {
		var startKey map[string]types.AttributeValue
		for {
			out, err := svc.Query(ctx, &dynamodb.QueryInput{
				TableName: aws.String(tableJobs), IndexName: aws.String(gsiQueueState),
				KeyConditionExpression:   aws.String("#qs = :qs"),
				ExpressionAttributeNames: map[string]string{"#qs": "queue_state"},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":qs": &types.AttributeValueMemberS{Value: qsKey(queue, string(state))},
				},
				Select: types.SelectCount, ExclusiveStartKey: startKey,
			})
			if err != nil {
				return driver.QueueStats{}, err
			}
			stats.States[state] += int64(out.Count)
			stats.Total += int64(out.Count)
			if len(out.LastEvaluatedKey) == 0 {
				break
			}
			startKey = out.LastEvaluatedKey
		}
	}
	return stats, nil
}

// ---- JobFetchBatch ----

// JobFetchBatch claims up to params.Limit due available or scheduled jobs using GSI queries +
// conditional UpdateItem. Each UpdateItem checks both version and state, so
// only one worker wins per job.
func jobFetchBatch(ctx context.Context, svc *dynamodb.Client, clk clock.Clock, params driver.FetchParams) ([]driver.JobRow, error) {
	paused, err := isQueuePaused(ctx, svc, params.Queue)
	if err != nil {
		return nil, err
	}
	if paused {
		return nil, nil
	}

	now := clk.Now()
	nowStr := now.UTC().Format(timeFmt)
	leaseExpiresAt := ""
	if params.LeaseDuration > 0 {
		leaseExpiresAt = now.Add(params.LeaseDuration).UTC().Format(timeFmt)
	}

	candidates := make([]dynamoJob, 0, params.Limit*6)
	for _, state := range []driver.JobState{driver.JobStateAvailable, driver.JobStateScheduled} {
		var startKey map[string]types.AttributeValue
		for {
			out, queryErr := svc.Query(ctx, &dynamodb.QueryInput{
				TableName:              aws.String(tableJobs),
				IndexName:              aws.String(gsiQueueState),
				KeyConditionExpression: aws.String("#qs = :qs AND run_at <= :now"),
				ExpressionAttributeNames: map[string]string{
					"#qs": "queue_state",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":qs":  &types.AttributeValueMemberS{Value: qsKey(params.Queue, string(state))},
					":now": &types.AttributeValueMemberS{Value: nowStr},
				},
				ExclusiveStartKey: startKey,
			})
			if queryErr != nil {
				return nil, fmt.Errorf("query %s jobs: %w", state, queryErr)
			}
			for _, item := range out.Items {
				var j dynamoJob
				if err := attributevalue.UnmarshalMap(item, &j); err != nil {
					continue
				}
				candidates = append(candidates, j)
			}
			if len(out.LastEvaluatedKey) == 0 {
				break
			}
			startKey = out.LastEvaluatedKey
		}
	}

	// Portable order: highest priority, then earliest run_at/created_at/id.
	sort.Slice(candidates, func(i, k int) bool {
		if candidates[i].Priority != candidates[k].Priority {
			return candidates[i].Priority > candidates[k].Priority
		}
		ti := parseTime(candidates[i].RunAt)
		tk := parseTime(candidates[k].RunAt)
		if !ti.Equal(tk) {
			return ti.Before(tk)
		}
		ci := parseTime(candidates[i].CreatedAt)
		ck := parseTime(candidates[k].CreatedAt)
		if !ci.Equal(ck) {
			return ci.Before(ck)
		}
		return candidates[i].ID < candidates[k].ID
	})

	qsRunning := qsKey(params.Queue, string(driver.JobStateRunning))
	var claimed []driver.JobRow

	for _, c := range candidates {
		if len(claimed) >= params.Limit {
			break
		}

		updateExpression := `SET #state = :running, #qs = :qs_running, #wid = :wid, ` +
			`#aat = :now, #anum = #anum + :one, #ver = #ver + :one`
		names := map[string]string{
			"#state": "state", "#qs": "queue_state", "#wid": "worker_id", "#aat": "attempted_at",
			"#anum": "attempt_num", "#ver": "version", "#lease": "lease_expires_at",
		}
		values := map[string]types.AttributeValue{
			":running":    &types.AttributeValueMemberS{Value: string(driver.JobStateRunning)},
			":qs_running": &types.AttributeValueMemberS{Value: qsRunning},
			":wid":        &types.AttributeValueMemberS{Value: params.WorkerID}, ":now": &types.AttributeValueMemberS{Value: nowStr},
			":one": &types.AttributeValueMemberN{Value: "1"}, ":ver": &types.AttributeValueMemberN{Value: strconv.FormatInt(c.Version, 10)},
			":avail":     &types.AttributeValueMemberS{Value: string(driver.JobStateAvailable)},
			":scheduled": &types.AttributeValueMemberS{Value: string(driver.JobStateScheduled)},
		}
		if leaseExpiresAt != "" {
			updateExpression += `, #lease = :lease`
			values[":lease"] = &types.AttributeValueMemberS{Value: leaseExpiresAt}
		} else {
			updateExpression += ` REMOVE #lease`
		}
		_, claimErr := svc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(tableJobs),
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: c.ID},
			},
			UpdateExpression:          aws.String(updateExpression),
			ConditionExpression:       aws.String("#ver = :ver AND #state IN (:avail, :scheduled)"),
			ExpressionAttributeNames:  names,
			ExpressionAttributeValues: values,
		})
		if claimErr != nil {
			var cce *types.ConditionalCheckFailedException
			if errors.As(claimErr, &cce) {
				continue // another worker claimed it first
			}
			return nil, fmt.Errorf("claim update: %w", claimErr)
		}

		attemptedAt := now.UTC()
		var leaseExpiry *time.Time
		if leaseExpiresAt != "" {
			t := parseTime(leaseExpiresAt)
			leaseExpiry = &t
		}
		row := driver.JobRow{
			ID:             c.ID,
			Queue:          c.Queue,
			Kind:           c.Kind,
			Args:           c.Args,
			State:          driver.JobStateRunning,
			Priority:       c.Priority,
			RunAt:          parseTime(c.RunAt),
			CreatedAt:      parseTime(c.CreatedAt),
			AttemptedAt:    &attemptedAt,
			LeaseExpiresAt: leaseExpiry,
			AttemptNum:     c.AttemptNum + 1,
			MaxRetry:       c.MaxRetry,
			Timeout:        time.Duration(c.TimeoutMs) * time.Millisecond,
			Tags:           c.Tags,
			Errors:         unmarshalErrors(c.ErrorsJSON),
			UniqueKey:      c.UniqueKey,
			WorkerID:       params.WorkerID,
			PipelineID:     c.PipelineID,
		}
		claimed = append(claimed, row)
	}
	return claimed, nil
}

func jobRescueStuck(ctx context.Context, svc *dynamodb.Client, params driver.JobRescueParams) (int64, error) {
	var rescued int64
	var startKey map[string]types.AttributeValue
	for {
		out, err := svc.Query(ctx, &dynamodb.QueryInput{
			TableName:              aws.String(tableJobs),
			IndexName:              aws.String(gsiQueueState),
			KeyConditionExpression: aws.String("#qs = :qs"),
			ExpressionAttributeNames: map[string]string{
				"#qs": "queue_state",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":qs": &types.AttributeValueMemberS{Value: qsKey(params.Queue, string(driver.JobStateRunning))},
			},
			ExclusiveStartKey: startKey,
		})
		if err != nil {
			return rescued, err
		}
		for _, item := range out.Items {
			var job dynamoJob
			if err := attributevalue.UnmarshalMap(item, &job); err != nil || job.AttemptedAt == "" {
				continue
			}
			expired := job.LeaseExpiresAt != "" && !parseTime(job.LeaseExpiresAt).After(params.At)
			legacyExpired := job.LeaseExpiresAt == "" && !parseTime(job.AttemptedAt).After(params.Before)
			if !expired && !legacyExpired {
				continue
			}
			_, err := svc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
				TableName: aws.String(tableJobs),
				Key: map[string]types.AttributeValue{
					"id": &types.AttributeValueMemberS{Value: job.ID},
				},
				UpdateExpression:    aws.String("SET #state=:available, #qs=:qs_available, #wid=:empty, #ver=#ver+:one REMOVE #aat, #lease"),
				ConditionExpression: aws.String("#state=:running AND ((attribute_exists(#lease) AND #lease<=:at) OR (attribute_not_exists(#lease) AND #aat<=:before))"),
				ExpressionAttributeNames: map[string]string{
					"#state": "state", "#qs": "queue_state", "#wid": "worker_id", "#ver": "version", "#aat": "attempted_at", "#lease": "lease_expires_at",
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":available":    &types.AttributeValueMemberS{Value: string(driver.JobStateAvailable)},
					":qs_available": &types.AttributeValueMemberS{Value: qsKey(params.Queue, string(driver.JobStateAvailable))},
					":empty":        &types.AttributeValueMemberS{Value: ""},
					":one":          &types.AttributeValueMemberN{Value: "1"},
					":running":      &types.AttributeValueMemberS{Value: string(driver.JobStateRunning)},
					":before":       &types.AttributeValueMemberS{Value: params.Before.UTC().Format(timeFmt)},
					":at":           &types.AttributeValueMemberS{Value: params.At.UTC().Format(timeFmt)},
				},
			})
			if err != nil {
				var conditional *types.ConditionalCheckFailedException
				if errors.As(err, &conditional) {
					continue
				}
				return rescued, err
			}
			rescued++
		}
		if len(out.LastEvaluatedKey) == 0 {
			return rescued, nil
		}
		startKey = out.LastEvaluatedKey
	}
}

func jobHeartbeat(ctx context.Context, svc *dynamodb.Client, params driver.JobHeartbeatParams) (bool, error) {
	leaseExpiresAt := params.LeaseExpiresAt
	if leaseExpiresAt.IsZero() {
		leaseExpiresAt = params.At
	}
	_, err := svc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableJobs),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: params.ID},
		},
		UpdateExpression:    aws.String("SET #lease=:at, #ver=#ver+:one"),
		ConditionExpression: aws.String("#state=:running AND #wid=:wid AND #anum=:anum"),
		ExpressionAttributeNames: map[string]string{
			"#lease": "lease_expires_at", "#ver": "version", "#state": "state", "#wid": "worker_id", "#anum": "attempt_num",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":at":      &types.AttributeValueMemberS{Value: leaseExpiresAt.UTC().Format(timeFmt)},
			":one":     &types.AttributeValueMemberN{Value: "1"},
			":running": &types.AttributeValueMemberS{Value: string(driver.JobStateRunning)},
			":wid":     &types.AttributeValueMemberS{Value: params.WorkerID},
			":anum":    &types.AttributeValueMemberN{Value: strconv.Itoa(params.Attempt)},
		},
	})
	if err != nil {
		var conditional *types.ConditionalCheckFailedException
		if errors.As(err, &conditional) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// ---- JobSetStateIfRunning ----

func jobSetStateIfRunning(ctx context.Context, svc *dynamodb.Client, clk clock.Clock, params driver.JobSetStateParams) error {
	j, err := selectJob(ctx, svc, params.ID)
	if err != nil {
		return err
	}
	if j == nil || j.State != string(driver.JobStateRunning) || !params.MatchesClaim(j.WorkerID, j.AttemptNum) {
		return fmt.Errorf("%w: job %q", driver.ErrStaleClaim, params.ID)
	}

	if params.Yield {
		qsAvail := qsKey(j.Queue, string(driver.JobStateAvailable))
		_, err = svc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
			TableName: aws.String(tableJobs),
			Key: map[string]types.AttributeValue{
				"id": &types.AttributeValueMemberS{Value: params.ID},
			},
			UpdateExpression: aws.String(
				`SET #state = :avail, #qs = :qs, #wid = :empty, #ver = #ver + :one` +
					` REMOVE #aat, #lease` +
					` ADD #anum :neg_one`,
			),
			ConditionExpression: aws.String("#state = :running AND #ver = :expected_ver"),
			ExpressionAttributeNames: map[string]string{
				"#state": "state",
				"#qs":    "queue_state",
				"#wid":   "worker_id",
				"#aat":   "attempted_at",
				"#lease": "lease_expires_at",
				"#anum":  "attempt_num",
				"#ver":   "version",
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":avail":        &types.AttributeValueMemberS{Value: string(driver.JobStateAvailable)},
				":qs":           &types.AttributeValueMemberS{Value: qsAvail},
				":empty":        &types.AttributeValueMemberS{Value: ""},
				":one":          &types.AttributeValueMemberN{Value: "1"},
				":neg_one":      &types.AttributeValueMemberN{Value: "-1"},
				":running":      &types.AttributeValueMemberS{Value: string(driver.JobStateRunning)},
				":expected_ver": &types.AttributeValueMemberN{Value: strconv.FormatInt(j.Version, 10)},
			},
		})
		if err != nil {
			var cce *types.ConditionalCheckFailedException
			if errors.As(err, &cce) {
				return fmt.Errorf("%w: job %q", driver.ErrStaleClaim, params.ID)
			}
		}
		return err
	}

	now := clk.Now()
	newErrors := unmarshalErrors(j.ErrorsJSON)
	if params.Err != nil {
		stored := driver.AttemptError{
			At:      now,
			Attempt: j.AttemptNum,
			Error:   *params.Err,
		}
		if params.Trace != nil {
			stored.Trace = *params.Trace
		}
		newErrors = append(newErrors, stored)
	}
	errJSON := marshalErrors(newErrors)

	var updateExpr string
	exprNames := map[string]string{
		"#state": "state",
		"#qs":    "queue_state",
		"#wid":   "worker_id",
		"#errj":  "errors_json",
		"#ver":   "version",
		"#lease": "lease_expires_at",
	}
	exprVals := map[string]types.AttributeValue{
		":empty":        &types.AttributeValueMemberS{Value: ""},
		":errj":         &types.AttributeValueMemberS{Value: errJSON},
		":one":          &types.AttributeValueMemberN{Value: "1"},
		":running":      &types.AttributeValueMemberS{Value: string(driver.JobStateRunning)},
		":expected_ver": &types.AttributeValueMemberN{Value: strconv.FormatInt(j.Version, 10)},
	}

	if params.State == driver.JobStateRetryable {
		retryAt := params.RetryAt
		if retryAt.IsZero() {
			retryAt = now
		}
		exprNames["#run_at"] = "run_at"
		exprVals[":state"] = &types.AttributeValueMemberS{Value: string(driver.JobStateAvailable)}
		exprVals[":qs"] = &types.AttributeValueMemberS{Value: qsKey(j.Queue, string(driver.JobStateAvailable))}
		exprVals[":run_at"] = &types.AttributeValueMemberS{Value: retryAt.UTC().Format(timeFmt)}
		updateExpr = `SET #state = :state, #qs = :qs, #wid = :empty, #errj = :errj, #run_at = :run_at, #ver = #ver + :one REMOVE #lease`
	} else {
		exprNames["#fat"] = "finalized_at"
		exprVals[":state"] = &types.AttributeValueMemberS{Value: string(params.State)}
		exprVals[":qs"] = &types.AttributeValueMemberS{Value: qsKey(j.Queue, string(params.State))}
		exprVals[":fat"] = &types.AttributeValueMemberS{Value: now.UTC().Format(timeFmt)}
		updateExpr = `SET #state = :state, #qs = :qs, #wid = :empty, #errj = :errj, #fat = :fat, #ver = #ver + :one REMOVE #lease`
	}

	_, err = svc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableJobs),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: params.ID},
		},
		UpdateExpression:          aws.String(updateExpr),
		ConditionExpression:       aws.String("#state = :running AND #ver = :expected_ver"),
		ExpressionAttributeNames:  exprNames,
		ExpressionAttributeValues: exprVals,
	})
	if err != nil {
		var cce *types.ConditionalCheckFailedException
		if errors.As(err, &cce) {
			return fmt.Errorf("%w: job %q", driver.ErrStaleClaim, params.ID)
		}
		return err
	}
	if params.State != driver.JobStateRetryable && j.UniqueKey != "" && !driver.IsPermanentUniqueKey(j.UniqueKey) {
		_, err = svc.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(tableUniq),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: j.UniqueKey},
			},
		})
		return err
	}
	return nil
}

// ---- JobCancel ----

func jobCancel(ctx context.Context, svc *dynamodb.Client, clk clock.Clock, id string) error {
	j, err := selectJob(ctx, svc, id)
	if err != nil {
		return err
	}
	if j == nil {
		return fmt.Errorf("%w: job %q", driver.ErrNotFound, id)
	}
	if j.State != string(driver.JobStateAvailable) && j.State != string(driver.JobStateScheduled) {
		return fmt.Errorf("%w: job %q is in state %s, can only cancel available/scheduled", driver.ErrConflict, id, j.State)
	}

	now := clk.Now()
	_, err = svc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableJobs),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: id},
		},
		UpdateExpression: aws.String(
			`SET #state = :cancelled, #qs = :qs, #fat = :fat, #ver = #ver + :one`,
		),
		ConditionExpression: aws.String("#state IN (:avail, :sched)"),
		ExpressionAttributeNames: map[string]string{
			"#state": "state",
			"#qs":    "queue_state",
			"#fat":   "finalized_at",
			"#ver":   "version",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":cancelled": &types.AttributeValueMemberS{Value: string(driver.JobStateCancelled)},
			":qs":        &types.AttributeValueMemberS{Value: qsKey(j.Queue, string(driver.JobStateCancelled))},
			":fat":       &types.AttributeValueMemberS{Value: now.UTC().Format(timeFmt)},
			":one":       &types.AttributeValueMemberN{Value: "1"},
			":avail":     &types.AttributeValueMemberS{Value: string(driver.JobStateAvailable)},
			":sched":     &types.AttributeValueMemberS{Value: string(driver.JobStateScheduled)},
		},
	})
	if err != nil {
		var cce *types.ConditionalCheckFailedException
		if errors.As(err, &cce) {
			return fmt.Errorf("%w: job %q changed before cancellation", driver.ErrConflict, id)
		}
		return err
	}

	if j.UniqueKey != "" && !driver.IsPermanentUniqueKey(j.UniqueKey) {
		_, _ = svc.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(tableUniq),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: j.UniqueKey},
			},
		})
	}
	return nil
}

// ---- JobDelete ----

func jobDelete(ctx context.Context, svc *dynamodb.Client, id string) error {
	j, err := selectJob(ctx, svc, id)
	if err != nil {
		return err
	}
	if j == nil {
		return fmt.Errorf("%w: job %q", driver.ErrNotFound, id)
	}
	if _, err := svc.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(tableJobs),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: id},
		},
	}); err != nil {
		return err
	}
	if j.UniqueKey != "" {
		_, _ = svc.DeleteItem(ctx, &dynamodb.DeleteItemInput{
			TableName: aws.String(tableUniq),
			Key: map[string]types.AttributeValue{
				"pk": &types.AttributeValueMemberS{Value: j.UniqueKey},
			},
		})
	}
	return nil
}

// ---- JobReschedule ----

func jobReschedule(ctx context.Context, svc *dynamodb.Client, params driver.RescheduleParams) error {
	j, err := selectJob(ctx, svc, params.ID)
	if err != nil {
		return err
	}
	if j == nil {
		return fmt.Errorf("%w: job %q", driver.ErrNotFound, params.ID)
	}
	_, err = svc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableJobs),
		Key: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: params.ID},
		},
		UpdateExpression: aws.String(`SET #state = :sched, #qs = :qs, #run_at = :run_at, #ver = #ver + :one`),
		ExpressionAttributeNames: map[string]string{
			"#state":  "state",
			"#qs":     "queue_state",
			"#run_at": "run_at",
			"#ver":    "version",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":sched":  &types.AttributeValueMemberS{Value: string(driver.JobStateScheduled)},
			":qs":     &types.AttributeValueMemberS{Value: qsKey(j.Queue, string(driver.JobStateScheduled))},
			":run_at": &types.AttributeValueMemberS{Value: params.RunAt.UTC().Format(timeFmt)},
			":one":    &types.AttributeValueMemberN{Value: "1"},
		},
	})
	return err
}

// ---- Queue ----

func isQueuePaused(ctx context.Context, svc *dynamodb.Client, name string) (bool, error) {
	out, err := svc.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableQueues),
		Key: map[string]types.AttributeValue{
			"name": &types.AttributeValueMemberS{Value: name},
		},
	})
	if err != nil {
		return false, err
	}
	if out.Item == nil {
		return false, nil
	}
	if v, ok := out.Item["paused"]; ok {
		if bv, ok := v.(*types.AttributeValueMemberBOOL); ok {
			return bv.Value, nil
		}
	}
	return false, nil
}

func queueGet(ctx context.Context, svc *dynamodb.Client, name string) (*driver.QueueRow, error) {
	out, err := svc.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(tableQueues),
		Key: map[string]types.AttributeValue{
			"name": &types.AttributeValueMemberS{Value: name},
		},
	})
	if err != nil {
		return nil, err
	}
	if out.Item == nil {
		return nil, fmt.Errorf("%w: queue %q", driver.ErrNotFound, name)
	}
	return itemToQueueRow(out.Item, name), nil
}

func queueSetPaused(ctx context.Context, svc *dynamodb.Client, clk clock.Clock, name string, paused bool) error {
	nowStr := clk.Now().UTC().Format(timeFmt)
	_, err := svc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(tableQueues),
		Key: map[string]types.AttributeValue{
			"name": &types.AttributeValueMemberS{Value: name},
		},
		UpdateExpression: aws.String(`SET #p = :paused, #uat = :now`),
		ExpressionAttributeNames: map[string]string{
			"#p":   "paused",
			"#uat": "updated_at",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":paused": &types.AttributeValueMemberBOOL{Value: paused},
			":now":    &types.AttributeValueMemberS{Value: nowStr},
		},
	})
	return err
}

func queueList(ctx context.Context, svc *dynamodb.Client, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	cursor := ""
	if params.Cursor != "" {
		var err error
		cursor, err = driver.DecodeQueueCursor(params.Cursor)
		if err != nil {
			return nil, err
		}
	}
	limit := params.Limit
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	var startKey map[string]types.AttributeValue
	var rows []*driver.QueueRow
	for {
		out, err := svc.Scan(ctx, &dynamodb.ScanInput{TableName: aws.String(tableQueues), ExclusiveStartKey: startKey})
		if err != nil {
			return nil, err
		}
		for _, item := range out.Items {
			name := ""
			if v, ok := item["name"]; ok {
				if sv, ok := v.(*types.AttributeValueMemberS); ok {
					name = sv.Value
				}
			}
			if name > cursor {
				rows = append(rows, itemToQueueRow(item, name))
			}
		}
		if len(out.LastEvaluatedKey) == 0 {
			break
		}
		startKey = out.LastEvaluatedKey
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
	if len(rows) > limit {
		rows = rows[:limit]
	}
	return rows, nil
}

func itemToQueueRow(item map[string]types.AttributeValue, name string) *driver.QueueRow {
	row := &driver.QueueRow{Name: name}
	if v, ok := item["paused"]; ok {
		if bv, ok := v.(*types.AttributeValueMemberBOOL); ok {
			row.Paused = bv.Value
		}
	}
	if v, ok := item["created_at"]; ok {
		if sv, ok := v.(*types.AttributeValueMemberS); ok {
			row.CreatedAt = parseTime(sv.Value)
		}
	}
	if v, ok := item["updated_at"]; ok {
		if sv, ok := v.(*types.AttributeValueMemberS); ok {
			row.UpdatedAt = parseTime(sv.Value)
		}
	}
	return row
}

// ---- Leader election ----

// leaderAttemptElect claims or renews leadership using a conditional PutItem.
// The condition succeeds when: no leader exists, the existing lease is expired,
// or the caller is already the leader (renewal).
func leaderAttemptElect(ctx context.Context, svc *dynamodb.Client, clk clock.Clock, params driver.LeaderElectParams) (bool, error) {
	now := clk.Now()
	nowStr := now.UTC().Format(timeFmt)
	expiresAt := now.Add(params.TTL).UTC().Format(timeFmt)

	_, err := svc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableLeaders),
		Item: map[string]types.AttributeValue{
			"name":       &types.AttributeValueMemberS{Value: params.Name},
			"worker_id":  &types.AttributeValueMemberS{Value: params.WorkerID},
			"expires_at": &types.AttributeValueMemberS{Value: expiresAt},
		},
		ConditionExpression: aws.String(
			`attribute_not_exists(#n) OR expires_at < :now OR worker_id = :wid`,
		),
		ExpressionAttributeNames: map[string]string{"#n": "name"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":now": &types.AttributeValueMemberS{Value: nowStr},
			":wid": &types.AttributeValueMemberS{Value: params.WorkerID},
		},
	})
	if err != nil {
		var cce *types.ConditionalCheckFailedException
		if errors.As(err, &cce) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func leaderResign(ctx context.Context, svc *dynamodb.Client, params driver.LeaderResignParams) error {
	_, err := svc.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(tableLeaders),
		Key: map[string]types.AttributeValue{
			"name": &types.AttributeValueMemberS{Value: params.Name},
		},
		ConditionExpression: aws.String("worker_id = :wid"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":wid": &types.AttributeValueMemberS{Value: params.WorkerID},
		},
	})
	var conflict *types.ConditionalCheckFailedException
	if errors.As(err, &conflict) {
		return nil
	}
	return err
}

func scheduleCursorGetOrCreate(ctx context.Context, svc *dynamodb.Client, params driver.ScheduleCursorCreateParams) (driver.ScheduleCursorResult, error) {
	initial := params.InitialAt.UTC().Format(timeFmt)
	created := true
	_, err := svc.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(tableCursors),
		Item: map[string]types.AttributeValue{
			"id": &types.AttributeValueMemberS{Value: params.ID}, "cursor_at": &types.AttributeValueMemberS{Value: initial},
		},
		ConditionExpression:      aws.String("attribute_not_exists(#id)"),
		ExpressionAttributeNames: map[string]string{"#id": "id"},
	})
	if err != nil {
		var conflict *types.ConditionalCheckFailedException
		if !errors.As(err, &conflict) {
			return driver.ScheduleCursorResult{}, err
		}
		created = false
	}
	out, err := svc.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(tableCursors),
		Key:            map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: params.ID}},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return driver.ScheduleCursorResult{}, err
	}
	value, ok := out.Item["cursor_at"].(*types.AttributeValueMemberS)
	if !ok {
		return driver.ScheduleCursorResult{}, fmt.Errorf("schedule cursor %q has no cursor_at", params.ID)
	}
	return driver.ScheduleCursorResult{At: parseTime(value.Value), Created: created}, nil
}

func scheduleCursorAdvance(ctx context.Context, svc *dynamodb.Client, params driver.ScheduleCursorAdvanceParams) (bool, error) {
	_, err := svc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:           aws.String(tableCursors),
		Key:                 map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: params.ID}},
		UpdateExpression:    aws.String("SET cursor_at = :next"),
		ConditionExpression: aws.String("cursor_at = :expected"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":next":     &types.AttributeValueMemberS{Value: params.Next.UTC().Format(timeFmt)},
			":expected": &types.AttributeValueMemberS{Value: params.Expected.UTC().Format(timeFmt)},
		},
	})
	if err != nil {
		var conflict *types.ConditionalCheckFailedException
		if errors.As(err, &conflict) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// compile-time checks
var _ driver.Executor = (*executor)(nil)
var _ driver.ExecutorTx = (*txExecutor)(nil)
