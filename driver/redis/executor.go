package redisdriver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"

	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/driver"
)

// ---- key schema ----

const (
	jobKeyPrefix = "goncordia:job:"
	queuesSetKey = "goncordia:queues"
)

func availKey(q string) string      { return "goncordia:q:" + q + ":avail" }
func schedKey(q string) string      { return "goncordia:q:" + q + ":sched" }
func runKey(q string) string        { return "goncordia:q:" + q + ":run" }
func metaKey(q string) string       { return "goncordia:q:" + q + ":meta" }
func jobKey(id string) string       { return jobKeyPrefix + id }
func uniqKey(q, k string) string    { return "goncordia:uniq:" + q + ":" + k }
func leaderKey(n string) string     { return "goncordia:leader:" + n }
func notifyChannel(q string) string { return "goncordia:notify:" + q }

// priorityScore encodes priority and run_at into a sorted-set score.
// ZPOPMIN picks lowest score, so higher priority → lower score → claimed first.
func priorityScore(priority int, runAt time.Time) float64 {
	return float64(runAt.UnixMilli()) - float64(priority)*1e12
}

// ---- job document ----

type redisJob struct {
	ID            string            `json:"id"`
	Queue         string            `json:"queue"`
	Kind          string            `json:"kind"`
	Args          string            `json:"args"` // raw JSON string
	State         string            `json:"state"`
	Priority      int               `json:"priority"`
	RunAtMs       millis            `json:"run_at_ms"`
	CreatedAtMs   millis            `json:"created_at_ms"`
	AttemptedAtMs millis            `json:"attempted_at_ms,omitempty"`
	FinalizedAtMs millis            `json:"finalized_at_ms,omitempty"`
	AttemptNum    int               `json:"attempt_num"`
	MaxRetry      int               `json:"max_retry"`
	TimeoutMs     int64             `json:"timeout_ms"`
	UniqueKey     string            `json:"unique_key,omitempty"`
	WorkerID      string            `json:"worker_id,omitempty"`
	Tags          []string          `json:"tags"`
	Errors        []redisAttemptErr `json:"errors"`
	PipelineID    string            `json:"pipeline_id,omitempty"`
}

// millis accepts the scientific notation emitted by Redis's embedded cjson
// after a Lua script updates a job document.
type millis int64

func (m *millis) UnmarshalJSON(data []byte) error {
	var value float64
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*m = millis(int64(value))
	return nil
}

type redisAttemptErr struct {
	AtMs    int64  `json:"at_ms"`
	Attempt int    `json:"attempt"`
	Message string `json:"message"`
}

func jobToRow(j redisJob) *driver.JobRow {
	row := &driver.JobRow{
		ID:         j.ID,
		Queue:      j.Queue,
		Kind:       j.Kind,
		Args:       []byte(j.Args),
		State:      driver.JobState(j.State),
		Priority:   j.Priority,
		RunAt:      time.UnixMilli(int64(j.RunAtMs)).UTC(),
		CreatedAt:  time.UnixMilli(int64(j.CreatedAtMs)).UTC(),
		AttemptNum: j.AttemptNum,
		MaxRetry:   j.MaxRetry,
		Timeout:    time.Duration(j.TimeoutMs) * time.Millisecond,
		UniqueKey:  j.UniqueKey,
		WorkerID:   j.WorkerID,
		Tags:       j.Tags,
		PipelineID: j.PipelineID,
	}
	if j.AttemptedAtMs != 0 {
		t := time.UnixMilli(int64(j.AttemptedAtMs)).UTC()
		row.AttemptedAt = &t
	}
	if j.FinalizedAtMs != 0 {
		t := time.UnixMilli(int64(j.FinalizedAtMs)).UTC()
		row.FinalizedAt = &t
	}
	for _, e := range j.Errors {
		row.Errors = append(row.Errors, driver.AttemptError{
			At:      time.UnixMilli(e.AtMs).UTC(),
			Attempt: e.Attempt,
			Error:   e.Message,
		})
	}
	return row
}

// ---- Lua: atomic fetch-and-claim ----
//
// KEYS[1] = avail sorted set
// KEYS[2] = sched sorted set
// KEYS[3] = running hash
// ARGV[1] = now_ms
// ARGV[2] = worker_id
// ARGV[3] = job key prefix
//
// Returns: updated job JSON, or empty string when the queue is empty.
var fetchOneScript = redis.NewScript(`
local avail  = KEYS[1]
local sched  = KEYS[2]
local run_h  = KEYS[3]
local now_ms = tonumber(ARGV[1])
local prefix = ARGV[3]

-- Promote due scheduled jobs to the available set.
local due = redis.call('ZRANGEBYSCORE', sched, '-inf', tostring(now_ms))
for i = 1, #due do
    local id  = due[i]
    local raw = redis.call('GET', prefix .. id)
    if raw and raw ~= false then
        local ok, job = pcall(cjson.decode, raw)
        local priority  = 0
        local run_at_ms = now_ms
        if ok and type(job) == 'table' then
            priority  = tonumber(job.priority)   or 0
            run_at_ms = tonumber(job.run_at_ms)  or now_ms
        end
        redis.call('ZREM',  sched, id)
        redis.call('ZADD',  avail, run_at_ms - priority * 1000000000000, id)
    end
end

-- Claim one job.
local res = redis.call('ZPOPMIN', avail, 1)
if #res == 0 then return '' end

local id = res[1]
local raw = redis.call('GET', prefix .. id)
if not raw then return '' end
local ok, job = pcall(cjson.decode, raw)
if not ok or type(job) ~= 'table' then return '' end
job.state = 'running'
job.attempted_at_ms = now_ms
job.attempt_num = tonumber(job.attempt_num or 0) + 1
job.worker_id = ARGV[2]
if type(job.tags) == 'table' and next(job.tags) == nil then job.tags = cjson.empty_array end
if type(job.errors) == 'table' and next(job.errors) == nil then job.errors = cjson.empty_array end
cjson.encode_number_precision(14)
local updated = cjson.encode(job)
redis.call('SET', prefix .. id, updated)
redis.call('HSET', run_h, id, tostring(now_ms))
return updated
`)

var rescueOneScript = redis.NewScript(`
local attempted = redis.call('HGET', KEYS[1], ARGV[1])
if not attempted or tonumber(attempted) > tonumber(ARGV[2]) then return 0 end
local raw = redis.call('GET', KEYS[3])
if not raw then
    redis.call('HDEL', KEYS[1], ARGV[1])
    return 0
end
local ok, job = pcall(cjson.decode, raw)
if not ok or job.state ~= 'running' then
    redis.call('HDEL', KEYS[1], ARGV[1])
    return 0
end
job.state = 'available'
job.attempted_at_ms = nil
job.worker_id = nil
if type(job.tags) == 'table' and next(job.tags) == nil then job.tags = cjson.empty_array end
if type(job.errors) == 'table' and next(job.errors) == nil then job.errors = cjson.empty_array end
cjson.encode_number_precision(14)
redis.call('SET', KEYS[3], cjson.encode(job))
redis.call('HDEL', KEYS[1], ARGV[1])
local score = tonumber(job.run_at_ms) - tonumber(job.priority or 0) * 1000000000000
redis.call('ZADD', KEYS[2], score, ARGV[1])
return 1
`)

var heartbeatScript = redis.NewScript(`
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local ok, job = pcall(cjson.decode, raw)
if not ok or job.state ~= 'running' then return 0 end
if tostring(job.worker_id or '') ~= ARGV[1] then return 0 end
if tonumber(job.attempt_num or 0) ~= tonumber(ARGV[2]) then return 0 end
job.attempted_at_ms = tonumber(ARGV[3])
if type(job.tags) == 'table' and next(job.tags) == nil then job.tags = cjson.empty_array end
if type(job.errors) == 'table' and next(job.errors) == nil then job.errors = cjson.empty_array end
cjson.encode_number_precision(14)
redis.call('SET', KEYS[1], cjson.encode(job))
redis.call('HSET', KEYS[2], ARGV[4], ARGV[3])
return 1
`)

// ---- executor ----

type executor struct {
	rdb *redis.Client
	clk clock.Clock
}

func (e *executor) Begin(_ context.Context) (driver.ExecutorTx, error) {
	return nil, fmt.Errorf("%w: redis transactions", driver.ErrUnsupported)
}

func (e *executor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertMany(ctx, e.rdb, e.clk, params)
}
func (e *executor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	return jobGetByID(ctx, e.rdb, id)
}
func (e *executor) JobList(ctx context.Context, params driver.JobListParams) ([]driver.JobRow, error) {
	return jobList(ctx, e.rdb, params)
}
func (e *executor) QueueStats(ctx context.Context, queue string) (driver.QueueStats, error) {
	return queueStats(ctx, e.rdb, queue)
}
func (e *executor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	return jobFetchBatch(ctx, e.rdb, e.clk, params)
}
func (e *executor) JobRescueStuck(ctx context.Context, params driver.JobRescueParams) (int64, error) {
	ids, err := e.rdb.HKeys(ctx, runKey(params.Queue)).Result()
	if err != nil {
		return 0, err
	}
	var rescued int64
	for _, id := range ids {
		n, err := rescueOneScript.Run(ctx, e.rdb,
			[]string{runKey(params.Queue), availKey(params.Queue), jobKey(id)},
			id, params.Before.UnixMilli(),
		).Int64()
		if err != nil {
			return rescued, err
		}
		rescued += n
	}
	return rescued, nil
}
func (e *executor) JobHeartbeat(ctx context.Context, params driver.JobHeartbeatParams) (bool, error) {
	job, err := jobGetByID(ctx, e.rdb, params.ID)
	if err != nil || job == nil {
		return false, err
	}
	renewed, err := heartbeatScript.Run(ctx, e.rdb,
		[]string{jobKey(params.ID), runKey(job.Queue)},
		params.WorkerID, params.Attempt, params.At.UTC().UnixMilli(), params.ID,
	).Int64()
	return renewed == 1, err
}
func (e *executor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(ctx, e.rdb, e.clk, params)
}
func (e *executor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(ctx, e.rdb, e.clk, id)
}
func (e *executor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(ctx, e.rdb, id)
}
func (e *executor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(ctx, e.rdb, params)
}
func (e *executor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(ctx, e.rdb, e.clk, name)
}
func (e *executor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(ctx, e.rdb, e.clk, name, true)
}
func (e *executor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(ctx, e.rdb, e.clk, name, false)
}
func (e *executor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(ctx, e.rdb, e.clk, params)
}
func (e *executor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(ctx, e.rdb, e.clk, params)
}
func (e *executor) LeaderResign(ctx context.Context, name string) error {
	return leaderResign(ctx, e.rdb, name)
}

// ---- txExecutor ----

type txExecutor struct{ executor }

func (t *txExecutor) Commit(_ context.Context) error   { return nil }
func (t *txExecutor) Rollback(_ context.Context) error { return nil }
func (t *txExecutor) Begin(_ context.Context) (driver.ExecutorTx, error) {
	return nil, fmt.Errorf("nested transactions not supported")
}

func (t *txExecutor) JobInsertMany(ctx context.Context, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return jobInsertMany(ctx, t.rdb, t.clk, params)
}
func (t *txExecutor) JobGetByID(ctx context.Context, id string) (*driver.JobRow, error) {
	return jobGetByID(ctx, t.rdb, id)
}
func (t *txExecutor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	return jobFetchBatch(ctx, t.rdb, t.clk, params)
}
func (t *txExecutor) JobSetStateIfRunning(ctx context.Context, params driver.JobSetStateParams) error {
	return jobSetStateIfRunning(ctx, t.rdb, t.clk, params)
}
func (t *txExecutor) JobCancel(ctx context.Context, id string) error {
	return jobCancel(ctx, t.rdb, t.clk, id)
}
func (t *txExecutor) JobDelete(ctx context.Context, id string) error {
	return jobDelete(ctx, t.rdb, id)
}
func (t *txExecutor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	return jobReschedule(ctx, t.rdb, params)
}
func (t *txExecutor) QueueGet(ctx context.Context, name string) (*driver.QueueRow, error) {
	return queueGet(ctx, t.rdb, t.clk, name)
}
func (t *txExecutor) QueuePause(ctx context.Context, name string) error {
	return queueSetPaused(ctx, t.rdb, t.clk, name, true)
}
func (t *txExecutor) QueueResume(ctx context.Context, name string) error {
	return queueSetPaused(ctx, t.rdb, t.clk, name, false)
}
func (t *txExecutor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	return queueList(ctx, t.rdb, t.clk, params)
}
func (t *txExecutor) LeaderAttemptElect(ctx context.Context, params driver.LeaderElectParams) (bool, error) {
	return leaderAttemptElect(ctx, t.rdb, t.clk, params)
}
func (t *txExecutor) LeaderResign(ctx context.Context, name string) error {
	return leaderResign(ctx, t.rdb, name)
}

// ---- core functions ----

func jobInsertMany(ctx context.Context, rdb *redis.Client, clk clock.Clock, params []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
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

		// Unique-key deduplication: SET NX
		if p.UniqueKey != "" {
			ok, err := rdb.SetNX(ctx, uniqKey(p.Queue, p.UniqueKey), id, 0).Result()
			if err != nil {
				return nil, fmt.Errorf("unique key check: %w", err)
			}
			if !ok {
				results[i] = driver.JobInsertResult{UniqueSkip: true}
				continue
			}
		}

		tags := p.Tags
		if tags == nil {
			tags = []string{}
		}
		job := redisJob{
			ID:          id,
			Queue:       p.Queue,
			Kind:        p.Kind,
			Args:        string(p.Args),
			State:       string(state),
			Priority:    p.Priority,
			RunAtMs:     millis(runAt.UnixMilli()),
			CreatedAtMs: millis(now.UnixMilli()),
			MaxRetry:    p.MaxRetry,
			TimeoutMs:   p.Timeout.Milliseconds(),
			UniqueKey:   p.UniqueKey,
			Tags:        tags,
			Errors:      []redisAttemptErr{},
			PipelineID:  p.PipelineID,
		}

		raw, err := json.Marshal(job)
		if err != nil {
			return nil, fmt.Errorf("marshal job: %w", err)
		}

		pipe := rdb.Pipeline()
		pipe.Set(ctx, jobKey(id), raw, 0)
		if state == driver.JobStateScheduled {
			pipe.ZAdd(ctx, schedKey(p.Queue), redis.Z{Score: float64(runAt.UnixMilli()), Member: id})
		} else {
			pipe.ZAdd(ctx, availKey(p.Queue), redis.Z{Score: priorityScore(p.Priority, runAt), Member: id})
		}
		ensureQueueMeta(pipe, ctx, p.Queue, now)
		pipe.Publish(ctx, notifyChannel(p.Queue), "1")
		if _, err := pipe.Exec(ctx); err != nil {
			return nil, fmt.Errorf("insert job: %w", err)
		}

		results[i] = driver.JobInsertResult{Job: jobToRow(job)}
	}
	return results, nil
}

func jobGetByID(ctx context.Context, rdb *redis.Client, id string) (*driver.JobRow, error) {
	raw, err := rdb.Get(ctx, jobKey(id)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var job redisJob
	if err := json.Unmarshal(raw, &job); err != nil {
		return nil, err
	}
	return jobToRow(job), nil
}

func scanJobs(ctx context.Context, rdb *redis.Client, params driver.JobListParams) ([]driver.JobRow, error) {
	var cursor uint64
	var rows []driver.JobRow
	for {
		keys, next, err := rdb.Scan(ctx, cursor, jobKeyPrefix+"*", 500).Result()
		if err != nil {
			return nil, err
		}
		for _, key := range keys {
			raw, err := rdb.Get(ctx, key).Bytes()
			if err != nil {
				continue
			}
			var job redisJob
			if err := json.Unmarshal(raw, &job); err != nil {
				return nil, fmt.Errorf("decode %s: %w", key, err)
			}
			if params.Queue != "" && job.Queue != params.Queue || params.State != "" && driver.JobState(job.State) != params.State || params.Kind != "" && job.Kind != params.Kind || params.Cursor != "" && job.ID >= params.Cursor {
				continue
			}
			rows = append(rows, *jobToRow(job))
		}
		cursor = next
		if cursor == 0 {
			break
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

func jobList(ctx context.Context, rdb *redis.Client, params driver.JobListParams) ([]driver.JobRow, error) {
	rows, err := scanJobs(ctx, rdb, params)
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

func queueStats(ctx context.Context, rdb *redis.Client, queue string) (driver.QueueStats, error) {
	rows, err := scanJobs(ctx, rdb, driver.JobListParams{Queue: queue})
	if err != nil {
		return driver.QueueStats{}, err
	}
	return driver.CountQueueStats(queue, rows), nil
}

func jobFetchBatch(ctx context.Context, rdb *redis.Client, clk clock.Clock, params driver.FetchParams) ([]driver.JobRow, error) {
	// Check pause state.
	paused, err := isQueuePaused(ctx, rdb, params.Queue)
	if err != nil {
		return nil, err
	}
	if paused {
		return nil, nil
	}

	now := clk.Now()
	nowMs := now.UnixMilli()

	var rows []driver.JobRow
	for range params.Limit {
		rawJSON, err := fetchOneScript.Run(ctx, rdb,
			[]string{availKey(params.Queue), schedKey(params.Queue), runKey(params.Queue)},
			nowMs, params.WorkerID, jobKeyPrefix,
		).Text()
		if err != nil {
			return rows, fmt.Errorf("claim job: %w", err)
		}
		if rawJSON == "" {
			break
		}

		var job redisJob
		if err := json.Unmarshal([]byte(rawJSON), &job); err != nil {
			continue
		}

		rows = append(rows, *jobToRow(job))
	}
	return rows, nil
}

func jobSetStateIfRunning(ctx context.Context, rdb *redis.Client, clk clock.Clock, params driver.JobSetStateParams) error {
	key := jobKey(params.ID)
	for range 8 {
		err := rdb.Watch(ctx, func(tx *redis.Tx) error {
			raw, err := tx.Get(ctx, key).Bytes()
			if err == redis.Nil {
				return nil
			}
			if err != nil {
				return err
			}

			var job redisJob
			if err := json.Unmarshal(raw, &job); err != nil {
				return err
			}
			if job.State != string(driver.JobStateRunning) || !params.MatchesClaim(job.WorkerID, job.AttemptNum) {
				return nil
			}

			now := clk.Now()
			if params.Yield {
				job.State = string(driver.JobStateAvailable)
				job.AttemptNum--
				job.AttemptedAtMs = 0
				job.WorkerID = ""
			} else {
				if params.Err != nil {
					job.Errors = append(job.Errors, redisAttemptErr{
						AtMs: now.UnixMilli(), Attempt: job.AttemptNum, Message: *params.Err,
					})
				}
				if params.State == driver.JobStateRetryable {
					retryAt := params.RetryAt
					if retryAt.IsZero() {
						retryAt = now
					}
					job.State = string(driver.JobStateAvailable)
					job.RunAtMs = millis(retryAt.UnixMilli())
					job.WorkerID = ""
				} else {
					job.State = string(params.State)
					job.FinalizedAtMs = millis(now.UnixMilli())
					job.WorkerID = ""
				}
			}

			updated, err := json.Marshal(job)
			if err != nil {
				return err
			}
			_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.HDel(ctx, runKey(job.Queue), params.ID)
				pipe.Set(ctx, key, updated, 0)
				switch {
				case params.Yield:
					pipe.ZAdd(ctx, availKey(job.Queue), redis.Z{
						Score: priorityScore(job.Priority, time.UnixMilli(int64(job.RunAtMs))), Member: params.ID,
					})
				case params.State == driver.JobStateRetryable:
					retryAt := time.UnixMilli(int64(job.RunAtMs))
					if retryAt.After(now) {
						pipe.ZAdd(ctx, schedKey(job.Queue), redis.Z{Score: float64(retryAt.UnixMilli()), Member: params.ID})
					} else {
						pipe.ZAdd(ctx, availKey(job.Queue), redis.Z{Score: priorityScore(job.Priority, retryAt), Member: params.ID})
					}
				case job.UniqueKey != "":
					pipe.Del(ctx, uniqKey(job.Queue, job.UniqueKey))
				}
				return nil
			})
			return err
		}, key)
		if errors.Is(err, redis.TxFailedErr) {
			continue
		}
		return err
	}
	return fmt.Errorf("set state for job %s: too much concurrent contention", params.ID)
}

func jobCancel(ctx context.Context, rdb *redis.Client, clk clock.Clock, id string) error {
	raw, err := rdb.Get(ctx, jobKey(id)).Bytes()
	if err == redis.Nil {
		return fmt.Errorf("job %q not found", id)
	}
	if err != nil {
		return err
	}
	var job redisJob
	if err := json.Unmarshal(raw, &job); err != nil {
		return err
	}
	if job.State != string(driver.JobStateAvailable) && job.State != string(driver.JobStateScheduled) {
		return fmt.Errorf("job %q is in state %s, can only cancel available/scheduled", id, job.State)
	}

	now := clk.Now()
	job.State = string(driver.JobStateCancelled)
	job.FinalizedAtMs = millis(now.UnixMilli())
	if job.UniqueKey != "" {
		rdb.Del(ctx, uniqKey(job.Queue, job.UniqueKey)) //nolint:errcheck
	}

	pipe := rdb.Pipeline()
	pipe.ZRem(ctx, availKey(job.Queue), id)
	pipe.ZRem(ctx, schedKey(job.Queue), id)
	updated, _ := json.Marshal(job)
	pipe.Set(ctx, jobKey(id), updated, 0)
	_, err = pipe.Exec(ctx)
	return err
}

func jobDelete(ctx context.Context, rdb *redis.Client, id string) error {
	raw, err := rdb.Get(ctx, jobKey(id)).Bytes()
	if err == redis.Nil {
		return nil
	}
	if err != nil {
		return err
	}
	var job redisJob
	if err := json.Unmarshal(raw, &job); err != nil {
		return nil
	}
	pipe := rdb.Pipeline()
	pipe.Del(ctx, jobKey(id))
	pipe.ZRem(ctx, availKey(job.Queue), id)
	pipe.ZRem(ctx, schedKey(job.Queue), id)
	pipe.HDel(ctx, runKey(job.Queue), id)
	if job.UniqueKey != "" {
		pipe.Del(ctx, uniqKey(job.Queue, job.UniqueKey))
	}
	_, err = pipe.Exec(ctx)
	return err
}

func jobReschedule(ctx context.Context, rdb *redis.Client, params driver.RescheduleParams) error {
	raw, err := rdb.Get(ctx, jobKey(params.ID)).Bytes()
	if err == redis.Nil {
		return fmt.Errorf("job %q not found", params.ID)
	}
	if err != nil {
		return err
	}
	var job redisJob
	if err := json.Unmarshal(raw, &job); err != nil {
		return err
	}
	job.RunAtMs = millis(params.RunAt.UnixMilli())
	job.State = string(driver.JobStateScheduled)

	pipe := rdb.Pipeline()
	pipe.ZRem(ctx, availKey(job.Queue), params.ID)
	pipe.ZAdd(ctx, schedKey(job.Queue), redis.Z{Score: float64(params.RunAt.UnixMilli()), Member: params.ID})
	updated, _ := json.Marshal(job)
	pipe.Set(ctx, jobKey(params.ID), updated, 0)
	_, err = pipe.Exec(ctx)
	return err
}

// ---- queue metadata ----

func ensureQueueMeta(pipe redis.Pipeliner, ctx context.Context, name string, now time.Time) {
	nowMs := now.UnixMilli()
	pipe.SAdd(ctx, queuesSetKey, name)
	// HSetNX: only sets if field doesn't already exist.
	pipe.HSetNX(ctx, metaKey(name), "paused", "0")
	pipe.HSetNX(ctx, metaKey(name), "created_at_ms", strconv.FormatInt(nowMs, 10))
	pipe.HSetNX(ctx, metaKey(name), "updated_at_ms", strconv.FormatInt(nowMs, 10))
}

func isQueuePaused(ctx context.Context, rdb *redis.Client, name string) (bool, error) {
	val, err := rdb.HGet(ctx, metaKey(name), "paused").Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return val == "1", nil
}

func queueGet(ctx context.Context, rdb *redis.Client, clk clock.Clock, name string) (*driver.QueueRow, error) {
	vals, err := rdb.HGetAll(ctx, metaKey(name)).Result()
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 {
		return nil, fmt.Errorf("queue %q not found", name)
	}
	return parseQueueRow(name, vals), nil
}

func queueSetPaused(ctx context.Context, rdb *redis.Client, clk clock.Clock, name string, paused bool) error {
	now := clk.Now()
	val := "0"
	if paused {
		val = "1"
	}
	pipe := rdb.Pipeline()
	pipe.SAdd(ctx, queuesSetKey, name)
	pipe.HSetNX(ctx, metaKey(name), "created_at_ms", strconv.FormatInt(now.UnixMilli(), 10))
	pipe.HSet(ctx, metaKey(name), "paused", val, "updated_at_ms", strconv.FormatInt(now.UnixMilli(), 10))
	_, err := pipe.Exec(ctx)
	return err
}

func queueList(ctx context.Context, rdb *redis.Client, clk clock.Clock, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	names, err := rdb.SMembers(ctx, queuesSetKey).Result()
	if err != nil {
		return nil, err
	}
	rows := make([]*driver.QueueRow, 0, len(names))
	for _, name := range names {
		vals, err := rdb.HGetAll(ctx, metaKey(name)).Result()
		if err != nil || len(vals) == 0 {
			continue
		}
		rows = append(rows, parseQueueRow(name, vals))
	}
	return rows, nil
}

func parseQueueRow(name string, vals map[string]string) *driver.QueueRow {
	row := &driver.QueueRow{Name: name}
	row.Paused = vals["paused"] == "1"
	if ms := parseInt64(vals["created_at_ms"]); ms != 0 {
		row.CreatedAt = time.UnixMilli(ms).UTC()
	}
	if ms := parseInt64(vals["updated_at_ms"]); ms != 0 {
		row.UpdatedAt = time.UnixMilli(ms).UTC()
	}
	return row
}

func parseInt64(s string) int64 {
	v, _ := strconv.ParseInt(s, 10, 64)
	return v
}

// ---- leader election ----

func leaderAttemptElect(ctx context.Context, rdb *redis.Client, clk clock.Clock, params driver.LeaderElectParams) (bool, error) {
	key := leaderKey(params.Name)

	// Try to become leader (NX = only set if not exists).
	ok, err := rdb.SetNX(ctx, key, params.WorkerID, params.TTL).Result()
	if err != nil {
		return false, err
	}
	if ok {
		return true, nil
	}

	// Key exists; check if it belongs to us (renew TTL).
	current, err := rdb.Get(ctx, key).Result()
	if err == redis.Nil {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if current == params.WorkerID {
		rdb.Expire(ctx, key, params.TTL) //nolint:errcheck
		return true, nil
	}
	return false, nil
}

func leaderResign(ctx context.Context, rdb *redis.Client, name string) error {
	return rdb.Del(ctx, leaderKey(name)).Err()
}

// ---- listener ----

type listener struct {
	rdb  *redis.Client
	mu   sync.Mutex
	subs map[string]*redisSub
}

type redisSub struct {
	ps *redis.PubSub
	ch chan driver.Notification
}

func (l *listener) Listen(ctx context.Context, queue string) (<-chan driver.Notification, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.subs == nil {
		l.subs = make(map[string]*redisSub)
	}
	if _, ok := l.subs[queue]; ok {
		return l.subs[queue].ch, nil
	}

	ch := make(chan driver.Notification, 16)
	ps := l.rdb.Subscribe(ctx, notifyChannel(queue))
	l.subs[queue] = &redisSub{ps: ps, ch: ch}

	go func() {
		defer close(ch)
		for range ps.Channel() {
			select {
			case ch <- driver.Notification{Queue: queue}:
			default:
			}
		}
	}()

	return ch, nil
}

func (l *listener) Unlisten(_ context.Context, queue string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if sub, ok := l.subs[queue]; ok {
		sub.ps.Close() //nolint:errcheck
		delete(l.subs, queue)
	}
	return nil
}

func (l *listener) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, sub := range l.subs {
		sub.ps.Close() //nolint:errcheck
	}
	l.subs = make(map[string]*redisSub)
	return nil
}

// compile-time checks
var _ driver.Executor = (*executor)(nil)
var _ driver.ExecutorTx = (*txExecutor)(nil)
