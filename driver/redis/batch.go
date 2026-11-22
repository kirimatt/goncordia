package redisdriver

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/kirimatt/goncordia/v2/driver"
)

// Hot-path scripts. Each one does in a single round trip what the generic
// path does with WATCH/GET/MULTI, and a batch of them shares one pipeline.
// They derive the per-queue running hash from the job document, like the
// WATCH path does, so they assume a standalone server (the driver takes a
// *redis.Client, not a cluster client).

// fetchManyScript promotes due scheduled jobs and claims up to ARGV[5] jobs.
//
// KEYS[1] = avail sorted set, KEYS[2] = sched sorted set,
// KEYS[3] = running hash, KEYS[4] = queue metadata hash.
// ARGV[1] = now_ms, ARGV[2] = worker_id, ARGV[3] = job key prefix,
// ARGV[4] = lease duration in ms, ARGV[5] = limit.
//
// Returns the claimed job documents (empty when paused or empty).
var fetchManyScript = redis.NewScript(`
if redis.call('HGET', KEYS[4], 'paused') == '1' then return {} end
local avail  = KEYS[1]
local sched  = KEYS[2]
local run_h  = KEYS[3]
local now_ms = tonumber(ARGV[1])
local prefix = ARGV[3]
local lease_ms = tonumber(ARGV[4]) or 0
local limit = tonumber(ARGV[5]) or 1
cjson.encode_number_precision(14)

local due = redis.call('ZRANGEBYSCORE', sched, '-inf', tostring(now_ms), 'LIMIT', 0, 1024)
for i = 1, #due do
    local id  = due[i]
    local raw = redis.call('GET', prefix .. id)
    if raw then
        local ok, job = pcall(cjson.decode, raw)
        local priority  = 0
        local run_at_ms = now_ms
        if ok and type(job) == 'table' then
            priority  = tonumber(job.priority)  or 0
            run_at_ms = tonumber(job.run_at_ms) or now_ms
        end
        redis.call('ZREM', sched, id)
        redis.call('ZADD', avail, run_at_ms - priority * 1000000000000, id)
    end
end

local out = {}
while #out < limit do
    local res = redis.call('ZPOPMIN', avail, 1)
    if #res == 0 then break end
    local id = res[1]
    local raw = redis.call('GET', prefix .. id)
    if raw then
        local ok, job = pcall(cjson.decode, raw)
        if ok and type(job) == 'table' then
            job.state = 'running'
            job.attempted_at_ms = now_ms
            job.started_at_ms = nil
            if lease_ms > 0 then
                job.lease_expires_at_ms = now_ms + lease_ms
            else
                job.lease_expires_at_ms = nil
            end
            job.attempt_num = tonumber(job.attempt_num or 0) + 1
            job.worker_id = ARGV[2]
            if type(job.tags) == 'table' and next(job.tags) == nil then job.tags = cjson.empty_array end
            if type(job.errors) == 'table' and next(job.errors) == nil then job.errors = cjson.empty_array end
            local updated = cjson.encode(job)
            redis.call('SET', prefix .. id, updated)
            redis.call('HSET', run_h, id, tostring(job.lease_expires_at_ms or now_ms))
            out[#out + 1] = updated
        end
    end
end
return out
`)

// completeScript marks one running job completed if its claim still matches.
//
// KEYS[1] = job key, KEYS[2] = completed index.
// ARGV[1] = expected worker (” matches any), ARGV[2] = expected attempt
// (<= 0 matches any), ARGV[3] = now_ms, ARGV[4] = job id,
// ARGV[5] = unique key prefix, ARGV[6] = permanent unique key prefix.
//
// Returns 1 when completed, 0 when the claim was stale.
var completeScript = redis.NewScript(`
local raw = redis.call('GET', KEYS[1])
if not raw then return 0 end
local ok, job = pcall(cjson.decode, raw)
if not ok or type(job) ~= 'table' or job.state ~= 'running' then return 0 end
if ARGV[1] ~= '' and tostring(job.worker_id or '') ~= ARGV[1] then return 0 end
local attempt = tonumber(ARGV[2]) or 0
if attempt > 0 and tonumber(job.attempt_num or 0) ~= attempt then return 0 end
local now_ms = tonumber(ARGV[3])
job.state = 'completed'
job.finalized_at_ms = now_ms
job.worker_id = nil
job.lease_expires_at_ms = nil
if type(job.tags) == 'table' and next(job.tags) == nil then job.tags = cjson.empty_array end
if type(job.errors) == 'table' and next(job.errors) == nil then job.errors = cjson.empty_array end
cjson.encode_number_precision(14)
redis.call('SET', KEYS[1], cjson.encode(job))
redis.call('HDEL', 'goncordia:q:' .. tostring(job.queue) .. ':run', ARGV[4])
redis.call('ZADD', KEYS[2], now_ms, ARGV[4])
local key = job.unique_key
if type(key) == 'string' and key ~= '' and string.sub(key, 1, #ARGV[6]) ~= ARGV[6] then
    redis.call('DEL', ARGV[5] .. key)
end
return 1
`)

// permanentUniqueKeyPrefix mirrors driver.IsPermanentUniqueKey for the Lua
// side: completing a job keeps a reservation with this prefix.
const permanentUniqueKeyPrefix = "uf2_"

// scriptCall is one script invocation in a pipeline.
type scriptCall struct {
	script *redis.Script
	keys   []string
	args   []any
}

// runScripts sends calls in one pipeline using EVALSHA. A call that fails
// with NOSCRIPT never ran, so those calls are resent with EVAL (which also
// caches the script) in a second pipeline.
func runScripts(ctx context.Context, rdb *redis.Client, calls []scriptCall) []*redis.Cmd {
	cmds := make([]*redis.Cmd, len(calls))
	_, _ = rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for i, c := range calls {
			cmds[i] = c.script.EvalSha(ctx, pipe, c.keys, c.args...)
		}
		return nil
	})
	var retry []int
	for i, cmd := range cmds {
		if err := cmd.Err(); err != nil && strings.HasPrefix(err.Error(), "NOSCRIPT") {
			retry = append(retry, i)
		}
	}
	if len(retry) > 0 {
		_, _ = rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			for _, i := range retry {
				cmds[i] = calls[i].script.Eval(ctx, pipe, calls[i].keys, calls[i].args...)
			}
			return nil
		})
	}
	return cmds
}

// runFlags runs calls that return 0 or 1 and reports them as booleans.
func runFlags(ctx context.Context, rdb *redis.Client, calls []scriptCall, op string) ([]bool, error) {
	out := make([]bool, len(calls))
	for i, cmd := range runScripts(ctx, rdb, calls) {
		n, err := cmd.Int64()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", op, err)
		}
		out[i] = n == 1
	}
	return out, nil
}

func completeCall(params driver.JobSetStateParams, nowMs int64) scriptCall {
	return scriptCall{
		script: completeScript,
		keys:   []string{jobKey(params.ID), finalKey(driver.JobStateCompleted)},
		args: []any{
			params.ExpectedWorkerID, params.ExpectedAttempt, nowMs, params.ID,
			uniqKey(""), permanentUniqueKeyPrefix,
		},
	}
}

// JobCompleteMany completes many running jobs in one pipeline; see
// driver.JobBatchCompleter.
func (e *executor) JobCompleteMany(ctx context.Context, params []driver.JobSetStateParams) ([]bool, error) {
	nowMs := e.clk.Now().UnixMilli()
	calls := make([]scriptCall, len(params))
	for i, p := range params {
		calls[i] = completeCall(p, nowMs)
	}
	return runFlags(ctx, e.rdb, calls, "complete jobs")
}

// JobMarkStartedMany persists the start instant of many running jobs in one
// pipeline; see driver.JobBatchStartMarker.
func (e *executor) JobMarkStartedMany(ctx context.Context, params []driver.JobMarkStartedParams) ([]bool, error) {
	calls := make([]scriptCall, len(params))
	for i, p := range params {
		calls[i] = scriptCall{
			script: markStartedScript,
			keys:   []string{jobKey(p.ID)},
			args:   []any{p.WorkerID, p.Attempt, p.At.UTC().UnixMilli()},
		}
	}
	return runFlags(ctx, e.rdb, calls, "mark jobs started")
}

// fetchMany claims up to params.Limit jobs with one script call.
func fetchMany(ctx context.Context, rdb *redis.Client, nowMs int64, params driver.FetchParams) ([]driver.JobRow, error) {
	docs, err := fetchManyScript.Run(ctx, rdb,
		[]string{availKey(params.Queue), schedKey(params.Queue), runKey(params.Queue), metaKey(params.Queue)},
		nowMs, params.WorkerID, jobKeyPrefix, params.LeaseDuration.Milliseconds(), params.Limit,
	).StringSlice()
	if err != nil {
		return nil, fmt.Errorf("claim jobs: %w", err)
	}
	rows := make([]driver.JobRow, 0, len(docs))
	for _, doc := range docs {
		var job redisJob
		if err := json.Unmarshal([]byte(doc), &job); err != nil {
			continue // a corrupt document cannot be run; rescue will not see it either
		}
		rows = append(rows, *jobToRow(job))
	}
	return rows, nil
}
