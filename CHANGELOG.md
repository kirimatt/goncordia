# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [v2.10.1] — 2026-11-23

### Fixed
- A worker pool could stop draining a backlog until the next `PollInterval`
  tick: when every job claimed by a full fetch finished before that fetch
  pass returned, the jobs read the previous backlog state and none of them
  woke the fetch loop. The pass now asks for the next one itself after a
  full fetch. Seen as a CI flake of
  `TestWorkerPoolDrainsBacklogWithoutWaitingForPoll`; covered by a
  deterministic test.

---

## [drivers mongodb, mongodbv2, cassandra, clickhouse, dynamodb, firestore v2.10.0] — 2026-11-23

Found with the repository benchmarks (`bench/`), run per backend in Docker.

### Changed
- mongodb, mongodbv2: `JobInsertMany` writes jobs without a unique key with
  one `InsertMany` instead of one `InsertOne` each. A batch of 100: 180 ms →
  4 ms.
- dynamodb: `JobInsertMany` writes jobs without a unique key with
  `BatchWriteItem` (25 per request, requests in parallel, unprocessed items
  resent with backoff) and the queue row once per queue instead of once per
  job. A batch of 100: 174 → 24 ms; end-to-end with 4 workers 122 → ~390
  jobs/s.
- cassandra: `JobInsertMany` creates the queue row (a lightweight
  transaction) once per queue instead of once per job and writes job rows in
  parallel; unique keys are still reserved in input order. A batch of 100:
  566 → 20 ms; end-to-end with 4 workers 243 → ~520 jobs/s.
- firestore: `JobInsertMany` writes jobs without a unique key through a
  `BulkWriter` and the queue document once per queue. A batch of 100: 212 →
  49 ms.
- clickhouse: completions and start marks are written in batches
  (`JobCompleteMany`, `JobMarkStartedMany`): one read and one multi-row
  `INSERT` (one part) for many jobs instead of one of each per job.
- cassandra, clickhouse, dynamodb, firestore: fetching checks the queue's
  pause flag concurrently with the candidate read instead of before it, one
  round trip less per fetch.

### Fixed
- `bench/`: every benchmark starts from an empty backend. Rows left by an
  earlier benchmark made later ones slower (Firestore emulator end-to-end:
  ~17 jobs/s after the batch benchmark, ~270 jobs/s on its own).

---

## [driver pgxv5 v2.10.0] — 2026-11-23

### Changed
- `JobInsertMany` inserts the whole batch with one
  `INSERT ... SELECT FROM unnest(...)` statement instead of one statement per
  job in a pgx batch: ~19,000 → ~32,000 jobs/s for batches of 100.

---

## [v2.10.0] — 2026-11-23

### Changed
- Worker pool: the handler no longer waits for the `started_at` write. It
  runs in the background and the job's state write waits for it, so
  `started_at` still lands before the outcome. Drain at concurrency 10 on
  Postgres rose from ~4,000 to ~5,700 jobs/s (`bench/compare`).

### Added
- `bench/compare`: the same workloads (single and batch enqueue, drain at
  concurrency 10 and 100 with an exactly-once check, idle pickup latency)
  against river on Postgres and asynq and machinery on Redis. Results in
  `docs/benchmarks.mdx`.

---

## [drivers gorm, bun v2.9.0] — 2026-11-23

Require stdlib v2.9.0 and root v2.9.1. Both use the stdlib executor, so they
get its fix for concurrent fetchers claiming the same job, batched start
marks, and MySQL batch completions. Their suites now also run
`drivertest.RunConcurrentClaims`.

---

## [drivers pgxv5, stdlib, redis, mongodbv2, mongodb, cassandra, clickhouse, dynamodb, firestore v2.9.0] — 2026-11-23

All require root v2.9.1, so their suites run the new conformance checks
(batch writers keep per-job fencing; drivers with `LinearizableCAS` also run
`drivertest.RunConcurrentClaims`: concurrent fetchers never share a job).

### Fixed
- stdlib: fetching ran `SELECT ... FOR UPDATE SKIP LOCKED` and the claiming
  `UPDATE` as separate autocommit statements, so the row locks were gone
  before the claim and two workers could claim the same job (Postgres, MySQL;
  SQLite had the same race between its two statements). Postgres and SQLite
  now claim with one atomic `UPDATE ... WHERE id IN (SELECT ...) RETURNING`,
  which is also one round trip instead of three; MySQL claims inside a
  transaction. gorm and bun use the stdlib executor.

### Performance
- pgxv5, stdlib (Postgres, SQLite, MySQL), redis, mongodbv2: implement
  `driver.JobBatchStartMarker`.
- stdlib MySQL: implements `driver.JobBatchCompleter` (lock, check, update in
  one transaction), so every SQL dialect batches completions.
- redis: a fetch claims the whole batch in one script call (and reads the
  pause flag in it) instead of one call per job plus a pause lookup;
  `EnqueueMany` sends all inserts in one pipeline; completing a job is one
  script instead of WATCH/GET/MULTI, and batches of completions and start
  marks share one pipeline. Scripts go out as EVALSHA, falling back to EVAL
  on NOSCRIPT.
- mongodbv2: fetching more than one job takes three round trips (find ids,
  one `updateMany` that re-checks readiness and stamps a claim token, read
  back by token) instead of one `findOneAndUpdate` per job.
- cassandra, dynamodb, firestore: a fetch claims its candidates in concurrent
  waves (as many at once as jobs are still missing), so a batch costs about
  one conditional write's latency instead of one per job. Each claim is still
  its own LWT / conditional update / transaction. A fetch that fails after
  claiming some jobs now returns them instead of leaving them to rescue.
- clickhouse: `EnqueueMany` and a fetch write their rows with one INSERT
  (one part) instead of one per job, and queue metadata once per queue.

### Tests
- redis: the suite runs against an existing server when
  `GONCORDIA_TEST_REDIS_ADDR` is set.

---

## [v2.9.1] — 2026-11-23

### Tests
- The concurrent-claim check moved out of `drivertest.Run` into
  `drivertest.RunConcurrentClaims`, for drivers that report
  `LinearizableCAS`. ClickHouse does not (it claims by inserting a higher
  version row) and can hand one job to two fetchers, as documented.
- The batch-completion check completes the job holding the unique key
  whatever the driver's fetch order among equal jobs (Redis ids are UUIDs).

---

## [v2.9.0] — 2026-11-23

### Fixed
- A worker pool with a backlog fetched at most `MaxPending` jobs per
  `PollInterval`: after a full fetch it waited for the next tick, although
  `PollInterval` is documented as the pause for an empty queue. With the
  defaults (Concurrency 10, MaxPending 40, PollInterval 1s) a backlog drained
  at about 40 jobs/s per process (measured: 42) unless new inserts sent
  notifications. A pass
  that comes back full (or stops at a queue concurrency limit) now marks a
  backlog, and finishing jobs wake the loop once half of the pending capacity
  is free.

### Performance
- The handler slot is released as soon as the handler returns, so the state
  write overlaps the next handler (as river's async completer does).
  `MaxPending` still bounds the writes in flight.
- New optional `driver.JobBatchStartMarker`: concurrent `started_at` writes
  share one `JobMarkStartedMany` call, like completions do with
  `JobBatchCompleter`. The group commit behind both is now one generic type.
- Measured on Postgres (pgxv5, embedded Postgres 18, localhost; v2.8.0 and
  this release in the same run, ratios only), 5000 jobs: c=32 with a 5 ms poll
  ~9 300 → ~15 500 jobs/s; c=4 ~2 600 → ~4 200 jobs/s; c=10 with the default
  1 s poll 42 → ~8 700 jobs/s.

### Tests
- `drivertest.Run` checks the batch writers (`JobMarkStartedMany`,
  `JobCompleteMany`) against the per-job fencing rules, and that concurrent
  fetchers never claim the same job.

---

## [driver mongodbv2 v2.8.0] — 2026-11-22

### Performance
- Implements `driver.JobBatchCompleter`: a batch of completions costs one
  `find` and one unordered `bulkWrite` instead of a `findOne` and an
  `updateOne` per job. Requires root v2.8.0.

### Tests
- The suite runs against an existing replica set when
  `GONCORDIA_TEST_MONGO_URI` is set (no Docker).

---

## [drivers pgxv5, stdlib, gorm, bun v2.8.0] — 2026-11-22

All four require root v2.8.0 (gorm and bun require stdlib v2.8.0).

### Performance
- pgxv5: partial fetch index (migration `010_fetch_ready_index.sql`),
  `JobInsertMany` in one pgx batch, and `driver.JobBatchCompleter`
  (one `UPDATE ... FROM unnest(...)` per batch of completions).
- stdlib: partial fetch index for Postgres (`011_fetch_ready_index.sql`) and
  `driver.JobBatchCompleter` for Postgres and SQLite
  (`UPDATE ... FROM (VALUES ...) RETURNING id`). MySQL keeps one statement
  per completion because its `UPDATE` has no `RETURNING`. gorm and bun use
  the stdlib executor, so they batch on Postgres and SQLite too.

### Tests
- pgxv5 and stdlib suites run against an existing Postgres when
  `GONCORDIA_TEST_PG_DSN` is set (no Docker); stdlib MySQL tests then need
  `GONCORDIA_TEST_MYSQL_DSN`.

---

## [v2.8.0] — 2026-11-22

Root module v2.8.0; drivers pgxv5 and stdlib follow with the driver-side
changes and require v2.8.0.

### Performance
- Fetching jobs no longer sorts the whole backlog. The fetch query filters
  `state IN ('available', 'scheduled')`, and no index returned rows in fetch
  order for that predicate, so every fetch sorted all ready jobs. A new
  partial index (migration `010_fetch_ready_index.sql` in pgxv5,
  `011_fetch_ready_index.sql` in stdlib Postgres) lets `LIMIT` stop after the
  first rows. With a 20k backlog: fetch query 4.9 ms → 0.05 ms,
  fetch + complete 5.4 ms → 0.55 ms per job.
- pgxv5 `EnqueueMany` / `JobInsertMany` sends all inserts in one pgx batch
  instead of one round trip per job: 100-job batches 28 ms → 6 ms
  (~3.5k → ~16.6k jobs/s). Unique-key behaviour is unchanged.
- Worker pools claim jobs in batches. `fetchAndDispatch` fetched one job
  per round trip under a mutex, so throughput stopped at one query latency
  per job whatever the concurrency. Each slot of the weighted queue schedule
  now claims up to `free / len(schedule)` jobs, capped by the queue's
  `Concurrency` policy, so queue weights keep their share (3:1 still claims
  3:1). Claim order inside a pass can differ from the one-by-one order.
- Successful completions are written in batches when the driver implements
  the new optional `driver.JobBatchCompleter` (pgxv5 does). Completions that
  finish while a write is in flight share the next `UPDATE` (group commit):
  no added wait, and each job's completion still returns only after its row
  is written, so observers, futures and pipelines see the same order. With
  32 workers the database ran ~4× fewer transactions for the same 5 000 jobs
  (~9 900 → ~2 350). Drivers without the interface are unchanged.
- End-to-end worker pool on Postgres, 5 000 jobs: c=4 ~850 → ~2 900 jobs/s,
  c=32 ~880 → ~8 150 jobs/s. (Embedded Postgres 18 on Windows, localhost;
  relative numbers only.)

---

## [drivers stdlib, gorm, bun v2.7.2] — 2026-11-19

### Fixed
- The MySQL dialect works on MariaDB. Recording a job error used
  `CAST(? AS JSON)`, which MariaDB rejects, so a failed job stayed running
  until rescued. CI now runs the MySQL suite on MariaDB 11.4 as well.
  gorm and bun v2.7.2 require stdlib v2.7.2.

---

## [v2.7.2] — 2026-11-18

### Fixed
- Stopping a worker pool (or the context of `RunRetention`) during a
  retention pass no longer reports `retention: ... context canceled` to
  `ErrorHandler`, `OnPrune`, or the retention observer, so a graceful
  shutdown no longer logs an error.

---

## [v2.7.1] — 2026-11-18

### Fixed
- Rate-limited workers wait until an absolute time. The wait used to be a
  duration computed from an earlier `Now`, so time that passed before the
  timer was created extended the wait instead of shortening it.

### Security (drivers v2.7.1)
- pgxv5, stdlib, mongodb, redis, cassandra, dynamodb, and firestore require
  `github.com/klauspost/compress` v1.20.4, which fixes GO-2026-6865. gorm and
  bun pick it up through stdlib v2.7.1. All of them require root v2.7.1.

### Added
- Documentation site at https://goncordia.docs7.io, built from `docs/`.

---

## [v2.7.0] — 2026-11-18

### Added
- The retention conformance suite (`drivertest.RunRetention`) checks that a
  discarded job rescheduled by an operator survives retention.
- `drivertest.RunScheduled` checks `JobReschedule`: the job is not claimable
  at its old `run_at` and is claimable at the new one.

### Added (drivers v2.7.0)
- Cassandra, ClickHouse, DynamoDB, and Firestore implement `driver.JobPruner`,
  so every driver now prunes through an index instead of listing jobs:
  - DynamoDB: sparse, 16-way sharded GSI `gsi_retention` on terminal jobs.
  - Firestore: query on `state`, `finalized_at`; needs the composite index
    `state ASC, finalized_at ASC` on `goncordia_jobs`.
  - Cassandra: lookup tables `goncordia_jobs_finalized` and
    `goncordia_results_written`, walked from a stored watermark.
  - ClickHouse: skip indexes on `state`, `finalized_at`, `id`, and result
    `updated_at`; expired rows, and rows `JobDelete` marked deleted, are
    removed with lightweight `DELETE`.

### Fixed (drivers v2.7.0)
- Cassandra: `JobReschedule` (admin "retry" of a discarded job) always failed
  with `Invalid operation (version = version + 1)`. It now updates the
  version conditionally and moves the job's `run_at` lookup entry, so the
  rescheduled job is claimed at its new time.

### Upgrading (drivers v2.7.0)
- These drivers do not migrate existing data. A DynamoDB jobs table created
  before v2.7.0 has no `gsi_retention`, so retention fails until you add it
  (key `rt_pk` HASH, `rt_at` RANGE, keys-only projection) or recreate the
  table. Cassandra and DynamoDB prune only jobs finalized after the upgrade.
  ClickHouse tables keep working without the skip indexes, but each pass
  scans the table; add them with `ALTER TABLE … ADD INDEX`.

---

## [v2.6.0] — 2026-11-18

### Added
- Retention metrics: `goncordia.NewRetentionMetrics()` renders
  `goncordia_retention_passes_total`, `_jobs_deleted_total`,
  `_results_deleted_total`, `_last_success_timestamp_seconds`, and
  `_last_pass_duration_seconds` for `admin.WithMetrics`.
- `goncordia.RetentionObserver` and `RetentionPassEvent`: a worker observer
  that implements it receives every retention pass; `WorkerObservers`
  forwards it, and `RetentionRunConfig.Observer` does the same for
  `RunRetention`.

### Added (drivers v2.6.0)
- pgxv5, stdlib (Postgres, MySQL, SQLite), gorm, bun, mongodb, and mongodbv2
  implement `driver.JobPruner`: retention deletes expired jobs and results in
  batches through new indexes instead of listing every job. `Migrate` adds
  `idx_goncordia_jobs_finalized` and `idx_goncordia_job_results_updated`; on
  a large Postgres table, create them `CONCURRENTLY` first (see README).

### Changed
- README: the two Testing sections are merged.

---

## [v2.5.1] — 2026-11-18

### Fixed
- `future.Await` no longer panics when a driver reports a missing job as
  `(nil, nil)`; it returns `future.ErrNotFound`. On Firestore this happened
  when awaiting a job that retention had deleted.
- The `Executor` contract now states that `JobGetByID` reports a missing job
  with `driver.ErrNotFound`, and `drivertest.Run` checks it.

### Added (driver/redis v2.5.1)
- Retention indexes (terminal jobs by finalization time, results by write
  time) and `driver.JobPruner`, so retention reads only expired entries
  instead of every job. `Migrate` backfills them; run it again after a
  rolling upgrade.

### Fixed (driver/firestore v2.5.1)
- `JobGetByID` returns `driver.ErrNotFound` for a missing job instead of
  `(nil, nil)`.

---

## [v2.5.0] — 2026-11-18

### Changed
- **Worker pools now prune old jobs by default**, on every driver, under a
  shared leader lease (`goncordia.DefaultRetentionPolicy`): completed and
  cancelled jobs after 24 hours, discarded jobs after 7 days, future results
  after 1 hour. Set `WorkerConfig.Retention` to change it
  (`goncordia.KeepForever` keeps a state) or `RetentionInterval: -1` to
  disable it. Before upgrading, check that nothing relies on keeping
  terminal jobs longer.
- `future.Func.Call` deletes its job and result after returning a value, since
  the job ID never leaves the call. Unique and failed jobs stay;
  `future.Config.KeepCalledJobs` keeps all.

### Added
- `goncordia.KeepForever`, `goncordia.DefaultRetentionPolicy`,
  `WorkerConfig.Retention`, and `WorkerConfig.RetentionInterval`.
- `driver.JobPruner`: an optional executor capability that lets retention
  delete expired jobs and results from an index instead of listing all jobs.
- `drivertest.RunRetention` checks retention against a driver; every built-in
  driver runs it.

---

## [v2.4.0] — 2026-11-18

### Added
- `future.ErrResultLost` and `driver.ErrResultLost` tell an evicted or
  otherwise lost result apart from `ErrNoResult`; outcome `result_lost`.
- `RetentionPolicy.Results` expires future results of completed jobs before
  the job rows; `PruneResult.ResultsDeleted` counts them.
- `Maintenance.RunRetention` prunes on a schedule under a leader lease, so
  every instance can run it and only one prunes at a time.
- `driver/mongodbv2`: the MongoDB driver on the official Go driver v2
  (`go.mongodb.org/mongo-driver/v2` v2.10.0). `New` takes a v2
  `*mongo.Client`, and the transaction type is `*mongo.Session`. It uses the
  same collections as `driver/mongodb`, so processes can migrate one by one.

### Added (driver/redis v2.4.0)
- `Migrate` rejects an `allkeys-*` `maxmemory-policy` with
  `redisdriver.ErrEvictionPolicy`; `WithAllowEviction()` opts out, and the
  check is skipped when `CONFIG` is blocked.
- An evicted result is reported as `driver.ErrResultLost`.

### Deprecated
- `driver/mongodb`, which uses the deprecated mongo-driver v1. Use
  `driver/mongodbv2`.

---

## [v2.3.0] — 2026-11-18

### Added
- `future.NewMetrics` aggregates futures metrics in memory and renders them in
  the Prometheus text format: in-flight awaits, await duration by kind and
  outcome, polls, wakeups, store duration, and result size.
- `admin.WithMetrics` appends any `WritePrometheus` writer, such as
  `future.Metrics`, to the admin `/metrics` endpoint.
- `future.Observers` combines futures observers, for example
  `future.Metrics` with OpenTelemetry instrumentation.

### Dependencies
- Driver, otel, gontest, gorm, bun, and bench modules require v2.2.0.
- AWS SDK v2 1.48.0 (config 1.33.18, credentials 1.21.0, DynamoDB 1.74.0,
  attributevalue 1.21.21), modernc.org/sqlite 1.62.0, and
  testcontainers MySQL module 0.45.0.

---

## [v2.2.0] — 2026-11-18

### Added
- `future.ErrNotFound` for a job that does not exist (rolled-back or
  uncommitted transaction, deleted job). It also matches
  `driver.ErrNotFound`, so existing checks keep working.

### Documentation
- `Await` on a job whose transaction rolled back (or has not committed)
  returns `future.ErrNotFound` immediately; documented and covered by a pgx v5
  test.

---

## [v2.1.0] — 2026-11-18

### Added
- `future.Define[A, R]()` declares a `Task` that ties job arguments to the
  handler's result type; `Task.Register` type-checks the handler.
- `future.Bind` returns a `*future.Func[A, R]` with `Call` (enqueue and
  await), `Enqueue`, `With` (insert options), and `Of`, so application code
  needs neither type parameters nor the queue client. `future.BindTx` adds
  `EnqueueTx`.
- Futures examples split into contract, `main` wiring, and application code.

---

## [v2.0.0] — 2026-11-18

### Breaking
- Module paths gain the `/v2` suffix (`github.com/kirimatt/goncordia/v2`,
  `github.com/kirimatt/goncordia/driver/pgxv5/v2`, ...). See
  `MIGRATING_TO_V2.md`.
- Removed deprecated APIs: `WorkerPool.Stop`, `ClientConfig.Now`,
  `core.WorkerOpts.Queue`, `QueuePolicy.RateLimit`/`RatePeriod`,
  `clock.Mock`/`NewMock`, and the `internal/clock` forwarding package.
- `gontest.Jobs` returns `([]*core.Job[T], error)`; `JobsE` is removed.
- `WorkerObserver` includes the lifecycle events; `WorkerLifecycleObserver`
  is removed. Embed the new `NopWorkerObserver` for partial observers.
- `driver.Executor` embeds `driver.JobResultStore`; `future.New` no longer
  fails for drivers without result storage.
- `clock.Clock` requires `NewTimer`.

### Added
- `goncordia.NopWorkerObserver` for observers that handle only some events.
- Runnable futures examples for the in-memory driver and pgx v5.
- `future` package: `future.Enqueue` / `EnqueueTx` return a `Future[R]` whose
  `Await` returns the value produced by a `future.RegisterWorker` handler, with
  typed `ErrDiscarded`, `ErrCancelled`, and `ErrNoResult` outcomes, `TryGet`,
  and `future.Of` for jobs enqueued elsewhere. Waiters in the worker's process
  wake on completion; others poll on the injected clock.
- `driver.JobResultStore`, embedded in every `Executor`, with the
  `drivertest.RunResults` conformance suite, implemented by every driver:
  memory, pgxv5 and stdlib (migration 008, `goncordia_job_results`), gorm,
  bun, MongoDB, Redis, Cassandra, ClickHouse, DynamoDB (new
  `goncordia_job_results` table created by `Migrate`), and Firestore.
  Deleting a job deletes its result.

### Fixed
- ClickHouse `JobGetByID` reports logically deleted jobs as `ErrNotFound`
  instead of returning them with state `deleted`.
- `goncordia.WorkerObservers` combines several worker observers.
- `otelgoncordia.NewFutureInstrumentation`: `goncordia.future.await` and
  `goncordia.future.store` spans plus await duration, polls, wakeups, result
  size, and store duration metrics.

### Changed
- CI, static analysis, race tests, and vulnerability scanning now validate the
  root module and every released driver/tooling module with workspace mode
  disabled, matching the dependency graph downloaded by consumers.
- The supported Go floor is now Go 1.26. Current releases of `golang.org/x/*`,
  testcontainers, go-redis, modernc sqlite, and OpenTelemetry require it, and
  Go 1.25 no longer receives security fixes. CI tests Go 1.26 and Go 1.27.

### Dependencies
- OpenTelemetry 1.47.0, pgx 5.13.0, go-redis 9.23.0, clickhouse-go 2.51.0,
  Firestore 1.29.2, gRPC 1.85.0, mongo-driver 1.17.10, bun 1.3.0,
  modernc sqlite 1.60.1, go-sql-driver/mysql 1.10.1, AWS SDK 1.44.0
  (DynamoDB 1.64.0), and testcontainers 0.45.0.
- Dependabot groups related updates across every module directory so one
  upstream release produces one pull request.

### Release engineering
- Added an idempotent grouped release workflow for nested modules. A rerun may
  reuse only tags that already point to the selected release commit, and nested
  releases do not replace the root release as the repository `Latest` release.
- Added committed dependency manifests for every independently versioned
  driver, integration, OpenTelemetry, `gontest`, and benchmark module release.

### Documentation
- Added a Contributor Covenant-based code of conduct, contribution guide,
  security policy, structured bug and feature request forms, and a pull request
  template to complete the GitHub community profile.

---

## [v1.0.0] — 2026-08-23

### Changed
- The root engine is now a lightweight module with only `robfig/cron` as a
  direct dependency. Production drivers, OpenTelemetry, `gontest`, and bench
  tooling are independent nested Go modules.
- Driver and integration import paths are unchanged, but consumers must pin the
  selected submodule explicitly (for example,
  `github.com/kirimatt/goncordia/driver/pgxv5@v1.0.0`).
- Repository development uses `go.work`; CI and release validation enumerate
  every module independently rather than relying on root `./...` to cross
  module boundaries.

### Compatibility
- The runtime API is the stabilized v0.19 API; v1 does not rename job, client,
  worker, clock, admission, or rate-limit types.
- Existing v0 imports continue to compile. Applications upgrading to v1 must
  update both the root requirement and every selected driver/integration module.
- Database schema compatibility is unchanged from v0.19. Run each selected
  driver's `Migrate` before starting workers.

### Documentation
- Added `MIGRATING_TO_V1.md` with module mapping, upgrade commands, rollout
  order, and verification steps.

---

## [v0.19.0] — 2026-08-23

### Added
- `NewWorkerPoolChecked`, `ValidateWorkerConfig`, `NewCronSchedulerChecked`, and
  `ValidateCronConfig` for fail-fast startup validation.
- Explicit driver capabilities for linearizable leases/CAS, persisted lifecycle
  timestamps, bounded fetches, and strict backend ordering.
- Keyed rate limits and keyed local concurrency by job kind, pipeline, or tag,
  plus UTC-aligned `RateLimitModeFixedWindow` limits.
- `WorkerPool.UpdateQueuePolicy` for atomic runtime policy reload; blocked
  rate-limit waiters wake immediately and re-evaluate the new policy.
- Portable enqueue admission control, typed `QueueFullError`, and
  `NewQueueDepthAdmission` for queue-depth backpressure.
- Persisted fenced `started_at` timestamps in every strict built-in driver and
  opt-in `RequireLifecyclePersistence` / `RequireStrictOrdering` checks.
- Worker lifecycle observer events and OTel claim-wait, rate-wait, started, and
  finished metrics.
- Error-returning `gontest.JobsE`, public `core.DecodePayload`, payload fuzzing,
  concurrent fixed-window properties, and a GCRA acquisition benchmark.

### Changed
- Strict global limits now use an O(1) storage-backed GCRA compare-and-swap
  cursor and return the exact next permit time; polling is only an error fallback.
- Local smooth limits use the same GCRA semantics. The legacy `RateLimit` field
  retains its burst-equals-limit behavior.
- Execution fetches are backend-bounded. DynamoDB, Firestore, Cassandra, and
  Redis examine at most 64–1024 due candidates per claim pass instead of loading
  the complete due backlog.
- SQL and MongoDB ready-order indexes now include the full priority, run time,
  creation time, and ID tie-break sequence.
- `attempted_at` remains claim time; `started_at` is handler-entry time, making
  claimed/waiting/executing latency observable without changing job states.

### Compatibility and guarantees
- Existing constructors remain source-compatible and return validation errors
  from `Start`; checked constructors report them immediately.
- ClickHouse is rejected for strict global limits and distributed pipelines
  because it cannot provide the required linearizable primitives.
- DynamoDB, Firestore, Cassandra, and Redis expose bounded candidate ordering,
  not strict global priority over an arbitrarily large due backlog. Enable
  `RequireStrictOrdering` to reject those drivers at startup.
- `QueueDepthAdmission` is advisory: its portable statistics read and later
  insert are not one atomic storage operation.

### Upgrade notes
- Run `Migrate` before starting v0.19 workers. SQL drivers apply migration 007;
  Cassandra and ClickHouse add `started_at`; MongoDB creates the v2 fetch index.
- Smooth rules with `Burst > 1` now use standard GCRA burst semantics rather
  than the v0.18 lease-lane approximation.

### Testing
- Expanded driver conformance with fenced lifecycle persistence and CAS rate
  primitives; added deterministic injected-clock tests for keyed quotas,
  fixed windows, runtime reload, admission rejection, and lifecycle metrics.

---

## [v0.18.0] — 2026-08-23

### Added
- Composable `QueuePolicy.RateLimits`: combine independent per-second,
  per-minute, per-hour, and per-day execution limits on one queue.
- `QueueRateLimit.Burst` for explicit burst capacity and
  `RateLimitScopeGlobal` for storage-backed coordination across worker pools.
- `WorkerConfig.RateLimitPollInterval` to bound retries for contended global
  permits.

### Changed
- Rate limits now account for handler starts immediately before execution rather
  than jobs claimed from storage. Prefetching therefore does not spend permits.
- New rate-limit rules use bounded leaky-bucket lanes driven by the injected
  clock. Every configured rule must grant a permit before the handler starts.
- Jobs waiting for a rate permit keep their claim lease alive, while a per-queue
  gate prevents the waiting backlog from consuming all execution slots.

### Compatibility
- The deprecated `QueuePolicy.RateLimit` and `RatePeriod` fields remain source
  compatible. They are normalized to a local rule whose burst equals the limit.
- Global limits reuse ownership-safe leader leases and require no schema
  migration. Participants must share a backend, queue name, and identical rule.
- ClickHouse global rate limits inherit its best-effort leader-election semantics;
  use local limits or an atomic-lease backend for a strict global ceiling.

### Testing
- Added deterministic injected-clock coverage for legacy fields, combined
  second/minute limits, global coordination across two worker pools, permit
  expiry, and shutdown while a job is waiting for execution capacity.

---

## [v0.17.0] — 2026-08-23

### Added
- `WorkerPool.Shutdown(ctx) error` for observable graceful-drain timeouts while
  retaining the source-compatible `Stop` wrapper.
- Explicit persisted claim leases on every built-in driver. `attempted_at` is
  now the immutable attempt start and `lease_expires_at` is renewed by heartbeat.
- Weighted queue scheduling through `WorkerConfig.QueuePolicies`, including
  per-queue concurrency limits and injected-clock fixed-window rate limits.
- Optional distributed `PipelineID` serialization across worker processes using
  renewable, ownership-safe driver leader leases.
- Versioned job payload envelopes, `VersionedJobArgs`, `InsertOpts.PayloadVersion`,
  and ordered `WorkerOpts.Upcasters` for schema evolution.
- Permanent decode classification for malformed, unsupported-future, and
  non-upcastable payloads so incompatible messages are discarded without retry churn.
- Native Redis created-at job indexing with migration backfill, and DynamoDB
  queue/state GSI queries for administrative listing and metrics paths.

### Changed
- Queue polling claims one job per weighted slot instead of allowing the first
  non-empty queue to consume the whole pending budget.
- Active jobs keep heartbeating while graceful shutdown drains; only jobs still
  waiting for execution capacity are yielded.
- Rescue uses explicit lease deadlines and falls back to legacy `attempted_at`
  only for rows written before the lease field existed.
- The supported Go floor is now Go 1.25. CI tests both Go 1.25 and Go 1.26 and
  uses `actions/setup-go` v7.
- Redis administrative reads use the maintained job index instead of a keyspace
  `SCAN`; a `SCAN` is performed only by `Migrate` to backfill legacy jobs.
- Tag creation continues to publish its GitHub Release in the same workflow;
  the release workflow retains manual recovery for an existing tag.

### Fixed
- Graceful shutdown no longer stops claim heartbeats before long-running handlers
  finish, preventing healthy work from being rescued during deployment drains.
- Heartbeats no longer overwrite the original attempt timestamp.
- Cassandra lease-column migration is repeatable even when the migration journal
  is rebuilt, and its running lookup now tracks lease deadlines.

### Testing
- Expanded reusable driver conformance to assert lease creation, immutable
  attempt timestamps, heartbeat extension, legacy fallback, and deadline rescue.
- Added deterministic tests for shutdown timeout reporting, heartbeat continuity,
  weighted fairness, queue concurrency, rate-window advancement, distributed
  pipelines across two pools, payload upcasting, and permanent decode failures.
- CI validates the full build and race suite on both supported Go release lines.

### Upgrade notes
- Call `Migrate` before starting v0.17 workers. SQL drivers apply migration 006;
  Cassandra and ClickHouse add `lease_expires_at`; MongoDB creates the lease index;
  Redis backfills its administrative created-at index.
- Rolling upgrades are supported for rescue: jobs without `lease_expires_at` use
  the legacy `attempted_at` cutoff until reclaimed by a v0.17 worker.
- Use `Shutdown(ctx)` when a deployment must fail or report an incomplete drain.
  `Stop()` remains available but reports timeout only through `ErrorHandler`.
- Distributed pipelines are opt-in. Enable `DistributedPipelines` only when all
  participants use the same backing store and `PipelineID` namespace.
- Payloads remain unwrapped at version 1. Version 2 and newer are stored in an
  internal envelope; register every N-to-N+1 upcaster before deploying producers
  that emit the newer version.

---

## [v0.16.0] — 2026-08-16

### Added
- Fenced worker state transitions using the expected worker ID and attempt
  number, preventing a rescued stale worker from completing a newer claim.
- Claim heartbeats on every built-in driver, driven by the injected clock.
- `WorkerConfig.MaxPending` to bound prefetched jobs independently of active
  execution concurrency.
- `WorkerConfig.HeartbeatInterval`, defaulting to one third of
  `StuckJobTimeout`.
- Stable driver error categories, beginning with `driver.ErrUnsupported` for
  `errors.Is` checks.
- Opaque versioned job and queue cursors plus `driver.JobPage` and
  `driver.QueuePage` response types.
- A portable `Maintenance` service with retention pruning, partial-result bulk
  retry/cancel/delete, and paginated dead-letter inspection/replay.
- Configurable symmetric retry jitter with an injectable deterministic random
  source.
- Enqueue/worker observer hooks and OpenTelemetry enqueue spans, enqueue
  duration, scheduled-lag, heartbeat, and rescued-lease metrics.
- Worker-returned `core.Discard`, `core.RetryAfter`, and `core.RetryAt`
  directives.
- `UniqueOpts.Key` for caller-defined deduplication dimensions.
- `UniqueOpts.Forever` for durable idempotency keys that survive terminal job
  states until explicit deletion.
- `core.Cron` for validated standard five-field cron expressions evaluated in
  an explicit time zone with daylight-saving support.
- Durable periodic job IDs, persisted compare-and-swap schedule cursors, and
  `CronCatchUpAll`, `CronCatchUpLatest`, and `CronSkipMissed` policies.
- Admin read-only mode, per-operation authorization hooks, and configurable job
  response redaction with secure payload-hiding defaults.
- Reproducible CI/release checks, a core-package coverage floor, cursor fuzz
  tests, automated changelog-derived GitHub releases, and grouped dependency
  update automation.

### Changed
- Pipeline and per-kind waiters no longer occupy global execution slots.
- Worker cancellation yields an interrupted claim without recording an error
  or consuming an attempt.
- Ready-job selection is standardized as `priority DESC, run_at ASC,
  created_at ASC, id ASC` across built-in drivers.
- Redis completion/retry/yield transitions now use `WATCH`/`MULTI` so claim
  validation and state changes are atomic.
- The memory driver no longer advertises fake transaction support, and
  non-transactional executors return `driver.ErrUnsupported` from `Begin`.
- Exponential retry calculation is integer-based and overflow-safe for
  arbitrarily large attempt numbers.
- Unique jobs now use bounded, versioned SHA-256 canonical keys. Uniqueness is
  global by default; `UniqueOpts.ByQueue` explicitly scopes it per queue, and
  `ByPeriod` uses fixed UTC-aligned windows.
- Redis and DynamoDB now create a job and its uniqueness reservation atomically.
- Every built-in driver enforces the same global canonical-key contract;
  ClickHouse remains best-effort under concurrent inserts.
- Cron leader resignation is ownership-safe on every built-in driver, and the
  scheduler releases its lease during shutdown without reusing the cancelled
  run context.
- Durable cron occurrences use permanent idempotency keys and only advance their
  persisted cursor after enqueue succeeds or is recognized as a duplicate.
- Administrative job ordering now uses the composite `created_at DESC, id DESC`
  cursor on every backend; queues are consistently ordered by name.
- The admin jobs and queues endpoints return `items`, `next_cursor`, and
  `has_more` instead of bare arrays.
- Built-in drivers classify missing records, invalid state transitions, and
  stale fenced claims with `ErrNotFound`, `ErrConflict`, and `ErrStaleClaim`.
- Admin API errors map invalid cursors to 400, missing records to 404, conflicts
  and stale claims to 409, and unsupported operations to 501.
- Drivers no longer close caller-supplied clients, pools, connections,
  databases, or sessions; resource ownership is consistently retained by the
  caller.
- SQL migrations are serialized across application instances with PostgreSQL/
  MySQL advisory locks and a SQLite immediate transaction. Concurrent DynamoDB
  migration calls now wait for tables another instance is creating.
- Panic failures now retain their runtime stack in `AttemptError.Trace` across
  every built-in backend. Queue-time metrics measure only time after a job is
  eligible, excluding intentional scheduled delay.
- Admin mutations require an explicit action confirmation header. Liveness and
  storage readiness are separate probes, metrics failures return 503 without
  partial samples, and dashboard assets now comply with a strict CSP without
  inline scripts or styles.
- Updated the AWS SDK, Firestore, ClickHouse, pgx, Redis, Testcontainers,
  OpenTelemetry, gRPC, GORM, and modernc SQLite dependency families in isolated
  batches validated against their affected driver suites.

### Testing
- Driver conformance now covers stale-worker fencing, heartbeat protection, and
  global priority selection beyond a storage-ordered candidate subset, plus
  uniqueness across queues, durable uniqueness after cancellation, and
  ownership-safe leader resignation, opaque pagination without duplicates, and
  invalid cursor classification.
- Added deterministic worker tests for heartbeat renewal, cancellation yield,
  bounded pending work, and pipeline/global-concurrency isolation.
- Added concurrent migration tests for pgx, PostgreSQL, MySQL, and SQLite, plus
  ownership checks proving `Driver.Close` leaves supplied resources usable.
- Added deterministic maintenance tests driven entirely by an injected manual
  clock, including retention cutoffs, partial bulk failures, and dead-letter
  state validation.
- Cross-driver conformance now verifies attempt stack-trace serialization, and
  retry jitter tests use an injected random source.
- Added admin security and failure-mode tests for authorization, read-only mode,
  payload redaction, explicit mutation confirmation, independent probes, strict
  CSP assets, and all-or-nothing metrics output.
- CI actions and analysis tools are pinned to immutable revisions or explicit
  versions; release tags must match the latest dated changelog section and the
  README installation version.

### Upgrade notes
- SQL users must run `Migrate` before starting workers. Migration 004 replaces
  the `(queue, unique_key)` active-job index with a global `unique_key` index.
- MongoDB users must run `Migrate` to create the global v2 unique index.
  Cassandra users must run `Migrate` to create `goncordia_uniq_v2`.
- Existing active jobs with the same legacy key in different queues must be
  completed, cancelled, or otherwise reconciled before the SQL or MongoDB
  global unique index can be created.
- Unique keys generated by this release intentionally differ from legacy keys;
  deploy updated producers only after the schema migration is complete.
- SQL users must also apply migration 005 for `goncordia_schedule_cursors`.
  Cassandra and ClickHouse users must call `Migrate`; DynamoDB migration creates
  the new `goncordia_schedule_cursors` table. Redis, MongoDB, Firestore, and the
  memory driver create cursor storage lazily.
- Cassandra compensates failed writes after reserving a key, but a process crash
  between the cross-partition writes can leave an orphaned reservation in
  `goncordia_uniq_v2`. ClickHouse uniqueness remains best-effort.
- Admin API consumers must read list results from the `items` field and follow
  `next_cursor`; the previous bare-array response is replaced by a page envelope.
- Callers that previously relied on `Driver.Close` to close a supplied resource
  must now close that resource directly.
- Admin API mutation clients must send `X-Goncordia-Confirm` with the action
  name. Job arguments, unique keys, and panic traces are now redacted by default;
  install an explicit `WithJobRedactor` policy if trusted clients require them.

---

## [v0.15.1] — 2026-08-16

### Fixed
- Due scheduled jobs are now claimable on Cassandra, ClickHouse, DynamoDB, and
  Firestore instead of remaining permanently in the `scheduled` state.
- Cassandra migrations backfill the due-job lookup for scheduled jobs created
  by earlier releases.

### Testing
- Added a reusable scheduled-job conformance test driven by `clock.Manual`, so
  drivers are verified before and after `run_at` without wall-clock sleeps.

### Upgrade notes
- Cassandra users must call `Migrate` once during deployment. It creates the
  migration journal and backfills available/scheduled lookup rows idempotently.
- Firestore still requires the documented composite index on `queue`, `state`,
  and `run_at`; the scheduled query now uses both supported ready states.

---

## [v0.15.0] — 2026-08-16

### Added
- Process-local `PipelineID` serialization across all drivers.
- Per-kind `WorkerOpts.Concurrency`, worker/job timeouts, generated or configured
  worker IDs, round-robin queue polling, and asynchronous error reporting.
- Automatic rescue of abandoned running jobs on every built-in backend.
- Lease-based cron leader election and enqueue-error reporting.
- MongoDB Change Stream notifications with polling fallback.
- `EnqueueBatch` and `EnqueueBatchTx` for per-item enqueue options.
- Embeddable `admin` HTTP dashboard and JSON API, health/readiness probes, and
  Prometheus-compatible queue metrics.
- Reusable `driver/drivertest` conformance suite, enabled for every built-in driver.
- Public injectable `clock` package with deterministic `Manual` timers and tickers.
- OpenTelemetry worker/pipeline attributes and queue-time histogram.
- Expanded `CLAUDE.md` guidance for PostgreSQL LISTEN/NOTIFY, polling fallback,
  non-transactional backends, and idempotent worker design.

### Changed
- SQL migrations are versioned in `goncordia_schema_migrations`; existing
  installations receive `pipeline_id` and leader-lease tables without replaying
  the initial schema.
- Production timestamps are persisted in UTC.
- Worker default maximum attempts now matches the documented value of three.
- `core.NoRetry` now discards immediately after the first failed attempt.
- Direct module dependencies are classified correctly by `go mod tidy`.
- Minimum Go patch and affected transitive modules were raised to versions with
  no reachable vulnerabilities reported by `govulncheck`.

### Fixed
- Worker metadata now propagates `CreatedAt` and `WorkerID` to handlers.
- State-transition, fetch, rescue, cron election, and cron enqueue failures are
  surfaced through configured error handlers.
- Redis claiming is atomic and keeps JSON array/timestamp fields valid after Lua
  transitions.
- Firestore queue pause creates missing queue metadata.
- Release tag input is validated before reaching the shell.

### Removed
- Configurable unique-state exclusions, which were never implemented consistently.
  Terminal jobs never block a new unique insert on any driver.

### Upgrade notes
- Go 1.26.6 or newer is required.
- Run each SQL driver's `Migrate` method during deployment so the versioned
  `pipeline_id` and cron-leader schema updates are applied before workers start.
- `core.UniqueOpts.ExcludeStates` has been removed; delete that field from callers.
- `PipelineID` ordering is process-local. Distributed serialization still requires
  routing a pipeline to one worker-pool process or an application-level lock.
- Mount the new admin handler behind application authentication; mutating queue and
  job endpoints intentionally do not implement an authentication policy.

---

## [v0.14.0] — 2026-05-17

### Added
- `context7.json` — Context7 MCP configuration: project title, description, indexing rules, and version history for AI assistant discovery
- `llms.txt` — standard AI crawler file describing the project and linking to documentation
- `CLAUDE.md` — context file for AI coding assistants (Claude Code reads this in projects that depend on goncordia)

### Changed
- `context7.json`: added `previousVersions` (v0.7.0 – v0.13.0) for multi-version indexing in Context7

---

## [v0.13.0] — 2026-05-17

### Added
- Competitor comparison table in README (River, Asynq, Machinery) with honest per-feature breakdown
- Transactional outbox pattern description in README intro

### Fixed
- `gofmt` formatting in `bench/bench_containers_test.go`, `driver/dynamodb/dynamodb.go`, `driver/dynamodb/executor.go`, `driver/firestore/executor.go`, `driver/firestore/firestore.go` — was causing CI failures

---

## [v0.11.0] — 2026-05-08

### Added
- **Test helpers** (`gontest/`): ergonomic testing utilities for workers and enqueue assertions
  - `NewClient(t)` — in-memory test client + `Tracker` sharing one store; `t.Cleanup` registered automatically
  - `NewClientWithClock(t, clk)` — same with a `MockClock` for scheduled-job tests
  - `Tracker.NewWorkerPool(registry, cfg)` — worker pool sharing the tracker's store for E2E in-memory tests
  - `Tracker.Driver()` — raw memory driver for state inspection (`AllJobs`, queue pausing, etc.)
  - `Jobs[T](tracker)` — return all jobs of kind T with deserialized args
  - `RequireEnqueued[T](t, tracker, n)` — assert exactly n jobs of kind T, returns them for arg inspection
  - `RequireNoEnqueued[T](t, tracker)` — assert zero jobs of kind T
  - `WorkerHelper[T]` — run a worker directly without a pool or database
  - `NewWorkerHelper[T](w)` / `WorkerFuncHelper[T](fn)` — constructors
  - `WorkerHelper.Work(ctx, args)` — invoke with minimal job fields (AttemptNum=1)
  - `WorkerHelper.WorkJob(ctx, job)` — invoke with a fully-specified `*core.Job[T]`
  - `RequireWork[T](t, ctx, w, args)` — one-liner: run worker, fatal on error
  - `MustEnqueue[T](t, ctx, client, args, opts)` — enqueue, fatal on error
  - `MockClock` (type alias for `internal/clock.Mock`) + `NewMockClock()`
  - `FormatJobList[T](jobs)` — readable job list for custom failure messages
- Added `gontest/` to project tree in README
- Added Testing section to README with usage examples

---

## [v0.10.0] — 2026-05-08

### Added
- Firestore benchmarks added to `bench/bench_containers_test.go`
- `benchmarkEndToEndN` helper for backends that need a smaller per-iteration workload
- Updated README benchmark tables with Firestore results (all 10 backends)

---

## [v0.9.0] — 2026-05-08

### Added
- **Cloud Firestore driver** (`driver/firestore`): Google Cloud Firestore via `cloud.google.com/go/firestore`
  - ACID multi-document transactions via `RunTransaction` — `EnqueueTx` is truly atomic
  - Pass `*firestore.Transaction` to `EnqueueTx` from inside a `RunTransaction` callback
  - Reads-before-writes ordering respected in the transactional insert path
  - Unique-key deduplication via conditional `Create` in a transaction
  - Optimistic concurrency for concurrent job claiming — each claim uses `RunTransaction`
  - `Migrate` is a no-op; composite index `(queue ASC, state ASC, run_at ASC)` must be created in the Firebase console for production
  - Firestore emulator supported: set `FIRESTORE_EMULATOR_HOST` before creating the client
  - Four tests: `EnqueueAndProcess`, `UniqueJobs`, `RetryAndDiscard`, `EnqueueTx`

---

## [v0.8.0] — 2026-05-08

### Added
- **Amazon DynamoDB driver** (`driver/dynamodb`): Amazon DynamoDB via AWS SDK for Go v2
  - Four-table schema: `goncordia_jobs`, `goncordia_uniq`, `goncordia_queues`, `goncordia_leaders`
  - GSI `gsi_queue_state` (PK: `queue_state = "{queue}#{state}"`, SK: `run_at`) for ordered, efficient job polling
  - Conditional `UpdateItem` with version + state check for lock-free concurrent job claiming
  - Unique-key deduplication via `PutItem` with `attribute_not_exists` condition on `goncordia_uniq`
  - Leader election via conditional `PutItem` with TTL expiry check on `goncordia_leaders`
  - `NoTx` type — DynamoDB conditional writes cannot span tables; `EnqueueTx` behaves like `Enqueue`
  - Compatible with DynamoDB Local for integration tests
- DynamoDB benchmarks added to `bench/bench_containers_test.go`
- Updated README with DynamoDB backend table entry, installation instructions, quick-start example, and transaction guarantees

---

## [v0.7.4] — 2026-05-07

### Fixed
- **Cassandra driver**: replaced `ScanCAS()` with `MapScanCAS()` on all LWT queries — Cassandra returns the existing row on failure, causing a scan error when no destination variables were provided

---

## [v0.7.3] — 2026-05-07

### Changed
- Cleaned git history (removed `refs/original` backup left by `filter-branch`)

---

## [v0.7.2] — 2026-05-07

### Changed
- Cleaned git history (removed Co-Authored-By from commit messages)

---

## [v0.7.1] — 2026-05-07

### Changed
- Retagged from v0.7.0 to v0.7.1 (clean git history)

---

## [v0.7.0] — 2026-05-07

### Added
- **Cassandra driver** (`driver/cassandra`): Apache Cassandra 3.11+, ScyllaDB, and DataStax Enterprise via `gocql`
  - Lightweight transactions (`IF NOT EXISTS` / `IF condition`) for atomic job claiming and unique-key deduplication
  - Two-table schema: `goncordia_jobs` (by id) + `goncordia_jobs_avail` (by queue/run\_at) for efficient ordered fetch
  - Leader election via LWT INSERT/UPDATE on `goncordia_leaders`
  - `NoTx` type — no rollback guarantee; `EnqueueTx` behaves like `Enqueue`
- **ClickHouse driver** (`driver/clickhouse`): ClickHouse 23+ via `clickhouse-go/v2`
  - `ReplacingMergeTree(version)` on all three tables; reads use `SELECT … FINAL`
  - Each state transition inserts a new higher-version row (append-only writes)
  - At-least-once semantics with brief race window between claim INSERT and FINAL confirmation
  - `NoTx` type — ClickHouse has no transactions
- Benchmarks for Cassandra and ClickHouse added to `bench/bench_containers_test.go`
- Updated README benchmark tables with results for all 9 backends

---

## [v0.6.0] — 2026-05-07

### Added
- `CHANGELOG.md` following [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) format
- Link to CHANGELOG from README

---

## [v0.5.0] — 2026-05-07

### Added
- Benchmarks for PostgreSQL, MongoDB, and Redis drivers via testcontainers (`bench/bench_containers_test.go`)
- GitHub Actions CI workflow (`go vet`, `gofmt` check, full test suite with `-race`)
- CI, Go Reference, and version badges in README

### Changed
- Updated benchmark results table in README with all 7 backends
- README code examples cleaned up: removed `{...}` placeholder syntax, fixed `intPtr` helper, unified type names

### Fixed
- Upgraded CI actions to Node.js 24 (`actions/checkout@v5`, `actions/setup-go@v6`)

---

## [v0.4.0] — 2026-05-07

### Added
- **Periodic / cron jobs**: `CronScheduler[TTx]` enqueues jobs on a configurable schedule
  - `core.Every(d time.Duration)` — fires on first tick, then every `d`
  - `core.ScheduleFunc` — plain function adapter for custom schedules
  - `CronConfig.TickInterval` and `CronConfig.Clock` for test control
- `core/schedule.go`: `Schedule` interface

---

## [v0.3.0] — 2026-05-07

### Added
- Benchmarks package (`bench/`) covering memory and SQLite drivers:
  - `BenchmarkEnqueue`, `BenchmarkEnqueueBatch100`, `BenchmarkFetchAndComplete` — raw driver path
  - `BenchmarkEndToEnd` — full WorkerPool round-trip

---

## [v0.2.0] — 2026-05-07

### Added
- **OpenTelemetry observability** (`otel/` package):
  - Span `goncordia.process` with attributes `kind`, `queue`, `id`, `attempt`
  - Histogram `goncordia.job.duration` (seconds) labelled by kind, queue, status
  - Counter `goncordia.job.count` labelled by kind, queue, status
  - `WithTracerProvider` / `WithMeterProvider` options
- `JobMiddleware` — composable middleware chain in `WorkerConfig.Middleware`
- Panic recovery inside the innermost handler: panics are converted to errors and recorded on the span; the worker pool always stays alive

---

## [v0.1.0] — 2026-05-07

### Added
- Core engine: job state machine (`available → running → completed / retryable / discarded / cancelled / scheduled`), retry policies, priority queues, unique jobs, scheduled jobs
- `Client[TTx]`: `Enqueue`, `EnqueueTx`, `EnqueueMany`, `EnqueueManyTx`, `Cancel`
- `WorkerPool[TTx]`: `Start`, `Stop`, graceful shutdown, configurable concurrency and poll interval
- `Driver[TTx]` interface parameterized by the native transaction type of each backend
- **PostgreSQL** driver (`driver/pgxv5`): pgx v5, LISTEN/NOTIFY, SKIP LOCKED, advisory locks, migrations
- **PostgreSQL / MySQL / SQLite** driver (`driver/stdlib`): `database/sql`, dialect-aware SQL
- **gorm** adapter (`driver/gorm`): thin wrapper over stdlib
- **bun** adapter (`driver/bun`): thin wrapper over stdlib
- **MongoDB** driver (`driver/mongodb`): replica set required, multi-document transactions via `mongo.SessionContext`, Change Streams notifications, migrations
- **Redis** driver (`driver/redis`): at-least-once semantics, Lua-atomic fetch via sorted sets, Pub/Sub notifications; `EnqueueTx` explicitly rejected
- **In-memory** driver (`driver/memory`): no persistence, deterministic, for tests; `AllJobs()` for state inspection
- `RetryPolicy` interface with `ExponentialRetry`, `FixedRetry`, `NoRetry` implementations
- `Clock` interface + `MockClock` for deterministic time control in tests
- MIT License

[v0.16.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.16.0
[v0.15.1]: https://github.com/kirimatt/goncordia/releases/tag/v0.15.1
[v0.15.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.15.0
[v0.14.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.14.0
[v0.13.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.13.0
[v0.12.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.12.0
[v0.11.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.11.0
[v0.10.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.10.0
[v0.9.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.9.0
[v0.8.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.8.0
[v0.7.4]: https://github.com/kirimatt/goncordia/releases/tag/v0.7.4
[v0.7.3]: https://github.com/kirimatt/goncordia/releases/tag/v0.7.3
[v0.7.2]: https://github.com/kirimatt/goncordia/releases/tag/v0.7.2
[v0.7.1]: https://github.com/kirimatt/goncordia/releases/tag/v0.7.1
[v0.7.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.7.0
[v0.6.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.6.0
[v0.5.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.5.0
[v0.4.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.4.0
[v0.3.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.3.0
[v0.2.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.2.0
[v0.1.0]: https://github.com/kirimatt/goncordia/releases/tag/v0.1.0
