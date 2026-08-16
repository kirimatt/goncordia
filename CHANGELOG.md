# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

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
