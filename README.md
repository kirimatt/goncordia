# goncordia

[![CI](https://github.com/kirimatt/goncordia/actions/workflows/ci.yml/badge.svg)](https://github.com/kirimatt/goncordia/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/kirimatt/goncordia.svg)](https://pkg.go.dev/github.com/kirimatt/goncordia)
[![GitHub release](https://img.shields.io/github/v/tag/kirimatt/goncordia?label=version)](https://github.com/kirimatt/goncordia/releases)

[Changelog](CHANGELOG.md)

A job queue engine for Go that works with the database you already have.

One `Driver[TTx]` interface parameterized by your library's native transaction type covers Postgres, MySQL, SQLite, MongoDB, Redis, Cassandra, ClickHouse, DynamoDB, Firestore, and in-memory — without forcing you to adopt a new dependency.

`EnqueueTx` implements the [transactional outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html): the job is written inside your existing transaction and becomes visible to workers only after commit. If the transaction rolls back, the job disappears — no separate relay process, no dual-write risk.

```go
tx, _ := pool.Begin(ctx)
_, _ = queries.CreateOrder(ctx, tx, order)
_, _ = client.EnqueueTx(ctx, tx, SendConfirmationArgs{OrderID: order.ID}, nil)
tx.Commit(ctx)  // job and order appear atomically
```

---

## Features

- **Transactional inserts** — `EnqueueTx` shares your existing transaction; the job appears if and only if that transaction commits
- **Scheduled jobs** — `InsertOpts.RunAt` for future execution
- **Priority queues** — higher priority processed first within a queue
- **Unique jobs** — deduplicate by kind, args, queue, or time window
- **Retry with backoff** — exponential (default), fixed, or custom `RetryPolicy`
- **Crash recovery** — abandoned `running` jobs are automatically returned to the queue
- **Execution controls** — global and per-kind concurrency, per-job timeouts, stable worker IDs
- **Pipelines** — serialize jobs sharing a `PipelineID` locally or, optionally, across worker processes
- **Fair multi-queue scheduling** — weighted round-robin with per-queue concurrency and rate limits
- **Payload evolution** — versioned payloads with ordered upcasters and permanent decode failures
- **Batch enqueue** — shared options or independent per-item options, with transactional variants
- **Queue pause/resume** — drain a queue without stopping workers
- **Push notifications** — LISTEN/NOTIFY (Postgres), Change Streams (MongoDB), Pub/Sub (Redis); polling fallback elsewhere
- **SKIP LOCKED** — lock-free concurrent fetching on Postgres and MySQL
- **Periodic / cron jobs** — lease-based single-leader scheduling across multiple instances
- **Admin and metrics** — embeddable dashboard, JSON API, health probes, and Prometheus endpoint
- **`clock.Manual`** — injected deterministic time for clients, workers, schedulers, and drivers

---

## Backends

| Driver | Package | Tx type | Atomic insert | Notes |
|---|---|---|---|---|
| PostgreSQL (pgx v5) | `driver/pgxv5` | `pgx.Tx` | ✅ | LISTEN/NOTIFY, lease leadership, SKIP LOCKED |
| PostgreSQL / MySQL / SQLite | `driver/stdlib` | `*sql.Tx` | ✅ | pgx stdlib, go-sql-driver/mysql, modernc sqlite |
| gorm | `driver/gorm` | `*gorm.DB` | ✅ | thin adapter over stdlib |
| bun | `driver/bun` | `bun.Tx` | ✅ | thin adapter over stdlib |
| MongoDB 4.0+ | `driver/mongodb` | `mongo.SessionContext` | ✅ | replica set required |
| Redis | `driver/redis` | `NoTx` | ❌ | at-least-once; Pub/Sub notifications |
| Cassandra 3.11+ | `driver/cassandra` | `NoTx` | ❌ | LWT claiming; ScyllaDB / DSE compatible |
| ClickHouse 23+ | `driver/clickhouse` | `NoTx` | ❌ | ReplacingMergeTree; at-least-once |
| Amazon DynamoDB | `driver/dynamodb` | `NoTx` | ❌ | conditional writes; at-least-once |
| Cloud Firestore | `driver/firestore` | `*firestore.Transaction` | ✅ | RunTransaction; composite index required |
| In-memory | `driver/memory` | `memory.NoTx` | ❌ | synchronized operations, no rollback; for tests |

---

## How it compares

| | goncordia | [River](https://github.com/riverqueue/river) | [Asynq](https://github.com/hibiken/asynq) | [Machinery](https://github.com/RichardKnop/machinery) |
|---|---|---|---|---|
| Backends | Postgres, MySQL, SQLite, MongoDB, Redis, Cassandra, ClickHouse, DynamoDB, Firestore, memory | **PostgreSQL only** | **Redis only** | Redis, RabbitMQ, SQS, MongoDB |
| Transactional insert (outbox pattern) | ✅ where backend supports tx | ✅ Postgres only | ❌ | ❌ |
| Generic `Driver[TTx]` interface | ✅ | ✅ (Postgres only) | — | — |
| Scheduled / cron jobs | ✅ | ✅ | ✅ | ✅ |
| Unique jobs | ✅ | ✅ | ✅ | ❌ |
| Priority queues | ✅ | ✅ | ✅ | ❌ |
| Push notifications | ✅ LISTEN/NOTIFY, Change Streams, Pub/Sub | ✅ LISTEN/NOTIFY | ✅ Pub/Sub | ❌ polling |
| Web UI | ✅ embeddable | ✅ (River UI, Pro) | ✅ (asynqmon) | ❌ |
| Workflow primitives (chains, chords) | ❌ | ❌ | ❌ | ✅ |

**Choose goncordia** when you want the transactional outbox pattern and don't want to introduce Redis or a dedicated Postgres instance — it works with whatever database your application already uses.

**Choose River** if you're on Postgres and want a mature, battle-tested library with a polished UI and a professional support tier.

**Choose Asynq** if Redis is already in your stack, you don't need transactional inserts, and you want a built-in web dashboard.

**Choose Machinery** if you need Celery-style workflow primitives (chains, chords, groups) across RabbitMQ or SQS.

---

## Installation

Requires Go 1.25 or newer.

```bash
go get github.com/kirimatt/goncordia@v0.17.0
```

Pick a driver:

```bash
# PostgreSQL via pgx v5
go get github.com/kirimatt/goncordia/driver/pgxv5 github.com/jackc/pgx/v5

# PostgreSQL / MySQL / SQLite via database/sql
go get github.com/kirimatt/goncordia/driver/stdlib

# gorm adapter
go get github.com/kirimatt/goncordia/driver/gorm gorm.io/gorm

# bun adapter
go get github.com/kirimatt/goncordia/driver/bun github.com/uptrace/bun

# MongoDB (replica set required)
go get github.com/kirimatt/goncordia/driver/mongodb go.mongodb.org/mongo-driver/mongo

# Redis
go get github.com/kirimatt/goncordia/driver/redis github.com/redis/go-redis/v9

# Cassandra / ScyllaDB
go get github.com/kirimatt/goncordia/driver/cassandra github.com/gocql/gocql

# ClickHouse
go get github.com/kirimatt/goncordia/driver/clickhouse github.com/ClickHouse/clickhouse-go/v2

# Amazon DynamoDB
go get github.com/kirimatt/goncordia/driver/dynamodb github.com/aws/aws-sdk-go-v2/service/dynamodb

# Cloud Firestore
go get github.com/kirimatt/goncordia/driver/firestore cloud.google.com/go/firestore
```

---

## Quick start

### PostgreSQL (pgx v5)

```go
import (
    "github.com/kirimatt/goncordia"
    "github.com/kirimatt/goncordia/core"
    pgxdriver "github.com/kirimatt/goncordia/driver/pgxv5"
    "github.com/jackc/pgx/v5/pgxpool"
)

pool, _ := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
defer pool.Close()
d := pgxdriver.New(pool)
d.Migrate(ctx)

type SendEmailArgs struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
}
func (SendEmailArgs) Kind() string { return "send_email" }

registry := core.NewRegistry()
core.RegisterWorker(registry, core.WorkerFunc[SendEmailArgs](
    func(ctx context.Context, job *core.Job[SendEmailArgs]) error {
        return sendEmail(job.Args.To, job.Args.Subject)
    },
), core.WorkerOpts{MaxRetry: 5})

client := pgxdriver.NewClient(d, goncordia.ClientConfig{})
client.Enqueue(ctx, SendEmailArgs{To: "user@example.com", Subject: "Welcome"}, nil)

wp := pgxdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
    Queues:      []string{"default"},
    Concurrency: 10,
})
wp.Start(ctx)  // blocks

shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
defer cancel()
_ = wp.Shutdown(shutdownCtx) // reports an incomplete drain
```

### Transactional insert (PostgreSQL)

```go
tx, _ := pool.Begin(ctx)
_, _ = queries.CreateOrder(ctx, tx, orderParams)
_, _ = client.EnqueueTx(ctx, tx, SendConfirmationArgs{OrderID: id}, nil)
tx.Commit(ctx)  // job and order are atomic
```

### MongoDB

```go
import (
    mongodriver "github.com/kirimatt/goncordia/driver/mongodb"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
)

client, _ := mongo.Connect(ctx, options.Client().ApplyURI(os.Getenv("MONGO_URI")))
defer client.Disconnect(ctx)
d, err := mongodriver.New(ctx, client, "myapp")  // fails if not a replica set
d.Migrate(ctx)

mqClient := mongodriver.NewClient(d, goncordia.ClientConfig{})

// Transactional insert via mongo.SessionContext
mongoClient.UseSession(ctx, func(sc mongo.SessionContext) error {
    sc.StartTransaction()
    db.Collection("orders").InsertOne(sc, order)
    mqClient.EnqueueTx(sc, sc, SendConfirmationArgs{OrderID: order.ID}, nil)
    return sc.CommitTransaction(sc)
})
```

### gorm

```go
import (
    gormdriver "github.com/kirimatt/goncordia/driver/gorm"
    "gorm.io/gorm"
)

d, _ := gormdriver.New(db)  // db is *gorm.DB
d.Migrate(ctx)

client := gormdriver.NewClient(d, goncordia.ClientConfig{})

db.Transaction(func(tx *gorm.DB) error {
    tx.Create(&order)
    client.EnqueueTx(ctx, tx, SendConfirmationArgs{OrderID: order.ID}, nil)
    return nil  // commit — job appears atomically with the order
})
```

### bun

```go
import (
    bundriver "github.com/kirimatt/goncordia/driver/bun"
    "github.com/uptrace/bun"
)

d := bundriver.New(db)  // db is *bun.DB
d.Migrate(ctx)

client := bundriver.NewClient(d, goncordia.ClientConfig{})

tx, _ := db.BeginTx(ctx, nil)
tx.NewInsert().Model(&order).Exec(ctx)
client.EnqueueTx(ctx, tx, SendConfirmationArgs{OrderID: order.ID}, nil)
tx.Commit()
```

### Redis

```go
import (
    redisdriver "github.com/kirimatt/goncordia/driver/redis"
    "github.com/redis/go-redis/v9"
)

rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
defer rdb.Close()
d := redisdriver.New(rdb)
d.Migrate(ctx)  // verifies connectivity and backfills the admin index

client := redisdriver.NewClient(d, goncordia.ClientConfig{})
client.Enqueue(ctx, SendEmailArgs{To: "user@example.com", Subject: "Welcome"}, nil)

// EnqueueTx is not supported on the Redis driver:
// there is no rollback guarantee. Use Enqueue (post-commit pattern) instead.
```

### Cassandra / ScyllaDB

```go
import (
    cassandradriver "github.com/kirimatt/goncordia/driver/cassandra"
    "github.com/gocql/gocql"
)

cluster := gocql.NewCluster("localhost")
cluster.Keyspace = "myapp"  // keyspace must already exist
session, _ := cluster.CreateSession()
defer session.Close()

d := cassandradriver.New(session)
d.Migrate(ctx)  // creates tables and runs idempotent lookup backfills

client := cassandradriver.NewClient(d, goncordia.ClientConfig{})
client.Enqueue(ctx, SendEmailArgs{To: "user@example.com", Subject: "Welcome"}, nil)

// EnqueueTx returns an unsupported-operation error on Cassandra.
// Enqueue after the business transaction commits and keep workers idempotent.
```

### ClickHouse

```go
import (
    clickhousedriver "github.com/kirimatt/goncordia/driver/clickhouse"
    "github.com/ClickHouse/clickhouse-go/v2"
)

conn, _ := clickhouse.Open(&clickhouse.Options{
    Addr: []string{"localhost:9000"},
    Auth: clickhouse.Auth{Database: "myapp"},
})
defer conn.Close()

d := clickhousedriver.New(conn)
d.Migrate(ctx)  // creates ReplacingMergeTree tables (idempotent)

client := clickhousedriver.NewClient(d, goncordia.ClientConfig{})
client.Enqueue(ctx, SendEmailArgs{To: "user@example.com", Subject: "Welcome"}, nil)

// ClickHouse has no transactions. Jobs use at-least-once delivery — workers
// should be idempotent. Best suited for high-throughput analytics pipelines.
```

### Amazon DynamoDB

```go
import (
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    dynamodbdriver "github.com/kirimatt/goncordia/driver/dynamodb"
)

cfg, _ := config.LoadDefaultConfig(ctx)
svc := dynamodb.NewFromConfig(cfg)

d := dynamodbdriver.New(svc)
d.Migrate(ctx)  // creates goncordia_jobs + goncordia_uniq + goncordia_queues + goncordia_leaders (idempotent)

client := dynamodbdriver.NewClient(d, goncordia.ClientConfig{})
client.Enqueue(ctx, SendEmailArgs{To: "user@example.com", Subject: "Welcome"}, nil)

// DynamoDB has no cross-table transactions. EnqueueTx returns an error.
// Unique-key deduplication uses PutItem with attribute_not_exists condition.
// Jobs are claimed with conditional UpdateItem — safe for concurrent workers.
```

### Cloud Firestore

```go
import (
    "cloud.google.com/go/firestore"
    firestoredriver "github.com/kirimatt/goncordia/driver/firestore"
)

fsClient, _ := firestore.NewClient(ctx, "my-gcp-project")
defer fsClient.Close()
d := firestoredriver.New(fsClient)
// Migrate is a no-op; create composite index in Firebase console:
//   collection: goncordia_jobs, fields: queue (ASC), state (ASC), run_at (ASC)
d.Migrate(ctx)

client := firestoredriver.NewClient(d, goncordia.ClientConfig{})

// Transactional — job is enqueued atomically with business writes:
fsClient.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
    tx.Create(orders.Doc(id), orderData)
    _, err := client.EnqueueTx(ctx, tx, SendConfirmationArgs{OrderID: id}, nil)
    return err
})
```

### SQLite (no Docker, good for tests)

```go
import (
    _ "modernc.org/sqlite"
    stdlibdriver "github.com/kirimatt/goncordia/driver/stdlib"
)

db, _ := sql.Open("sqlite", "./jobs.db")
defer db.Close()
db.SetMaxOpenConns(1)  // SQLite: single writer

d := stdlibdriver.New(db, stdlibdriver.SQLite)
d.Migrate(ctx)
```

All driver constructors are non-owning: a client, pool, connection, database,
or session passed to `New` remains the caller's responsibility. `Driver.Close`
only releases resources created internally by a driver and never closes the
supplied resource.

`Migrate` may be called concurrently during a rolling deployment. PostgreSQL
and MySQL use database advisory locks, while SQLite holds an immediate write
transaction across the complete migration set. Cassandra, ClickHouse, MongoDB,
DynamoDB, Redis, and Firestore use idempotent server-side schema operations;
DynamoDB also waits for tables created by another instance to become active.

---

## Job lifecycle

```
available ──► running ──► completed
                │
                ├──► retryable ──► available  (scheduled retry)
                └──► discarded               (max retries exhausted)

available ──► cancelled   (via JobCancel)
scheduled ──► running     (claimed once run_at is reached)
```

---

## InsertOpts

```go
maxRetry := 3
client.Enqueue(ctx, SendEmailArgs{To: "user@example.com", Subject: "Welcome"}, &core.InsertOpts{
    Queue:    "critical",                    // override default queue
    Priority: 10,                            // higher = processed first
    RunAt:    time.Now().Add(time.Hour),     // schedule for later

    UniqueOpts: &core.UniqueOpts{            // deduplicate
        ByArgs:  true,
        ByQueue: true,
        Key:     "welcome-email",           // optional caller-defined dimension
        ByPeriod: 24 * time.Hour,            // fixed UTC-aligned windows
        Forever:  false,                     // true keeps the key after finalization
    },

    MaxRetry: &maxRetry,
    Tags:     []string{"user:42"},
})
```

Unique jobs are global by default: the same canonical key is rejected even if
the second enqueue targets another queue. Set `ByQueue` to include the queue in
the key and allow one active job per queue. `ByArgs`, `Key`, and `ByPeriod` add
the serialized arguments, a caller-defined value, and the start of a fixed UTC
window respectively. The stored key is a bounded SHA-256 digest, so large job
arguments or caller keys do not expand database indexes. Completed, discarded,
and cancelled jobs release their key.

Set `Forever` for durable idempotency keys that must survive completion,
discard, or cancellation. An explicit job deletion still releases the key.

ClickHouse uniqueness is best-effort because concurrent inserts cannot be
serialized by `ReplacingMergeTree`. Cassandra uses an LWT reservation followed
by job writes; failed writes are compensated, but a process crash between them
can leave an orphaned reservation that must be removed operationally.

---

## WorkerConfig

```go
goncordia.WorkerConfig{
	Queues:          []string{"default", "critical"},
	QueuePolicies: map[string]goncordia.QueuePolicy{
		"critical": {Weight: 4, Concurrency: 12},
		"default":  {Weight: 1, RateLimit: 100, RatePeriod: time.Second},
	},
    Concurrency:     20,
    MaxPending:      80,                             // claimed jobs waiting/running; defaults to 4x concurrency
    WorkerID:        "mailer-eu-1",                  // generated when empty
    PollInterval:    500 * time.Millisecond,         // fallback when no push notifications
    RetryPolicy:     core.ExponentialRetry{Base: time.Second, Max: time.Hour},
    ShutdownTimeout: 30 * time.Second,
    StuckJobTimeout: time.Hour,                       // negative disables rescue
    RescueInterval: time.Minute,
	HeartbeatInterval: 20 * time.Minute,              // defaults to one third of StuckJobTimeout
	DistributedPipelines: true,                        // optional cross-process PipelineID lock
	PipelineLeaseDuration: 30 * time.Second,
    Clock:           clock.NewManual(time.Now()),     // omit in production; inject for tests
    ErrorHandler:    func(err error) { logger.Error("worker", "err", err) },
}

// Per-kind defaults and limits are configured at registration.
core.RegisterWorker(registry, worker, core.WorkerOpts{
    MaxRetry: 3, Timeout: 30 * time.Second, Concurrency: 4,
})
```

Running claims are fenced by worker ID and attempt number. `attempted_at` remains
the immutable attempt start; active and waiting claims renew `lease_expires_at`,
so rescue does not duplicate healthy long-running work. `Shutdown(ctx)` stops
new claims, keeps active heartbeats alive, and returns `ctx.Err()` if the drain
does not finish. Cancelling the pool context yields an interrupted claim without
consuming an attempt.

Across all built-in drivers, due jobs are selected by `priority DESC`, then
`run_at ASC`, `created_at ASC`, and `id ASC`.

---

## Batch enqueue and pipelines

Use `EnqueueBatch` when each job needs independent options; `EnqueueBatchTx`
keeps the whole batch inside the caller's native transaction.

```go
client.EnqueueBatch(ctx, []goncordia.InsertRequest{
    {Args: ResizeImageArgs{ID: "cover"}, Opts: &core.InsertOpts{Priority: 10}},
    {Args: IndexBookArgs{ID: "book-42"}, Opts: &core.InsertOpts{PipelineID: "book-42"}},
})
```

Jobs with the same non-empty `PipelineID` run sequentially in claim order inside
one `WorkerPool`. Set `DistributedPipelines: true` to extend the guarantee across
worker processes sharing the same driver. The pool renews an ownership-fenced
lease while the handler runs and cancels the handler context if renewal is lost.

### Versioned payloads

Version 1 remains plain legacy JSON. A type may declare a newer version and the
worker registers each adjacent upcast step:

```go
func (SendEmailArgs) PayloadVersion() int { return 2 }

core.RegisterWorker(registry, emailWorker, core.WorkerOpts{
    PayloadVersion: 2,
    Upcasters: map[int]core.PayloadUpcaster{
        1: func(old json.RawMessage) (json.RawMessage, error) {
            // Decode v1 and return v2 JSON.
            return upgradeEmailV1ToV2(old)
        },
    },
})
```

Malformed payloads, future versions, and missing/failed upcasters are permanent
decode failures and go directly to `discarded`; they do not consume the retry
schedule. `InsertOpts.PayloadVersion` can override the type-provided version.

---

## Admin dashboard, health, and metrics

```go
import "github.com/kirimatt/goncordia/admin"

handler := admin.New(d,
    admin.WithReadOnly(false),
    admin.WithAuthorizer(func(r *http.Request, operation admin.Operation) error {
        if !validSession(r) {
            return admin.ErrUnauthenticated
        }
        if operation == admin.OperationMutate && !canOperateJobs(r) {
            return errors.New("forbidden")
        }
        return nil
    }),
)
http.Handle("/jobs/", http.StripPrefix("/jobs", handler))
```

The handler exposes the embedded dashboard, `/healthz`, `/readyz`, `/metrics`,
and JSON routes under `/api`. It supports job filtering, cancel/delete/retry/
reschedule, queue pause/resume, and per-state queue counts. Redis uses a migrated
created-at index and DynamoDB uses its queue/state GSI for scoped reads. Unscoped
DynamoDB reads and Cassandra/Firestore administrative paths still scan records
and are intended for moderate operational use rather than high-frequency scraping.

The default JSON representation removes job arguments, unique keys, and panic
stack traces. Use `admin.WithJobRedactor` only when an application has a safe,
explicit policy for exposing or replacing those fields. `admin.WithReadOnly(true)`
disables every mutation while retaining inspection and metrics. The authorization
hook receives separate dashboard, read, mutate, metrics, and health operations;
return `admin.ErrUnauthenticated` for HTTP 401 and any other error for HTTP 403.

`/healthz` is a process liveness probe and does not touch storage. `/readyz`
checks that the backing driver can answer a query and returns HTTP 503 when it
cannot. Metrics are buffered until all queue statistics have been collected, so
a collection failure returns 503 without a misleading partial Prometheus body.
The embedded dashboard serves script and style assets separately under a strict
Content Security Policy.

Every mutation requires `X-Goncordia-Confirm` to exactly match the action name
(`pause`, `resume`, `cancel`, `delete`, `retry`, or `reschedule`). This is an
additional guard against accidental requests, not a substitute for authorization:

```text
POST /jobs/api/jobs/01J.../delete
X-Goncordia-Confirm: delete
```

`GET /api/jobs` and `GET /api/queues` return a page envelope:

```json
{"items": [], "next_cursor": "opaque-value", "has_more": true}
```

Pass `next_cursor` back as the `cursor` query parameter. Cursors are opaque and
versioned; callers must not parse or construct them. Job cursors preserve the
stable `created_at DESC, id DESC` order, including ties, and queue cursors use
queue-name order. Invalid cursors return HTTP 400. Driver errors map consistently:
not found to 404, conflict or stale claim to 409, and unsupported operations to
501. Applications can classify the same errors with `errors.Is` and
`driver.ErrNotFound`, `driver.ErrConflict`, `driver.ErrStaleClaim`,
`driver.ErrUnsupported`, or `driver.ErrInvalidCursor`.

---

## Retention, bulk actions, and dead letters

The portable maintenance service works with every built-in driver and uses the
same injected-clock model as clients, workers, and schedulers:

```go
maintenance := goncordia.NewMaintenance(d, goncordia.MaintenanceConfig{
    Clock: appClock,
})

result, err := maintenance.Prune(ctx, goncordia.RetentionPolicy{
    Completed: 30 * 24 * time.Hour,
    Discarded: 90 * 24 * time.Hour,
    Cancelled: 7 * 24 * time.Hour,
})

cancelled, err := maintenance.BulkCancel(ctx, jobIDs)
retried, err := maintenance.BulkRetry(ctx, jobIDs, time.Time{}) // now via Clock
deleted, err := maintenance.BulkDelete(ctx, jobIDs)

page, err := maintenance.DeadLetterList(ctx, driver.JobListParams{Limit: 100})
replayed, err := maintenance.DeadLetterReplay(ctx, jobIDs, time.Time{})
```

Bulk operations preserve partial progress in `BulkResult`; their returned error
joins per-job failures, so `errors.Is` still recognizes typed driver errors.
Dead-letter replay first verifies that each job is still discarded. Retention
durations of zero disable that state, and negative durations are rejected.

---

## Periodic / cron jobs

`CronScheduler` enqueues jobs on a schedule. Pair it with a `WorkerPool` that processes them.

```go
import "github.com/kirimatt/goncordia/core"

cs := goncordia.NewCronScheduler(d, []goncordia.PeriodicJob{
    {
        ID:       "hourly-cleanup", // stable ID enables durable cursor + deduplication
        StartAt:  time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
        Schedule: core.Every(time.Hour),
        Args:     CleanupArgs{},
        CatchUp:  goncordia.CronCatchUpAll,
    },
    {
        Schedule: core.Every(24 * time.Hour),
        Args:     ReportArgs{},
        Opts:     &core.InsertOpts{Queue: "low-priority"},
    },
}, goncordia.CronConfig{
    TickInterval: time.Second, // how often to check for due jobs
    LeaderName:   "hourly-maintenance",
    LeaderTTL:    30 * time.Second,
    MaxCatchUp:   100,
})

go cs.Start(ctx)   // blocks; cancel ctx to stop
go wp.Start(ctx)   // worker pool processes the enqueued jobs
```

### Custom schedule

```go
// core.ScheduleFunc adapts any function to the Schedule interface.
sched := core.ScheduleFunc(func(last time.Time) time.Time {
    if last.IsZero() {
        return time.Time{} // run immediately on first tick
    }
    // Business-hours only: next run at 09:00 the following day
    next := last.Add(24 * time.Hour)
    next = time.Date(next.Year(), next.Month(), next.Day(), 9, 0, 0, 0, next.Location())
    return next
})
```

### Cron expressions and time zones

```go
newYork, _ := time.LoadLocation("America/New_York")
weekdayMorning, err := core.Cron("30 9 * * 1-5", newYork)

cs := goncordia.NewCronScheduler(d, []goncordia.PeriodicJob{
    {Schedule: weekdayMorning, Args: ReportArgs{}},
}, goncordia.CronConfig{})
```

`core.Cron` accepts standard five-field expressions and evaluates calendar
boundaries in the supplied location, including daylight-saving transitions.
Pass `nil` to use UTC. Invalid expressions return an error during setup.

### Notes

- The scheduler fires each job on the **first tick** after `Start`, then respects the interval.
- A job with an empty `ID` uses that legacy process-local behavior. For durable
  scheduling, provide a stable `ID` and `StartAt`; the first occurrence is
  `Schedule.Next(StartAt)`.
- Durable cursors are persisted by the driver and advanced with compare-and-swap
  after enqueue. Each occurrence uses a permanent uniqueness key, so leader
  failover or a crash between enqueue and cursor advancement cannot duplicate it.
- `CronCatchUpAll` enqueues missed occurrences up to `MaxCatchUp` per tick,
  `CronCatchUpLatest` keeps only the latest, and `CronSkipMissed` drops old
  occurrences while still enqueueing an occurrence reached on the current tick.
- `CronScheduler` only *enqueues* — workers run via `WorkerPool`.
- Multiple scheduler instances are safe: only the current lease holder enqueues jobs.
- Scheduler shutdown releases leadership only when that scheduler instance still
  owns the lease; a stale instance cannot resign a newer leader.

---

## Retry policies

```go
// Exponential backoff: 1s, 2s, 4s, … capped at Max, with ±20% jitter
core.ExponentialRetry{Base: time.Second, Max: 24 * time.Hour, Jitter: 0.2}

// Tests can inject deterministic randomization:
core.ExponentialRetry{Base: time.Second, Jitter: 0.2, Random: func() float64 { return 0.5 }}

// Fixed delay
core.FixedRetry{Delay: 30 * time.Second}

// No retry — discard immediately
core.NoRetry{}

// Per-attempt directives returned by a worker
return core.Discard(err)                   // permanent failure
return core.RetryAfter(30*time.Second, err)
return core.RetryAt(nextWindow, err)

// Custom
import "github.com/kirimatt/goncordia/clock"

type MyPolicy struct{}
func (MyPolicy) NextRetryAt(attempt int, err error, clk clock.Clock) time.Time {
    return clk.Now().Add(time.Duration(attempt) * time.Minute)
}
```

---

## Testing

Use the in-memory driver — no database, no Docker, deterministic time:

```go
import (
    "github.com/kirimatt/goncordia/clock"
    "github.com/kirimatt/goncordia/driver/memory"
)

clk := clock.NewManual(time.Now())
d   := memory.New(memory.WithClock(clk))

client := goncordia.NewClient[memory.NoTx](d, goncordia.ClientConfig{Clock: clk})
wp     := goncordia.NewWorkerPool[memory.NoTx](d, registry, goncordia.WorkerConfig{Clock: clk})

go wp.Start(ctx)
clk.Advance(time.Hour)  // trigger scheduled jobs instantly

jobs := d.AllJobs()  // inspect state without a real database
```

---

## Implementing a custom driver

Implement `driver.Driver[TTx]` where `TTx` is your transaction type:

```go
type MyDriver struct{}

func (d *MyDriver) Name() string                        { return "mydb" }
func (d *MyDriver) Capabilities() driver.Capabilities  { return driver.Capabilities{NativeTx: true} }
func (d *MyDriver) Executor() driver.Executor          { return &myExecutor{} }
func (d *MyDriver) UnwrapTx(tx MyTx) driver.ExecutorTx { return &myTxExecutor{tx: tx} }
func (d *MyDriver) Listener() driver.Listener          { return nil } // nil = polling fallback
func (d *MyDriver) Close() error                       { return nil }
```

See [`driver/driver.go`](driver/driver.go) for the full core interface and optional rescue/admin capabilities, and [`driver/memory/memory.go`](driver/memory/memory.go) for a reference implementation. Driver authors can reuse `driver/drivertest.Run` for the base contract and `driver/drivertest.RunScheduled` with an injected `clock.Manual` for scheduled-job conformance. The base contract includes opaque job/queue pagination, deterministic ordering, invalid-cursor classification, fencing, and typed stale-claim behavior.

---

## Benchmarks

```
go test ./bench/... -bench=. -benchmem -benchtime=5s -timeout=15m
```

Apple M5, single process. Memory/SQLite are in-process (no network); Postgres/MongoDB/Redis run in Docker on localhost.

**Enqueue — single job**

| Backend | ns/op | Notes |
|---|---|---|
| Memory | 0.57 µs | in-process mutex, no I/O |
| SQLite | 27 µs | WAL mode, single connection |
| Redis | 109 µs | ZADD over localhost |
| Postgres (pgx v5) | 129 µs | INSERT over localhost |
| MongoDB | 338 µs | insertOne over localhost |
| DynamoDB | 632 µs | PutItem over localhost |
| Firestore | 1 195 µs | RunTransaction + Create over emulator |
| ClickHouse | 1 378 µs | INSERT + new data part over localhost |
| Cassandra | 7 216 µs | LWT requires Paxos quorum (3 round trips) |

**EnqueueBatch(100) — 100 jobs per call**

| Backend | ms/batch | jobs/s |
|---|---|---|
| Memory | 0.06 ms | ~1 775 000 |
| SQLite | 2.8 ms | ~35 300 |
| Redis | 10.9 ms | ~9 200 |
| Postgres (pgx v5) | 12.8 ms | ~7 800 |
| MongoDB | 34.9 ms | ~2 900 |
| DynamoDB | 62.9 ms | ~1 590 |
| Firestore | 93.5 ms | ~1 070 |
| ClickHouse | 150 ms | ~665 |
| Cassandra | 708 ms | ~141 |

**FetchAndComplete — hot worker loop path**

| Backend | µs/op | Notes |
|---|---|---|
| SQLite | 53 µs | indexed; faster than memory at scale |
| Memory | 520 µs | O(N) linear scan |
| Redis | 729 µs | Lua ZPOPMIN + HSET |
| DynamoDB | 1 731 µs | Query GSI + conditional UpdateItem |
| MongoDB | 2 475 µs | findAndModify + updateOne |
| Postgres (pgx v5) | 12 190 µs | SELECT SKIP LOCKED + UPDATE |
| ClickHouse | 14 416 µs | SELECT FINAL + INSERT new version |
| Cassandra | 18 813 µs | SELECT avail + LWT UPDATE per job |
| Firestore | 140 943 µs | Query + per-job RunTransaction on emulator |

**End-to-end — full WorkerPool**

| Backend | workload | concurrency | jobs/s | Notes |
|---|---|---|---|---|
| Memory | 1 000 | c=10 | ~2 020 | |
| Redis | 1 000 | c=4 | ~1 084 | Pub/Sub notifications |
| SQLite | 1 000 | c=4 | ~800 | |
| DynamoDB | 1 000 | c=4 | ~780 | polling; GSI query overhead |
| MongoDB | 1 000 | c=4 | ~452 | Change Streams |
| Postgres (pgx v5) | 1 000 | c=4 | ~179 | LISTEN/NOTIFY |
| Cassandra | 1 000 | c=4 | ~153 | polling; LWT overhead |
| ClickHouse | 1 000 | c=4 | ~148 | polling; SELECT FINAL overhead |
| Firestore | 200 | c=4 | ~14 | polling; per-job RunTransaction on emulator |

End-to-end throughput is bounded by the 5 ms poll interval used in the benchmark. In production the pgxv5 driver uses LISTEN/NOTIFY and the Redis driver uses Pub/Sub, eliminating poll latency entirely — real throughput matches the FetchAndComplete numbers above.

Cassandra's high per-operation latency comes from Lightweight Transaction consensus (Paxos, ~3 network round trips per claim). ClickHouse's overhead comes from `SELECT … FINAL` deduplication at query time. DynamoDB's per-operation cost is dominated by HTTP/JSON round trips to the service — measured against DynamoDB Local on localhost, so real AWS numbers include additional network latency. Firestore's per-job `RunTransaction` for claiming is the primary bottleneck on the emulator; production GCP Firestore will be faster due to lower round-trip latency, but the sequential-per-job claiming model inherently limits throughput. All four backends are best suited for workloads where horizontal scale matters more than raw per-job latency.

---

## Testing

`gontest` makes it easy to test workers and enqueue assertions without a real database.

```bash
go get github.com/kirimatt/goncordia/gontest
```

**Assert that business logic enqueues the right jobs:**

```go
func TestPlaceOrder_EnqueuesConfirmationEmail(t *testing.T) {
    ctx := context.Background()
    client, tracker := gontest.NewClient(t)

    _ = PlaceOrder(ctx, client, "order-123") // calls client.Enqueue internally

    jobs := gontest.RequireEnqueued[SendEmailArgs](t, tracker, 1)
    if jobs[0].Args.OrderID != "order-123" {
        t.Errorf("unexpected order ID: %s", jobs[0].Args.OrderID)
    }
}
```

**Unit-test a worker function without a database or pool:**

```go
func TestEmailWorker_SendsEmail(t *testing.T) {
    h := gontest.NewWorkerHelper[SendEmailArgs](emailWorker)
    if err := h.Work(ctx, SendEmailArgs{To: "user@example.com"}); err != nil {
        t.Fatal(err)
    }
}

// Or with a one-liner:
gontest.RequireWork(t, ctx, emailWorker, SendEmailArgs{To: "user@example.com"})
```

**Test scheduled jobs with a controllable clock:**

```go
clk := gontest.NewMockClock()
client, tracker := gontest.NewClientWithClock(t, clk)

client.Enqueue(ctx, ReminderArgs{UserID: "u1"}, &core.InsertOpts{
    RunAt: clk.Now().Add(24 * time.Hour),
})

// Job exists but is not yet available:
gontest.RequireEnqueued[ReminderArgs](t, tracker, 1)

// Advance past the scheduled time:
clk.Advance(25 * time.Hour)
// now start the pool — the job will be picked up immediately
```

**Run an end-to-end flow in memory:**

```go
registry := core.NewRegistry()
core.RegisterWorker(registry, emailWorker, core.WorkerOpts{Queue: "default"})

client, tracker := gontest.NewClient(t)
pool := tracker.NewWorkerPool(registry, goncordia.WorkerConfig{
    Queues: []string{"default"}, Concurrency: 2,
})

runCtx, cancel := context.WithCancel(ctx)
defer cancel()
go pool.Start(runCtx)
// enqueue and wait for processed.Load() >= 1 ...
pool.Stop()
```

---

## Observability (OpenTelemetry)

```bash
go get github.com/kirimatt/goncordia/otel
```

```go
import otelgoncordia "github.com/kirimatt/goncordia/otel"

instrumentation := otelgoncordia.NewInstrumentation(
    otelgoncordia.WithTracerProvider(tp),
    otelgoncordia.WithMeterProvider(mp),
    otelgoncordia.WithClock(clk),
)

client := pgxdriver.NewClient(d, goncordia.ClientConfig{
    Observer: instrumentation,
})

wp := pgxdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
    Queues:      []string{"default"},
    Concurrency: 10,
    Observer:    instrumentation,
    Middleware: []goncordia.JobMiddleware{
        otelgoncordia.NewMiddleware(
            // optional — defaults to otel.GetTracerProvider() / otel.GetMeterProvider()
            otelgoncordia.WithTracerProvider(tp),
            otelgoncordia.WithMeterProvider(mp),
            otelgoncordia.WithClock(clk), // optional; use the same Manual clock in tests
        ),
    },
})
```

The instrumentation produces:

- **Span** `goncordia.enqueue` with driver, batch, queue/kind, and outcome attributes
- **Span** `goncordia.process` with job, worker, and pipeline attributes
- **Histogram** `goncordia.enqueue.duration` (seconds) — storage latency by driver/status
- **Histogram** `goncordia.job.duration` (seconds) — labelled by kind, queue, status
- **Histogram** `goncordia.job.queue_time` (seconds) — time from eligibility
  (`max(created_at, run_at)`) to handler start; scheduled waiting is excluded
- **Histogram** `goncordia.job.schedule_lag` (seconds) — scheduled `run_at` to claim
- **Counter** `goncordia.job.heartbeat.count` — heartbeat outcomes (`ok`/`stale`/`error`)
- **Counter** `goncordia.job.lease_rescued` — expired claims returned to queues
- **Counter** `goncordia.job.count` — labelled by kind, queue, status (`ok` / `error`)

Panics are recovered, converted to errors, recorded on the span, and persisted
with their stack trace in `driver.AttemptError.Trace` before retry/discard. The
worker pool always stays alive.

You can also add your own middleware for logging or custom metrics:

```go
func loggingMiddleware(ctx context.Context, job *core.RawJob, next func(context.Context, *core.RawJob) error) error {
    slog.InfoContext(ctx, "job started", "kind", job.Kind, "id", job.ID)
    err := next(ctx, job)
    slog.InfoContext(ctx, "job finished", "kind", job.Kind, "err", err)
    return err
}

goncordia.WorkerConfig{
    Middleware: []goncordia.JobMiddleware{
        otelgoncordia.NewMiddleware(),
        loggingMiddleware,
    },
}
```

---

## Project layout

```
goncordia/
├── client.go              # Client[TTx] — Enqueue, EnqueueTx, Cancel
├── worker.go              # WorkerPool[TTx] — Start, Shutdown, queue policies, middleware
├── cron.go                # CronScheduler[TTx] — periodic/cron job scheduling
├── admin/                 # dashboard, JSON API, probes, Prometheus metrics
├── clock/                 # injectable Real and Manual clocks
├── core/
│   ├── job.go             # JobArgs, Worker, InsertOpts, WorkerOpts
│   ├── registry.go        # type-erased worker dispatch
│   ├── retry.go           # RetryPolicy, ExponentialRetry, FixedRetry, NoRetry
│   └── schedule.go        # Schedule interface, Every, ScheduleFunc
├── driver/
│   ├── driver.go          # Driver[TTx], Executor, ExecutorTx, Listener interfaces
│   ├── memory/            # in-memory (no persistence; for tests)
│   ├── pgxv5/             # PostgreSQL via pgx v5 (LISTEN/NOTIFY, lease leadership)
│   ├── stdlib/            # PostgreSQL + MySQL + SQLite via database/sql
│   ├── gorm/              # gorm adapter (wraps stdlib)
│   ├── bun/               # bun adapter (wraps stdlib)
│   ├── mongodb/           # MongoDB 4.0+ replica set
│   ├── redis/             # Redis (at-least-once; Pub/Sub notifications)
│   ├── cassandra/         # Cassandra 3.11+ / ScyllaDB (LWT claiming; at-least-once)
│   ├── clickhouse/        # ClickHouse 23+ (ReplacingMergeTree; at-least-once)
│   ├── dynamodb/          # Amazon DynamoDB (conditional writes; at-least-once)
│   └── firestore/         # Cloud Firestore (RunTransaction; ACID inserts)
├── gontest/               # test helpers (Tracker, WorkerHelper, MockClock)
└── otel/                  # OpenTelemetry middleware (spans + metrics)
```

---

## Transaction guarantees by backend

| Backend | Guarantee | Mechanism |
|---|---|---|
| Postgres / MySQL / SQLite | Atomic with business tx | Same DB connection, same `BEGIN`/`COMMIT` |
| gorm / bun | Atomic with business tx | Extracts underlying `*sql.Tx` |
| MongoDB | Atomic with business tx | Multi-document transaction on replica set |
| Redis | **None** — at-least-once | Pub/Sub + idempotent workers |
| Cassandra | **None** — at-least-once | LWT for claiming; no cross-statement tx |
| ClickHouse | **None** — at-least-once | ReplacingMergeTree + FINAL; no transactions |
| DynamoDB | **None** — at-least-once | Conditional writes; no cross-table tx |
| Firestore | Atomic with business tx | `RunTransaction` + `EnqueueTx` |
| In-memory | Atomic (in-process) | Single mutex |
