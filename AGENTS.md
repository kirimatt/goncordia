# goncordia

Reference for coding agents (Codex, Claude Code, Cursor, and others) using goncordia in an application. `CLAUDE.md` imports this file.

For the full, version-specific docs use the Context7 MCP server with library ID `/kirimatt/goncordia` (or fetch `https://context7.com/kirimatt/goncordia/llms.txt?topic=<topic>`). Copy this file into your application's `AGENTS.md` / `CLAUDE.md` to keep these rules in the agent's context.

Job queue engine for Go. One `Driver[TTx]` interface works across Postgres, MySQL, SQLite, MongoDB, Redis, Cassandra, ClickHouse, DynamoDB, Firestore, and in-memory.

## Core concept

`TTx` is the native transaction type of your database library (e.g. `pgx.Tx`, `*sql.Tx`, `*gorm.DB`, `*mongo.Session` for mongodbv2). The client and worker pool are parameterized by it — you never touch an adapter layer.

## Picking a driver

| You use | Driver package | Import |
|---|---|---|
| pgx v5 | `driver/pgxv5` | `pgxdriver "github.com/kirimatt/goncordia/driver/pgxv5/v2"` |
| `database/sql` (Postgres, MySQL, MariaDB, SQLite) | `driver/stdlib` | `stdlibdriver "github.com/kirimatt/goncordia/driver/stdlib/v2"` |
| gorm | `driver/gorm` | `gormdriver "github.com/kirimatt/goncordia/driver/gorm/v2"` |
| bun | `driver/bun` | `bundriver "github.com/kirimatt/goncordia/driver/bun/v2"` |
| MongoDB (mongo-driver v2) | `driver/mongodbv2` | `mongodriver "github.com/kirimatt/goncordia/driver/mongodbv2/v2"` |
| MongoDB (mongo-driver v1, deprecated) | `driver/mongodb` | `mongodriver "github.com/kirimatt/goncordia/driver/mongodb/v2"` |
| Redis | `driver/redis` | `redisdriver "github.com/kirimatt/goncordia/driver/redis/v2"` |
| Cassandra / ScyllaDB | `driver/cassandra` | `cassandradriver "github.com/kirimatt/goncordia/driver/cassandra/v2"` |
| ClickHouse | `driver/clickhouse` | `clickhousedriver "github.com/kirimatt/goncordia/driver/clickhouse/v2"` |
| DynamoDB | `driver/dynamodb` | `dynamodbdriver "github.com/kirimatt/goncordia/driver/dynamodb/v2"` |
| Firestore | `driver/firestore` | `firestoredriver "github.com/kirimatt/goncordia/driver/firestore/v2"` |
| tests / no DB | `driver/memory` | `memorydriver "github.com/kirimatt/goncordia/v2/driver/memory"` |

## Defining a job

Every job type must implement `core.JobArgs` — a single `Kind() string` method plus be JSON-serializable:

```go
type SendEmailArgs struct {
    To      string `json:"to"`
    Subject string `json:"subject"`
}

func (SendEmailArgs) Kind() string { return "send_email" }
```

## Registering a worker

```go
registry := core.NewRegistry()
core.RegisterWorker(registry, core.WorkerFunc[SendEmailArgs](
    func(ctx context.Context, job *core.Job[SendEmailArgs]) error {
        return sendEmail(job.Args.To, job.Args.Subject)
    },
), core.WorkerOpts{MaxRetry: 5})
```

## Transactional insert (outbox pattern)

Use `EnqueueTx` to enqueue a job inside an existing transaction. The job becomes visible to workers only if the transaction commits — this is the transactional outbox pattern.

```go
// pgx v5 example
tx, _ := pool.Begin(ctx)
_, _ = queries.CreateOrder(ctx, tx, orderParams)
_, _ = client.EnqueueTx(ctx, tx, SendEmailArgs{To: "user@example.com"}, nil)
tx.Commit(ctx)  // job and order are atomic
```

Works with: Postgres, MySQL, SQLite (pgxv5, stdlib, gorm, bun), MongoDB, Firestore.
Rejected with `driver.ErrUnsupported`: Redis, Cassandra, ClickHouse, DynamoDB, in-memory (use `Enqueue` after commit instead).

## Enqueue without a transaction

```go
_, err := client.Enqueue(ctx, SendEmailArgs{To: "user@example.com", Subject: "Hi"}, nil)
```

## InsertOpts

```go
maxRetry := 3
client.Enqueue(ctx, args, &core.InsertOpts{
    Queue:    "critical",
    Priority: 10,
    RunAt:    time.Now().Add(time.Hour),
    MaxRetry: &maxRetry,
    UniqueOpts: &core.UniqueOpts{ByArgs: true, ByQueue: true},
})
```

## Starting a worker pool

```go
wp := pgxdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
    Queues:      []string{"default", "critical"},
    Concurrency: 10,
    PollInterval: 500 * time.Millisecond,
})
go wp.Start(ctx)  // blocks; cancel ctx or call wp.Shutdown(ctx) to drain
```

## Testing — always use the in-memory driver

No Docker, no database, deterministic time:

```go
import (
    "github.com/kirimatt/goncordia/gontest/v2"
    "github.com/kirimatt/goncordia/v2/clock"
)

// Assert that business code enqueues the right job:
client, tracker := gontest.NewClient(t)
_ = PlaceOrder(ctx, client, "order-123")
jobs := gontest.RequireEnqueued[SendEmailArgs](t, tracker, 1)

// Unit-test a worker function directly:
gontest.RequireWork(t, ctx, emailWorker, SendEmailArgs{To: "u@example.com"})

// Control time for scheduled jobs:
clk := gontest.NewMockClock()
client, tracker = gontest.NewClientWithClock(t, clk)
client.Enqueue(ctx, args, &core.InsertOpts{RunAt: clk.Now().Add(time.Hour)})
clk.Advance(2 * time.Hour)  // job is now available
```

## Waiting for a job's result (futures)

```go
// contract (shared by producers and workers)
var Sum = future.Define[SumArgs, int]()

// main: wiring
m, _ := future.New(d, future.Config{})            // every built-in driver stores results; run Migrate after upgrading
Sum.Register(registry, m, sumHandler, core.WorkerOpts{}) // sumHandler: func(ctx, *core.Job[SumArgs]) (int, error)
wp := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{Observer: m}) // local wake-up
billing := &Billing{sum: future.Bind(m, client, Sum)} // *future.Func[SumArgs, int]

// application code: no type parameters, no queue client
total, err := b.sum.Call(ctx, SumArgs{A: 2, B: 3}) // deletes the job after a value; errors.Is(err, future.ErrDiscarded / ErrCancelled / ErrNoResult / ErrNotFound)
f, err := b.sum.With(&core.InsertOpts{Queue: "critical"}).Enqueue(ctx, args) // wait later with f.Await
```

Use `future.BindTx` for `EnqueueTx(ctx, tx, args)`; await only after commit. After a rollback (or before commit) the job does not exist and `Await` returns `future.ErrNotFound` immediately.

Full runnable examples: `future/example_test.go` (memory) and
`driver/pgxv5/example_future_test.go` (Postgres with `EnqueueTx`).

Use `goncordia.WorkerObservers(a, b)` to combine the Manager with OTel
instrumentation, and `otelgoncordia.NewFutureInstrumentation()` as
`future.Config.Observer` for await/store spans and metrics. Custom observers embed
`goncordia.NopWorkerObserver` and override only the events they need.

## Periodic / cron jobs

```go
cs := goncordia.NewCronScheduler(d, []goncordia.PeriodicJob{
    {Schedule: core.Every(time.Hour), Args: CleanupArgs{}},
}, goncordia.CronConfig{TickInterval: time.Second})
go cs.Start(ctx)
```

## Push notifications — how LISTEN/NOTIFY works (pgxv5)

No user code required. When `d.Migrate(ctx)` runs, it creates a PostgreSQL trigger that calls `pg_notify('goncordia:{queue}', job_id)` after every INSERT into `goncordia_jobs`. The WorkerPool automatically calls `LISTEN "goncordia:{queue}"` on a dedicated connection. When a job is enqueued, the trigger fires immediately and the worker wakes up without waiting for the next poll cycle.

```
Enqueue() → INSERT → trigger → pg_notify('goncordia:default', '42')
                                    ↓
WorkerPool (LISTEN) ← notification arrives → fetch job immediately
```

If the LISTEN connection drops or a notification is missed, the worker falls back to polling at `PollInterval` — no jobs are lost.

Push notification support by backend:
- **pgxv5** — LISTEN/NOTIFY via PostgreSQL trigger (automatic, zero latency)
- **stdlib (Postgres)** — polling only (`ListenNotify: false`)
- **MongoDB** — Change Streams
- **Redis** — Pub/Sub
- **Cassandra, ClickHouse, DynamoDB, Firestore** — polling only

## At-least-once delivery for non-transactional backends

Redis, Cassandra, ClickHouse, and DynamoDB do not support transactions. Their
`EnqueueTx` method returns an unsupported-operation error. Use the post-commit
enqueue pattern:

```go
// Safe pattern for non-transactional backends
err := db.Transaction(func(tx *sql.Tx) error {
    return createOrder(tx, order)
})
if err == nil {
    // Enqueue only after the business transaction committed
    client.Enqueue(ctx, SendConfirmationArgs{OrderID: order.ID}, nil)
}
```

Workers on these backends must be **idempotent** — a job may be executed more than once if a worker crashes after claiming but before completing. Use `UniqueOpts` or application-level deduplication (e.g. check-then-insert with a unique key) to guard against duplicates.

## Common mistakes

- **Import paths**: v2 uses `github.com/kirimatt/goncordia/v2/...` for the root module and `github.com/kirimatt/goncordia/driver/<name>/v2` for drivers. `WorkerPool.Stop`, `ClientConfig.Now`, and `QueuePolicy.RateLimit` no longer exist; see `MIGRATING_TO_V2.md`.

- **Redis / Cassandra / ClickHouse / DynamoDB**: `EnqueueTx` is rejected. Use the post-commit enqueue pattern for at-least-once delivery.
- **Redis eviction**: `Migrate` fails with `redisdriver.ErrEvictionPolicy` under an `allkeys-*` `maxmemory-policy`, which could drop jobs and results. Use `noeviction` (or `volatile-*`); `WithAllowEviction()` opts out. An evicted result surfaces as `future.ErrResultLost`.
- **Retention**: worker pools prune by default (`goncordia.DefaultRetentionPolicy`: completed/cancelled 24h, discarded 7d, future results 1h) under a shared leader lease. Override with `WorkerConfig.Retention` (zero = default, `goncordia.KeepForever` = keep), disable with `RetentionInterval: -1`. Without a pool, run `maintenance.RunRetention`.
- **Futures cleanup**: `Func.Call` deletes its job and result after returning a value (not unique or failed jobs); `future.Config.KeepCalledJobs` keeps them.
- **MongoDB**: requires a replica set; standalone MongoDB will fail at `New()`.
- **SQLite**: set `db.SetMaxOpenConns(1)` — SQLite allows only one writer.
- **Firestore**: `Migrate` is a no-op; create the composite indexes manually in Firebase console: collection `goncordia_jobs`, fields `queue ASC, state ASC, run_at ASC` (fetching) and `state ASC, finalized_at ASC` (retention).

## MCP server for the agent itself

`github.com/kirimatt/goncordia/mcpserver/v2` exposes the queue to MCP clients:
`mcpserver.New(d)` (read-only: `list_queues`, `list_jobs`, `get_job`,
`get_metrics`), `mcpserver.WithWriteTools()` adds `retry_job`,
`reschedule_job`, `cancel_job`, `delete_job`, `pause_queue`, `resume_queue`.
Serve with `mcpserver.Handler(srv)` behind auth, or
`srv.Run(ctx, &mcp.StdioTransport{})`. Job args, unique keys and panic traces
are hidden unless `mcpserver.WithJobRedactor` exposes them.

## Injected time

Use `github.com/kirimatt/goncordia/v2/clock`. Pass the same `clock.Manual` to the
client, worker/scheduler config, and driver `WithClock` option in tests. Calling
`Advance` fires due timers and tickers without sleeping.

## Operations

Mount `admin.New(driver)` behind application authentication for the dashboard,
JSON job/queue actions, health/readiness probes, and Prometheus metrics. Add
futures metrics with `admin.WithMetrics(futureMetrics)`, where
`futureMetrics := future.NewMetrics(nil)` is also the Manager's
`future.Config.Observer` (combine observers with `future.Observers`).
Retention metrics come from `retention := goncordia.NewRetentionMetrics()`,
passed as `WorkerConfig.Observer` (via `goncordia.WorkerObservers`) and to
`admin.WithMetrics`; every driver except memory prunes through an index
(created by `Migrate`, except the Firestore composite index
`state ASC, finalized_at ASC`, which you create). Workers
automatically rescue stale running jobs; tune `StuckJobTimeout` and
`RescueInterval` in `WorkerConfig`.
