# goncordia

Job queue engine for Go. One `Driver[TTx]` interface works across Postgres, MySQL, SQLite, MongoDB, Redis, Cassandra, ClickHouse, DynamoDB, Firestore, and in-memory.

## Core concept

`TTx` is the native transaction type of your database library (e.g. `pgx.Tx`, `*sql.Tx`, `*gorm.DB`, `mongo.SessionContext`). The client and worker pool are parameterized by it — you never touch an adapter layer.

## Picking a driver

| You use | Driver package | Import |
|---|---|---|
| pgx v5 | `driver/pgxv5` | `pgxdriver "github.com/kirimatt/goncordia/driver/pgxv5"` |
| `database/sql` (Postgres, MySQL, SQLite) | `driver/stdlib` | `stdlibdriver "github.com/kirimatt/goncordia/driver/stdlib"` |
| gorm | `driver/gorm` | `gormdriver "github.com/kirimatt/goncordia/driver/gorm"` |
| bun | `driver/bun` | `bundriver "github.com/kirimatt/goncordia/driver/bun"` |
| MongoDB | `driver/mongodb` | `mongodriver "github.com/kirimatt/goncordia/driver/mongodb"` |
| Redis | `driver/redis` | `redisdriver "github.com/kirimatt/goncordia/driver/redis"` |
| Cassandra / ScyllaDB | `driver/cassandra` | `cassandradriver "github.com/kirimatt/goncordia/driver/cassandra"` |
| ClickHouse | `driver/clickhouse` | `clickhousedriver "github.com/kirimatt/goncordia/driver/clickhouse"` |
| DynamoDB | `driver/dynamodb` | `dynamodbdriver "github.com/kirimatt/goncordia/driver/dynamodb"` |
| Firestore | `driver/firestore` | `firestoredriver "github.com/kirimatt/goncordia/driver/firestore"` |
| tests / no DB | `driver/memory` | `memorydriver "github.com/kirimatt/goncordia/driver/memory"` |

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
), core.WorkerOpts{Queue: "default", MaxRetry: 5})
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

Works with: Postgres (pgxv5, stdlib, gorm, bun), MongoDB, Firestore, in-memory.
Does NOT support rollback: Redis, Cassandra, ClickHouse, DynamoDB (use `Enqueue` after commit instead).

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
go wp.Start(ctx)  // blocks; cancel ctx or call wp.Stop() to drain
```

## Testing — always use the in-memory driver

No Docker, no database, deterministic time:

```go
import (
    "github.com/kirimatt/goncordia/gontest"
    "github.com/kirimatt/goncordia/clock"
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

- **Redis / Cassandra / ClickHouse / DynamoDB**: `EnqueueTx` is rejected. Use the post-commit enqueue pattern for at-least-once delivery.
- **MongoDB**: requires a replica set; standalone MongoDB will fail at `New()`.
- **SQLite**: set `db.SetMaxOpenConns(1)` — SQLite allows only one writer.
- **Firestore**: `Migrate` is a no-op; create the composite index manually in Firebase console: collection `goncordia_jobs`, fields `queue ASC, state ASC, run_at ASC`.

## Injected time

Use `github.com/kirimatt/goncordia/clock`. Pass the same `clock.Manual` to the
client, worker/scheduler config, and driver `WithClock` option in tests. Calling
`Advance` fires due timers and tickers without sleeping.

## Operations

Mount `admin.New(driver)` behind application authentication for the dashboard,
JSON job/queue actions, health/readiness probes, and Prometheus metrics. Workers
automatically rescue stale running jobs; tune `StuckJobTimeout` and
`RescueInterval` in `WorkerConfig`.
