# Migrating to Goncordia v2

V2 removes the compatibility layer that v1 kept for v0 callers, folds result
storage into every driver, and merges the two worker observer interfaces.
Stored jobs are compatible: run `Migrate` once and existing rows keep working.

## 1. Update import paths

Every module gains a `/v2` suffix. The root module puts it after the module
root; nested modules put it at the end of their own path:

| v1 | v2 |
|---|---|
| `github.com/kirimatt/goncordia` | `github.com/kirimatt/goncordia/v2` |
| `github.com/kirimatt/goncordia/core` | `github.com/kirimatt/goncordia/v2/core` |
| `github.com/kirimatt/goncordia/driver/memory` | `github.com/kirimatt/goncordia/v2/driver/memory` |
| `github.com/kirimatt/goncordia/driver/pgxv5` | `github.com/kirimatt/goncordia/driver/pgxv5/v2` |
| `github.com/kirimatt/goncordia/otel` | `github.com/kirimatt/goncordia/otel/v2` |
| `github.com/kirimatt/goncordia/gontest` | `github.com/kirimatt/goncordia/gontest/v2` |

The same rule applies to `driver/stdlib`, `driver/mongodb`, `driver/redis`,
`driver/cassandra`, `driver/clickhouse`, `driver/dynamodb`,
`driver/firestore`, `driver/gorm`, and `driver/bun`.

```bash
go get github.com/kirimatt/goncordia/v2@v2.2.0
go get github.com/kirimatt/goncordia/driver/pgxv5/v2@v2.2.0
go mod tidy
```

## 2. Run Migrate

Every driver now stores future results. `Migrate` adds `goncordia_job_results`
(SQL migration 008, Cassandra, ClickHouse, DynamoDB). MongoDB, Firestore, and
Redis need no schema change.

## 3. Replace removed APIs

| Removed | Use instead |
|---|---|
| `WorkerPool.Stop()` | `WorkerPool.Shutdown(ctx)`, which reports whether the drain finished |
| `ClientConfig.Now` | `ClientConfig.Clock` |
| `core.WorkerOpts.Queue` (had no effect) | `InsertOpts.Queue` or `ClientConfig.DefaultQueue` |
| `QueuePolicy.RateLimit` / `RatePeriod` | `QueuePolicy.RateLimits: []QueueRateLimit{{Limit: n, Period: p, Burst: n}}` |
| `clock.Mock`, `clock.NewMock` | `clock.Manual`, `clock.NewManual` |
| `internal/clock` | `clock` |
| `gontest.Jobs` (dropped decode errors), `gontest.JobsE` | `gontest.Jobs`, which now returns `([]*core.Job[T], error)` |
| `WorkerLifecycleObserver` | `WorkerObserver`, which now has all six methods |

## 4. Observers

`WorkerObserver` now includes `JobStarted`, `JobFinished`, and
`JobRateLimited`. Embed `goncordia.NopWorkerObserver` to implement only the
events you need, and combine observers with `goncordia.WorkerObservers`:

```go
type metrics struct {
    goncordia.NopWorkerObserver
}

func (metrics) JobFinished(ctx context.Context, e goncordia.JobFinishedEvent) { /* ... */ }
```

## 5. Custom drivers and clocks

- `driver.Executor` (and `ExecutorTx`) now embeds `driver.JobResultStore`:
  implement `JobResultPut`, `JobResultGet` (return `driver.ErrNotFound` when
  absent), and `JobResultDelete`, and delete a job's result in `JobDelete`.
  `drivertest.RunResults` checks the contract.
- `clock.Clock` now requires `NewTimer(time.Duration) clock.Timer`.

## 6. Unchanged

Database schemas other than the new results table, job payloads, metric and
span names, and the OpenTelemetry instrumentation scope
(`github.com/kirimatt/goncordia`) stay the same, so dashboards and alerts keep
working.
