# Migrating to Goncordia v1

V1 stabilizes the v0.19 runtime API and splits optional integrations into
independently versioned Go modules. Import paths do not change.

## 1. Upgrade the engine

```bash
go get github.com/kirimatt/goncordia@v1.0.0
```

The root module contains the client, worker, scheduler, admin API, clock,
driver contracts, memory driver, and conformance harness. It no longer pulls in
database SDKs, OpenTelemetry, GORM, Bun, or testcontainers.

## 2. Pin the modules you use

```bash
go get github.com/kirimatt/goncordia/driver/pgxv5@v1.0.0
go get github.com/kirimatt/goncordia/otel@v1.0.0
go get github.com/kirimatt/goncordia/gontest@v1.0.0
go mod tidy
```

Available production modules are:

| Module | Purpose |
|---|---|
| `driver/stdlib` | PostgreSQL, MySQL, and SQLite through `database/sql` |
| `driver/pgxv5` | Native pgx v5 PostgreSQL |
| `driver/mongodb` | MongoDB |
| `driver/redis` | Redis |
| `driver/cassandra` | Cassandra and ScyllaDB |
| `driver/clickhouse` | ClickHouse |
| `driver/dynamodb` | Amazon DynamoDB |
| `driver/firestore` | Cloud Firestore |
| `driver/gorm` | GORM adapter over stdlib |
| `driver/bun` | Bun adapter over stdlib |
| `otel` | OpenTelemetry instrumentation |
| `gontest` | Typed test helpers and manual clock |
| `bench` | Repository integration benchmarks; applications normally do not require it |

## 3. Migrate storage before workers

V1 uses the v0.19 schema. During a rolling deployment, run `Driver.Migrate`
with the new binary before starting v1 workers. SQL migration 007 and equivalent
backend setup add the fenced `started_at` lifecycle field and ready-order index.

## 4. Validate strict guarantees

Use `NewWorkerPoolChecked` and enable the guarantees your deployment requires:

```go
pool, err := goncordia.NewWorkerPoolChecked(d, registry, goncordia.WorkerConfig{
    Queues:                      []string{"default"},
    RequireLifecyclePersistence: true,
    RequireStrictOrdering:       true,
})
```

ClickHouse does not advertise linearizable leases/CAS or strict lifecycle
persistence. DynamoDB, Firestore, Cassandra, and Redis use bounded candidate
ordering and therefore do not advertise strict global priority ordering.

## 5. Verify the resolved graph

```bash
go list -m all | grep github.com/kirimatt/goncordia
go test ./...
```

The output should contain the root v1 module plus only the driver and integration
submodules selected by the application.
