// Package clickhousedriver provides a goncordia driver backed by ClickHouse.
//
// # Transaction guarantees
//
// ClickHouse does not support transactions. EnqueueTx is rejected; enqueue after
// the business operation commits.
//
// # Job claiming semantics
//
// ClickHouse uses a ReplacingMergeTree(version) engine. Each state transition
// (enqueue, claim, complete) inserts a new row with an incremented version number.
// Reads use SELECT … FINAL to retrieve the highest-version (current) row for each job.
//
// Under high concurrency multiple workers may attempt to claim the same job.
// The driver re-reads with FINAL after claiming and returns only jobs where the
// worker is the confirmed owner. Workers should nonetheless be idempotent because
// a brief window exists between the claim INSERT and the confirmation read.
//
// # Best for
//
// High-throughput workloads, analytics-adjacent pipelines, or environments where
// ClickHouse is already the primary store. For strict at-most-once guarantees use
// a SQL (driver/stdlib, driver/pgxv5) or MongoDB (driver/mongodb) backend.
//
// # Usage
//
//	conn, _ := clickhouse.Open(&clickhouse.Options{Addr: []string{"localhost:9000"}, ...})
//	d := clickhousedriver.New(conn)
//	d.Migrate(ctx)
//
//	client := clickhousedriver.NewClient(d, goncordia.ClientConfig{})
//	client.Enqueue(ctx, MyJob{...}, nil)
package clickhousedriver

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	goncordia "github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	gdriver "github.com/kirimatt/goncordia/driver"
)

// NoTx is the transaction type for the ClickHouse driver.
// ClickHouse has no transactions; EnqueueTx is rejected.
type NoTx struct{}

// Driver implements gdriver.Driver[NoTx] backed by ClickHouse.
type Driver struct {
	conn driver.Conn
	clk  clock.Clock
}

// Option configures the Driver.
type Option func(*Driver)

// WithClock injects a custom clock (useful for tests).
func WithClock(c clock.Clock) Option { return func(d *Driver) { d.clk = c } }

// New creates a Driver wrapping the given clickhouse driver.Conn.
// Call Migrate to create the schema before starting workers.
func New(conn driver.Conn, opts ...Option) *Driver {
	d := &Driver{conn: conn, clk: clock.Real{}}
	for _, o := range opts {
		o(d)
	}
	return d
}

// Migrate creates the required tables. Safe to call multiple times.
func (d *Driver) Migrate(ctx context.Context) error {
	stmts := []string{
		// Jobs table. ReplacingMergeTree deduplicates by (queue, id), keeping highest version.
		// Reads use SELECT … FINAL to materialise the merge at query time.
		`CREATE TABLE IF NOT EXISTS goncordia_jobs (
			id           String,
			queue        LowCardinality(String),
			kind         LowCardinality(String),
			args         String,
			state        LowCardinality(String),
			priority     Int32,
			run_at       DateTime64(3, 'UTC'),
			created_at   DateTime64(3, 'UTC'),
			attempted_at Nullable(DateTime64(3, 'UTC')),
			finalized_at Nullable(DateTime64(3, 'UTC')),
			attempt_num  Int32,
			max_retry    Int32,
			timeout_ms   Int64,
			unique_key   String,
			worker_id    String,
			tags         Array(String),
			errors_json  String,
			version      Int64,
			pipeline_id  String
		) ENGINE = ReplacingMergeTree(version)
		ORDER BY (queue, id)
		SETTINGS index_granularity = 8192`,

		// Queue metadata.
		`CREATE TABLE IF NOT EXISTS goncordia_queues (
			name       LowCardinality(String),
			paused     UInt8,
			created_at DateTime64(3, 'UTC'),
			updated_at DateTime64(3, 'UTC'),
			version    Int64
		) ENGINE = ReplacingMergeTree(version)
		ORDER BY name`,

		// Leader election soft-lock.
		`CREATE TABLE IF NOT EXISTS goncordia_leaders (
			name       String,
			worker_id  String,
			expires_at DateTime64(3, 'UTC'),
			version    Int64
		) ENGINE = ReplacingMergeTree(version)
		ORDER BY name`,
	}
	for _, stmt := range stmts {
		if err := d.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("clickhouse migrate: %w", err)
		}
	}
	return nil
}

func (d *Driver) Name() string { return "clickhouse" }

func (d *Driver) Capabilities() gdriver.Capabilities {
	return gdriver.Capabilities{
		NativeTx:      false,
		ListenNotify:  false,
		ChangeStreams: false,
		SkipLocked:    false,
		UniqueJobs:    true, // soft deduplication via ReplacingMergeTree
		AdvisoryLocks: false,
	}
}

func (d *Driver) Executor() gdriver.Executor {
	return &executor{conn: d.conn, clk: d.clk}
}

// UnwrapTx exists to satisfy Driver; Client rejects EnqueueTx before calling it.
func (d *Driver) UnwrapTx(_ NoTx) gdriver.ExecutorTx {
	return &txExecutor{executor: executor{conn: d.conn, clk: d.clk}}
}

// Listener returns nil — ClickHouse driver uses polling.
func (d *Driver) Listener() gdriver.Listener { return nil }

func (d *Driver) Close() error { return d.conn.Close() }

// Client is a type alias so callers never write goncordia.Client[NoTx].
type Client = goncordia.Client[NoTx]

// WorkerPool is a type alias so callers never write goncordia.WorkerPool[NoTx].
type WorkerPool = goncordia.WorkerPool[NoTx]

// NewClient creates a Client bound to this ClickHouse driver.
func NewClient(d *Driver, cfg goncordia.ClientConfig) *Client {
	return goncordia.NewClient[NoTx](d, cfg)
}

// NewWorkerPool creates a WorkerPool bound to this ClickHouse driver.
func NewWorkerPool(d *Driver, r *core.Registry, cfg goncordia.WorkerConfig) *WorkerPool {
	return goncordia.NewWorkerPool[NoTx](d, r, cfg)
}

// FetchParams is a convenience constructor for driver.FetchParams used in tests.
func FetchParams(queue string, limit int) gdriver.FetchParams {
	return gdriver.FetchParams{Queue: queue, Limit: limit}
}

// compile-time check
var _ gdriver.Driver[NoTx] = (*Driver)(nil)
