// Package cassandradriver provides a goncordia driver backed by Apache Cassandra.
//
// # Transaction guarantees
//
// Cassandra does not support multi-statement transactions with rollback semantics.
// EnqueueTx is rejected; enqueue after the business operation commits. Jobs are
// delivered at-least-once when combined with idempotent workers.
//
// Lightweight transactions (IF NOT EXISTS / IF condition) are used internally for
// atomic job claiming and unique-key deduplication.
//
// # Requirements
//
// Cassandra 3.11+ or compatible (ScyllaDB 4.0+, DataStax Enterprise 6.0+).
// A keyspace must already exist; pass its name to New.
//
// # Usage
//
//	cluster := gocql.NewCluster("localhost")
//	cluster.Keyspace = "mykeyspace"
//	session, _ := cluster.CreateSession()
//	defer session.Close()
//
//	d := cassandradriver.New(session)
//	d.Migrate(ctx)
//
//	client := cassandradriver.NewClient(d, goncordia.ClientConfig{})
//	client.Enqueue(ctx, MyJob{...}, nil)
package cassandradriver

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"

	goncordia "github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver"
)

// NoTx is the transaction type for the Cassandra driver.
// Cassandra sessions have no rollback guarantee; EnqueueTx is rejected.
type NoTx struct{}

// Driver implements driver.Driver[NoTx] backed by Cassandra.
type Driver struct {
	session *gocql.Session
	clk     clock.Clock
}

// Option configures the Driver.
type Option func(*Driver)

// WithClock injects a custom clock (useful for tests).
func WithClock(c clock.Clock) Option { return func(d *Driver) { d.clk = c } }

// New creates a Driver wrapping the given *gocql.Session.
// The session's keyspace must already be set (cluster.Keyspace = "...").
// Call Migrate to create the schema before starting workers.
func New(session *gocql.Session, opts ...Option) *Driver {
	d := &Driver{session: session, clk: clock.Real{}}
	for _, o := range opts {
		o(d)
	}
	return d
}

// Migrate creates the required tables and indexes. Safe to call multiple times.
func (d *Driver) Migrate(_ context.Context) error {
	stmts := []string{
		// Main job store — queried by id.
		`CREATE TABLE IF NOT EXISTS goncordia_jobs (
			id            text,
			queue         text,
			kind          text,
			args          blob,
			state         text,
			priority      int,
			run_at        timestamp,
			created_at    timestamp,
			attempted_at  timestamp,
			finalized_at  timestamp,
			attempt_num   int,
			max_retry     int,
			timeout_ms    bigint,
			unique_key    text,
			worker_id     text,
			tags          list<text>,
			errors_json   text,
			version       bigint,
			pipeline_id   text,
			PRIMARY KEY (id)
		)`,

		// Available-job lookup table. Partitioned by queue; clustered by run_at/priority
		// so workers can claim the oldest highest-priority jobs first.
		`CREATE TABLE IF NOT EXISTS goncordia_jobs_avail (
			queue       text,
			run_at      timestamp,
			priority    int,
			id          text,
			PRIMARY KEY ((queue), run_at, priority, id)
		) WITH CLUSTERING ORDER BY (run_at ASC, priority DESC, id ASC)`,

		// Running-job lookup used to recover claims abandoned by crashed workers.
		`CREATE TABLE IF NOT EXISTS goncordia_jobs_running (
			queue        text,
			attempted_at timestamp,
			id           text,
			PRIMARY KEY ((queue), attempted_at, id)
		) WITH CLUSTERING ORDER BY (attempted_at ASC, id ASC)`,

		// Queue metadata (paused flag, timestamps).
		`CREATE TABLE IF NOT EXISTS goncordia_queues (
			name        text PRIMARY KEY,
			paused      boolean,
			created_at  timestamp,
			updated_at  timestamp
		)`,

		// Unique-key deduplication. INSERT IF NOT EXISTS used for atomicity.
		`CREATE TABLE IF NOT EXISTS goncordia_uniq (
			queue   text,
			ukey    text,
			job_id  text,
			PRIMARY KEY ((queue, ukey))
		)`,

		// Leader election. Row TTL is set per insert to expire stale leaders.
		`CREATE TABLE IF NOT EXISTS goncordia_leaders (
			name       text PRIMARY KEY,
			worker_id  text,
			expires_at timestamp
		)`,
	}
	for _, stmt := range stmts {
		if err := d.session.Query(stmt).Exec(); err != nil {
			return fmt.Errorf("cassandra migrate: %w", err)
		}
	}
	return nil
}

func (d *Driver) Name() string { return "cassandra" }

func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		NativeTx:      false,
		ListenNotify:  false,
		ChangeStreams: false,
		SkipLocked:    false,
		UniqueJobs:    true, // via LWT INSERT IF NOT EXISTS
		AdvisoryLocks: false,
	}
}

func (d *Driver) Executor() driver.Executor {
	return &executor{session: d.session, clk: d.clk}
}

// UnwrapTx returns a non-transactional executor — Cassandra has no real tx.
func (d *Driver) UnwrapTx(_ NoTx) driver.ExecutorTx {
	return &txExecutor{executor: executor{session: d.session, clk: d.clk}}
}

// Listener returns nil — Cassandra driver uses polling.
func (d *Driver) Listener() driver.Listener { return nil }

func (d *Driver) Close() error {
	d.session.Close()
	return nil
}

// Client is a type alias so callers never write goncordia.Client[NoTx].
type Client = goncordia.Client[NoTx]

// WorkerPool is a type alias so callers never write goncordia.WorkerPool[NoTx].
type WorkerPool = goncordia.WorkerPool[NoTx]

// NewClient creates a Client bound to this Cassandra driver.
func NewClient(d *Driver, cfg goncordia.ClientConfig) *Client {
	return goncordia.NewClient[NoTx](d, cfg)
}

// NewWorkerPool creates a WorkerPool bound to this Cassandra driver.
func NewWorkerPool(d *Driver, r *core.Registry, cfg goncordia.WorkerConfig) *WorkerPool {
	return goncordia.NewWorkerPool[NoTx](d, r, cfg)
}

// FetchParams is a convenience constructor for driver.FetchParams used in tests.
func FetchParams(queue string, limit int) driver.FetchParams {
	return driver.FetchParams{Queue: queue, Limit: limit}
}

// compile-time check
var _ driver.Driver[NoTx] = (*Driver)(nil)
