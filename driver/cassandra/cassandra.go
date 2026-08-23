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
	"strings"
	"time"

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

// New creates a Driver wrapping the given *gocql.Session. The caller retains
// ownership of session and must close it after the driver is no longer in use.
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
func (d *Driver) Migrate(ctx context.Context) error {
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

		`CREATE TABLE IF NOT EXISTS goncordia_uniq_v2 (
			ukey   text PRIMARY KEY,
			job_id text
		)`,

		`CREATE TABLE IF NOT EXISTS goncordia_schedule_cursors (
			id        text PRIMARY KEY,
			cursor_at timestamp
		)`,

		// Leader election. Row TTL is set per insert to expire stale leaders.
		`CREATE TABLE IF NOT EXISTS goncordia_leaders (
			name       text PRIMARY KEY,
			worker_id  text,
			expires_at timestamp
		)`,

		`CREATE TABLE IF NOT EXISTS goncordia_schema_migrations (
			version    text PRIMARY KEY,
			applied_at timestamp
		)`,
	}
	for _, stmt := range stmts {
		if err := d.session.Query(stmt).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("cassandra migrate: %w", err)
		}
	}
	if err := d.ensureLeaseColumn(ctx); err != nil {
		return err
	}
	return d.backfillScheduledLookup(ctx)
}

const leaseColumnMigration = "20260823_job_lease_column"

func (d *Driver) ensureLeaseColumn(ctx context.Context) error {
	var appliedAt time.Time
	if err := d.session.Query(
		`SELECT applied_at FROM goncordia_schema_migrations WHERE version=?`, leaseColumnMigration,
	).WithContext(ctx).Scan(&appliedAt); err == nil {
		return nil
	} else if err != gocql.ErrNotFound {
		return fmt.Errorf("cassandra migrate: check lease column: %w", err)
	}
	if err := d.session.Query(`ALTER TABLE goncordia_jobs ADD lease_expires_at timestamp`).WithContext(ctx).Exec(); err != nil &&
		!strings.Contains(strings.ToLower(err.Error()), "already exists") {
		return fmt.Errorf("cassandra migrate: add lease column: %w", err)
	}
	if err := d.session.Query(
		`INSERT INTO goncordia_schema_migrations (version, applied_at) VALUES (?, ?)`,
		leaseColumnMigration, d.clk.Now(),
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("cassandra migrate: record lease column: %w", err)
	}
	return nil
}

const scheduledLookupMigration = "20260816_scheduled_lookup"

// backfillScheduledLookup makes jobs created by versions before v0.15.1
// visible to the run_at lookup. Concurrent runs are safe because lookup writes
// are idempotent; the marker is written only after a complete scan.
func (d *Driver) backfillScheduledLookup(ctx context.Context) error {
	var appliedAt time.Time
	if err := d.session.Query(
		`SELECT applied_at FROM goncordia_schema_migrations WHERE version=?`,
		scheduledLookupMigration,
	).WithContext(ctx).Scan(&appliedAt); err == nil {
		return nil
	} else if err != gocql.ErrNotFound {
		return fmt.Errorf("cassandra migrate: check scheduled lookup: %w", err)
	}

	iter := d.session.Query(
		`SELECT id, queue, state, run_at, priority FROM goncordia_jobs`,
	).WithContext(ctx).Iter()
	type lookupRow struct {
		id       string
		queue    string
		state    string
		runAt    time.Time
		priority int
	}
	var rows []lookupRow
	var row lookupRow
	for iter.Scan(&row.id, &row.queue, &row.state, &row.runAt, &row.priority) {
		if row.state == string(driver.JobStateAvailable) || row.state == string(driver.JobStateScheduled) {
			rows = append(rows, row)
		}
		row = lookupRow{}
	}
	if err := iter.Close(); err != nil {
		return fmt.Errorf("cassandra migrate: scan scheduled lookup: %w", err)
	}
	for _, row := range rows {
		if err := d.session.Query(
			`INSERT INTO goncordia_jobs_avail (queue, run_at, priority, id) VALUES (?, ?, ?, ?)`,
			row.queue, row.runAt, row.priority, row.id,
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("cassandra migrate: backfill scheduled lookup: %w", err)
		}
	}
	if err := d.session.Query(
		`INSERT INTO goncordia_schema_migrations (version, applied_at) VALUES (?, ?)`,
		scheduledLookupMigration, d.clk.Now(),
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("cassandra migrate: record scheduled lookup: %w", err)
	}
	return nil
}

func (d *Driver) Name() string { return "cassandra" }

func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		NativeTx:           false,
		ListenNotify:       false,
		ChangeStreams:      false,
		SkipLocked:         false,
		UniqueJobs:         true, // via LWT INSERT IF NOT EXISTS
		AdvisoryLocks:      false,
		LinearizableLeases: true,
		LinearizableCAS:    true,
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
