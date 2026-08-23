// Package pgxv5 provides a goncordia driver backed by PostgreSQL via pgx/v5.
//
// Usage:
//
//	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
//	d := pgxv5.New(pool)
//	client := pgxv5.NewClient(d, goncordia.ClientConfig{})  // no type parameter needed
//
//	// Transactional insert — atomic with your business logic:
//	tx, _ := pool.Begin(ctx)
//	_, _ = client.EnqueueTx(ctx, tx, SendEmailArgs{To: "..."}, nil)
//	tx.Commit(ctx)
package pgxv5

import (
	"context"
	"embed"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	goncordia "github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

// Driver implements driver.Driver[pgx.Tx] backed by a pgxpool.Pool.
type Driver struct {
	pool *pgxpool.Pool
	clk  clock.Clock
}

// Option configures the Driver.
type Option func(*Driver)

// WithClock injects a custom clock (for testing).
func WithClock(c clock.Clock) Option { return func(d *Driver) { d.clk = c } }

// New creates a Driver from an existing pgxpool.Pool. The caller retains
// ownership of pool and must close it after the driver is no longer in use.
// Call Migrate to create the schema before starting workers.
func New(pool *pgxpool.Pool, opts ...Option) *Driver {
	d := &Driver{pool: pool, clk: clock.Real{}}
	for _, o := range opts {
		o(d)
	}
	return d
}

// Migrate runs embedded SQL migrations against the database.
// Safe to call multiple times (uses IF NOT EXISTS / CREATE OR REPLACE).
func (d *Driver) Migrate(ctx context.Context) error {
	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtext('goncordia_schema_migrations'))`); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = conn.Exec(releaseCtx, `SELECT pg_advisory_unlock(hashtext('goncordia_schema_migrations'))`)
	}()

	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS goncordia_schema_migrations (
		version TEXT PRIMARY KEY,
		applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`); err != nil {
		return fmt.Errorf("create migration journal: %w", err)
	}
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		var applied bool
		if err := conn.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM goncordia_schema_migrations WHERE version=$1)`,
			e.Name(),
		).Scan(&applied); err != nil {
			return fmt.Errorf("check migration %s: %w", e.Name(), err)
		}
		if applied {
			continue
		}
		sql, err := migrationFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			return fmt.Errorf("read migration %s: %w", e.Name(), err)
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			return fmt.Errorf("begin migration %s: %w", e.Name(), err)
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("apply migration %s: %w", e.Name(), err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO goncordia_schema_migrations (version) VALUES ($1)`, e.Name()); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("record migration %s: %w", e.Name(), err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("commit migration %s: %w", e.Name(), err)
		}
	}
	return nil
}

func (d *Driver) Name() string { return "postgres" }

func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		NativeTx:           true,
		ListenNotify:       true,
		SkipLocked:         true,
		UniqueJobs:         true,
		AdvisoryLocks:      false,
		LinearizableLeases: true,
		LinearizableCAS:    true,
	}
}

func (d *Driver) Executor() driver.Executor {
	return &executor{pool: d.pool, clk: d.clk}
}

// UnwrapTx converts the user's pgx.Tx into an ExecutorTx.
func (d *Driver) UnwrapTx(tx pgx.Tx) driver.ExecutorTx {
	return &txExecutor{querier: tx, clk: d.clk}
}

func (d *Driver) Listener() driver.Listener {
	return &listener{pool: d.pool}
}

func (d *Driver) Close() error {
	return nil
}

// FetchParams is a convenience constructor for driver.FetchParams used in tests.
func FetchParams(queue string, limit int) driver.FetchParams {
	return driver.FetchParams{Queue: queue, Limit: limit}
}

// Client is a type alias so callers never write goncordia.Client[pgx.Tx].
type Client = goncordia.Client[pgx.Tx]

// WorkerPool is a type alias so callers never write goncordia.WorkerPool[pgx.Tx].
type WorkerPool = goncordia.WorkerPool[pgx.Tx]

// NewClient creates a Client bound to this pgxv5 driver.
func NewClient(d *Driver, cfg goncordia.ClientConfig) *Client {
	return goncordia.NewClient[pgx.Tx](d, cfg)
}

// NewWorkerPool creates a WorkerPool bound to this pgxv5 driver.
func NewWorkerPool(d *Driver, r *core.Registry, cfg goncordia.WorkerConfig) *WorkerPool {
	return goncordia.NewWorkerPool[pgx.Tx](d, r, cfg)
}
