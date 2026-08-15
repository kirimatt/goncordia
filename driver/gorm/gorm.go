// Package gormdriver provides a goncordia driver that accepts *gorm.DB transactions.
//
// It is a thin adapter over driver/stdlib: the underlying SQL execution is identical,
// only UnwrapTx differs — it extracts the *sql.Tx from gorm's transaction object.
//
// Usage:
//
//	db, _ := gorm.Open(postgres.Open(dsn), &gorm.Config{})
//	d, _ := gormdriver.New(db)
//	d.Migrate(ctx)
//
//	client := gormdriver.NewClient(d, goncordia.ClientConfig{})
//
//	// Transactional insert:
//	tx := db.Begin()
//	tx.Create(&order)
//	client.EnqueueTx(ctx, tx, SendConfirmationArgs{OrderID: order.ID}, nil)
//	tx.Commit()
package gormdriver

import (
	"context"
	"database/sql"
	"fmt"

	goncordia "github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver"
	stdlibdriver "github.com/kirimatt/goncordia/driver/stdlib"
	gormpkg "gorm.io/gorm"
)

// Driver implements driver.Driver[*gorm.DB].
// Internally it delegates to the stdlib driver using the same SQL logic.
type Driver struct {
	inner *stdlibdriver.Driver
}

// Option configures the Driver.
type Option func(*driverConfig)

type driverConfig struct {
	stdOpts []stdlibdriver.Option
}

// WithClock injects a custom clock (useful for tests).
func WithClock(c clock.Clock) Option {
	return func(cfg *driverConfig) {
		cfg.stdOpts = append(cfg.stdOpts, stdlibdriver.WithClock(c))
	}
}

// New creates a Driver from a *gorm.DB.
// The dialect is detected automatically from db.Dialector.Name().
// Call Migrate before starting workers.
func New(db *gormpkg.DB, opts ...Option) (*Driver, error) {
	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("gorm driver: get underlying sql.DB: %w", err)
	}
	dialect := detectDialect(db)

	cfg := &driverConfig{}
	for _, o := range opts {
		o(cfg)
	}

	inner := stdlibdriver.New(sqlDB, dialect, cfg.stdOpts...)
	return &Driver{inner: inner}, nil
}

// Migrate creates the goncordia schema in the database.
func (d *Driver) Migrate(ctx context.Context) error { return d.inner.Migrate(ctx) }

func (d *Driver) Name() string                      { return d.inner.Name() }
func (d *Driver) Capabilities() driver.Capabilities { return d.inner.Capabilities() }
func (d *Driver) Executor() driver.Executor         { return d.inner.Executor() }
func (d *Driver) Listener() driver.Listener         { return nil }
func (d *Driver) Close() error                      { return nil } // gorm owns the connection lifecycle

// UnwrapTx extracts the *sql.Tx from a gorm transaction object.
// tx must be a *gorm.DB returned by db.Begin() or passed inside db.Transaction().
func (d *Driver) UnwrapTx(tx *gormpkg.DB) driver.ExecutorTx {
	sqlTx, ok := tx.Statement.ConnPool.(*sql.Tx)
	if !ok {
		panic("gormdriver: UnwrapTx called with a non-transaction *gorm.DB; use db.Begin() first")
	}
	return d.inner.UnwrapTx(sqlTx)
}

// Client is a type alias so callers never write goncordia.Client[*gorm.DB].
type Client = goncordia.Client[*gormpkg.DB]

// WorkerPool is a type alias so callers never write goncordia.WorkerPool[*gorm.DB].
type WorkerPool = goncordia.WorkerPool[*gormpkg.DB]

// NewClient creates a Client bound to this gorm driver.
func NewClient(d *Driver, cfg goncordia.ClientConfig) *Client {
	return goncordia.NewClient[*gormpkg.DB](d, cfg)
}

// NewWorkerPool creates a WorkerPool bound to this gorm driver.
func NewWorkerPool(d *Driver, r *core.Registry, cfg goncordia.WorkerConfig) *WorkerPool {
	return goncordia.NewWorkerPool[*gormpkg.DB](d, r, cfg)
}

// FetchParams is a convenience constructor for driver.FetchParams used in tests.
func FetchParams(queue string, limit int) driver.FetchParams {
	return driver.FetchParams{Queue: queue, Limit: limit}
}

func detectDialect(db *gormpkg.DB) stdlibdriver.Dialect {
	switch db.Dialector.Name() {
	case "postgres", "pgx":
		return stdlibdriver.Postgres
	case "mysql":
		return stdlibdriver.MySQL
	default: // "sqlite", "sqlite3", anything else
		return stdlibdriver.SQLite
	}
}

// compile-time check
var _ driver.Driver[*gormpkg.DB] = (*Driver)(nil)
