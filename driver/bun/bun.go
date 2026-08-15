// Package bundriver provides a goncordia driver that accepts *bun.Tx transactions.
//
// It is a thin adapter over driver/stdlib: bun.DB embeds *sql.DB and bun.Tx embeds
// *sql.Tx, so no reflection or internal field access is needed.
//
// Usage:
//
//	sqlDB, _ := sql.Open("pgx", os.Getenv("DATABASE_URL"))
//	db := bun.NewDB(sqlDB, pgdialect.New())
//
//	d := bundriver.New(db)
//	d.Migrate(ctx)
//
//	client := bundriver.NewClient(d, goncordia.ClientConfig{})
//
//	// Transactional insert:
//	bunTx, _ := db.BeginTx(ctx, nil)
//	db.NewInsert().Model(&order).Exec(ctx)  // or bunTx.NewInsert()...
//	client.EnqueueTx(ctx, bunTx, SendConfirmationArgs{OrderID: order.ID}, nil)
//	bunTx.Commit()
package bundriver

import (
	"context"

	"github.com/uptrace/bun"
	bundialect "github.com/uptrace/bun/dialect"

	goncordia "github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver"
	stdlibdriver "github.com/kirimatt/goncordia/driver/stdlib"
)

// Driver implements driver.Driver[bun.Tx].
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

// New creates a Driver from a *bun.DB.
// The dialect is detected automatically from db.Dialect().Name().
// Call Migrate before starting workers.
func New(db *bun.DB, opts ...Option) *Driver {
	cfg := &driverConfig{}
	for _, o := range opts {
		o(cfg)
	}
	inner := stdlibdriver.New(db.DB, detectDialect(db), cfg.stdOpts...)
	return &Driver{inner: inner}
}

// Migrate creates the goncordia schema in the database.
func (d *Driver) Migrate(ctx context.Context) error { return d.inner.Migrate(ctx) }

func (d *Driver) Name() string                      { return d.inner.Name() }
func (d *Driver) Capabilities() driver.Capabilities { return d.inner.Capabilities() }
func (d *Driver) Executor() driver.Executor         { return d.inner.Executor() }
func (d *Driver) Listener() driver.Listener         { return nil }
func (d *Driver) Close() error                      { return nil } // bun owns the connection lifecycle

// UnwrapTx extracts the *sql.Tx embedded in bun.Tx.
// tx must be obtained from db.BeginTx or db.RunInTx.
func (d *Driver) UnwrapTx(tx bun.Tx) driver.ExecutorTx {
	return d.inner.UnwrapTx(tx.Tx)
}

// Client is a type alias so callers never write goncordia.Client[bun.Tx].
type Client = goncordia.Client[bun.Tx]

// WorkerPool is a type alias so callers never write goncordia.WorkerPool[bun.Tx].
type WorkerPool = goncordia.WorkerPool[bun.Tx]

// NewClient creates a Client bound to this bun driver.
func NewClient(d *Driver, cfg goncordia.ClientConfig) *Client {
	return goncordia.NewClient[bun.Tx](d, cfg)
}

// NewWorkerPool creates a WorkerPool bound to this bun driver.
func NewWorkerPool(d *Driver, r *core.Registry, cfg goncordia.WorkerConfig) *WorkerPool {
	return goncordia.NewWorkerPool[bun.Tx](d, r, cfg)
}

// FetchParams is a convenience constructor for driver.FetchParams used in tests.
func FetchParams(queue string, limit int) driver.FetchParams {
	return driver.FetchParams{Queue: queue, Limit: limit}
}

func detectDialect(db *bun.DB) stdlibdriver.Dialect {
	switch db.Dialect().Name() {
	case bundialect.PG:
		return stdlibdriver.Postgres
	case bundialect.MySQL:
		return stdlibdriver.MySQL
	default: // bundialect.SQLite and anything else
		return stdlibdriver.SQLite
	}
}

// compile-time check
var _ driver.Driver[bun.Tx] = (*Driver)(nil)
