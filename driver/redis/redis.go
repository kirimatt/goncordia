// Package redisdriver provides a goncordia driver backed by Redis.
//
// # Transaction guarantees
//
// Redis does not support transactions that can wrap business-store writes.
// EnqueueTx is rejected; enqueue after the business operation commits. Jobs are
// delivered at-least-once
// when combined with idempotent workers.
//
// For truly atomic "enqueue only if the business transaction commits" semantics,
// use a SQL (driver/stdlib, driver/pgxv5) or MongoDB (driver/mongodb) backend.
//
// # Usage
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	d := redisdriver.New(rdb)
//	d.Migrate(ctx)
//
//	client := redisdriver.NewClient(d, goncordia.ClientConfig{})
//	client.Enqueue(ctx, MyJob{...}, nil)
package redisdriver

import (
	"context"

	"github.com/redis/go-redis/v9"

	goncordia "github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver"
)

// NoTx is the transaction type for the Redis driver.
// Redis sessions have no rollback guarantee; EnqueueTx is rejected.
type NoTx struct{}

// Driver implements driver.Driver[NoTx] backed by Redis.
type Driver struct {
	rdb *redis.Client
	clk clock.Clock
}

// Option configures the Driver.
type Option func(*Driver)

// WithClock injects a custom clock (useful for tests).
func WithClock(c clock.Clock) Option { return func(d *Driver) { d.clk = c } }

// New creates a Driver wrapping the given *redis.Client. The caller retains
// ownership of rdb and must close it after the driver is no longer in use.
// Call Migrate to verify the connection before starting workers.
func New(rdb *redis.Client, opts ...Option) *Driver {
	d := &Driver{rdb: rdb, clk: clock.Real{}}
	for _, o := range opts {
		o(d)
	}
	return d
}

// Migrate pings Redis to verify connectivity. Safe to call multiple times.
func (d *Driver) Migrate(ctx context.Context) error {
	return d.rdb.Ping(ctx).Err()
}

func (d *Driver) Name() string { return "redis" }

func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		NativeTx:      false,
		ListenNotify:  true,
		ChangeStreams: false,
		SkipLocked:    false,
		UniqueJobs:    true,
		AdvisoryLocks: false,
	}
}

func (d *Driver) Executor() driver.Executor {
	return &executor{rdb: d.rdb, clk: d.clk}
}

// UnwrapTx returns a non-transactional executor — Redis has no real tx.
func (d *Driver) UnwrapTx(_ NoTx) driver.ExecutorTx {
	return &txExecutor{executor: executor{rdb: d.rdb, clk: d.clk}}
}

func (d *Driver) Listener() driver.Listener {
	return &listener{rdb: d.rdb}
}

func (d *Driver) Close() error { return nil }

// Client is a type alias so callers never write goncordia.Client[NoTx].
type Client = goncordia.Client[NoTx]

// WorkerPool is a type alias so callers never write goncordia.WorkerPool[NoTx].
type WorkerPool = goncordia.WorkerPool[NoTx]

// NewClient creates a Client bound to this Redis driver.
func NewClient(d *Driver, cfg goncordia.ClientConfig) *Client {
	return goncordia.NewClient[NoTx](d, cfg)
}

// NewWorkerPool creates a WorkerPool bound to this Redis driver.
func NewWorkerPool(d *Driver, r *core.Registry, cfg goncordia.WorkerConfig) *WorkerPool {
	return goncordia.NewWorkerPool[NoTx](d, r, cfg)
}

// FetchParams is a convenience constructor for driver.FetchParams used in tests.
func FetchParams(queue string, limit int) driver.FetchParams {
	return driver.FetchParams{Queue: queue, Limit: limit}
}

// compile-time check
var _ driver.Driver[NoTx] = (*Driver)(nil)
