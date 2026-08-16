// Package firestoredriver provides a goncordia driver backed by Google Cloud Firestore.
//
// # Transaction guarantees
//
// Firestore supports ACID multi-document transactions via RunTransaction. Pass the
// *firestore.Transaction from your callback to EnqueueTx to enqueue a job atomically
// with your business writes:
//
//	err = fsClient.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
//	    tx.Create(orders.Doc(id), orderData)
//	    _, err := client.EnqueueTx(ctx, tx, SendConfirmationArgs{OrderID: id}, nil)
//	    return err
//	})
//
// # Required Firestore composite index
//
// The job fetch query uses three fields. Create a composite index in the Firebase console
// or via firestore.indexes.json before deploying to production:
//
//	collection: goncordia_jobs
//	fields:     queue (ASC), state (ASC), run_at (ASC)
//
// The Firestore emulator does not enforce index requirements, so tests pass without it.
//
// # Usage
//
//	client, _ := firestore.NewClient(ctx, "my-gcp-project")
//	d := firestoredriver.New(client)
//	c := firestoredriver.NewClient(d, goncordia.ClientConfig{})
//	c.Enqueue(ctx, MyJob{...}, nil)
package firestoredriver

import (
	"context"

	"cloud.google.com/go/firestore"

	goncordia "github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver"
)

const (
	colJobs    = "goncordia_jobs"
	colUniq    = "goncordia_uniq"
	colQueues  = "goncordia_queues"
	colLeaders = "goncordia_leaders"
	colCursors = "goncordia_schedule_cursors"
)

// Driver implements driver.Driver[*firestore.Transaction] backed by Cloud Firestore.
type Driver struct {
	client *firestore.Client
	clk    clock.Clock
}

// Option configures the Driver.
type Option func(*Driver)

// WithClock injects a custom clock (useful for tests).
func WithClock(c clock.Clock) Option { return func(d *Driver) { d.clk = c } }

// New creates a Driver wrapping the given *firestore.Client.
func New(client *firestore.Client, opts ...Option) *Driver {
	d := &Driver{client: client, clk: clock.Real{}}
	for _, o := range opts {
		o(d)
	}
	return d
}

// Migrate is a no-op for Firestore — collections are created automatically on first write.
// For production, create a composite index on goncordia_jobs:
//
//	fields: queue (ASC), state (ASC), run_at (ASC)
func (d *Driver) Migrate(_ context.Context) error { return nil }

func (d *Driver) Name() string { return "firestore" }

func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		NativeTx:      true, // *firestore.Transaction is an ACID transaction
		UniqueJobs:    true, // atomic Create with conflict detection
		ListenNotify:  false,
		ChangeStreams: false,
		SkipLocked:    false,
		AdvisoryLocks: false,
	}
}

func (d *Driver) Executor() driver.Executor {
	return &executor{client: d.client, clk: d.clk}
}

// UnwrapTx wraps the caller's *firestore.Transaction as an ExecutorTx.
// The transaction must have been started by the caller's RunTransaction callback.
func (d *Driver) UnwrapTx(tx *firestore.Transaction) driver.ExecutorTx {
	return &txExecutor{client: d.client, tx: tx, clk: d.clk}
}

// Listener returns nil — Firestore driver uses polling.
func (d *Driver) Listener() driver.Listener { return nil }

func (d *Driver) Close() error { return d.client.Close() }

// Client is a type alias so callers never write goncordia.Client[*firestore.Transaction].
type Client = goncordia.Client[*firestore.Transaction]

// WorkerPool is a type alias so callers never write goncordia.WorkerPool[*firestore.Transaction].
type WorkerPool = goncordia.WorkerPool[*firestore.Transaction]

// NewClient creates a Client bound to this Firestore driver.
func NewClient(d *Driver, cfg goncordia.ClientConfig) *Client {
	return goncordia.NewClient[*firestore.Transaction](d, cfg)
}

// NewWorkerPool creates a WorkerPool bound to this Firestore driver.
func NewWorkerPool(d *Driver, r *core.Registry, cfg goncordia.WorkerConfig) *WorkerPool {
	return goncordia.NewWorkerPool[*firestore.Transaction](d, r, cfg)
}

// compile-time check
var _ driver.Driver[*firestore.Transaction] = (*Driver)(nil)
