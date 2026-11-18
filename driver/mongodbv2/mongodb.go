// Package mongodbv2 provides a goncordia driver backed by MongoDB through the
// official Go driver v2. It replaces driver/mongodb, which uses the deprecated
// mongo-driver v1, and stores jobs in the same collections and format, so
// processes using either driver can share one database during a migration.
//
// Requires MongoDB 4.0+ with a replica set (or sharded cluster).
// Standalone deployments do not support multi-document transactions and will
// cause New to return an error at startup.
//
// The transaction type is *mongo.Session: start a transaction on a session, write
// your documents through it, and pass the same session to EnqueueTx:
//
//	session, err := mongoClient.StartSession()
//	defer session.EndSession(ctx)
//	_, err = session.WithTransaction(ctx, func(ctx context.Context) (any, error) {
//	    if _, err := db.Collection("orders").InsertOne(ctx, order); err != nil {
//	        return nil, err
//	    }
//	    return client.EnqueueTx(ctx, session, SendConfirmationArgs{OrderID: order.ID}, nil)
//	})
package mongodbv2

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	goncordia "github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
)

const (
	jobsCollection    = "goncordia_jobs"
	queuesCollection  = "goncordia_queues"
	leadersCollection = "goncordia_leaders"
	cursorsCollection = "goncordia_schedule_cursors"
	resultsCollection = "goncordia_job_results"
)

// Driver implements driver.Driver[*mongo.Session].
type Driver struct {
	client *mongo.Client
	db     *mongo.Database
	clk    clock.Clock
}

// Option configures the Driver.
type Option func(*Driver)

// WithClock injects a custom clock (useful for tests).
func WithClock(c clock.Clock) Option { return func(d *Driver) { d.clk = c } }

// New creates a Driver connected to the given *mongo.Client. The caller retains
// ownership of client and must disconnect it after the driver is no longer in use.
// dbName is the database that will hold the goncordia collections.
// Returns an error if the server is not a replica set member (transactions require replica set).
func New(ctx context.Context, client *mongo.Client, dbName string, opts ...Option) (*Driver, error) {
	if !isReplicaSet(ctx, client) {
		return nil, errors.New(
			"mongodbv2: MongoDB must run as a replica set (transactions require it). " +
				"Local dev: use 'mongo:8.0' with --replSet rs0 or use testcontainers.WithReplicaSet",
		)
	}
	d := &Driver{client: client, db: client.Database(dbName), clk: clock.Real{}}
	for _, o := range opts {
		o(d)
	}
	return d, nil
}

// Migrate creates the required indexes. Safe to call multiple times.
func (d *Driver) Migrate(ctx context.Context) error {
	jobs := d.db.Collection(jobsCollection)

	jobIndexes := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "queue", Value: 1},
				{Key: "state", Value: 1},
				{Key: "priority", Value: -1},
				{Key: "run_at", Value: 1},
				{Key: "created_at", Value: 1},
				{Key: "_id", Value: 1},
			},
			Options: options.Index().SetName("goncordia_jobs_fetch_v2"),
		},
		{
			Keys: bson.D{
				{Key: "queue", Value: 1},
				{Key: "unique_key", Value: 1},
			},
			Options: options.Index().
				SetName("goncordia_jobs_unique_key").
				SetUnique(true).
				SetPartialFilterExpression(bson.M{
					"unique_key": bson.M{"$exists": true},
				}),
		},
		{
			Keys: bson.D{{Key: "unique_key", Value: 1}},
			Options: options.Index().
				SetName("goncordia_jobs_unique_key_v2").
				SetUnique(true).
				SetPartialFilterExpression(bson.M{
					"unique_key": bson.M{"$exists": true},
				}),
		},
		{
			Keys: bson.D{
				{Key: "queue", Value: 1},
				{Key: "state", Value: 1},
				{Key: "lease_expires_at", Value: 1},
			},
			Options: options.Index().SetName("goncordia_jobs_running_lease"),
		},
		{
			// Retention reads expired terminal jobs through this index.
			Keys:    bson.D{{Key: "state", Value: 1}, {Key: "finalized_at", Value: 1}},
			Options: options.Index().SetName("goncordia_jobs_finalized"),
		},
	}
	if _, err := jobs.Indexes().CreateMany(ctx, jobIndexes); err != nil {
		return fmt.Errorf("create jobs indexes: %w", err)
	}

	queues := d.db.Collection(queuesCollection)
	if _, err := queues.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "name", Value: 1}},
		Options: options.Index().SetName("goncordia_queues_name").SetUnique(true),
	}); err != nil {
		return fmt.Errorf("create queues index: %w", err)
	}

	results := d.db.Collection(resultsCollection)
	if _, err := results.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "updated_at", Value: 1}},
		Options: options.Index().SetName("goncordia_job_results_updated"),
	}); err != nil {
		return fmt.Errorf("create results index: %w", err)
	}

	leaders := d.db.Collection(leadersCollection)
	if _, err := leaders.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys:    bson.D{{Key: "expires_at", Value: 1}},
		Options: options.Index().SetName("goncordia_leaders_ttl").SetExpireAfterSeconds(0),
	}); err != nil {
		return fmt.Errorf("create leaders index: %w", err)
	}
	return nil
}

func (d *Driver) Name() string { return "mongodb" }

func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		NativeTx:            true,
		ChangeStreams:       true,
		UniqueJobs:          true,
		SkipLocked:          false, // uses findOneAndUpdate instead
		ListenNotify:        false,
		AdvisoryLocks:       false,
		LinearizableLeases:  true,
		LinearizableCAS:     true,
		LifecycleTimestamps: true,
		BoundedFetch:        true,
		StrictFetchOrdering: true,
	}
}

func (d *Driver) Executor() driver.Executor {
	return &executor{client: d.client, db: d.db, clk: d.clk}
}

// UnwrapTx wraps a session whose transaction the caller has already started.
// Commit/Rollback on the returned executor delegate to the session, which the
// caller keeps owning and ends.
func (d *Driver) UnwrapTx(session *mongo.Session) driver.ExecutorTx {
	return &txExecutor{db: d.db, session: session, clk: d.clk}
}

func (d *Driver) Listener() driver.Listener { return &listener{db: d.db} }
func (d *Driver) Close() error              { return nil }

// Client is a type alias so callers never write goncordia.Client[*mongo.Session].
type Client = goncordia.Client[*mongo.Session]

// WorkerPool is a type alias so callers never write goncordia.WorkerPool[*mongo.Session].
type WorkerPool = goncordia.WorkerPool[*mongo.Session]

// NewClient creates a Client bound to this MongoDB driver.
func NewClient(d *Driver, cfg goncordia.ClientConfig) *Client {
	return goncordia.NewClient[*mongo.Session](d, cfg)
}

// NewWorkerPool creates a WorkerPool bound to this MongoDB driver.
func NewWorkerPool(d *Driver, r *core.Registry, cfg goncordia.WorkerConfig) *WorkerPool {
	return goncordia.NewWorkerPool[*mongo.Session](d, r, cfg)
}

// FetchParams is a convenience constructor for driver.FetchParams used in tests.
func FetchParams(queue string, limit int) driver.FetchParams {
	return driver.FetchParams{Queue: queue, Limit: limit}
}

func isReplicaSet(ctx context.Context, client *mongo.Client) bool {
	res := client.Database("admin").RunCommand(ctx, bson.D{{Key: "isMaster", Value: 1}})
	var doc bson.M
	if err := res.Decode(&doc); err != nil {
		return false
	}
	_, ok := doc["setName"]
	return ok
}

// compile-time check
var _ driver.Driver[*mongo.Session] = (*Driver)(nil)
