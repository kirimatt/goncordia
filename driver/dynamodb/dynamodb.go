// Package dynamodbdriver provides a goncordia driver backed by Amazon DynamoDB.
//
// # Transaction guarantees
//
// DynamoDB does not support transactions that can wrap external business
// operations. EnqueueTx is rejected; enqueue after the business operation commits.
// Jobs are delivered at-least-once when combined with idempotent workers.
//
// Conditional writes (ConditionExpression) are used internally for atomic job
// claiming and unique-key deduplication.
//
// # Requirements
//
// Amazon DynamoDB or DynamoDB Local. Tables are created automatically by Migrate.
//
// # Usage
//
//	cfg, _ := config.LoadDefaultConfig(ctx)
//	svc := dynamodb.NewFromConfig(cfg)
//
//	d := dynamodbdriver.New(svc)
//	d.Migrate(ctx)
//
//	client := dynamodbdriver.NewClient(d, goncordia.ClientConfig{})
//	client.Enqueue(ctx, MyJob{...}, nil)
package dynamodbdriver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	goncordia "github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver"
)

const (
	tableJobs     = "goncordia_jobs"
	tableUniq     = "goncordia_uniq"
	tableQueues   = "goncordia_queues"
	tableLeaders  = "goncordia_leaders"
	tableCursors  = "goncordia_schedule_cursors"
	gsiQueueState = "gsi_queue_state"
)

// NoTx is the transaction type for the DynamoDB driver.
// DynamoDB's TransactWriteItems covers only DynamoDB operations and cannot
// span external business transactions; EnqueueTx is rejected.
type NoTx struct{}

// Driver implements driver.Driver[NoTx] backed by Amazon DynamoDB.
type Driver struct {
	svc *dynamodb.Client
	clk clock.Clock
}

// Option configures the Driver.
type Option func(*Driver)

// WithClock injects a custom clock (useful for tests).
func WithClock(c clock.Clock) Option { return func(d *Driver) { d.clk = c } }

// New creates a Driver wrapping the given *dynamodb.Client.
// Call Migrate to create the required tables before starting workers.
func New(svc *dynamodb.Client, opts ...Option) *Driver {
	d := &Driver{svc: svc, clk: clock.Real{}}
	for _, o := range opts {
		o(d)
	}
	return d
}

// Migrate creates the required DynamoDB tables and indexes. Safe to call multiple times.
func (d *Driver) Migrate(ctx context.Context) error {
	tables := []*dynamodb.CreateTableInput{
		{
			TableName:   aws.String(tableJobs),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("queue_state"), AttributeType: types.ScalarAttributeTypeS},
				{AttributeName: aws.String("run_at"), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
			},
			GlobalSecondaryIndexes: []types.GlobalSecondaryIndex{
				{
					IndexName: aws.String(gsiQueueState),
					KeySchema: []types.KeySchemaElement{
						{AttributeName: aws.String("queue_state"), KeyType: types.KeyTypeHash},
						{AttributeName: aws.String("run_at"), KeyType: types.KeyTypeRange},
					},
					Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
				},
			},
		},
		{
			TableName:   aws.String(tableUniq),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("pk"), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("pk"), KeyType: types.KeyTypeHash},
			},
		},
		{
			TableName:   aws.String(tableQueues),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("name"), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("name"), KeyType: types.KeyTypeHash},
			},
		},
		{
			TableName:   aws.String(tableLeaders),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("name"), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("name"), KeyType: types.KeyTypeHash},
			},
		},
		{
			TableName:   aws.String(tableCursors),
			BillingMode: types.BillingModePayPerRequest,
			AttributeDefinitions: []types.AttributeDefinition{
				{AttributeName: aws.String("id"), AttributeType: types.ScalarAttributeTypeS},
			},
			KeySchema: []types.KeySchemaElement{
				{AttributeName: aws.String("id"), KeyType: types.KeyTypeHash},
			},
		},
	}

	waiter := dynamodb.NewTableExistsWaiter(d.svc)
	for _, input := range tables {
		if _, err := d.svc.CreateTable(ctx, input); err != nil {
			var riu *types.ResourceInUseException
			if !errors.As(err, &riu) {
				return fmt.Errorf("dynamodb migrate: create %s: %w", *input.TableName, err)
			}
		}
		if err := waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: input.TableName}, 30*time.Second); err != nil {
			return fmt.Errorf("dynamodb migrate: wait %s: %w", *input.TableName, err)
		}
	}
	return nil
}

func (d *Driver) Name() string { return "dynamodb" }

func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		NativeTx:            false,
		ListenNotify:        false,
		ChangeStreams:       false,
		SkipLocked:          false,
		UniqueJobs:          true, // via PutItem ConditionExpression attribute_not_exists
		AdvisoryLocks:       false,
		LinearizableLeases:  true,
		LinearizableCAS:     true,
		LifecycleTimestamps: true,
		BoundedFetch:        true,
		StrictFetchOrdering: false,
	}
}

func (d *Driver) Executor() driver.Executor {
	return &executor{svc: d.svc, clk: d.clk}
}

// UnwrapTx returns a non-transactional executor — DynamoDB driver has no real tx.
func (d *Driver) UnwrapTx(_ NoTx) driver.ExecutorTx {
	return &txExecutor{executor: executor{svc: d.svc, clk: d.clk}}
}

// Listener returns nil — DynamoDB driver uses polling.
func (d *Driver) Listener() driver.Listener { return nil }

func (d *Driver) Close() error { return nil }

// Client is a type alias so callers never write goncordia.Client[NoTx].
type Client = goncordia.Client[NoTx]

// WorkerPool is a type alias so callers never write goncordia.WorkerPool[NoTx].
type WorkerPool = goncordia.WorkerPool[NoTx]

// NewClient creates a Client bound to this DynamoDB driver.
func NewClient(d *Driver, cfg goncordia.ClientConfig) *Client {
	return goncordia.NewClient[NoTx](d, cfg)
}

// NewWorkerPool creates a WorkerPool bound to this DynamoDB driver.
func NewWorkerPool(d *Driver, r *core.Registry, cfg goncordia.WorkerConfig) *WorkerPool {
	return goncordia.NewWorkerPool[NoTx](d, r, cfg)
}

// compile-time check
var _ driver.Driver[NoTx] = (*Driver)(nil)
