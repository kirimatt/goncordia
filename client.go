// Package goncordia provides a transactional job queue engine for Go.
// It supports multiple storage backends (Postgres, MySQL, SQLite, MongoDB, Redis, in-memory)
// through a driver interface parameterized by the native transaction type of each backend.
//
// Transactional usage (shared transaction with business logic):
//
//	tx, _ := pool.Begin(ctx)
//	_, _ = queries.CreateOrder(ctx, tx, orderParams)
//	_, _ = client.EnqueueTx(ctx, tx, SendConfirmationEmailArgs{OrderID: id}, nil)
//	tx.Commit(ctx) // both operations are atomic
//
// Non-transactional usage (at-least-once semantics):
//
//	client.Enqueue(ctx, SendConfirmationEmailArgs{OrderID: id}, nil)
package goncordia

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver"
)

// Client enqueues jobs into the job queue.
// TTx is the transaction type of the chosen backend driver
// (e.g. *pgx.Tx for pgxv5, *sql.Tx for stdlib, mongo.SessionContext for mongodb).
type Client[TTx any] struct {
	driver driver.Driver[TTx]
	config ClientConfig
}

// ClientConfig controls optional Client behavior.
type ClientConfig struct {
	// DefaultQueue is used when InsertOpts.Queue is empty. Default: "default".
	DefaultQueue string
	// Clock controls period-based unique-key windows. Default: clock.Real{}.
	Clock clock.Clock
	// Now overrides the wall clock used for period-based unique keys.
	// Deprecated: use Clock.
	Now func() time.Time
	// Observer instruments enqueue storage operations.
	Observer ClientObserver
}

// InsertRequest allows each item in a batch to have independent options.
type InsertRequest struct {
	Args core.JobArgs
	Opts *core.InsertOpts
}

// NewClient creates a Client backed by the given driver.
func NewClient[TTx any](d driver.Driver[TTx], cfg ClientConfig) *Client[TTx] {
	if cfg.DefaultQueue == "" {
		cfg.DefaultQueue = "default"
	}
	if cfg.Clock == nil && cfg.Now == nil {
		cfg.Clock = clock.Real{}
	}
	return &Client[TTx]{driver: d, config: cfg}
}

// Enqueue inserts a single job without a transaction (at-least-once semantics).
// Safe to call for all backends; for SQL/MongoDB backends prefer EnqueueTx for atomicity.
func (c *Client[TTx]) Enqueue(ctx context.Context, args core.JobArgs, opts *core.InsertOpts) (*driver.JobInsertResult, error) {
	params, err := c.buildInsertParams(args, opts)
	if err != nil {
		return nil, err
	}
	ctx, finish := c.startEnqueue(ctx, []driver.JobInsertParams{params}, false)
	results, err := c.driver.Executor().JobInsertMany(ctx, []driver.JobInsertParams{params})
	if err != nil {
		finish(results, err)
		return nil, err
	}
	if len(results) != 1 {
		err = fmt.Errorf("driver %q returned %d results for one insert", c.driver.Name(), len(results))
		finish(results, err)
		return nil, err
	}
	finish(results, nil)
	return &results[0], nil
}

// EnqueueTx inserts a job within an existing transaction.
// The job becomes visible to workers only when tx is committed.
// Only available on backends with Capabilities.NativeTx == true.
func (c *Client[TTx]) EnqueueTx(ctx context.Context, tx TTx, args core.JobArgs, opts *core.InsertOpts) (*driver.JobInsertResult, error) {
	if !c.driver.Capabilities().NativeTx {
		return nil, fmt.Errorf("%w: driver %q does not support transactional inserts", driver.ErrUnsupported, c.driver.Name())
	}
	params, err := c.buildInsertParams(args, opts)
	if err != nil {
		return nil, err
	}
	etx := c.driver.UnwrapTx(tx)
	ctx, finish := c.startEnqueue(ctx, []driver.JobInsertParams{params}, true)
	results, err := etx.JobInsertMany(ctx, []driver.JobInsertParams{params})
	if err != nil {
		finish(results, err)
		return nil, err
	}
	if len(results) != 1 {
		err = fmt.Errorf("driver %q returned %d results for one transactional insert", c.driver.Name(), len(results))
		finish(results, err)
		return nil, err
	}
	finish(results, nil)
	return &results[0], nil
}

// EnqueueBatch inserts jobs with per-item options.
func (c *Client[TTx]) EnqueueBatch(ctx context.Context, requests []InsertRequest) ([]driver.JobInsertResult, error) {
	params := make([]driver.JobInsertParams, 0, len(requests))
	for _, request := range requests {
		p, err := c.buildInsertParams(request.Args, request.Opts)
		if err != nil {
			return nil, err
		}
		params = append(params, p)
	}
	ctx, finish := c.startEnqueue(ctx, params, false)
	results, err := c.driver.Executor().JobInsertMany(ctx, params)
	finish(results, err)
	return results, err
}

// EnqueueBatchTx inserts jobs with per-item options in an existing transaction.
func (c *Client[TTx]) EnqueueBatchTx(ctx context.Context, tx TTx, requests []InsertRequest) ([]driver.JobInsertResult, error) {
	if !c.driver.Capabilities().NativeTx {
		return nil, fmt.Errorf("%w: driver %q does not support transactional inserts", driver.ErrUnsupported, c.driver.Name())
	}
	params := make([]driver.JobInsertParams, 0, len(requests))
	for _, request := range requests {
		p, err := c.buildInsertParams(request.Args, request.Opts)
		if err != nil {
			return nil, err
		}
		params = append(params, p)
	}
	ctx, finish := c.startEnqueue(ctx, params, true)
	results, err := c.driver.UnwrapTx(tx).JobInsertMany(ctx, params)
	finish(results, err)
	return results, err
}

// EnqueueMany inserts multiple jobs in a single batch (non-transactional).
func (c *Client[TTx]) EnqueueMany(ctx context.Context, args []core.JobArgs, opts *core.InsertOpts) ([]driver.JobInsertResult, error) {
	params := make([]driver.JobInsertParams, 0, len(args))
	for _, a := range args {
		p, err := c.buildInsertParams(a, opts)
		if err != nil {
			return nil, err
		}
		params = append(params, p)
	}
	ctx, finish := c.startEnqueue(ctx, params, false)
	results, err := c.driver.Executor().JobInsertMany(ctx, params)
	finish(results, err)
	return results, err
}

// EnqueueManyTx inserts multiple jobs within an existing transaction.
func (c *Client[TTx]) EnqueueManyTx(ctx context.Context, tx TTx, args []core.JobArgs, opts *core.InsertOpts) ([]driver.JobInsertResult, error) {
	if !c.driver.Capabilities().NativeTx {
		return nil, fmt.Errorf("%w: driver %q does not support transactional inserts", driver.ErrUnsupported, c.driver.Name())
	}
	params := make([]driver.JobInsertParams, 0, len(args))
	for _, a := range args {
		p, err := c.buildInsertParams(a, opts)
		if err != nil {
			return nil, err
		}
		params = append(params, p)
	}
	etx := c.driver.UnwrapTx(tx)
	ctx, finish := c.startEnqueue(ctx, params, true)
	results, err := etx.JobInsertMany(ctx, params)
	finish(results, err)
	return results, err
}

func (c *Client[TTx]) startEnqueue(ctx context.Context, params []driver.JobInsertParams, transactional bool) (context.Context, func([]driver.JobInsertResult, error)) {
	if c.config.Observer == nil {
		return ctx, func([]driver.JobInsertResult, error) {}
	}
	start := EnqueueStart{Driver: c.driver.Name(), Count: len(params), Transactional: transactional}
	if len(params) > 0 {
		start.Queue, start.Kind = params[0].Queue, params[0].Kind
		for _, param := range params[1:] {
			if param.Queue != start.Queue {
				start.Queue = ""
			}
			if param.Kind != start.Kind {
				start.Kind = ""
			}
		}
	}
	observedCtx, finish := c.config.Observer.StartEnqueue(ctx, start)
	if observedCtx == nil {
		observedCtx = ctx
	}
	if finish == nil {
		finish = func(EnqueueFinish) {}
	}
	return observedCtx, func(results []driver.JobInsertResult, err error) {
		outcome := EnqueueFinish{Err: err}
		for _, result := range results {
			if result.UniqueSkip {
				outcome.UniqueSkipped++
			} else if result.Job != nil {
				outcome.Inserted++
			}
		}
		finish(outcome)
	}
}

// Cancel marks a job as cancelled. The job must be in available or scheduled state.
func (c *Client[TTx]) Cancel(ctx context.Context, id string) error {
	return c.driver.Executor().JobCancel(ctx, id)
}

func (c *Client[TTx]) buildInsertParams(args core.JobArgs, opts *core.InsertOpts) (driver.JobInsertParams, error) {
	if args == nil || reflect.ValueOf(args).Kind() == reflect.Pointer && reflect.ValueOf(args).IsNil() {
		return driver.JobInsertParams{}, fmt.Errorf("job args must not be nil")
	}
	kind := args.Kind()
	if kind == "" {
		return driver.JobInsertParams{}, fmt.Errorf("job kind must not be empty")
	}
	argsJSON, err := json.Marshal(args)
	if err != nil {
		return driver.JobInsertParams{}, fmt.Errorf("marshal job args: %w", err)
	}

	queue := c.config.DefaultQueue
	var priority int
	var runAt time.Time
	var uniqueKey string
	var maxRetry int
	var timeout time.Duration
	var tags []string
	var pipelineID string

	if opts != nil {
		if opts.MaxRetry != nil && *opts.MaxRetry < 0 {
			return driver.JobInsertParams{}, fmt.Errorf("max retry must not be negative")
		}
		if opts.Timeout != nil && *opts.Timeout < 0 {
			return driver.JobInsertParams{}, fmt.Errorf("timeout must not be negative")
		}
		if opts.UniqueOpts != nil && opts.UniqueOpts.ByPeriod < 0 {
			return driver.JobInsertParams{}, fmt.Errorf("unique period must not be negative")
		}
		if opts.Queue != "" {
			queue = opts.Queue
		}
		priority = opts.Priority
		runAt = opts.RunAt
		maxRetry = func() int {
			if opts.MaxRetry != nil {
				return *opts.MaxRetry
			}
			return 0
		}()
		timeout = func() time.Duration {
			if opts.Timeout != nil {
				return *opts.Timeout
			}
			return 0
		}()
		tags = opts.Tags
		pipelineID = opts.PipelineID

		if opts.UniqueOpts != nil {
			var now time.Time
			if c.config.Clock != nil {
				now = c.config.Clock.Now()
			} else {
				now = c.config.Now()
			}
			uniqueKey, err = buildUniqueKey(args, queue, opts.UniqueOpts, now)
			if err != nil {
				return driver.JobInsertParams{}, err
			}
		}
	}

	return driver.JobInsertParams{
		Queue:      queue,
		Kind:       kind,
		Args:       argsJSON,
		Priority:   priority,
		RunAt:      runAt,
		UniqueKey:  uniqueKey,
		MaxRetry:   maxRetry,
		Timeout:    timeout,
		Tags:       tags,
		PipelineID: pipelineID,
	}, nil
}

func buildUniqueKey(args core.JobArgs, queue string, opts *core.UniqueOpts, now time.Time) (string, error) {
	if opts == nil {
		return "", nil
	}

	canonical := struct {
		Version     int             `json:"version"`
		Kind        string          `json:"kind"`
		Queue       string          `json:"queue,omitempty"`
		CallerKey   string          `json:"caller_key,omitempty"`
		Args        json.RawMessage `json:"args,omitempty"`
		PeriodStart int64           `json:"period_start,omitempty"`
	}{Version: 2, Kind: args.Kind(), CallerKey: opts.Key}
	if opts.ByQueue {
		canonical.Queue = queue
	}
	if opts.ByArgs {
		b, err := json.Marshal(args)
		if err != nil {
			return "", fmt.Errorf("marshal args for unique key: %w", err)
		}
		canonical.Args = b
	}
	if opts.ByPeriod > 0 {
		canonical.PeriodStart = now.Truncate(opts.ByPeriod).UnixNano()
	}
	payload, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("marshal canonical unique key: %w", err)
	}
	digest := sha256.Sum256(payload)
	prefix := "u2_"
	if opts.Forever {
		prefix = "uf2_"
	}
	return fmt.Sprintf("%s%x", prefix, digest[:]), nil
}
