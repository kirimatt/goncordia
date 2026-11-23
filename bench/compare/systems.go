package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"

	pgxdriver "github.com/kirimatt/goncordia/driver/pgxv5/v2"
	redisdriver "github.com/kirimatt/goncordia/driver/redis/v2"
	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/core"
)

type noopArgs struct {
	N int `json:"n"`
}

func (noopArgs) Kind() string { return "noop" }

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// ---- goncordia (pgxv5) ----

type goncordiaPG struct {
	pool   *pgxpool.Pool
	d      *pgxdriver.Driver
	client *pgxdriver.Client
}

func newGoncordiaPG(ctx context.Context, dsn string) (system, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	d := pgxdriver.New(pool)
	return &goncordiaPG{pool: pool, d: d, client: pgxdriver.NewClient(d, goncordia.ClientConfig{})}, nil
}

func (s *goncordiaPG) Name() string { return "goncordia-pgxv5" }
func (s *goncordiaPG) Reset(ctx context.Context) error {
	if _, err := s.pool.Exec(ctx, `DROP TABLE IF EXISTS goncordia_jobs, goncordia_schema_migrations,
		goncordia_leaders, goncordia_schedule_cursors, goncordia_job_results, goncordia_queues CASCADE`); err != nil {
		return err
	}
	if err := s.d.Migrate(ctx); err != nil {
		return err
	}
	s.pool.Reset() // drop statements cached against the old tables
	return nil
}
func (s *goncordiaPG) Enqueue(ctx context.Context, n int) error {
	_, err := s.client.Enqueue(ctx, noopArgs{N: n}, nil)
	return err
}
func (s *goncordiaPG) EnqueueBatch(ctx context.Context, ns []int) (bool, error) {
	args := make([]core.JobArgs, len(ns))
	for i, n := range ns {
		args[i] = noopArgs{N: n}
	}
	_, err := s.client.EnqueueMany(ctx, args, nil)
	return true, err
}
func (s *goncordiaPG) Start(ctx context.Context, c int, handled func(int)) (func(), error) {
	return startGoncordia(ctx, c, handled, func(r *core.Registry) *goncordia.WorkerPool[pgx.Tx] {
		return pgxdriver.NewWorkerPool(s.d, r, goncordia.WorkerConfig{Concurrency: c})
	})
}
func (s *goncordiaPG) Close() { s.pool.Close() }

// ---- goncordia (redis) ----

type goncordiaRedis struct {
	rdb    *redis.Client
	d      *redisdriver.Driver
	client *goncordia.Client[redisdriver.NoTx]
}

func newGoncordiaRedis(_ context.Context, addr string) (system, error) {
	rdb := redis.NewClient(&redis.Options{Addr: addr, PoolSize: 200})
	d := redisdriver.New(rdb)
	return &goncordiaRedis{rdb: rdb, d: d, client: goncordia.NewClient[redisdriver.NoTx](d, goncordia.ClientConfig{})}, nil
}

func (s *goncordiaRedis) Name() string { return "goncordia-redis" }
func (s *goncordiaRedis) Reset(ctx context.Context) error {
	if err := s.rdb.FlushAll(ctx).Err(); err != nil {
		return err
	}
	return s.d.Migrate(ctx)
}
func (s *goncordiaRedis) Enqueue(ctx context.Context, n int) error {
	_, err := s.client.Enqueue(ctx, noopArgs{N: n}, nil)
	return err
}
func (s *goncordiaRedis) EnqueueBatch(ctx context.Context, ns []int) (bool, error) {
	args := make([]core.JobArgs, len(ns))
	for i, n := range ns {
		args[i] = noopArgs{N: n}
	}
	_, err := s.client.EnqueueMany(ctx, args, nil)
	return true, err
}
func (s *goncordiaRedis) Start(ctx context.Context, c int, handled func(int)) (func(), error) {
	return startGoncordia(ctx, c, handled, func(r *core.Registry) *goncordia.WorkerPool[redisdriver.NoTx] {
		return goncordia.NewWorkerPool[redisdriver.NoTx](s.d, r, goncordia.WorkerConfig{Concurrency: c})
	})
}
func (s *goncordiaRedis) Close() { _ = s.rdb.Close() }

func startGoncordia[TTx any](ctx context.Context, _ int, handled func(int), pool func(*core.Registry) *goncordia.WorkerPool[TTx]) (func(), error) {
	r := core.NewRegistry()
	core.RegisterWorker(r, core.WorkerFunc[noopArgs](func(_ context.Context, j *core.Job[noopArgs]) error {
		handled(j.Args.N)
		return nil
	}), core.WorkerOpts{})
	wp := pool(r)
	runCtx, cancel := context.WithCancel(ctx)
	go func() { _ = wp.Start(runCtx) }()
	return func() {
		_ = wp.Shutdown(context.Background())
		cancel()
	}, nil
}

// ---- river ----

type riverSystem struct {
	pool   *pgxpool.Pool
	insert *river.Client[pgx.Tx]
	tuned  bool // FetchCooldown 1ms, FetchPollInterval 10ms instead of 100ms / 1s
}

type riverNoopWorker struct {
	river.WorkerDefaults[noopArgs]
	handled func(int)
}

func (w *riverNoopWorker) Work(_ context.Context, j *river.Job[noopArgs]) error {
	w.handled(j.Args.N)
	return nil
}

func newRiver(ctx context.Context, dsn string, tuned bool) (system, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	insert, err := river.NewClient(riverpgxv5.New(pool), &river.Config{Logger: quiet})
	if err != nil {
		return nil, err
	}
	return &riverSystem{pool: pool, insert: insert, tuned: tuned}, nil
}

func (s *riverSystem) Name() string {
	if s.tuned {
		return "river-tuned"
	}
	return "river"
}
func (s *riverSystem) Reset(ctx context.Context) error {
	m, err := rivermigrate.New(riverpgxv5.New(s.pool), &rivermigrate.Config{Logger: quiet})
	if err != nil {
		return err
	}
	if _, err := m.Migrate(ctx, rivermigrate.DirectionDown, &rivermigrate.MigrateOpts{TargetVersion: -1}); err != nil {
		return fmt.Errorf("river migrate down: %w", err)
	}
	if _, err := m.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		return fmt.Errorf("river migrate up: %w", err)
	}
	s.pool.Reset() // drop statements cached against the old tables
	return nil
}
func (s *riverSystem) Enqueue(ctx context.Context, n int) error {
	_, err := s.insert.Insert(ctx, noopArgs{N: n}, nil)
	return err
}
func (s *riverSystem) EnqueueBatch(ctx context.Context, ns []int) (bool, error) {
	params := make([]river.InsertManyParams, len(ns))
	for i, n := range ns {
		params[i] = river.InsertManyParams{Args: noopArgs{N: n}}
	}
	_, err := s.insert.InsertMany(ctx, params)
	return true, err
}
func (s *riverSystem) Start(ctx context.Context, c int, handled func(int)) (func(), error) {
	workers := river.NewWorkers()
	river.AddWorker(workers, &riverNoopWorker{handled: handled})
	cfg := &river.Config{
		Logger:  quiet,
		Queues:  map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: c}},
		Workers: workers,
	}
	if s.tuned {
		cfg.FetchCooldown = time.Millisecond
		cfg.FetchPollInterval = 10 * time.Millisecond
	}
	client, err := river.NewClient(riverpgxv5.New(s.pool), cfg)
	if err != nil {
		return nil, err
	}
	if err := client.Start(ctx); err != nil {
		return nil, err
	}
	return func() { _ = client.Stop(context.Background()) }, nil
}
func (s *riverSystem) Close() { s.pool.Close() }

// ---- asynq ----

type asynqSystem struct {
	tuned  bool // TaskCheckInterval 10ms instead of 1s
	addr   string
	rdb    *redis.Client
	client *asynq.Client
}

func newAsynq(addr string, tuned bool) (system, error) {
	return &asynqSystem{
		tuned:  tuned,
		addr:   addr,
		rdb:    redis.NewClient(&redis.Options{Addr: addr}),
		client: asynq.NewClient(asynq.RedisClientOpt{Addr: addr}),
	}, nil
}

func (s *asynqSystem) Name() string {
	if s.tuned {
		return "asynq-tuned"
	}
	return "asynq"
}
func (s *asynqSystem) Reset(ctx context.Context) error {
	return s.rdb.FlushAll(ctx).Err()
}
func (s *asynqSystem) Enqueue(ctx context.Context, n int) error {
	payload, _ := json.Marshal(noopArgs{N: n})
	_, err := s.client.EnqueueContext(ctx, asynq.NewTask("noop", payload))
	return err
}
func (s *asynqSystem) EnqueueBatch(context.Context, []int) (bool, error) { return false, nil }
func (s *asynqSystem) Start(_ context.Context, c int, handled func(int)) (func(), error) {
	cfg := asynq.Config{
		Concurrency: c,
		LogLevel:    asynq.ErrorLevel,
	}
	if s.tuned {
		cfg.TaskCheckInterval = 10 * time.Millisecond
	}
	srv := asynq.NewServer(asynq.RedisClientOpt{Addr: s.addr}, cfg)
	mux := asynq.NewServeMux()
	mux.HandleFunc("noop", func(_ context.Context, t *asynq.Task) error {
		var a noopArgs
		if err := json.Unmarshal(t.Payload(), &a); err != nil {
			return err
		}
		handled(a.N)
		return nil
	})
	if err := srv.Start(mux); err != nil {
		return nil, err
	}
	return srv.Shutdown, nil
}
func (s *asynqSystem) Close() {
	_ = s.client.Close()
	_ = s.rdb.Close()
}
