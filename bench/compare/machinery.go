package main

import (
	"context"
	"io"
	stdlog "log"
	"sync/atomic"

	"github.com/RichardKnop/machinery/v2"
	redisbackend "github.com/RichardKnop/machinery/v2/backends/redis"
	redisbroker "github.com/RichardKnop/machinery/v2/brokers/redis"
	"github.com/RichardKnop/machinery/v2/config"
	eagerlock "github.com/RichardKnop/machinery/v2/locks/eager"
	machlog "github.com/RichardKnop/machinery/v2/log"
	"github.com/RichardKnop/machinery/v2/tasks"
	"github.com/redis/go-redis/v9"
)

// machinery with its Redis broker and Redis result backend (go-redis based),
// default poll periods.
type machinerySystem struct {
	addr    string
	cnf     *config.Config
	rdb     *redis.Client
	server  *machinery.Server
	handled atomic.Pointer[func(int)]
}

func newMachinery(addr string) (system, error) {
	machlog.Set(stdlog.New(io.Discard, "", 0))
	s := &machinerySystem{
		addr: addr,
		cnf: &config.Config{
			DefaultQueue:    "machinery_tasks",
			ResultsExpireIn: 3600,
			Redis: &config.RedisConfig{
				MaxIdle: 3, IdleTimeout: 240, ReadTimeout: 15, WriteTimeout: 15, ConnectTimeout: 15,
				NormalTasksPollPeriod: 1000, DelayedTasksPollPeriod: 500,
			},
		},
		rdb: redis.NewClient(&redis.Options{Addr: addr}),
	}
	return s, s.build()
}

// build creates a fresh server: stopping a worker closes the broker's client.
func (s *machinerySystem) build() error {
	s.server = machinery.NewServer(s.cnf,
		redisbroker.NewGR(s.cnf, []string{s.addr}, 0),
		redisbackend.NewGR(s.cnf, []string{s.addr}, 0),
		eagerlock.New())
	return s.server.RegisterTask("noop", func(n int64) error {
		if h := s.handled.Load(); h != nil {
			(*h)(int(n))
		}
		return nil
	})
}

func (s *machinerySystem) Name() string { return "machinery" }
func (s *machinerySystem) Reset(ctx context.Context) error {
	return s.rdb.FlushAll(ctx).Err()
}
func (s *machinerySystem) Enqueue(ctx context.Context, n int) error {
	_, err := s.server.SendTaskWithContext(ctx, &tasks.Signature{
		Name: "noop",
		Args: []tasks.Arg{{Type: "int64", Value: int64(n)}},
	})
	return err
}
func (s *machinerySystem) EnqueueBatch(context.Context, []int) (bool, error) { return false, nil }
func (s *machinerySystem) Start(_ context.Context, c int, handled func(int)) (func(), error) {
	s.handled.Store(&handled)
	w := s.server.NewWorker("bench", c)
	errs := make(chan error, 1)
	w.LaunchAsync(errs)
	return func() {
		w.Quit()
		s.handled.Store(nil)
		_ = s.build()
	}, nil
}
func (s *machinerySystem) Close() { _ = s.rdb.Close() }
