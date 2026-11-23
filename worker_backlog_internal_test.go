package goncordia

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

type backlogArgs struct {
	N int `json:"n"`
}

func (backlogArgs) Kind() string { return "backlog_race" }

// racingFetchDriver makes the first fetch pass end only after every job it
// claimed has finished, so those jobs cannot see the pass's backlog result.
type racingFetchDriver struct {
	*memory.Driver
	pool  atomic.Pointer[WorkerPool[memory.NoTx]]
	calls atomic.Int64
}

type racingFetchExecutor struct {
	driver.Executor
	d *racingFetchDriver
}

func (d *racingFetchDriver) Executor() driver.Executor {
	return racingFetchExecutor{Executor: d.Driver.Executor(), d: d}
}

func (e racingFetchExecutor) JobFetchBatch(ctx context.Context, params driver.FetchParams) ([]driver.JobRow, error) {
	switch e.d.calls.Add(1) {
	case 2: // queue "idle", after the first full claim from "work"
		p := e.d.pool.Load()
		deadline := time.Now().Add(5 * time.Second)
		for len(p.pending) > 0 && time.Now().Before(deadline) {
			runtime.Gosched()
		}
		return nil, nil
	case 3: // "work" again: end the pass with a miss
		return nil, nil
	}
	return e.Executor.JobFetchBatch(ctx, params)
}

// Jobs that finish before their fetch pass returns must not lose the
// backlog wake-up: the pass itself has to ask for the next one.
func TestWorkerPoolBacklogWakeSurvivesFastJobs(t *testing.T) {
	d := &racingFetchDriver{Driver: memory.New()}
	client := NewClient[memory.NoTx](d, ClientConfig{})
	registry := core.NewRegistry()
	var done atomic.Int64
	core.RegisterWorker(registry, core.WorkerFunc[backlogArgs](func(context.Context, *core.Job[backlogArgs]) error {
		done.Add(1)
		return nil
	}), core.WorkerOpts{})
	const jobs = 40
	args := make([]core.JobArgs, jobs)
	for i := range args {
		args[i] = backlogArgs{N: i}
	}
	if _, err := client.EnqueueMany(context.Background(), args, &core.InsertOpts{Queue: "work"}); err != nil {
		t.Fatal(err)
	}
	pool := NewWorkerPool[memory.NoTx](d, registry, WorkerConfig{
		Queues: []string{"work", "idle"}, Concurrency: 4, MaxPending: 8, PollInterval: time.Hour,
	})
	d.pool.Store(pool)
	go pool.Start(context.Background()) //nolint:errcheck
	defer func() { _ = pool.Shutdown(context.Background()) }()
	deadline := time.Now().Add(5 * time.Second)
	for done.Load() != jobs {
		if time.Now().After(deadline) {
			t.Fatalf("processed %d of %d jobs: the backlog wake-up was lost", done.Load(), jobs)
		}
		time.Sleep(time.Millisecond)
	}
}
