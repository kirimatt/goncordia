package goncordia_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

// txMemory reports native transactions so the client's Tx methods run
// against the memory store.
type txMemory struct{ *memory.Driver }

func (d txMemory) Capabilities() driver.Capabilities {
	caps := d.Driver.Capabilities()
	caps.NativeTx = true
	return caps
}

func TestEnqueueTxMethods(t *testing.T) {
	ctx := context.Background()
	d := txMemory{memory.New()}
	observer := &recordingClientObserver{}
	client := goncordia.NewClient[memory.NoTx](d, goncordia.ClientConfig{Observer: observer})
	var tx memory.NoTx

	if _, err := client.EnqueueTx(ctx, tx, EmailArgs{To: "one"}, nil); err != nil {
		t.Fatal(err)
	}
	if !observer.start.Transactional || observer.finish.Inserted != 1 {
		t.Fatalf("EnqueueTx observed start=%+v finish=%+v", observer.start, observer.finish)
	}
	if results, err := client.EnqueueManyTx(ctx, tx, []core.JobArgs{EmailArgs{To: "two"}, EmailArgs{To: "three"}}, &core.InsertOpts{Queue: "many"}); err != nil || len(results) != 2 {
		t.Fatalf("EnqueueManyTx = %d results, %v", len(results), err)
	}
	if observer.start.Queue != "many" || observer.finish.Inserted != 2 {
		t.Fatalf("EnqueueManyTx observed start=%+v finish=%+v", observer.start, observer.finish)
	}
	results, err := client.EnqueueBatchTx(ctx, tx, []goncordia.InsertRequest{
		{Args: EmailArgs{To: "four"}, Opts: &core.InsertOpts{Queue: "a"}},
		{Args: EmailArgs{To: "five"}, Opts: &core.InsertOpts{Queue: "b"}},
	})
	if err != nil || len(results) != 2 || results[0].Job.Queue != "a" || results[1].Job.Queue != "b" {
		t.Fatalf("EnqueueBatchTx = %+v, %v", results, err)
	}
	if observer.start.Queue != "" {
		t.Fatalf("mixed-queue batch reported queue %q", observer.start.Queue)
	}

	if _, err := client.EnqueueTx(ctx, tx, nil, nil); err == nil {
		t.Fatal("EnqueueTx accepted nil args")
	}
	if _, err := client.EnqueueManyTx(ctx, tx, []core.JobArgs{nil}, nil); err == nil {
		t.Fatal("EnqueueManyTx accepted nil args")
	}
	if _, err := client.EnqueueBatchTx(ctx, tx, []goncordia.InsertRequest{{}}); err == nil {
		t.Fatal("EnqueueBatchTx accepted nil args")
	}
}

func TestEnqueueTxRejectedWithoutNativeTx(t *testing.T) {
	ctx := context.Background()
	client := goncordia.NewClient(memory.New(), goncordia.ClientConfig{})
	var tx memory.NoTx
	if _, err := client.EnqueueTx(ctx, tx, EmailArgs{}, nil); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("EnqueueTx error = %v, want ErrUnsupported", err)
	}
	if _, err := client.EnqueueManyTx(ctx, tx, []core.JobArgs{EmailArgs{}}, nil); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("EnqueueManyTx error = %v, want ErrUnsupported", err)
	}
	if _, err := client.EnqueueBatchTx(ctx, tx, []goncordia.InsertRequest{{Args: EmailArgs{}}}); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("EnqueueBatchTx error = %v, want ErrUnsupported", err)
	}
}

func TestClientCancel(t *testing.T) {
	ctx := context.Background()
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	result, err := client.Enqueue(ctx, EmailArgs{To: "cancel"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Cancel(ctx, result.Job.ID); err != nil {
		t.Fatal(err)
	}
	row, err := d.Executor().JobGetByID(ctx, result.Job.ID)
	if err != nil || row.State != driver.JobStateCancelled || row.FinalizedAt == nil {
		t.Fatalf("cancelled row = %+v, %v", row, err)
	}
	if err := client.Cancel(ctx, result.Job.ID); !errors.Is(err, driver.ErrConflict) {
		t.Fatalf("second Cancel = %v, want ErrConflict", err)
	}
	if err := client.Cancel(ctx, "missing"); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("Cancel(missing) = %v, want ErrNotFound", err)
	}
}

// pruningExecutor records JobPruner calls and reports page-sized batches
// until each call's budget runs out.
type pruningExecutor struct {
	driver.Executor
	jobs, results map[driver.JobState]int
	calls         []driver.JobPruneParams
	err           error
}

func (e *pruningExecutor) JobPruneFinalized(_ context.Context, params driver.JobPruneParams) (int, error) {
	e.calls = append(e.calls, params)
	n := min(e.jobs[params.State], params.Limit)
	e.jobs[params.State] -= n
	return n, e.err
}

func (e *pruningExecutor) JobResultPrune(_ context.Context, params driver.JobPruneParams) (int, error) {
	e.calls = append(e.calls, params)
	n := min(e.results[params.State], params.Limit)
	e.results[params.State] -= n
	return n, nil
}

type pruningDriver struct {
	*memory.Driver
	exec *pruningExecutor
}

func (d pruningDriver) Executor() driver.Executor { return d.exec }

func TestMaintenancePrunesThroughJobPruner(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	exec := &pruningExecutor{
		Executor: memory.New().Executor(),
		jobs:     map[driver.JobState]int{driver.JobStateCompleted: 1200, driver.JobStateDiscarded: 3},
		results:  map[driver.JobState]int{driver.JobStateCompleted: 7},
	}
	m := goncordia.NewMaintenance[memory.NoTx](pruningDriver{memory.New(), exec}, goncordia.MaintenanceConfig{Clock: clock.NewManual(now)})

	result, err := m.Prune(ctx, goncordia.RetentionPolicy{Completed: time.Hour, Discarded: 2 * time.Hour, Results: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if result.Deleted != 1203 || result.ResultsDeleted != 7 {
		t.Fatalf("result = %+v, want 1203 jobs and 7 results", result)
	}
	var completedCalls int
	for _, call := range exec.calls {
		if call.Limit <= 0 {
			t.Fatalf("call without a batch limit: %+v", call)
		}
		switch call.State {
		case driver.JobStateCompleted:
			completedCalls++
		case driver.JobStateDiscarded:
			if !call.Before.Equal(now.Add(-2 * time.Hour)) {
				t.Fatalf("discarded cutoff = %s", call.Before)
			}
		case driver.JobStateCancelled:
			t.Fatal("pruned cancelled jobs with a zero duration")
		}
	}
	if completedCalls < 3 {
		t.Fatalf("completed jobs pruned in %d calls; want repeated full pages", completedCalls)
	}

	exec.err = errors.New("store down")
	exec.jobs[driver.JobStateCompleted] = 1
	if _, err := m.Prune(ctx, goncordia.RetentionPolicy{Completed: time.Hour}); err == nil || !errors.Is(err, exec.err) {
		t.Fatalf("Prune error = %v, want the pruner's error", err)
	}
}
