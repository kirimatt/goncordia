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

type maintenanceArgs struct{ Value string }

func (maintenanceArgs) Kind() string { return "maintenance_test" }

func TestMaintenancePruneUsesInjectedClock(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewManual(time.Date(2026, 8, 16, 8, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
	maintenance := goncordia.NewMaintenance(d, goncordia.MaintenanceConfig{Clock: clk})

	completed := insertTerminalJob(t, ctx, client, d.Executor(), driver.JobStateCompleted)
	discarded := insertTerminalJob(t, ctx, client, d.Executor(), driver.JobStateDiscarded)
	cancelled, err := client.Enqueue(ctx, maintenanceArgs{Value: "cancelled"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := d.Executor().JobCancel(ctx, cancelled.Job.ID); err != nil {
		t.Fatal(err)
	}

	clk.Advance(2 * time.Hour)
	fresh := insertTerminalJob(t, ctx, client, d.Executor(), driver.JobStateCompleted)
	result, err := maintenance.Prune(ctx, goncordia.RetentionPolicy{
		Completed: time.Hour,
		Discarded: time.Hour,
		Cancelled: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Deleted != 3 || result.Scanned != 4 {
		t.Fatalf("prune result=%+v", result)
	}
	for _, id := range []string{completed, discarded, cancelled.Job.ID} {
		if _, err := d.Executor().JobGetByID(ctx, id); !errors.Is(err, driver.ErrNotFound) {
			t.Fatalf("job %s still exists: %v", id, err)
		}
	}
	if row, err := d.Executor().JobGetByID(ctx, fresh); err != nil || row == nil {
		t.Fatalf("fresh job was pruned: row=%+v err=%v", row, err)
	}
}

func TestMaintenanceBulkAndDeadLetterAPIs(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewManual(time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
	maintenance := goncordia.NewMaintenance(d, goncordia.MaintenanceConfig{Clock: clk})

	firstDiscarded := insertTerminalJob(t, ctx, client, d.Executor(), driver.JobStateDiscarded)
	secondDiscarded := insertTerminalJob(t, ctx, client, d.Executor(), driver.JobStateDiscarded)
	page, err := maintenance.DeadLetterList(ctx, driver.JobListParams{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || !page.HasMore || page.NextCursor == "" {
		t.Fatalf("dead-letter page=%+v", page)
	}

	replayed, err := maintenance.DeadLetterReplay(ctx, []string{firstDiscarded}, time.Time{})
	if err != nil || len(replayed.Succeeded) != 1 {
		t.Fatalf("replay result=%+v err=%v", replayed, err)
	}
	row, err := d.Executor().JobGetByID(ctx, firstDiscarded)
	if err != nil || row.State != driver.JobStateScheduled || !row.RunAt.Equal(clk.Now()) {
		t.Fatalf("replayed row=%+v err=%v", row, err)
	}
	if result, err := maintenance.DeadLetterReplay(ctx, []string{firstDiscarded}, time.Time{}); !errors.Is(err, driver.ErrConflict) || len(result.Failed) != 1 {
		t.Fatalf("non-discarded replay result=%+v err=%v", result, err)
	}

	available := make([]string, 0, 3)
	for _, value := range []string{"one", "two", "three"} {
		inserted, err := client.Enqueue(ctx, maintenanceArgs{Value: value}, nil)
		if err != nil {
			t.Fatal(err)
		}
		available = append(available, inserted.Job.ID)
	}
	cancelled, err := maintenance.BulkCancel(ctx, []string{available[0], available[1], "missing"})
	if !errors.Is(err, driver.ErrNotFound) || len(cancelled.Succeeded) != 2 || len(cancelled.Failed) != 1 {
		t.Fatalf("bulk cancel result=%+v err=%v", cancelled, err)
	}
	deleted, err := maintenance.BulkDelete(ctx, available[:2])
	if err != nil || len(deleted.Succeeded) != 2 {
		t.Fatalf("bulk delete result=%+v err=%v", deleted, err)
	}
	retried, err := maintenance.BulkRetry(ctx, []string{available[2], available[2]}, time.Time{})
	if err != nil || len(retried.Succeeded) != 1 {
		t.Fatalf("bulk retry result=%+v err=%v", retried, err)
	}
	if _, err := d.Executor().JobGetByID(ctx, secondDiscarded); err != nil {
		t.Fatalf("unreplayed dead letter missing: %v", err)
	}
}

func insertTerminalJob[TTx any](t *testing.T, ctx context.Context, client *goncordia.Client[TTx], exec driver.Executor, state driver.JobState) string {
	t.Helper()
	inserted, err := client.Enqueue(ctx, maintenanceArgs{Value: string(state)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := exec.JobFetchBatch(ctx, driver.FetchParams{Queue: "default", Limit: 1, WorkerID: "maintenance-test"})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim terminal job: rows=%+v err=%v", claimed, err)
	}
	if err := exec.JobSetStateIfRunning(ctx, driver.JobSetStateParams{
		ID: claimed[0].ID, State: state,
		ExpectedWorkerID: claimed[0].WorkerID, ExpectedAttempt: claimed[0].AttemptNum,
	}); err != nil {
		t.Fatal(err)
	}
	return inserted.Job.ID
}

func TestMaintenancePruneResultsBeforeJobs(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewManual(time.Date(2026, 8, 16, 8, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
	maintenance := goncordia.NewMaintenance(d, goncordia.MaintenanceConfig{Clock: clk})
	exec := d.Executor()

	old := insertTerminalJob(t, ctx, client, exec, driver.JobStateCompleted)
	if err := exec.JobResultPut(ctx, old, []byte(`1`)); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Hour)
	fresh := insertTerminalJob(t, ctx, client, exec, driver.JobStateCompleted)
	if err := exec.JobResultPut(ctx, fresh, []byte(`2`)); err != nil {
		t.Fatal(err)
	}
	policy := goncordia.RetentionPolicy{Completed: 24 * time.Hour, Results: time.Hour}

	result, err := maintenance.Prune(ctx, policy)
	if err != nil || result.ResultsDeleted != 1 || result.Deleted != 0 {
		t.Fatalf("first pass = %+v, %v", result, err)
	}
	if _, err := exec.JobResultGet(ctx, old); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("old result err = %v, want ErrNotFound", err)
	}
	if _, err := exec.JobGetByID(ctx, old); err != nil {
		t.Fatalf("old job row must stay until Completed: %v", err)
	}
	if _, err := exec.JobResultGet(ctx, fresh); err != nil {
		t.Fatalf("fresh result removed early: %v", err)
	}
	if result, _ := maintenance.Prune(ctx, policy); result.ResultsDeleted != 0 {
		t.Fatalf("second pass counted an already deleted result: %+v", result)
	}

	clk.Advance(23 * time.Hour)
	result, err = maintenance.Prune(ctx, policy)
	if err != nil || result.Deleted != 1 || result.ResultsDeleted != 1 {
		t.Fatalf("pass after Completed = %+v, %v", result, err)
	}
	if _, err := maintenance.Prune(ctx, goncordia.RetentionPolicy{Results: -time.Second}); err == nil {
		t.Fatal("negative Results accepted")
	}
}

func TestMaintenanceRunRetentionElectsOneLeader(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 8, 16, 8, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
	policy := goncordia.RetentionPolicy{Completed: 10 * time.Minute}

	type pass struct {
		worker string
		result goncordia.PruneResult
		err    error
	}
	passes := make(chan pass, 16)
	start := func(worker string) (context.CancelFunc, <-chan error) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		maintenance := goncordia.NewMaintenance(d, goncordia.MaintenanceConfig{Clock: clk})
		go func() {
			done <- maintenance.RunRetention(ctx, goncordia.RetentionRunConfig{
				Policy: policy, Interval: 10 * time.Minute, WorkerID: worker,
				OnPrune: func(r goncordia.PruneResult, err error) { passes <- pass{worker, r, err} },
			})
		}()
		return cancel, done
	}

	stopA, doneA := start("a")
	first := <-passes // the first pass runs immediately and wins the lease
	if first.worker != "a" || first.err != nil {
		t.Fatalf("first pass = %+v", first)
	}
	waitForCondition(t, time.Second, func() bool { return clk.ActiveTimers() == 1 }, "leader ticker")
	stopB, doneB := start("b")
	defer stopB()
	waitForCondition(t, time.Second, func() bool { return clk.ActiveTimers() == 2 }, "follower ticker")

	insertTerminalJob(t, context.Background(), client, d.Executor(), driver.JobStateCompleted)
	// One interval later the leader renews its 20-minute lease and prunes;
	// the follower's election fails.
	clk.Advance(10 * time.Minute)
	got := <-passes
	if got.worker != "a" || got.result.Deleted != 1 {
		t.Fatalf("leader pass = %+v", got)
	}
	select {
	case extra := <-passes:
		t.Fatalf("follower pruned while the leader holds the lease: %+v", extra)
	default:
	}

	stopA() // the leader resigns on exit
	if err := <-doneA; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunRetention returned %v", err)
	}
	clk.Advance(10 * time.Minute)
	if got := <-passes; got.worker != "b" {
		t.Fatalf("pass after resignation = %+v, want follower b", got)
	}
	stopB()
	<-doneB

	m := goncordia.NewMaintenance(d, goncordia.MaintenanceConfig{Clock: clk})
	if err := m.RunRetention(context.Background(), goncordia.RetentionRunConfig{Interval: -time.Second}); err == nil {
		t.Fatal("negative Interval accepted")
	}
}

func TestWorkerPoolPrunesByDefault(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 8, 16, 8, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
	exec := d.Executor()
	old := insertTerminalJob(t, context.Background(), client, exec, driver.JobStateCompleted)
	if err := exec.JobResultPut(context.Background(), old, []byte(`1`)); err != nil {
		t.Fatal(err)
	}
	kept := insertTerminalJob(t, context.Background(), client, exec, driver.JobStateDiscarded)

	start := func(cfg goncordia.WorkerConfig) func() {
		cfg.Clock = clk
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		pool := goncordia.NewWorkerPool(d, core.NewRegistry(), cfg)
		go func() {
			defer close(done)
			_ = pool.Start(ctx)
		}()
		return func() { cancel(); <-done }
	}

	stop := start(goncordia.WorkerConfig{RetentionInterval: -1})
	clk.Advance(25 * time.Hour)
	waitForCondition(t, time.Second, func() bool { return clk.ActiveTimers() >= 2 }, "pool tickers")
	clk.Advance(10 * time.Minute)
	if _, err := exec.JobGetByID(context.Background(), old); err != nil {
		t.Fatalf("disabled retention pruned a job: %v", err)
	}
	stop()

	stop = start(goncordia.WorkerConfig{}) // defaults: completed 24h, discarded 7d
	defer stop()
	waitForCondition(t, time.Second, func() bool {
		_, err := exec.JobGetByID(context.Background(), old)
		return errors.Is(err, driver.ErrNotFound)
	}, "default retention of an old completed job")
	if _, err := exec.JobResultGet(context.Background(), old); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("result survived: %v", err)
	}
	if _, err := exec.JobGetByID(context.Background(), kept); err != nil {
		t.Fatalf("discarded job pruned before 7 days: %v", err)
	}
}
