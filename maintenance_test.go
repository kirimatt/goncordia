package goncordia_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/driver"
	"github.com/kirimatt/goncordia/driver/memory"
)

type maintenanceArgs struct{ Value string }

func (maintenanceArgs) Kind() string { return "maintenance_test" }

func TestMaintenancePruneUsesInjectedClock(t *testing.T) {
	ctx := context.Background()
	clk := clock.NewMock(time.Date(2026, 8, 16, 8, 0, 0, 0, time.UTC))
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
	clk := clock.NewMock(time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC))
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
