// Package drivertest provides a reusable behavioral contract suite for drivers.
package drivertest

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/driver"
)

var sequence atomic.Uint64

// Run verifies the common executor, rescue, and administrative contracts.
// Drivers should call it from their integration tests after Migrate.
func Run(t *testing.T, exec driver.Executor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	queue := fmt.Sprintf("conformance-%d", sequence.Add(1))
	now := time.Now().UTC().Add(-time.Second)

	if err := exec.QueuePause(ctx, queue); err != nil {
		t.Fatalf("pause queue: %v", err)
	}
	if err := exec.QueueResume(ctx, queue); err != nil {
		t.Fatalf("resume queue: %v", err)
	}

	insert := driver.JobInsertParams{
		Queue: queue, Kind: "conformance", Args: []byte(`{"value":1}`),
		RunAt: now, UniqueKey: "unique", Timeout: time.Second,
		Tags: []string{"contract"}, PipelineID: "entity-1",
	}
	results, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{insert})
	if err != nil || len(results) != 1 || results[0].Job == nil {
		t.Fatalf("insert: results=%+v err=%v", results, err)
	}
	id := results[0].Job.ID
	t.Cleanup(func() { _ = exec.JobDelete(context.Background(), id) })

	duplicates, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{insert})
	if err != nil || len(duplicates) != 1 || !duplicates[0].UniqueSkip {
		t.Fatalf("unique insert: results=%+v err=%v", duplicates, err)
	}

	row, err := exec.JobGetByID(ctx, id)
	if err != nil || row == nil || row.PipelineID != "entity-1" || row.Timeout != time.Second {
		t.Fatalf("get inserted job: row=%+v err=%v", row, err)
	}

	adminExec, ok := exec.(driver.AdminExecutor)
	if !ok {
		t.Fatal("executor does not implement driver.AdminExecutor")
	}
	waitUntil(t, func() bool {
		listed, err := adminExec.JobList(ctx, driver.JobListParams{Queue: queue, Limit: 10})
		return err == nil && len(listed) == 1
	}, "job listing")
	waitUntil(t, func() bool {
		stats, err := adminExec.QueueStats(ctx, queue)
		return err == nil && stats.Total == 1 && stats.States[driver.JobStateAvailable] == 1
	}, "queue stats")

	claimed := fetchEventually(t, ctx, exec, queue)
	if claimed.WorkerID != "conformance-worker" || claimed.AttemptNum != 1 {
		t.Fatalf("claim metadata: %+v", claimed)
	}
	rescuer, ok := exec.(driver.StuckJobRescuer)
	if !ok {
		t.Fatal("executor does not implement driver.StuckJobRescuer")
	}
	rescueDeadline := time.Now().Add(5 * time.Second)
	for {
		rescued, rescueErr := rescuer.JobRescueStuck(ctx, driver.JobRescueParams{Queue: queue, Before: time.Now().UTC().Add(time.Hour)})
		if rescueErr == nil && rescued >= 1 {
			break
		}
		if time.Now().After(rescueDeadline) {
			current, getErr := exec.JobGetByID(ctx, id)
			t.Fatalf("timed out waiting for stuck-job rescue: rescue_err=%v row=%+v get_err=%v", rescueErr, current, getErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
	claimed = fetchEventually(t, ctx, exec, queue)
	if claimed.AttemptNum < 2 {
		t.Fatalf("rescued job did not retain attempt count: %+v", claimed)
	}
	if err := exec.JobSetStateIfRunning(ctx, driver.JobSetStateParams{ID: id, State: driver.JobStateCompleted}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	reinserted, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{insert})
	if err != nil || len(reinserted) != 1 || reinserted[0].UniqueSkip || reinserted[0].Job == nil {
		t.Fatalf("terminal job retained unique slot: results=%+v err=%v", reinserted, err)
	}
	t.Cleanup(func() { _ = exec.JobDelete(context.Background(), reinserted[0].Job.ID) })
}

func waitUntil(t *testing.T, condition func() bool, label string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", label)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func fetchEventually(t *testing.T, ctx context.Context, exec driver.Executor, queue string) driver.JobRow {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		rows, err := exec.JobFetchBatch(ctx, driver.FetchParams{Queue: queue, Limit: 1, WorkerID: "conformance-worker"})
		if err != nil {
			t.Fatalf("fetch: %v", err)
		}
		if len(rows) == 1 {
			return rows[0]
		}
		if time.Now().After(deadline) {
			var current []driver.JobRow
			var listErr error
			if adminExec, ok := exec.(driver.AdminExecutor); ok {
				current, listErr = adminExec.JobList(ctx, driver.JobListParams{Queue: queue, Limit: 10})
			}
			t.Fatalf("timed out fetching conformance job: rows=%+v list_err=%v", current, listErr)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
