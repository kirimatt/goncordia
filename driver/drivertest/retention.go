package drivertest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	goncordia "github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/driver"
)

// RunRetention verifies that Maintenance retention works on the driver:
// results expire before job rows, each terminal state follows its own
// duration, KeepForever keeps a state, and RunRetention prunes under a leader
// lease. It prunes every old terminal job in the database, so call it after
// the other suites.
func RunRetention[TTx any](t *testing.T, d driver.Driver[TTx]) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	exec := d.Executor()
	queue := fmt.Sprintf("retention-%d", sequence.Add(1))

	finish := func(state driver.JobState, withResult bool) driver.JobRow {
		t.Helper()
		if _, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{{
			Queue: queue, Kind: "retention_test", Args: []byte(`{}`), MaxRetry: 1,
		}}); err != nil {
			t.Fatalf("insert: %v", err)
		}
		row := fetchEventually(t, ctx, exec, queue)
		if withResult {
			if err := exec.JobResultPut(ctx, row.ID, []byte(`1`)); err != nil {
				t.Fatalf("put result: %v", err)
			}
		}
		if err := exec.JobSetStateIfRunning(ctx, driver.JobSetStateParams{
			ID: row.ID, State: state, Attempt: row.AttemptNum,
			ExpectedWorkerID: row.WorkerID, ExpectedAttempt: row.AttemptNum,
		}); err != nil {
			t.Fatalf("finish %s: %v", state, err)
		}
		final, err := exec.JobGetByID(ctx, row.ID)
		if err != nil || final.FinalizedAt == nil {
			t.Fatalf("finished job = %+v, %v; want FinalizedAt", final, err)
		}
		return *final
	}
	completed := []driver.JobRow{finish(driver.JobStateCompleted, true), finish(driver.JobStateCompleted, true)}
	discarded := finish(driver.JobStateDiscarded, false)

	// Retention runs two hours after the last finalization, whatever clock
	// the driver itself uses.
	latest := *completed[0].FinalizedAt
	for _, row := range append(completed[1:], discarded) {
		if row.FinalizedAt.After(latest) {
			latest = *row.FinalizedAt
		}
	}
	clk := clock.NewManual(latest.Add(2 * time.Hour))
	maintenance := goncordia.NewMaintenance(d, goncordia.MaintenanceConfig{Clock: clk})
	exists := func(id string) bool {
		row, err := exec.JobGetByID(ctx, id)
		if err != nil && !errors.Is(err, driver.ErrNotFound) {
			t.Fatalf("get %s: %v", id, err)
		}
		if err == nil && row == nil {
			t.Fatalf("JobGetByID(%s) returned (nil, nil); want ErrNotFound", id)
		}
		return err == nil
	}

	result, err := maintenance.Prune(ctx, goncordia.RetentionPolicy{
		Completed: 3 * time.Hour, Discarded: goncordia.KeepForever, Results: time.Hour,
	})
	if err != nil || result.ResultsDeleted < len(completed) {
		t.Fatalf("results pass = %+v, %v; want results deleted", result, err)
	}
	for _, row := range completed {
		if _, err := exec.JobResultGet(ctx, row.ID); !errors.Is(err, driver.ErrNotFound) {
			t.Fatalf("result of %s survived: %v", row.ID, err)
		}
		if !exists(row.ID) {
			t.Fatalf("job %s deleted before its Completed retention", row.ID)
		}
	}

	result, err = maintenance.Prune(ctx, goncordia.RetentionPolicy{Completed: time.Hour})
	if err != nil || result.Deleted < len(completed) {
		t.Fatalf("completed pass = %+v, %v", result, err)
	}
	for _, row := range completed {
		if exists(row.ID) {
			t.Fatalf("completed job %s survived", row.ID)
		}
	}
	if !exists(discarded.ID) {
		t.Fatal("discarded job pruned by a Completed-only policy")
	}

	passes := make(chan goncordia.PruneResult, 4)
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		done <- maintenance.RunRetention(runCtx, goncordia.RetentionRunConfig{
			Policy:     goncordia.RetentionPolicy{Discarded: time.Hour},
			Interval:   time.Hour,
			LeaderName: "retention-" + queue,
			OnPrune: func(r goncordia.PruneResult, err error) {
				if err != nil {
					t.Errorf("RunRetention pass: %v", err)
				}
				passes <- r
			},
		})
	}()
	select {
	case r := <-passes:
		if r.Deleted < 1 {
			t.Fatalf("RunRetention pass = %+v, want the discarded job deleted", r)
		}
	case <-ctx.Done():
		t.Fatal("RunRetention ran no pass")
	}
	stop()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunRetention returned %v", err)
	}
	if exists(discarded.ID) {
		t.Fatal("discarded job survived RunRetention")
	}
}
