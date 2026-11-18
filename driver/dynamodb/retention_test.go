package dynamodbdriver_test

import (
	"context"
	"errors"
	"testing"
	"time"

	dynamodbdriver "github.com/kirimatt/goncordia/driver/dynamodb/v2"
	goncordia "github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/driver"
)

func finishJob(t *testing.T, exec driver.Executor, state driver.JobState, result bool) string {
	t.Helper()
	ctx := context.Background()
	if _, err := exec.JobInsertMany(ctx, []driver.JobInsertParams{{Queue: "retention", Kind: "k", Args: []byte(`{}`), MaxRetry: 1}}); err != nil {
		t.Fatal(err)
	}
	var rows []driver.JobRow
	deadline := time.Now().Add(5 * time.Second)
	for len(rows) == 0 {
		var err error
		rows, err = exec.JobFetchBatch(ctx, driver.FetchParams{Queue: "retention", Limit: 1, WorkerID: "w", LeaseDuration: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		if len(rows) == 0 && time.Now().After(deadline) {
			t.Fatal("job was never claimable")
		}
	}
	if result {
		if err := exec.JobResultPut(ctx, rows[0].ID, []byte(`1`)); err != nil {
			t.Fatal(err)
		}
	}
	if err := exec.JobSetStateIfRunning(ctx, driver.JobSetStateParams{
		ID: rows[0].ID, State: state, ExpectedWorkerID: "w", ExpectedAttempt: rows[0].AttemptNum,
	}); err != nil {
		t.Fatal(err)
	}
	return rows[0].ID
}

// TestDynamoDB_RetentionIndex checks the gsi_retention buckets: a result
// pass moves a job out of completed_result so the next pass skips it, and a
// rescheduled job leaves the index.
func TestDynamoDB_RetentionIndex(t *testing.T) {
	svc, cleanup := newTestClient(t)
	defer cleanup()
	ctx := context.Background()
	d := dynamodbdriver.New(svc)
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	exec := d.Executor()
	done := finishJob(t, exec, driver.JobStateCompleted, true)
	revived := finishJob(t, exec, driver.JobStateDiscarded, false)
	if err := exec.JobReschedule(ctx, driver.RescheduleParams{ID: revived, RunAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}

	m := goncordia.NewMaintenance(d, goncordia.MaintenanceConfig{Clock: clock.NewManual(time.Now().Add(2 * time.Hour))})
	policy := goncordia.RetentionPolicy{Completed: 24 * time.Hour, Results: time.Hour}
	result, err := m.Prune(ctx, policy)
	if err != nil || result.ResultsDeleted != 1 || result.Deleted != 0 {
		t.Fatalf("results pass = %+v, %v", result, err)
	}
	if _, err := exec.JobResultGet(ctx, done); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("result survived: %v", err)
	}
	if again, _ := m.Prune(ctx, policy); again.ResultsDeleted != 0 {
		t.Fatalf("second results pass revisited jobs: %+v", again)
	}
	result, err = m.Prune(ctx, goncordia.RetentionPolicy{Completed: time.Hour, Discarded: time.Hour})
	if err != nil || result.Deleted != 1 {
		t.Fatalf("jobs pass = %+v, %v", result, err)
	}
	if _, err := exec.JobGetByID(ctx, done); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("completed job survived: %v", err)
	}
	if _, err := exec.JobGetByID(ctx, revived); err != nil {
		t.Fatalf("rescheduled job was pruned: %v", err)
	}
}
