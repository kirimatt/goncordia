package dynamodbdriver_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

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

// TestDynamoDB_RetentionIndexUpgrade drops gsi_retention and the index keys
// of an existing job, as on a table from before v2.7.0, and checks that
// Migrate restores the index and backfills the job.
func TestDynamoDB_RetentionIndexUpgrade(t *testing.T) {
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

	if _, err := svc.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName:        aws.String("goncordia_jobs"),
		Key:              map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: done}},
		UpdateExpression: aws.String("REMOVE rt_pk, rt_at, has_result"),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String("goncordia_schedule_cursors"),
		Key:       map[string]types.AttributeValue{"id": &types.AttributeValueMemberS{Value: "goncordia:migration:retention-v1"}},
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.UpdateTable(ctx, &dynamodb.UpdateTableInput{
		TableName:                   aws.String("goncordia_jobs"),
		GlobalSecondaryIndexUpdates: []types.GlobalSecondaryIndexUpdate{{Delete: &types.DeleteGlobalSecondaryIndexAction{IndexName: aws.String("gsi_retention")}}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := d.Migrate(ctx); err != nil {
		t.Fatalf("Migrate on an old table: %v", err)
	}

	// Retrying a discarded job takes it out of the index.
	if err := exec.JobReschedule(ctx, driver.RescheduleParams{ID: revived, RunAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	m := goncordia.NewMaintenance(d, goncordia.MaintenanceConfig{Clock: clock.NewManual(time.Now().Add(2 * time.Hour))})
	result, err := m.Prune(ctx, goncordia.RetentionPolicy{Completed: 24 * time.Hour, Results: time.Hour})
	if err != nil || result.ResultsDeleted != 1 || result.Deleted != 0 {
		t.Fatalf("results pass = %+v, %v", result, err)
	}
	if _, err := exec.JobResultGet(ctx, done); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("backfilled result survived: %v", err)
	}
	if again, _ := m.Prune(ctx, goncordia.RetentionPolicy{Completed: 24 * time.Hour, Results: time.Hour}); again.ResultsDeleted != 0 {
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
