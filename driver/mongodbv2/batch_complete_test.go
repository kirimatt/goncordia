package mongodbv2_test

import (
	"context"
	"slices"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/kirimatt/goncordia/v2/driver"
)

func TestMongo_JobCompleteMany(t *testing.T) {
	d, _ := newDriver(t)
	exec := d.Executor()
	ctx := context.Background()
	completer, ok := exec.(interface {
		JobCompleteMany(context.Context, []driver.JobSetStateParams) ([]bool, error)
	})
	if !ok {
		t.Fatal("executor does not implement JobCompleteMany")
	}
	params := []driver.JobInsertParams{
		{Queue: "bq", Kind: "k", Args: []byte(`{}`), MaxRetry: 3, UniqueKey: "plain-key"},
		{Queue: "bq", Kind: "k", Args: []byte(`{}`), MaxRetry: 3, UniqueKey: "uf2_forever"},
		{Queue: "bq", Kind: "k", Args: []byte(`{}`), MaxRetry: 3},
		{Queue: "bq", Kind: "k", Args: []byte(`{}`), MaxRetry: 3},
	}
	if _, err := exec.JobInsertMany(ctx, params); err != nil {
		t.Fatal(err)
	}
	rows, err := exec.JobFetchBatch(ctx, driver.FetchParams{Queue: "bq", Limit: 3, WorkerID: "w1"})
	if err != nil || len(rows) != 3 {
		t.Fatalf("fetch: rows=%d err=%v", len(rows), err)
	}
	idle, err := exec.JobFetchBatch(ctx, driver.FetchParams{Queue: "bq", Limit: 1})
	if err != nil || len(idle) != 1 {
		t.Fatalf("fetch 4th: rows=%d err=%v", len(idle), err)
	}
	if err := exec.JobSetStateIfRunning(ctx, driver.JobSetStateParams{ID: idle[0].ID, Yield: true}); err != nil {
		t.Fatal(err)
	}

	got, err := completer.JobCompleteMany(ctx, []driver.JobSetStateParams{
		{ID: rows[0].ID, ExpectedWorkerID: "w1", ExpectedAttempt: rows[0].AttemptNum},
		{ID: rows[1].ID, ExpectedWorkerID: "w1", ExpectedAttempt: rows[1].AttemptNum},
		{ID: rows[2].ID, ExpectedWorkerID: "other", ExpectedAttempt: rows[2].AttemptNum},
		{ID: idle[0].ID},
	})
	if err != nil {
		t.Fatal(err)
	}
	if want := []bool{true, true, false, false}; !slices.Equal(got, want) {
		t.Fatalf("completed=%v want %v", got, want)
	}
	for i, r := range rows {
		j, err := exec.JobGetByID(ctx, r.ID)
		if err != nil {
			t.Fatal(err)
		}
		switch i {
		case 0, 1:
			if j.State != driver.JobStateCompleted || j.FinalizedAt == nil || j.WorkerID != "" {
				t.Fatalf("job %d: state=%s finalized=%v worker=%q", i, j.State, j.FinalizedAt, j.WorkerID)
			}
			if r.UniqueKey == "plain-key" && j.UniqueKey != "" {
				t.Fatalf("plain unique key kept: %q", j.UniqueKey)
			}
			if r.UniqueKey == "uf2_forever" && j.UniqueKey != "uf2_forever" {
				t.Fatalf("uf2_ key dropped: %q", j.UniqueKey)
			}
		case 2:
			if j.State != driver.JobStateRunning {
				t.Fatalf("stale job state=%s, want running", j.State)
			}
		}
	}
	if got, err := completer.JobCompleteMany(ctx, nil); err != nil || len(got) != 0 {
		t.Fatalf("empty batch: got=%v err=%v", got, err)
	}
	if _, err := completer.JobCompleteMany(ctx, []driver.JobSetStateParams{{ID: "zz"}}); err == nil {
		t.Fatal("invalid id accepted")
	}
	// All claims stale: no write is sent.
	got, err = completer.JobCompleteMany(ctx, []driver.JobSetStateParams{{ID: bson.NewObjectID().Hex()}})
	if err != nil || !slices.Equal(got, []bool{false}) {
		t.Fatalf("unknown job: got=%v err=%v", got, err)
	}
}
