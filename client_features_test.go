package goncordia_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver"
	"github.com/kirimatt/goncordia/driver/memory"
)

type recordingClientObserver struct {
	start  goncordia.EnqueueStart
	finish goncordia.EnqueueFinish
}

func (o *recordingClientObserver) StartEnqueue(ctx context.Context, start goncordia.EnqueueStart) (context.Context, func(goncordia.EnqueueFinish)) {
	o.start = start
	return ctx, func(finish goncordia.EnqueueFinish) { o.finish = finish }
}

func TestClientObserverReceivesEnqueueOutcome(t *testing.T) {
	d := memory.New()
	observer := &recordingClientObserver{}
	client := goncordia.NewClient(d, goncordia.ClientConfig{Observer: observer})
	if _, err := client.Enqueue(context.Background(), EmailArgs{To: "observed"}, nil); err != nil {
		t.Fatal(err)
	}
	if observer.start.Driver != "memory" || observer.start.Count != 1 || observer.start.Queue != "default" ||
		observer.start.Kind != "send_email" || observer.start.Transactional || observer.finish.Inserted != 1 || observer.finish.Err != nil {
		t.Fatalf("start=%+v finish=%+v", observer.start, observer.finish)
	}
}

func TestEnqueueBatchUsesPerItemOptions(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	results, err := client.EnqueueBatch(context.Background(), []goncordia.InsertRequest{
		{Args: EmailArgs{To: "first"}, Opts: &core.InsertOpts{Queue: "critical", PipelineID: "account-1"}},
		{Args: EmailArgs{To: "second"}, Opts: &core.InsertOpts{Priority: 9}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].Job.Queue != "critical" || results[0].Job.PipelineID != "account-1" || results[1].Job.Priority != 9 {
		t.Fatalf("unexpected batch results: %+v", results)
	}
}

func TestUniquePeriodUsesInjectedClientClock(t *testing.T) {
	manual := clock.NewManual(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(manual))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: manual})
	opts := &core.InsertOpts{UniqueOpts: &core.UniqueOpts{ByPeriod: time.Hour}}
	first, err := client.Enqueue(context.Background(), EmailArgs{To: "period"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := client.Enqueue(context.Background(), EmailArgs{To: "period"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	manual.Advance(time.Hour)
	nextWindow, err := client.Enqueue(context.Background(), EmailArgs{To: "period"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if first.UniqueSkip || !duplicate.UniqueSkip || nextWindow.UniqueSkip {
		t.Fatalf("unexpected unique-window results: first=%+v duplicate=%+v next=%+v", first, duplicate, nextWindow)
	}
}

func TestUniqueScopeAndBoundedKey(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	ctx := context.Background()
	large := strings.Repeat("x", 10_000)
	global := &core.InsertOpts{Queue: "one", UniqueOpts: &core.UniqueOpts{ByArgs: true, Key: large}}
	first, err := client.Enqueue(ctx, EmailArgs{To: large}, global)
	if err != nil {
		t.Fatal(err)
	}
	global.Queue = "two"
	duplicate, err := client.Enqueue(ctx, EmailArgs{To: large}, global)
	if err != nil {
		t.Fatal(err)
	}
	if first.UniqueSkip || !duplicate.UniqueSkip || len(first.Job.UniqueKey) != 67 || !strings.HasPrefix(first.Job.UniqueKey, "u2_") {
		t.Fatalf("global/bounded unique results: first=%+v duplicate=%+v", first, duplicate)
	}

	queueScoped := &core.InsertOpts{Queue: "one", UniqueOpts: &core.UniqueOpts{ByArgs: true, ByQueue: true}}
	q1, err := client.Enqueue(ctx, EmailArgs{To: "queue-scoped"}, queueScoped)
	if err != nil {
		t.Fatal(err)
	}
	queueScoped.Queue = "two"
	q2, err := client.Enqueue(ctx, EmailArgs{To: "queue-scoped"}, queueScoped)
	if err != nil {
		t.Fatal(err)
	}
	if q1.UniqueSkip || q2.UniqueSkip || q1.Job.UniqueKey == q2.Job.UniqueKey {
		t.Fatalf("queue-scoped unique results: q1=%+v q2=%+v", q1, q2)
	}
}

func TestPermanentUniqueKeySurvivesFinalization(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	ctx := context.Background()
	opts := &core.InsertOpts{UniqueOpts: &core.UniqueOpts{Key: "periodic:daily-report:2026-08-16", Forever: true}}

	first, err := client.Enqueue(ctx, EmailArgs{To: "report"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(first.Job.UniqueKey, "uf2_") || len(first.Job.UniqueKey) != 68 {
		t.Fatalf("unexpected permanent key %q", first.Job.UniqueKey)
	}
	claimed, err := d.Executor().JobFetchBatch(ctx, driver.FetchParams{Queue: "default", Limit: 1, WorkerID: "worker"})
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claim: jobs=%+v err=%v", claimed, err)
	}
	if err := d.Executor().JobSetStateIfRunning(ctx, driver.JobSetStateParams{
		ID: first.Job.ID, State: driver.JobStateCompleted,
		ExpectedWorkerID: claimed[0].WorkerID, ExpectedAttempt: claimed[0].AttemptNum,
	}); err != nil {
		t.Fatal(err)
	}
	duplicate, err := client.Enqueue(ctx, EmailArgs{To: "report"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.UniqueSkip {
		t.Fatal("permanent unique key was released on completion")
	}
	if err := d.Executor().JobDelete(ctx, first.Job.ID); err != nil {
		t.Fatal(err)
	}
	afterDelete, err := client.Enqueue(ctx, EmailArgs{To: "report"}, opts)
	if err != nil || afterDelete.UniqueSkip {
		t.Fatalf("explicit delete did not release key: result=%+v err=%v", afterDelete, err)
	}
}

func TestEnqueueRejectsTypedNilArgs(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	var args *EmailArgs
	if _, err := client.Enqueue(context.Background(), args, nil); err == nil {
		t.Fatal("expected typed nil args to be rejected")
	}
}
