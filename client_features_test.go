package goncordia_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver/memory"
)

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

func TestEnqueueRejectsTypedNilArgs(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	var args *EmailArgs
	if _, err := client.Enqueue(context.Background(), args, nil); err == nil {
		t.Fatal("expected typed nil args to be rejected")
	}
}
