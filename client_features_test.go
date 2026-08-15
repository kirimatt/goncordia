package goncordia_test

import (
	"context"
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

func TestEnqueueRejectsTypedNilArgs(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	var args *EmailArgs
	if _, err := client.Enqueue(context.Background(), args, nil); err == nil {
		t.Fatal("expected typed nil args to be rejected")
	}
}
