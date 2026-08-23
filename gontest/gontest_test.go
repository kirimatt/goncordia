package gontest_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	goncordia "github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver"
	"github.com/kirimatt/goncordia/gontest"
)

// --- test job types ---

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func (EmailJob) Kind() string { return "email" }

type SMSJob struct {
	Phone string `json:"phone"`
}

func (SMSJob) Kind() string { return "sms" }

// --- Tracker / client helpers ---

func TestRequireEnqueued_exact(t *testing.T) {
	ctx := context.Background()
	client, tracker := gontest.NewClient(t)

	gontest.MustEnqueue[EmailJob](t, ctx, client, EmailJob{To: "a@b.com"}, nil)
	gontest.MustEnqueue[EmailJob](t, ctx, client, EmailJob{To: "c@d.com"}, nil)

	jobs := gontest.RequireEnqueued[EmailJob](t, tracker, 2)
	if jobs[0].Args.To == "" || jobs[1].Args.To == "" {
		t.Error("args not deserialized")
	}
}

func TestRequireNoEnqueued_passes(t *testing.T) {
	_, tracker := gontest.NewClient(t)
	gontest.RequireNoEnqueued[EmailJob](t, tracker)
}

func TestRequireEnqueued_wrongKindNotCounted(t *testing.T) {
	ctx := context.Background()
	client, tracker := gontest.NewClient(t)

	gontest.MustEnqueue[SMSJob](t, ctx, client, SMSJob{Phone: "+1"}, nil)

	gontest.RequireNoEnqueued[EmailJob](t, tracker)
	gontest.RequireEnqueued[SMSJob](t, tracker, 1)
}

func TestJobs_emptyWhenNoneEnqueued(t *testing.T) {
	_, tracker := gontest.NewClient(t)
	if got := gontest.Jobs[EmailJob](tracker); len(got) != 0 {
		t.Errorf("expected 0 jobs, got %d", len(got))
	}
}

func TestJobs_returnsTypedArgs(t *testing.T) {
	ctx := context.Background()
	client, tracker := gontest.NewClient(t)

	gontest.MustEnqueue[EmailJob](t, ctx, client, EmailJob{To: "x@y.com", Subject: "Hi"}, nil)

	jobs := gontest.Jobs[EmailJob](tracker)
	if len(jobs) != 1 {
		t.Fatalf("want 1 job, got %d", len(jobs))
	}
	if jobs[0].Args.Subject != "Hi" {
		t.Errorf("unexpected subject: %s", jobs[0].Args.Subject)
	}
}

func TestJobsE_reportsCorruptPayload(t *testing.T) {
	ctx := context.Background()
	_, tracker := gontest.NewClient(t)
	_, err := tracker.Driver().Executor().JobInsertMany(ctx, []driver.JobInsertParams{{
		Queue: "default", Kind: "email", Args: []byte(`{"to":`),
	}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := gontest.JobsE[EmailJob](tracker); err == nil {
		t.Fatal("JobsE accepted corrupt payload")
	}
}

func TestJobsE_unwrapsVersionedPayload(t *testing.T) {
	ctx := context.Background()
	_, tracker := gontest.NewClient(t)
	payload, err := json.Marshal(EmailJob{To: "versioned@test.com", Subject: "v2"})
	if err != nil {
		t.Fatal(err)
	}
	payload, err = core.EncodePayload(payload, 2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tracker.Driver().Executor().JobInsertMany(ctx, []driver.JobInsertParams{{
		Queue: "default", Kind: "email", Args: payload,
	}})
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := gontest.JobsE[EmailJob](tracker)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 1 || jobs[0].Args.To != "versioned@test.com" || jobs[0].PayloadVersion != 2 {
		t.Fatalf("unexpected jobs: %s", gontest.FormatJobList(jobs))
	}
}

// --- WorkerHelper ---

func TestWorkerHelper_success(t *testing.T) {
	ctx := context.Background()
	var called bool
	h := gontest.NewWorkerHelper[EmailJob](core.WorkerFunc[EmailJob](func(_ context.Context, job *core.Job[EmailJob]) error {
		called = true
		if job.Args.To != "test@example.com" {
			return errors.New("wrong email")
		}
		return nil
	}))
	if err := h.Work(ctx, EmailJob{To: "test@example.com"}); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Error("worker was not called")
	}
}

func TestWorkerHelper_error(t *testing.T) {
	ctx := context.Background()
	h := gontest.NewWorkerHelper[EmailJob](core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		return errors.New("smtp down")
	}))
	if err := h.Work(ctx, EmailJob{}); err == nil {
		t.Error("expected error")
	}
}

func TestWorkerHelper_workJob(t *testing.T) {
	ctx := context.Background()
	var gotAttempt int
	h := gontest.NewWorkerHelper[EmailJob](core.WorkerFunc[EmailJob](func(_ context.Context, job *core.Job[EmailJob]) error {
		gotAttempt = job.AttemptNum
		return nil
	}))
	if err := h.WorkJob(ctx, &core.Job[EmailJob]{Args: EmailJob{}, AttemptNum: 3, MaxRetry: 5}); err != nil {
		t.Fatal(err)
	}
	if gotAttempt != 3 {
		t.Errorf("want AttemptNum=3, got %d", gotAttempt)
	}
}

func TestWorkerFuncHelper(t *testing.T) {
	ctx := context.Background()
	h := gontest.WorkerFuncHelper[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		return nil
	})
	if err := h.Work(ctx, EmailJob{}); err != nil {
		t.Fatal(err)
	}
}

func TestRequireWork(t *testing.T) {
	ctx := context.Background()
	w := core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error { return nil })
	gontest.RequireWork(t, ctx, w, EmailJob{To: "a@b.com"})
}

// --- MockClock ---

func TestMockClock_advance(t *testing.T) {
	clk := gontest.NewMockClock()
	start := clk.Now()
	clk.Advance(24 * time.Hour)
	if clk.Now().Sub(start) != 24*time.Hour {
		t.Errorf("expected 24h advance")
	}
}

func TestNewClientWithClock_scheduledJob(t *testing.T) {
	ctx := context.Background()
	clk := gontest.NewMockClock()
	client, tracker := gontest.NewClientWithClock(t, clk)

	future := clk.Now().Add(time.Hour)
	gontest.MustEnqueue[EmailJob](t, ctx, client, EmailJob{To: "sched@test.com"}, &core.InsertOpts{RunAt: future})

	// Job is enqueued but not available yet — still appears in store.
	gontest.RequireEnqueued[EmailJob](t, tracker, 1)

	// Advance past scheduled time.
	clk.Advance(2 * time.Hour)

	// Job is now available for processing.
	registry := core.NewRegistry()
	var processed atomic.Int64
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		processed.Add(1)
		return nil
	}), core.WorkerOpts{Queue: "default"})

	pool := tracker.NewWorkerPool(registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  1,
		PollInterval: 5 * time.Millisecond,
		Clock:        clk,
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go pool.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(5 * time.Second)
	for processed.Load() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("timeout: scheduled job was not processed")
		}
		time.Sleep(10 * time.Millisecond)
	}
	pool.Stop()
}

// --- Tracker.NewWorkerPool integration ---

func TestTrackerNewWorkerPool_endToEnd(t *testing.T) {
	ctx := context.Background()
	client, tracker := gontest.NewClient(t)

	registry := core.NewRegistry()
	var processed atomic.Int64
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		processed.Add(1)
		return nil
	}), core.WorkerOpts{Queue: "default"})

	gontest.MustEnqueue[EmailJob](t, ctx, client, EmailJob{To: "pool@test.com"}, nil)

	pool := tracker.NewWorkerPool(registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  1,
		PollInterval: 5 * time.Millisecond,
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go pool.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(5 * time.Second)
	for processed.Load() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("timeout")
		}
		time.Sleep(10 * time.Millisecond)
	}
	pool.Stop()
}
