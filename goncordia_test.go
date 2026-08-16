package goncordia_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver"
	memdriver "github.com/kirimatt/goncordia/driver/memory"
)

// --- test job types ---

type EmailArgs struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func (EmailArgs) Kind() string { return "send_email" }

type EmailWorker struct {
	processed atomic.Int64
}

func (w *EmailWorker) Process(_ context.Context, job *core.Job[EmailArgs]) error {
	w.processed.Add(1)
	return nil
}

// --- tests ---

func TestEnqueueAndProcess(t *testing.T) {
	d := memdriver.New()
	registry := core.NewRegistry()

	w := &EmailWorker{}
	core.RegisterWorker(registry, w, core.WorkerOpts{Queue: "default", MaxRetry: 3})

	client := goncordia.NewClient(d, goncordia.ClientConfig{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Enqueue 5 jobs
	for i := 0; i < 5; i++ {
		_, err := client.Enqueue(ctx, EmailArgs{To: "user@example.com", Subject: "Hello"}, nil)
		if err != nil {
			t.Fatalf("Enqueue failed: %v", err)
		}
	}

	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:      []string{"default"},
		Concurrency: 5,
	})

	// Run pool briefly
	runCtx, runCancel := context.WithTimeout(ctx, 2*time.Second)
	defer runCancel()

	done := make(chan struct{})
	go func() {
		pool.Start(runCtx) //nolint:errcheck
		close(done)
	}()

	// Poll until all jobs are processed or timeout
	deadline := time.Now().Add(3 * time.Second)
	for w.processed.Load() < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for jobs, processed: %d/5", w.processed.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}

	pool.Stop()
	<-done

	if got := w.processed.Load(); got != 5 {
		t.Errorf("expected 5 processed jobs, got %d", got)
	}
}

func TestMemoryEnqueueTxUnsupported(t *testing.T) {
	d := memdriver.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	ctx := context.Background()

	tx := memdriver.NoTx{}
	_, err := client.EnqueueTx(ctx, tx, EmailArgs{To: "a@b.com", Subject: "Tx test"}, nil)
	if !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("EnqueueTx error=%v, want ErrUnsupported", err)
	}
	if _, err := d.Executor().Begin(ctx); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("Begin error=%v, want ErrUnsupported", err)
	}
}

func TestUniqueJobs(t *testing.T) {
	d := memdriver.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	ctx := context.Background()

	opts := &core.InsertOpts{
		UniqueOpts: &core.UniqueOpts{ByArgs: true, ByQueue: true},
	}

	r1, err := client.Enqueue(ctx, EmailArgs{To: "dup@example.com", Subject: "Hello"}, opts)
	if err != nil {
		t.Fatalf("first enqueue failed: %v", err)
	}
	if r1.UniqueSkip {
		t.Fatal("first insert should not be a duplicate")
	}

	r2, err := client.Enqueue(ctx, EmailArgs{To: "dup@example.com", Subject: "Hello"}, opts)
	if err != nil {
		t.Fatalf("second enqueue failed: %v", err)
	}
	if !r2.UniqueSkip {
		t.Fatal("second insert with same args should be a duplicate")
	}
}

func TestQueuePauseResume(t *testing.T) {
	d := memdriver.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	registry := core.NewRegistry()

	w := &EmailWorker{}
	core.RegisterWorker(registry, w, core.WorkerOpts{Queue: "default", MaxRetry: 1})

	ctx := context.Background()

	// Pause before enqueueing
	exec := d.Executor()
	if err := exec.QueuePause(ctx, "default"); err != nil {
		t.Fatal(err)
	}

	_, err := client.Enqueue(ctx, EmailArgs{To: "x@y.com", Subject: "Paused"}, nil)
	if err != nil {
		t.Fatal(err)
	}

	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  2,
		PollInterval: 50 * time.Millisecond,
	})

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	go pool.Start(runCtx) //nolint:errcheck

	time.Sleep(200 * time.Millisecond)
	if w.processed.Load() != 0 {
		t.Fatal("expected 0 processed jobs while queue is paused")
	}

	// Resume
	if err := exec.QueueResume(ctx, "default"); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for w.processed.Load() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("job not processed after queue resume")
		}
		time.Sleep(10 * time.Millisecond)
	}
	pool.Stop()
}
