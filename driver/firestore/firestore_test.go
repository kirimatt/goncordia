package firestoredriver_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/firestore"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	goncordia "github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver/drivertest"
	firestoredriver "github.com/kirimatt/goncordia/driver/firestore"
)

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func (EmailJob) Kind() string { return "email" }

func skipIfNoDocker(t *testing.T) {
	t.Helper()
	if os.Getenv("SKIP_DOCKER") != "" {
		t.Skip("SKIP_DOCKER set")
	}
}

func newTestClient(t *testing.T) (*firestore.Client, func()) {
	t.Helper()
	skipIfNoDocker(t)

	ctx := context.Background()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "gcr.io/google.com/cloudsdktool/cloud-sdk:emulators",
			ExposedPorts: []string{"8080/tcp"},
			Cmd: []string{
				"gcloud", "beta", "emulators", "firestore", "start",
				"--host-port=0.0.0.0:8080", "--project=test",
			},
			WaitingFor: wait.ForListeningPort("8080/tcp").WithStartupTimeout(120 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Skipf("firestore emulator not available: %v", err)
		return nil, nil
	}

	addr, err := ctr.Endpoint(ctx, "")
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		t.Fatal(err)
	}

	// Point the Firestore client at the emulator.
	t.Setenv("FIRESTORE_EMULATOR_HOST", addr)

	client, err := firestore.NewClient(ctx, "test")
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		t.Fatalf("create firestore client: %v", err)
	}

	return client, func() {
		client.Close()     //nolint:errcheck
		ctr.Terminate(ctx) //nolint:errcheck
	}
}

func TestFirestore_EnqueueAndProcess(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	d := firestoredriver.New(client)
	drivertest.Run(t, d.Executor())

	registry := core.NewRegistry()
	var processed atomic.Int64
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		processed.Add(1)
		return nil
	}), core.WorkerOpts{Queue: "default"})

	c := firestoredriver.NewClient(d, goncordia.ClientConfig{})
	if _, err := c.Enqueue(ctx, EmailJob{To: "test@example.com"}, nil); err != nil {
		t.Fatal(err)
	}

	pool := firestoredriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  1,
		PollInterval: 50 * time.Millisecond,
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go pool.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(30 * time.Second)
	for processed.Load() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for job to be processed")
		}
		time.Sleep(50 * time.Millisecond)
	}
	pool.Stop()
}

func TestFirestore_UniqueJobs(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	d := firestoredriver.New(client)

	c := firestoredriver.NewClient(d, goncordia.ClientConfig{})
	opts := &core.InsertOpts{UniqueOpts: &core.UniqueOpts{ByArgs: true, ByQueue: true}}

	r1, err := c.Enqueue(ctx, EmailJob{To: "dup@test.com", Subject: "Hello"}, opts)
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if r1.UniqueSkip {
		t.Fatal("first insert should not be a duplicate")
	}

	r2, err := c.Enqueue(ctx, EmailJob{To: "dup@test.com", Subject: "Hello"}, opts)
	if err != nil {
		t.Fatalf("second insert: %v", err)
	}
	if !r2.UniqueSkip {
		t.Fatal("expected second insert to be a duplicate")
	}
}

func TestFirestore_RetryAndDiscard(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	d := firestoredriver.New(client)

	var attempts atomic.Int64
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		attempts.Add(1)
		return errors.New("always fails")
	}), core.WorkerOpts{Queue: "default", MaxRetry: 3})

	c := firestoredriver.NewClient(d, goncordia.ClientConfig{})
	if _, err := c.Enqueue(ctx, EmailJob{To: "fail@test.com"}, nil); err != nil {
		t.Fatal(err)
	}

	pool := firestoredriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  1,
		PollInterval: 50 * time.Millisecond,
		RetryPolicy:  core.FixedRetry{Delay: 100 * time.Millisecond},
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go pool.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(30 * time.Second)
	for attempts.Load() < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d attempts", attempts.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
	pool.Stop()
	if got := attempts.Load(); got < 3 {
		t.Errorf("expected >= 3 attempts, got %d", got)
	}
}

func TestFirestore_EnqueueTx(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	d := firestoredriver.New(client)

	registry := core.NewRegistry()
	var processed atomic.Int64
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		processed.Add(1)
		return nil
	}), core.WorkerOpts{Queue: "default"})

	c := firestoredriver.NewClient(d, goncordia.ClientConfig{})

	// Enqueue inside a Firestore transaction.
	orderRef := client.Collection("orders").Doc("order-1")
	if err := client.RunTransaction(ctx, func(ctx context.Context, tx *firestore.Transaction) error {
		if err := tx.Create(orderRef, map[string]interface{}{
			"item": "widget", "qty": 1,
		}); err != nil {
			return fmt.Errorf("create order: %w", err)
		}
		_, err := c.EnqueueTx(ctx, tx, EmailJob{To: "tx@test.com"}, nil)
		return err
	}); err != nil {
		t.Fatalf("RunTransaction: %v", err)
	}

	pool := firestoredriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  1,
		PollInterval: 50 * time.Millisecond,
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go pool.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(30 * time.Second)
	for processed.Load() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for transactional job")
		}
		time.Sleep(50 * time.Millisecond)
	}
	pool.Stop()
}
