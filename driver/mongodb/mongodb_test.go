package mongodriver_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	tcmongo "github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver/drivertest"
	mongodriver "github.com/kirimatt/goncordia/driver/mongodb"
)

var mongoURI string

func TestMain(m *testing.M) {
	ctx := context.Background()
	code := 0
	defer func() { os.Exit(code) }()

	ctr, err := tcmongo.Run(ctx, "mongo:8.0",
		tcmongo.WithReplicaSet("rs0"),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "start mongo container: %v\n", err)
		code = 1
		return
	}
	defer ctr.Terminate(ctx) //nolint:errcheck

	uri, err := ctr.ConnectionString(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mongo connection string: %v\n", err)
		code = 1
		return
	}
	// The module configures the replica set with the container's internal IP,
	// so topology discovery from outside Docker fails. directConnection bypasses this.
	if strings.Contains(uri, "?") {
		mongoURI = uri + "&directConnection=true"
	} else {
		mongoURI = uri + "?directConnection=true"
	}

	code = m.Run()
}

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func (EmailJob) Kind() string { return "email" }

func newDriver(t *testing.T, opts ...mongodriver.Option) (*mongodriver.Driver, *mongo.Client) {
	t.Helper()
	ctx := context.Background()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		t.Fatalf("connect mongo: %v", err)
	}
	t.Cleanup(func() { client.Disconnect(ctx) }) //nolint:errcheck

	dbName := fmt.Sprintf("goncordia_test_%d", time.Now().UnixNano())
	d, err := mongodriver.New(ctx, client, dbName, opts...)
	if err != nil {
		t.Fatalf("new driver: %v", err)
	}
	if err := d.Migrate(ctx); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	t.Cleanup(func() { client.Database(dbName).Drop(ctx) }) //nolint:errcheck
	return d, client
}

func TestMongo_EnqueueAndProcess(t *testing.T) {
	d, _ := newDriver(t)
	drivertest.Run(t, d.Executor())
	ctx := context.Background()

	var processed atomic.Int64
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		processed.Add(1)
		return nil
	}), core.WorkerOpts{Queue: "default", MaxRetry: 3})

	client := mongodriver.NewClient(d, goncordia.ClientConfig{})
	for i := 0; i < 5; i++ {
		if _, err := client.Enqueue(ctx, EmailJob{To: "a@b.com", Subject: "hi"}, nil); err != nil {
			t.Fatal(err)
		}
	}

	wp := mongodriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  5,
		PollInterval: 100 * time.Millisecond,
	})
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go wp.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(20 * time.Second)
	for processed.Load() < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: processed %d/5", processed.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
	wp.Stop()
}

func TestMongo_EnqueueTx(t *testing.T) {
	d, mongoClient := newDriver(t)
	ctx := context.Background()
	client := mongodriver.NewClient(d, goncordia.ClientConfig{})

	// Commit path: job must appear.
	err := mongoClient.UseSession(ctx, func(sc mongo.SessionContext) error {
		if err := sc.StartTransaction(); err != nil {
			return err
		}
		result, err := client.EnqueueTx(sc, sc, EmailJob{To: "commit@test.com"}, nil)
		if err != nil {
			sc.AbortTransaction(sc) //nolint:errcheck
			return err
		}
		if result.Job == nil {
			sc.AbortTransaction(sc) //nolint:errcheck
			return errors.New("expected non-nil job")
		}
		return sc.CommitTransaction(sc)
	})
	if err != nil {
		t.Fatal(err)
	}

	rows, err := d.Executor().JobFetchBatch(ctx, mongodriver.FetchParams("default", 10))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected 1 job after commit, got %d", len(rows))
	}

	// Rollback path: job must not appear.
	err = mongoClient.UseSession(ctx, func(sc mongo.SessionContext) error {
		if err := sc.StartTransaction(); err != nil {
			return err
		}
		if _, err := client.EnqueueTx(sc, sc, EmailJob{To: "rollback@test.com"}, nil); err != nil {
			sc.AbortTransaction(sc) //nolint:errcheck
			return err
		}
		return sc.AbortTransaction(sc)
	})
	if err != nil {
		t.Fatal(err)
	}

	rows2, err := d.Executor().JobFetchBatch(ctx, mongodriver.FetchParams("default", 10))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows2) != 0 {
		t.Fatalf("expected 0 after rollback, got %d", len(rows2))
	}
}

func TestMongo_UniqueJobs(t *testing.T) {
	d, _ := newDriver(t)
	ctx := context.Background()
	client := mongodriver.NewClient(d, goncordia.ClientConfig{})
	opts := &core.InsertOpts{UniqueOpts: &core.UniqueOpts{ByArgs: true, ByQueue: true}}

	r1, err := client.Enqueue(ctx, EmailJob{To: "dup@test.com", Subject: "Hello"}, opts)
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if r1.UniqueSkip {
		t.Fatal("first insert should not be duplicate")
	}

	r2, err := client.Enqueue(ctx, EmailJob{To: "dup@test.com", Subject: "Hello"}, opts)
	if err != nil {
		t.Fatalf("second insert: %v", err)
	}
	if !r2.UniqueSkip {
		t.Fatal("expected second insert to be duplicate")
	}
}

func TestMongo_ScheduledJob(t *testing.T) {
	clk := clock.NewMock(time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC))
	d, _ := newDriver(t, mongodriver.WithClock(clk))
	ctx := context.Background()
	client := mongodriver.NewClient(d, goncordia.ClientConfig{})

	_, err := client.Enqueue(ctx, EmailJob{To: "scheduled@test.com"}, &core.InsertOpts{
		RunAt: clk.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}

	rows, _ := d.Executor().JobFetchBatch(ctx, mongodriver.FetchParams("default", 10))
	if len(rows) != 0 {
		t.Fatalf("expected 0 before RunAt, got %d", len(rows))
	}

	clk.Advance(2 * time.Hour)
	rows, _ = d.Executor().JobFetchBatch(ctx, mongodriver.FetchParams("default", 10))
	if len(rows) != 1 {
		t.Fatalf("expected 1 after RunAt, got %d", len(rows))
	}
}

func TestMongo_RetryAndDiscard(t *testing.T) {
	clk := clock.NewMock(time.Now())
	d, _ := newDriver(t, mongodriver.WithClock(clk))
	ctx := context.Background()

	var attempts atomic.Int64
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		attempts.Add(1)
		return errors.New("always fails")
	}), core.WorkerOpts{Queue: "default", MaxRetry: 2})

	client := mongodriver.NewClient(d, goncordia.ClientConfig{})
	if _, err := client.Enqueue(ctx, EmailJob{To: "fail@test.com"}, nil); err != nil {
		t.Fatal(err)
	}

	wp := mongodriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  1,
		PollInterval: 100 * time.Millisecond,
		RetryPolicy:  core.FixedRetry{Delay: 100 * time.Millisecond},
		Clock:        clk,
	})
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go wp.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(30 * time.Second)
	for attempts.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d attempts", attempts.Load())
		}
		clk.Advance(200 * time.Millisecond)
		time.Sleep(100 * time.Millisecond)
	}
	wp.Stop()
}

func TestMongo_QueuePauseResume(t *testing.T) {
	d, _ := newDriver(t)
	ctx := context.Background()
	client := mongodriver.NewClient(d, goncordia.ClientConfig{})

	for i := 0; i < 3; i++ {
		if _, err := client.Enqueue(ctx, EmailJob{To: "pause@test.com"}, nil); err != nil {
			t.Fatal(err)
		}
	}

	exec := d.Executor()
	if err := exec.QueuePause(ctx, "default"); err != nil {
		t.Fatal(err)
	}

	rows, _ := exec.JobFetchBatch(ctx, mongodriver.FetchParams("default", 10))
	if len(rows) != 0 {
		t.Errorf("expected 0 jobs from paused queue, got %d", len(rows))
	}

	if err := exec.QueueResume(ctx, "default"); err != nil {
		t.Fatal(err)
	}

	rows, _ = exec.JobFetchBatch(ctx, mongodriver.FetchParams("default", 10))
	if len(rows) != 3 {
		t.Errorf("expected 3 jobs after resume, got %d", len(rows))
	}
}
