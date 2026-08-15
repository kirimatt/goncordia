package cassandradriver_test

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gocql/gocql"
	tccassandra "github.com/testcontainers/testcontainers-go/modules/cassandra"

	"github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/core"
	cassandradriver "github.com/kirimatt/goncordia/driver/cassandra"
	"github.com/kirimatt/goncordia/driver/drivertest"
)

func skipIfNoDocker(t *testing.T) {
	t.Helper()
	if os.Getenv("SKIP_INTEGRATION") != "" {
		t.Skip("SKIP_INTEGRATION set")
	}
}

func newTestSession(t *testing.T) (*gocql.Session, func()) {
	t.Helper()
	ctx := context.Background()

	ctr, err := tccassandra.Run(ctx, "cassandra:4.1")
	if err != nil {
		t.Fatalf("start cassandra container: %v", err)
	}

	host, err := ctr.ConnectionHost(ctx)
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		t.Fatalf("get connection host: %v", err)
	}

	cluster := gocql.NewCluster(host)
	cluster.Timeout = 15 * time.Second
	cluster.ConnectTimeout = 15 * time.Second
	cluster.Consistency = gocql.Quorum

	// Create keyspace.
	sysSession, err := cluster.CreateSession()
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		t.Fatalf("connect to cassandra: %v", err)
	}
	if err := sysSession.Query(
		`CREATE KEYSPACE IF NOT EXISTS goncordia_test
		 WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`,
	).Exec(); err != nil {
		sysSession.Close()
		ctr.Terminate(ctx) //nolint:errcheck
		t.Fatalf("create keyspace: %v", err)
	}
	sysSession.Close()

	cluster.Keyspace = "goncordia_test"
	session, err := cluster.CreateSession()
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		t.Fatalf("connect with keyspace: %v", err)
	}

	return session, func() {
		session.Close()
		ctr.Terminate(ctx) //nolint:errcheck
	}
}

type EmailJob struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func (EmailJob) Kind() string { return "email" }

// --- tests ---

func TestCassandra_EnqueueAndProcess(t *testing.T) {
	skipIfNoDocker(t)
	session, cleanup := newTestSession(t)
	defer cleanup()

	ctx := context.Background()
	d := cassandradriver.New(session)
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	drivertest.Run(t, d.Executor())

	var processed atomic.Int64
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		processed.Add(1)
		return nil
	}), core.WorkerOpts{Queue: "default", MaxRetry: 3})

	client := cassandradriver.NewClient(d, goncordia.ClientConfig{})
	for i := 0; i < 5; i++ {
		if _, err := client.Enqueue(ctx, EmailJob{To: "a@b.com", Subject: "hi"}, nil); err != nil {
			t.Fatal(err)
		}
	}

	pool := cassandradriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  5,
		PollInterval: 100 * time.Millisecond,
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go pool.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(15 * time.Second)
	for processed.Load() < 5 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: processed %d/5", processed.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
	pool.Stop()
}

func TestCassandra_UniqueJobs(t *testing.T) {
	skipIfNoDocker(t)
	session, cleanup := newTestSession(t)
	defer cleanup()

	ctx := context.Background()
	d := cassandradriver.New(session)
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	client := cassandradriver.NewClient(d, goncordia.ClientConfig{})

	opts := &core.InsertOpts{UniqueOpts: &core.UniqueOpts{ByArgs: true, ByQueue: true}}

	r1, err := client.Enqueue(ctx, EmailJob{To: "dup@test.com", Subject: "Hello"}, opts)
	if err != nil {
		t.Fatalf("first insert failed: %v", err)
	}
	if r1.UniqueSkip {
		t.Fatal("first insert should not be a duplicate")
	}

	r2, err := client.Enqueue(ctx, EmailJob{To: "dup@test.com", Subject: "Hello"}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if !r2.UniqueSkip {
		t.Fatal("expected second insert to be a duplicate")
	}
}

func TestCassandra_RetryAndDiscard(t *testing.T) {
	skipIfNoDocker(t)
	session, cleanup := newTestSession(t)
	defer cleanup()

	ctx := context.Background()
	d := cassandradriver.New(session)
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	var attempts atomic.Int64
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		attempts.Add(1)
		return errors.New("always fails")
	}), core.WorkerOpts{Queue: "default", MaxRetry: 2})

	client := cassandradriver.NewClient(d, goncordia.ClientConfig{})
	if _, err := client.Enqueue(ctx, EmailJob{To: "fail@test.com"}, nil); err != nil {
		t.Fatal(err)
	}

	pool := cassandradriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  1,
		PollInterval: 50 * time.Millisecond,
		RetryPolicy:  core.FixedRetry{Delay: 100 * time.Millisecond},
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go pool.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(15 * time.Second)
	for attempts.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d attempts", attempts.Load())
		}
		time.Sleep(50 * time.Millisecond)
	}
	pool.Stop()
	if got := attempts.Load(); got < 2 {
		t.Errorf("expected >= 2 attempts, got %d", got)
	}
}
