package dynamodbdriver_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	goncordia "github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver/drivertest"
	dynamodbdriver "github.com/kirimatt/goncordia/driver/dynamodb"
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

func newTestClient(t *testing.T) (*dynamodb.Client, func()) {
	t.Helper()
	skipIfNoDocker(t)

	ctx := context.Background()
	ctr, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "amazon/dynamodb-local:latest",
			ExposedPorts: []string{"8000/tcp"},
			// ForHTTP waits until DynamoDB local responds at HTTP level (returns any status).
			WaitingFor: wait.ForHTTP("/").
				WithPort("8000/tcp").
				WithStatusCodeMatcher(func(code int) bool { return code >= 100 }),
		},
		Started: true,
	})
	if err != nil {
		t.Skipf("dynamodb-local not available: %v", err)
		return nil, nil
	}

	addr, err := ctr.Endpoint(ctx, "")
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		t.Fatal(err)
	}

	endpoint := fmt.Sprintf("http://%s", addr)
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
	)
	if err != nil {
		ctr.Terminate(ctx) //nolint:errcheck
		t.Fatal(err)
	}
	svc := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.EndpointDiscovery = dynamodb.EndpointDiscoveryOptions{
			EnableEndpointDiscovery: aws.EndpointDiscoveryDisabled,
		}
	})

	return svc, func() { ctr.Terminate(ctx) } //nolint:errcheck
}

func TestDynamoDB_EnqueueAndProcess(t *testing.T) {
	svc, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	d := dynamodbdriver.New(svc)
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}
	drivertest.Run(t, d.Executor())

	registry := core.NewRegistry()
	var processed atomic.Int64
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		processed.Add(1)
		return nil
	}), core.WorkerOpts{Queue: "default"})

	client := dynamodbdriver.NewClient(d, goncordia.ClientConfig{})
	if _, err := client.Enqueue(ctx, EmailJob{To: "test@example.com"}, nil); err != nil {
		t.Fatal(err)
	}

	pool := dynamodbdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  1,
		PollInterval: 50 * time.Millisecond,
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go pool.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(15 * time.Second)
	for processed.Load() < 1 {
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for job to be processed")
		}
		time.Sleep(50 * time.Millisecond)
	}
	pool.Stop()
}

func TestDynamoDB_ScheduledConformance(t *testing.T) {
	svc, cleanup := newTestClient(t)
	defer cleanup()

	clk := clock.NewManual(time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC))
	d := dynamodbdriver.New(svc, dynamodbdriver.WithClock(clk))
	if err := d.Migrate(context.Background()); err != nil {
		t.Fatal(err)
	}
	drivertest.RunScheduled(t, d.Executor(), clk)
}

func TestDynamoDB_UniqueJobs(t *testing.T) {
	svc, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	d := dynamodbdriver.New(svc)
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	client := dynamodbdriver.NewClient(d, goncordia.ClientConfig{})
	opts := &core.InsertOpts{UniqueOpts: &core.UniqueOpts{ByArgs: true, ByQueue: true}}

	r1, err := client.Enqueue(ctx, EmailJob{To: "dup@test.com", Subject: "Hello"}, opts)
	if err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if r1.UniqueSkip {
		t.Fatal("first insert should not be a duplicate")
	}

	r2, err := client.Enqueue(ctx, EmailJob{To: "dup@test.com", Subject: "Hello"}, opts)
	if err != nil {
		t.Fatalf("second insert: %v", err)
	}
	if !r2.UniqueSkip {
		t.Fatal("expected second insert to be a duplicate")
	}
}

func TestDynamoDB_RetryAndDiscard(t *testing.T) {
	svc, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	d := dynamodbdriver.New(svc)
	if err := d.Migrate(ctx); err != nil {
		t.Fatal(err)
	}

	var attempts atomic.Int64
	registry := core.NewRegistry()
	core.RegisterWorker(registry, core.WorkerFunc[EmailJob](func(_ context.Context, _ *core.Job[EmailJob]) error {
		attempts.Add(1)
		return errors.New("always fails")
	}), core.WorkerOpts{Queue: "default", MaxRetry: 3})

	client := dynamodbdriver.NewClient(d, goncordia.ClientConfig{})
	if _, err := client.Enqueue(ctx, EmailJob{To: "fail@test.com"}, nil); err != nil {
		t.Fatal(err)
	}

	pool := dynamodbdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues:       []string{"default"},
		Concurrency:  1,
		PollInterval: 50 * time.Millisecond,
		RetryPolicy:  core.FixedRetry{Delay: 100 * time.Millisecond},
	})

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go pool.Start(runCtx) //nolint:errcheck

	deadline := time.Now().Add(15 * time.Second)
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
