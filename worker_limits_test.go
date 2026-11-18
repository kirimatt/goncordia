package goncordia_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

func TestValidateWorkerConfigReportsEveryProblem(t *testing.T) {
	limited := &limitedCapabilityDriver{Driver: memory.New()}
	cfg := goncordia.WorkerConfig{
		Concurrency: -1, MaxPending: -1,
		Queues: []string{"a"},
		QueuePolicies: map[string]goncordia.QueuePolicy{
			"a": {
				Weight: -1,
				RateLimits: []goncordia.QueueRateLimit{
					{Limit: -1},
					{Limit: 1, Burst: 2},
					{Limit: 1, Scope: 9, Key: 9, Mode: 9},
					{Limit: 5000, Period: time.Second, Mode: goncordia.RateLimitModeFixedWindow},
					{Limit: 1, Scope: goncordia.RateLimitScopeGlobal},
				},
				KeyConcurrency: []goncordia.KeyConcurrencyLimit{
					{Key: goncordia.RateLimitKeyQueue, Limit: 0},
				},
			},
			"unlisted": {},
		},
		DistributedPipelines:        true,
		RequireLifecyclePersistence: true,
		RequireStrictOrdering:       true,
	}
	err := goncordia.ValidateWorkerConfig[memory.NoTx](limited, cfg)
	if err == nil {
		t.Fatal("invalid config accepted")
	}
	for _, want := range []string{
		"Concurrency must not be negative",
		"MaxPending must not be negative",
		`QueuePolicies["unlisted"] is not listed in Queues`,
		`QueuePolicies["a"] contains a negative limit`,
		"RateLimits[0] Limit must not be negative",
		"RateLimits[1] Burst must not exceed Limit",
		"RateLimits[2] has an unknown Scope",
		"RateLimits[2] has an unknown Key",
		"RateLimits[2] has an unknown Mode",
		"RateLimits[3] fixed-window Limit must be smaller than Period milliseconds",
		"RateLimits[4] requires linearizable CAS",
		"KeyConcurrency[0] Limit must be positive",
		"KeyConcurrency[0] Key must be kind, pipeline, or tag",
		"DistributedPipelines requires linearizable leases",
		"RequireLifecyclePersistence is unsupported",
		"RequireStrictOrdering is unsupported",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error lacks %q:\n%v", want, err)
		}
	}
	if err := goncordia.ValidateWorkerConfig(memory.New(), goncordia.WorkerConfig{Concurrency: 4, MaxPending: 2}); err == nil ||
		!strings.Contains(err.Error(), "MaxPending must be at least Concurrency") {
		t.Fatalf("MaxPending below Concurrency: %v", err)
	}
	if err := goncordia.ValidateWorkerConfig(memory.New(), goncordia.WorkerConfig{}); err != nil {
		t.Fatalf("zero config rejected: %v", err)
	}
	pool := goncordia.NewWorkerPool[memory.NoTx](limited, core.NewRegistry(), cfg)
	if err := pool.Start(context.Background()); err == nil {
		t.Fatal("Start accepted an invalid config")
	}
}

// startRecorder registers a handler that reports each start by pipeline ID
// and blocks until release is closed.
func startRecorder(registry *core.Registry, started chan<- string, release <-chan struct{}) {
	core.RegisterWorker(registry, core.WorkerFunc[featureArgs](func(_ context.Context, job *core.Job[featureArgs]) error {
		started <- job.PipelineID
		<-release
		return nil
	}), core.WorkerOpts{})
}

// expectStarts receives exactly n starts and checks that no further job
// starts right after them.
func expectStarts(t *testing.T, started <-chan string, n int, label string) []string {
	t.Helper()
	var got []string
	for range n {
		select {
		case id := <-started:
			got = append(got, id)
		case <-time.After(2 * time.Second):
			t.Fatalf("%s: %d of %d jobs started", label, len(got), n)
		}
	}
	select {
	case extra := <-started:
		t.Fatalf("%s: unexpected extra start %q", label, extra)
	case <-time.After(30 * time.Millisecond):
	}
	return got
}

func TestFixedWindowRateLimitResetsAtWindowBoundary(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 11, 18, 12, 0, 10, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
	registry := core.NewRegistry()
	started := make(chan string, 8)
	release := make(chan struct{})
	startRecorder(registry, started, release)
	for i := range 5 {
		if _, err := client.Enqueue(context.Background(), featureArgs{N: i}, nil); err != nil {
			t.Fatal(err)
		}
	}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Concurrency: 5, MaxPending: 5, PollInterval: time.Hour, Clock: clk,
		QueuePolicies: map[string]goncordia.QueuePolicy{"default": {RateLimits: []goncordia.QueueRateLimit{{
			Limit: 2, Period: time.Minute, Mode: goncordia.RateLimitModeFixedWindow,
		}}}},
	})
	go pool.Start(context.Background()) //nolint:errcheck
	expectStarts(t, started, 2, "first window")
	clk.Advance(50 * time.Second) // 12:01:00 opens the next window
	expectStarts(t, started, 2, "second window")
	clk.Advance(time.Minute)
	expectStarts(t, started, 1, "third window")
	close(release)
	_ = pool.Shutdown(context.Background())
}

func TestPipelineRateLimitHasIndependentBudgets(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
	registry := core.NewRegistry()
	started := make(chan string, 8)
	// Jobs of one pipeline run one at a time anyway, so handlers return at
	// once and only the rate limit holds back the second p1 job.
	release := make(chan struct{})
	close(release)
	startRecorder(registry, started, release)
	for i, pipeline := range []string{"p1", "p1", "p2"} {
		if _, err := client.Enqueue(context.Background(), featureArgs{N: i}, &core.InsertOpts{PipelineID: pipeline}); err != nil {
			t.Fatal(err)
		}
	}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Concurrency: 3, MaxPending: 3, PollInterval: time.Hour, Clock: clk,
		QueuePolicies: map[string]goncordia.QueuePolicy{"default": {RateLimits: []goncordia.QueueRateLimit{{
			Limit: 1, Period: time.Minute, Key: goncordia.RateLimitKeyPipeline,
		}}}},
	})
	go pool.Start(context.Background()) //nolint:errcheck
	got := expectStarts(t, started, 2, "one start per pipeline")
	if got[0] == got[1] {
		t.Fatalf("one pipeline started twice: %v", got)
	}
	clk.Advance(time.Minute)
	if got := expectStarts(t, started, 1, "after refill"); got[0] != "p1" {
		t.Fatalf("refilled start = %v, want p1", got)
	}
	_ = pool.Shutdown(context.Background())
}

// TestKindConcurrencyWaiterYieldsOnShutdown checks that a claimed job waiting
// for a kind slot goes back to the queue when the pool shuts down.
func TestKindConcurrencyWaiterYieldsOnShutdown(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	registry := core.NewRegistry()
	started := make(chan string, 4)
	release := make(chan struct{})
	startRecorder(registry, started, release)
	for i := range 2 {
		if _, err := client.Enqueue(context.Background(), featureArgs{N: i}, nil); err != nil {
			t.Fatal(err)
		}
	}
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Concurrency: 2, MaxPending: 2, PollInterval: time.Hour,
		QueuePolicies: map[string]goncordia.QueuePolicy{"default": {KeyConcurrency: []goncordia.KeyConcurrencyLimit{{
			Key: goncordia.RateLimitKeyKind, Limit: 1,
		}}}},
	})
	go pool.Start(context.Background()) //nolint:errcheck
	expectStarts(t, started, 1, "one job per kind")
	waitForCondition(t, 2*time.Second, func() bool {
		running := 0
		for _, job := range d.AllJobs() {
			if job.State == driver.JobStateRunning {
				running++
			}
		}
		return running == 2
	}, "second job claimed and waiting for the kind slot")

	// An expired context makes Shutdown close the pool and return at once;
	// the waiting job leaves its slot queue and yields.
	expired, cancel := context.WithCancel(context.Background())
	cancel()
	_ = pool.Shutdown(expired)
	waitForCondition(t, 2*time.Second, func() bool {
		for _, job := range d.AllJobs() {
			if job.State == driver.JobStateAvailable {
				return true
			}
		}
		return false
	}, "waiting job yielded")
	close(release)
	if err := pool.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	counts := map[driver.JobState]int{}
	for _, job := range d.AllJobs() {
		counts[job.State]++
	}
	if counts[driver.JobStateCompleted] != 1 || counts[driver.JobStateAvailable] != 1 {
		t.Fatalf("states after shutdown = %v; want one completed, one available", counts)
	}
}
