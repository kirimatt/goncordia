package future

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

type sumArgs struct {
	A, B int
}

func (sumArgs) Kind() string { return "sum" }

type sumResult struct {
	Total int `json:"total"`
}

type recorder struct {
	mu      sync.Mutex
	awaits  []AwaitStart
	results []AwaitFinish
	stores  []StoreStart
	stored  []StoreFinish
}

func (r *recorder) StartAwait(ctx context.Context, start AwaitStart) (context.Context, func(AwaitFinish)) {
	r.mu.Lock()
	r.awaits = append(r.awaits, start)
	r.mu.Unlock()
	return ctx, func(finish AwaitFinish) {
		r.mu.Lock()
		r.results = append(r.results, finish)
		r.mu.Unlock()
	}
}

func (r *recorder) StartStore(ctx context.Context, start StoreStart) (context.Context, func(StoreFinish)) {
	r.mu.Lock()
	r.stores = append(r.stores, start)
	r.mu.Unlock()
	return ctx, func(finish StoreFinish) {
		r.mu.Lock()
		r.stored = append(r.stored, finish)
		r.mu.Unlock()
	}
}

func (r *recorder) lastAwait(t *testing.T) AwaitFinish {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.results) == 0 {
		t.Fatal("observer saw no finished await")
	}
	return r.results[len(r.results)-1]
}

type fixture struct {
	clk    *clock.Manual
	d      *memory.Driver
	m      *Manager
	client *goncordia.Client[memory.NoTx]
	obs    *recorder
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	clk := clock.NewManual(time.Time{})
	d := memory.New(memory.WithClock(clk))
	obs := &recorder{}
	m, err := New(d, Config{Clock: clk, PollInterval: time.Second, Observer: obs})
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{clk: clk, d: d, m: m, client: goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk}), obs: obs}
}

// startPool runs a worker pool that uses the Manager as its observer, so
// completion reaches Await through the local signal.
func (fx *fixture) startPool(t *testing.T, registry *core.Registry, observer goncordia.WorkerObserver) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	wp := goncordia.NewWorkerPool(fx.d, registry, goncordia.WorkerConfig{
		Clock: fx.clk, Concurrency: 2, PollInterval: time.Hour, Observer: observer,
		RetryPolicy: core.NoRetry{},
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = wp.Start(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

// claim moves the only available job to running, as a worker would.
func (fx *fixture) claim(t *testing.T) driver.JobRow {
	t.Helper()
	rows, err := fx.d.Executor().JobFetchBatch(context.Background(), driver.FetchParams{
		Queue: "default", Limit: 1, WorkerID: "test-worker", LeaseDuration: time.Hour,
	})
	if err != nil || len(rows) != 1 {
		t.Fatalf("claim: rows=%d err=%v", len(rows), err)
	}
	return rows[0]
}

func (fx *fixture) finish(t *testing.T, id string, state driver.JobState, msg string) {
	t.Helper()
	params := driver.JobSetStateParams{ID: id, State: state, Attempt: 1}
	if msg != "" {
		params.Err = &msg
	}
	if err := fx.d.Executor().JobSetStateIfRunning(context.Background(), params); err != nil {
		t.Fatal(err)
	}
}

// waitForTicker blocks until an Await call has armed its poll ticker.
func (fx *fixture) waitForTicker(t *testing.T, id string, timers int) {
	t.Helper()
	for fx.m.waiting(id) == 0 || fx.clk.ActiveTimers() < timers {
		select {
		case <-t.Context().Done():
			t.Fatal("await never armed its poll ticker")
		default:
		}
		yield()
	}
}

func yield() { time.Sleep(0) }

type awaitResult[R any] struct {
	value R
	err   error
}

func awaitAsync[R any](ctx context.Context, f *Future[R]) <-chan awaitResult[R] {
	ch := make(chan awaitResult[R], 1)
	go func() {
		value, err := f.Await(ctx)
		ch <- awaitResult[R]{value, err}
	}()
	return ch
}

func TestNewValidatesConfig(t *testing.T) {
	if _, err := New[memory.NoTx](nil, Config{}); err == nil {
		t.Fatal("nil driver accepted")
	}
	if _, err := New[memory.NoTx](memory.New(), Config{PollInterval: -time.Second}); err == nil {
		t.Fatal("negative poll interval accepted")
	}
}

func TestExplicitStoreOverridesDriver(t *testing.T) {
	d := memory.New()
	other := memory.New().Executor()
	m, err := New(d, Config{Store: other})
	if err != nil {
		t.Fatal(err)
	}
	if m.store != other {
		t.Fatal("Config.Store was not used")
	}
}

func TestAwaitWakesLocallyWithoutPolling(t *testing.T) {
	fx := newFixture(t)
	registry := core.NewRegistry()
	RegisterWorker(registry, fx.m, func(_ context.Context, job *core.Job[sumArgs]) (sumResult, error) {
		return sumResult{Total: job.Args.A + job.Args.B}, nil
	}, core.WorkerOpts{})

	f, err := Enqueue[sumResult](t.Context(), fx.m, fx.client, sumArgs{A: 2, B: 3}, nil)
	if err != nil {
		t.Fatal(err)
	}
	pending := awaitAsync(t.Context(), f)
	fx.waitForTicker(t, f.ID(), 1)
	// The clock never advances: only the local completion signal can wake Await.
	fx.startPool(t, registry, goncordia.WorkerObservers(nil, fx.m))

	got := <-pending
	if got.err != nil || got.value.Total != 5 {
		t.Fatalf("Await = %+v, %v", got.value, got.err)
	}
	finish := fx.obs.lastAwait(t)
	if finish.Outcome != OutcomeCompleted || finish.Wakeups < 1 || finish.Job == nil || finish.ResultBytes == 0 {
		t.Fatalf("finish = %+v", finish)
	}
	if fx.m.waiting(f.ID()) != 0 {
		t.Fatal("waiter not removed")
	}
	fx.obs.mu.Lock()
	defer fx.obs.mu.Unlock()
	if len(fx.obs.stores) != 1 || fx.obs.stores[0].Kind != "sum" || fx.obs.stores[0].Bytes != len(`{"total":5}`) ||
		fx.obs.stored[0].Err != nil || fx.obs.awaits[0].Kind != "sum" || !fx.obs.awaits[0].Blocking {
		t.Fatalf("stores=%+v stored=%+v awaits=%+v", fx.obs.stores, fx.obs.stored, fx.obs.awaits)
	}
}

func TestAwaitPollsWhenNoLocalSignal(t *testing.T) {
	fx := newFixture(t)
	f, err := Enqueue[sumResult](t.Context(), fx.m, fx.client, sumArgs{A: 1, B: 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	pending := awaitAsync(t.Context(), f)
	fx.waitForTicker(t, f.ID(), 1)

	row := fx.claim(t)
	if err := fx.d.Executor().(driver.JobResultStore).JobResultPut(t.Context(), row.ID, []byte(`{"total":2}`)); err != nil {
		t.Fatal(err)
	}
	fx.finish(t, row.ID, driver.JobStateCompleted, "")
	fx.clk.Advance(time.Second)

	got := <-pending
	if got.err != nil || got.value.Total != 2 {
		t.Fatalf("Await = %+v, %v", got.value, got.err)
	}
	if finish := fx.obs.lastAwait(t); finish.Polls != 2 || finish.Wakeups != 0 {
		t.Fatalf("finish = %+v, want 2 polls and no wakeups", finish)
	}
	if fx.clk.ActiveTimers() != 0 {
		t.Fatal("poll ticker leaked")
	}
}

func TestAwaitReturnsDiscardedJobError(t *testing.T) {
	fx := newFixture(t)
	registry := core.NewRegistry()
	RegisterWorker(registry, fx.m, func(context.Context, *core.Job[sumArgs]) (sumResult, error) {
		return sumResult{}, core.Discard(errors.New("bad input"))
	}, core.WorkerOpts{})
	fx.startPool(t, registry, fx.m)

	f, err := Enqueue[sumResult](t.Context(), fx.m, fx.client, sumArgs{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.Await(t.Context())
	var jobErr *JobError
	if !errors.Is(err, ErrDiscarded) || errors.Is(err, ErrCancelled) || !errors.As(err, &jobErr) ||
		jobErr.Message != "bad input" || jobErr.Kind != "sum" || jobErr.Attempt != 1 {
		t.Fatalf("err = %v (%+v)", err, jobErr)
	}
	if fx.obs.lastAwait(t).Outcome != OutcomeDiscarded {
		t.Fatalf("outcome = %s", fx.obs.lastAwait(t).Outcome)
	}
}

func TestEncodeFailureDiscardsJob(t *testing.T) {
	fx := newFixture(t)
	registry := core.NewRegistry()
	RegisterWorker(registry, fx.m, func(context.Context, *core.Job[sumArgs]) (func(), error) {
		return func() {}, nil
	}, core.WorkerOpts{MaxRetry: 5})
	fx.startPool(t, registry, fx.m)

	f, err := Enqueue[func()](t.Context(), fx.m, fx.client, sumArgs{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Await(t.Context()); !errors.Is(err, ErrDiscarded) {
		t.Fatalf("err = %v, want ErrDiscarded", err)
	}
}

func TestTryGetCancelledAndPending(t *testing.T) {
	fx := newFixture(t)
	f, err := Enqueue[sumResult](t.Context(), fx.m, fx.client, sumArgs{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, done, err := f.TryGet(t.Context()); done || err != nil {
		t.Fatalf("pending TryGet: done=%v err=%v", done, err)
	}
	if fx.obs.lastAwait(t).Outcome != OutcomePending {
		t.Fatalf("outcome = %s", fx.obs.lastAwait(t).Outcome)
	}
	if err := fx.d.Executor().JobCancel(t.Context(), f.ID()); err != nil {
		t.Fatal(err)
	}
	if _, done, err := f.TryGet(t.Context()); !done || !errors.Is(err, ErrCancelled) {
		t.Fatalf("cancelled TryGet: done=%v err=%v", done, err)
	}
}

func TestCompletedWithoutResult(t *testing.T) {
	fx := newFixture(t)
	f, err := Enqueue[sumResult](t.Context(), fx.m, fx.client, sumArgs{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	fx.claim(t)
	fx.finish(t, f.ID(), driver.JobStateCompleted, "")
	if _, err := f.Await(t.Context()); !errors.Is(err, ErrNoResult) {
		t.Fatalf("err = %v, want ErrNoResult", err)
	}
}

func TestAwaitUnknownJob(t *testing.T) {
	fx := newFixture(t)
	_, err := Of[int](fx.m, "missing").Await(t.Context())
	if !errors.Is(err, ErrNotFound) || !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("err = %v, want future.ErrNotFound and driver.ErrNotFound", err)
	}
	if _, _, err := Of[int](fx.m, "missing").TryGet(t.Context()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("TryGet err = %v, want ErrNotFound", err)
	}
	if fx.obs.lastAwait(t).Outcome != OutcomeNotFound {
		t.Fatalf("outcome = %s", fx.obs.lastAwait(t).Outcome)
	}
}

func TestAwaitContextCancelReleasesResources(t *testing.T) {
	fx := newFixture(t)
	f, err := Enqueue[sumResult](t.Context(), fx.m, fx.client, sumArgs{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	pending := awaitAsync(ctx, f)
	fx.waitForTicker(t, f.ID(), 1)
	cancel()
	if got := <-pending; !errors.Is(got.err, context.Canceled) {
		t.Fatalf("err = %v", got.err)
	}
	if fx.m.waiting(f.ID()) != 0 || fx.clk.ActiveTimers() != 0 {
		t.Fatalf("waiters=%d timers=%d", fx.m.waiting(f.ID()), fx.clk.ActiveTimers())
	}
	if fx.obs.lastAwait(t).Outcome != OutcomeAbandoned {
		t.Fatalf("outcome = %s", fx.obs.lastAwait(t).Outcome)
	}
	// A later Await still works: abandonment is not cached.
	fx.claim(t)
	if err := fx.m.store.JobResultPut(t.Context(), f.ID(), []byte(`{"total":7}`)); err != nil {
		t.Fatal(err)
	}
	fx.finish(t, f.ID(), driver.JobStateCompleted, "")
	if got, err := f.Await(t.Context()); err != nil || got.Total != 7 {
		t.Fatalf("Await = %+v, %v", got, err)
	}
}

func TestResolvedFutureIsCached(t *testing.T) {
	fx := newFixture(t)
	f, err := Enqueue[sumResult](t.Context(), fx.m, fx.client, sumArgs{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	fx.claim(t)
	if err := fx.m.store.JobResultPut(t.Context(), f.ID(), []byte(`{"total":9}`)); err != nil {
		t.Fatal(err)
	}
	fx.finish(t, f.ID(), driver.JobStateCompleted, "")
	if _, err := f.Await(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := fx.m.Forget(t.Context(), f.ID()); err != nil {
		t.Fatal(err)
	}
	calls := len(fx.obs.results)
	got, err := f.Await(t.Context())
	if err != nil || got.Total != 9 || len(fx.obs.results) != calls {
		t.Fatalf("cached Await = %+v, %v (observer calls %d -> %d)", got, err, calls, len(fx.obs.results))
	}
	if _, err := Of[sumResult](fx.m, f.ID()).Await(t.Context()); !errors.Is(err, ErrNoResult) {
		t.Fatalf("after Forget: %v", err)
	}
}

func TestUniqueEnqueueFollowsExistingJob(t *testing.T) {
	fx := newFixture(t)
	opts := &core.InsertOpts{UniqueOpts: &core.UniqueOpts{ByArgs: true}}
	first, err := Enqueue[sumResult](t.Context(), fx.m, fx.client, sumArgs{A: 1}, opts)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Enqueue[sumResult](t.Context(), fx.m, fx.client, sumArgs{A: 1}, opts)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID() != second.ID() {
		t.Fatalf("ids %s != %s", first.ID(), second.ID())
	}
}

func TestEnqueueTxPropagatesDriverError(t *testing.T) {
	fx := newFixture(t)
	f, err := EnqueueTx[sumResult](t.Context(), fx.m, fx.client, memory.NoTx{}, sumArgs{}, nil)
	if !errors.Is(err, driver.ErrUnsupported) || f != nil {
		t.Fatalf("f=%v err=%v", f, err)
	}
}

func TestOfLearnsKindFromRow(t *testing.T) {
	fx := newFixture(t)
	f, err := Enqueue[sumResult](t.Context(), fx.m, fx.client, sumArgs{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	other := Of[sumResult](fx.m, f.ID())
	if _, done, err := other.TryGet(t.Context()); done || err != nil {
		t.Fatalf("done=%v err=%v", done, err)
	}
	if other.jobKind() != "sum" {
		t.Fatalf("kind = %q", other.jobKind())
	}
}

func TestStoreFailureIsRetryable(t *testing.T) {
	fx := newFixture(t)
	failing := &failingStore{err: errors.New("disk full")}
	m, err := New(fx.d, Config{Store: failing, Clock: fx.clk, Observer: fx.obs})
	if err != nil {
		t.Fatal(err)
	}
	registry := core.NewRegistry()
	RegisterWorker(registry, m, func(context.Context, *core.Job[sumArgs]) (int, error) { return 1, nil }, core.WorkerOpts{})
	err = registry.Process(t.Context(), &core.RawJob{ID: "job-1", Kind: "sum", Args: []byte(`{}`), AttemptNum: 1})
	if !errors.Is(err, failing.err) {
		t.Fatalf("err = %v", err)
	}
	if _, discard := core.AsDiscard(err); discard {
		t.Fatal("store failure must be retryable")
	}
	fx.obs.mu.Lock()
	defer fx.obs.mu.Unlock()
	if len(fx.obs.stored) != 1 || !errors.Is(fx.obs.stored[0].Err, failing.err) {
		t.Fatalf("stored = %+v", fx.obs.stored)
	}
}

type failingStore struct{ err error }

func (s *failingStore) JobResultPut(context.Context, string, []byte) error { return s.err }
func (s *failingStore) JobResultGet(context.Context, string) ([]byte, error) {
	return nil, s.err
}
func (s *failingStore) JobResultDelete(context.Context, string) error { return s.err }

// lostStore reports every result as lost, as the Redis driver does after
// eviction.
type lostStore struct{ driver.JobResultStore }

func (lostStore) JobResultGet(context.Context, string) ([]byte, error) {
	return nil, fmt.Errorf("%w: %w", driver.ErrResultLost, driver.ErrNotFound)
}

func TestAwaitReportsLostResult(t *testing.T) {
	fx := newFixture(t)
	m, err := New(fx.d, Config{Clock: fx.clk, Store: lostStore{fx.d.Executor()}, Observer: fx.obs})
	if err != nil {
		t.Fatal(err)
	}
	f, err := Enqueue[int](t.Context(), m, fx.client, sumArgs{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	fx.claim(t)
	fx.finish(t, f.ID(), driver.JobStateCompleted, "")
	_, err = f.Await(t.Context())
	if !errors.Is(err, ErrResultLost) || errors.Is(err, ErrNoResult) {
		t.Fatalf("err = %v, want ErrResultLost only", err)
	}
	if fx.obs.lastAwait(t).Outcome != OutcomeResultLost {
		t.Fatalf("outcome = %s", fx.obs.lastAwait(t).Outcome)
	}
}
