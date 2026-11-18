package goncordia_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/driver"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

// execDriver serves a replacement executor for a memory driver.
type execDriver struct {
	*memory.Driver
	exec driver.Executor
}

func (d *execDriver) Executor() driver.Executor { return d.exec }

// listingFaults wraps the memory executor, keeps its AdminExecutor methods,
// and fails selected calls. failListAt fails the n-th JobList call (1-based).
type listingFaults struct {
	driver.Executor
	admin       driver.AdminExecutor
	lists       atomic.Int32
	failListAt  int32
	deleteErr   error
	resultErr   error
	electErr    error
	extraRows   []driver.JobRow
	electCalled atomic.Int32
}

func newListingFaults(mem *memory.Driver) *listingFaults {
	return &listingFaults{Executor: mem.Executor(), admin: mem.Executor().(driver.AdminExecutor)}
}

func (e *listingFaults) JobList(ctx context.Context, p driver.JobListParams) ([]driver.JobRow, error) {
	if e.lists.Add(1) == e.failListAt {
		return nil, errors.New("list down")
	}
	rows, err := e.admin.JobList(ctx, p)
	if p.Cursor == "" {
		rows = append(rows, e.extraRows...)
	}
	return rows, err
}

func (e *listingFaults) QueueStats(ctx context.Context, queue string) (driver.QueueStats, error) {
	return e.admin.QueueStats(ctx, queue)
}

func (e *listingFaults) JobDelete(ctx context.Context, id string) error {
	if e.deleteErr != nil {
		return e.deleteErr
	}
	return e.Executor.JobDelete(ctx, id)
}

func (e *listingFaults) JobResultGet(ctx context.Context, id string) ([]byte, error) {
	if e.resultErr != nil {
		return nil, e.resultErr
	}
	return e.Executor.JobResultGet(ctx, id)
}

func (e *listingFaults) LeaderAttemptElect(ctx context.Context, p driver.LeaderElectParams) (bool, error) {
	e.electCalled.Add(1)
	if e.electErr != nil {
		return false, e.electErr
	}
	return e.Executor.LeaderAttemptElect(ctx, p)
}

// finishJobs inserts n jobs in queue and moves them to state; completed jobs
// get a stored result.
func finishJobs(t *testing.T, exec driver.Executor, n int, state driver.JobState) []string {
	t.Helper()
	ctx := context.Background()
	params := make([]driver.JobInsertParams, n)
	for i := range params {
		params[i] = driver.JobInsertParams{Queue: "retention", Kind: "k", Args: []byte(`{}`), MaxRetry: 1}
	}
	if _, err := exec.JobInsertMany(ctx, params); err != nil {
		t.Fatal(err)
	}
	rows, err := exec.JobFetchBatch(ctx, driver.FetchParams{Queue: "retention", Limit: n, WorkerID: "w", LeaseDuration: time.Minute})
	if err != nil || len(rows) != n {
		t.Fatalf("fetched %d of %d: %v", len(rows), n, err)
	}
	ids := make([]string, n)
	for i, row := range rows {
		if state == driver.JobStateCompleted {
			if err := exec.JobResultPut(ctx, row.ID, []byte(`1`)); err != nil {
				t.Fatal(err)
			}
		}
		if err := exec.JobSetStateIfRunning(ctx, driver.JobSetStateParams{
			ID: row.ID, State: state, ExpectedWorkerID: "w", ExpectedAttempt: row.AttemptNum,
		}); err != nil {
			t.Fatal(err)
		}
		ids[i] = row.ID
	}
	return ids
}

func TestMaintenanceListingPruneFailures(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	later := clock.NewManual(start.Add(48 * time.Hour))
	setup := func() (*memory.Driver, *listingFaults, *goncordia.Maintenance[memory.NoTx]) {
		mem := memory.New(memory.WithClock(clock.NewManual(start)))
		faults := newListingFaults(mem)
		finishJobs(t, mem.Executor(), 1, driver.JobStateCompleted)
		return mem, faults, goncordia.NewMaintenance[memory.NoTx](&execDriver{Driver: mem, exec: faults}, goncordia.MaintenanceConfig{Clock: later})
	}

	_, faults, m := setup()
	faults.failListAt = 1
	if _, err := m.Prune(ctx, goncordia.RetentionPolicy{Completed: time.Hour}); err == nil || !strings.Contains(err.Error(), "list completed jobs for retention") {
		t.Fatalf("list failure: %v", err)
	}

	_, faults, m = setup()
	faults.deleteErr = errors.New("delete down")
	if result, err := m.Prune(ctx, goncordia.RetentionPolicy{Completed: time.Hour}); err == nil || result.Deleted != 0 || !strings.Contains(err.Error(), "delete job") {
		t.Fatalf("delete failure: %+v %v", result, err)
	}

	_, faults, m = setup()
	faults.resultErr = errors.New("result store down")
	if result, err := m.Prune(ctx, goncordia.RetentionPolicy{Completed: 72 * time.Hour, Results: time.Hour}); err == nil || result.ResultsDeleted != 0 || !strings.Contains(err.Error(), "delete result of job") {
		t.Fatalf("result failure: %+v %v", result, err)
	}

	// A listed row without a finalization time is counted but kept.
	_, faults, m = setup()
	faults.extraRows = []driver.JobRow{{ID: "unfinalized", State: driver.JobStateCompleted}}
	if result, err := m.Prune(ctx, goncordia.RetentionPolicy{Completed: time.Hour}); err != nil || result.Scanned != 2 || result.Deleted != 1 {
		t.Fatalf("unfinalized row: %+v %v", result, err)
	}

	if _, err := m.Prune(ctx, goncordia.RetentionPolicy{Completed: -time.Second}); err == nil {
		t.Fatal("negative retention accepted")
	}
	unsupported := goncordia.NewMaintenance[memory.NoTx](&execDriver{Driver: memory.New(), exec: plainMemExecutor{memory.New().Executor()}}, goncordia.MaintenanceConfig{})
	if _, err := unsupported.Prune(ctx, goncordia.RetentionPolicy{Completed: time.Hour}); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("prune without listing: %v", err)
	}
	if _, err := unsupported.DeadLetterList(ctx, driver.JobListParams{}); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("dead letters without listing: %v", err)
	}
}

// plainMemExecutor hides the memory executor's AdminExecutor methods.
type plainMemExecutor struct{ driver.Executor }

func TestMaintenanceListingPrunePages(t *testing.T) {
	start := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	mem := memory.New(memory.WithClock(clock.NewManual(start)))
	finishJobs(t, mem.Executor(), 501, driver.JobStateCancelled)
	m := goncordia.NewMaintenance(mem, goncordia.MaintenanceConfig{Clock: clock.NewManual(start.Add(time.Hour))})
	result, err := m.Prune(context.Background(), goncordia.RetentionPolicy{Cancelled: time.Minute})
	if err != nil || result.Deleted != 501 {
		t.Fatalf("two-page prune: %+v %v", result, err)
	}
}

// failingPruner implements driver.JobPruner and fails after a number of
// full pages.
type failingPruner struct {
	driver.Executor
	fullPages   int
	finalizeErr error
	resultErr   error
}

func (p *failingPruner) JobPruneFinalized(_ context.Context, params driver.JobPruneParams) (int, error) {
	if p.fullPages > 0 {
		p.fullPages--
		return params.Limit, nil
	}
	return 0, p.finalizeErr
}

func (p *failingPruner) JobResultPrune(context.Context, driver.JobPruneParams) (int, error) {
	return 0, p.resultErr
}

func TestMaintenanceIndexedPruneFailures(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	boom := errors.New("index down")

	pruner := &failingPruner{Executor: mem.Executor(), fullPages: 2, finalizeErr: boom}
	m := goncordia.NewMaintenance[memory.NoTx](&execDriver{Driver: mem, exec: pruner}, goncordia.MaintenanceConfig{})
	result, err := m.Prune(ctx, goncordia.RetentionPolicy{Completed: time.Hour})
	if !errors.Is(err, boom) || !strings.Contains(err.Error(), "prune completed jobs") || result.Deleted != 1000 {
		t.Fatalf("finalized failure after two pages: %+v %v", result, err)
	}

	pruner = &failingPruner{Executor: mem.Executor(), resultErr: boom}
	m = goncordia.NewMaintenance[memory.NoTx](&execDriver{Driver: mem, exec: pruner}, goncordia.MaintenanceConfig{})
	if _, err := m.Prune(ctx, goncordia.RetentionPolicy{Completed: time.Hour, Results: time.Minute}); !errors.Is(err, boom) || !strings.Contains(err.Error(), "prune results") {
		t.Fatalf("result failure: %v", err)
	}
}

func TestMaintenanceRunRetentionConfigAndElection(t *testing.T) {
	m := goncordia.NewMaintenance(memory.New(), goncordia.MaintenanceConfig{})
	if err := m.RunRetention(context.Background(), goncordia.RetentionRunConfig{Policy: goncordia.RetentionPolicy{Completed: -time.Hour}}); err == nil {
		t.Fatal("negative policy accepted")
	}
	if err := m.RunRetention(context.Background(), goncordia.RetentionRunConfig{Interval: -time.Second}); err == nil {
		t.Fatal("negative interval accepted")
	}

	clk := clock.NewManual(time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC))
	mem := memory.New(memory.WithClock(clk))
	faults := newListingFaults(mem)
	faults.electErr = errors.New("lease store down")
	reported := make(chan error, 4)
	m = goncordia.NewMaintenance[memory.NoTx](&execDriver{Driver: mem, exec: faults}, goncordia.MaintenanceConfig{Clock: clk})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- m.RunRetention(ctx, goncordia.RetentionRunConfig{
			OnPrune: func(_ goncordia.PruneResult, err error) { reported <- err },
		})
	}()
	select {
	case err := <-reported:
		if err == nil || !strings.Contains(err.Error(), "retention leader election") {
			t.Fatalf("election failure reported as %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("election failure was not reported")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunRetention returned %v", err)
	}
}

func TestMaintenanceDeadLetterEdges(t *testing.T) {
	ctx := context.Background()
	mem := memory.New()
	discarded := finishJobs(t, mem.Executor(), 2, driver.JobStateDiscarded)
	completed := finishJobs(t, mem.Executor(), 1, driver.JobStateCompleted)

	faults := newListingFaults(mem)
	m := goncordia.NewMaintenance[memory.NoTx](&execDriver{Driver: mem, exec: faults}, goncordia.MaintenanceConfig{})
	page, err := m.DeadLetterList(ctx, driver.JobListParams{Limit: 5000})
	if err != nil || len(page.Items) != 2 || page.HasMore {
		t.Fatalf("limit above 1000 falls back to 100: %+v %v", page, err)
	}
	faults.failListAt = faults.lists.Load() + 1
	if _, err := m.DeadLetterList(ctx, driver.JobListParams{}); err == nil {
		t.Fatal("list failure ignored")
	}
	faults.failListAt = faults.lists.Load() + 2
	if _, err := m.DeadLetterList(ctx, driver.JobListParams{Limit: 1}); err == nil {
		t.Fatal("page probe failure ignored")
	}

	result, err := m.DeadLetterReplay(ctx, []string{discarded[0], completed[0], "", discarded[0], "missing"}, time.Time{})
	if err == nil {
		t.Fatal("replay of invalid ids reported no error")
	}
	if len(result.Succeeded) != 1 || result.Succeeded[0] != discarded[0] || len(result.Failed) != 3 {
		t.Fatalf("replay result = %+v", result)
	}
	if !errors.Is(err, driver.ErrConflict) || !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("replay errors = %v", err)
	}

	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if result, err := m.BulkDelete(cancelled, []string{discarded[1]}); !errors.Is(err, context.Canceled) || len(result.Succeeded) != 0 {
		t.Fatalf("bulk on cancelled context: %+v %v", result, err)
	}
}
