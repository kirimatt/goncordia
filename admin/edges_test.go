package admin_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/admin"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

// plainExecutor hides the memory executor's AdminExecutor methods.
type plainExecutor struct{ driver.Executor }

// scriptedExecutor fails the n-th call (1-based) of QueueList or JobList and
// can fail queue actions and rescheduling.
type scriptedExecutor struct {
	driver.Executor
	admin         driver.AdminExecutor
	failQueueList int
	failJobList   int
	queueLists    int
	jobLists      int
	actionErr     error
}

func (e *scriptedExecutor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	e.queueLists++
	if e.queueLists == e.failQueueList {
		return nil, errors.New("queue list down")
	}
	return e.Executor.QueueList(ctx, params)
}

func (e *scriptedExecutor) JobList(ctx context.Context, params driver.JobListParams) ([]driver.JobRow, error) {
	e.jobLists++
	if e.jobLists == e.failJobList {
		return nil, errors.New("job list down")
	}
	return e.admin.JobList(ctx, params)
}

func (e *scriptedExecutor) QueueStats(ctx context.Context, queue string) (driver.QueueStats, error) {
	return e.admin.QueueStats(ctx, queue)
}

func (e *scriptedExecutor) QueuePause(ctx context.Context, queue string) error {
	if e.actionErr != nil {
		return e.actionErr
	}
	return e.Executor.QueuePause(ctx, queue)
}

func (e *scriptedExecutor) JobReschedule(ctx context.Context, params driver.RescheduleParams) error {
	if e.actionErr != nil {
		return e.actionErr
	}
	return e.Executor.JobReschedule(ctx, params)
}

func serve(t *testing.T, h http.Handler, method, path string, header map[string]string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, v := range header {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func seedQueues(t *testing.T, d *memory.Driver, queues ...string) []string {
	t.Helper()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	var ids []string
	for _, queue := range queues {
		res, err := client.Enqueue(context.Background(), args{Value: queue}, &core.InsertOpts{Queue: queue})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, res.Job.ID)
	}
	return ids
}

func TestAdminPaginationProbeFailures(t *testing.T) {
	mem := memory.New()
	seedQueues(t, mem, "a", "b")
	newHandler := func(exec *scriptedExecutor) http.Handler {
		exec.Executor = mem.Executor()
		exec.admin = mem.Executor().(driver.AdminExecutor)
		return admin.New[memory.NoTx](&wrappedDriver{Driver: mem, executor: exec})
	}

	if rec := serve(t, newHandler(&scriptedExecutor{failQueueList: 1}), "GET", "/api/queues", nil, ""); rec.Code != http.StatusInternalServerError {
		t.Fatalf("queue list failure: %d %s", rec.Code, rec.Body)
	}
	if rec := serve(t, newHandler(&scriptedExecutor{failQueueList: 2}), "GET", "/api/queues?limit=1", nil, ""); rec.Code != http.StatusInternalServerError {
		t.Fatalf("queue page probe failure: %d %s", rec.Code, rec.Body)
	}
	rec := serve(t, newHandler(&scriptedExecutor{}), "GET", "/api/queues?limit=1", nil, "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"has_more":true`) {
		t.Fatalf("queue page: %d %s", rec.Code, rec.Body)
	}
	if rec := serve(t, newHandler(&scriptedExecutor{failJobList: 1}), "GET", "/api/jobs", nil, ""); rec.Code != http.StatusInternalServerError {
		t.Fatalf("job list failure: %d %s", rec.Code, rec.Body)
	}
	if rec := serve(t, newHandler(&scriptedExecutor{failJobList: 2}), "GET", "/api/jobs?limit=1", nil, ""); rec.Code != http.StatusInternalServerError {
		t.Fatalf("job page probe failure: %d %s", rec.Code, rec.Body)
	}
	if rec := serve(t, newHandler(&scriptedExecutor{failQueueList: 1}), "GET", "/metrics", nil, ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("metrics queue list failure: %d %s", rec.Code, rec.Body)
	}
}

func TestAdminWithoutJobListing(t *testing.T) {
	mem := memory.New()
	ids := seedQueues(t, mem, "a")
	h := admin.New[memory.NoTx](&wrappedDriver{Driver: mem, executor: plainExecutor{mem.Executor()}})

	if rec := serve(t, h, "GET", "/api/jobs", nil, ""); rec.Code != http.StatusNotImplemented {
		t.Fatalf("job list without AdminExecutor: %d %s", rec.Code, rec.Body)
	}
	if rec := serve(t, h, "GET", "/metrics", nil, ""); rec.Code != http.StatusNotImplemented {
		t.Fatalf("metrics without AdminExecutor: %d %s", rec.Code, rec.Body)
	}
	rec := serve(t, h, "GET", "/api/queues", nil, "")
	if rec.Code != http.StatusOK || strings.Contains(rec.Body.String(), `"stats"`) {
		t.Fatalf("queues without stats: %d %s", rec.Code, rec.Body)
	}
	if rec := serve(t, h, "GET", "/api/jobs?id="+ids[0], nil, ""); rec.Code != http.StatusOK {
		t.Fatalf("job by id still works: %d %s", rec.Code, rec.Body)
	}
}

func TestAdminActionErrors(t *testing.T) {
	mem := memory.New()
	ids := seedQueues(t, mem, "a")
	exec := &scriptedExecutor{Executor: mem.Executor(), admin: mem.Executor().(driver.AdminExecutor), actionErr: driver.ErrUnsupported}
	h := admin.New[memory.NoTx](&wrappedDriver{Driver: mem, executor: exec})

	for _, tc := range []struct {
		path, confirm string
		body          string
		want          int
	}{
		{"/api/queues/a", "pause", "", http.StatusBadRequest},
		{"/api/queues//pause", "pause", "", http.StatusBadRequest},
		{"/api/queues/a/drain", "drain", "", http.StatusBadRequest},
		{"/api/queues/a/pause", "", "", http.StatusPreconditionRequired},
		{"/api/queues/a/pause", "pause", "", http.StatusNotImplemented},
		{"/api/jobs/" + ids[0] + "/reschedule", "reschedule", `{"run_at":"2030-01-01T00:00:00Z"}`, http.StatusNotImplemented},
		{"/api/jobs/" + ids[0] + "/reschedule", "reschedule", `{}`, http.StatusBadRequest},
	} {
		rec := serve(t, h, "POST", tc.path, map[string]string{"X-Goncordia-Confirm": tc.confirm}, tc.body)
		if rec.Code != tc.want {
			t.Errorf("POST %s (confirm %q) = %d %s, want %d", tc.path, tc.confirm, rec.Code, rec.Body, tc.want)
		}
	}
	if rec := serve(t, h, "DELETE", "/api/jobs", nil, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown route: %d", rec.Code)
	}
}

func TestAdminDefaultsAndRedaction(t *testing.T) {
	mem := memory.New()
	client := goncordia.NewClient(mem, goncordia.ClientConfig{})
	res, err := client.Enqueue(context.Background(), args{Value: "secret"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	exec := mem.Executor()
	rows, err := exec.JobFetchBatch(context.Background(), driver.FetchParams{Queue: "default", Limit: 1, WorkerID: "w", LeaseDuration: time.Minute})
	if err != nil || len(rows) != 1 {
		t.Fatalf("fetch: %v %v", rows, err)
	}
	errText, trace := "boom", "goroutine 1 [running]"
	if err := exec.JobSetStateIfRunning(context.Background(), driver.JobSetStateParams{
		ID: res.Job.ID, State: driver.JobStateDiscarded, ExpectedWorkerID: "w", ExpectedAttempt: rows[0].AttemptNum,
		Err: &errText, Trace: &trace, Attempt: rows[0].AttemptNum,
	}); err != nil {
		t.Fatal(err)
	}

	// nil options fall back to the real clock and the default redactor.
	h := admin.New[memory.NoTx](mem, admin.WithClock(nil), admin.WithJobRedactor(nil))
	rec := serve(t, h, "GET", "/api/jobs?limit=5000", nil, "")
	body := rec.Body.String()
	if rec.Code != http.StatusOK || strings.Contains(body, "goroutine") || strings.Contains(body, "secret") || !strings.Contains(body, "boom") {
		t.Fatalf("redacted list: %d %s", rec.Code, body)
	}

	denied := admin.New[memory.NoTx](mem, admin.WithAuthorizer(func(*http.Request, admin.Operation) error { return admin.ErrForbidden }))
	for _, path := range []string{"/", "/dashboard.css", "/dashboard.js"} {
		if rec := serve(t, denied, "GET", path, nil, ""); rec.Code != http.StatusForbidden {
			t.Errorf("GET %s denied = %d", path, rec.Code)
		}
	}
}
