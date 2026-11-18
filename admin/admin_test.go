package admin_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/admin"
	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
	"github.com/kirimatt/goncordia/v2/driver/memory"
	"github.com/kirimatt/goncordia/v2/future"
)

type args struct{ Value string }

func (args) Kind() string { return "admin_test" }

type wrappedDriver struct {
	*memory.Driver
	executor driver.Executor
}

func (d *wrappedDriver) Executor() driver.Executor { return d.executor }

type failingExecutor struct {
	driver.Executor
	admin        driver.AdminExecutor
	queueListErr error
	statsErr     error
}

func (e *failingExecutor) QueueList(ctx context.Context, params driver.QueueListParams) ([]*driver.QueueRow, error) {
	if e.queueListErr != nil {
		return nil, e.queueListErr
	}
	return e.Executor.QueueList(ctx, params)
}

func (e *failingExecutor) JobList(ctx context.Context, params driver.JobListParams) ([]driver.JobRow, error) {
	return e.admin.JobList(ctx, params)
}

func (e *failingExecutor) QueueStats(ctx context.Context, queue string) (driver.QueueStats, error) {
	if e.statsErr != nil {
		return driver.QueueStats{}, e.statsErr
	}
	return e.admin.QueueStats(ctx, queue)
}

func TestHandler(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	if _, err := client.Enqueue(context.Background(), args{Value: "one"}, &core.InsertOpts{
		UniqueOpts: &core.UniqueOpts{Key: "sensitive-key"},
	}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(admin.New(d))
	defer server.Close()

	for _, path := range []string{"/", "/dashboard.css", "/dashboard.js", "/healthz", "/readyz", "/api/queues", "/api/jobs", "/metrics"} {
		response, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("GET %s: status=%d body=%s", path, response.StatusCode, body)
		}
		if strings.Contains(response.Header.Get("Content-Security-Policy"), "unsafe-inline") {
			t.Fatalf("GET %s permits inline content: %q", path, response.Header.Get("Content-Security-Policy"))
		}
		if path == "/" && (strings.Contains(string(body), "<style>") || strings.Contains(string(body), "onclick=")) {
			t.Fatalf("dashboard contains inline content: %s", body)
		}
	}

	response, err := http.Get(server.URL + "/api/jobs?queue=default")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var page driver.JobPage
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].Kind != "admin_test" || page.HasMore || page.NextCursor != "" {
		t.Fatalf("unexpected page: %+v", page)
	}
	if len(page.Items[0].Args) != 0 || page.Items[0].UniqueKey != "" {
		t.Fatalf("default redactor exposed sensitive job data: %+v", page.Items[0])
	}

	response, err = http.Get(server.URL + "/api/jobs?cursor=invalid")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid cursor status=%d, want 400", response.StatusCode)
	}

	response, err = http.Get(server.URL + "/api/jobs?id=missing")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("missing job status=%d, want 404", response.StatusCode)
	}

	for _, value := range []string{"two", "three"} {
		if _, err := client.Enqueue(context.Background(), args{Value: value}, nil); err != nil {
			t.Fatal(err)
		}
	}
	response, err = http.Get(server.URL + "/api/jobs?limit=2")
	if err != nil {
		t.Fatal(err)
	}
	var firstPage driver.JobPage
	if err := json.NewDecoder(response.Body).Decode(&firstPage); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if len(firstPage.Items) != 2 || !firstPage.HasMore || firstPage.NextCursor == "" {
		t.Fatalf("unexpected first page: %+v", firstPage)
	}
	response, err = http.Get(server.URL + "/api/jobs?limit=2&cursor=" + firstPage.NextCursor)
	if err != nil {
		t.Fatal(err)
	}
	var secondPage driver.JobPage
	if err := json.NewDecoder(response.Body).Decode(&secondPage); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if len(secondPage.Items) != 1 || secondPage.HasMore || secondPage.NextCursor != "" {
		t.Fatalf("unexpected second page: %+v", secondPage)
	}

	response, err = http.Post(server.URL+"/api/queues/default/pause", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusPreconditionRequired {
		t.Fatalf("unconfirmed mutation status=%d, want 428", response.StatusCode)
	}

	request, err := http.NewRequest(http.MethodPost, server.URL+"/api/queues/default/pause", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Goncordia-Confirm", "pause")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("confirmed mutation status=%d, want 200", response.StatusCode)
	}
	queue, err := d.Executor().QueueGet(context.Background(), "default")
	if err != nil || !queue.Paused {
		t.Fatalf("queue not paused: %+v, %v", queue, err)
	}

	metrics, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(metrics.Body)
	metrics.Body.Close()
	if !strings.Contains(string(data), "goncordia_queue_jobs") {
		t.Fatalf("unexpected metrics: %s", data)
	}
}

func TestReadOnlyAndAuthorization(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	if _, err := client.Enqueue(context.Background(), args{Value: "one"}, nil); err != nil {
		t.Fatal(err)
	}

	readOnly := httptest.NewServer(admin.New(d, admin.WithReadOnly(true)))
	defer readOnly.Close()
	request, err := http.NewRequest(http.MethodPost, readOnly.URL+"/api/queues/default/pause", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Goncordia-Confirm", "pause")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("read-only mutation status=%d, want 403", response.StatusCode)
	}
	queue, err := d.Executor().QueueGet(context.Background(), "default")
	if err != nil || queue.Paused {
		t.Fatalf("read-only mutation changed queue: %+v, %v", queue, err)
	}

	forbidden := errors.New("forbidden")
	authorized := httptest.NewServer(admin.New(d, admin.WithAuthorizer(func(r *http.Request, operation admin.Operation) error {
		if r.Header.Get("Authorization") != "Bearer test" {
			return admin.ErrUnauthenticated
		}
		if operation == admin.OperationMutate {
			return forbidden
		}
		return nil
	})))
	defer authorized.Close()

	response, err = http.Get(authorized.URL + "/api/jobs")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status=%d, want 401", response.StatusCode)
	}
	request, err = http.NewRequest(http.MethodGet, authorized.URL+"/api/jobs", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer test")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("authorized read status=%d, want 200", response.StatusCode)
	}
	request, err = http.NewRequest(http.MethodPost, authorized.URL+"/api/queues/default/pause", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer test")
	request.Header.Set("X-Goncordia-Confirm", "pause")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("unauthorized mutation status=%d, want 403", response.StatusCode)
	}
}

func TestCustomRedactorCanExposePayload(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	if _, err := client.Enqueue(context.Background(), args{Value: "visible"}, nil); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(admin.New(d, admin.WithJobRedactor(func(row driver.JobRow) driver.JobRow { return row })))
	defer server.Close()

	response, err := http.Get(server.URL + "/api/jobs")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var page driver.JobPage
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || len(page.Items[0].Args) == 0 {
		t.Fatalf("custom redactor did not expose payload: %+v", page)
	}
}

func TestLivenessAndReadinessAreIndependent(t *testing.T) {
	d := memory.New()
	base := d.Executor()
	executor := &failingExecutor{
		Executor:     base,
		admin:        base.(driver.AdminExecutor),
		queueListErr: errors.New("database unavailable"),
	}
	server := httptest.NewServer(admin.New(&wrappedDriver{Driver: d, executor: executor}))
	defer server.Close()

	for path, want := range map[string]int{"/healthz": http.StatusOK, "/readyz": http.StatusServiceUnavailable} {
		response, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != want {
			t.Fatalf("GET %s status=%d, want %d", path, response.StatusCode, want)
		}
	}
}

func TestMetricsFailureReturnsNoPartialMetrics(t *testing.T) {
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	if _, err := client.Enqueue(context.Background(), args{Value: "one"}, nil); err != nil {
		t.Fatal(err)
	}
	base := d.Executor()
	executor := &failingExecutor{
		Executor: base,
		admin:    base.(driver.AdminExecutor),
		statsErr: errors.New("stats unavailable"),
	}
	server := httptest.NewServer(admin.New(&wrappedDriver{Driver: d, executor: executor}))
	defer server.Close()

	response, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("metrics status=%d, want 503: %s", response.StatusCode, body)
	}
	if strings.Contains(string(body), "goncordia_queue_jobs") {
		t.Fatalf("metrics response contains partial samples: %s", body)
	}
}

func TestMetricsIncludesFutures(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	futureMetrics := future.NewMetrics(clk)
	m, err := future.New(d, future.Config{Clock: clk, Observer: futureMetrics})
	if err != nil {
		t.Fatal(err)
	}
	registry := core.NewRegistry()
	future.RegisterWorker(registry, m, func(_ context.Context, job *core.Job[args]) (string, error) {
		return "done:" + job.Args.Value, nil
	}, core.WorkerOpts{})
	ctx, cancel := context.WithCancel(context.Background())
	pool := goncordia.NewWorkerPool(d, registry, goncordia.WorkerConfig{Clock: clk, PollInterval: time.Hour, Observer: m})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = pool.Start(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()
	client := goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk})
	f, err := future.Enqueue[string](context.Background(), m, client, args{Value: "x"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := f.Await(context.Background()); err != nil || got != "done:x" {
		t.Fatalf("Await = %q, %v", got, err)
	}

	server := httptest.NewServer(admin.New(d, admin.WithMetrics(futureMetrics, nil)))
	defer server.Close()
	response, err := http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := io.ReadAll(response.Body)
	response.Body.Close()
	body := string(data)
	for _, want := range []string{
		`goncordia_queue_jobs{queue="default",state="completed"} 1`,
		`goncordia_future_awaits_in_flight 0`,
		`goncordia_future_await_duration_seconds_count{kind="admin_test",outcome="completed"} 1`,
		`goncordia_future_store_duration_seconds_count{kind="admin_test",status="ok"} 1`,
		`goncordia_future_result_size_bytes_sum{kind="admin_test"} 8`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q:\n%s", want, body)
		}
	}
}
