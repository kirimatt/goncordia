package mcpserver_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/kirimatt/goncordia/mcpserver/v2"
	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

type emailArgs struct {
	To string `json:"to"`
}

func (emailArgs) Kind() string { return "send_email" }

var now = time.Date(2026, 11, 23, 12, 0, 0, 0, time.UTC)

type fixture struct {
	d      *memory.Driver
	client *goncordia.Client[memory.NoTx]
	clk    *clock.Manual
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	clk := clock.NewManual(now)
	d := memory.New(memory.WithClock(clk))
	return fixture{d: d, client: goncordia.NewClient(d, goncordia.ClientConfig{Clock: clk}), clk: clk}
}

func (f fixture) enqueue(t *testing.T, queue, to string) string {
	t.Helper()
	res, err := f.client.Enqueue(context.Background(), emailArgs{To: to}, &core.InsertOpts{Queue: queue})
	if err != nil {
		t.Fatal(err)
	}
	return res.Job.ID
}

// fail claims the job and records a final failure with a panic trace.
func (f fixture) fail(t *testing.T, queue, id string) {
	t.Helper()
	ctx := context.Background()
	exec := f.d.Executor()
	rows, err := exec.JobFetchBatch(ctx, driver.FetchParams{Queue: queue, Limit: 10, WorkerID: "w1", LeaseDuration: time.Minute})
	if err != nil || len(rows) == 0 {
		t.Fatalf("fetch: %v (%d rows)", err, len(rows))
	}
	msg, trace := "smtp: connection refused", "goroutine 1 [running]"
	if err := exec.JobSetStateIfRunning(ctx, driver.JobSetStateParams{ID: id, State: driver.JobStateDiscarded, Err: &msg, Trace: &trace, Attempt: 1}); err != nil {
		t.Fatal(err)
	}
}

func connect(t *testing.T, server *mcp.Server) *mcp.ClientSession {
	t.Helper()
	ctx := context.Background()
	st, ct := mcp.NewInMemoryTransports()
	ss, err := server.Connect(ctx, st, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ss.Close() })
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "v0"}, nil).Connect(ctx, ct, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cs.Close() })
	return cs
}

func call[Out any](t *testing.T, cs *mcp.ClientSession, name string, args any) Out {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	if res.IsError {
		t.Fatalf("%s: tool error: %s", name, text(res))
	}
	var out Out
	raw, err := json.Marshal(res.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("%s: decode %s: %v", name, raw, err)
	}
	return out
}

func callErr(t *testing.T, cs *mcp.ClientSession, name string, args any) string {
	t.Helper()
	res, err := cs.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		return err.Error()
	}
	if !res.IsError {
		t.Fatalf("%s: expected an error, got %s", name, text(res))
	}
	return text(res)
}

func text(res *mcp.CallToolResult) string {
	var b strings.Builder
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			b.WriteString(tc.Text)
		}
	}
	return b.String()
}

func toolNames(t *testing.T, cs *mcp.ClientSession) []string {
	t.Helper()
	res, err := cs.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tool := range res.Tools {
		names = append(names, tool.Name)
	}
	slices.Sort(names)
	return names
}

func TestReadOnlyByDefault(t *testing.T) {
	f := newFixture(t)
	cs := connect(t, mcpserver.New(f.d))
	want := []string{"get_job", "get_metrics", "list_jobs", "list_queues"}
	if got := toolNames(t, cs); !slices.Equal(got, want) {
		t.Fatalf("tools = %v, want %v", got, want)
	}
	if msg := callErr(t, cs, "delete_job", map[string]any{"id": "x"}); !strings.Contains(msg, "delete_job") {
		t.Fatalf("unexpected error %q", msg)
	}
}

func TestInspectFailedJob(t *testing.T) {
	f := newFixture(t)
	failed := f.enqueue(t, "email", "a@example.com")
	f.fail(t, "email", failed)
	f.enqueue(t, "email", "b@example.com")
	f.enqueue(t, "reports", "c@example.com")
	cs := connect(t, mcpserver.New(f.d))

	queues := call[mcpserver.ListQueuesOutput](t, cs, "list_queues", map[string]any{})
	got := map[string]mcpserver.QueueInfo{}
	for _, q := range queues.Queues {
		got[q.Name] = q
	}
	if email := got["email"]; email.Total == nil || *email.Total != 2 || email.States["discarded"] != 1 || email.States["available"] != 1 {
		t.Fatalf("email queue = %+v", email)
	}

	jobs := call[mcpserver.ListJobsOutput](t, cs, "list_jobs", map[string]any{"state": "discarded"})
	if len(jobs.Jobs) != 1 || jobs.Jobs[0].ID != failed {
		t.Fatalf("discarded jobs = %+v", jobs.Jobs)
	}

	job := call[mcpserver.JobInfo](t, cs, "get_job", map[string]any{"id": failed})
	if job.State != "discarded" || job.Kind != "send_email" || job.Queue != "email" || len(job.Errors) != 1 {
		t.Fatalf("job = %+v", job)
	}
	if job.Errors[0].Error != "smtp: connection refused" {
		t.Fatalf("error = %q", job.Errors[0].Error)
	}
	if job.Args != "" || job.Errors[0].Trace != "" {
		t.Fatalf("default redactor leaked args %q or trace %q", job.Args, job.Errors[0].Trace)
	}

	if msg := callErr(t, cs, "get_job", map[string]any{"id": "missing"}); !strings.Contains(msg, "not found") {
		t.Fatalf("missing job error = %q", msg)
	}
	if msg := callErr(t, cs, "list_jobs", map[string]any{"state": "failed"}); !strings.Contains(msg, "unknown state") {
		t.Fatalf("bad state error = %q", msg)
	}
}

func TestJobRedactorCanExposePayload(t *testing.T) {
	f := newFixture(t)
	id := f.enqueue(t, "email", "a@example.com")
	f.fail(t, "email", id)
	cs := connect(t, mcpserver.New(f.d, mcpserver.WithJobRedactor(func(r driver.JobRow) driver.JobRow { return r })))
	job := call[mcpserver.JobInfo](t, cs, "get_job", map[string]any{"id": id})
	if !strings.Contains(job.Args, "a@example.com") || job.Errors[0].Trace == "" {
		t.Fatalf("job = %+v", job)
	}
}

func TestListJobsPagination(t *testing.T) {
	f := newFixture(t)
	for range 5 {
		f.enqueue(t, "email", "user")
		f.clk.Advance(time.Second)
	}
	cs := connect(t, mcpserver.New(f.d))
	seen := map[string]bool{}
	cursor := ""
	for page := 0; ; page++ {
		if page > 5 {
			t.Fatal("pagination did not end")
		}
		out := call[mcpserver.ListJobsOutput](t, cs, "list_jobs", map[string]any{"limit": 2, "cursor": cursor})
		for _, j := range out.Jobs {
			if seen[j.ID] {
				t.Fatalf("job %s returned twice", j.ID)
			}
			seen[j.ID] = true
		}
		if out.NextCursor == "" {
			break
		}
		cursor = out.NextCursor
	}
	if len(seen) != 5 {
		t.Fatalf("saw %d jobs, want 5", len(seen))
	}
}

func TestWriteTools(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t)
	failed := f.enqueue(t, "email", "a@example.com")
	f.fail(t, "email", failed)
	waiting := f.enqueue(t, "email", "b@example.com")
	doomed := f.enqueue(t, "email", "c@example.com")
	cs := connect(t, mcpserver.New(f.d, mcpserver.WithWriteTools(), mcpserver.WithClock(f.clk)))

	want := []string{"cancel_job", "delete_job", "get_job", "get_metrics", "list_jobs", "list_queues", "pause_queue", "reschedule_job", "resume_queue", "retry_job"}
	if got := toolNames(t, cs); !slices.Equal(got, want) {
		t.Fatalf("tools = %v, want %v", got, want)
	}

	row := func(id string) *driver.JobRow {
		t.Helper()
		r, err := f.d.Executor().JobGetByID(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}

	call[mcpserver.ActionOutput](t, cs, "retry_job", map[string]any{"id": failed})
	if r := row(failed); r.State != driver.JobStateScheduled || !r.RunAt.Equal(now) {
		t.Fatalf("after retry: state %s run_at %s", r.State, r.RunAt)
	}

	later := now.Add(2 * time.Hour)
	call[mcpserver.ActionOutput](t, cs, "reschedule_job", map[string]any{"id": failed, "run_at": later.Format(time.RFC3339)})
	if r := row(failed); !r.RunAt.Equal(later) {
		t.Fatalf("after reschedule: run_at %s", r.RunAt)
	}

	call[mcpserver.ActionOutput](t, cs, "cancel_job", map[string]any{"id": waiting})
	if r := row(waiting); r.State != driver.JobStateCancelled {
		t.Fatalf("after cancel: state %s", r.State)
	}
	if msg := callErr(t, cs, "cancel_job", map[string]any{"id": waiting}); msg == "" {
		t.Fatal("cancelling a cancelled job should fail")
	}

	call[mcpserver.ActionOutput](t, cs, "delete_job", map[string]any{"id": doomed})
	if _, err := f.d.Executor().JobGetByID(ctx, doomed); err == nil {
		t.Fatal("job still exists after delete")
	}

	call[mcpserver.ActionOutput](t, cs, "pause_queue", map[string]any{"queue": "email"})
	if q, _ := f.d.Executor().QueueGet(ctx, "email"); q == nil || !q.Paused {
		t.Fatalf("queue not paused: %+v", q)
	}
	metrics := call[mcpserver.MetricsOutput](t, cs, "get_metrics", map[string]any{})
	if len(metrics.Queues) != 1 || !metrics.Queues[0].Paused {
		t.Fatalf("metrics = %+v", metrics)
	}
	call[mcpserver.ActionOutput](t, cs, "resume_queue", map[string]any{"queue": "email"})
	if q, _ := f.d.Executor().QueueGet(ctx, "email"); q.Paused {
		t.Fatal("queue still paused")
	}
}

type staticMetrics string

func (m staticMetrics) WritePrometheus(w io.Writer) error {
	_, err := io.WriteString(w, string(m))
	return err
}

func TestMetricsIncludesExtraWriters(t *testing.T) {
	f := newFixture(t)
	f.enqueue(t, "email", "a@example.com")
	cs := connect(t, mcpserver.New(f.d, mcpserver.WithMetrics(staticMetrics("goncordia_future_awaits_total 3\n"))))
	out := call[mcpserver.MetricsOutput](t, cs, "get_metrics", map[string]any{})
	if len(out.Queues) != 1 || out.Queues[0].States["available"] != 1 || out.Queues[0].Total != 1 {
		t.Fatalf("queues = %+v", out.Queues)
	}
	if !strings.Contains(out.Prometheus, "goncordia_future_awaits_total 3") {
		t.Fatalf("prometheus = %q", out.Prometheus)
	}
}

func TestStreamableHTTPHandler(t *testing.T) {
	f := newFixture(t)
	f.enqueue(t, "email", "a@example.com")
	httpServer := httptest.NewServer(mcpserver.Handler(mcpserver.New(f.d, mcpserver.WithName("goncordia-test"))))
	defer httpServer.Close()

	ctx := context.Background()
	cs, err := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "v0"}, nil).
		Connect(ctx, &mcp.StreamableClientTransport{Endpoint: httpServer.URL}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	if info := cs.InitializeResult().ServerInfo; info.Name != "goncordia-test" || info.Version != mcpserver.Version() {
		t.Fatalf("server info = %+v", info)
	}
	out := call[mcpserver.ListQueuesOutput](t, cs, "list_queues", map[string]any{})
	if len(out.Queues) != 1 || out.Queues[0].Name != "email" {
		t.Fatalf("queues = %+v", out.Queues)
	}
}
