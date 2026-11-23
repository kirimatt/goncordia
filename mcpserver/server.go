// Package mcpserver exposes goncordia queues and jobs to AI coding agents over
// the Model Context Protocol (MCP).
//
// An agent connected to the server can list queues with their job counts,
// search and inspect jobs (state, attempts, last errors), and read queue
// metrics. With [WithWriteTools] it can also retry, cancel, delete and
// reschedule jobs and pause or resume queues. Without it the server is
// read-only.
//
// Serve it over Streamable HTTP behind your application's authentication:
//
//	srv := mcpserver.New(d)
//	mux.Handle("/mcp", requireAdmin(mcpserver.Handler(srv)))
//
// or over stdio from a small command that a local agent starts:
//
//	_ = mcpserver.New(d).Run(ctx, &mcp.StdioTransport{})
//
// Job payloads, unique keys and panic traces are hidden by default, as in the
// admin package; pass [WithJobRedactor] to change that.
package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	gonclock "github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/driver"
)

// Version returns the version of this module reported to MCP clients, or
// "(devel)" when it is built from a workspace checkout.
func Version() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "(devel)"
	}
	for _, dep := range info.Deps {
		if dep.Path == modulePath {
			if dep.Replace != nil {
				return "(devel)"
			}
			return dep.Version
		}
	}
	if info.Main.Path == modulePath && info.Main.Version != "" {
		return info.Main.Version
	}
	return "(devel)"
}

const modulePath = "github.com/kirimatt/goncordia/mcpserver/v2"

const (
	defaultLimit = 50
	maxLimit     = 500
)

// JobRedactor returns the representation of a job shown to the agent.
type JobRedactor func(driver.JobRow) driver.JobRow

// MetricsWriter renders additional metrics in the Prometheus text format,
// for example *future.Metrics or *goncordia.RetentionMetrics.
type MetricsWriter interface {
	WritePrometheus(io.Writer) error
}

type config struct {
	clock  gonclock.Clock
	write  bool
	redact JobRedactor
	extra  []MetricsWriter
	name   string
}

// Option configures the MCP server.
type Option func(*config)

// WithWriteTools registers the tools that change jobs and queues: retry_job,
// cancel_job, delete_job, reschedule_job, pause_queue and resume_queue.
// Without this option the server only reads.
func WithWriteTools() Option {
	return func(cfg *config) { cfg.write = true }
}

// WithClock sets the clock used by retry_job to make a job due now.
func WithClock(clk gonclock.Clock) Option {
	return func(cfg *config) { cfg.clock = clk }
}

// WithJobRedactor replaces the default redactor, which hides job arguments,
// unique keys and panic traces. An identity redactor exposes them to the agent:
//
//	mcpserver.WithJobRedactor(func(r driver.JobRow) driver.JobRow { return r })
func WithJobRedactor(redact JobRedactor) Option {
	return func(cfg *config) { cfg.redact = redact }
}

// WithMetrics appends the output of each writer to the get_metrics tool,
// after the queue counts.
func WithMetrics(writers ...MetricsWriter) Option {
	return func(cfg *config) {
		for _, w := range writers {
			if w != nil {
				cfg.extra = append(cfg.extra, w)
			}
		}
	}
}

// WithName sets the server name reported to MCP clients (default "goncordia").
// Use it to tell environments apart, for example "goncordia-staging".
func WithName(name string) Option {
	return func(cfg *config) { cfg.name = name }
}

// New returns an MCP server for the jobs and queues stored by d.
func New[TTx any](d driver.Driver[TTx], opts ...Option) *mcp.Server {
	cfg := config{clock: gonclock.Real{}, redact: defaultJobRedactor, name: "goncordia"}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.clock == nil {
		cfg.clock = gonclock.Real{}
	}
	if cfg.redact == nil {
		cfg.redact = defaultJobRedactor
	}
	t := &tools{exec: d.Executor(), driverName: d.Name(), cfg: cfg}

	server := mcp.NewServer(&mcp.Implementation{Name: cfg.name, Title: "goncordia job queue", Version: Version()}, &mcp.ServerOptions{
		Instructions: "Inspect and operate the goncordia background job queue (driver " + d.Name() + "). " +
			"Start with list_queues or get_metrics, then list_jobs with a state filter (for example discarded or retryable) and get_job for errors. " +
			"Times are RFC 3339 UTC.",
	})
	readOnly := &mcp.ToolAnnotations{ReadOnlyHint: true, OpenWorldHint: ptr(false)}

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_queues",
		Description: "List queues with their paused flag and job counts by state.",
		Annotations: readOnly,
	}, t.listQueues)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_jobs",
		Description: "List jobs, newest first, filtered by queue, state and kind. Page with next_cursor.",
		Annotations: readOnly,
	}, t.listJobs)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_job",
		Description: "Get one job by id: state, attempts, schedule and the error of every failed attempt.",
		Annotations: readOnly,
	}, t.getJob)
	mcp.AddTool(server, &mcp.Tool{
		Name:        "get_metrics",
		Description: "Job counts by queue and state, plus any extra Prometheus metrics the application registered (futures, retention).",
		Annotations: readOnly,
	}, t.getMetrics)

	if cfg.write {
		mutate := func(destructive, idempotent bool) *mcp.ToolAnnotations {
			return &mcp.ToolAnnotations{DestructiveHint: ptr(destructive), IdempotentHint: idempotent, OpenWorldHint: ptr(false)}
		}
		mcp.AddTool(server, &mcp.Tool{
			Name:        "retry_job",
			Description: "Make a job due now so a worker picks it up again (for retryable, discarded or scheduled jobs).",
			Annotations: mutate(false, true),
		}, t.retryJob)
		mcp.AddTool(server, &mcp.Tool{
			Name:        "reschedule_job",
			Description: "Move a job's run time to run_at.",
			Annotations: mutate(false, true),
		}, t.rescheduleJob)
		mcp.AddTool(server, &mcp.Tool{
			Name:        "cancel_job",
			Description: "Cancel a job that is waiting to run (available or scheduled). Running and finished jobs cannot be cancelled.",
			Annotations: mutate(true, true),
		}, t.cancelJob)
		mcp.AddTool(server, &mcp.Tool{
			Name:        "delete_job",
			Description: "Delete a job permanently. It cannot be restored.",
			Annotations: mutate(true, true),
		}, t.deleteJob)
		mcp.AddTool(server, &mcp.Tool{
			Name:        "pause_queue",
			Description: "Pause a queue: workers stop fetching its jobs until resume_queue.",
			Annotations: mutate(false, true),
		}, t.pauseQueue)
		mcp.AddTool(server, &mcp.Tool{
			Name:        "resume_queue",
			Description: "Resume a paused queue.",
			Annotations: mutate(false, true),
		}, t.resumeQueue)
	}
	return server
}

// Handler serves server over MCP Streamable HTTP. Mount it behind your
// application's authentication, like the admin dashboard.
func Handler(server *mcp.Server) http.Handler {
	return mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)
}

type tools struct {
	exec       driver.Executor
	driverName string
	cfg        config
}

// QueueInfo is one queue in list_queues.
type QueueInfo struct {
	Name   string           `json:"name"`
	Paused bool             `json:"paused"`
	States map[string]int64 `json:"states,omitempty" jsonschema:"job counts by state"`
	Total  *int64           `json:"total,omitempty" jsonschema:"all jobs in the queue"`
}

// ListQueuesInput is the input of list_queues.
type ListQueuesInput struct {
	Limit  int    `json:"limit,omitempty" jsonschema:"maximum queues to return (default 50, max 500)"`
	Cursor string `json:"cursor,omitempty" jsonschema:"next_cursor from the previous page"`
}

// ListQueuesOutput is the output of list_queues.
type ListQueuesOutput struct {
	Queues     []QueueInfo `json:"queues"`
	NextCursor string      `json:"next_cursor,omitempty" jsonschema:"pass as cursor to get the next page; empty on the last page"`
}

func (t *tools) listQueues(ctx context.Context, _ *mcp.CallToolRequest, in ListQueuesInput) (*mcp.CallToolResult, ListQueuesOutput, error) {
	limit := clampLimit(in.Limit)
	rows, err := t.exec.QueueList(ctx, driver.QueueListParams{Limit: limit, Cursor: in.Cursor})
	if err != nil {
		return nil, ListQueuesOutput{}, toolError(err)
	}
	adminExec, hasStats := t.exec.(driver.AdminExecutor)
	out := ListQueuesOutput{Queues: make([]QueueInfo, 0, len(rows))}
	for _, row := range rows {
		info := QueueInfo{Name: row.Name, Paused: row.Paused}
		if hasStats {
			stats, err := adminExec.QueueStats(ctx, row.Name)
			if err != nil {
				return nil, ListQueuesOutput{}, toolError(err)
			}
			info.States, info.Total = stateCounts(stats), &stats.Total
		}
		out.Queues = append(out.Queues, info)
	}
	if len(rows) == limit && limit > 0 {
		out.NextCursor = driver.EncodeQueueCursor(*rows[len(rows)-1])
	}
	return nil, out, nil
}

// AttemptErrorInfo is the error of one failed attempt.
type AttemptErrorInfo struct {
	Attempt int       `json:"attempt"`
	At      time.Time `json:"at"`
	Error   string    `json:"error"`
	Trace   string    `json:"trace,omitempty" jsonschema:"panic stack trace, hidden unless the redactor exposes it"`
}

// JobInfo is a job as shown to the agent.
type JobInfo struct {
	ID          string             `json:"id"`
	Queue       string             `json:"queue"`
	Kind        string             `json:"kind"`
	State       string             `json:"state" jsonschema:"available, scheduled, running, retryable, completed, discarded or cancelled"`
	Priority    int                `json:"priority"`
	Attempt     int                `json:"attempt" jsonschema:"number of attempts started"`
	MaxRetry    int                `json:"max_retry"`
	RunAt       time.Time          `json:"run_at"`
	CreatedAt   time.Time          `json:"created_at"`
	AttemptedAt *time.Time         `json:"attempted_at,omitempty"`
	FinalizedAt *time.Time         `json:"finalized_at,omitempty"`
	Timeout     string             `json:"timeout,omitempty"`
	Tags        []string           `json:"tags,omitempty"`
	TraceID     string             `json:"trace_id,omitempty" jsonschema:"OpenTelemetry trace id of the enqueue, when the client set ContextTags to otelgoncordia.TraceContextTags; search it in your tracing backend"`
	WorkerID    string             `json:"worker_id,omitempty"`
	Args        string             `json:"args,omitempty" jsonschema:"JSON arguments, hidden unless the redactor exposes them"`
	UniqueKey   string             `json:"unique_key,omitempty"`
	Errors      []AttemptErrorInfo `json:"errors,omitempty"`
}

// ListJobsInput is the input of list_jobs.
type ListJobsInput struct {
	Queue  string `json:"queue,omitempty" jsonschema:"only jobs in this queue"`
	State  string `json:"state,omitempty" jsonschema:"only jobs in this state: available, scheduled, running, retryable, completed, discarded or cancelled"`
	Kind   string `json:"kind,omitempty" jsonschema:"only jobs of this kind"`
	Limit  int    `json:"limit,omitempty" jsonschema:"maximum jobs to return (default 50, max 500)"`
	Cursor string `json:"cursor,omitempty" jsonschema:"next_cursor from the previous page"`
}

// ListJobsOutput is the output of list_jobs.
type ListJobsOutput struct {
	Jobs       []JobInfo `json:"jobs"`
	NextCursor string    `json:"next_cursor,omitempty" jsonschema:"pass as cursor to get the next page; empty on the last page"`
}

func (t *tools) listJobs(ctx context.Context, _ *mcp.CallToolRequest, in ListJobsInput) (*mcp.CallToolResult, ListJobsOutput, error) {
	adminExec, ok := t.exec.(driver.AdminExecutor)
	if !ok {
		return nil, ListJobsOutput{}, fmt.Errorf("driver %q does not support job listing", t.driverName)
	}
	state, err := parseState(in.State)
	if err != nil {
		return nil, ListJobsOutput{}, err
	}
	limit := clampLimit(in.Limit)
	rows, err := adminExec.JobList(ctx, driver.JobListParams{Queue: in.Queue, State: state, Kind: in.Kind, Limit: limit, Cursor: in.Cursor})
	if err != nil {
		return nil, ListJobsOutput{}, toolError(err)
	}
	out := ListJobsOutput{Jobs: make([]JobInfo, 0, len(rows))}
	for _, row := range rows {
		out.Jobs = append(out.Jobs, jobInfo(t.cfg.redact(row)))
	}
	if len(rows) == limit && limit > 0 {
		out.NextCursor = driver.EncodeJobCursor(rows[len(rows)-1])
	}
	return nil, out, nil
}

// JobIDInput identifies one job.
type JobIDInput struct {
	ID string `json:"id" jsonschema:"job id"`
}

func (t *tools) getJob(ctx context.Context, _ *mcp.CallToolRequest, in JobIDInput) (*mcp.CallToolResult, JobInfo, error) {
	if in.ID == "" {
		return nil, JobInfo{}, errors.New("id is required")
	}
	row, err := t.exec.JobGetByID(ctx, in.ID)
	if err != nil {
		return nil, JobInfo{}, toolError(err)
	}
	if row == nil {
		return nil, JobInfo{}, fmt.Errorf("job %q not found", in.ID)
	}
	return nil, jobInfo(t.cfg.redact(*row)), nil
}

// MetricsInput is the (empty) input of get_metrics.
type MetricsInput struct{}

// QueueMetrics are the job counts of one queue.
type QueueMetrics struct {
	Queue  string           `json:"queue"`
	Paused bool             `json:"paused"`
	States map[string]int64 `json:"states"`
	Total  int64            `json:"total"`
}

// MetricsOutput is the output of get_metrics.
type MetricsOutput struct {
	Queues     []QueueMetrics `json:"queues"`
	Prometheus string         `json:"prometheus,omitempty" jsonschema:"extra metrics registered by the application, Prometheus text format"`
}

func (t *tools) getMetrics(ctx context.Context, _ *mcp.CallToolRequest, _ MetricsInput) (*mcp.CallToolResult, MetricsOutput, error) {
	adminExec, ok := t.exec.(driver.AdminExecutor)
	if !ok {
		return nil, MetricsOutput{}, fmt.Errorf("driver %q does not support queue metrics", t.driverName)
	}
	out := MetricsOutput{Queues: []QueueMetrics{}}
	cursor := ""
	for {
		rows, err := t.exec.QueueList(ctx, driver.QueueListParams{Limit: maxLimit, Cursor: cursor})
		if err != nil {
			return nil, MetricsOutput{}, toolError(err)
		}
		for _, row := range rows {
			stats, err := adminExec.QueueStats(ctx, row.Name)
			if err != nil {
				return nil, MetricsOutput{}, toolError(err)
			}
			out.Queues = append(out.Queues, QueueMetrics{Queue: row.Name, Paused: row.Paused, States: stateCounts(stats), Total: stats.Total})
		}
		if len(rows) < maxLimit {
			break
		}
		cursor = driver.EncodeQueueCursor(*rows[len(rows)-1])
	}
	if len(t.cfg.extra) > 0 {
		var b strings.Builder
		for _, w := range t.cfg.extra {
			if err := w.WritePrometheus(&b); err != nil {
				return nil, MetricsOutput{}, err
			}
		}
		out.Prometheus = b.String()
	}
	return nil, out, nil
}

// ActionOutput is the result of a write tool.
type ActionOutput struct {
	Status string `json:"status"`
}

var ok = ActionOutput{Status: "ok"}

func (t *tools) retryJob(ctx context.Context, _ *mcp.CallToolRequest, in JobIDInput) (*mcp.CallToolResult, ActionOutput, error) {
	if in.ID == "" {
		return nil, ActionOutput{}, errors.New("id is required")
	}
	if err := t.exec.JobReschedule(ctx, driver.RescheduleParams{ID: in.ID, RunAt: t.cfg.clock.Now()}); err != nil {
		return nil, ActionOutput{}, toolError(err)
	}
	return nil, ok, nil
}

// RescheduleInput is the input of reschedule_job.
type RescheduleInput struct {
	ID    string    `json:"id" jsonschema:"job id"`
	RunAt time.Time `json:"run_at" jsonschema:"new run time, RFC 3339"`
}

func (t *tools) rescheduleJob(ctx context.Context, _ *mcp.CallToolRequest, in RescheduleInput) (*mcp.CallToolResult, ActionOutput, error) {
	if in.ID == "" || in.RunAt.IsZero() {
		return nil, ActionOutput{}, errors.New("id and run_at are required")
	}
	if err := t.exec.JobReschedule(ctx, driver.RescheduleParams{ID: in.ID, RunAt: in.RunAt}); err != nil {
		return nil, ActionOutput{}, toolError(err)
	}
	return nil, ok, nil
}

func (t *tools) cancelJob(ctx context.Context, _ *mcp.CallToolRequest, in JobIDInput) (*mcp.CallToolResult, ActionOutput, error) {
	if in.ID == "" {
		return nil, ActionOutput{}, errors.New("id is required")
	}
	if err := t.exec.JobCancel(ctx, in.ID); err != nil {
		return nil, ActionOutput{}, toolError(err)
	}
	return nil, ok, nil
}

func (t *tools) deleteJob(ctx context.Context, _ *mcp.CallToolRequest, in JobIDInput) (*mcp.CallToolResult, ActionOutput, error) {
	if in.ID == "" {
		return nil, ActionOutput{}, errors.New("id is required")
	}
	if err := t.exec.JobDelete(ctx, in.ID); err != nil {
		return nil, ActionOutput{}, toolError(err)
	}
	return nil, ok, nil
}

// QueueInput identifies one queue.
type QueueInput struct {
	Queue string `json:"queue" jsonschema:"queue name"`
}

func (t *tools) pauseQueue(ctx context.Context, _ *mcp.CallToolRequest, in QueueInput) (*mcp.CallToolResult, ActionOutput, error) {
	if in.Queue == "" {
		return nil, ActionOutput{}, errors.New("queue is required")
	}
	if err := t.exec.QueuePause(ctx, in.Queue); err != nil {
		return nil, ActionOutput{}, toolError(err)
	}
	return nil, ok, nil
}

func (t *tools) resumeQueue(ctx context.Context, _ *mcp.CallToolRequest, in QueueInput) (*mcp.CallToolResult, ActionOutput, error) {
	if in.Queue == "" {
		return nil, ActionOutput{}, errors.New("queue is required")
	}
	if err := t.exec.QueueResume(ctx, in.Queue); err != nil {
		return nil, ActionOutput{}, toolError(err)
	}
	return nil, ok, nil
}

func jobInfo(row driver.JobRow) JobInfo {
	info := JobInfo{
		ID: row.ID, Queue: row.Queue, Kind: row.Kind, State: string(row.State),
		Priority: row.Priority, Attempt: row.AttemptNum, MaxRetry: row.MaxRetry,
		RunAt: row.RunAt.UTC(), CreatedAt: row.CreatedAt.UTC(),
		AttemptedAt: utc(row.AttemptedAt), FinalizedAt: utc(row.FinalizedAt),
		Tags: row.Tags, WorkerID: row.WorkerID, UniqueKey: row.UniqueKey,
	}
	info.TraceID = traceID(row.Tags)
	if row.Timeout > 0 {
		info.Timeout = row.Timeout.String()
	}
	if len(row.Args) > 0 {
		info.Args = string(row.Args)
	}
	for _, e := range row.Errors {
		info.Errors = append(info.Errors, AttemptErrorInfo{Attempt: e.Attempt, At: e.At.UTC(), Error: e.Error, Trace: e.Trace})
	}
	return info
}

// traceID returns the trace id of a "traceparent=00-<trace id>-<span id>-<flags>"
// job tag, or "".
func traceID(tags []string) string {
	for _, tag := range tags {
		value, ok := strings.CutPrefix(tag, "traceparent=")
		if !ok {
			continue
		}
		parts := strings.Split(value, "-")
		if len(parts) == 4 && len(parts[1]) == 32 && isHex(parts[1]) && strings.Trim(parts[1], "0") != "" {
			return parts[1]
		}
	}
	return ""
}

func isHex(s string) bool {
	for _, r := range s {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

func defaultJobRedactor(row driver.JobRow) driver.JobRow {
	row.Args = nil
	row.UniqueKey = ""
	row.Errors = append([]driver.AttemptError(nil), row.Errors...)
	for i := range row.Errors {
		row.Errors[i].Trace = ""
	}
	return row
}

var states = []driver.JobState{
	driver.JobStateAvailable, driver.JobStateScheduled, driver.JobStateRunning, driver.JobStateRetryable,
	driver.JobStateCompleted, driver.JobStateDiscarded, driver.JobStateCancelled,
}

func parseState(s string) (driver.JobState, error) {
	if s == "" {
		return "", nil
	}
	for _, state := range states {
		if string(state) == s {
			return state, nil
		}
	}
	return "", fmt.Errorf("unknown state %q", s)
}

func stateCounts(stats driver.QueueStats) map[string]int64 {
	out := make(map[string]int64, len(stats.States))
	for state, n := range stats.States {
		out[string(state)] = n
	}
	return out
}

func toolError(err error) error {
	switch {
	case errors.Is(err, driver.ErrNotFound):
		return fmt.Errorf("not found: %w", err)
	case errors.Is(err, driver.ErrInvalidCursor):
		return fmt.Errorf("invalid cursor: %w", err)
	case errors.Is(err, driver.ErrUnsupported):
		return fmt.Errorf("not supported by this driver: %w", err)
	}
	return err
}

func clampLimit(limit int) int {
	if limit <= 0 {
		return defaultLimit
	}
	return min(limit, maxLimit)
}

func utc(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	u := t.UTC()
	return &u
}

func ptr[T any](v T) *T { return &v }
