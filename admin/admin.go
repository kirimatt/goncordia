// Package admin provides an embeddable operational HTTP API and dashboard.
// Mount it behind your application's authentication and authorization layer.
package admin

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	gonclock "github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/driver"
)

//go:embed dashboard.html
var dashboardHTML []byte

type handler[TTx any] struct {
	driver driver.Driver[TTx]
	clock  gonclock.Clock
}

type config struct{ clock gonclock.Clock }

// Option configures the admin handler.
type Option func(*config)

// WithClock controls timestamps created by administrative retry actions.
func WithClock(clk gonclock.Clock) Option {
	return func(cfg *config) { cfg.clock = clk }
}

// New returns an HTTP handler exposing the dashboard, JSON admin API,
// health/readiness probes, and Prometheus-compatible queue metrics.
func New[TTx any](d driver.Driver[TTx], opts ...Option) http.Handler {
	cfg := config{clock: gonclock.Real{}}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.clock == nil {
		cfg.clock = gonclock.Real{}
	}
	return &handler[TTx]{driver: d, clock: cfg.clock}
}

func (h *handler[TTx]) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	switch {
	case r.URL.Path == "/" && r.Method == http.MethodGet:
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(dashboardHTML)
	case (r.URL.Path == "/healthz" || r.URL.Path == "/readyz") && r.Method == http.MethodGet:
		h.health(w, r)
	case r.URL.Path == "/metrics" && r.Method == http.MethodGet:
		h.metrics(w, r)
	case r.URL.Path == "/api/queues" && r.Method == http.MethodGet:
		h.queues(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/queues/") && r.Method == http.MethodPost:
		h.queueAction(w, r)
	case r.URL.Path == "/api/jobs" && r.Method == http.MethodGet:
		h.jobs(w, r)
	case strings.HasPrefix(r.URL.Path, "/api/jobs/") && r.Method == http.MethodPost:
		h.jobAction(w, r)
	default:
		http.NotFound(w, r)
	}
}

func (h *handler[TTx]) health(w http.ResponseWriter, r *http.Request) {
	if _, err := h.driver.Executor().QueueList(r.Context(), driver.QueueListParams{Limit: 1}); err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok", "driver": h.driver.Name()})
}

type queueView struct {
	*driver.QueueRow
	Stats *driver.QueueStats `json:"stats,omitempty"`
}

type queuePage struct {
	Items      []queueView `json:"items"`
	NextCursor string      `json:"next_cursor,omitempty"`
	HasMore    bool        `json:"has_more"`
}

func (h *handler[TTx]) queues(w http.ResponseWriter, r *http.Request) {
	limit := parseLimit(r, 100)
	params := driver.QueueListParams{Limit: limit, Cursor: r.URL.Query().Get("cursor")}
	rows, err := h.driver.Executor().QueueList(r.Context(), params)
	if err != nil {
		writeDriverError(w, err)
		return
	}
	nextCursor, hasMore := "", false
	if len(rows) == limit && len(rows) > 0 {
		candidate := driver.EncodeQueueCursor(*rows[len(rows)-1])
		params.Cursor, params.Limit = candidate, 1
		following, probeErr := h.driver.Executor().QueueList(r.Context(), params)
		if probeErr != nil {
			writeDriverError(w, probeErr)
			return
		}
		if len(following) > 0 {
			nextCursor, hasMore = candidate, true
		}
	}
	adminExec, hasStats := h.driver.Executor().(driver.AdminExecutor)
	views := make([]queueView, 0, len(rows))
	for _, row := range rows {
		view := queueView{QueueRow: row}
		if hasStats {
			stats, err := adminExec.QueueStats(r.Context(), row.Name)
			if err != nil {
				writeDriverError(w, err)
				return
			}
			view.Stats = &stats
		}
		views = append(views, view)
	}
	writeJSON(w, http.StatusOK, queuePage{Items: views, NextCursor: nextCursor, HasMore: hasMore})
}

func (h *handler[TTx]) queueAction(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/queues/"), "/")
	if len(parts) != 2 || parts[0] == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("expected /api/queues/{name}/{pause|resume}"))
		return
	}
	var err error
	switch parts[1] {
	case "pause":
		err = h.driver.Executor().QueuePause(r.Context(), parts[0])
	case "resume":
		err = h.driver.Executor().QueueResume(r.Context(), parts[0])
	default:
		writeError(w, http.StatusBadRequest, fmt.Errorf("unknown queue action %q", parts[1]))
		return
	}
	if err != nil {
		writeDriverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *handler[TTx]) jobs(w http.ResponseWriter, r *http.Request) {
	if id := r.URL.Query().Get("id"); id != "" {
		row, err := h.driver.Executor().JobGetByID(r.Context(), id)
		if err != nil {
			writeDriverError(w, err)
			return
		}
		if row == nil {
			writeDriverError(w, fmt.Errorf("%w: job %q", driver.ErrNotFound, id))
			return
		}
		writeJSON(w, http.StatusOK, row)
		return
	}
	adminExec, ok := h.driver.Executor().(driver.AdminExecutor)
	if !ok {
		writeDriverError(w, fmt.Errorf("%w: driver %q does not support job listing", driver.ErrUnsupported, h.driver.Name()))
		return
	}
	limit := parseLimit(r, 100)
	params := driver.JobListParams{
		Queue:  r.URL.Query().Get("queue"),
		State:  driver.JobState(r.URL.Query().Get("state")),
		Kind:   r.URL.Query().Get("kind"),
		Cursor: r.URL.Query().Get("cursor"),
		Limit:  limit,
	}
	rows, err := adminExec.JobList(r.Context(), params)
	if err != nil {
		writeDriverError(w, err)
		return
	}
	if rows == nil {
		rows = []driver.JobRow{}
	}
	nextCursor, hasMore := "", false
	if len(rows) == limit && len(rows) > 0 {
		candidate := driver.EncodeJobCursor(rows[len(rows)-1])
		params.Cursor, params.Limit = candidate, 1
		following, probeErr := adminExec.JobList(r.Context(), params)
		if probeErr != nil {
			writeDriverError(w, probeErr)
			return
		}
		if len(following) > 0 {
			nextCursor, hasMore = candidate, true
		}
	}
	writeJSON(w, http.StatusOK, driver.JobPage{Items: rows, NextCursor: nextCursor, HasMore: hasMore})
}

func (h *handler[TTx]) jobAction(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/jobs/"), "/")
	if len(parts) != 2 || parts[0] == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("expected /api/jobs/{id}/{action}"))
		return
	}
	id, action := parts[0], parts[1]
	var err error
	switch action {
	case "cancel":
		err = h.driver.Executor().JobCancel(r.Context(), id)
	case "delete":
		err = h.driver.Executor().JobDelete(r.Context(), id)
	case "retry":
		err = h.driver.Executor().JobReschedule(r.Context(), driver.RescheduleParams{ID: id, RunAt: h.clock.Now()})
	case "reschedule":
		var body struct {
			RunAt time.Time `json:"run_at"`
		}
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if decodeErr := json.NewDecoder(r.Body).Decode(&body); decodeErr != nil || body.RunAt.IsZero() {
			writeError(w, http.StatusBadRequest, fmt.Errorf("valid run_at is required"))
			return
		}
		err = h.driver.Executor().JobReschedule(r.Context(), driver.RescheduleParams{ID: id, RunAt: body.RunAt})
	default:
		writeError(w, http.StatusBadRequest, fmt.Errorf("unknown job action %q", action))
		return
	}
	if err != nil {
		writeDriverError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *handler[TTx]) metrics(w http.ResponseWriter, r *http.Request) {
	adminExec, ok := h.driver.Executor().(driver.AdminExecutor)
	if !ok {
		writeError(w, http.StatusNotImplemented, fmt.Errorf("driver %q does not support queue metrics", h.driver.Name()))
		return
	}
	queues, err := h.driver.Executor().QueueList(r.Context(), driver.QueueListParams{Limit: 1000})
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, err)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = fmt.Fprintln(w, "# HELP goncordia_queue_jobs Number of jobs by queue and state.")
	_, _ = fmt.Fprintln(w, "# TYPE goncordia_queue_jobs gauge")
	for _, queue := range queues {
		stats, err := adminExec.QueueStats(r.Context(), queue.Name)
		if err != nil {
			continue
		}
		for state, count := range stats.States {
			_, _ = fmt.Fprintf(w, "goncordia_queue_jobs{queue=%q,state=%q} %d\n", queue.Name, state, count)
		}
	}
}

func parseLimit(r *http.Request, fallback int) int {
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil || limit <= 0 {
		return fallback
	}
	if limit > 1000 {
		return 1000
	}
	return limit
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeDriverError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	switch {
	case errors.Is(err, driver.ErrInvalidCursor):
		status = http.StatusBadRequest
	case errors.Is(err, driver.ErrNotFound):
		status = http.StatusNotFound
	case errors.Is(err, driver.ErrConflict), errors.Is(err, driver.ErrStaleClaim):
		status = http.StatusConflict
	case errors.Is(err, driver.ErrUnsupported):
		status = http.StatusNotImplemented
	}
	writeError(w, status, err)
}

// Probe can be used without HTTP to verify that the backing store is reachable.
func Probe(ctx context.Context, exec driver.Executor) error {
	_, err := exec.QueueList(ctx, driver.QueueListParams{Limit: 1})
	return err
}
