package goncordia

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
)

// RetentionMetrics counts retention passes and renders them in the
// Prometheus text format. Use it as WorkerConfig.Observer (combine it with
// other observers through WorkerObservers) and serve it with
// admin.WithMetrics. Only the instance holding the retention lease runs
// passes, so sum the series across instances.
//
// RetentionMetrics is safe for concurrent use.
type RetentionMetrics struct {
	NopWorkerObserver

	mu             sync.Mutex
	passesOK       uint64
	passesError    uint64
	jobsDeleted    uint64
	resultsDeleted uint64
	lastSuccess    float64 // unix seconds
	lastDuration   float64 // seconds
}

// NewRetentionMetrics creates empty retention metrics.
func NewRetentionMetrics() *RetentionMetrics { return &RetentionMetrics{} }

// RetentionPassed implements RetentionObserver.
func (m *RetentionMetrics) RetentionPassed(_ context.Context, event RetentionPassEvent) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// A failed pass may still have deleted part of its work.
	m.jobsDeleted += uint64(event.Result.Deleted)
	m.resultsDeleted += uint64(event.Result.ResultsDeleted)
	m.lastDuration = event.FinishedAt.Sub(event.StartedAt).Seconds()
	if event.Err != nil {
		m.passesError++
		return
	}
	m.passesOK++
	m.lastSuccess = float64(event.FinishedAt.UnixNano()) / 1e9
}

// WritePrometheus writes the metrics in the Prometheus text exposition format.
func (m *RetentionMetrics) WritePrometheus(w io.Writer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var b strings.Builder
	series := func(name, kind, help string, lines ...string) {
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, kind)
		for _, line := range lines {
			b.WriteString(line + "\n")
		}
	}
	float := func(v float64) string { return strconv.FormatFloat(v, 'g', -1, 64) }
	series("goncordia_retention_passes_total", "counter", "Retention passes run by this instance as leader.",
		fmt.Sprintf(`goncordia_retention_passes_total{status="ok"} %d`, m.passesOK),
		fmt.Sprintf(`goncordia_retention_passes_total{status="error"} %d`, m.passesError))
	series("goncordia_retention_jobs_deleted_total", "counter", "Terminal jobs deleted by retention.",
		fmt.Sprintf("goncordia_retention_jobs_deleted_total %d", m.jobsDeleted))
	series("goncordia_retention_results_deleted_total", "counter", "Future results deleted by retention.",
		fmt.Sprintf("goncordia_retention_results_deleted_total %d", m.resultsDeleted))
	series("goncordia_retention_last_success_timestamp_seconds", "gauge", "Unix time of the last successful pass; 0 before the first.",
		"goncordia_retention_last_success_timestamp_seconds "+float(m.lastSuccess))
	series("goncordia_retention_last_pass_duration_seconds", "gauge", "Duration of the last pass.",
		"goncordia_retention_last_pass_duration_seconds "+float(m.lastDuration))
	_, err := io.WriteString(w, b.String())
	return err
}

var (
	_ WorkerObserver    = (*RetentionMetrics)(nil)
	_ RetentionObserver = (*RetentionMetrics)(nil)
)
