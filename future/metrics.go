package future

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/kirimatt/goncordia/v2/clock"
)

var (
	durationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300}
	sizeBuckets     = []float64{64, 256, 1 << 10, 4 << 10, 16 << 10, 64 << 10, 256 << 10, 1 << 20, 4 << 20}
)

// Metrics is an Observer that aggregates futures metrics in memory and
// renders them in the Prometheus text format. Pass it to admin.WithMetrics to
// serve them from the admin /metrics endpoint, and combine it with other
// observers through Observers. Values cover the current process only;
// Prometheus aggregates them across instances.
//
// Metrics is safe for concurrent use.
type Metrics struct {
	clock clock.Clock

	mu       sync.Mutex
	inFlight int64
	awaits   map[[2]string]*histogram // kind, outcome
	polls    map[string]uint64        // kind
	wakeups  map[string]uint64        // kind
	stores   map[[2]string]*histogram // kind, status
	sizes    map[string]*histogram    // kind
}

// NewMetrics creates an empty Metrics. clk measures durations; nil uses
// clock.Real{}. Pass the same clock.Manual as the Manager in tests.
func NewMetrics(clk clock.Clock) *Metrics {
	if clk == nil {
		clk = clock.Real{}
	}
	return &Metrics{
		clock:   clk,
		awaits:  make(map[[2]string]*histogram),
		polls:   make(map[string]uint64),
		wakeups: make(map[string]uint64),
		stores:  make(map[[2]string]*histogram),
		sizes:   make(map[string]*histogram),
	}
}

// StartAwait implements Observer.
func (m *Metrics) StartAwait(ctx context.Context, start AwaitStart) (context.Context, func(AwaitFinish)) {
	began := m.clock.Now()
	if start.Blocking {
		m.mu.Lock()
		m.inFlight++
		m.mu.Unlock()
	}
	return ctx, func(finish AwaitFinish) {
		kind := start.Kind
		if kind == "" && finish.Job != nil {
			kind = finish.Job.Kind
		}
		elapsed := m.clock.Since(began).Seconds()
		m.mu.Lock()
		defer m.mu.Unlock()
		if start.Blocking {
			m.inFlight--
		}
		observe(m.awaits, [2]string{kind, string(finish.Outcome)}, durationBuckets, elapsed)
		m.polls[kind] += uint64(finish.Polls)
		m.wakeups[kind] += uint64(finish.Wakeups)
	}
}

// StartStore implements Observer.
func (m *Metrics) StartStore(ctx context.Context, start StoreStart) (context.Context, func(StoreFinish)) {
	began := m.clock.Now()
	return ctx, func(finish StoreFinish) {
		status := "ok"
		if finish.Err != nil {
			status = "error"
		}
		elapsed := m.clock.Since(began).Seconds()
		m.mu.Lock()
		defer m.mu.Unlock()
		observe(m.stores, [2]string{start.Kind, status}, durationBuckets, elapsed)
		if finish.Err == nil {
			observe(m.sizes, start.Kind, sizeBuckets, float64(start.Bytes))
		}
	}
}

// WritePrometheus writes the metrics in the Prometheus text exposition
// format. Series are sorted, so the output is stable between scrapes.
func (m *Metrics) WritePrometheus(w io.Writer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	var b strings.Builder

	b.WriteString("# HELP goncordia_future_awaits_in_flight Blocking Await calls currently waiting.\n")
	b.WriteString("# TYPE goncordia_future_awaits_in_flight gauge\n")
	fmt.Fprintf(&b, "goncordia_future_awaits_in_flight %d\n", m.inFlight)

	writeHistograms(&b, "goncordia_future_await_duration_seconds",
		"Time from Await or TryGet start to its outcome.", m.awaits, []string{"kind", "outcome"})
	writeCounters(&b, "goncordia_future_await_polls_total", "Storage reads made by Await and TryGet.", m.polls)
	writeCounters(&b, "goncordia_future_await_wakeups_total", "Local completion signals that woke Await.", m.wakeups)
	writeHistograms(&b, "goncordia_future_store_duration_seconds",
		"Time to write a handler result.", m.stores, []string{"kind", "status"})
	writeHistograms(&b, "goncordia_future_result_size_bytes",
		"Encoded size of stored handler results.", m.sizes, []string{"kind"})

	_, err := io.WriteString(w, b.String())
	return err
}

type histogram struct {
	buckets []float64
	counts  []uint64 // per bucket, not cumulative
	sum     float64
	count   uint64
}

func observe[K comparable](series map[K]*histogram, key K, buckets []float64, v float64) {
	h := series[key]
	if h == nil {
		h = &histogram{buckets: buckets, counts: make([]uint64, len(buckets))}
		series[key] = h
	}
	for i, upper := range buckets {
		if v <= upper {
			h.counts[i]++
			break
		}
	}
	h.sum += v
	h.count++
}

func labelValues[K comparable](key K) []string {
	switch k := any(key).(type) {
	case string:
		return []string{k}
	case [2]string:
		return k[:]
	}
	panic("unsupported label key")
}

func writeHistograms[K comparable](b *strings.Builder, name, help string, series map[K]*histogram, labels []string) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s histogram\n", name, help, name)
	keys := make([]K, 0, len(series))
	for k := range series {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return strings.Join(labelValues(keys[i]), "\x00") < strings.Join(labelValues(keys[j]), "\x00")
	})
	for _, k := range keys {
		h := series[k]
		base := formatLabels(labels, labelValues(k))
		var cumulative uint64
		for i, upper := range h.buckets {
			cumulative += h.counts[i]
			fmt.Fprintf(b, "%s_bucket{%s,le=%q} %d\n", name, base, strconv.FormatFloat(upper, 'g', -1, 64), cumulative)
		}
		fmt.Fprintf(b, "%s_bucket{%s,le=\"+Inf\"} %d\n", name, base, h.count)
		fmt.Fprintf(b, "%s_sum{%s} %s\n", name, base, strconv.FormatFloat(h.sum, 'g', -1, 64))
		fmt.Fprintf(b, "%s_count{%s} %d\n", name, base, h.count)
	}
}

func writeCounters(b *strings.Builder, name, help string, series map[string]uint64) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s counter\n", name, help, name)
	keys := make([]string, 0, len(series))
	for k := range series {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(b, "%s{%s} %d\n", name, formatLabels([]string{"kind"}, []string{k}), series[k])
	}
}

var labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

func formatLabels(names, values []string) string {
	parts := make([]string, len(names))
	for i, name := range names {
		parts[i] = name + `="` + labelEscaper.Replace(values[i]) + `"`
	}
	return strings.Join(parts, ",")
}

// Observers fans futures events out to several observers in order, for
// example OpenTelemetry instrumentation and Metrics. Nil observers are
// skipped.
func Observers(observers ...Observer) Observer {
	list := make(observerList, 0, len(observers))
	for _, o := range observers {
		if o != nil {
			list = append(list, o)
		}
	}
	return list
}

type observerList []Observer

func (l observerList) StartAwait(ctx context.Context, start AwaitStart) (context.Context, func(AwaitFinish)) {
	finishes := make([]func(AwaitFinish), len(l))
	for i, o := range l {
		ctx, finishes[i] = o.StartAwait(ctx, start)
	}
	return ctx, func(finish AwaitFinish) {
		for i := len(finishes) - 1; i >= 0; i-- {
			finishes[i](finish)
		}
	}
}

func (l observerList) StartStore(ctx context.Context, start StoreStart) (context.Context, func(StoreFinish)) {
	finishes := make([]func(StoreFinish), len(l))
	for i, o := range l {
		ctx, finishes[i] = o.StartStore(ctx, start)
	}
	return ctx, func(finish StoreFinish) {
		for i := len(finishes) - 1; i >= 0; i-- {
			finishes[i](finish)
		}
	}
}

var (
	_ Observer = (*Metrics)(nil)
	_ Observer = observerList(nil)
)
