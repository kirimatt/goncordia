package future

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/driver"
)

func render(t *testing.T, m *Metrics) string {
	t.Helper()
	var b strings.Builder
	if err := m.WritePrometheus(&b); err != nil {
		t.Fatal(err)
	}
	return b.String()
}

func requireLines(t *testing.T, body string, lines ...string) {
	t.Helper()
	for _, line := range lines {
		if !strings.Contains(body, line+"\n") {
			t.Errorf("missing %q in:\n%s", line, body)
		}
	}
}

func TestMetricsAwaitDurationsInFlightAndCounters(t *testing.T) {
	clk := clock.NewManual(time.Time{})
	m := NewMetrics(clk)
	ctx := context.Background()

	_, finish := m.StartAwait(ctx, AwaitStart{JobID: "1", Blocking: true})
	requireLines(t, render(t, m), "goncordia_future_awaits_in_flight 1")
	clk.Advance(3 * time.Second)
	// Kind is learned from the job row when the Future did not know it.
	finish(AwaitFinish{Job: &driver.JobRow{Kind: "sum"}, Outcome: OutcomeCompleted, Polls: 2, Wakeups: 1})

	_, peek := m.StartAwait(ctx, AwaitStart{JobID: "2", Kind: "sum"}) // TryGet: not in flight
	requireLines(t, render(t, m), "goncordia_future_awaits_in_flight 0")
	peek(AwaitFinish{Outcome: OutcomePending, Polls: 1})

	requireLines(t, render(t, m),
		`goncordia_future_await_duration_seconds_bucket{kind="sum",outcome="completed",le="2.5"} 0`,
		`goncordia_future_await_duration_seconds_bucket{kind="sum",outcome="completed",le="5"} 1`,
		`goncordia_future_await_duration_seconds_bucket{kind="sum",outcome="completed",le="+Inf"} 1`,
		`goncordia_future_await_duration_seconds_sum{kind="sum",outcome="completed"} 3`,
		`goncordia_future_await_duration_seconds_count{kind="sum",outcome="pending"} 1`,
		`goncordia_future_await_polls_total{kind="sum"} 3`,
		`goncordia_future_await_wakeups_total{kind="sum"} 1`,
		"# TYPE goncordia_future_await_duration_seconds histogram",
	)
}

func TestMetricsStoreRecordsSizeOnlyOnSuccess(t *testing.T) {
	clk := clock.NewManual(time.Time{})
	m := NewMetrics(clk)
	ctx := context.Background()

	_, ok := m.StartStore(ctx, StoreStart{Kind: "sum", Bytes: 300})
	clk.Advance(20 * time.Millisecond)
	ok(StoreFinish{})
	_, failed := m.StartStore(ctx, StoreStart{Kind: "sum", Bytes: 999})
	failed(StoreFinish{Err: errors.New("disk full")})

	body := render(t, m)
	requireLines(t, body,
		`goncordia_future_store_duration_seconds_count{kind="sum",status="ok"} 1`,
		`goncordia_future_store_duration_seconds_count{kind="sum",status="error"} 1`,
		`goncordia_future_store_duration_seconds_bucket{kind="sum",status="ok",le="0.025"} 1`,
		`goncordia_future_result_size_bytes_bucket{kind="sum",le="256"} 0`,
		`goncordia_future_result_size_bytes_bucket{kind="sum",le="1024"} 1`,
		`goncordia_future_result_size_bytes_count{kind="sum"} 1`,
		`goncordia_future_result_size_bytes_sum{kind="sum"} 300`,
	)
}

func TestMetricsEscapesLabelsAndSortsSeries(t *testing.T) {
	m := NewMetrics(clock.NewManual(time.Time{}))
	for _, kind := range []string{"b", `a"q\` + "\n"} {
		_, finish := m.StartAwait(context.Background(), AwaitStart{Kind: kind, Blocking: true})
		finish(AwaitFinish{Outcome: OutcomeNotFound})
	}
	body := render(t, m)
	escaped := `goncordia_future_await_polls_total{kind="a\"q\\\n"} 0`
	requireLines(t, body, escaped, `goncordia_future_await_polls_total{kind="b"} 0`)
	if strings.Index(body, escaped) > strings.Index(body, `goncordia_future_await_polls_total{kind="b"}`) {
		t.Fatal("series are not sorted")
	}
}

type ctxKey string

type orderObserver struct {
	name string
	log  *[]string
}

func (o orderObserver) StartAwait(ctx context.Context, _ AwaitStart) (context.Context, func(AwaitFinish)) {
	*o.log = append(*o.log, "start "+o.name)
	return context.WithValue(ctx, ctxKey(o.name), true), func(AwaitFinish) { *o.log = append(*o.log, "finish "+o.name) }
}

func (o orderObserver) StartStore(ctx context.Context, _ StoreStart) (context.Context, func(StoreFinish)) {
	*o.log = append(*o.log, "store "+o.name)
	return ctx, func(StoreFinish) { *o.log = append(*o.log, "stored "+o.name) }
}

func TestObserversFanOutAndNestContexts(t *testing.T) {
	var log []string
	combined := Observers(orderObserver{"a", &log}, nil, orderObserver{"b", &log})
	ctx, finish := combined.StartAwait(context.Background(), AwaitStart{})
	if ctx.Value(ctxKey("a")) == nil || ctx.Value(ctxKey("b")) == nil {
		t.Fatal("contexts from both observers must be kept")
	}
	finish(AwaitFinish{})
	_, stored := combined.StartStore(context.Background(), StoreStart{})
	stored(StoreFinish{})
	want := "start a,start b,finish b,finish a,store a,store b,stored b,stored a"
	if got := strings.Join(log, ","); got != want {
		t.Fatalf("order = %s, want %s", got, want)
	}
}
