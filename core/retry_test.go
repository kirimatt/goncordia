package core_test

import (
	"errors"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
)

func TestExponentialRetry(t *testing.T) {
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	clk := clock.NewMock(base)

	policy := core.ExponentialRetry{Base: time.Second, Max: 24 * time.Hour}

	cases := []struct {
		attempt int
		want    time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{10, 512 * time.Second},
	}

	for _, c := range cases {
		got := policy.NextRetryAt(c.attempt, nil, clk)
		if diff := got.Sub(clk.Now()); diff != c.want {
			t.Errorf("attempt %d: expected delay %v, got %v", c.attempt, c.want, diff)
		}
	}
}

func TestExponentialRetryCapsAtMax(t *testing.T) {
	clk := clock.NewMock(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	policy := core.ExponentialRetry{Base: time.Hour, Max: 6 * time.Hour}

	got := policy.NextRetryAt(10, nil, clk)
	if diff := got.Sub(clk.Now()); diff != 6*time.Hour {
		t.Errorf("expected delay capped at 6h, got %v", diff)
	}
}

func TestExponentialRetryDoesNotOverflow(t *testing.T) {
	clk := clock.NewManual(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	policy := core.ExponentialRetry{Base: time.Second, Max: 24 * time.Hour}
	if got := policy.NextRetryAt(1_000_000, nil, clk).Sub(clk.Now()); got != 24*time.Hour {
		t.Fatalf("overflowed delay=%v, want 24h", got)
	}
}

func TestRetryDirectivesUnwrap(t *testing.T) {
	cause := errors.New("temporary")
	if directive, ok := core.AsRetry(core.RetryAfter(time.Minute, cause)); !ok || directive.After != time.Minute || !errors.Is(directive, cause) {
		t.Fatalf("retry directive=%+v ok=%v", directive, ok)
	}
	if directive, ok := core.AsDiscard(core.Discard(cause)); !ok || !errors.Is(directive, cause) {
		t.Fatalf("discard directive=%+v ok=%v", directive, ok)
	}
}

func TestFixedRetry(t *testing.T) {
	base := time.Date(2024, 3, 15, 9, 0, 0, 0, time.UTC)
	clk := clock.NewMock(base)

	policy := core.FixedRetry{Delay: 30 * time.Second}
	got := policy.NextRetryAt(5, nil, clk)

	if diff := got.Sub(clk.Now()); diff != 30*time.Second {
		t.Errorf("expected 30s, got %v", diff)
	}
}

func TestNoRetry(t *testing.T) {
	clk := clock.NewMock(time.Now())
	got := core.NoRetry{}.NextRetryAt(1, nil, clk)
	if !got.IsZero() {
		t.Errorf("expected zero time, got %v", got)
	}
}
