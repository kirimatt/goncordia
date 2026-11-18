package core_test

import (
	"errors"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/core"
)

func TestExponentialRetryEdges(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC))
	delay := func(policy core.ExponentialRetry, attempt int) time.Duration {
		return policy.NextRetryAt(attempt, nil, clk).Sub(clk.Now())
	}
	if got := delay(core.ExponentialRetry{}, 1); got != time.Second {
		t.Fatalf("zero policy first delay = %s, want 1s default base", got)
	}
	if got := delay(core.ExponentialRetry{}, 100); got != 24*time.Hour {
		t.Fatalf("zero policy late delay = %s, want 24h default cap", got)
	}
	if got := delay(core.ExponentialRetry{Base: time.Second, Max: 3 * time.Second}, 3); got != 3*time.Second {
		t.Fatalf("capped delay = %s, want 3s", got)
	}
	if got := delay(core.ExponentialRetry{Base: 10 * time.Second, Max: 5 * time.Second}, 1); got != 5*time.Second {
		t.Fatalf("base above max = %s, want 5s", got)
	}
	for name, tc := range map[string]struct {
		jitter float64
		random float64
		want   time.Duration
	}{
		"negative jitter is none":  {jitter: -1, random: 0, want: 4 * time.Second},
		"jitter above one clamps":  {jitter: 5, random: 0, want: 0},
		"random below zero clamps": {jitter: 0.5, random: -3, want: 2 * time.Second},
		"random above one clamps":  {jitter: 0.5, random: 3, want: 6 * time.Second},
		"jitter never exceeds max": {jitter: 1, random: 1, want: 5 * time.Second},
	} {
		max := time.Hour
		if name == "jitter never exceeds max" {
			max = 5 * time.Second
		}
		random := tc.random
		policy := core.ExponentialRetry{Base: time.Second, Max: max, Jitter: tc.jitter, Random: func() float64 { return random }}
		if got := delay(policy, 3); got != tc.want {
			t.Errorf("%s: delay = %s, want %s", name, got, tc.want)
		}
	}
}

func TestRetryDirectives(t *testing.T) {
	at := time.Date(2026, 11, 18, 1, 0, 0, 0, time.UTC)
	cause := errors.New("cause")
	retry, ok := core.AsRetry(core.RetryAt(at, cause))
	if !ok || !retry.At.Equal(at) || retry.Error() != "cause" || !errors.Is(retry, cause) {
		t.Fatalf("RetryAt = %+v, %v", retry, ok)
	}
	if got := (&core.RetryError{}).Error(); got != "retry job" {
		t.Fatalf("empty RetryError = %q", got)
	}
	if got := (&core.DiscardError{}).Error(); got != "discard job" {
		t.Fatalf("empty DiscardError = %q", got)
	}
	if _, ok := core.AsDiscard(cause); ok {
		t.Fatal("plain error reported as discard")
	}
}
