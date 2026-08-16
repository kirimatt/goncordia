package core

import (
	"errors"
	"math/rand/v2"
	"time"

	"github.com/kirimatt/goncordia/clock"
)

// RetryPolicy determines when a failed job should next be retried.
type RetryPolicy interface {
	NextRetryAt(attempt int, err error, clk clock.Clock) time.Time
}

// ExponentialRetry implements exponential backoff: delay = base * 2^(attempt-1).
// This is the default retry policy.
type ExponentialRetry struct {
	// Base is the initial delay. Default: 1 second.
	Base time.Duration
	// Max caps the calculated delay. Default: 24 hours.
	Max time.Duration
	// Jitter randomizes the delay by this fraction in both directions. Values
	// above 1 are treated as 1; zero disables jitter.
	Jitter float64
	// Random returns a value in [0,1) for jitter. Tests can inject a
	// deterministic source. Default: rand.Float64.
	Random func() float64
}

// DefaultRetryPolicy is the out-of-the-box exponential backoff.
var DefaultRetryPolicy RetryPolicy = ExponentialRetry{
	Base: time.Second,
	Max:  24 * time.Hour,
}

func (r ExponentialRetry) NextRetryAt(attempt int, _ error, clk clock.Clock) time.Time {
	base := r.Base
	if base <= 0 {
		base = time.Second
	}
	max := r.Max
	if max <= 0 {
		max = 24 * time.Hour
	}
	delay := base
	for n := 1; n < attempt && delay < max; n++ {
		if delay > max/2 {
			delay = max
			break
		}
		delay *= 2
	}
	if delay > max {
		delay = max
	}
	jitter := r.Jitter
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}
	if jitter > 0 {
		random := r.Random
		if random == nil {
			random = rand.Float64
		}
		value := random()
		if value < 0 {
			value = 0
		}
		if value > 1 {
			value = 1
		}
		factor := 1 - jitter + 2*jitter*value
		jittered := float64(delay) * factor
		if jittered >= float64(max) {
			delay = max
		} else {
			delay = time.Duration(jittered)
		}
	}
	return clk.Now().Add(delay)
}

// FixedRetry retries after a constant delay.
type FixedRetry struct {
	Delay time.Duration
}

func (r FixedRetry) NextRetryAt(_ int, _ error, clk clock.Clock) time.Time {
	return clk.Now().Add(r.Delay)
}

// NoRetry discards a job after the first failure (NextRetryAt returns zero time).
type NoRetry struct{}

func (NoRetry) NextRetryAt(_ int, _ error, _ clock.Clock) time.Time { return time.Time{} }

// DiscardError tells the worker to discard immediately without consulting the
// retry policy. The wrapped error is retained in attempt history.
type DiscardError struct{ Err error }

func (e *DiscardError) Error() string {
	if e == nil || e.Err == nil {
		return "discard job"
	}
	return e.Err.Error()
}
func (e *DiscardError) Unwrap() error { return e.Err }

// Discard marks err as permanent.
func Discard(err error) error { return &DiscardError{Err: err} }

// RetryError overrides the retry policy with an absolute time or delay.
type RetryError struct {
	Err   error
	At    time.Time
	After time.Duration
}

func (e *RetryError) Error() string {
	if e == nil || e.Err == nil {
		return "retry job"
	}
	return e.Err.Error()
}
func (e *RetryError) Unwrap() error { return e.Err }

// RetryAt retries err at the supplied absolute time.
func RetryAt(at time.Time, err error) error { return &RetryError{Err: err, At: at} }

// RetryAfter retries err after delay measured by the worker's injected clock.
func RetryAfter(delay time.Duration, err error) error {
	return &RetryError{Err: err, After: delay}
}

// AsDiscard and AsRetry expose directives without requiring callers to depend
// on their concrete pointer representation.
func AsDiscard(err error) (*DiscardError, bool) {
	var target *DiscardError
	ok := errors.As(err, &target)
	return target, ok
}

func AsRetry(err error) (*RetryError, bool) {
	var target *RetryError
	ok := errors.As(err, &target)
	return target, ok
}
