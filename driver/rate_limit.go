package driver

import (
	"context"
	"fmt"
	"time"
)

// RateLimitAcquireParams describes one GCRA permit request. Key must identify
// one logical limiter across every participating process.
type RateLimitAcquireParams struct {
	Key    string
	Now    time.Time
	Limit  int
	Period time.Duration
	Burst  int
	Mode   RateLimitMode
}

// RateLimitMode selects the distributed limiter algorithm.
type RateLimitMode uint8

const (
	// RateLimitModeGCRA spaces starts while allowing an explicit burst.
	RateLimitModeGCRA RateLimitMode = iota
	// RateLimitModeFixedWindow resets the count at UTC duration boundaries.
	RateLimitModeFixedWindow
)

// RateLimitAcquireResult reports whether a permit was reserved. RetryAt is set
// when Acquired is false.
type RateLimitAcquireResult struct {
	Acquired bool
	RetryAt  time.Time
}

// RateLimitAcquirer lets a driver provide a native atomic rate-limit primitive.
// Built-in drivers without an override use the schedule-cursor CAS fallback.
type RateLimitAcquirer interface {
	RateLimitAcquire(context.Context, RateLimitAcquireParams) (RateLimitAcquireResult, error)
}

type rateLimitCursorStore interface {
	ScheduleCursorGetOrCreate(context.Context, ScheduleCursorCreateParams) (ScheduleCursorResult, error)
	ScheduleCursorAdvance(context.Context, ScheduleCursorAdvanceParams) (bool, error)
}

// AcquireRateLimit reserves one distributed GCRA permit. The fallback stores a
// theoretical-arrival-time cursor and advances it with compare-and-swap, so it
// requires a driver whose Capabilities.LinearizableCAS is true.
func AcquireRateLimit(
	ctx context.Context, store rateLimitCursorStore, params RateLimitAcquireParams,
) (RateLimitAcquireResult, error) {
	if native, ok := store.(RateLimitAcquirer); ok {
		return native.RateLimitAcquire(ctx, params)
	}
	if params.Key == "" || params.Limit <= 0 || params.Period <= 0 {
		return RateLimitAcquireResult{}, fmt.Errorf("invalid rate-limit parameters")
	}
	if params.Burst <= 0 {
		params.Burst = 1
	}
	if params.Burst > params.Limit {
		return RateLimitAcquireResult{}, fmt.Errorf("rate-limit burst %d exceeds limit %d", params.Burst, params.Limit)
	}
	if params.Mode == RateLimitModeFixedWindow {
		return acquireFixedWindow(ctx, store, params)
	}
	if params.Mode != RateLimitModeGCRA {
		return RateLimitAcquireResult{}, fmt.Errorf("unknown rate-limit mode %d", params.Mode)
	}
	interval := rateInterval(params.Period, params.Limit)
	tolerance := multiplyDuration(interval, params.Burst-1)
	now := params.Now.UTC()

	for {
		cursor, err := store.ScheduleCursorGetOrCreate(ctx, ScheduleCursorCreateParams{
			ID: params.Key, InitialAt: now,
		})
		if err != nil {
			return RateLimitAcquireResult{}, err
		}
		retryAt := cursor.At.Add(-tolerance)
		if now.Before(retryAt) {
			return RateLimitAcquireResult{RetryAt: retryAt}, nil
		}
		base := cursor.At
		if base.Before(now) {
			base = now
		}
		next := base.Add(interval)
		advanced, err := store.ScheduleCursorAdvance(ctx, ScheduleCursorAdvanceParams{
			ID: params.Key, Expected: cursor.At, Next: next,
		})
		if err != nil {
			return RateLimitAcquireResult{}, err
		}
		if advanced {
			return RateLimitAcquireResult{Acquired: true}, nil
		}
		select {
		case <-ctx.Done():
			return RateLimitAcquireResult{}, ctx.Err()
		default:
		}
	}
}

// acquireFixedWindow encodes the count as nanoseconds after the UTC-aligned
// window boundary. This preserves the existing portable one-timestamp CAS
// storage primitive without scanning or adding a backend-specific counter.
func acquireFixedWindow(
	ctx context.Context, store rateLimitCursorStore, params RateLimitAcquireParams,
) (RateLimitAcquireResult, error) {
	const countUnit = time.Millisecond // portable across Cassandra and SQL timestamp precision
	if int64(params.Limit) >= int64(params.Period/countUnit) {
		return RateLimitAcquireResult{}, fmt.Errorf("fixed-window limit must be smaller than period milliseconds")
	}
	now := params.Now.UTC()
	window := now.Truncate(params.Period)
	initial := window.Add(countUnit)
	for {
		cursor, err := store.ScheduleCursorGetOrCreate(ctx, ScheduleCursorCreateParams{
			ID: params.Key, InitialAt: initial,
		})
		if err != nil {
			return RateLimitAcquireResult{}, err
		}
		if cursor.Created {
			return RateLimitAcquireResult{Acquired: true}, nil
		}
		cursorWindow := cursor.At.Truncate(params.Period)
		count := int(cursor.At.Sub(cursorWindow) / countUnit)
		if !cursorWindow.Equal(window) {
			advanced, err := store.ScheduleCursorAdvance(ctx, ScheduleCursorAdvanceParams{
				ID: params.Key, Expected: cursor.At, Next: initial,
			})
			if err != nil {
				return RateLimitAcquireResult{}, err
			}
			if advanced {
				return RateLimitAcquireResult{Acquired: true}, nil
			}
			continue
		}
		if count >= params.Limit {
			return RateLimitAcquireResult{RetryAt: window.Add(params.Period)}, nil
		}
		advanced, err := store.ScheduleCursorAdvance(ctx, ScheduleCursorAdvanceParams{
			ID: params.Key, Expected: cursor.At, Next: cursor.At.Add(countUnit),
		})
		if err != nil {
			return RateLimitAcquireResult{}, err
		}
		if advanced {
			return RateLimitAcquireResult{Acquired: true}, nil
		}
		select {
		case <-ctx.Done():
			return RateLimitAcquireResult{}, ctx.Err()
		default:
		}
	}
}

func rateInterval(period time.Duration, limit int) time.Duration {
	interval := time.Duration(float64(period) / float64(limit))
	if interval < time.Nanosecond {
		return time.Nanosecond
	}
	return interval
}

func multiplyDuration(duration time.Duration, multiplier int) time.Duration {
	if multiplier <= 0 {
		return 0
	}
	if duration > time.Duration(1<<63-1)/time.Duration(multiplier) {
		return time.Duration(1<<63 - 1)
	}
	return duration * time.Duration(multiplier)
}
