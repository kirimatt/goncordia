package driver_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/v2/driver"
)

// cursorStore is a scripted schedule-cursor store: it holds one cursor,
// fails calls on request, and can lose the next CAS rounds.
type cursorStore struct {
	at        time.Time
	exists    bool
	getErr    error
	advErr    error
	loseCAS   int // number of upcoming advances that report a lost race
	onLostCAS func()
	advances  int
}

func (s *cursorStore) ScheduleCursorGetOrCreate(_ context.Context, p driver.ScheduleCursorCreateParams) (driver.ScheduleCursorResult, error) {
	if s.getErr != nil {
		return driver.ScheduleCursorResult{}, s.getErr
	}
	if !s.exists {
		s.at, s.exists = p.InitialAt, true
		return driver.ScheduleCursorResult{At: s.at, Created: true}, nil
	}
	return driver.ScheduleCursorResult{At: s.at}, nil
}

func (s *cursorStore) ScheduleCursorAdvance(_ context.Context, p driver.ScheduleCursorAdvanceParams) (bool, error) {
	s.advances++
	if s.advErr != nil {
		return false, s.advErr
	}
	if s.loseCAS > 0 {
		s.loseCAS--
		if s.onLostCAS != nil {
			s.onLostCAS()
		}
		return false, nil
	}
	if !s.at.Equal(p.Expected) {
		return false, nil
	}
	s.at = p.Next
	return true, nil
}

type nativeStore struct{ cursorStore }

func (nativeStore) RateLimitAcquire(context.Context, driver.RateLimitAcquireParams) (driver.RateLimitAcquireResult, error) {
	return driver.RateLimitAcquireResult{Acquired: true}, nil
}

var rateNow = time.Date(2026, 11, 18, 12, 0, 30, 0, time.UTC)

func TestAcquireRateLimitRejectsInvalidParameters(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		params driver.RateLimitAcquireParams
		want   string
	}{
		{driver.RateLimitAcquireParams{Limit: 1, Period: time.Second}, "invalid rate-limit parameters"},
		{driver.RateLimitAcquireParams{Key: "k", Period: time.Second}, "invalid rate-limit parameters"},
		{driver.RateLimitAcquireParams{Key: "k", Limit: 1}, "invalid rate-limit parameters"},
		{driver.RateLimitAcquireParams{Key: "k", Limit: 1, Burst: 2, Period: time.Second}, "burst 2 exceeds limit 1"},
		{driver.RateLimitAcquireParams{Key: "k", Limit: 1, Period: time.Second, Mode: 7}, "unknown rate-limit mode 7"},
		{driver.RateLimitAcquireParams{Key: "k", Limit: 1000, Period: time.Second, Mode: driver.RateLimitModeFixedWindow}, "smaller than period milliseconds"},
	} {
		_, err := driver.AcquireRateLimit(ctx, &cursorStore{}, tc.params)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%+v: err = %v, want %q", tc.params, err, tc.want)
		}
	}
}

func TestAcquireRateLimitUsesNativeAcquirer(t *testing.T) {
	store := &nativeStore{}
	result, err := driver.AcquireRateLimit(context.Background(), store, driver.RateLimitAcquireParams{})
	if err != nil || !result.Acquired || store.advances != 0 {
		t.Fatalf("native acquirer bypassed: %+v %v advances=%d", result, err, store.advances)
	}
}

func TestAcquireRateLimitStoreErrors(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("store down")
	for _, mode := range []driver.RateLimitMode{driver.RateLimitModeGCRA, driver.RateLimitModeFixedWindow} {
		params := driver.RateLimitAcquireParams{Key: "k", Limit: 2, Period: time.Minute, Now: rateNow, Mode: mode}
		if _, err := driver.AcquireRateLimit(ctx, &cursorStore{getErr: boom}, params); !errors.Is(err, boom) {
			t.Errorf("mode %d get error: %v", mode, err)
		}
		// Same window (fixed) or a due cursor (GCRA): the advance fails.
		same := &cursorStore{exists: true, at: rateNow.Truncate(time.Minute).Add(time.Millisecond), advErr: boom}
		if _, err := driver.AcquireRateLimit(ctx, same, params); !errors.Is(err, boom) {
			t.Errorf("mode %d advance error: %v", mode, err)
		}
	}
	// Fixed window: a cursor from an earlier window is reset, and that
	// advance can fail too.
	old := &cursorStore{exists: true, at: rateNow.Add(-time.Hour), advErr: boom}
	params := driver.RateLimitAcquireParams{Key: "k", Limit: 2, Period: time.Minute, Now: rateNow, Mode: driver.RateLimitModeFixedWindow}
	if _, err := driver.AcquireRateLimit(ctx, old, params); !errors.Is(err, boom) {
		t.Errorf("window reset error: %v", err)
	}
}

func TestAcquireRateLimitRetriesLostCAS(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name   string
		cursor time.Time
		mode   driver.RateLimitMode
	}{
		{"gcra", rateNow.Add(-time.Hour), driver.RateLimitModeGCRA},
		{"fixed same window", rateNow.Truncate(time.Minute).Add(time.Millisecond), driver.RateLimitModeFixedWindow},
		{"fixed new window", rateNow.Add(-time.Hour), driver.RateLimitModeFixedWindow},
	} {
		store := &cursorStore{exists: true, at: tc.cursor, loseCAS: 2}
		params := driver.RateLimitAcquireParams{Key: "k", Limit: 2, Period: time.Minute, Now: rateNow, Mode: tc.mode}
		result, err := driver.AcquireRateLimit(ctx, store, params)
		if err != nil || !result.Acquired || store.advances != 3 {
			t.Errorf("%s: %+v %v after %d advances, want a permit on the third", tc.name, result, err, store.advances)
		}
	}
}

func TestAcquireRateLimitStopsOnCancelAfterLostCAS(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cursor time.Time
		mode   driver.RateLimitMode
	}{
		{"gcra", rateNow.Add(-time.Hour), driver.RateLimitModeGCRA},
		{"fixed", rateNow.Truncate(time.Minute).Add(time.Millisecond), driver.RateLimitModeFixedWindow},
	} {
		ctx, cancel := context.WithCancel(context.Background())
		store := &cursorStore{exists: true, at: tc.cursor, loseCAS: 1, onLostCAS: cancel}
		params := driver.RateLimitAcquireParams{Key: "k", Limit: 2, Period: time.Minute, Now: rateNow, Mode: tc.mode}
		if _, err := driver.AcquireRateLimit(ctx, store, params); !errors.Is(err, context.Canceled) {
			t.Errorf("%s: err = %v, want context.Canceled", tc.name, err)
		}
		cancel()
	}
}

func TestAcquireRateLimitExtremeIntervals(t *testing.T) {
	ctx := context.Background()
	// 10 permits per nanosecond round the interval up to 1ns.
	tiny := &cursorStore{}
	params := driver.RateLimitAcquireParams{Key: "k", Limit: 10, Period: time.Nanosecond, Now: rateNow}
	if result, err := driver.AcquireRateLimit(ctx, tiny, params); err != nil || !result.Acquired {
		t.Fatalf("tiny interval: %+v %v", result, err)
	}
	blocked, err := driver.AcquireRateLimit(ctx, tiny, params)
	if err != nil || blocked.Acquired || !blocked.RetryAt.Equal(rateNow.Add(time.Nanosecond)) {
		t.Fatalf("same instant: %+v %v, want retry 1ns later", blocked, err)
	}
	params.Now = blocked.RetryAt
	if result, err := driver.AcquireRateLimit(ctx, tiny, params); err != nil || !result.Acquired {
		t.Fatalf("1ns later: %+v %v", result, err)
	}
	// A burst tolerance that overflows time.Duration saturates instead of
	// wrapping negative, so every request is admitted.
	huge := &cursorStore{exists: true, at: rateNow.Add(100 * 365 * 24 * time.Hour)}
	params = driver.RateLimitAcquireParams{Key: "k", Limit: 4, Burst: 4, Period: time.Duration(1<<63 - 1), Now: rateNow}
	if result, err := driver.AcquireRateLimit(ctx, huge, params); err != nil || !result.Acquired {
		t.Fatalf("saturated tolerance: %+v %v", result, err)
	}
}
