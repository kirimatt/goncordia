package driver_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/driver"
	"github.com/kirimatt/goncordia/driver/memory"
)

func TestAcquireRateLimitFixedWindow(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 8, 23, 12, 0, 30, 0, time.UTC))
	d := memory.New(memory.WithClock(clk))
	params := driver.RateLimitAcquireParams{
		Key: "fixed", Limit: 2, Period: time.Minute, Burst: 1,
		Mode: driver.RateLimitModeFixedWindow,
	}
	for range 2 {
		params.Now = clk.Now()
		result, err := driver.AcquireRateLimit(context.Background(), d.Executor(), params)
		if err != nil || !result.Acquired {
			t.Fatalf("permit: result=%+v err=%v", result, err)
		}
	}
	params.Now = clk.Now()
	blocked, err := driver.AcquireRateLimit(context.Background(), d.Executor(), params)
	if err != nil || blocked.Acquired || !blocked.RetryAt.Equal(time.Date(2026, 8, 23, 12, 1, 0, 0, time.UTC)) {
		t.Fatalf("blocked result=%+v err=%v", blocked, err)
	}
	clk.Advance(30 * time.Second)
	params.Now = clk.Now()
	next, err := driver.AcquireRateLimit(context.Background(), d.Executor(), params)
	if err != nil || !next.Acquired {
		t.Fatalf("new window: result=%+v err=%v", next, err)
	}
}

func TestAcquireRateLimitFixedWindowConcurrentCeiling(t *testing.T) {
	d := memory.New()
	params := driver.RateLimitAcquireParams{
		Key: "concurrent-fixed", Now: time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC),
		Limit: 10, Period: time.Minute, Mode: driver.RateLimitModeFixedWindow,
	}
	var acquired atomic.Int64
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := driver.AcquireRateLimit(context.Background(), d.Executor(), params)
			if err != nil {
				t.Errorf("acquire: %v", err)
				return
			}
			if result.Acquired {
				acquired.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := acquired.Load(); got != 10 {
		t.Fatalf("acquired=%d, want exact ceiling 10", got)
	}
}

func BenchmarkAcquireRateLimitGCRA(b *testing.B) {
	d := memory.New()
	ctx := context.Background()
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	b.ResetTimer()
	for i := range b.N {
		result, err := driver.AcquireRateLimit(ctx, d.Executor(), driver.RateLimitAcquireParams{
			Key: "benchmark", Now: now.Add(time.Duration(i) * time.Millisecond),
			Limit: 1000, Period: time.Second, Burst: 1,
		})
		if err != nil || !result.Acquired {
			b.Fatalf("permit %d: result=%+v err=%v", i, result, err)
		}
	}
}
