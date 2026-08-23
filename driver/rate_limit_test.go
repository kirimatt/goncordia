package driver_test

import (
	"context"
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
