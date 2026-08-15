package clock_test

import (
	"context"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/clock"
)

func TestManualAdvanceFiresTimerAndTicker(t *testing.T) {
	base := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	manual := clock.NewManual(base)
	after := manual.After(time.Minute)
	ticker := manual.NewTicker(30 * time.Second)
	defer ticker.Stop()

	manual.Advance(29 * time.Second)
	select {
	case <-ticker.C():
		t.Fatal("ticker fired early")
	default:
	}

	manual.Advance(time.Second)
	if got := <-ticker.C(); !got.Equal(base.Add(30 * time.Second)) {
		t.Fatalf("ticker time=%v", got)
	}
	manual.Advance(30 * time.Second)
	if got := <-after; !got.Equal(base.Add(time.Minute)) {
		t.Fatalf("timer time=%v", got)
	}
	if got := <-ticker.C(); !got.Equal(base.Add(time.Minute)) {
		t.Fatalf("second ticker time=%v", got)
	}
}

func TestRealNowIsUTC(t *testing.T) {
	if got := (clock.Real{}).Now().Location(); got != time.UTC {
		t.Fatalf("location=%v, want UTC", got)
	}
}

func TestWithTimeoutUsesManualClock(t *testing.T) {
	manual := clock.NewManual(time.Time{})
	ctx, cancel := clock.WithTimeout(context.Background(), manual, time.Minute)
	defer cancel()
	manual.Advance(time.Minute)
	<-ctx.Done()
	if ctx.Err() != context.DeadlineExceeded {
		t.Fatalf("err=%v", ctx.Err())
	}
}

func TestWithTimeoutCancelStopsManualTimer(t *testing.T) {
	manual := clock.NewManual(time.Time{})
	_, cancel := clock.WithTimeout(context.Background(), manual, time.Hour)
	if manual.ActiveTimers() != 1 {
		t.Fatalf("active timers=%d", manual.ActiveTimers())
	}
	cancel()
	if manual.ActiveTimers() != 0 {
		t.Fatalf("active timers after cancel=%d", manual.ActiveTimers())
	}
}
