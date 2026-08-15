// Package clock provides injectable production and manually controlled clocks.
package clock

import (
	"context"
	"sync"
	"time"
)

// WithTimeout is context.WithTimeout driven by clk instead of the process wall
// clock. It lets worker deadline behavior advance deterministically in tests.
func WithTimeout(parent context.Context, clk Clock, d time.Duration) (context.Context, context.CancelFunc) {
	ctx := &timeoutContext{
		parent: parent, done: make(chan struct{}), deadline: clk.Now().Add(d),
	}
	var timerC <-chan time.Time
	stopTimer := func() {}
	if factory, ok := clk.(interface{ NewTimer(time.Duration) Timer }); ok {
		timer := factory.NewTimer(d)
		timerC = timer.C()
		stopTimer = timer.Stop
	} else {
		timerC = clk.After(d)
	}
	go func() {
		defer stopTimer()
		select {
		case <-parent.Done():
			ctx.finish(parent.Err())
		case <-timerC:
			ctx.finish(context.DeadlineExceeded)
		case <-ctx.done:
		}
	}()
	return ctx, func() {
		ctx.finish(context.Canceled)
		stopTimer()
	}
}

type timeoutContext struct {
	parent   context.Context
	done     chan struct{}
	deadline time.Time
	once     sync.Once
	mu       sync.RWMutex
	err      error
}

func (c *timeoutContext) Deadline() (time.Time, bool) {
	if deadline, ok := c.parent.Deadline(); ok && deadline.Before(c.deadline) {
		return deadline, true
	}
	return c.deadline, true
}
func (c *timeoutContext) Done() <-chan struct{} { return c.done }
func (c *timeoutContext) Err() error {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.err
}
func (c *timeoutContext) Value(key any) any { return c.parent.Value(key) }
func (c *timeoutContext) finish(err error) {
	c.once.Do(func() {
		c.mu.Lock()
		c.err = err
		c.mu.Unlock()
		close(c.done)
	})
}

// Ticker is the small subset of time.Ticker used by goncordia.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// Timer is a stoppable one-shot timer.
type Timer interface {
	C() <-chan time.Time
	Stop()
}

// Clock is the time source used by clients, schedulers, workers, and drivers.
// Implementations must be safe for concurrent use.
type Clock interface {
	Now() time.Time
	Since(time.Time) time.Duration
	After(time.Duration) <-chan time.Time
	NewTicker(time.Duration) Ticker
}

// Real is the production clock. Persisted timestamps are always UTC.
type Real struct{}

func (Real) Now() time.Time                         { return time.Now().UTC() }
func (Real) Since(t time.Time) time.Duration        { return time.Since(t) }
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }
func (Real) NewTicker(d time.Duration) Ticker       { return realTicker{time.NewTicker(d)} }
func (Real) NewTimer(d time.Duration) Timer         { return realTimer{time.NewTimer(d)} }

type realTicker struct{ ticker *time.Ticker }
type realTimer struct{ timer *time.Timer }

func (t realTicker) C() <-chan time.Time { return t.ticker.C }
func (t realTicker) Stop()               { t.ticker.Stop() }
func (t realTimer) C() <-chan time.Time  { return t.timer.C }
func (t realTimer) Stop()                { t.timer.Stop() }

// Manual is a deterministic clock for tests. Advance synchronously updates the
// current time and fires due timers and tickers without sleeping.
type Manual struct {
	mu     sync.Mutex
	now    time.Time
	timers map[*manualTimer]struct{}
}

type manualTimer struct {
	owner    *Manual
	ch       chan time.Time
	next     time.Time
	interval time.Duration
	stopped  bool
}

// NewManual creates a manually controlled clock at t. A zero t uses
// 2024-01-01T00:00:00Z.
func NewManual(t time.Time) *Manual {
	if t.IsZero() {
		t = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return &Manual{now: t, timers: make(map[*manualTimer]struct{})}
}

// Mock and NewMock are compatibility aliases for the former test clock names.
type Mock = Manual

func NewMock(t time.Time) *Manual { return NewManual(t) }

func (m *Manual) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.now
}

func (m *Manual) Since(t time.Time) time.Duration { return m.Now().Sub(t) }

func (m *Manual) After(d time.Duration) <-chan time.Time {
	return m.NewTimer(d).C()
}

// NewTimer creates a stoppable one-shot timer driven by Advance.
func (m *Manual) NewTimer(d time.Duration) Timer {
	m.mu.Lock()
	defer m.mu.Unlock()
	timer := &manualTimer{owner: m, ch: make(chan time.Time, 1), next: m.now.Add(d)}
	if d <= 0 {
		timer.stopped = true
		timer.ch <- m.now
		return timer
	}
	m.timers[timer] = struct{}{}
	return timer
}

func (m *Manual) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("non-positive interval for NewTicker")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	timer := &manualTimer{
		owner: m, ch: make(chan time.Time, 1), next: m.now.Add(d), interval: d,
	}
	m.timers[timer] = struct{}{}
	return timer
}

// Advance moves time forward and makes all timers due at the new time ready.
func (m *Manual) Advance(d time.Duration) {
	if d < 0 {
		panic("cannot advance a manual clock backwards")
	}
	m.Set(m.Now().Add(d))
}

// Set changes the current time. Moving forward fires due timers; moving
// backward only changes Now and leaves existing deadlines unchanged.
func (m *Manual) Set(t time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	forward := !t.Before(m.now)
	m.now = t
	if !forward {
		return
	}
	for timer := range m.timers {
		if timer.stopped || timer.next.After(t) {
			continue
		}
		firedAt := timer.next
		select {
		case timer.ch <- firedAt:
		default: // match time.Ticker: drop ticks when the receiver is slow
		}
		if timer.interval == 0 {
			timer.stopped = true
			delete(m.timers, timer)
			continue
		}
		steps := t.Sub(timer.next)/timer.interval + 1
		timer.next = timer.next.Add(steps * timer.interval)
	}
}

// ActiveTimers reports the number of non-stopped timers and tickers. It is
// useful for synchronizing a test before calling Advance.
func (m *Manual) ActiveTimers() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.timers)
}

func (t *manualTimer) C() <-chan time.Time { return t.ch }

func (t *manualTimer) Stop() {
	t.owner.mu.Lock()
	defer t.owner.mu.Unlock()
	if t.stopped {
		return
	}
	t.stopped = true
	delete(t.owner.timers, t)
}
