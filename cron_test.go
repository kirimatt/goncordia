package goncordia_test

import (
	"context"
	"testing"
	"time"

	"github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver/memory"
)

type cronJob struct{ N int }

func (cronJob) Kind() string { return "cron_job" }

func TestCronScheduler_FiresOnFirstTick(t *testing.T) {
	d := memory.New()
	cs := goncordia.NewCronScheduler(d, []goncordia.PeriodicJob{
		{Schedule: core.Every(time.Hour), Args: cronJob{N: 1}},
	}, goncordia.CronConfig{TickInterval: 30 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go cs.Start(ctx) //nolint:errcheck

	deadline := time.Now().Add(3 * time.Second)
	for len(d.AllJobs()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("job was never enqueued")
		}
		time.Sleep(10 * time.Millisecond)
	}

	jobs := d.AllJobs()
	if len(jobs) != 1 {
		t.Fatalf("want 1 job, got %d", len(jobs))
	}
	if jobs[0].Kind != "cron_job" {
		t.Fatalf("want kind=cron_job, got %q", jobs[0].Kind)
	}
}

func TestCronScheduler_RespectsInterval(t *testing.T) {
	d := memory.New()
	cs := goncordia.NewCronScheduler(d, []goncordia.PeriodicJob{
		{Schedule: core.Every(60 * time.Millisecond), Args: cronJob{N: 2}},
	}, goncordia.CronConfig{TickInterval: 20 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go cs.Start(ctx) //nolint:errcheck

	// Wait long enough for the initial fire + at least one repeat.
	time.Sleep(250 * time.Millisecond)
	cancel()
	time.Sleep(30 * time.Millisecond) // let goroutine exit

	n := len(d.AllJobs())
	if n < 2 {
		t.Fatalf("expected at least 2 enqueued jobs, got %d", n)
	}
}

func TestCronScheduler_MultipleJobs(t *testing.T) {
	d := memory.New()
	cs := goncordia.NewCronScheduler(d, []goncordia.PeriodicJob{
		{Schedule: core.Every(time.Hour), Args: cronJob{N: 10}},
		{Schedule: core.Every(time.Hour), Args: cronJob{N: 20}},
		{Schedule: core.Every(time.Hour), Args: cronJob{N: 30}},
	}, goncordia.CronConfig{TickInterval: 30 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go cs.Start(ctx) //nolint:errcheck

	deadline := time.Now().Add(3 * time.Second)
	for len(d.AllJobs()) < 3 {
		if time.Now().After(deadline) {
			t.Fatalf("timeout: only %d/3 jobs enqueued", len(d.AllJobs()))
		}
		time.Sleep(10 * time.Millisecond)
	}

	if n := len(d.AllJobs()); n != 3 {
		t.Fatalf("want exactly 3 jobs on first tick, got %d", n)
	}
}

func TestCronScheduler_ScheduleFunc(t *testing.T) {
	d := memory.New()
	var calls int
	sched := core.ScheduleFunc(func(last time.Time) time.Time {
		calls++
		if last.IsZero() {
			return time.Time{} // fire immediately
		}
		return last.Add(time.Hour) // then wait a long time
	})

	cs := goncordia.NewCronScheduler(d, []goncordia.PeriodicJob{
		{Schedule: sched, Args: cronJob{N: 99}},
	}, goncordia.CronConfig{TickInterval: 30 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go cs.Start(ctx) //nolint:errcheck

	deadline := time.Now().Add(3 * time.Second)
	for len(d.AllJobs()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("job was never enqueued")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// After the initial fire the next is 1 hour away — only 1 job should exist.
	time.Sleep(100 * time.Millisecond)
	if n := len(d.AllJobs()); n != 1 {
		t.Fatalf("want 1 job, got %d", n)
	}
}

func TestCronScheduler_StopsOnContextCancel(t *testing.T) {
	d := memory.New()
	cs := goncordia.NewCronScheduler(d, []goncordia.PeriodicJob{
		{Schedule: core.Every(20 * time.Millisecond), Args: cronJob{N: 0}},
	}, goncordia.CronConfig{TickInterval: 10 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cs.Start(ctx) }()

	time.Sleep(80 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Start did not return after ctx cancel")
	}
}

func TestCronScheduler_UsesLeaderElection(t *testing.T) {
	d := memory.New()
	jobs := []goncordia.PeriodicJob{{Schedule: core.Every(time.Hour), Args: cronJob{N: 7}}}
	config := goncordia.CronConfig{TickInterval: 10 * time.Millisecond, LeaderTTL: 100 * time.Millisecond}
	first := goncordia.NewCronScheduler(d, jobs, config)
	second := goncordia.NewCronScheduler(d, jobs, config)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go first.Start(ctx)  //nolint:errcheck
	go second.Start(ctx) //nolint:errcheck
	time.Sleep(80 * time.Millisecond)
	if got := len(d.AllJobs()); got != 1 {
		t.Fatalf("leader election enqueued %d jobs, want 1", got)
	}
}

func TestCronScheduler_UsesInjectedManualClock(t *testing.T) {
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	manual := clock.NewManual(base)
	d := memory.New(memory.WithClock(manual))
	cs := goncordia.NewCronScheduler(d, []goncordia.PeriodicJob{
		{Schedule: core.Every(time.Hour), Args: cronJob{N: 8}},
	}, goncordia.CronConfig{TickInterval: time.Second, Clock: manual})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go cs.Start(ctx) //nolint:errcheck
	waitForCondition(t, time.Second, func() bool { return manual.ActiveTimers() == 1 }, "cron ticker registration")

	manual.Advance(time.Second)
	waitForCondition(t, time.Second, func() bool { return len(d.AllJobs()) == 1 }, "first manual tick")
	manual.Advance(time.Hour)
	waitForCondition(t, time.Second, func() bool { return len(d.AllJobs()) == 2 }, "scheduled manual tick")
}

func TestCronScheduler_DurableCursorSurvivesFailover(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	manual := clock.NewManual(start.Add(3 * time.Hour))
	d := memory.New(memory.WithClock(manual))
	job := goncordia.PeriodicJob{
		ID: "hourly-report", StartAt: start, CatchUp: goncordia.CronCatchUpAll,
		Schedule: core.Every(time.Hour), Args: cronJob{N: 10},
	}
	config := goncordia.CronConfig{
		TickInterval: time.Second, Clock: manual, LeaderName: "durable-test", WorkerID: "first",
	}

	ctx, cancel := context.WithCancel(context.Background())
	first := goncordia.NewCronScheduler(d, []goncordia.PeriodicJob{job}, config)
	done := make(chan error, 1)
	go func() { done <- first.Start(ctx) }()
	waitForCondition(t, time.Second, func() bool { return manual.ActiveTimers() == 1 }, "first durable ticker")
	manual.Advance(time.Second)
	waitForCondition(t, time.Second, func() bool { return len(d.AllJobs()) == 3 }, "durable catch-up")
	cancel()
	waitForCondition(t, time.Second, func() bool { return manual.ActiveTimers() == 0 }, "first durable stop")
	<-done

	config.WorkerID = "second"
	second := goncordia.NewCronScheduler(d, []goncordia.PeriodicJob{job}, config)
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	go second.Start(ctx) //nolint:errcheck
	waitForCondition(t, time.Second, func() bool { return manual.ActiveTimers() == 1 }, "second durable ticker")
	manual.Advance(time.Second)
	time.Sleep(10 * time.Millisecond)
	if got := len(d.AllJobs()); got != 3 {
		t.Fatalf("failover duplicated occurrences: got %d jobs, want 3", got)
	}
	manual.Advance(time.Hour)
	waitForCondition(t, time.Second, func() bool { return len(d.AllJobs()) == 4 }, "post-failover occurrence")
}

func TestCronScheduler_CatchUpPolicies(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("latest", func(t *testing.T) {
		manual := clock.NewManual(start.Add(3 * time.Hour))
		d := memory.New(memory.WithClock(manual))
		cs := goncordia.NewCronScheduler(d, []goncordia.PeriodicJob{{
			ID: "latest", StartAt: start, CatchUp: goncordia.CronCatchUpLatest,
			Schedule: core.Every(time.Hour), Args: cronJob{N: 20},
		}}, goncordia.CronConfig{TickInterval: time.Second, Clock: manual})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go cs.Start(ctx) //nolint:errcheck
		waitForCondition(t, time.Second, func() bool { return manual.ActiveTimers() == 1 }, "latest ticker")
		manual.Advance(time.Second)
		waitForCondition(t, time.Second, func() bool { return len(d.AllJobs()) == 1 }, "latest catch-up")
	})

	t.Run("skip", func(t *testing.T) {
		manual := clock.NewManual(start.Add(3*time.Hour + 30*time.Minute))
		d := memory.New(memory.WithClock(manual))
		cs := goncordia.NewCronScheduler(d, []goncordia.PeriodicJob{{
			ID: "skip", StartAt: start, CatchUp: goncordia.CronSkipMissed,
			Schedule: core.Every(time.Hour), Args: cronJob{N: 30},
		}}, goncordia.CronConfig{TickInterval: time.Second, Clock: manual})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go cs.Start(ctx) //nolint:errcheck
		waitForCondition(t, time.Second, func() bool { return manual.ActiveTimers() == 1 }, "skip ticker")
		manual.Advance(time.Second)
		time.Sleep(10 * time.Millisecond)
		if got := len(d.AllJobs()); got != 0 {
			t.Fatalf("skip enqueued backlog: got %d jobs", got)
		}
		manual.Advance(30 * time.Minute)
		waitForCondition(t, time.Second, func() bool { return len(d.AllJobs()) == 1 }, "next current occurrence")
	})
}
