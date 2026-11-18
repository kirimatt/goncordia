package goncordia_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/clock"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

func TestValidateCronConfigRejects(t *testing.T) {
	d := memory.New()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	every := core.Every(time.Hour)
	for _, tc := range []struct {
		jobs []goncordia.PeriodicJob
		cfg  goncordia.CronConfig
		want string
	}{
		{[]goncordia.PeriodicJob{{ID: "a"}}, goncordia.CronConfig{}, `"a" has no schedule`},
		{[]goncordia.PeriodicJob{{ID: "a", Schedule: every}}, goncordia.CronConfig{}, "requires StartAt"},
		{[]goncordia.PeriodicJob{{ID: "a", Schedule: every, StartAt: start, CatchUp: 9}}, goncordia.CronConfig{}, "invalid catch-up policy 9"},
		{[]goncordia.PeriodicJob{{ID: "a", Schedule: every, StartAt: start, MaxCatchUp: -1}}, goncordia.CronConfig{}, "negative MaxCatchUp"},
		{[]goncordia.PeriodicJob{{ID: "a", Schedule: every, StartAt: start}, {ID: "a", Schedule: every, StartAt: start}}, goncordia.CronConfig{}, `duplicate periodic job ID "a"`},
		{nil, goncordia.CronConfig{LeaderTTL: -1}, "must not be negative"},
	} {
		err := goncordia.ValidateCronConfig(d, tc.jobs, tc.cfg)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("err = %v, want %q", err, tc.want)
		}
	}
	noLeases := &limitedCapabilityDriver{Driver: d}
	if err := goncordia.ValidateCronConfig[memory.NoTx](noLeases, nil, goncordia.CronConfig{}); err == nil ||
		!strings.Contains(err.Error(), "requires linearizable leases") {
		t.Fatalf("driver without leases: %v", err)
	}
	cs := goncordia.NewCronScheduler[memory.NoTx](noLeases, nil, goncordia.CronConfig{})
	if err := cs.Start(context.Background()); err == nil {
		t.Fatal("Start accepted an invalid config")
	}
}

func TestCronSchedulerRejectsSecondStart(t *testing.T) {
	clk := clock.NewManual(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	cs := goncordia.NewCronScheduler(memory.New(memory.WithClock(clk)), nil, goncordia.CronConfig{Clock: clk})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cs.Start(ctx) }()
	waitForCondition(t, time.Second, func() bool { return clk.ActiveTimers() == 1 }, "cron ticker")
	if err := cs.Start(ctx); err == nil || !strings.Contains(err.Error(), "already started") {
		t.Fatalf("second Start: %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Start returned %v", err)
	}
}

// cronFaultExecutor fails selected scheduler calls.
type cronFaultExecutor struct {
	driver.Executor
	mu                              sync.Mutex
	electErr, cursorErr, advanceErr error
	insertErr, resignErr            error
}

func (e *cronFaultExecutor) fault(err *error) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return *err
}

func (e *cronFaultExecutor) LeaderAttemptElect(ctx context.Context, p driver.LeaderElectParams) (bool, error) {
	if err := e.fault(&e.electErr); err != nil {
		return false, err
	}
	return e.Executor.LeaderAttemptElect(ctx, p)
}

func (e *cronFaultExecutor) LeaderResign(ctx context.Context, p driver.LeaderResignParams) error {
	if err := e.fault(&e.resignErr); err != nil {
		return err
	}
	return e.Executor.LeaderResign(ctx, p)
}

func (e *cronFaultExecutor) ScheduleCursorGetOrCreate(ctx context.Context, p driver.ScheduleCursorCreateParams) (driver.ScheduleCursorResult, error) {
	if err := e.fault(&e.cursorErr); err != nil {
		return driver.ScheduleCursorResult{}, err
	}
	return e.Executor.ScheduleCursorGetOrCreate(ctx, p)
}

func (e *cronFaultExecutor) ScheduleCursorAdvance(ctx context.Context, p driver.ScheduleCursorAdvanceParams) (bool, error) {
	if err := e.fault(&e.advanceErr); err != nil {
		return false, err
	}
	return e.Executor.ScheduleCursorAdvance(ctx, p)
}

func (e *cronFaultExecutor) JobInsertMany(ctx context.Context, p []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	if err := e.fault(&e.insertErr); err != nil {
		return nil, err
	}
	return e.Executor.JobInsertMany(ctx, p)
}

type cronFaultDriver struct {
	*memory.Driver
	exec *cronFaultExecutor
}

func (d *cronFaultDriver) Executor() driver.Executor { return d.exec }

// runCronTick starts a scheduler on a manual clock, fires one tick, stops
// it, and returns the errors it reported.
func runCronTick(t *testing.T, mem *memory.Driver, clk *clock.Manual, exec *cronFaultExecutor, jobs []goncordia.PeriodicJob, wantJobs int) []error {
	t.Helper()
	exec.Executor = mem.Executor()
	var mu sync.Mutex
	var errs []error
	cs := goncordia.NewCronScheduler[memory.NoTx](&cronFaultDriver{Driver: mem, exec: exec}, jobs, goncordia.CronConfig{
		TickInterval: time.Second, Clock: clk,
		ErrorHandler: func(err error) { mu.Lock(); errs = append(errs, err); mu.Unlock() },
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- cs.Start(ctx) }()
	waitForCondition(t, time.Second, func() bool { return clk.ActiveTimers() == 1 }, "cron ticker")
	before := len(mem.AllJobs())
	clk.Advance(time.Second)
	// A tick ends either with an error or with wantJobs new jobs.
	waitForCondition(t, time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(errs) > 0 || len(mem.AllJobs()) >= before+wantJobs
	}, "tick outcome")
	cancel()
	<-done
	mu.Lock()
	defer mu.Unlock()
	return append([]error(nil), errs...)
}

func requireCronError(t *testing.T, errs []error, want string) {
	t.Helper()
	for _, err := range errs {
		if strings.Contains(err.Error(), want) {
			return
		}
	}
	t.Fatalf("errors %v lack %q", errs, want)
}

func TestCronSchedulerReportsStoreFailures(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	now := start.Add(90 * time.Minute)
	boom := errors.New("store down")
	durable := []goncordia.PeriodicJob{{ID: "d", StartAt: start, Schedule: core.Every(time.Hour), Args: cronJob{N: 1}}}
	local := []goncordia.PeriodicJob{{Schedule: core.Every(time.Hour), Args: cronJob{N: 2}}}

	for _, tc := range []struct {
		name string
		exec *cronFaultExecutor
		jobs []goncordia.PeriodicJob
		want string
	}{
		{"election", &cronFaultExecutor{electErr: boom}, durable, "cron leader election"},
		{"cursor", &cronFaultExecutor{cursorErr: boom}, durable, `load periodic cursor "d"`},
		{"durable enqueue", &cronFaultExecutor{insertErr: boom}, durable, `enqueue periodic job "d" occurrence`},
		{"local enqueue", &cronFaultExecutor{insertErr: boom}, local, `enqueue periodic job "cron_job"`},
		{"advance", &cronFaultExecutor{advanceErr: boom}, durable, `advance periodic cursor "d"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clk := clock.NewManual(now)
			errs := runCronTick(t, memory.New(memory.WithClock(clk)), clk, tc.exec, tc.jobs, 1)
			requireCronError(t, errs, tc.want)
		})
	}

	t.Run("advance after enqueue", func(t *testing.T) {
		// The advance error surfaces after the job is inserted; wait for it.
		clk := clock.NewManual(now)
		mem := memory.New(memory.WithClock(clk))
		exec := &cronFaultExecutor{advanceErr: boom, resignErr: boom}
		exec.Executor = mem.Executor()
		var mu sync.Mutex
		var errs []error
		cs := goncordia.NewCronScheduler[memory.NoTx](&cronFaultDriver{Driver: mem, exec: exec}, durable, goncordia.CronConfig{
			TickInterval: time.Second, Clock: clk,
			ErrorHandler: func(err error) { mu.Lock(); errs = append(errs, err); mu.Unlock() },
		})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- cs.Start(ctx) }()
		waitForCondition(t, time.Second, func() bool { return clk.ActiveTimers() == 1 }, "cron ticker")
		clk.Advance(time.Second)
		waitForCondition(t, time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(errs) > 0 }, "advance error")
		cancel()
		<-done
		mu.Lock()
		defer mu.Unlock()
		requireCronError(t, errs, `advance periodic cursor "d"`)
		requireCronError(t, errs, "cron leader resignation")
	})
}

func TestCronSchedulerBadSchedules(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	t.Run("schedule does not advance", func(t *testing.T) {
		clk := clock.NewManual(start.Add(time.Hour))
		stuck := core.ScheduleFunc(func(last time.Time) time.Time { return last })
		errs := runCronTick(t, memory.New(memory.WithClock(clk)), clk, &cronFaultExecutor{},
			[]goncordia.PeriodicJob{{ID: "s", StartAt: start, Schedule: stuck, Args: cronJob{N: 3}}}, 1)
		requireCronError(t, errs, `"s" schedule did not advance`)
	})

	t.Run("too many missed occurrences", func(t *testing.T) {
		clk := clock.NewManual(start.Add(48 * time.Hour))
		errs := runCronTick(t, memory.New(memory.WithClock(clk)), clk, &cronFaultExecutor{},
			[]goncordia.PeriodicJob{{ID: "m", StartAt: start, CatchUp: goncordia.CronCatchUpLatest, Schedule: core.Every(time.Second), Args: cronJob{N: 4}}}, 1)
		requireCronError(t, errs, `"m" exceeded 100000 missed occurrences`)
	})

	t.Run("catch-up all honours the job limit", func(t *testing.T) {
		clk := clock.NewManual(start.Add(10 * time.Hour))
		mem := memory.New(memory.WithClock(clk))
		runCronTick(t, mem, clk, &cronFaultExecutor{},
			[]goncordia.PeriodicJob{{ID: "a", StartAt: start, MaxCatchUp: 2, Schedule: core.Every(time.Hour), Args: cronJob{N: 5}}}, 2)
		if got := len(mem.AllJobs()); got != 2 {
			t.Fatalf("enqueued %d occurrences, want the job limit of 2", got)
		}
	})
}
