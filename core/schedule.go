package core

import (
	"fmt"
	"time"

	cronlib "github.com/robfig/cron/v3"
)

// Schedule determines when a periodic job should next run.
type Schedule interface {
	// Next returns the next run time given the time of the last run.
	// If last is zero (first run), implementations should return the zero
	// time to signal "run immediately on the first tick".
	Next(last time.Time) time.Time
}

// ScheduleFunc adapts a plain function to the Schedule interface.
type ScheduleFunc func(last time.Time) time.Time

func (f ScheduleFunc) Next(last time.Time) time.Time { return f(last) }

type everySchedule struct{ d time.Duration }

// Every returns a Schedule that fires every d duration.
// The job runs on the first scheduler tick, then every d thereafter.
func Every(d time.Duration) Schedule { return everySchedule{d: d} }

func (s everySchedule) Next(last time.Time) time.Time {
	if last.IsZero() {
		return time.Time{} // zero → fire immediately
	}
	return last.Add(s.d)
}

type cronSchedule struct {
	schedule cronlib.Schedule
	location *time.Location
}

// Cron parses a standard five-field cron expression (minute, hour,
// day-of-month, month, day-of-week). location controls calendar evaluation and
// daylight-saving transitions; nil means UTC. Like Every, a Cron schedule fires
// immediately on its first scheduler tick, then follows the expression.
func Cron(expression string, location *time.Location) (Schedule, error) {
	parsed, err := cronlib.ParseStandard(expression)
	if err != nil {
		return nil, fmt.Errorf("parse cron expression %q: %w", expression, err)
	}
	if location == nil {
		location = time.UTC
	}
	return cronSchedule{schedule: parsed, location: location}, nil
}

func (s cronSchedule) Next(last time.Time) time.Time {
	if last.IsZero() {
		return time.Time{}
	}
	return s.schedule.Next(last.In(s.location))
}
