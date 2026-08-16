package core_test

import (
	"testing"
	"time"

	"github.com/kirimatt/goncordia/core"
)

func TestCronUsesExplicitLocation(t *testing.T) {
	newYork, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	schedule, err := core.Cron("0 9 * * *", newYork)
	if err != nil {
		t.Fatal(err)
	}
	last := time.Date(2026, 3, 7, 15, 0, 0, 0, time.UTC)
	next := schedule.Next(last)
	want := time.Date(2026, 3, 8, 9, 0, 0, 0, newYork)
	if !next.Equal(want) || next.Location() != newYork {
		t.Fatalf("next=%s (%s), want=%s (%s)", next, next.Location(), want, want.Location())
	}
}

func TestCronFirstTickAndValidation(t *testing.T) {
	schedule, err := core.Cron("*/15 * * * *", nil)
	if err != nil {
		t.Fatal(err)
	}
	if next := schedule.Next(time.Time{}); !next.IsZero() {
		t.Fatalf("first tick should be immediate, got %s", next)
	}
	if _, err := core.Cron("not a cron", time.UTC); err == nil {
		t.Fatal("expected invalid expression error")
	}
}
