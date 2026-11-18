package driver_test

import (
	"testing"

	"github.com/kirimatt/goncordia/v2/driver"
)

func TestCountQueueStats(t *testing.T) {
	stats := driver.CountQueueStats("q", []driver.JobRow{
		{State: driver.JobStateAvailable}, {State: driver.JobStateAvailable}, {State: driver.JobStateCompleted},
	})
	if stats.Queue != "q" || stats.Total != 3 ||
		stats.States[driver.JobStateAvailable] != 2 || stats.States[driver.JobStateCompleted] != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	if empty := driver.CountQueueStats("q", nil); empty.Total != 0 || len(empty.States) != 0 {
		t.Fatalf("empty stats = %+v", empty)
	}
}
