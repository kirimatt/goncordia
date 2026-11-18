package memory_test

import (
	"testing"

	"github.com/kirimatt/goncordia/v2/driver/drivertest"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

func TestConformance(t *testing.T) {
	d := memory.New()
	drivertest.Run(t, d.Executor())
}

func TestResultStoreConformance(t *testing.T) {
	drivertest.RunResults(t, memory.New().Executor())
}

func TestRetentionConformance(t *testing.T) {
	drivertest.RunRetention(t, memory.New())
}
