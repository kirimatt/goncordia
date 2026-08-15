package memory_test

import (
	"testing"

	"github.com/kirimatt/goncordia/driver/drivertest"
	"github.com/kirimatt/goncordia/driver/memory"
)

func TestConformance(t *testing.T) {
	d := memory.New()
	drivertest.Run(t, d.Executor())
}
