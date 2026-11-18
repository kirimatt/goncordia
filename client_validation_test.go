package goncordia_test

import (
	"context"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

type emptyKindArgs struct{}

func (emptyKindArgs) Kind() string { return "" }

func TestEnqueueRejectsInvalidOptions(t *testing.T) {
	ctx := context.Background()
	d := memory.New()
	client := goncordia.NewClient(d, goncordia.ClientConfig{})
	negative := -1
	negativeTimeout := -time.Second
	for name, opts := range map[string]*core.InsertOpts{
		"payload version": {PayloadVersion: -1},
		"max retry":       {MaxRetry: &negative},
		"timeout":         {Timeout: &negativeTimeout},
		"unique period":   {UniqueOpts: &core.UniqueOpts{ByPeriod: -time.Minute}},
	} {
		if _, err := client.Enqueue(ctx, EmailArgs{To: name}, opts); err == nil {
			t.Errorf("Enqueue accepted negative %s", name)
		}
		if _, err := client.EnqueueMany(ctx, []core.JobArgs{EmailArgs{To: name}}, opts); err == nil {
			t.Errorf("EnqueueMany accepted negative %s", name)
		}
		if _, err := client.EnqueueBatch(ctx, []goncordia.InsertRequest{{Args: EmailArgs{To: name}, Opts: opts}}); err == nil {
			t.Errorf("EnqueueBatch accepted negative %s", name)
		}
	}
	if _, err := client.Enqueue(ctx, emptyKindArgs{}, nil); err == nil {
		t.Error("Enqueue accepted an empty kind")
	}
	if jobs := d.AllJobs(); len(jobs) != 0 {
		t.Fatalf("rejected enqueues inserted %d jobs", len(jobs))
	}

	maxRetry, timeout := 7, 3*time.Second
	result, err := client.Enqueue(ctx, EmailArgs{To: "valid"}, &core.InsertOpts{
		Queue: "q", Priority: 2, MaxRetry: &maxRetry, Timeout: &timeout,
		Tags: []string{"a"}, PipelineID: "p", PayloadVersion: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	job := result.Job
	if job.Queue != "q" || job.Priority != 2 || job.MaxRetry != 7 || job.Timeout != timeout ||
		len(job.Tags) != 1 || job.PipelineID != "p" {
		t.Fatalf("inserted job = %+v", job)
	}
}
