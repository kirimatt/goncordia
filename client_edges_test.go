package goncordia_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
	"github.com/kirimatt/goncordia/v2/driver/memory"
)

type rejectAll struct{ calls []goncordia.AdmissionRequest }

func (r *rejectAll) Admit(_ context.Context, req goncordia.AdmissionRequest) error {
	r.calls = append(r.calls, req)
	return goncordia.ErrAdmissionRejected
}

func TestEveryEnqueueMethodConsultsAdmission(t *testing.T) {
	ctx := context.Background()
	d := txMemory{memory.New()}
	admission := &rejectAll{}
	client := goncordia.NewClient[memory.NoTx](d, goncordia.ClientConfig{Admission: admission})
	var tx memory.NoTx
	one := []core.JobArgs{EmailArgs{To: "a"}}
	batch := []goncordia.InsertRequest{{Args: EmailArgs{To: "a"}}}

	calls := map[string]func() error{
		"Enqueue":        func() error { _, err := client.Enqueue(ctx, one[0], nil); return err },
		"EnqueueTx":      func() error { _, err := client.EnqueueTx(ctx, tx, one[0], nil); return err },
		"EnqueueMany":    func() error { _, err := client.EnqueueMany(ctx, one, nil); return err },
		"EnqueueManyTx":  func() error { _, err := client.EnqueueManyTx(ctx, tx, one, nil); return err },
		"EnqueueBatch":   func() error { _, err := client.EnqueueBatch(ctx, batch); return err },
		"EnqueueBatchTx": func() error { _, err := client.EnqueueBatchTx(ctx, tx, batch); return err },
	}
	for name, call := range calls {
		if err := call(); !errors.Is(err, goncordia.ErrAdmissionRejected) {
			t.Errorf("%s: err = %v, want admission rejection", name, err)
		}
	}
	transactional := 0
	for _, req := range admission.calls {
		if req.Transactional {
			transactional++
		}
	}
	if len(admission.calls) != 6 || transactional != 3 {
		t.Fatalf("admission saw %d requests, %d transactional; want 6 and 3", len(admission.calls), transactional)
	}
	if len(d.AllJobs()) != 0 {
		t.Fatalf("rejected enqueues stored %d jobs", len(d.AllJobs()))
	}
}

// shortInsert returns no results, which a client must not accept for a
// single insert.
type shortInsert struct{ driver.Executor }

func (shortInsert) JobInsertMany(context.Context, []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return nil, nil
}

type shortInsertDriver struct{ txMemory }

func (d shortInsertDriver) Executor() driver.Executor { return shortInsert{d.txMemory.Executor()} }
func (d shortInsertDriver) UnwrapTx(memory.NoTx) driver.ExecutorTx {
	return shortInsertTx{d.txMemory.UnwrapTx(memory.NoTx{})}
}

type shortInsertTx struct{ driver.ExecutorTx }

func (shortInsertTx) JobInsertMany(context.Context, []driver.JobInsertParams) ([]driver.JobInsertResult, error) {
	return nil, nil
}

func TestEnqueueRejectsMissingDriverResult(t *testing.T) {
	ctx := context.Background()
	observer := &recordingClientObserver{}
	client := goncordia.NewClient[memory.NoTx](shortInsertDriver{txMemory{memory.New()}}, goncordia.ClientConfig{Observer: observer})
	if _, err := client.Enqueue(ctx, EmailArgs{To: "a"}, nil); err == nil || !strings.Contains(err.Error(), "returned 0 results for one insert") {
		t.Fatalf("Enqueue: %v", err)
	}
	if observer.finish.Err == nil {
		t.Fatal("observer did not see the failure")
	}
	if _, err := client.EnqueueTx(ctx, memory.NoTx{}, EmailArgs{To: "a"}, nil); err == nil || !strings.Contains(err.Error(), "returned 0 results for one transactional insert") {
		t.Fatalf("EnqueueTx: %v", err)
	}
}

type badVersionArgs struct{}

func (badVersionArgs) Kind() string        { return "bad_version" }
func (badVersionArgs) PayloadVersion() int { return 0 }

type unmarshalableArgs struct{ C chan int }

func (unmarshalableArgs) Kind() string { return "unmarshalable" }

func TestEnqueueRejectsBadArgs(t *testing.T) {
	ctx := context.Background()
	client := goncordia.NewClient(memory.New(), goncordia.ClientConfig{})
	if _, err := client.Enqueue(ctx, badVersionArgs{}, nil); err == nil || !strings.Contains(err.Error(), "payload version must be positive") {
		t.Fatalf("version 0: %v", err)
	}
	if _, err := client.Enqueue(ctx, unmarshalableArgs{C: make(chan int)}, nil); err == nil || !strings.Contains(err.Error(), "marshal job args") {
		t.Fatalf("channel args: %v", err)
	}
	for name, call := range map[string]func() error{
		"EnqueueMany": func() error { _, err := client.EnqueueMany(ctx, []core.JobArgs{badVersionArgs{}}, nil); return err },
		"EnqueueBatch": func() error {
			_, err := client.EnqueueBatch(ctx, []goncordia.InsertRequest{{Args: badVersionArgs{}}})
			return err
		},
	} {
		if err := call(); err == nil {
			t.Errorf("%s accepted invalid args", name)
		}
	}
}

// bareObserver returns neither a context nor a completion callback.
type bareObserver struct{ start goncordia.EnqueueStart }

func (o *bareObserver) StartEnqueue(_ context.Context, start goncordia.EnqueueStart) (context.Context, func(goncordia.EnqueueFinish)) {
	o.start = start
	return nil, nil
}

func TestEnqueueObserverEdges(t *testing.T) {
	ctx := context.Background()
	d := memory.New()
	bare := &bareObserver{}
	client := goncordia.NewClient(d, goncordia.ClientConfig{Observer: bare})
	_, err := client.EnqueueBatch(ctx, []goncordia.InsertRequest{
		{Args: EmailArgs{To: "a"}, Opts: &core.InsertOpts{Queue: "q1"}},
		{Args: featureArgs{N: 1}, Opts: &core.InsertOpts{Queue: "q1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if bare.start.Queue != "q1" || bare.start.Kind != "" || bare.start.Count != 2 {
		t.Fatalf("mixed-kind batch start = %+v", bare.start)
	}

	observer := &recordingClientObserver{}
	client = goncordia.NewClient(d, goncordia.ClientConfig{Observer: observer})
	unique := &core.InsertOpts{UniqueOpts: &core.UniqueOpts{ByArgs: true}}
	if _, err := client.Enqueue(ctx, EmailArgs{To: "once"}, unique); err != nil {
		t.Fatal(err)
	}
	if _, err := client.Enqueue(ctx, EmailArgs{To: "once"}, unique); err != nil {
		t.Fatal(err)
	}
	if observer.finish.UniqueSkipped != 1 || observer.finish.Inserted != 0 {
		t.Fatalf("duplicate observed as %+v", observer.finish)
	}
}
