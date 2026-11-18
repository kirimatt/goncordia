package mongodbv2_test

import (
	"context"
	"errors"
	"testing"
	"time"

	mongodriver "github.com/kirimatt/goncordia/driver/mongodbv2/v2"
	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
	"github.com/kirimatt/goncordia/v2/future"
)

type tripleArgs struct{ N int }

func (tripleArgs) Kind() string { return "triple" }

var tripleTask = future.Define[tripleArgs, int]()

// TestMongo_FutureWithSessionTransactions enqueues bound tasks inside
// mongo-driver v2 session transactions and awaits them from a separate
// Manager, so results arrive by polling MongoDB.
func TestMongo_FutureWithSessionTransactions(t *testing.T) {
	d, mongoClient := newDriver(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	workerSide, err := future.New(d, future.Config{})
	if err != nil {
		t.Fatal(err)
	}
	producerSide, err := future.New(d, future.Config{PollInterval: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	registry := core.NewRegistry()
	tripleTask.Register(registry, workerSide, func(_ context.Context, job *core.Job[tripleArgs]) (int, error) {
		return job.Args.N * 3, nil
	}, core.WorkerOpts{})
	wp := mongodriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues: []string{"default"}, PollInterval: 50 * time.Millisecond, Observer: workerSide,
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = wp.Start(ctx)
	}()
	defer func() {
		cancel()
		<-done
	}()

	triple := future.BindTx(producerSide, mongodriver.NewClient(d, goncordia.ClientConfig{}), tripleTask)
	session, err := mongoClient.StartSession()
	if err != nil {
		t.Fatal(err)
	}
	defer session.EndSession(context.Background())

	// Rolled back: the job never exists.
	if err := session.StartTransaction(); err != nil {
		t.Fatal(err)
	}
	lost, err := triple.EnqueueTx(ctx, session, tripleArgs{N: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := session.AbortTransaction(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := lost.Await(ctx); !errors.Is(err, future.ErrNotFound) {
		t.Fatalf("Await of a rolled-back job err = %v, want ErrNotFound", err)
	}

	// Committed through WithTransaction: the result arrives.
	res, err := session.WithTransaction(ctx, func(ctx context.Context) (any, error) {
		return triple.EnqueueTx(ctx, session, tripleArgs{N: 14})
	})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := res.(*future.Future[int]).Await(ctx); err != nil || got != 42 {
		t.Fatalf("Await = %d, %v", got, err)
	}

	if got, err := triple.Call(ctx, tripleArgs{N: 5}); err != nil || got != 15 {
		t.Fatalf("Call = %d, %v", got, err)
	}
}

// TestMongo_ExecutorBeginCommitAndRollback covers transactions started by the
// driver itself rather than by the caller.
func TestMongo_ExecutorBeginCommitAndRollback(t *testing.T) {
	d, _ := newDriver(t)
	ctx := context.Background()
	for _, commit := range []bool{true, false} {
		tx, err := d.Executor().Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		res, err := tx.JobInsertMany(ctx, []driver.JobInsertParams{{Queue: "default", Kind: "email", Args: []byte(`{}`)}})
		if err != nil {
			t.Fatal(err)
		}
		if commit {
			err = tx.Commit(ctx)
		} else {
			err = tx.Rollback(ctx)
		}
		if err != nil {
			t.Fatal(err)
		}
		_, err = d.Executor().JobGetByID(ctx, res[0].Job.ID)
		if commit && err != nil {
			t.Fatalf("committed job missing: %v", err)
		}
		if !commit && err == nil {
			t.Fatal("rolled-back job is visible")
		}
	}
}
