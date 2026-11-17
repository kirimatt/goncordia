package pgxv5_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	pgxdriver "github.com/kirimatt/goncordia/driver/pgxv5/v2"
	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/future"
)

type InvoiceArgs struct {
	OrderID string `json:"order_id"`
}

func (InvoiceArgs) Kind() string { return "invoice" }

type Invoice struct {
	Number string `json:"number"`
	Total  int64  `json:"total"`
}

// Example_future enqueues a job inside a business transaction and awaits the
// worker's result. Producer and worker may run in different processes: the
// producer then learns the result by polling Postgres every PollInterval.
func Example_future() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	d := pgxdriver.New(pool)
	if err := d.Migrate(ctx); err != nil { // creates goncordia_job_results
		log.Fatal(err)
	}
	m, err := future.New(d, future.Config{PollInterval: 200 * time.Millisecond})
	if err != nil {
		log.Fatal(err)
	}

	// Worker side.
	registry := core.NewRegistry()
	future.RegisterWorker(registry, m, func(ctx context.Context, job *core.Job[InvoiceArgs]) (Invoice, error) {
		return Invoice{Number: "INV-" + job.Args.OrderID, Total: 4200}, nil
	}, core.WorkerOpts{MaxRetry: 5})
	wp := pgxdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{
		Queues: []string{"default"}, Observer: m,
	})
	go func() { _ = wp.Start(ctx) }()
	defer func() { _ = wp.Shutdown(context.Background()) }()

	// Producer side: the job exists only if the order transaction commits.
	client := pgxdriver.NewClient(d, goncordia.ClientConfig{})
	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO orders (id) VALUES ($1)`, "42"); err != nil {
		log.Fatal(err)
	}
	f, err := future.EnqueueTx[Invoice](ctx, m, client, tx, InvoiceArgs{OrderID: "42"}, nil)
	if err != nil {
		log.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		log.Fatal(err)
	}

	waitCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	invoice, err := f.Await(waitCtx)
	switch {
	case errors.Is(err, future.ErrDiscarded):
		log.Printf("job %s failed permanently: %v", f.ID(), err)
	case err != nil:
		log.Fatal(err)
	default:
		fmt.Println(invoice.Number, invoice.Total)
	}

	// Another process can await the same job by ID.
	_ = future.Of[Invoice](m, f.ID())
}
