package pgxv5_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	pgxdriver "github.com/kirimatt/goncordia/driver/pgxv5/v2"
	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/future"
)

// --- Contract: shared by producers and workers. ---

type InvoiceArgs struct {
	OrderID string `json:"order_id"`
}

func (InvoiceArgs) Kind() string { return "invoice" }

type Invoice struct {
	Number string `json:"number"`
	Total  int64  `json:"total"`
}

var IssueInvoice = future.Define[InvoiceArgs, Invoice]()

// --- Worker code. ---

func issueInvoice(_ context.Context, job *core.Job[InvoiceArgs]) (Invoice, error) {
	return Invoice{Number: "INV-" + job.Args.OrderID, Total: 4200}, nil
}

// --- Caller code: the order and its invoice job commit together. ---

type Orders struct {
	db      *pgxpool.Pool
	invoice *future.TxFunc[InvoiceArgs, Invoice, pgx.Tx]
}

func (o *Orders) Place(ctx context.Context, orderID string) (Invoice, error) {
	tx, err := o.db.Begin(ctx)
	if err != nil {
		return Invoice{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx, `INSERT INTO orders (id) VALUES ($1)`, orderID); err != nil {
		return Invoice{}, err
	}
	f, err := o.invoice.EnqueueTx(ctx, tx, InvoiceArgs{OrderID: orderID})
	if err != nil {
		return Invoice{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Invoice{}, err
	}
	return f.Await(ctx) // only after the commit
}

// Example_future wires pgx v5 in main and places an order from application
// code. Producer and worker may also run in different processes; the producer
// then learns the result by polling Postgres every PollInterval.
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
	m, err := future.New(d, future.Config{})
	if err != nil {
		log.Fatal(err)
	}

	registry := core.NewRegistry()
	IssueInvoice.Register(registry, m, issueInvoice, core.WorkerOpts{MaxRetry: 5})
	wp := pgxdriver.NewWorkerPool(d, registry, goncordia.WorkerConfig{Observer: m})
	go func() { _ = wp.Start(ctx) }()
	defer func() { _ = wp.Shutdown(context.Background()) }()

	client := pgxdriver.NewClient(d, goncordia.ClientConfig{})
	orders := &Orders{db: pool, invoice: future.BindTx(m, client, IssueInvoice)}

	invoice, err := orders.Place(ctx, "42")
	switch {
	case errors.Is(err, future.ErrDiscarded):
		log.Printf("invoice failed permanently: %v", err)
	case err != nil:
		log.Fatal(err)
	default:
		fmt.Println(invoice.Number, invoice.Total) // INV-42 4200
	}
}
