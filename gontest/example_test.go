package gontest_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/kirimatt/goncordia/gontest/v2"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
)

// --- Application code under test. ---

type SendEmailArgs struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
}

func (SendEmailArgs) Kind() string { return "send_email" }

// PlaceOrder is business code that enqueues a confirmation email.
func PlaceOrder(ctx context.Context, client *gontest.Client, orderID string) error {
	_, err := client.Enqueue(ctx, SendEmailArgs{
		To:      "buyer@example.com",
		Subject: "Order " + orderID + " confirmed",
	}, nil)
	return err
}

var emailWorker = core.WorkerFunc[SendEmailArgs](func(_ context.Context, job *core.Job[SendEmailArgs]) error {
	if job.Args.To == "" {
		return errors.New("missing recipient")
	}
	fmt.Println("sent:", job.Args.Subject)
	return nil
})

// exampleT stands in for the *testing.T a real test receives. Examples have
// no *testing.T, so it implements the parts of testing.TB that gontest uses.
type exampleT struct {
	testing.TB
	cleanups []func()
}

func (*exampleT) Helper()                        {}
func (t *exampleT) Cleanup(f func())             { t.cleanups = append(t.cleanups, f) }
func (*exampleT) Fatal(args ...any)              { panic(fmt.Sprint(args...)) }
func (*exampleT) Fatalf(format string, a ...any) { panic(fmt.Sprintf(format, a...)) }

func (t *exampleT) finish() {
	for i := len(t.cleanups) - 1; i >= 0; i-- {
		t.cleanups[i]()
	}
}

// ExampleRequireEnqueued asserts that business code enqueued exactly one job
// and inspects its arguments. In a real test, pass the test's *testing.T.
func ExampleRequireEnqueued() {
	t := &exampleT{}
	defer t.finish()
	ctx := context.Background()

	client, tracker := gontest.NewClient(t)
	if err := PlaceOrder(ctx, client, "order-123"); err != nil {
		panic(err)
	}

	jobs := gontest.RequireEnqueued[SendEmailArgs](t, tracker, 1)
	fmt.Println(jobs[0].Args.To)
	fmt.Println(jobs[0].Args.Subject)

	// Output:
	// buyer@example.com
	// Order order-123 confirmed
}

// ExampleRequireWork unit-tests a worker directly, without a WorkerPool or
// database. The test fails if the worker returns an error.
func ExampleRequireWork() {
	t := &exampleT{}
	ctx := context.Background()

	gontest.RequireWork(t, ctx, emailWorker, SendEmailArgs{
		To:      "user@example.com",
		Subject: "Welcome",
	})

	// Output:
	// sent: Welcome
}

// ExampleNewMockClock controls time so a job scheduled for later becomes
// available only after the clock is advanced.
func ExampleNewMockClock() {
	t := &exampleT{}
	defer t.finish()
	ctx := context.Background()

	clk := gontest.NewMockClock()
	client, tracker := gontest.NewClientWithClock(t, clk)
	gontest.MustEnqueue(t, ctx, client, SendEmailArgs{To: "user@example.com", Subject: "Reminder"},
		&core.InsertOpts{RunAt: clk.Now().Add(time.Hour)})

	fetch := func() int {
		jobs, err := tracker.Driver().Executor().JobFetchBatch(ctx, driver.FetchParams{
			Queue: "default", Limit: 10, WorkerID: "example", LeaseDuration: time.Minute,
		})
		if err != nil {
			panic(err)
		}
		return len(jobs)
	}

	fmt.Println("available before Advance:", fetch())
	clk.Advance(2 * time.Hour)
	fmt.Println("available after Advance:", fetch())

	// Output:
	// available before Advance: 0
	// available after Advance: 1
}
