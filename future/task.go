package future

import (
	"context"

	"github.com/kirimatt/goncordia/v2"
	"github.com/kirimatt/goncordia/v2/core"
	"github.com/kirimatt/goncordia/v2/driver"
)

// Task ties a job's argument type A to the result type R its handler returns.
// Declare it once next to A and share it between producers and workers, so
// neither side spells R by hand:
//
//	var Sum = future.Define[SumArgs, int]()
//
// A Task holds no state; it only carries the two types.
type Task[A core.JobArgs, R any] struct{}

// Define declares a Task for jobs with arguments A that produce R.
func Define[A core.JobArgs, R any]() Task[A, R] { return Task[A, R]{} }

// Register registers h as the handler of the task. The compiler rejects a
// handler whose argument or result type does not match the Task.
func (Task[A, R]) Register(registry *core.Registry, m *Manager, h Handler[A, R], opts core.WorkerOpts) {
	RegisterWorker(registry, m, h, opts)
}

// Of returns a Future for an existing job of this task.
func (Task[A, R]) Of(m *Manager, jobID string) *Future[R] { return Of[R](m, jobID) }

type enqueueFunc func(context.Context, core.JobArgs, *core.InsertOpts) (*driver.JobInsertResult, error)

// Func is a task bound to a Manager and a client. Pass it to the code that
// needs results: callers write sum.Call(ctx, args) and get an R back, without
// type parameters or access to the queue client.
//
// A Func is immutable and safe for concurrent use.
type Func[A core.JobArgs, R any] struct {
	m       *Manager
	enqueue enqueueFunc
	opts    *core.InsertOpts
}

// Bind binds task to m and client.
func Bind[A core.JobArgs, R any, TTx any](m *Manager, client *goncordia.Client[TTx], task Task[A, R]) *Func[A, R] {
	if m == nil || client == nil {
		panic("goncordia/future: Bind requires a Manager and a client")
	}
	_ = task
	return &Func[A, R]{m: m, enqueue: client.Enqueue}
}

// With returns a copy of f that enqueues with opts, for example a queue,
// priority, or unique options. Nil restores the client defaults.
func (f *Func[A, R]) With(opts *core.InsertOpts) *Func[A, R] {
	c := *f
	c.opts = opts
	return &c
}

// Enqueue inserts a job and returns its Future without waiting.
func (f *Func[A, R]) Enqueue(ctx context.Context, args A) (*Future[R], error) {
	result, err := f.enqueue(ctx, args, f.opts)
	if err != nil {
		return nil, err
	}
	return attach[R](f.m, result, args)
}

// Call inserts a job and waits for its result. If ctx ends first, Call
// returns ctx's error and the job keeps running; use Enqueue and Future.ID to
// follow it later.
func (f *Func[A, R]) Call(ctx context.Context, args A) (R, error) {
	future, err := f.Enqueue(ctx, args)
	if err != nil {
		var zero R
		return zero, err
	}
	return future.Await(ctx)
}

// Of returns a Future for an existing job of this task.
func (f *Func[A, R]) Of(jobID string) *Future[R] { return Of[R](f.m, jobID) }

// TxFunc is a Func that can also enqueue inside a transaction of type TTx.
type TxFunc[A core.JobArgs, R any, TTx any] struct {
	*Func[A, R]
	enqueueTx func(context.Context, TTx, core.JobArgs, *core.InsertOpts) (*driver.JobInsertResult, error)
}

// BindTx binds task to m and client, keeping transactional enqueue.
func BindTx[A core.JobArgs, R any, TTx any](m *Manager, client *goncordia.Client[TTx], task Task[A, R]) *TxFunc[A, R, TTx] {
	return &TxFunc[A, R, TTx]{Func: Bind(m, client, task), enqueueTx: client.EnqueueTx}
}

// With returns a copy of f that enqueues with opts.
func (f *TxFunc[A, R, TTx]) With(opts *core.InsertOpts) *TxFunc[A, R, TTx] {
	return &TxFunc[A, R, TTx]{Func: f.Func.With(opts), enqueueTx: f.enqueueTx}
}

// EnqueueTx inserts a job inside tx. The job is invisible until tx commits,
// so call Await only after the commit.
func (f *TxFunc[A, R, TTx]) EnqueueTx(ctx context.Context, tx TTx, args A) (*Future[R], error) {
	result, err := f.enqueueTx(ctx, tx, args, f.opts)
	if err != nil {
		return nil, err
	}
	return attach[R](f.m, result, args)
}
