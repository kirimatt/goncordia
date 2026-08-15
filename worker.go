package goncordia

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver"
)

// JobMiddleware wraps job execution. Call next to continue the chain.
// Middleware is applied around the actual job handler, inside panic recovery,
// so err is never nil when the handler panicked — panics are converted to errors.
type JobMiddleware func(ctx context.Context, job *core.RawJob, next func(context.Context, *core.RawJob) error) error

// WorkerConfig configures the worker pool.
type WorkerConfig struct {
	// Queues lists the queues this worker pool processes.
	// If empty, only "default" is polled.
	Queues []string
	// Concurrency is the maximum number of jobs running simultaneously.
	// Default: 10.
	Concurrency int
	// MaxPending bounds jobs claimed by this process but waiting for pipeline,
	// per-kind, or global execution capacity. Default: 4 * Concurrency.
	MaxPending int
	// WorkerID identifies claims made by this pool. If empty, a random ID is generated.
	WorkerID string
	// PollInterval is how long to wait between polls when the queue is empty.
	// Only used when the backend has no push notification support.
	// Default: 1 second.
	PollInterval time.Duration
	// RetryPolicy controls retry timing. Default: ExponentialRetry.
	RetryPolicy core.RetryPolicy
	// ShutdownTimeout is the max duration to wait for in-flight jobs during shutdown.
	// Default: 30 seconds.
	ShutdownTimeout time.Duration
	// StuckJobTimeout is the age after which an abandoned running job is made
	// available again. Default: 1 hour. Set to a negative duration to disable.
	StuckJobTimeout time.Duration
	// RescueInterval controls how often abandoned jobs are checked. Default: 1 minute.
	RescueInterval time.Duration
	// HeartbeatInterval controls how often active claims renew their lease. It
	// defaults to one third of StuckJobTimeout. A negative value disables it.
	HeartbeatInterval time.Duration
	// Clock overrides the time source. Defaults to clock.Real{}.
	// Inject clock.NewMock() in tests to control time.
	Clock clock.Clock
	// Middleware is applied around each job execution in order (outermost first).
	// Use it to add tracing, logging, or metrics without modifying job handlers.
	Middleware []JobMiddleware
	// ErrorHandler receives asynchronous fetch, rescue, and state-transition errors.
	// Default: slog.Error.
	ErrorHandler func(error)
}

// WorkerPool processes jobs from the queue using a pool of goroutines.
// TTx is the driver's transaction type (needed only for type parameter inference;
// the pool itself does not open user-visible transactions).
type WorkerPool[TTx any] struct {
	driver   driver.Driver[TTx]
	registry *core.Registry
	config   WorkerConfig

	wg             sync.WaitGroup
	sem            chan struct{}
	pending        chan struct{}
	shutdownOnce   sync.Once
	shutdownCh     chan struct{}
	isShuttingDown atomic.Bool
	started        atomic.Bool
	queueCursor    atomic.Uint64
	fetchMu        sync.Mutex
	pipelineMu     sync.Mutex
	pipelineTails  map[string]chan struct{}
	kindSemMu      sync.Mutex
	kindSemaphores map[string]chan struct{}
}

// NewWorkerPool creates a WorkerPool.
// Register workers using core.RegisterWorker before calling Start.
func NewWorkerPool[TTx any](d driver.Driver[TTx], registry *core.Registry, cfg WorkerConfig) *WorkerPool[TTx] {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 10
	}
	if cfg.MaxPending <= 0 {
		cfg.MaxPending = cfg.Concurrency * 4
	}
	if cfg.MaxPending < cfg.Concurrency {
		cfg.MaxPending = cfg.Concurrency
	}
	if cfg.WorkerID == "" {
		cfg.WorkerID = newWorkerID()
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.ShutdownTimeout <= 0 {
		cfg.ShutdownTimeout = 30 * time.Second
	}
	if cfg.StuckJobTimeout == 0 {
		cfg.StuckJobTimeout = time.Hour
	}
	if cfg.RescueInterval <= 0 {
		cfg.RescueInterval = time.Minute
	}
	if cfg.HeartbeatInterval == 0 && cfg.StuckJobTimeout > 0 {
		cfg.HeartbeatInterval = cfg.StuckJobTimeout / 3
	}
	if cfg.StuckJobTimeout > 0 && cfg.HeartbeatInterval >= cfg.StuckJobTimeout {
		cfg.HeartbeatInterval = cfg.StuckJobTimeout / 3
	}
	if cfg.RetryPolicy == nil {
		cfg.RetryPolicy = core.DefaultRetryPolicy
	}
	if len(cfg.Queues) == 0 {
		cfg.Queues = []string{"default"}
	} else {
		seen := make(map[string]struct{}, len(cfg.Queues))
		queues := make([]string, 0, len(cfg.Queues))
		for _, queue := range cfg.Queues {
			if queue == "" {
				queue = "default"
			}
			if _, ok := seen[queue]; ok {
				continue
			}
			seen[queue] = struct{}{}
			queues = append(queues, queue)
		}
		cfg.Queues = queues
	}
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.ErrorHandler == nil {
		cfg.ErrorHandler = func(err error) { slog.Error("goncordia worker error", "error", err) }
	}

	return &WorkerPool[TTx]{
		driver:         d,
		registry:       registry,
		config:         cfg,
		sem:            make(chan struct{}, cfg.Concurrency),
		pending:        make(chan struct{}, cfg.MaxPending),
		shutdownCh:     make(chan struct{}),
		pipelineTails:  make(map[string]chan struct{}),
		kindSemaphores: make(map[string]chan struct{}),
	}
}

func newWorkerID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("worker-%d", workerSequence.Add(1))
	}
	return "worker-" + hex.EncodeToString(b[:])
}

var workerSequence atomic.Uint64

// Start launches the fetch-and-process loops. Blocks until ctx is cancelled or Stop is called.
func (p *WorkerPool[TTx]) Start(ctx context.Context) error {
	if !p.started.CompareAndSwap(false, true) {
		return fmt.Errorf("worker pool already started")
	}
	if listener := p.driver.Listener(); listener != nil {
		return p.runWithNotifications(ctx, listener)
	}
	return p.runWithPolling(ctx)
}

// Stop initiates a graceful shutdown, waiting up to ShutdownTimeout for in-flight jobs.
func (p *WorkerPool[TTx]) Stop() {
	p.shutdownOnce.Do(func() {
		// Serialize shutdown with claiming so no goroutine can be added after
		// Wait begins.
		p.fetchMu.Lock()
		p.isShuttingDown.Store(true)
		close(p.shutdownCh)
		p.fetchMu.Unlock()
	})

	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-p.config.Clock.After(p.config.ShutdownTimeout):
	}
}

// runWithPolling polls the store at PollInterval when no push notifications are available.
func (p *WorkerPool[TTx]) runWithPolling(ctx context.Context) error {
	ticker := p.config.Clock.NewTicker(p.config.PollInterval)
	defer ticker.Stop()
	p.fetchAndDispatch(ctx)
	return p.runMainLoop(ctx, ticker.C())
}

func (p *WorkerPool[TTx]) runMainLoop(ctx context.Context, ticks <-chan time.Time) error {
	var rescue <-chan time.Time
	var rescueTicker clock.Ticker
	if p.config.StuckJobTimeout > 0 {
		if _, ok := p.driver.Executor().(driver.StuckJobRescuer); ok {
			rescueTicker = p.config.Clock.NewTicker(p.config.RescueInterval)
			defer rescueTicker.Stop()
			rescue = rescueTicker.C()
		}
	}

	for {
		select {
		case <-ctx.Done():
			p.Stop()
			return ctx.Err()
		case <-p.shutdownCh:
			return nil
		case <-ticks:
			p.fetchAndDispatch(ctx)
		case <-rescue:
			p.rescueStuck(ctx)
		}
	}
}

// runWithNotifications listens for push notifications and fetches immediately on receipt.
// A fallback ticker also polls periodically in case notifications are missed (e.g. queue resume).
func (p *WorkerPool[TTx]) runWithNotifications(ctx context.Context, l driver.Listener) error {
	fallbackTicker := p.config.Clock.NewTicker(p.config.PollInterval)
	defer fallbackTicker.Stop()
	var subscribed []string
	defer func() {
		cleanupCtx, cancel := clock.WithTimeout(context.Background(), p.config.Clock, 5*time.Second)
		defer cancel()
		for _, queue := range subscribed {
			if err := l.Unlisten(cleanupCtx, queue); err != nil {
				p.reportError(fmt.Errorf("unlisten queue %q: %w", queue, err))
			}
		}
		if err := l.Close(); err != nil {
			p.reportError(fmt.Errorf("close listener: %w", err))
		}
	}()

	for _, q := range p.config.Queues {
		ch, err := l.Listen(ctx, q)
		if err != nil {
			return err
		}
		subscribed = append(subscribed, q)
		p.wg.Add(1)
		go func(notifications <-chan driver.Notification) {
			defer p.wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-p.shutdownCh:
					return
				case _, ok := <-notifications:
					if !ok {
						return
					}
					p.fetchAndDispatch(ctx)
				}
			}
		}(ch)
	}
	p.fetchAndDispatch(ctx)
	return p.runMainLoop(ctx, fallbackTicker.C())
}

// fetchAndDispatch claims a batch of jobs and starts a goroutine per job.
func (p *WorkerPool[TTx]) fetchAndDispatch(ctx context.Context) {
	p.fetchMu.Lock()
	defer p.fetchMu.Unlock()
	if p.isShuttingDown.Load() {
		return
	}

	free := cap(p.pending) - len(p.pending)
	if free <= 0 {
		return
	}

	exec := p.driver.Executor()

	start := int(p.queueCursor.Add(1)-1) % len(p.config.Queues)
	for offset := range len(p.config.Queues) {
		queue := p.config.Queues[(start+offset)%len(p.config.Queues)]
		rows, err := exec.JobFetchBatch(ctx, driver.FetchParams{
			Queue:    queue,
			Limit:    free,
			WorkerID: p.config.WorkerID,
		})
		if err != nil {
			p.reportError(fmt.Errorf("fetch queue %q: %w", queue, err))
			continue
		}
		if len(rows) == 0 {
			continue
		}
		sort.SliceStable(rows, func(i, j int) bool {
			if rows[i].Priority != rows[j].Priority {
				return rows[i].Priority > rows[j].Priority
			}
			if !rows[i].RunAt.Equal(rows[j].RunAt) {
				return rows[i].RunAt.Before(rows[j].RunAt)
			}
			if !rows[i].CreatedAt.Equal(rows[j].CreatedAt) {
				return rows[i].CreatedAt.Before(rows[j].CreatedAt)
			}
			return rows[i].ID < rows[j].ID
		})

		for i := range rows {
			row := rows[i]
			pipelineReady, pipelineDone := p.pipelineGate(row.PipelineID)
			p.pending <- struct{}{}
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				defer func() { <-p.pending }()
				defer pipelineDone()
				claimCtx, stopHeartbeat := p.startHeartbeat(ctx, exec, row)
				defer stopHeartbeat()
				if pipelineReady != nil {
					select {
					case <-pipelineReady:
					case <-claimCtx.Done():
						p.setState(ctx, exec, fencedStateParams(row, driver.JobSetStateParams{Yield: true}))
						return
					case <-p.shutdownCh:
						p.setState(context.Background(), exec, fencedStateParams(row, driver.JobSetStateParams{Yield: true}))
						return
					}
				}
				kindSem := p.kindSemaphore(row.Kind)
				if kindSem != nil {
					select {
					case kindSem <- struct{}{}:
						defer func() { <-kindSem }()
					case <-claimCtx.Done():
						p.setState(ctx, exec, fencedStateParams(row, driver.JobSetStateParams{Yield: true}))
						return
					}
				}
				select {
				case p.sem <- struct{}{}:
					defer func() { <-p.sem }()
				case <-claimCtx.Done():
					p.setState(ctx, exec, fencedStateParams(row, driver.JobSetStateParams{Yield: true}))
					return
				case <-p.shutdownCh:
					p.setState(context.Background(), exec, fencedStateParams(row, driver.JobSetStateParams{Yield: true}))
					return
				}
				p.processRow(claimCtx, exec, row)
			}()
		}
		free = cap(p.pending) - len(p.pending)
		if free <= 0 {
			return
		}
	}
}

func (p *WorkerPool[TTx]) pipelineGate(id string) (<-chan struct{}, func()) {
	if id == "" {
		return nil, func() {}
	}
	p.pipelineMu.Lock()
	previous := p.pipelineTails[id]
	done := make(chan struct{})
	p.pipelineTails[id] = done
	p.pipelineMu.Unlock()
	return previous, func() {
		close(done)
		p.pipelineMu.Lock()
		if p.pipelineTails[id] == done {
			delete(p.pipelineTails, id)
		}
		p.pipelineMu.Unlock()
	}
}

func (p *WorkerPool[TTx]) kindSemaphore(kind string) chan struct{} {
	opts, ok := p.registry.Opts(kind)
	if !ok || opts.Concurrency <= 0 {
		return nil
	}
	p.kindSemMu.Lock()
	defer p.kindSemMu.Unlock()
	if sem := p.kindSemaphores[kind]; sem != nil {
		return sem
	}
	sem := make(chan struct{}, opts.Concurrency)
	p.kindSemaphores[kind] = sem
	return sem
}

// processRow executes a single job, applies middleware, then updates its state.
func (p *WorkerPool[TTx]) processRow(ctx context.Context, exec driver.Executor, row driver.JobRow) {
	// Resolve effective MaxRetry: job-level overrides worker default; 0 means "use worker default".
	maxRetry := row.MaxRetry
	if maxRetry <= 0 {
		if opts, ok := p.registry.Opts(row.Kind); ok && opts.MaxRetry > 0 {
			maxRetry = opts.MaxRetry
		}
	}
	if maxRetry <= 0 {
		maxRetry = 3
	}

	raw := &core.RawJob{
		ID:         row.ID,
		Queue:      row.Queue,
		Kind:       row.Kind,
		Args:       row.Args,
		AttemptNum: row.AttemptNum,
		MaxRetry:   maxRetry,
		CreatedAt:  row.CreatedAt,
		WorkerID:   row.WorkerID,
		Tags:       row.Tags,
		PipelineID: row.PipelineID,
	}

	// Innermost handler: runs the job with panic recovery so middleware always
	// sees a clean error return (panics are never propagated up the chain).
	var handler func(context.Context, *core.RawJob) error
	handler = func(ctx context.Context, job *core.RawJob) (err error) {
		defer func() {
			if r := recover(); r != nil {
				switch v := r.(type) {
				case error:
					err = v
				default:
					err = &panicError{val: v}
				}
			}
		}()
		return p.registry.Process(ctx, job)
	}

	// Wrap with middleware in reverse order (last registered = innermost).
	for i := len(p.config.Middleware) - 1; i >= 0; i-- {
		mw := p.config.Middleware[i]
		next := handler
		handler = func(ctx context.Context, job *core.RawJob) error {
			return mw(ctx, job, next)
		}
	}

	jobCtx := ctx
	var cancel context.CancelFunc
	timeout := row.Timeout
	if timeout <= 0 {
		if opts, ok := p.registry.Opts(row.Kind); ok {
			timeout = opts.Timeout
		}
	}
	if timeout > 0 {
		jobCtx, cancel = clock.WithTimeout(ctx, p.config.Clock, timeout)
		defer cancel()
	}

	jobErr := callHandler(jobCtx, raw, handler)
	// Losing the pool/claim context is not a job failure. Return the fenced
	// claim to the queue without recording an error or consuming an attempt.
	// A job-specific timeout only cancels jobCtx, so it still follows retry rules.
	if ctx.Err() != nil {
		p.setState(ctx, exec, fencedStateParams(row, driver.JobSetStateParams{Yield: true}))
		return
	}

	if jobErr == nil {
		p.setState(ctx, exec, fencedStateParams(row, driver.JobSetStateParams{
			State: driver.JobStateCompleted,
		}))
		return
	}

	errStr := jobErr.Error()
	if _, discard := core.AsDiscard(jobErr); discard {
		p.setState(ctx, exec, fencedStateParams(row, driver.JobSetStateParams{
			State: driver.JobStateDiscarded, Err: &errStr, Attempt: row.AttemptNum,
		}))
		return
	}

	if row.AttemptNum >= maxRetry {
		p.setState(ctx, exec, fencedStateParams(row, driver.JobSetStateParams{
			State:   driver.JobStateDiscarded,
			Err:     &errStr,
			Attempt: row.AttemptNum,
		}))
		return
	}

	var retryAt time.Time
	if directive, ok := core.AsRetry(jobErr); ok {
		retryAt = directive.At
		if retryAt.IsZero() {
			retryAt = p.config.Clock.Now().Add(directive.After)
		}
	} else {
		retryAt = p.config.RetryPolicy.NextRetryAt(row.AttemptNum, jobErr, p.config.Clock)
	}
	if retryAt.IsZero() {
		p.setState(ctx, exec, fencedStateParams(row, driver.JobSetStateParams{
			State:   driver.JobStateDiscarded,
			Err:     &errStr,
			Attempt: row.AttemptNum,
		}))
		return
	}
	p.setState(ctx, exec, fencedStateParams(row, driver.JobSetStateParams{
		State:   driver.JobStateRetryable,
		Err:     &errStr,
		Attempt: row.AttemptNum,
		RetryAt: retryAt,
	}))
}

func fencedStateParams(row driver.JobRow, params driver.JobSetStateParams) driver.JobSetStateParams {
	params.ID = row.ID
	params.ExpectedWorkerID = row.WorkerID
	params.ExpectedAttempt = row.AttemptNum
	return params
}

func (p *WorkerPool[TTx]) startHeartbeat(ctx context.Context, exec driver.Executor, row driver.JobRow) (context.Context, func()) {
	heartbeater, ok := exec.(driver.JobHeartbeater)
	if !ok || p.config.HeartbeatInterval <= 0 {
		return ctx, func() {}
	}
	claimCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	ticker := p.config.Clock.NewTicker(p.config.HeartbeatInterval)
	go func() {
		defer close(done)
		defer ticker.Stop()
		for {
			select {
			case <-claimCtx.Done():
				return
			case <-p.shutdownCh:
				return
			case <-ticker.C():
				renewed, err := heartbeater.JobHeartbeat(claimCtx, driver.JobHeartbeatParams{
					ID: row.ID, WorkerID: row.WorkerID, Attempt: row.AttemptNum, At: p.config.Clock.Now(),
				})
				if err != nil {
					p.reportError(fmt.Errorf("heartbeat job %s: %w", row.ID, err))
					continue
				}
				if !renewed {
					cancel()
					return
				}
			}
		}
	}()
	return claimCtx, func() {
		cancel()
		<-done
	}
}

func callHandler(ctx context.Context, job *core.RawJob, handler func(context.Context, *core.RawJob) error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			switch v := r.(type) {
			case error:
				err = v
			default:
				err = &panicError{val: v}
			}
		}
	}()
	return handler(ctx, job)
}

func (p *WorkerPool[TTx]) setState(ctx context.Context, exec driver.Executor, params driver.JobSetStateParams) {
	stateCtx := ctx
	var cancel context.CancelFunc
	if ctx.Err() != nil {
		stateCtx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
	}
	if err := exec.JobSetStateIfRunning(stateCtx, params); err != nil {
		p.reportError(fmt.Errorf("set state for job %s: %w", params.ID, err))
	}
}

func (p *WorkerPool[TTx]) rescueStuck(ctx context.Context) {
	rescuer, ok := p.driver.Executor().(driver.StuckJobRescuer)
	if !ok {
		return
	}
	before := p.config.Clock.Now().Add(-p.config.StuckJobTimeout)
	for _, queue := range p.config.Queues {
		if _, err := rescuer.JobRescueStuck(ctx, driver.JobRescueParams{Queue: queue, Before: before}); err != nil {
			p.reportError(fmt.Errorf("rescue queue %q: %w", queue, err))
		}
	}
}

func (p *WorkerPool[TTx]) reportError(err error) {
	if err != nil {
		p.config.ErrorHandler(err)
	}
}

type panicError struct{ val any }

func (e *panicError) Error() string {
	return "panic: " + anyToString(e.val)
}

func anyToString(v any) string {
	if s, ok := v.(interface{ Error() string }); ok {
		return s.Error()
	}
	if s, ok := v.(string); ok {
		return s
	}
	return "unknown panic value"
}
