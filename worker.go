package goncordia

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
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

// RateLimitScope controls whether a queue rate limit is enforced by one worker
// pool or coordinated through the shared driver backend.
type RateLimitScope uint8

const (
	// RateLimitScopeLocal coordinates starts inside one WorkerPool.
	RateLimitScopeLocal RateLimitScope = iota
	// RateLimitScopeGlobal coordinates starts across every WorkerPool using the
	// same driver backend and queue/rule configuration.
	RateLimitScopeGlobal
)

func (s RateLimitScope) String() string {
	if s == RateLimitScopeGlobal {
		return "global"
	}
	return "local"
}

// RateLimitKey selects the job attribute that owns an independent rate or
// concurrency budget.
type RateLimitKey uint8

const (
	// RateLimitKeyQueue shares one budget across the whole queue.
	RateLimitKeyQueue RateLimitKey = iota
	// RateLimitKeyKind creates one budget per job kind.
	RateLimitKeyKind
	// RateLimitKeyPipeline creates one budget per PipelineID.
	RateLimitKeyPipeline
	// RateLimitKeyTag creates one budget per matching tag.
	RateLimitKeyTag
)

// RateLimitMode selects smooth or fixed-window start accounting.
type RateLimitMode = driver.RateLimitMode

const (
	RateLimitModeGCRA        = driver.RateLimitModeGCRA
	RateLimitModeFixedWindow = driver.RateLimitModeFixedWindow
)

// QueueRateLimit bounds handler starts over time. Multiple limits may be
// combined on one queue (for example, per-second and per-day limits).
type QueueRateLimit struct {
	// Limit is the sustained number of starts allowed per Period. Non-positive
	// values disable this rule.
	Limit int
	// Period is the unit over which Limit is expressed. Default: 1 second.
	Period time.Duration
	// Burst is the number of starts that may happen without spacing. Default: 1;
	// values greater than Limit are clamped to Limit.
	Burst int
	// Scope defaults to RateLimitScopeLocal.
	Scope RateLimitScope
	// Key gives each kind, pipeline, or matching tag an independent budget.
	// Default: RateLimitKeyQueue.
	Key RateLimitKey
	// TagPrefix restricts RateLimitKeyTag to tags with this prefix. The first
	// lexical match is used; jobs without one share an explicit empty bucket.
	TagPrefix string
	// Mode defaults to smooth GCRA. FixedWindow resets on UTC boundaries of
	// Period and ignores Burst.
	Mode RateLimitMode
}

// KeyConcurrencyLimit bounds concurrent jobs for each selected key inside one
// WorkerPool. For cross-process serialization use DistributedPipelines.
type KeyConcurrencyLimit struct {
	Key       RateLimitKey
	TagPrefix string
	Limit     int
}

// QueuePolicy controls how a queue shares capacity with other queues in a pool.
type QueuePolicy struct {
	// Weight is the number of weighted round-robin slots. Default: 1.
	Weight int
	// Concurrency bounds jobs from this queue claimed by this pool. Zero is unlimited.
	Concurrency int
	// KeyConcurrency adds independent local concurrency ceilings per kind,
	// pipeline, or matching tag.
	KeyConcurrency []KeyConcurrencyLimit
	// RateLimits bounds handler starts. Every configured rule must allow a start.
	RateLimits []QueueRateLimit
	// RateLimit bounds starts per RatePeriod using a local burst equal to the
	// limit. Zero is unlimited.
	// Deprecated: use RateLimits.
	RateLimit int
	// RatePeriod is the legacy rate-limit period. Default: 1 second.
	// Deprecated: use RateLimits.
	RatePeriod time.Duration
}

// WorkerConfig configures the worker pool.
type WorkerConfig struct {
	// Queues lists the queues this worker pool processes.
	// If empty, only "default" is polled.
	Queues []string
	// QueuePolicies configures weighted fairness and per-queue concurrency/rate limits.
	QueuePolicies map[string]QueuePolicy
	// DistributedPipelines serializes PipelineID values across worker processes
	// using the driver's renewable leader leases.
	DistributedPipelines bool
	// PipelineLeaseDuration is the distributed pipeline lease TTL. Default: 30 seconds.
	PipelineLeaseDuration time.Duration
	// PipelinePollInterval controls how often a waiting job retries the lease. Default: 250ms.
	PipelinePollInterval time.Duration
	// RateLimitPollInterval is the maximum interval between attempts to acquire
	// a contended global rate permit. Default: 1 second.
	RateLimitPollInterval time.Duration
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
	// Observer receives claim, heartbeat, and rescue lifecycle events.
	Observer WorkerObserver
	// ErrorHandler receives asynchronous fetch, rescue, and state-transition errors.
	// Default: slog.Error.
	ErrorHandler func(error)
}

// WorkerPool processes jobs from the queue using a pool of goroutines.
// TTx is the driver's transaction type (needed only for type parameter inference;
// the pool itself does not open user-visible transactions).
type WorkerPool[TTx any] struct {
	driver    driver.Driver[TTx]
	registry  *core.Registry
	config    WorkerConfig
	configErr error

	wg             sync.WaitGroup
	sem            chan struct{}
	pending        chan struct{}
	shutdownOnce   sync.Once
	shutdownCh     chan struct{}
	shutdownDone   chan struct{}
	isShuttingDown atomic.Bool
	started        atomic.Bool
	queueCursor    atomic.Uint64
	policyMu       sync.RWMutex
	policyChanged  chan struct{}
	queueSchedule  []string
	queueStateMu   sync.Mutex
	queueStates    map[string]*queueRuntimeState
	rateLimits     map[string][]normalizedRateLimit
	rateGates      map[string]chan struct{}
	fetchMu        sync.Mutex
	pipelineMu     sync.Mutex
	pipelineTails  map[string]chan struct{}
	kindSemMu      sync.Mutex
	kindSemaphores map[string]chan struct{}
	keySemMu       sync.Mutex
	keySemaphores  map[string]*keySemaphore
}

type keySemaphore struct {
	ch   chan struct{}
	refs int
}

type queueRuntimeState struct {
	active int
	rates  map[string]*localRateState
}

type localRateState struct {
	tat         time.Time
	windowStart time.Time
	count       int
}

type normalizedRateLimit struct {
	key       string
	limit     int
	burst     int
	period    time.Duration
	interval  time.Duration
	tolerance time.Duration
	scope     RateLimitScope
	keyBy     RateLimitKey
	tagPrefix string
	mode      RateLimitMode
}

// ValidateWorkerConfig checks values that cannot be safely normalized. Zero
// values documented as defaults remain valid.
func ValidateWorkerConfig[TTx any](d driver.Driver[TTx], cfg WorkerConfig) error {
	var problems []string
	if cfg.Concurrency < 0 {
		problems = append(problems, "Concurrency must not be negative")
	}
	if cfg.MaxPending < 0 {
		problems = append(problems, "MaxPending must not be negative")
	}
	if cfg.Concurrency > 0 && cfg.MaxPending > 0 && cfg.MaxPending < cfg.Concurrency {
		problems = append(problems, "MaxPending must be at least Concurrency")
	}
	queues := make(map[string]struct{}, len(cfg.Queues)+1)
	if len(cfg.Queues) == 0 {
		queues["default"] = struct{}{}
	} else {
		for _, queue := range cfg.Queues {
			if queue == "" {
				queue = "default"
			}
			queues[queue] = struct{}{}
		}
	}
	for queue, policy := range cfg.QueuePolicies {
		if _, configured := queues[queue]; !configured {
			problems = append(problems, fmt.Sprintf("QueuePolicies[%q] is not listed in Queues", queue))
		}
		if policy.Weight < 0 || policy.Concurrency < 0 || policy.RateLimit < 0 {
			problems = append(problems, fmt.Sprintf("QueuePolicies[%q] contains a negative limit", queue))
		}
		for i, rule := range policy.RateLimits {
			prefix := fmt.Sprintf("QueuePolicies[%q].RateLimits[%d]", queue, i)
			if rule.Limit < 0 {
				problems = append(problems, prefix+" Limit must not be negative")
			}
			if rule.Limit > 0 && rule.Burst > rule.Limit {
				problems = append(problems, prefix+" Burst must not exceed Limit")
			}
			if rule.Scope != RateLimitScopeLocal && rule.Scope != RateLimitScopeGlobal {
				problems = append(problems, prefix+" has an unknown Scope")
			}
			if rule.Key > RateLimitKeyTag {
				problems = append(problems, prefix+" has an unknown Key")
			}
			if rule.Mode != RateLimitModeGCRA && rule.Mode != RateLimitModeFixedWindow {
				problems = append(problems, prefix+" has an unknown Mode")
			}
			period := rule.Period
			if period <= 0 {
				period = time.Second
			}
			if rule.Mode == RateLimitModeFixedWindow && rule.Limit > 0 && int64(rule.Limit) >= int64(period/time.Millisecond) {
				problems = append(problems, prefix+" fixed-window Limit must be smaller than Period milliseconds")
			}
			if rule.Scope == RateLimitScopeGlobal && rule.Limit > 0 && !d.Capabilities().LinearizableCAS {
				problems = append(problems, fmt.Sprintf("%s requires linearizable CAS, unsupported by driver %q", prefix, d.Name()))
			}
		}
		for i, rule := range policy.KeyConcurrency {
			prefix := fmt.Sprintf("QueuePolicies[%q].KeyConcurrency[%d]", queue, i)
			if rule.Limit <= 0 {
				problems = append(problems, prefix+" Limit must be positive")
			}
			if rule.Key == RateLimitKeyQueue || rule.Key > RateLimitKeyTag {
				problems = append(problems, prefix+" Key must be kind, pipeline, or tag")
			}
		}
	}
	if cfg.DistributedPipelines && !d.Capabilities().LinearizableLeases {
		problems = append(problems, fmt.Sprintf("DistributedPipelines requires linearizable leases, unsupported by driver %q", d.Name()))
	}
	if len(problems) > 0 {
		return fmt.Errorf("invalid worker config: %s", strings.Join(problems, "; "))
	}
	return nil
}

// NewWorkerPool creates a WorkerPool. Invalid configuration is reported by
// Start. Use NewWorkerPoolChecked to fail before retaining the pool.
func NewWorkerPool[TTx any](d driver.Driver[TTx], registry *core.Registry, cfg WorkerConfig) *WorkerPool[TTx] {
	return newWorkerPool(d, registry, cfg)
}

// NewWorkerPoolChecked validates cfg and returns an error immediately.
func NewWorkerPoolChecked[TTx any](
	d driver.Driver[TTx], registry *core.Registry, cfg WorkerConfig,
) (*WorkerPool[TTx], error) {
	pool := newWorkerPool(d, registry, cfg)
	if pool.configErr != nil {
		return nil, pool.configErr
	}
	return pool, nil
}

func newWorkerPool[TTx any](d driver.Driver[TTx], registry *core.Registry, cfg WorkerConfig) *WorkerPool[TTx] {
	configErr := ValidateWorkerConfig(d, cfg)
	cfg.QueuePolicies = cloneQueuePolicies(cfg.QueuePolicies)
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
	if cfg.PipelineLeaseDuration <= 0 {
		cfg.PipelineLeaseDuration = 30 * time.Second
	}
	if cfg.PipelinePollInterval <= 0 {
		cfg.PipelinePollInterval = 250 * time.Millisecond
	}
	if cfg.RateLimitPollInterval <= 0 {
		cfg.RateLimitPollInterval = time.Second
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
	queueSchedule := make([]string, 0, len(cfg.Queues))
	rateLimits := make(map[string][]normalizedRateLimit, len(cfg.Queues))
	rateGates := make(map[string]chan struct{}, len(cfg.Queues))
	for _, queue := range cfg.Queues {
		policy := cfg.QueuePolicies[queue]
		weight := policy.Weight
		if weight <= 0 {
			weight = 1
		}
		rateLimits[queue] = normalizeRateLimits(queue, policy)
		if len(rateLimits[queue]) > 0 {
			rateGates[queue] = make(chan struct{}, 1)
		}
		for range weight {
			queueSchedule = append(queueSchedule, queue)
		}
	}

	return &WorkerPool[TTx]{
		driver:         d,
		registry:       registry,
		config:         cfg,
		configErr:      configErr,
		sem:            make(chan struct{}, cfg.Concurrency),
		pending:        make(chan struct{}, cfg.MaxPending),
		shutdownCh:     make(chan struct{}),
		shutdownDone:   make(chan struct{}),
		policyChanged:  make(chan struct{}),
		queueSchedule:  queueSchedule,
		rateLimits:     rateLimits,
		rateGates:      rateGates,
		queueStates:    make(map[string]*queueRuntimeState),
		pipelineTails:  make(map[string]chan struct{}),
		kindSemaphores: make(map[string]chan struct{}),
		keySemaphores:  make(map[string]*keySemaphore),
	}
}

func cloneQueuePolicies(source map[string]QueuePolicy) map[string]QueuePolicy {
	if source == nil {
		return nil
	}
	cloned := make(map[string]QueuePolicy, len(source))
	for queue, policy := range source {
		policy.RateLimits = append([]QueueRateLimit(nil), policy.RateLimits...)
		policy.KeyConcurrency = append([]KeyConcurrencyLimit(nil), policy.KeyConcurrency...)
		cloned[queue] = policy
	}
	return cloned
}

// UpdateQueuePolicy atomically replaces one queue policy for future claims and
// starts. Already acquired permits and running jobs keep their original policy.
func (p *WorkerPool[TTx]) UpdateQueuePolicy(queue string, policy QueuePolicy) error {
	if queue == "" {
		queue = "default"
	}
	p.fetchMu.Lock()
	defer p.fetchMu.Unlock()
	p.policyMu.Lock()
	defer p.policyMu.Unlock()
	found := false
	for _, configured := range p.config.Queues {
		if configured == queue {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("queue %q is not configured", queue)
	}
	next := cloneQueuePolicies(p.config.QueuePolicies)
	if next == nil {
		next = make(map[string]QueuePolicy)
	}
	next[queue] = policy
	candidate := p.config
	candidate.QueuePolicies = next
	if err := ValidateWorkerConfig(p.driver, candidate); err != nil {
		return err
	}
	p.config.QueuePolicies = next
	p.rateLimits[queue] = normalizeRateLimits(queue, policy)
	if len(p.rateLimits[queue]) > 0 {
		if p.rateGates[queue] == nil {
			p.rateGates[queue] = make(chan struct{}, 1)
		}
	} else {
		delete(p.rateGates, queue)
	}
	p.queueSchedule = p.queueSchedule[:0]
	for _, configured := range p.config.Queues {
		weight := p.config.QueuePolicies[configured].Weight
		if weight <= 0 {
			weight = 1
		}
		for range weight {
			p.queueSchedule = append(p.queueSchedule, configured)
		}
	}
	close(p.policyChanged)
	p.policyChanged = make(chan struct{})
	return nil
}

func (p *WorkerPool[TTx]) queuePolicy(queue string) QueuePolicy {
	p.policyMu.RLock()
	defer p.policyMu.RUnlock()
	return p.config.QueuePolicies[queue]
}

func (p *WorkerPool[TTx]) queueRateLimits(queue string) []normalizedRateLimit {
	p.policyMu.RLock()
	defer p.policyMu.RUnlock()
	return append([]normalizedRateLimit(nil), p.rateLimits[queue]...)
}

func (p *WorkerPool[TTx]) queueRateGate(queue string) chan struct{} {
	p.policyMu.RLock()
	defer p.policyMu.RUnlock()
	return p.rateGates[queue]
}

func (p *WorkerPool[TTx]) currentPolicyChange() <-chan struct{} {
	p.policyMu.RLock()
	defer p.policyMu.RUnlock()
	return p.policyChanged
}

func normalizeRateLimits(queue string, policy QueuePolicy) []normalizedRateLimit {
	configured := append([]QueueRateLimit(nil), policy.RateLimits...)
	if policy.RateLimit > 0 {
		configured = append(configured, QueueRateLimit{
			Limit: policy.RateLimit, Period: policy.RatePeriod, Burst: policy.RateLimit,
		})
	}
	result := make([]normalizedRateLimit, 0, len(configured))
	seen := make(map[string]struct{}, len(configured))
	for _, configuredRule := range configured {
		if configuredRule.Limit <= 0 {
			continue
		}
		if configuredRule.Period <= 0 {
			configuredRule.Period = time.Second
		}
		if configuredRule.Burst <= 0 {
			configuredRule.Burst = 1
		}
		if configuredRule.Burst > configuredRule.Limit {
			configuredRule.Burst = configuredRule.Limit
		}
		if configuredRule.Scope != RateLimitScopeGlobal {
			configuredRule.Scope = RateLimitScopeLocal
		}
		interval := time.Duration(float64(configuredRule.Period) / float64(configuredRule.Limit))
		if interval < time.Nanosecond {
			interval = time.Nanosecond
		}
		tolerance := interval * time.Duration(configuredRule.Burst-1)
		signature := strconv.Itoa(configuredRule.Limit) + ":" +
			strconv.FormatInt(int64(configuredRule.Period), 10) + ":" +
			strconv.Itoa(configuredRule.Burst) + ":" + strconv.Itoa(int(configuredRule.Scope)) + ":" +
			strconv.Itoa(int(configuredRule.Key)) + ":" + configuredRule.TagPrefix + ":" + strconv.Itoa(int(configuredRule.Mode))
		if _, duplicate := seen[signature]; duplicate {
			continue
		}
		seen[signature] = struct{}{}
		digest := sha256.Sum256([]byte(queue + "\x00" + signature))
		result = append(result, normalizedRateLimit{
			key:       "goncordia:rate:" + hex.EncodeToString(digest[:16]),
			limit:     configuredRule.Limit,
			burst:     configuredRule.Burst,
			period:    configuredRule.Period,
			interval:  interval,
			tolerance: tolerance,
			scope:     configuredRule.Scope,
			keyBy:     configuredRule.Key,
			tagPrefix: configuredRule.TagPrefix,
			mode:      configuredRule.Mode,
		})
	}
	return result
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
	if p.configErr != nil {
		return p.configErr
	}
	if !p.started.CompareAndSwap(false, true) {
		return fmt.Errorf("worker pool already started")
	}
	if p.isShuttingDown.Load() {
		return fmt.Errorf("worker pool is shutting down")
	}
	if listener := p.driver.Listener(); listener != nil {
		return p.runWithNotifications(ctx, listener)
	}
	return p.runWithPolling(ctx)
}

// Stop initiates a graceful shutdown, waiting up to ShutdownTimeout for in-flight jobs.
// Deprecated: use Shutdown to observe whether the drain completed.
func (p *WorkerPool[TTx]) Stop() {
	ctx, cancel := clock.WithTimeout(context.Background(), p.config.Clock, p.config.ShutdownTimeout)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		p.reportError(fmt.Errorf("shutdown worker pool: %w", err))
	}
}

// Shutdown stops claiming new jobs and waits for claimed work to complete or
// yield. Active jobs continue renewing their leases while the pool drains.
func (p *WorkerPool[TTx]) Shutdown(ctx context.Context) error {
	p.shutdownOnce.Do(func() {
		// Serialize shutdown with claiming so no goroutine can be added after
		// Wait begins.
		p.fetchMu.Lock()
		p.isShuttingDown.Store(true)
		close(p.shutdownCh)
		p.fetchMu.Unlock()
		go func() {
			p.wg.Wait()
			close(p.shutdownDone)
		}()
	})

	select {
	case <-p.shutdownDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
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

	misses := 0
	for free > 0 && misses < len(p.queueSchedule) {
		index := int(p.queueCursor.Add(1)-1) % len(p.queueSchedule)
		queue := p.queueSchedule[index]
		if !p.queueCanClaim(queue) {
			misses++
			continue
		}
		rows, err := exec.JobFetchBatch(ctx, driver.FetchParams{
			Queue:         queue,
			Limit:         1,
			WorkerID:      p.config.WorkerID,
			LeaseDuration: p.config.StuckJobTimeout,
		})
		if err != nil {
			p.reportError(fmt.Errorf("fetch queue %q: %w", queue, err))
			misses++
			continue
		}
		if len(rows) == 0 {
			misses++
			continue
		}
		misses = 0
		p.queueClaimed(queue)
		free--
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
			if p.config.Observer != nil {
				p.config.Observer.JobClaimed(ctx, row)
			}
			pipelineReady, pipelineDone := p.pipelineGate(row.PipelineID)
			p.pending <- struct{}{}
			p.wg.Add(1)
			go func() {
				defer p.wg.Done()
				defer func() { <-p.pending }()
				defer p.queueDone(row.Queue)
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
				distributedCtx, releaseDistributed, acquired := p.acquireDistributedPipeline(claimCtx, exec, row)
				if !acquired {
					p.setState(context.Background(), exec, fencedStateParams(row, driver.JobSetStateParams{Yield: true}))
					return
				}
				defer releaseDistributed()
				claimCtx = distributedCtx
				releaseKeys, keysAcquired := p.acquireKeyConcurrency(claimCtx, row)
				if !keysAcquired {
					p.setState(context.Background(), exec, fencedStateParams(row, driver.JobSetStateParams{Yield: true}))
					return
				}
				defer releaseKeys()
				releaseRateGate, rateGateAcquired := p.acquireRateGate(claimCtx, row.Queue)
				if !rateGateAcquired {
					p.setState(context.Background(), exec, fencedStateParams(row, driver.JobSetStateParams{Yield: true}))
					return
				}
				defer releaseRateGate()
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
				if !p.waitForRatePermit(claimCtx, exec, row) {
					p.setState(context.Background(), exec, fencedStateParams(row, driver.JobSetStateParams{Yield: true}))
					return
				}
				// Let the next job approach execution capacity only after this job has
				// reserved every configured rate permit.
				releaseRateGate()
				startedAt := p.config.Clock.Now()
				if observer, ok := p.config.Observer.(WorkerLifecycleObserver); ok {
					claimWait := time.Duration(0)
					if row.AttemptedAt != nil {
						claimWait = startedAt.Sub(*row.AttemptedAt)
					}
					observer.JobStarted(claimCtx, JobStartedEvent{Job: row, StartedAt: startedAt, ClaimWait: claimWait})
				}
				state, jobErr := p.processRow(claimCtx, exec, row)
				if observer, ok := p.config.Observer.(WorkerLifecycleObserver); ok {
					observer.JobFinished(claimCtx, JobFinishedEvent{
						Job: row, State: state, Err: jobErr, StartedAt: startedAt, FinishedAt: p.config.Clock.Now(),
					})
				}
			}()
		}
	}
}

func (p *WorkerPool[TTx]) queueCanClaim(queue string) bool {
	p.queueStateMu.Lock()
	defer p.queueStateMu.Unlock()
	policy := p.queuePolicy(queue)
	state := p.queueStates[queue]
	if state == nil {
		state = &queueRuntimeState{}
		p.queueStates[queue] = state
	}
	if policy.Concurrency > 0 && state.active >= policy.Concurrency {
		return false
	}
	return true
}

func (p *WorkerPool[TTx]) queueClaimed(queue string) {
	p.queueStateMu.Lock()
	defer p.queueStateMu.Unlock()
	state := p.queueStates[queue]
	if state == nil {
		state = &queueRuntimeState{}
		p.queueStates[queue] = state
	}
	state.active++
}

func (p *WorkerPool[TTx]) queueDone(queue string) {
	p.queueStateMu.Lock()
	defer p.queueStateMu.Unlock()
	if state := p.queueStates[queue]; state != nil && state.active > 0 {
		state.active--
	}
}

func (p *WorkerPool[TTx]) acquireRateGate(ctx context.Context, queue string) (func(), bool) {
	gate := p.queueRateGate(queue)
	if gate == nil {
		return func() {}, true
	}
	select {
	case gate <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-gate }) }, true
	case <-ctx.Done():
		return nil, false
	case <-p.shutdownCh:
		return nil, false
	}
}

type localRateReservation struct {
	state          *localRateState
	previous       time.Time
	reserved       time.Time
	previousWindow time.Time
	previousCount  int
	fixedWindow    bool
}

func (p *WorkerPool[TTx]) localRateDelay(row driver.JobRow, now time.Time) time.Duration {
	p.queueStateMu.Lock()
	defer p.queueStateMu.Unlock()
	state := p.queueStates[row.Queue]
	if state == nil {
		state = &queueRuntimeState{}
		p.queueStates[row.Queue] = state
	}
	if state.rates == nil {
		state.rates = make(map[string]*localRateState)
	}
	var delay time.Duration
	for _, rule := range p.queueRateLimits(row.Queue) {
		if rule.scope != RateLimitScopeLocal {
			continue
		}
		key := rateRuleKey(rule, row)
		rateState := state.rates[key]
		if rateState == nil {
			rateState = &localRateState{}
			state.rates[key] = rateState
		}
		if rule.mode == RateLimitModeFixedWindow {
			window := now.UTC().Truncate(rule.period)
			if rateState.windowStart.Equal(window) && rateState.count >= rule.limit {
				if candidate := window.Add(rule.period).Sub(now); candidate > delay {
					delay = candidate
				}
			}
			continue
		}
		retryAt := rateState.tat.Add(-rule.tolerance)
		if retryAt.After(now) && retryAt.Sub(now) > delay {
			delay = retryAt.Sub(now)
		}
	}
	return delay
}

func (p *WorkerPool[TTx]) reserveLocalRateLimits(row driver.JobRow, now time.Time) (func(), bool) {
	p.queueStateMu.Lock()
	state := p.queueStates[row.Queue]
	if state == nil {
		state = &queueRuntimeState{}
		p.queueStates[row.Queue] = state
	}
	if state.rates == nil {
		state.rates = make(map[string]*localRateState)
	}
	rules := p.queueRateLimits(row.Queue)
	reservations := make([]localRateReservation, 0, len(rules))
	rollbackLocked := func() {
		for i := len(reservations) - 1; i >= 0; i-- {
			reservation := reservations[i]
			if reservation.fixedWindow {
				reservation.state.windowStart = reservation.previousWindow
				reservation.state.count = reservation.previousCount
			} else if reservation.state.tat.Equal(reservation.reserved) {
				reservation.state.tat = reservation.previous
			}
		}
	}
	for _, rule := range rules {
		if rule.scope != RateLimitScopeLocal {
			continue
		}
		key := rateRuleKey(rule, row)
		rateState := state.rates[key]
		if rateState == nil {
			rateState = &localRateState{}
			state.rates[key] = rateState
		}
		if rule.mode == RateLimitModeFixedWindow {
			window := now.UTC().Truncate(rule.period)
			if !rateState.windowStart.Equal(window) {
				reservations = append(reservations, localRateReservation{
					state: rateState, previousWindow: rateState.windowStart,
					previousCount: rateState.count, fixedWindow: true,
				})
				rateState.windowStart = window
				rateState.count = 1
				continue
			}
			if rateState.count >= rule.limit {
				rollbackLocked()
				p.queueStateMu.Unlock()
				return nil, false
			}
			reservations = append(reservations, localRateReservation{
				state: rateState, previousWindow: rateState.windowStart,
				previousCount: rateState.count, fixedWindow: true,
			})
			rateState.count++
			continue
		}
		if rateState.tat.Add(-rule.tolerance).After(now) {
			rollbackLocked()
			p.queueStateMu.Unlock()
			return nil, false
		}
		base := rateState.tat
		if base.Before(now) {
			base = now
		}
		reserved := base.Add(rule.interval)
		reservations = append(reservations, localRateReservation{
			state: rateState, previous: rateState.tat, reserved: reserved,
		})
		rateState.tat = reserved
	}
	p.queueStateMu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			p.queueStateMu.Lock()
			defer p.queueStateMu.Unlock()
			rollbackLocked()
		})
	}, true
}

func (p *WorkerPool[TTx]) tryGlobalRateLimits(
	ctx context.Context, exec driver.Executor, row driver.JobRow,
) (bool, time.Time, error) {
	for _, rule := range p.queueRateLimits(row.Queue) {
		if rule.scope != RateLimitScopeGlobal {
			continue
		}
		result, err := driver.AcquireRateLimit(ctx, exec, driver.RateLimitAcquireParams{
			Key: rateRuleKey(rule, row), Now: p.config.Clock.Now(), Limit: rule.limit,
			Period: rule.period, Burst: rule.burst, Mode: rule.mode,
		})
		if err != nil {
			return false, time.Time{}, err
		}
		if !result.Acquired {
			// A permit acquired for an earlier AND-combined rule is deliberately
			// not rolled back: doing so after a CAS could erase a concurrent start.
			// This may underutilize capacity under contention but never exceeds it.
			return false, result.RetryAt, nil
		}
	}
	return true, time.Time{}, nil
}

func (p *WorkerPool[TTx]) waitForRatePermit(ctx context.Context, exec driver.Executor, row driver.JobRow) bool {
	if len(p.queueRateLimits(row.Queue)) == 0 {
		return true
	}
	for {
		now := p.config.Clock.Now()
		if delay := p.localRateDelay(row, now); delay > 0 {
			if observer, ok := p.config.Observer.(WorkerLifecycleObserver); ok {
				observer.JobRateLimited(ctx, RateLimitWaitEvent{
					Job: row, Scope: RateLimitScopeLocal, RetryAt: now.Add(delay),
				})
			}
			if !p.waitForRateDelay(ctx, delay) {
				return false
			}
			continue
		}
		rollbackLocal, reserved := p.reserveLocalRateLimits(row, now)
		if !reserved {
			continue
		}
		acquired, retryAt, err := p.tryGlobalRateLimits(ctx, exec, row)
		if acquired && err == nil {
			// Successful local reservations and global cursors intentionally remain:
			// they account for this handler start.
			return true
		}
		rollbackLocal()
		delay := p.config.RateLimitPollInterval
		if err != nil {
			p.reportError(fmt.Errorf("acquire global rate permit for queue %q: %w", row.Queue, err))
		} else if retryAt.After(p.config.Clock.Now()) {
			delay = retryAt.Sub(p.config.Clock.Now())
		}
		if observer, ok := p.config.Observer.(WorkerLifecycleObserver); ok {
			observer.JobRateLimited(ctx, RateLimitWaitEvent{
				Job: row, Scope: RateLimitScopeGlobal, RetryAt: p.config.Clock.Now().Add(delay), Err: err,
			})
		}
		if !p.waitForRateDelay(ctx, delay) {
			return false
		}
	}
}

func (p *WorkerPool[TTx]) waitForRateDelay(ctx context.Context, delay time.Duration) bool {
	var timerC <-chan time.Time
	stop := func() {}
	if factory, ok := p.config.Clock.(interface {
		NewTimer(time.Duration) clock.Timer
	}); ok {
		timer := factory.NewTimer(delay)
		timerC = timer.C()
		stop = timer.Stop
	} else {
		timerC = p.config.Clock.After(delay)
	}
	defer stop()
	select {
	case <-timerC:
		return true
	case <-p.currentPolicyChange():
		return true
	case <-ctx.Done():
		return false
	case <-p.shutdownCh:
		return false
	}
}

func (p *WorkerPool[TTx]) acquireDistributedPipeline(
	ctx context.Context, exec driver.Executor, row driver.JobRow,
) (context.Context, func(), bool) {
	if !p.config.DistributedPipelines || row.PipelineID == "" {
		return ctx, func() {}, true
	}
	name := "goncordia:pipeline:" + row.PipelineID
	owner := fmt.Sprintf("%s:%s:%d", p.config.WorkerID, row.ID, row.AttemptNum)
	ticker := p.config.Clock.NewTicker(p.config.PipelinePollInterval)
	defer ticker.Stop()
	for {
		elected, err := exec.LeaderAttemptElect(ctx, driver.LeaderElectParams{
			Name: name, WorkerID: owner, TTL: p.config.PipelineLeaseDuration,
		})
		if err != nil {
			p.reportError(fmt.Errorf("acquire distributed pipeline %q: %w", row.PipelineID, err))
		} else if elected {
			pipelineCtx, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			renewEvery := p.config.PipelineLeaseDuration / 3
			if renewEvery <= 0 {
				renewEvery = time.Second
			}
			renewTicker := p.config.Clock.NewTicker(renewEvery)
			go func() {
				defer close(done)
				defer renewTicker.Stop()
				for {
					select {
					case <-pipelineCtx.Done():
						return
					case <-renewTicker.C():
						renewed, renewErr := exec.LeaderAttemptElect(pipelineCtx, driver.LeaderElectParams{
							Name: name, WorkerID: owner, TTL: p.config.PipelineLeaseDuration,
						})
						if renewErr != nil || !renewed {
							if renewErr != nil {
								p.reportError(fmt.Errorf("renew distributed pipeline %q: %w", row.PipelineID, renewErr))
							}
							cancel()
							return
						}
					}
				}
			}()
			release := func() {
				cancel()
				<-done
				if err := exec.LeaderResign(context.Background(), driver.LeaderResignParams{Name: name, WorkerID: owner}); err != nil {
					p.reportError(fmt.Errorf("release distributed pipeline %q: %w", row.PipelineID, err))
				}
			}
			return pipelineCtx, release, true
		}
		select {
		case <-ctx.Done():
			return ctx, func() {}, false
		case <-p.shutdownCh:
			return ctx, func() {}, false
		case <-ticker.C():
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

func rateLimitKeyValue(keyBy RateLimitKey, tagPrefix string, row driver.JobRow) string {
	switch keyBy {
	case RateLimitKeyKind:
		return row.Kind
	case RateLimitKeyPipeline:
		return row.PipelineID
	case RateLimitKeyTag:
		matches := make([]string, 0, len(row.Tags))
		for _, tag := range row.Tags {
			if strings.HasPrefix(tag, tagPrefix) {
				matches = append(matches, tag)
			}
		}
		sort.Strings(matches)
		if len(matches) > 0 {
			return matches[0]
		}
		return "<missing>"
	default:
		return row.Queue
	}
}

func rateRuleKey(rule normalizedRateLimit, row driver.JobRow) string {
	if rule.keyBy == RateLimitKeyQueue {
		return rule.key
	}
	digest := sha256.Sum256([]byte(rule.key + "\x00" + rateLimitKeyValue(rule.keyBy, rule.tagPrefix, row)))
	return rule.key + ":" + hex.EncodeToString(digest[:16])
}

func (p *WorkerPool[TTx]) acquireKeyConcurrency(ctx context.Context, row driver.JobRow) (func(), bool) {
	policy := p.queuePolicy(row.Queue)
	acquired := make([]struct {
		key   string
		entry *keySemaphore
	}, 0, len(policy.KeyConcurrency))
	release := func() {
		for i := len(acquired) - 1; i >= 0; i-- {
			item := acquired[i]
			<-item.entry.ch
			p.keySemMu.Lock()
			item.entry.refs--
			if item.entry.refs == 0 {
				delete(p.keySemaphores, item.key)
			}
			p.keySemMu.Unlock()
		}
	}
	for index, rule := range policy.KeyConcurrency {
		value := rateLimitKeyValue(rule.Key, rule.TagPrefix, row)
		key := row.Queue + "\x00" + strconv.Itoa(index) + "\x00" + value
		p.keySemMu.Lock()
		entry := p.keySemaphores[key]
		if entry == nil {
			entry = &keySemaphore{ch: make(chan struct{}, rule.Limit)}
			p.keySemaphores[key] = entry
		}
		entry.refs++
		p.keySemMu.Unlock()
		select {
		case entry.ch <- struct{}{}:
			acquired = append(acquired, struct {
				key   string
				entry *keySemaphore
			}{key: key, entry: entry})
		case <-ctx.Done():
			p.keySemMu.Lock()
			entry.refs--
			if entry.refs == 0 {
				delete(p.keySemaphores, key)
			}
			p.keySemMu.Unlock()
			release()
			return nil, false
		case <-p.shutdownCh:
			p.keySemMu.Lock()
			entry.refs--
			if entry.refs == 0 {
				delete(p.keySemaphores, key)
			}
			p.keySemMu.Unlock()
			release()
			return nil, false
		}
	}
	var once sync.Once
	return func() { once.Do(release) }, true
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
func (p *WorkerPool[TTx]) processRow(
	ctx context.Context, exec driver.Executor, row driver.JobRow,
) (driver.JobState, error) {
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
		RunAt:      row.RunAt,
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
				err = newPanicError(r)
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
		return driver.JobStateAvailable, ctx.Err()
	}

	if jobErr == nil {
		p.setState(ctx, exec, fencedStateParams(row, driver.JobSetStateParams{
			State: driver.JobStateCompleted,
		}))
		return driver.JobStateCompleted, nil
	}

	errStr := jobErr.Error()
	trace := panicTrace(jobErr)
	if _, discard := core.AsDiscard(jobErr); discard {
		p.setState(ctx, exec, fencedStateParams(row, driver.JobSetStateParams{
			State: driver.JobStateDiscarded, Err: &errStr, Trace: trace, Attempt: row.AttemptNum,
		}))
		return driver.JobStateDiscarded, jobErr
	}

	if row.AttemptNum >= maxRetry {
		p.setState(ctx, exec, fencedStateParams(row, driver.JobSetStateParams{
			State:   driver.JobStateDiscarded,
			Err:     &errStr,
			Trace:   trace,
			Attempt: row.AttemptNum,
		}))
		return driver.JobStateDiscarded, jobErr
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
			Trace:   trace,
			Attempt: row.AttemptNum,
		}))
		return driver.JobStateDiscarded, jobErr
	}
	p.setState(ctx, exec, fencedStateParams(row, driver.JobSetStateParams{
		State:   driver.JobStateRetryable,
		Err:     &errStr,
		Trace:   trace,
		Attempt: row.AttemptNum,
		RetryAt: retryAt,
	}))
	return driver.JobStateRetryable, jobErr
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
			case <-ticker.C():
				now := p.config.Clock.Now()
				params := driver.JobHeartbeatParams{
					ID: row.ID, WorkerID: row.WorkerID, Attempt: row.AttemptNum, At: now,
					LeaseExpiresAt: now.Add(p.config.StuckJobTimeout),
				}
				renewed, err := heartbeater.JobHeartbeat(claimCtx, params)
				if p.config.Observer != nil {
					p.config.Observer.JobHeartbeat(claimCtx, HeartbeatEvent{Job: row, Renewed: renewed, Err: err})
				}
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
			err = newPanicError(r)
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
	if err := exec.JobSetStateIfRunning(stateCtx, params); err != nil && !errors.Is(err, driver.ErrStaleClaim) {
		p.reportError(fmt.Errorf("set state for job %s: %w", params.ID, err))
	}
}

func (p *WorkerPool[TTx]) rescueStuck(ctx context.Context) {
	rescuer, ok := p.driver.Executor().(driver.StuckJobRescuer)
	if !ok {
		return
	}
	now := p.config.Clock.Now()
	before := now.Add(-p.config.StuckJobTimeout)
	for _, queue := range p.config.Queues {
		rescued, err := rescuer.JobRescueStuck(ctx, driver.JobRescueParams{Queue: queue, At: now, Before: before})
		if p.config.Observer != nil {
			p.config.Observer.JobsRescued(ctx, RescueEvent{Queue: queue, Before: before, Rescued: rescued, Err: err})
		}
		if err != nil {
			p.reportError(fmt.Errorf("rescue queue %q: %w", queue, err))
		}
	}
}

func (p *WorkerPool[TTx]) reportError(err error) {
	if err != nil {
		p.config.ErrorHandler(err)
	}
}

type panicError struct {
	val   any
	stack string
}

func newPanicError(value any) *panicError {
	return &panicError{val: value, stack: string(debug.Stack())}
}

func (e *panicError) Error() string {
	return "panic: " + anyToString(e.val)
}

func panicTrace(err error) *string {
	var panicErr *panicError
	if !errors.As(err, &panicErr) || panicErr.stack == "" {
		return nil
	}
	trace := panicErr.stack
	return &trace
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
