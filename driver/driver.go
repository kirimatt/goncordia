// Package driver defines the interfaces that all storage backend drivers must implement.
// Built-in backends include PostgreSQL, MySQL, SQLite, MongoDB, Redis,
// Cassandra, ClickHouse, DynamoDB, Firestore, and in-memory. Each implements
// Driver[TTx], parameterized by its native transaction type.
package driver

import (
	"context"
	"strings"
	"time"
)

const permanentUniqueKeyPrefix = "uf2_"

// IsPermanentUniqueKey reports whether key remains reserved after a terminal
// transition. Explicit deletion still releases the key.
func IsPermanentUniqueKey(key string) bool {
	return strings.HasPrefix(key, permanentUniqueKeyPrefix)
}

// Driver is the top-level interface for a job queue storage backend.
// TTx is the transaction type native to the backend library the user chose
// (e.g. *pgx.Tx, *sql.Tx, mongo.Session).
type Driver[TTx any] interface {
	// Name returns a human-readable identifier ("postgres", "mongodb", "redis", "memory").
	Name() string

	// Capabilities reports which optional features this driver supports.
	Capabilities() Capabilities

	// Executor returns a non-transactional query executor.
	Executor() Executor

	// UnwrapTx converts a user-supplied transaction into our ExecutorTx.
	// Used internally by Client.EnqueueTx so users pass their own tx type.
	UnwrapTx(tx TTx) ExecutorTx

	// Listener returns a push-notification listener, or nil if polling must be used.
	Listener() Listener

	// Close releases resources created by the driver itself. Constructors that
	// accept a client, pool, connection, or session never take ownership of it;
	// the caller remains responsible for closing that resource.
	Close() error
}

// Capabilities describes which optional features a driver supports.
// The engine checks these flags at startup and switches between
// push notifications vs polling, native tx vs at-least-once, etc.
type Capabilities struct {
	// NativeTx means EnqueueTx is truly atomic (SQL/MongoDB replica set).
	NativeTx bool
	// ListenNotify means the backend supports push notifications (Postgres LISTEN/NOTIFY).
	ListenNotify bool
	// ChangeStreams means the backend supports MongoDB change streams.
	ChangeStreams bool
	// SkipLocked means the backend supports SELECT FOR UPDATE SKIP LOCKED.
	SkipLocked bool
	// UniqueJobs means the backend can enforce unique job constraints natively.
	UniqueJobs bool
	// AdvisoryLocks means the backend supports advisory locks for leader election.
	AdvisoryLocks bool
	// LinearizableLeases means concurrent lease acquisition has exactly one
	// winner. Distributed pipelines require this guarantee.
	LinearizableLeases bool
	// LinearizableCAS means schedule-cursor compare-and-swap has exactly one
	// winner. Strict global rate limits require this guarantee.
	LinearizableCAS bool
	// LifecycleTimestamps means the executor persists StartedAt with fencing.
	LifecycleTimestamps bool
	// BoundedFetch means one execution fetch examines a backend-bounded
	// candidate set rather than reading the entire due backlog.
	BoundedFetch bool
	// StrictFetchOrdering means the backend can apply the full portable order
	// before limiting candidates.
	StrictFetchOrdering bool
}

// Executor executes job queue operations outside of a transaction.
type Executor interface {
	baseExecutor
	// Begin starts a new transaction and returns an ExecutorTx.
	Begin(ctx context.Context) (ExecutorTx, error)
}

// ExecutorTx executes job queue operations within an existing transaction.
type ExecutorTx interface {
	baseExecutor
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
}

// StuckJobRescuer is an optional executor capability implemented by drivers
// that can atomically return abandoned running jobs to the available state.
// WorkerPool detects this interface automatically.
type StuckJobRescuer interface {
	JobRescueStuck(ctx context.Context, params JobRescueParams) (int64, error)
}

// JobHeartbeater is an optional executor capability implemented by drivers
// that can renew a running claim. The worker pool uses it to prevent healthy
// long-running jobs from being rescued as abandoned.
type JobHeartbeater interface {
	JobHeartbeat(ctx context.Context, params JobHeartbeatParams) (renewed bool, err error)
}

// JobStartMarker is an optional executor capability that persists the instant
// a claimed job begins handler execution.
type JobStartMarker interface {
	JobMarkStarted(context.Context, JobMarkStartedParams) (marked bool, err error)
}

// AdminExecutor is an optional executor capability used by the admin HTTP API
// for filtered job inspection and queue-depth metrics.
type AdminExecutor interface {
	JobList(ctx context.Context, params JobListParams) ([]JobRow, error)
	QueueStats(ctx context.Context, queue string) (QueueStats, error)
}

// baseExecutor groups the core data-access methods shared by both
// transactional and non-transactional executors.
type baseExecutor interface {
	// --- Jobs ---

	JobInsertMany(ctx context.Context, params []JobInsertParams) ([]JobInsertResult, error)
	JobGetByID(ctx context.Context, id string) (*JobRow, error)
	// JobFetchBatch atomically claims up to limit due jobs for processing in
	// priority DESC, run_at ASC, created_at ASC, id ASC order.
	// Implementations use SELECT FOR UPDATE SKIP LOCKED (SQL) or findOneAndUpdate (MongoDB).
	JobFetchBatch(ctx context.Context, params FetchParams) ([]JobRow, error)
	// JobSetStateIfRunning atomically transitions a running job to a terminal/retry state.
	JobSetStateIfRunning(ctx context.Context, params JobSetStateParams) error
	JobCancel(ctx context.Context, id string) error
	JobDelete(ctx context.Context, id string) error
	JobReschedule(ctx context.Context, params RescheduleParams) error

	// --- Queues ---

	QueueGet(ctx context.Context, name string) (*QueueRow, error)
	QueuePause(ctx context.Context, name string) error
	QueueResume(ctx context.Context, name string) error
	QueueList(ctx context.Context, params QueueListParams) ([]*QueueRow, error)

	// --- Leader election (only called when Capabilities.AdvisoryLocks == false, others use DB-specific mechanisms) ---

	LeaderAttemptElect(ctx context.Context, params LeaderElectParams) (elected bool, err error)
	LeaderResign(ctx context.Context, params LeaderResignParams) error

	// --- Durable periodic schedule cursors ---

	ScheduleCursorGetOrCreate(ctx context.Context, params ScheduleCursorCreateParams) (ScheduleCursorResult, error)
	ScheduleCursorAdvance(ctx context.Context, params ScheduleCursorAdvanceParams) (advanced bool, err error)
}

// --- Parameter and result types ---

// JobInsertParams carries the data needed to enqueue a new job.
type JobInsertParams struct {
	Queue      string
	Kind       string    // job type name, used to dispatch to the right worker
	Args       []byte    // JSON-encoded job arguments
	Priority   int       // higher = processed first; default 0
	RunAt      time.Time // zero means "immediately"
	UniqueKey  string    // optional; globally prevents duplicate active jobs with the same canonical key
	MaxRetry   int
	Timeout    time.Duration
	Tags       []string
	PipelineID string // optional; jobs with the same non-empty PipelineID run sequentially
}

// JobInsertResult is returned after a successful insert.
type JobInsertResult struct {
	Job        *JobRow
	UniqueSkip bool // true if a duplicate was found and this insert was skipped
}

// FetchParams controls how many and which jobs a worker claims.
type FetchParams struct {
	Queue         string
	Limit         int
	WorkerID      string
	LeaseDuration time.Duration
}

// JobRescueParams selects claims whose explicit lease expired by At. Before is
// retained as a fallback cutoff for legacy rows without lease metadata.
type JobRescueParams struct {
	Queue  string
	At     time.Time
	Before time.Time
}

// JobHeartbeatParams renews one fenced running claim at At.
type JobHeartbeatParams struct {
	ID             string
	WorkerID       string
	Attempt        int
	At             time.Time
	LeaseExpiresAt time.Time
}

// JobMarkStartedParams fences an execution-start timestamp to one claim.
type JobMarkStartedParams struct {
	ID       string
	WorkerID string
	Attempt  int
	At       time.Time
}

// JobSetStateParams transitions a running job to a new state.
type JobSetStateParams struct {
	ID               string
	State            JobState
	Err              *string   // serialized error for failed/retryable states
	Trace            *string   // optional panic stack trace retained with Err
	Attempt          int       // attempt number associated with Err
	RetryAt          time.Time // populated when State == JobStateRetryable
	ExpectedWorkerID string    // optional fencing precondition
	ExpectedAttempt  int       // optional fencing precondition
	// Yield returns the job to available without recording an error or counting
	// an attempt. Used by the pipeline serialization mechanism when a job cannot
	// run yet because another job with the same PipelineID is already running.
	// When Yield is true all other fields except ID are ignored.
	Yield bool
}

// MatchesClaim reports whether the current claim satisfies the optional
// fencing preconditions on a state transition.
func (p JobSetStateParams) MatchesClaim(workerID string, attempt int) bool {
	return (p.ExpectedWorkerID == "" || p.ExpectedWorkerID == workerID) &&
		(p.ExpectedAttempt <= 0 || p.ExpectedAttempt == attempt)
}

// RescheduleParams reschedules a job to run at a future time.
type RescheduleParams struct {
	ID    string
	RunAt time.Time
}

// QueueListParams controls pagination for QueueList.
type QueueListParams struct {
	Limit  int
	Cursor string
}

// JobListParams controls filtered administrative job listing.
type JobListParams struct {
	Queue  string
	State  JobState
	Kind   string
	Limit  int
	Cursor string
}

// QueueStats contains job counts by state for one queue.
type QueueStats struct {
	Queue  string             `json:"queue"`
	States map[JobState]int64 `json:"states"`
	Total  int64              `json:"total"`
}

// CountQueueStats builds queue statistics from canonical rows.
func CountQueueStats(queue string, rows []JobRow) QueueStats {
	stats := QueueStats{Queue: queue, States: make(map[JobState]int64)}
	for _, row := range rows {
		stats.States[row.State]++
		stats.Total++
	}
	return stats
}

// LeaderElectParams carries the parameters for a leader election attempt.
type LeaderElectParams struct {
	Name     string
	WorkerID string
	TTL      time.Duration
}

// LeaderResignParams releases a lease only when WorkerID still owns it.
type LeaderResignParams struct {
	Name     string
	WorkerID string
}

// ScheduleCursorCreateParams initializes a durable schedule cursor if missing.
type ScheduleCursorCreateParams struct {
	ID        string
	InitialAt time.Time
}

// ScheduleCursorResult returns the persisted cursor and whether it was created.
type ScheduleCursorResult struct {
	At      time.Time
	Created bool
}

// ScheduleCursorAdvanceParams advances a cursor using compare-and-swap.
type ScheduleCursorAdvanceParams struct {
	ID       string
	Expected time.Time
	Next     time.Time
}

// --- Row types ---

// JobRow is the canonical in-memory representation of a job record.
type JobRow struct {
	ID          string
	Queue       string
	Kind        string
	Args        []byte
	State       JobState
	Priority    int
	RunAt       time.Time
	CreatedAt   time.Time
	AttemptedAt *time.Time // immutable start time of the current attempt
	StartedAt   *time.Time // handler start for the current attempt
	// LeaseExpiresAt is the renewable deadline after which the claim may be rescued.
	LeaseExpiresAt *time.Time
	FinalizedAt    *time.Time
	AttemptNum     int
	MaxRetry       int
	Timeout        time.Duration
	Tags           []string
	Errors         []AttemptError
	UniqueKey      string
	WorkerID       string
	PipelineID     string // groups jobs that must not run concurrently
}

// AttemptError records a single failed attempt.
type AttemptError struct {
	At      time.Time
	Attempt int
	Error   string
	Trace   string
}

// QueueRow represents a queue metadata record.
type QueueRow struct {
	Name      string
	Paused    bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

// JobState is the lifecycle state of a job.
type JobState string

const (
	JobStateAvailable JobState = "available"
	JobStateRunning   JobState = "running"
	JobStateCompleted JobState = "completed"
	JobStateRetryable JobState = "retryable"
	JobStateDiscarded JobState = "discarded"
	JobStateCancelled JobState = "cancelled"
	JobStateScheduled JobState = "scheduled"
)

// Listener is optionally implemented by backends that support push notifications.
// The engine uses it to avoid polling when a backend supports real-time notifications.
// Return nil from Driver.Listener() to fall back to polling.
type Listener interface {
	Listen(ctx context.Context, queue string) (<-chan Notification, error)
	Unlisten(ctx context.Context, queue string) error
	Close() error
}

// Notification is a message from the backend indicating new jobs are available.
type Notification struct {
	Queue string
}
