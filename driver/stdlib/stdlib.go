// Package stdlib provides a goncordia driver backed by database/sql.
// Supports PostgreSQL, MySQL, and SQLite via a Dialect switch.
//
// Usage (PostgreSQL via pgx stdlib adapter):
//
//	import _ "github.com/jackc/pgx/v5/stdlib"
//	db, _ := sql.Open("pgx", os.Getenv("DATABASE_URL"))
//	d, _ := stdlib.New(db, stdlib.Postgres)
//
// Usage (SQLite, no Docker needed):
//
//	import _ "modernc.org/sqlite"
//	db, _ := sql.Open("sqlite", "./jobs.db")
//	d, _ := stdlib.New(db, stdlib.SQLite)
package stdlib

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"strconv"
	"strings"
	"time"

	goncordia "github.com/kirimatt/goncordia"
	"github.com/kirimatt/goncordia/clock"
	"github.com/kirimatt/goncordia/core"
	"github.com/kirimatt/goncordia/driver"
)

//go:embed migrations/**/*.sql
var migrationFS embed.FS

// Dialect identifies the SQL dialect of the underlying database.
type Dialect int

const (
	Postgres Dialect = iota
	MySQL
	SQLite
)

func (d Dialect) String() string {
	switch d {
	case Postgres:
		return "postgres"
	case MySQL:
		return "mysql"
	case SQLite:
		return "sqlite"
	default:
		return "unknown"
	}
}

// q rebinds a query written with ? placeholders to the dialect's format.
// Postgres replaces each ? with $1, $2, etc.; other dialects return the query unchanged.
func (d Dialect) q(query string) string {
	if d != Postgres {
		return query
	}
	var b strings.Builder
	n := 0
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			n++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
		} else {
			b.WriteByte(query[i])
		}
	}
	return b.String()
}

// supportsSkipLocked reports whether this dialect supports SELECT FOR UPDATE SKIP LOCKED.
func (d Dialect) supportsSkipLocked() bool {
	return d == Postgres || d == MySQL
}

// Driver implements driver.Driver[*sql.Tx] backed by database/sql.
type Driver struct {
	db      *sql.DB
	dialect Dialect
	clk     clock.Clock
}

// Option configures the Driver.
type Option func(*Driver)

// WithClock injects a custom clock (for testing).
func WithClock(c clock.Clock) Option { return func(d *Driver) { d.clk = c } }

// New creates a Driver from an existing *sql.DB. The caller retains ownership
// of db and must close it after the driver is no longer in use.
// Call Migrate to create the schema before starting workers.
func New(db *sql.DB, dialect Dialect, opts ...Option) *Driver {
	d := &Driver{db: db, dialect: dialect, clk: clock.Real{}}
	for _, o := range opts {
		o(d)
	}
	return d
}

// Migrate runs embedded SQL migrations for the configured dialect.
func (d *Driver) Migrate(ctx context.Context) error {
	conn, err := d.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection for %s: %w", d.dialect, err)
	}
	defer conn.Close()

	if d.dialect == SQLite {
		if _, err := conn.ExecContext(ctx, `PRAGMA busy_timeout = 30000`); err != nil {
			return fmt.Errorf("configure sqlite migration lock: %w", err)
		}
		if _, err := conn.ExecContext(ctx, `BEGIN IMMEDIATE`); err != nil {
			return fmt.Errorf("acquire sqlite migration lock: %w", err)
		}
		if err := d.migrateWith(ctx, conn, func(version string, statements []string) error {
			for _, stmt := range statements {
				if _, err := conn.ExecContext(ctx, stmt); err != nil {
					return err
				}
			}
			_, err := conn.ExecContext(ctx,
				d.dialect.q(`INSERT INTO goncordia_schema_migrations (version) VALUES (?)`), version)
			return err
		}); err != nil {
			rollbackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = conn.ExecContext(rollbackCtx, `ROLLBACK`)
			return err
		}
		if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
			return fmt.Errorf("commit sqlite migrations: %w", err)
		}
		return nil
	}

	unlock, err := acquireMigrationLock(ctx, conn, d.dialect)
	if err != nil {
		return err
	}
	defer unlock()
	return d.migrateWith(ctx, conn, func(version string, statements []string) error {
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin migration %s: %w", version, err)
		}
		for _, stmt := range statements {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				_ = tx.Rollback()
				return fmt.Errorf("apply migration %s: %w", version, err)
			}
		}
		if _, err := tx.ExecContext(ctx,
			d.dialect.q(`INSERT INTO goncordia_schema_migrations (version) VALUES (?)`), version); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("record migration %s: %w", version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %s: %w", version, err)
		}
		return nil
	})
}

type migrationQueryExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (d *Driver) migrateWith(ctx context.Context, q migrationQueryExecer, apply func(string, []string) error) error {
	if _, err := q.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS goncordia_schema_migrations (
		version VARCHAR(255) PRIMARY KEY,
		applied_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		return fmt.Errorf("create migration journal for %s: %w", d.dialect, err)
	}
	dir := "migrations/" + d.dialect.String()
	entries, err := migrationFS.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read migrations for %s: %w", d.dialect, err)
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		var applied int
		if err := q.QueryRowContext(ctx,
			d.dialect.q(`SELECT COUNT(*) FROM goncordia_schema_migrations WHERE version = ?`),
			e.Name(),
		).Scan(&applied); err != nil {
			return fmt.Errorf("check migration %s: %w", e.Name(), err)
		}
		if applied > 0 {
			continue
		}
		sqlBytes, err := migrationFS.ReadFile(dir + "/" + e.Name())
		if err != nil {
			return fmt.Errorf("read migration %s: %w", e.Name(), err)
		}
		if err := apply(e.Name(), splitStatements(string(sqlBytes))); err != nil {
			return err
		}
	}
	return nil
}

func acquireMigrationLock(ctx context.Context, conn *sql.Conn, dialect Dialect) (func(), error) {
	switch dialect {
	case Postgres:
		if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock(hashtext('goncordia_schema_migrations'))`); err != nil {
			return nil, fmt.Errorf("acquire postgres migration lock: %w", err)
		}
		return func() {
			releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = conn.ExecContext(releaseCtx, `SELECT pg_advisory_unlock(hashtext('goncordia_schema_migrations'))`)
		}, nil
	case MySQL:
		var acquired sql.NullInt64
		if err := conn.QueryRowContext(ctx, `SELECT GET_LOCK('goncordia_schema_migrations', 60)`).Scan(&acquired); err != nil {
			return nil, fmt.Errorf("acquire mysql migration lock: %w", err)
		}
		if !acquired.Valid || acquired.Int64 != 1 {
			return nil, fmt.Errorf("acquire mysql migration lock: timed out")
		}
		return func() {
			releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = conn.ExecContext(releaseCtx, `SELECT RELEASE_LOCK('goncordia_schema_migrations')`)
		}, nil
	default:
		return func() {}, nil
	}
}

func (d *Driver) Name() string { return d.dialect.String() }

func (d *Driver) Capabilities() driver.Capabilities {
	return driver.Capabilities{
		NativeTx:           true,
		SkipLocked:         d.dialect.supportsSkipLocked(),
		UniqueJobs:         true,
		ListenNotify:       false, // stdlib doesn't support LISTEN/NOTIFY
		AdvisoryLocks:      false,
		LinearizableLeases: true,
		LinearizableCAS:    true,
	}
}

func (d *Driver) Executor() driver.Executor {
	return &executor{db: d.db, dialect: d.dialect, clk: d.clk}
}

func (d *Driver) UnwrapTx(tx *sql.Tx) driver.ExecutorTx {
	return &txExecutor{tx: tx, dialect: d.dialect, clk: d.clk}
}

// Listener returns nil — stdlib driver uses polling, not push notifications.
func (d *Driver) Listener() driver.Listener { return nil }

func (d *Driver) Close() error { return nil }

// splitStatements splits a SQL file into individual statements on semicolons,
// skipping empty/whitespace-only statements.
func splitStatements(sql string) []string {
	parts := strings.Split(sql, ";")
	var result []string
	for _, p := range parts {
		s := strings.TrimSpace(p)
		if s != "" {
			result = append(result, s)
		}
	}
	return result
}

// Client is a type alias so callers never write goncordia.Client[*sql.Tx].
type Client = goncordia.Client[*sql.Tx]

// WorkerPool is a type alias so callers never write goncordia.WorkerPool[*sql.Tx].
type WorkerPool = goncordia.WorkerPool[*sql.Tx]

// NewClient creates a Client bound to this stdlib driver.
func NewClient(d *Driver, cfg goncordia.ClientConfig) *Client {
	return goncordia.NewClient[*sql.Tx](d, cfg)
}

// NewWorkerPool creates a WorkerPool bound to this stdlib driver.
func NewWorkerPool(d *Driver, r *core.Registry, cfg goncordia.WorkerConfig) *WorkerPool {
	return goncordia.NewWorkerPool[*sql.Tx](d, r, cfg)
}

// FetchParams is a convenience constructor for driver.FetchParams used in tests.
func FetchParams(queue string, limit int) driver.FetchParams {
	return driver.FetchParams{Queue: queue, Limit: limit}
}

// DB returns the underlying *sql.DB (e.g. for opening transactions in tests).
func (d *Driver) DB() *sql.DB { return d.db }

// compile-time check
var _ driver.Driver[*sql.Tx] = (*Driver)(nil)
