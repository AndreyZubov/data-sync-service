// Package store persists sync runs, watermarks, and exported records in
// PostgreSQL.
package store

import (
	"context"
	"crypto/rand"
	"embed"
	"errors"
	"fmt"
	"os"

	"github.com/golang-migrate/migrate/v4"
	migratepgx "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

//go:embed migrations/*.sql
var migrations embed.FS

// ErrNotFound is returned when a requested row does not exist.
var ErrNotFound = errors.New("not found")

// Store is a PostgreSQL-backed store. It is safe for concurrent use.
type Store struct {
	pool *pgxpool.Pool
	// instance identifies this store's process as the owner of the runs it
	// executes; see the lease methods in runs.go.
	instance string
}

// Open connects to the database at dsn and verifies the connection.
// minConns raises the pool's connection limit to at least that many, so
// concurrent upsert workers are not starved; a higher pool_max_conns in dsn is
// kept.
func Open(ctx context.Context, dsn string, minConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	cfg.MaxConns = max(cfg.MaxConns, minConns)
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("connect to database: %w", err)
	}
	return &Store{pool: pool, instance: newInstanceID()}, nil
}

// newInstanceID returns an ID unique to this process: the host name, which
// makes it easy to tell instances apart in the database, and a random suffix,
// which keeps restarts and replicas on one host apart.
func newInstanceID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "syncd"
	}
	return host + "-" + rand.Text()[:8]
}

// Instance returns the ID this store leases runs under.
func (s *Store) Instance() string { return s.instance }

// Close closes all database connections.
func (s *Store) Close() { s.pool.Close() }

// Ping verifies that the database is reachable.
func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

// Migrate applies all pending schema migrations to the database at dsn.
// Concurrent callers are serialized by a Postgres advisory lock, so several
// replicas may start at once.
func Migrate(dsn string) error {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse database URL: %w", err)
	}
	src, err := iofs.New(migrations, "migrations")
	if err != nil {
		return fmt.Errorf("load migrations: %w", err)
	}
	// The migration driver closes the database it is given, so it gets its own
	// rather than sharing the service's pool.
	driver, err := migratepgx.WithInstance(stdlib.OpenDB(*cfg.ConnConfig), &migratepgx.Config{})
	if err != nil {
		return fmt.Errorf("connect for migration: %w", err)
	}
	m, err := migrate.NewWithInstance("iofs", src, "pgx5", driver)
	if err != nil {
		return fmt.Errorf("prepare migration: %w", err)
	}
	defer func() { _, _ = m.Close() }()

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}
