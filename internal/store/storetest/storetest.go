// Package storetest provides throwaway, migrated PostgreSQL databases for
// tests.
//
// The server comes from TEST_DATABASE_URL when it is set, which must point at
// a role allowed to create databases; otherwise a PostgreSQL container is
// started with testcontainers and shared by all tests of the package. Without
// either, tests are skipped, except on CI (CI=true), where they fail so a
// missing database cannot pass unnoticed.
package storetest

import (
	"context"
	"crypto/rand"
	"fmt"
	"net/url"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/AndreyZubov/data-sync-service/internal/store"
)

// image matches the version in docker-compose.yml.
const image = "postgres:16-alpine"

// DB is a fresh database with all migrations applied.
type DB struct {
	// DSN connects to the database.
	DSN string
	// Store is opened on the database.
	Store *store.Store
	// Pool queries the database directly, for assertions.
	Pool *pgxpool.Pool
}

var server struct {
	once sync.Once
	dsn  string
	err  error
}

// New creates a database that is dropped when the test ends.
func New(t testing.TB) *DB {
	t.Helper()
	ctx := context.Background()

	admin := serverDSN(t)
	name := "test_" + rand.Text()[:16]
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Fatalf("storetest: connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	// rand.Text is base32 upper case; quoting keeps the identifier as is.
	if _, err := conn.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatalf("storetest: create database: %v", err)
	}
	t.Cleanup(func() { dropDatabase(t, admin, name) })

	dsn, err := withDatabase(admin, name)
	if err != nil {
		t.Fatalf("storetest: %v", err)
	}
	if err := store.Migrate(dsn); err != nil {
		t.Fatalf("storetest: %v", err)
	}
	st, err := store.Open(ctx, dsn, 0)
	if err != nil {
		t.Fatalf("storetest: %v", err)
	}
	t.Cleanup(st.Close)
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("storetest: %v", err)
	}
	t.Cleanup(pool.Close)

	return &DB{DSN: dsn, Store: st, Pool: pool}
}

// serverDSN returns an administrative DSN for the test server, skipping or
// failing the test when none is available.
func serverDSN(t testing.TB) string {
	t.Helper()
	if dsn := os.Getenv("TEST_DATABASE_URL"); dsn != "" {
		return dsn
	}
	server.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		// The container outlives this test; the testcontainers reaper removes
		// it when the test binary exits.
		c, err := postgres.Run(ctx, image,
			postgres.WithDatabase("postgres"),
			postgres.WithUsername("postgres"),
			postgres.WithPassword("postgres"),
			postgres.BasicWaitStrategies(),
		)
		if err != nil {
			server.err = err
			return
		}
		server.dsn, server.err = c.ConnectionString(ctx, "sslmode=disable")
	})
	if server.err != nil {
		if os.Getenv("CI") == "true" {
			t.Fatalf("storetest: start PostgreSQL container: %v", server.err)
		}
		t.Skipf("storetest: no database (set TEST_DATABASE_URL or start Docker): %v", server.err)
	}
	return server.dsn
}

func dropDatabase(t testing.TB, admin, name string) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, admin)
	if err != nil {
		t.Errorf("storetest: connect to drop %s: %v", name, err)
		return
	}
	defer func() { _ = conn.Close(ctx) }()
	// FORCE ends connections a test left open, such as a leaked pool.
	if _, err := conn.Exec(ctx, "DROP DATABASE IF EXISTS "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)"); err != nil {
		t.Errorf("storetest: drop database %s: %v", name, err)
	}
}

// withDatabase returns dsn with its database replaced by name.
func withDatabase(dsn, name string) (string, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse DSN: %w", err)
	}
	u.Path = "/" + name
	return u.String(), nil
}
