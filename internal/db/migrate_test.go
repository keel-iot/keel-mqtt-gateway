package db_test

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/keel-iot/keel-mqtt-gateway/internal/db"
)

// TestMigrate requires a live Postgres (TEST_DATABASE_URL) — skipped
// otherwise. Point it at the docker-compose Postgres (see
// deploy/docker-compose), e.g.:
//
//	TEST_DATABASE_URL=postgres://postgres:postgres@localhost:15432/keel_devices?sslmode=disable go test ./internal/db/...
func TestMigrate(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set, skipping live-Postgres migration test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Clean slate so the test proves migrations create everything from
	// scratch, not just tolerate a pre-existing schema.
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS devices CASCADE`); err != nil {
		t.Fatalf("drop schema: %v", err)
	}

	if err := db.Migrate(ctx, pool, log); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}

	// Idempotency: running again must be a clean no-op.
	if err := db.Migrate(ctx, pool, log); err != nil {
		t.Fatalf("second Migrate (idempotency check): %v", err)
	}

	var jwksURLExists bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'devices'
			  AND table_name = 'tenant_gateway_config'
			  AND column_name = 'jwks_url'
		)`).Scan(&jwksURLExists)
	if err != nil {
		t.Fatalf("check jwks_url column: %v", err)
	}
	if !jwksURLExists {
		t.Fatal("expected devices.tenant_gateway_config.jwks_url to exist after migration")
	}

	var clavexCAURLExists bool
	err = pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = 'devices'
			  AND table_name = 'tenant_gateway_config'
			  AND column_name = 'clavex_ca_url'
		)`).Scan(&clavexCAURLExists)
	if err != nil {
		t.Fatalf("check clavex_ca_url column: %v", err)
	}
	if !clavexCAURLExists {
		t.Fatal("expected devices.tenant_gateway_config.clavex_ca_url to exist after migration")
	}

	// Asserted against the actual embedded migration count (Migrate reads
	// migrations/*.sql via fs.ReadDir) rather than a hardcoded number, so
	// this can't go stale the next time a migration file is added.
	entries, err := migrationEntries(t)
	if err != nil {
		t.Fatalf("read embedded migrations: %v", err)
	}

	var migrationCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM devices.schema_migrations`).Scan(&migrationCount); err != nil {
		t.Fatalf("count schema_migrations: %v", err)
	}
	if migrationCount != len(entries) {
		t.Fatalf("expected %d recorded migrations (one per embedded migration file), got %d", len(entries), migrationCount)
	}
}

// migrationEntries counts the actual embedded migration files, so
// TestMigrate's own expectation tracks internal/db/migrations/*.sql
// instead of a number that silently goes stale when a migration is added
// (see keel-iot/keel-mqtt-gateway#17).
func migrationEntries(t *testing.T) ([]string, error) {
	t.Helper()
	matches, err := filepath.Glob("migrations/*.sql")
	if err != nil {
		return nil, err
	}
	return matches, nil
}
