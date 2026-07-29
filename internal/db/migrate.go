// Package db owns keel-mqtt-gateway's PostgreSQL schema (the `devices`
// schema — tenants, devices, device_credentials, tenant_gateway_config).
// This service used to inherit that schema from another service living in
// the same monorepo; now that it's a standalone repo (and the broker other
// keel components will eventually depend on, alongside Clavex as IAM), it
// owns its own migrations instead of assuming the schema exists.
package db

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// Migrate applies every migration in migrations/ not yet recorded in
// devices.schema_migrations, in filename order (NNNN_description.sql).
// Each migration runs in its own transaction; a failure stops before
// recording that version, so a retry resumes from the same migration.
// Safe to call on every startup — a no-op once fully applied.
func Migrate(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	if log == nil {
		log = slog.Default()
	}

	if _, err := pool.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS devices`); err != nil {
		return fmt.Errorf("db: create devices schema: %w", err)
	}
	if _, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS devices.schema_migrations (
			version    INT PRIMARY KEY,
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`); err != nil {
		return fmt.Errorf("db: create schema_migrations table: %w", err)
	}

	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		return fmt.Errorf("db: read embedded migrations: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	for _, e := range entries {
		name := e.Name()
		version, err := versionFromName(name)
		if err != nil {
			return fmt.Errorf("db: migration %q: %w", name, err)
		}

		var applied bool
		if err := pool.QueryRow(ctx,
			`SELECT EXISTS(SELECT 1 FROM devices.schema_migrations WHERE version = $1)`,
			version,
		).Scan(&applied); err != nil {
			return fmt.Errorf("db: check migration %q: %w", name, err)
		}
		if applied {
			continue
		}

		sqlBytes, err := migrationsFS.ReadFile("migrations/" + name)
		if err != nil {
			return fmt.Errorf("db: read migration %q: %w", name, err)
		}

		tx, err := pool.Begin(ctx)
		if err != nil {
			return fmt.Errorf("db: begin tx for migration %q: %w", name, err)
		}
		if _, err := tx.Exec(ctx, string(sqlBytes)); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("db: apply migration %q: %w", name, err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO devices.schema_migrations (version) VALUES ($1)`, version,
		); err != nil {
			_ = tx.Rollback(ctx)
			return fmt.Errorf("db: record migration %q: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("db: commit migration %q: %w", name, err)
		}
		log.Info("db: applied migration", "file", name)
	}
	return nil
}

// versionFromName extracts the numeric prefix from "NNNN_description.sql".
func versionFromName(name string) (int, error) {
	prefix, _, ok := strings.Cut(name, "_")
	if !ok {
		return 0, fmt.Errorf("expected NNNN_description.sql, got %q", name)
	}
	v, err := strconv.Atoi(prefix)
	if err != nil {
		return 0, fmt.Errorf("expected numeric prefix, got %q: %w", prefix, err)
	}
	return v, nil
}
