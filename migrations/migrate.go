// Package migrations applies ordered, transactional database migrations.
package migrations

import (
	"context"
	_ "embed"
	"github.com/jackc/pgx/v5"
	"time"
)

//go:embed 001_telemetry.sql
var initial string

//go:embed 002_drop_payload_hash.sql
var dropPayloadHash string

// Beginner is the transaction boundary supplied by *pgxpool.Pool.
type Beginner interface {
	Begin(context.Context) (pgx.Tx, error)
}

func Apply(ctx context.Context, pool Beginner) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		rollbackctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(rollbackctx)
	}()
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(604785128771)"); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, "CREATE TABLE IF NOT EXISTS schema_migrations(version integer PRIMARY KEY)"); err != nil {
		return err
	}
	for _, migration := range []struct {
		version int
		sql     string
	}{{1, initial}, {2, dropPayloadHash}} {
		var exists bool
		if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)", migration.version).Scan(&exists); err != nil {
			return err
		}
		if !exists {
			if _, err = tx.Exec(ctx, migration.sql); err != nil {
				return err
			}
			if _, err = tx.Exec(ctx, "INSERT INTO schema_migrations(version) VALUES ($1)", migration.version); err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}
