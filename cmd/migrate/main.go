package main

import (
	"context"
	"gpu-telemetry-pipeline/internal/config"
	"gpu-telemetry-pipeline/internal/telemetry/postgres"
	"gpu-telemetry-pipeline/migrations"
	"log/slog"
	"os"
	"time"
)

func main() {
	if err := run(); err != nil {
		slog.Error("migration failed", "error", err)
		os.Exit(1)
	}
}
func run() error {
	cfg, err := config.LoadMigrate()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	repo, err := postgres.Open(ctx, cfg.DatabaseURL, 1)
	if err != nil {
		return err
	}
	defer repo.Pool.Close()
	return migrations.Apply(ctx, repo.Pool)
}
