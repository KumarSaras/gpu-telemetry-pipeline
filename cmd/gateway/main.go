package main

import (
	"context"
	"gpu-telemetry-pipeline/internal/config"
	"gpu-telemetry-pipeline/internal/gateway"
	"gpu-telemetry-pipeline/internal/observe"
	"gpu-telemetry-pipeline/internal/platform"
	"gpu-telemetry-pipeline/internal/telemetry/postgres"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("gateway stopped", "error", err)
		os.Exit(1)
	}
}
func run(log *slog.Logger) error {
	cfg, err := config.LoadGateway()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	initctx, cancel := context.WithTimeout(ctx, cfg.RequestTimeout)
	repo, err := postgres.Open(initctx, cfg.DatabaseURL, int32(cfg.DBConnections))
	cancel()
	if err != nil {
		return err
	}
	defer repo.Pool.Close()
	mux, _ := gateway.New(repo, gateway.Config{QueryTimeout: cfg.QueryTimeout, MaxQueries: cfg.MaxQueries}, log)
	metrics := observe.New()
	observe.Health(mux, repo.Ready)
	mux.Handle("GET /metrics", metrics)
	srv, errs, err := platform.HTTP(cfg.HTTPAddr, metrics.Wrap(mux, log), cfg.QueryTimeout+time.Second, log)
	if err != nil {
		return err
	}
	defer platform.ShutdownHTTP(srv, cfg.ShutdownTimeout)
	select {
	case <-ctx.Done():
		return nil
	case err := <-errs:
		return err
	}
}
