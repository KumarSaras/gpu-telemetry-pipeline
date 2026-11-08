package main

import (
	"context"
	"errors"
	pb "gpu-telemetry-pipeline/api/queue/v1"
	"gpu-telemetry-pipeline/internal/collector"
	"gpu-telemetry-pipeline/internal/config"
	"gpu-telemetry-pipeline/internal/observe"
	"gpu-telemetry-pipeline/internal/platform"
	"gpu-telemetry-pipeline/internal/telemetry/postgres"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("collector stopped", "error", err)
		os.Exit(1)
	}
}
func run(log *slog.Logger) error {
	cfg, err := config.LoadCollector()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	initctx, initcancel := context.WithTimeout(ctx, cfg.RequestTimeout)
	repo, err := postgres.Open(initctx, cfg.DatabaseURL, int32(cfg.DBConnections))
	initcancel()
	if err != nil {
		return err
	}
	defer repo.Pool.Close()
	conn, err := platform.Dial(cfg.QueueURL, cfg.MaxMessageBytes)
	if err != nil {
		return err
	}
	defer conn.Close()
	metrics := observe.New()
	worker := &collector.Collector{Client: pb.NewQueueClient(conn), Store: repo, Slots: cfg.CollectorSlots, ProcessTimeout: cfg.ProcessTimeout, SendTimeout: cfg.RequestTimeout, Log: log, Metrics: metrics}
	mux := http.NewServeMux()
	observe.Health(mux, func(ctx context.Context) error {
		if !worker.Ready.Load() {
			return errors.New("not subscribed")
		}
		return repo.Ready(ctx)
	})
	mux.Handle("GET /metrics", metrics)
	srv, httpErr, err := platform.HTTP(cfg.HTTPAddr, metrics.Wrap(mux, log), 5*time.Second, log)
	if err != nil {
		return err
	}
	defer platform.ShutdownHTTP(srv, cfg.ShutdownTimeout)

	log.Info("collector starting", "workers", 1, "slots", cfg.CollectorSlots)
	return platform.ManagedRun(cancel, func() error { return worker.Run(ctx) }, httpErr, cfg.ShutdownTimeout)
}
