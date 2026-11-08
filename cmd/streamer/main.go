package main

import (
	"context"
	"errors"
	pb "gpu-telemetry-pipeline/api/queue/v1"
	"gpu-telemetry-pipeline/internal/config"
	"gpu-telemetry-pipeline/internal/observe"
	"gpu-telemetry-pipeline/internal/platform"
	"gpu-telemetry-pipeline/internal/streamer"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("streamer stopped", "error", err)
		os.Exit(1)
	}
}
func run(log *slog.Logger) error {
	cfg, err := config.LoadStreamer()
	if err != nil {
		return err
	}
	conn, err := platform.Dial(cfg.QueueURL, cfg.MaxMessageBytes)
	if err != nil {
		return err
	}
	defer conn.Close()
	producer := &streamer.GRPCProducer{Client: pb.NewQueueClient(conn), Timeout: cfg.RequestTimeout}
	defer producer.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	metrics := observe.New()
	var running atomic.Bool
	running.Store(true)
	mux := http.NewServeMux()
	observe.Health(mux, func(context.Context) error {
		if !running.Load() {
			return errors.New("not running")
		}
		return nil
	})
	mux.Handle("GET /metrics", metrics)
	srv, httpErr, err := platform.HTTP(cfg.HTTPAddr, metrics.Wrap(mux, log), 5*time.Second, log)
	if err != nil {
		return err
	}
	defer platform.ShutdownHTTP(srv, cfg.ShutdownTimeout)

	runner := &streamer.Runner{Config: streamer.Config{Path: cfg.CSVPath, Interval: cfg.Interval, Loop: cfg.Loop, MaxFileBytes: cfg.MaxCSVBytes}, Producer: producer, Log: log, Metrics: metrics}
	err = platform.ManagedRun(cancel, func() error { return runner.Run(ctx) }, httpErr, cfg.ShutdownTimeout)
	running.Store(false)
	return err
}
