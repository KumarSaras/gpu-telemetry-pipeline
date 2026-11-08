package main

import (
	"context"
	"fmt"
	pb "gpu-telemetry-pipeline/api/queue/v1"
	"gpu-telemetry-pipeline/internal/config"
	"gpu-telemetry-pipeline/internal/observe"
	"gpu-telemetry-pipeline/internal/platform"
	"gpu-telemetry-pipeline/internal/queue"
	"gpu-telemetry-pipeline/internal/queue/transport"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("queue stopped", "error", err)
		os.Exit(1)
	}
}
func run(log *slog.Logger) error {
	cfg, err := config.LoadQueue()
	if err != nil {
		return err
	}
	b, err := queue.New(cfg.Queue)
	if err != nil {
		return err
	}
	defer b.Close()
	ln, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		return err
	}
	srv := grpc.NewServer(grpc.MaxRecvMsgSize(cfg.Queue.MaxMessageBytes), grpc.MaxSendMsgSize(cfg.Queue.MaxMessageBytes+1024), grpc.StreamInterceptor(transport.LimitStreams(cfg.MaxStreams)), grpc.KeepaliveParams(keepalive.ServerParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}), grpc.KeepaliveEnforcementPolicy(keepalive.EnforcementPolicy{MinTime: 10 * time.Second, PermitWithoutStream: true}))
	pb.RegisterQueueServer(srv, &transport.Server{Broker: b, Log: log, AckTimeout: cfg.Queue.AckTimeout})
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	log.Info("queue listening", "address", ln.Addr().String())
	mux := http.NewServeMux()
	metrics := observe.New()
	observe.Health(mux, func(ctx context.Context) error {
		s, err := b.Snapshot(ctx)
		if err != nil {
			return err
		}
		if s.Draining {
			return queue.ErrDraining
		}
		return nil
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		s, err := b.Snapshot(ctx)
		if err != nil {
			http.Error(w, "queue unavailable", 503)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprintf(w, "gpu_telemetry_queue_messages %d\ngpu_telemetry_queue_ready %d\ngpu_telemetry_queue_inflight %d\ngpu_telemetry_queue_bytes %d\ngpu_telemetry_subscribers %d\ngpu_telemetry_subscriber_slots %d\ngpu_telemetry_accepted_total %d\ngpu_telemetry_completed_total %d\ngpu_telemetry_duplicates_total %d\ngpu_telemetry_rejected_total %d\ngpu_telemetry_redelivered_total %d\ngpu_telemetry_full_total %d\n", s.Messages, s.Ready, s.InFlight, s.Bytes, s.Subscribers, s.Slots, s.Accepted, s.Completed, s.Duplicates, s.Rejected, s.Redelivered, s.Full)
		metrics.ServeHTTP(w, r)
	})
	httpSrv, httpErr, err := platform.HTTP(cfg.HTTPAddr, metrics.Wrap(mux, log), 5*time.Second, log)
	if err != nil {
		srv.Stop()
		return err
	}
	defer platform.ShutdownHTTP(httpSrv, cfg.ShutdownTimeout)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var failure error
	select {
	case <-ctx.Done():
	case failure = <-serveErr:
	case failure = <-httpErr:
	case failure = <-b.Failure():
	}
	drainctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	if err := b.Drain(drainctx); err != nil {
		stats, _ := b.Snapshot(context.Background())
		log.Warn("in-memory queue did not drain; remaining work will be lost", "messages", stats.Messages, "error", err)
	}
	// Long-lived bidi streams do not finish on GracefulStop alone.
	srv.Stop()
	return failure
}
