// Package testsupport provides an in-process real gRPC transport for tests.
package testsupport

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	pb "gpu-telemetry-pipeline/api/queue/v1"
	"gpu-telemetry-pipeline/internal/queue"
	"gpu-telemetry-pipeline/internal/queue/transport"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"
)

func Logger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }
func Queue(t testing.TB) (*queue.Broker, pb.QueueClient) {
	t.Helper()
	cfg := queue.Config{MaxMessages: 10000, MaxBytes: 64 << 20, MaxMessageBytes: 64 << 10, MaxSubscribers: 10, MaxSlots: 128, AckTimeout: 30 * time.Second}
	b, e := queue.New(cfg)
	if e != nil {
		t.Fatal(e)
	}
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	pb.RegisterQueueServer(server, &transport.Server{Broker: b, Log: Logger(), AckTimeout: cfg.AckTimeout})
	go func() { _ = server.Serve(listener) }()
	conn, e := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = conn.Close(); server.Stop(); _ = listener.Close(); b.Close() })
	return b, pb.NewQueueClient(conn)
}
func Eventually(t testing.TB, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if condition() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("condition did not become true")
		case <-tick.C:
		}
	}
}
