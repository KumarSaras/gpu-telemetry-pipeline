package transport_test

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	pb "gpu-telemetry-pipeline/api/queue/v1"
	"gpu-telemetry-pipeline/internal/queue"
	"gpu-telemetry-pipeline/internal/queue/transport"
	"gpu-telemetry-pipeline/internal/streamer"
	"gpu-telemetry-pipeline/internal/telemetry"
	"gpu-telemetry-pipeline/internal/testsupport"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func TestProducerAcceptanceDoesNotWaitForCollector(t *testing.T) {
	b, client := testsupport.Queue(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, e := client.Produce(ctx)
	if e != nil {
		t.Fatal(e)
	}
	env := telemetry.Envelope{Key: telemetry.Key{Session: "0123456789abcdef0123456789abcdef", Sequence: 1}, Telemetry: telemetry.Raw{UUID: "GPU-a"}}
	if e = stream.Send(transport.Wire(env)); e != nil {
		t.Fatal(e)
	}
	r, e := stream.Recv()
	if e != nil || r.Status != pb.ProduceStatus_PRODUCE_STATUS_ACCEPTED {
		t.Fatal(r, e)
	}
	s, e := b.Snapshot(ctx)
	if e != nil || s.Subscribers != 0 || s.Ready != 1 {
		t.Fatal(s, e)
	}
	if e = stream.Send(transport.Wire(env)); e != nil {
		t.Fatal(e)
	}
	r, e = stream.Recv()
	if e != nil || r.Status != pb.ProduceStatus_PRODUCE_STATUS_DUPLICATE {
		t.Fatal(r, e)
	}
	env.Key.Sequence = 0
	if e = stream.Send(transport.Wire(env)); e != nil {
		t.Fatal(e)
	}
	r, e = stream.Recv()
	if e != nil || r.Status != pb.ProduceStatus_PRODUCE_STATUS_INVALID {
		t.Fatal(r, e)
	}
}
func TestSubscriptionRequiresHandshake(t *testing.T) {
	_, client := testsupport.Queue(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s, e := client.Subscribe(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Send(&pb.ConsumerFrame{}); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Recv(); e == nil {
		t.Fatal("invalid handshake accepted")
	}
}

type dropAcceptance struct {
	transport.Server
	dropped atomic.Bool
}

func (s *dropAcceptance) Produce(stream grpc.BidiStreamingServer[pb.Envelope, pb.ProduceResult]) error {
	if !s.dropped.CompareAndSwap(false, true) {
		return s.Server.Produce(stream)
	}
	wire, err := stream.Recv()
	if err != nil {
		return err
	}
	e, err := transport.Envelope(wire)
	if err != nil {
		return err
	}
	if _, err = s.Broker.Enqueue(stream.Context(), e); err != nil {
		return err
	}
	return status.Error(codes.Unavailable, "simulated lost acceptance")
}
func TestLostProducerAcceptanceRetriesSameIdentity(t *testing.T) {
	b, e := queue.New(queue.Config{MaxMessages: 10, MaxBytes: 65536, MaxMessageBytes: 4096, MaxSubscribers: 1, MaxSlots: 1, AckTimeout: time.Second})
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	listener := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	defer srv.Stop()
	pb.RegisterQueueServer(srv, &dropAcceptance{Server: transport.Server{Broker: b, Log: testsupport.Logger(), AckTimeout: time.Second}})
	go func() { _ = srv.Serve(listener) }()
	conn, e := grpc.NewClient("passthrough:///test", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if e != nil {
		t.Fatal(e)
	}
	defer conn.Close()
	producer := &streamer.GRPCProducer{Client: pb.NewQueueClient(conn), Timeout: time.Second}
	defer producer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	record := telemetry.Envelope{Key: telemetry.Key{Session: "0123456789abcdef0123456789abcdef", Sequence: 9223372036854775807}, Telemetry: telemetry.Raw{Timestamp: "2026-10-30T10:00:00Z", UUID: "GPU-a", Value: "1"}}
	if e = producer.Send(ctx, record); e != nil {
		t.Fatal(e)
	}
	s, e := b.Snapshot(ctx)
	if e != nil || s.Accepted != 1 || s.Duplicates != 1 || s.Messages != 1 {
		t.Fatal(s, e)
	}
}
