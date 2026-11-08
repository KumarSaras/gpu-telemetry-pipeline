package transport

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	pb "gpu-telemetry-pipeline/api/queue/v1"
	"gpu-telemetry-pipeline/internal/queue"
	"gpu-telemetry-pipeline/internal/telemetry"
	"log/slog"
)

type produceStream struct {
	grpc.ServerStream
	ctx     context.Context
	receive func() (*pb.Envelope, error)
	send    func(*pb.ProduceResult) error
}

func (s *produceStream) Context() context.Context       { return s.ctx }
func (s *produceStream) Recv() (*pb.Envelope, error)    { return s.receive() }
func (s *produceStream) Send(v *pb.ProduceResult) error { return s.send(v) }

type subscribeStream struct {
	grpc.ServerStream
	ctx     context.Context
	receive func() (*pb.ConsumerFrame, error)
	send    func(*pb.Delivery) error
	header  func(metadata.MD) error
}

func (s *subscribeStream) Context() context.Context         { return s.ctx }
func (s *subscribeStream) Recv() (*pb.ConsumerFrame, error) { return s.receive() }
func (s *subscribeStream) Send(v *pb.Delivery) error        { return s.send(v) }
func (s *subscribeStream) SendHeader(v metadata.MD) error {
	if s.header != nil {
		return s.header(v)
	}
	return nil
}
func testBroker(t *testing.T) *queue.Broker {
	t.Helper()
	b, err := queue.New(queue.Config{MaxMessages: 1, MaxBytes: 65536, MaxMessageBytes: 4096, MaxSubscribers: 1, MaxSlots: 2, AckTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	return b
}
func testEnvelope(sequence int64) telemetry.Envelope {
	return telemetry.Envelope{Key: telemetry.Key{Session: "0123456789abcdef0123456789abcdef", Sequence: sequence}, Telemetry: telemetry.Raw{UUID: "GPU-a", MetricName: "util", Value: "1"}}
}
func runProduce(s *Server, ctx context.Context, envelope *pb.Envelope, send func(*pb.ProduceResult) error) error {
	first := true
	return s.Produce(&produceStream{ctx: ctx, receive: func() (*pb.Envelope, error) {
		if first {
			first = false
			return envelope, nil
		}
		return nil, io.EOF
	}, send: send})
}
func TestProducerFullDrainingAndClosedQueue(t *testing.T) {
	b := testBroker(t)
	ctx := context.Background()
	if _, err := b.Enqueue(ctx, testEnvelope(1)); err != nil {
		t.Fatal(err)
	}
	s := &Server{Broker: b}
	check := func(want pb.ProduceStatus) {
		t.Helper()
		if err := runProduce(s, ctx, Wire(testEnvelope(2)), func(v *pb.ProduceResult) error {
			if v.Status != want {
				t.Fatal(v)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	check(pb.ProduceStatus_PRODUCE_STATUS_FULL)
	drainCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if err := b.Drain(drainCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	check(pb.ProduceStatus_PRODUCE_STATUS_DRAINING)
	b.Close()
	if err := runProduce(s, ctx, Wire(testEnvelope(2)), func(*pb.ProduceResult) error { return nil }); status.Code(err) != codes.Unavailable {
		t.Fatal(err)
	}
}
func TestProducerReceiveSendAndCancellationFailures(t *testing.T) {
	failure := errors.New("stream failed")
	b := testBroker(t)
	s := &Server{Broker: b}
	if err := s.Produce(&produceStream{ctx: context.Background(), receive: func() (*pb.Envelope, error) { return nil, failure }}); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if err := runProduce(s, context.Background(), Wire(testEnvelope(1)), func(*pb.ProduceResult) error { return failure }); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	snapshot, err := b.Snapshot(context.Background())
	if err != nil || snapshot.Accepted != 1 || snapshot.Messages != 1 {
		t.Fatal(snapshot, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runProduce(s, ctx, Wire(testEnvelope(2)), func(*pb.ProduceResult) error { return nil }); status.Code(err) != codes.Canceled {
		t.Fatal(err)
	}
}
func TestEnvelopeRequiredFieldsAndMetadataRoundTrip(t *testing.T) {
	for _, p := range []*pb.Envelope{nil, {}, {Key: &pb.Key{}}, {Telemetry: &pb.Telemetry{}}} {
		if _, err := Envelope(p); err == nil {
			t.Fatal("missing key/payload accepted", p)
		}
	}
	e := testEnvelope(1)
	e.Telemetry.Timestamp = "2026-11-09T12:00:00Z"
	e.Telemetry.GPUID = "0"
	e.Telemetry.Device = "nvidia0"
	e.Telemetry.ModelName = "model"
	e.Telemetry.Hostname = "host"
	e.Telemetry.LabelsRaw = `a="b,c"`
	got, err := Envelope(Wire(e))
	if err != nil || got != e {
		t.Fatal(got, err)
	}
}
func await(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("stream did not finish")
		return nil
	}
}
func TestSubscriptionDispositionsReleaseOrRequeueWork(t *testing.T) {
	for _, action := range []pb.Disposition{pb.Disposition_DISPOSITION_ACK_PERSISTED, pb.Disposition_DISPOSITION_REJECT, pb.Disposition_DISPOSITION_RETRY} {
		t.Run(action.String(), func(t *testing.T) {
			b := testBroker(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			frames := make(chan *pb.ConsumerFrame, 2)
			frames <- &pb.ConsumerFrame{Body: &pb.ConsumerFrame_Subscribe{Subscribe: &pb.Subscription{Slots: 1}}}
			delivered := make(chan *pb.Delivery, 1)
			headers := make(chan metadata.MD, 1)
			stream := &subscribeStream{ctx: ctx, receive: func() (*pb.ConsumerFrame, error) {
				select {
				case f, ok := <-frames:
					if !ok {
						return nil, io.EOF
					}
					return f, nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}, send: func(v *pb.Delivery) error { delivered <- v; return nil }, header: func(h metadata.MD) error { headers <- h; return nil }}
			s := &Server{Broker: b, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), AckTimeout: time.Second}
			done := make(chan error, 1)
			go func() { done <- s.Subscribe(stream) }()
			select {
			case header := <-headers:
				if header.Get("ack-timeout")[0] != "1s" {
					t.Fatal(header)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("no handshake")
			}
			if _, err := b.Enqueue(ctx, testEnvelope(1)); err != nil {
				t.Fatal(err)
			}
			var delivery *pb.Delivery
			select {
			case delivery = <-delivered:
			case <-time.After(3 * time.Second):
				t.Fatal("no delivery")
			}
			frames <- &pb.ConsumerFrame{Body: &pb.ConsumerFrame_Completion{Completion: &pb.Completion{Key: delivery.Envelope.Key, DeliveryTag: delivery.DeliveryTag, Disposition: action, Reason: strings.Repeat("x", 400)}}}
			if action != pb.Disposition_DISPOSITION_RETRY {
				close(frames)
			}
			err := await(t, done)
			if action == pb.Disposition_DISPOSITION_RETRY {
				if err == nil {
					t.Fatal("retry retained subscription")
				}
			} else if err != nil {
				t.Fatal(err)
			}
			snapshot, err := b.Snapshot(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if snapshot.Subscribers != 0 {
				t.Fatal(snapshot)
			}
			switch action {
			case pb.Disposition_DISPOSITION_ACK_PERSISTED:
				if snapshot.Completed != 1 || snapshot.Messages != 0 || snapshot.Bytes != 0 {
					t.Fatal(snapshot)
				}
			case pb.Disposition_DISPOSITION_REJECT:
				if snapshot.Rejected != 1 || snapshot.Messages != 0 || snapshot.Bytes != 0 {
					t.Fatal(snapshot)
				}
			case pb.Disposition_DISPOSITION_RETRY:
				if snapshot.Ready != 1 || snapshot.Redelivered != 1 || snapshot.Messages != 1 {
					t.Fatal(snapshot)
				}
			}
		})
	}
}
func TestSubscriptionInvalidFramesAndHandshakeFailures(t *testing.T) {
	failure := errors.New("header write failed")
	for _, test := range []struct {
		name          string
		first, second *pb.ConsumerFrame
		headerError   error
		wantCode      codes.Code
	}{
		{name: "slots", first: &pb.ConsumerFrame{Body: &pb.ConsumerFrame_Subscribe{Subscribe: &pb.Subscription{Slots: 0}}}, wantCode: codes.ResourceExhausted},
		{name: "header", headerError: failure},
		{name: "missing completion", second: &pb.ConsumerFrame{}, wantCode: codes.InvalidArgument},
		{name: "invalid disposition", second: &pb.ConsumerFrame{Body: &pb.ConsumerFrame_Completion{Completion: &pb.Completion{Key: &pb.Key{StreamerSession: testEnvelope(1).Key.Session, Sequence: 1}, DeliveryTag: 1, Disposition: 99}}}, wantCode: codes.InvalidArgument},
	} {
		t.Run(test.name, func(t *testing.T) {
			b := testBroker(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			first := test.first
			if first == nil {
				first = &pb.ConsumerFrame{Body: &pb.ConsumerFrame_Subscribe{Subscribe: &pb.Subscription{Slots: 1}}}
			}
			index := 0
			stream := &subscribeStream{ctx: ctx, receive: func() (*pb.ConsumerFrame, error) {
				index++
				if index == 1 {
					return first, nil
				}
				if index == 2 {
					return test.second, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}, header: func(metadata.MD) error { return test.headerError }}
			err := (&Server{Broker: b, AckTimeout: time.Second}).Subscribe(stream)
			if test.headerError != nil {
				if !errors.Is(err, failure) {
					t.Fatal(err)
				}
			} else if status.Code(err) != test.wantCode {
				t.Fatal(err)
			}
			snapshot, _ := b.Snapshot(ctx)
			if snapshot.Subscribers != 0 {
				t.Fatal("subscription leaked", snapshot)
			}
		})
	}
	b := testBroker(t)
	if err := (&Server{Broker: b}).Subscribe(&subscribeStream{ctx: context.Background(), receive: func() (*pb.ConsumerFrame, error) { return nil, failure }}); !errors.Is(err, failure) {
		t.Fatal(err)
	}
}
func TestSubscriptionNetworkSendFailureRequeuesAssignment(t *testing.T) {
	b := testBroker(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failure := errors.New("network send failed")
	first := true
	ready := make(chan struct{})
	stream := &subscribeStream{ctx: ctx, receive: func() (*pb.ConsumerFrame, error) {
		if first {
			first = false
			return &pb.ConsumerFrame{Body: &pb.ConsumerFrame_Subscribe{Subscribe: &pb.Subscription{Slots: 1}}}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}, header: func(metadata.MD) error { close(ready); return nil }, send: func(*pb.Delivery) error { return failure }}
	done := make(chan error, 1)
	go func() { done <- (&Server{Broker: b, AckTimeout: time.Second}).Subscribe(stream) }()
	select {
	case <-ready:
	case <-time.After(3 * time.Second):
		t.Fatal("no handshake")
	}
	if _, err := b.Enqueue(ctx, testEnvelope(1)); err != nil {
		t.Fatal(err)
	}
	if err := await(t, done); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	snapshot, _ := b.Snapshot(ctx)
	if snapshot.Ready != 1 || snapshot.Redelivered != 1 || snapshot.Subscribers != 0 {
		t.Fatal(snapshot)
	}
}
func TestLimitStreamsBoundsConcurrentHandlersAndReleasesCapacity(t *testing.T) {
	intercept := LimitStreams(1)
	entered, release := make(chan struct{}), make(chan struct{})
	done := make(chan error, 1)
	failure := errors.New("handler failed")
	go func() {
		done <- intercept(nil, nil, &grpc.StreamServerInfo{}, func(any, grpc.ServerStream) error { close(entered); <-release; return failure })
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("first handler not admitted")
	}
	var rejectedCalled atomic.Bool
	if err := intercept(nil, nil, &grpc.StreamServerInfo{}, func(any, grpc.ServerStream) error { rejectedCalled.Store(true); return nil }); status.Code(err) != codes.ResourceExhausted || rejectedCalled.Load() {
		t.Fatal(err)
	}
	close(release)
	if err := await(t, done); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if err := intercept(nil, nil, &grpc.StreamServerInfo{}, func(any, grpc.ServerStream) error { return nil }); err != nil {
		t.Fatal("stream slot leaked", err)
	}
}
func TestReasonTruncationBounds(t *testing.T) {
	if got := truncate("ok", 256); got != "ok" {
		t.Fatal(got)
	}
	if got := truncate(strings.Repeat("x", 400), 256); len(got) != 256 {
		t.Fatal(len(got))
	}
}
