package collector

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	pb "gpu-telemetry-pipeline/api/queue/v1"
	"gpu-telemetry-pipeline/internal/queue/transport"
	"gpu-telemetry-pipeline/internal/telemetry"
	"gpu-telemetry-pipeline/internal/testsupport"
)

// The scripted stream honors its context while allowing deterministic failures
// and blocked sends that are difficult to force through a buffered transport.
type scriptedClient struct {
	pb.QueueClient
	open func(context.Context) *scriptedStream
}

func (c *scriptedClient) Subscribe(ctx context.Context, _ ...grpc.CallOption) (pb.Queue_SubscribeClient, error) {
	return c.open(ctx), nil
}

type scriptedStream struct {
	grpc.ClientStream
	ctx     context.Context
	header  func() (metadata.MD, error)
	receive func() (*pb.Delivery, error)
	send    func(*pb.ConsumerFrame) error
}

func (s *scriptedStream) Context() context.Context { return s.ctx }
func (s *scriptedStream) Header() (metadata.MD, error) {
	if s.header != nil {
		return s.header()
	}
	return metadata.Pairs("ack-timeout", "30s"), nil
}
func (s *scriptedStream) Send(frame *pb.ConsumerFrame) error {
	if s.send != nil {
		return s.send(frame)
	}
	return nil
}
func (s *scriptedStream) Recv() (*pb.Delivery, error) {
	if s.receive != nil {
		return s.receive()
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

type storeFunc func(context.Context, telemetry.Key, telemetry.Record) (bool, error)

func (f storeFunc) Insert(ctx context.Context, key telemetry.Key, record telemetry.Record) (bool, error) {
	return f(ctx, key, record)
}

func runSession(c *Collector, ctx context.Context) <-chan error {
	done := make(chan error, 1)
	go func() { done <- c.session(ctx) }()
	return done
}
func sessionResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("session did not finish")
		return nil
	}
}
func waitSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(3 * time.Second):
		t.Fatal("expected lifecycle event")
	}
}
func wireDelivery(sequence int64) *pb.Delivery {
	return &pb.Delivery{Envelope: transport.Wire(envelope(sequence)), DeliveryTag: sequence}
}

func TestHandshakeCancellationAndTimeout(t *testing.T) {
	for _, shutdown := range []bool{true, false} {
		name := "timeout"
		if shutdown {
			name = "service shutdown"
		}
		t.Run(name, func(t *testing.T) {
			serviceCtx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan struct{})
			client := &scriptedClient{open: func(ctx context.Context) *scriptedStream {
				return &scriptedStream{ctx: ctx, header: func() (metadata.MD, error) { close(entered); <-ctx.Done(); return nil, ctx.Err() }}
			}}
			c := &Collector{Client: client, Slots: 1, ProcessTimeout: time.Second, SendTimeout: 100 * time.Millisecond, Log: testsupport.Logger()}
			done := runSession(c, serviceCtx)
			waitSignal(t, entered)
			want := error(context.DeadlineExceeded)
			if shutdown {
				cancel()
				want = context.Canceled
			}
			if err := sessionResult(t, done); !errors.Is(err, want) {
				t.Fatalf("got %v, want %v", err, want)
			}
			if c.Ready.Load() {
				t.Fatal("failed handshake marked ready")
			}
		})
	}
}

func TestShutdownFinishesCurrentCommitAndAckWithoutDrainingPrefetch(t *testing.T) {
	serviceCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan context.Context, 1)
	release := make(chan struct{})
	var calls, received atomic.Int32
	store := storeFunc(func(ctx context.Context, _ telemetry.Key, _ telemetry.Record) (bool, error) {
		calls.Add(1)
		entered <- ctx
		select {
		case <-release:
			return true, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	})
	completed := make(chan *pb.Completion, 1)
	client := &scriptedClient{open: func(ctx context.Context) *scriptedStream {
		pending := make(chan *pb.Delivery, 3)
		for i := int64(1); i <= 3; i++ {
			pending <- wireDelivery(i)
		}
		return &scriptedStream{ctx: ctx, receive: func() (*pb.Delivery, error) {
			select {
			case d := <-pending:
				received.Add(1)
				return d, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}, send: func(frame *pb.ConsumerFrame) error {
			if completion := frame.GetCompletion(); completion != nil {
				completed <- completion
			}
			return nil
		}}
	}}
	c := &Collector{Client: client, Store: store, Slots: 3, ProcessTimeout: time.Second, SendTimeout: time.Second, Log: testsupport.Logger()}
	done := runSession(c, serviceCtx)
	var operationCtx context.Context
	select {
	case operationCtx = <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("processing did not start")
	}
	testsupport.Eventually(t, func() bool { return received.Load() == 3 })
	cancel()
	if operationCtx.Err() != nil {
		t.Fatal("normal shutdown canceled the current database operation")
	}
	close(release)
	if err := sessionResult(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case completion := <-completed:
		if completion.Key.Sequence != 1 || completion.Disposition != pb.Disposition_DISPOSITION_ACK_PERSISTED {
			t.Fatal(completion)
		}
	default:
		t.Fatal("committed operation was not acknowledged")
	}
	if calls.Load() != 1 || c.Ready.Load() {
		t.Fatal("prefetch drained or readiness retained", calls.Load())
	}
}

func TestReceiveFailureCancelsProcessingAndPreservesCause(t *testing.T) {
	serviceCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, failReceive, operationCanceled := make(chan struct{}), make(chan struct{}), make(chan struct{})
	failure := status.Error(codes.Unavailable, "simulated subscription receive failure")
	store := storeFunc(func(ctx context.Context, _ telemetry.Key, _ telemetry.Record) (bool, error) {
		close(entered)
		<-ctx.Done()
		close(operationCanceled)
		return false, ctx.Err()
	})
	var completions atomic.Int32
	client := &scriptedClient{open: func(ctx context.Context) *scriptedStream {
		first := true
		return &scriptedStream{ctx: ctx, receive: func() (*pb.Delivery, error) {
			if first {
				first = false
				return wireDelivery(1), nil
			}
			select {
			case <-failReceive:
				return nil, failure
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}, send: func(frame *pb.ConsumerFrame) error {
			if frame.GetCompletion() != nil {
				completions.Add(1)
			}
			return nil
		}}
	}}
	c := &Collector{Client: client, Store: store, Slots: 1, ProcessTimeout: time.Second, SendTimeout: time.Second, Log: testsupport.Logger()}
	done := runSession(c, serviceCtx)
	waitSignal(t, entered)
	close(failReceive)
	if err := sessionResult(t, done); !errors.Is(err, failure) {
		t.Fatalf("receive failure masked: %v", err)
	}
	waitSignal(t, operationCanceled)
	if completions.Load() != 0 || c.Ready.Load() {
		t.Fatal("failed session sent a completion or remained ready")
	}
}

func TestCompletionSendIsBoundedAndSerialized(t *testing.T) {
	serviceCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, exited := make(chan struct{}), make(chan struct{})
	var calls, sendCalls, active atomic.Int32
	store := storeFunc(func(context.Context, telemetry.Key, telemetry.Record) (bool, error) { calls.Add(1); return true, nil })
	client := &scriptedClient{open: func(ctx context.Context) *scriptedStream {
		pending := make(chan *pb.Delivery, 2)
		pending <- wireDelivery(1)
		pending <- wireDelivery(2)
		return &scriptedStream{ctx: ctx, receive: func() (*pb.Delivery, error) {
			select {
			case d := <-pending:
				return d, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}, send: func(frame *pb.ConsumerFrame) error {
			if frame.GetSubscribe() != nil {
				return nil
			}
			sendCalls.Add(1)
			active.Add(1)
			defer active.Add(-1)
			defer close(exited)
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}}
	}}
	c := &Collector{Client: client, Store: store, Slots: 2, ProcessTimeout: time.Second, SendTimeout: 100 * time.Millisecond, Log: testsupport.Logger()}
	done := runSession(c, serviceCtx)
	waitSignal(t, entered)
	cancel() // The bounded current completion may finish despite service shutdown.
	if err := sessionResult(t, done); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	waitSignal(t, exited)
	testsupport.Eventually(t, func() bool { return active.Load() == 0 })
	if sendCalls.Load() != 1 || calls.Load() != 1 || c.Ready.Load() {
		t.Fatal("parallel sends, drained prefetch, or stale readiness", sendCalls.Load(), calls.Load())
	}
}

func TestIncompatibleAcknowledgementBudgetFailsHandshake(t *testing.T) {
	client := &scriptedClient{open: func(ctx context.Context) *scriptedStream {
		return &scriptedStream{ctx: ctx, header: func() (metadata.MD, error) { return metadata.Pairs("ack-timeout", "2s"), nil }}
	}}
	c := &Collector{Client: client, Slots: 1, ProcessTimeout: time.Second, SendTimeout: time.Second, Log: testsupport.Logger()}
	if err := c.session(context.Background()); !errors.Is(err, ErrTimeoutConfiguration) {
		t.Fatal(err)
	}
	if c.Ready.Load() {
		t.Fatal("incompatible subscription marked ready")
	}
}

func TestStoreDuplicateAcknowledgesPersisted(t *testing.T) {
	c := &Collector{Store: storeFunc(func(context.Context, telemetry.Key, telemetry.Record) (bool, error) { return false, nil }), ProcessTimeout: time.Second, Log: testsupport.Logger()}
	action, reason := c.process(context.Background(), envelope(1))
	if action != pb.Disposition_DISPOSITION_ACK_PERSISTED || reason != "" {
		t.Fatal(action, reason)
	}
}

type logSink chan string

func (sink logSink) Write(p []byte) (int, error) { sink <- string(p); return len(p), nil }

func TestRunLogsOriginalReceiveFailure(t *testing.T) {
	serviceCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	logs := make(logSink, 4)
	failure := status.Error(codes.Unavailable, "meaningful receive failure")
	client := &scriptedClient{open: func(ctx context.Context) *scriptedStream {
		return &scriptedStream{ctx: ctx, receive: func() (*pb.Delivery, error) { return nil, failure }}
	}}
	c := &Collector{Client: client, Slots: 1, ProcessTimeout: time.Second, SendTimeout: time.Second, Log: slog.New(slog.NewJSONHandler(logs, nil))}
	done := make(chan error, 1)
	go func() { done <- c.Run(serviceCtx) }()
	select {
	case message := <-logs:
		if !strings.Contains(message, "meaningful receive failure") || !strings.Contains(message, "subscription reconnecting") {
			t.Fatal(message)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("missing reconnect diagnosis")
	}
	cancel()
	if err := sessionResult(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
