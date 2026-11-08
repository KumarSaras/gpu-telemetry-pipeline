// Package transport adapts the queue to its gRPC streaming protocol.
package transport

import (
	"context"
	"errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	pb "gpu-telemetry-pipeline/api/queue/v1"
	"gpu-telemetry-pipeline/internal/queue"
	"gpu-telemetry-pipeline/internal/telemetry"
	"io"
	"log/slog"
	"time"
)

type Server struct {
	pb.UnimplementedQueueServer
	Broker     *queue.Broker
	Log        *slog.Logger
	AckTimeout time.Duration
}

func Envelope(p *pb.Envelope) (telemetry.Envelope, error) {
	if p == nil || p.Key == nil || p.Telemetry == nil {
		return telemetry.Envelope{}, errors.New("key and telemetry are required")
	}
	t := p.Telemetry
	e := telemetry.Envelope{Key: telemetry.Key{Session: p.Key.StreamerSession, Sequence: p.Key.Sequence}, Telemetry: telemetry.Raw{Timestamp: t.Timestamp, MetricName: t.MetricName, GPUID: t.GpuId, Device: t.Device, UUID: t.Uuid, ModelName: t.ModelName, Hostname: t.Hostname, Container: t.Container, Pod: t.Pod, Namespace: t.Namespace, Value: t.Value, LabelsRaw: t.LabelsRaw}}
	return e, e.Key.Validate()
}
func Wire(e telemetry.Envelope) *pb.Envelope {
	t := e.Telemetry
	return &pb.Envelope{Key: &pb.Key{StreamerSession: e.Key.Session, Sequence: e.Key.Sequence}, Telemetry: &pb.Telemetry{Timestamp: t.Timestamp, MetricName: t.MetricName, GpuId: t.GPUID, Device: t.Device, Uuid: t.UUID, ModelName: t.ModelName, Hostname: t.Hostname, Container: t.Container, Pod: t.Pod, Namespace: t.Namespace, Value: t.Value, LabelsRaw: t.LabelsRaw}}
}
func (s *Server) Produce(stream grpc.BidiStreamingServer[pb.Envelope, pb.ProduceResult]) error {
	for {
		p, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		e, err := Envelope(p)
		duplicate := false
		if err == nil {
			duplicate, err = s.Broker.Enqueue(stream.Context(), e)
		}
		result := &pb.ProduceResult{Key: p.GetKey(), Status: pb.ProduceStatus_PRODUCE_STATUS_ACCEPTED}
		switch {
		case err == nil:
			if duplicate {
				result.Status = pb.ProduceStatus_PRODUCE_STATUS_DUPLICATE
			}
		case errors.Is(err, queue.ErrFull):
			result.Status = pb.ProduceStatus_PRODUCE_STATUS_FULL
		case errors.Is(err, queue.ErrDraining):
			result.Status = pb.ProduceStatus_PRODUCE_STATUS_DRAINING
		case errors.Is(err, queue.ErrClosed):
			return status.Error(codes.Unavailable, "queue unavailable")
		case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
			return status.FromContextError(err).Err()
		default:
			result.Status = pb.ProduceStatus_PRODUCE_STATUS_INVALID
			result.Reason = err.Error()
		}
		if err := stream.Send(result); err != nil {
			return err
		}
	}
}
func (s *Server) Subscribe(stream grpc.BidiStreamingServer[pb.ConsumerFrame, pb.Delivery]) error {
	// A silent connection cannot occupy an admission slot forever.
	type firstResult struct {
		frame *pb.ConsumerFrame
		err   error
	}
	first := make(chan firstResult, 1)
	go func() { f, e := stream.Recv(); first <- firstResult{f, e} }()
	var f *pb.ConsumerFrame
	select {
	case r := <-first:
		if r.err != nil {
			return r.err
		}
		f = r.frame
	case <-time.After(10 * time.Second):
		return status.Error(codes.DeadlineExceeded, "subscription handshake timed out")
	case <-stream.Context().Done():
		return status.FromContextError(stream.Context().Err()).Err()
	}
	if f.GetSubscribe() == nil {
		return status.Error(codes.InvalidArgument, "first frame must subscribe")
	}
	sub, err := s.Broker.Subscribe(stream.Context(), int(f.GetSubscribe().Slots))
	if err != nil {
		return status.Error(codes.ResourceExhausted, err.Error())
	}
	if err := stream.SendHeader(metadata.Pairs("ack-timeout", s.AckTimeout.String())); err != nil {
		_ = s.Broker.Disconnect(context.Background(), sub.ID)
		return err
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = s.Broker.Disconnect(ctx, sub.ID)
	}()
	received := make(chan error, 1)
	sent := make(chan error, 1)
	go func() {
		for {
			frame, err := stream.Recv()
			if err != nil {
				received <- err
				return
			}
			c := frame.GetCompletion()
			if c == nil || c.Key == nil || c.DeliveryTag <= 0 {
				received <- status.Error(codes.InvalidArgument, "invalid completion")
				return
			}
			var action queue.Disposition
			switch c.Disposition {
			case pb.Disposition_DISPOSITION_ACK_PERSISTED:
				action = queue.Ack
			case pb.Disposition_DISPOSITION_RETRY:
				action = queue.Retry
			case pb.Disposition_DISPOSITION_REJECT:
				action = queue.Reject
			default:
				received <- status.Error(codes.InvalidArgument, "invalid disposition")
				return
			}
			err = s.Broker.Complete(sub.Context, sub.ID, telemetry.Key{Session: c.Key.StreamerSession, Sequence: c.Key.Sequence}, c.DeliveryTag, action)
			if err != nil && !errors.Is(err, queue.ErrStale) {
				received <- err
				return
			}
			if action == queue.Reject {
				s.Log.Warn("delivery rejected", "session", c.Key.StreamerSession, "sequence", c.Key.Sequence, "reason", truncate(c.Reason, 256))
			}
		}
	}()
	go func() {
		for {
			select {
			case <-sub.Context.Done():
				sent <- sub.Context.Err()
				return
			case d := <-sub.Deliveries:
				if err := stream.Send(&pb.Delivery{Envelope: Wire(d.Envelope), DeliveryTag: d.Tag}); err != nil {
					sent <- err
					return
				}
			}
		}
	}()
	select {
	case err := <-received:
		if err == io.EOF {
			return nil
		}
		return err
	case err := <-sent:
		return err
	case <-sub.Context.Done():
		return status.Error(codes.Unavailable, "subscription ended; reconnect with backoff")
	}
}
func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// LimitStreams bounds active RPCs across connections, including idle streams.
func LimitStreams(max int) grpc.StreamServerInterceptor {
	slots := make(chan struct{}, max)
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
			return handler(srv, stream)
		default:
			return status.Error(codes.ResourceExhausted, "stream limit reached")
		}
	}
}
