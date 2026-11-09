// Package collector processes deliveries with exactly one worker per instance.
package collector

import (
	"context"
	"errors"
	"fmt"
	pb "gpu-telemetry-pipeline/api/queue/v1"
	"gpu-telemetry-pipeline/internal/observe"
	"gpu-telemetry-pipeline/internal/platform"
	"gpu-telemetry-pipeline/internal/queue/transport"
	"gpu-telemetry-pipeline/internal/telemetry"
	"log/slog"
	"sync/atomic"
	"time"
)

// Store persists one result per stable transport key.
type Store interface {
	// Ready checks that telemetry storage is available before subscribing.
	Ready(context.Context) error
	// Insert returns true for a newly committed row, or false when the key
	// already exists. Duplicate keys preserve the original row without
	// comparing payload content. An error must not be acknowledged as persisted.
	Insert(context.Context, telemetry.Key, telemetry.Record) (bool, error)
}
type Collector struct {
	Client pb.QueueClient
	Store  Store
	// Slots counts outstanding deliveries: the one processing plus prefetch.
	// It does not change the number of processing workers.
	Slots          int
	ProcessTimeout time.Duration
	Log            *slog.Logger
	SendTimeout    time.Duration
	Ready          atomic.Bool
	Metrics        *observe.Metrics
}

var ErrTimeoutConfiguration = errors.New("ACK_TIMEOUT must exceed PROCESS_TIMEOUT + REQUEST_TIMEOUT")

func (c *Collector) Run(ctx context.Context) error {
	if c.SendTimeout <= 0 {
		c.SendTimeout = 5 * time.Second
	}
	if c.Slots < 1 || c.Slots > 4096 || c.ProcessTimeout <= 0 {
		return errors.New("invalid collector slots or processing timeout")
	}
	for attempt := 0; ; attempt = min(attempt+1, 6) {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		checkctx, cancel := context.WithTimeout(ctx, c.SendTimeout)
		err := c.Store.Ready(checkctx)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			c.Log.Warn("waiting for telemetry storage", "error", err)
		} else {
			err = c.session(ctx)
			if errors.Is(err, ErrTimeoutConfiguration) {
				return err
			}
			if ctx.Err() == nil {
				c.Log.Warn("subscription reconnecting", "error", err)
				c.Metrics.Add("collector_reconnects_total", 1)
			}
		}
		if err := platform.Pause(ctx, attempt); err != nil {
			return err
		}
	}
}

// subscription owns one stream and its failure lifetime. The reusable gRPC
// client connection belongs to the service, not to this subscription.
type subscription struct {
	sessionCtx context.Context
	cancel     context.CancelCauseFunc
	stream     pb.Queue_SubscribeClient
}

func (c *Collector) subscribe(serviceCtx context.Context) (_ *subscription, err error) {
	// Bridge service cancellation during the handshake only. After setup,
	// the detached session lets the current bounded processing/send step finish
	// on normal shutdown. Stream failures still cancel that operation promptly.
	sessionCtx, cancel := context.WithCancelCause(context.WithoutCancel(serviceCtx))
	defer func() {
		if err != nil {
			if cause := context.Cause(sessionCtx); cause != nil {
				err = cause
			}
			cancel(err)
		}
	}()
	stopParentCancellation := context.AfterFunc(serviceCtx, func() { cancel(context.Cause(serviceCtx)) })
	defer stopParentCancellation()
	handshake := time.AfterFunc(c.SendTimeout, func() { cancel(context.DeadlineExceeded) })
	defer handshake.Stop()
	stream, err := c.Client.Subscribe(sessionCtx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&pb.ConsumerFrame{Body: &pb.ConsumerFrame_Subscribe{Subscribe: &pb.Subscription{Slots: int32(c.Slots)}}}); err != nil {
		return nil, err
	}
	headers, err := stream.Header()
	if err != nil {
		return nil, err
	}
	values := headers.Get("ack-timeout")
	if len(values) != 1 {
		return nil, errors.New("queue omitted acknowledgement timeout")
	}
	budget, err := time.ParseDuration(values[0])
	if err != nil || budget <= c.ProcessTimeout+c.SendTimeout {
		return nil, ErrTimeoutConfiguration
	}
	if !handshake.Stop() {
		cancel(context.DeadlineExceeded)
	}
	stopParentCancellation()
	if serviceCtx.Err() != nil {
		cancel(context.Cause(serviceCtx))
	}
	if cause := context.Cause(sessionCtx); cause != nil {
		return nil, cause
	}
	return &subscription{sessionCtx: sessionCtx, cancel: cancel, stream: stream}, nil
}

func (c *Collector) session(serviceCtx context.Context) error {
	sub, err := c.subscribe(serviceCtx)
	if err != nil {
		return err
	}
	defer sub.cancel(nil)
	c.Ready.Store(true)
	defer c.Ready.Store(false)
	// Slots include the single processing worker plus this bounded prefetch.
	deliveries := make(chan *pb.Delivery, c.Slots-1)
	go func() {
		defer close(deliveries)
		for {
			delivery, err := sub.stream.Recv()
			if err != nil {
				sub.cancel(err)
				return
			}
			select {
			case deliveries <- delivery:
			case <-sub.sessionCtx.Done():
				return
			}
		}
	}()
	// Only this worker processes messages and initiates completion sends.
	for {
		if serviceCtx.Err() != nil {
			return serviceCtx.Err()
		}
		if cause := context.Cause(sub.sessionCtx); cause != nil {
			return cause
		}
		select {
		case <-serviceCtx.Done():
			return serviceCtx.Err()
		case <-sub.sessionCtx.Done():
			return context.Cause(sub.sessionCtx)
		case delivery, ok := <-deliveries:
			if !ok {
				return context.Cause(sub.sessionCtx)
			}
			// A ready delivery must not start new work after service shutdown.
			if serviceCtx.Err() != nil {
				return serviceCtx.Err()
			}
			if cause := context.Cause(sub.sessionCtx); cause != nil {
				return cause
			}
			envelope, err := transport.Envelope(delivery.Envelope)
			if err != nil || delivery.DeliveryTag <= 0 {
				return errors.New("invalid delivery")
			}
			action, reason := c.process(sub.sessionCtx, envelope)
			if cause := context.Cause(sub.sessionCtx); cause != nil {
				return cause
			}
			frame := &pb.ConsumerFrame{Body: &pb.ConsumerFrame_Completion{Completion: &pb.Completion{Key: delivery.Envelope.Key, DeliveryTag: delivery.DeliveryTag, Disposition: action, Reason: reason}}}
			if err := c.sendCompletion(sub, frame); err != nil {
				return err
			}
			if action == pb.Disposition_DISPOSITION_RETRY {
				// RETRY ends this subscription and requeues all its unfinished
				// assignments. Run backs off this collector before subscribing
				// again; it is not a global delayed-redelivery mechanism.
				return errors.New("transient persistence failure")
			}
		}
	}
}

func (c *Collector) sendCompletion(sub *subscription, frame *pb.ConsumerFrame) error {
	// A gRPC Send can stall. The worker waits for this send or session
	// cancellation before it can initiate another send on this stream.
	done := make(chan error, 1)
	go func() { done <- sub.stream.Send(frame) }()
	timer := time.NewTimer(c.SendTimeout)
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil {
			sub.cancel(err)
		}
		return context.Cause(sub.sessionCtx)
	case <-timer.C:
		sub.cancel(context.DeadlineExceeded)
		return context.Cause(sub.sessionCtx)
	case <-sub.sessionCtx.Done():
		return context.Cause(sub.sessionCtx)
	}
}

func (c *Collector) process(ctx context.Context, e telemetry.Envelope) (pb.Disposition, string) {
	started := time.Now()
	defer func() { c.Metrics.Add("collector_processing_seconds_sum", time.Since(started).Seconds()) }()
	record, err := telemetry.Parse(e.Telemetry)
	if err != nil {
		c.Metrics.Add("collector_rejected_total", 1)
		c.Log.Warn("invalid telemetry", "error", err)
		return pb.Disposition_DISPOSITION_REJECT, "invalid telemetry"
	}
	opctx, cancel := context.WithTimeout(ctx, c.ProcessTimeout)
	defer cancel()
	inserted, err := c.Store.Insert(opctx, e.Key, record)
	if err != nil {
		c.Metrics.Add("collector_commit_errors_total", 1)
		c.Log.Error("telemetry commit failed", "error", err)
		return pb.Disposition_DISPOSITION_RETRY, "persistence unavailable"
	}
	if inserted {
		c.Metrics.Add("collector_persisted_total", 1)
		c.Metrics.Add("streamer_to_persistence_seconds_sum", time.Since(record.Timestamp).Seconds())
	}
	if !inserted {
		c.Metrics.Add("collector_duplicates_total", 1)
		c.Log.Debug("duplicate telemetry", "session", e.Key.Session, "sequence", e.Key.Sequence)
	}
	return pb.Disposition_DISPOSITION_ACK_PERSISTED, ""
}
func (c *Collector) String() string { return fmt.Sprintf("single worker, %d slots", c.Slots) }
