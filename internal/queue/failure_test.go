package queue

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"gpu-telemetry-pipeline/internal/telemetry"
)

func TestInvalidEnvelopeAndCompletionPreserveQueueState(t *testing.T) {
	b := broker(t)
	ctx := context.Background()
	bad := msg(0)
	if _, err := b.Enqueue(ctx, bad); err == nil {
		t.Fatal("invalid key admitted")
	}
	bad = msg(1)
	bad.Telemetry.LabelsRaw = strings.Repeat("x", 5000)
	if _, err := b.Enqueue(ctx, bad); err == nil {
		t.Fatal("oversized payload admitted")
	}
	if s := snapshot(t, b); s.Messages != 0 || s.Bytes != 0 {
		t.Fatal(s)
	}
	sub, err := b.Subscribe(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Enqueue(ctx, msg(1)); err != nil {
		t.Fatal(err)
	}
	d := delivery(t, sub)
	if err = b.Complete(ctx, sub.ID, d.Envelope.Key, d.Tag, 99); err == nil {
		t.Fatal("invalid completion accepted")
	}
	if s := snapshot(t, b); s.InFlight != 1 || s.Messages != 1 {
		t.Fatal("invalid disposition released capacity", s)
	}
	if err = b.Complete(ctx, sub.ID, d.Envelope.Key, d.Tag, Ack); err != nil {
		t.Fatal(err)
	}
}
func TestCanceledAndClosedBrokerOperations(t *testing.T) {
	b := broker(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := b.Enqueue(ctx, msg(1)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := b.Subscribe(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	b.Close()
	ctx = context.Background()
	if _, err := b.Enqueue(ctx, msg(1)); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if _, err := b.Subscribe(ctx, 1); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := b.Complete(ctx, 1, msg(1).Key, 1, Ack); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	if err := b.Drain(ctx); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
}
func TestSubscriberCounterOverflowStopsAdmissionWithoutWrapping(t *testing.T) {
	b := broker(t)
	ctx := context.Background()
	_, err := request(ctx, b, func(s *state) bool { s.ids.Last = math.MaxInt64; return true })
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Subscribe(ctx, 1); !errors.Is(err, telemetry.ErrSequenceExhausted) {
		t.Fatal(err)
	}
	select {
	case err := <-b.Failure():
		if !errors.Is(err, telemetry.ErrSequenceExhausted) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("missing exhaustion failure")
	}
	if s := snapshot(t, b); !s.Draining || s.Subscribers != 0 || s.InFlight != 0 {
		t.Fatal(s)
	}
}
func TestTickerRecoversCanceledSubscriptionWithoutChangingReservation(t *testing.T) {
	c := cfg()
	c.AckTimeout = 40 * time.Millisecond
	b, err := New(c)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sub, err := b.Subscribe(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = b.Enqueue(context.Background(), msg(1)); err != nil {
		t.Fatal(err)
	}
	_ = delivery(t, sub)
	bytes := snapshot(t, b).Bytes
	cancel()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		s := snapshot(t, b)
		if s.Subscribers == 0 {
			if s.Ready != 1 || s.InFlight != 0 || s.Bytes != bytes || s.Redelivered != 1 {
				t.Fatal(s)
			}
			break
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("canceled subscription was not recovered")
		}
	}
	if _, err := New(Config{}); err == nil {
		t.Fatal("invalid broker bounds accepted")
	}
}
