package queue

import (
	"context"
	"errors"
	"gpu-telemetry-pipeline/internal/telemetry"
	"math"
	"sync"
	"testing"
	"time"
)

func cfg() Config {
	return Config{MaxMessages: 1000, MaxBytes: 1 << 20, MaxMessageBytes: 4096, MaxSubscribers: 10, MaxSlots: 10, AckTimeout: time.Hour}
}
func broker(t *testing.T) *Broker {
	t.Helper()
	b, e := New(cfg())
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(b.Close)
	return b
}
func msg(n int64) telemetry.Envelope {
	return telemetry.Envelope{Key: telemetry.Key{Session: "0123456789abcdef0123456789abcdef", Sequence: n}, Telemetry: telemetry.Raw{Timestamp: "2026-10-30T10:00:00Z", UUID: "GPU-a", MetricName: "util", Value: "1"}}
}
func snapshot(t *testing.T, b *Broker) Stats {
	t.Helper()
	s, e := b.Snapshot(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func delivery(t *testing.T, s Subscription) Delivery {
	t.Helper()
	select {
	case d := <-s.Deliveries:
		return d
	case <-time.After(time.Second):
		t.Fatal("no delivery")
		return Delivery{}
	}
}
func TestAcceptanceDeduplicationAndCapacity(t *testing.T) {
	c := cfg()
	c.MaxMessages = 1
	b, e := New(c)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	ctx := context.Background()
	if d, e := b.Enqueue(ctx, msg(1)); d || e != nil {
		t.Fatal(d, e)
	}
	if d, e := b.Enqueue(ctx, msg(1)); !d || e != nil {
		t.Fatal(d, e)
	}
	changed := msg(1)
	changed.Telemetry.Value = "2"
	if duplicate, err := b.Enqueue(ctx, changed); !duplicate || err != nil {
		t.Fatal("duplicate identity must reuse the existing entry", duplicate, err)
	}
	if _, e = b.Enqueue(ctx, msg(2)); !errors.Is(e, ErrFull) {
		t.Fatal(e)
	}
	s := snapshot(t, b)
	if s.Ready != 1 || s.InFlight != 0 || s.Accepted != 1 || s.Duplicates != 2 {
		t.Fatal(s)
	}
	if e = b.Complete(ctx, 0, msg(1).Key, 0, Ack); !errors.Is(e, ErrStale) {
		t.Fatal("unassigned message completed", e)
	}
	sub, e := b.Subscribe(ctx, 1)
	if e != nil {
		t.Fatal(e)
	}
	d := delivery(t, sub)
	if d.Envelope.Telemetry.Value != "1" {
		t.Fatal("duplicate identity replaced the original payload")
	}
	if e = b.Complete(ctx, sub.ID, d.Envelope.Key, d.Tag, Ack); e != nil {
		t.Fatal(e)
	}
	if e = b.Complete(ctx, sub.ID, d.Envelope.Key, d.Tag, Ack); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	if snapshot(t, b).Bytes != 0 {
		t.Fatal("capacity leaked")
	}
	// Completed-key history is intentionally absent, but tags must never be reused.
	if _, e = b.Enqueue(ctx, msg(1)); e != nil {
		t.Fatal(e)
	}
	again := delivery(t, sub)
	if again.Tag == d.Tag {
		t.Fatal("tag reused")
	}
	if e = b.Complete(ctx, sub.ID, d.Envelope.Key, d.Tag, Ack); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
}
func TestRoundRobinHonorsSlotsAndSlowSubscriber(t *testing.T) {
	b := broker(t)
	ctx := context.Background()
	a, _ := b.Subscribe(ctx, 2)
	other, _ := b.Subscribe(ctx, 1)
	for i := int64(1); i <= 5; i++ {
		if _, e := b.Enqueue(ctx, msg(i)); e != nil {
			t.Fatal(e)
		}
	}
	s := snapshot(t, b)
	if s.InFlight != 3 || s.Ready != 2 {
		t.Fatal(s)
	}
	a1 := delivery(t, a)
	a2 := delivery(t, a)
	o1 := delivery(t, other)
	if a1.Envelope.Key.Sequence != 1 || o1.Envelope.Key.Sequence != 2 || a2.Envelope.Key.Sequence != 3 {
		t.Fatal(a1, a2, o1)
	}
	if e := b.Complete(ctx, a.ID, a1.Envelope.Key, a1.Tag, Ack); e != nil {
		t.Fatal(e)
	}
	next := delivery(t, a)
	if next.Envelope.Key.Sequence != 4 {
		t.Fatal(next)
	}
	if s = snapshot(t, b); s.InFlight != 3 || s.Ready != 1 {
		t.Fatal(s)
	}
	// Disconnect the slow collector; only its assignments return to the common queue.
	if e := b.Disconnect(ctx, other.ID); e != nil {
		t.Fatal(e)
	}
	s = snapshot(t, b)
	if s.InFlight != 2 || s.Ready != 2 || s.Redelivered != 1 {
		t.Fatal(s)
	}
	if e := b.Complete(ctx, other.ID, o1.Envelope.Key, o1.Tag, Ack); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
}
func TestConcurrentProducersAndTimeoutRecovery(t *testing.T) {
	b := broker(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := int64(1); i <= 100; i++ {
		wg.Add(1)
		go func(n int64) {
			defer wg.Done()
			if _, e := b.Enqueue(ctx, msg(n)); e != nil {
				t.Error(e)
			}
		}(i)
	}
	wg.Wait()
	sub, _ := b.Subscribe(ctx, 3)
	first := delivery(t, sub)
	_ = delivery(t, sub)
	_ = delivery(t, sub)
	_, e := request(ctx, b, func(s *state) bool { s.expire(time.Now().Add(4 * time.Hour)); return true })
	if e != nil {
		t.Fatal(e)
	}
	replacement, _ := b.Subscribe(ctx, 3)
	_ = delivery(t, replacement)
	if e = b.Complete(ctx, sub.ID, first.Envelope.Key, first.Tag, Ack); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	s := snapshot(t, b)
	if s.Messages != 100 || s.InFlight != 3 || s.Redelivered != 3 {
		t.Fatal(s)
	}
}
func TestTagOverflowDoesNotReserveWork(t *testing.T) {
	b := broker(t)
	ctx := context.Background()
	_, _ = request(ctx, b, func(s *state) bool { s.tags.Last = math.MaxInt64; return true })
	sub, _ := b.Subscribe(ctx, 1)
	if _, e := b.Enqueue(ctx, msg(1)); e != nil {
		t.Fatal(e)
	}
	select {
	case e := <-b.Failure():
		if !errors.Is(e, telemetry.ErrSequenceExhausted) {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("no overflow failure")
	}
	s := snapshot(t, b)
	if s.Ready != 1 || s.InFlight != 0 || !s.Draining {
		t.Fatal(s)
	}
	if len(sub.Deliveries) != 0 {
		t.Fatal("overflow dispatched work")
	}
}
func TestByteLimitsDrainAndSubscriberValidation(t *testing.T) {
	c := cfg()
	c.MaxBytes = int64(msg(1).Size())
	c.MaxMessageBytes = int(c.MaxBytes)
	b, e := New(c)
	if e != nil {
		t.Fatal(e)
	}
	defer b.Close()
	ctx := context.Background()
	if _, e = b.Subscribe(ctx, 0); !errors.Is(e, ErrSlots) {
		t.Fatal(e)
	}
	if _, e = b.Subscribe(ctx, 11); !errors.Is(e, ErrSlots) {
		t.Fatal(e)
	}
	if _, e = b.Enqueue(ctx, msg(1)); e != nil {
		t.Fatal(e)
	}
	if _, e = b.Enqueue(ctx, msg(2)); !errors.Is(e, ErrFull) {
		t.Fatal(e)
	}
	_, _ = request(ctx, b, func(s *state) bool { s.stats.Draining = true; return true })
	if _, e = b.Enqueue(ctx, msg(2)); !errors.Is(e, ErrDraining) {
		t.Fatal(e)
	}
	if _, e = b.Enqueue(ctx, msg(1)); e != nil {
		t.Fatal("existing retry must work during drain", e)
	}
	sub, _ := b.Subscribe(ctx, 1)
	d := delivery(t, sub)
	if e = b.Complete(ctx, sub.ID, d.Envelope.Key, d.Tag, Reject); e != nil {
		t.Fatal(e)
	}
	if e = b.Drain(ctx); e != nil {
		t.Fatal(e)
	}
	if s := snapshot(t, b); s.Messages != 0 || s.Bytes != 0 || s.Rejected != 1 {
		t.Fatal(s)
	}
}
