package collector

import (
	"context"
	"errors"
	pb "gpu-telemetry-pipeline/api/queue/v1"
	"gpu-telemetry-pipeline/internal/streamer"
	"gpu-telemetry-pipeline/internal/telemetry"
	"gpu-telemetry-pipeline/internal/testsupport"
	"sync/atomic"
	"testing"
	"time"
)

type blockedStore struct {
	entered     chan telemetry.Key
	release     chan struct{}
	active, max atomic.Int32
	calls       atomic.Int32
	err         error
}

func (s *blockedStore) Insert(ctx context.Context, k telemetry.Key, _ telemetry.Record) (bool, error) {
	n := s.active.Add(1)
	defer s.active.Add(-1)
	for old := s.max.Load(); n > old; old = s.max.Load() {
		if s.max.CompareAndSwap(old, n) {
			break
		}
	}
	s.calls.Add(1)
	if s.entered != nil {
		select {
		case s.entered <- k:
		case <-ctx.Done():
			return false, ctx.Err()
		}
		select {
		case <-s.release:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return true, s.err
}
func envelope(n int64) telemetry.Envelope {
	return telemetry.Envelope{Key: telemetry.Key{Session: "0123456789abcdef0123456789abcdef", Sequence: n}, Telemetry: telemetry.Raw{Timestamp: "2026-10-30T10:00:00Z", UUID: "GPU-a", MetricName: "util", Value: "1"}}
}
func TestConfigurableSlotsKeepExactlyOneWorker(t *testing.T) {
	b, client := testsupport.Queue(t)
	store := &blockedStore{entered: make(chan telemetry.Key, 6), release: make(chan struct{}, 6)}
	worker := &Collector{Client: client, Store: store, Slots: 3, ProcessTimeout: time.Second, SendTimeout: time.Second, Log: testsupport.Logger()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	producer := &streamer.GRPCProducer{Client: client, Timeout: time.Second}
	defer producer.Close()
	for i := int64(1); i <= 6; i++ {
		if e := producer.Send(ctx, envelope(i)); e != nil {
			t.Fatal(e)
		}
	}
	select {
	case <-store.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not start")
	}
	testsupport.Eventually(t, func() bool { s, _ := b.Snapshot(ctx); return s.InFlight == 3 && s.Ready == 3 })
	if store.calls.Load() != 1 || store.max.Load() != 1 {
		t.Fatal("more than one processing worker")
	}
	for range 6 {
		store.release <- struct{}{}
	}
	testsupport.Eventually(t, func() bool { s, _ := b.Snapshot(ctx); return s.Completed == 6 })
	if store.max.Load() != 1 {
		t.Fatal("processing parallelism changed")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not stop")
	}
}
func TestCommitFailureNeverAcknowledgesSuccess(t *testing.T) {
	s := &blockedStore{err: errors.New("database unavailable")}
	c := &Collector{Store: s, ProcessTimeout: time.Second, Log: testsupport.Logger()}
	action, _ := c.process(context.Background(), envelope(1))
	if action != pb.Disposition_DISPOSITION_RETRY {
		t.Fatal(action)
	}
	bad := envelope(1)
	bad.Telemetry.Value = "NaN"
	action, _ = c.process(context.Background(), bad)
	if action != pb.Disposition_DISPOSITION_REJECT {
		t.Fatal(action)
	}
}
