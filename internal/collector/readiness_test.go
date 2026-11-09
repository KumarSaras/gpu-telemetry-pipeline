package collector

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	pb "gpu-telemetry-pipeline/api/queue/v1"
	"gpu-telemetry-pipeline/internal/testsupport"
)

type readinessStore struct {
	Store
	check func(context.Context) error
}

func (s readinessStore) Ready(ctx context.Context) error { return s.check(ctx) }

func TestRunChecksStorageBeforeInitialSubscriptionAndReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	checks := make(chan context.Context, 4)
	initialReady, reconnectReady, disconnect := make(chan struct{}), make(chan struct{}), make(chan struct{})
	connected := make(chan struct{}, 2)
	var checkCalls, subscriptions atomic.Int32
	store := readinessStore{Store: &blockedStore{}, check: func(ctx context.Context) error {
		n := checkCalls.Add(1)
		checks <- ctx
		switch n {
		case 1:
			return errors.New("telemetry table not created")
		case 2:
			select {
			case <-initialReady:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		case 3:
			select {
			case <-reconnectReady:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		default:
			t.Error("unexpected storage check")
			return context.Canceled
		}
	}}
	client := &scriptedClient{open: func(ctx context.Context) *scriptedStream {
		n := subscriptions.Add(1)
		return &scriptedStream{ctx: ctx, receive: func() (*pb.Delivery, error) {
			connected <- struct{}{}
			if n == 1 {
				select {
				case <-disconnect:
					return nil, errors.New("connection lost")
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}}
	}}
	c := &Collector{Client: client, Store: store, Slots: 1, ProcessTimeout: time.Second, SendTimeout: time.Second, Log: testsupport.Logger()}
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	nextCheck := func() context.Context {
		t.Helper()
		select {
		case operation := <-checks:
			if _, ok := operation.Deadline(); !ok {
				t.Fatal("storage check has no deadline")
			}
			return operation
		case <-time.After(3 * time.Second):
			t.Fatal("storage check did not run")
			return nil
		}
	}
	first, second := nextCheck(), nextCheck()
	if subscriptions.Load() != 0 || c.Ready.Load() || !errors.Is(first.Err(), context.Canceled) {
		t.Fatal("subscribed before table availability, or leaked check context")
	}
	close(initialReady)
	waitSignal(t, connected)
	if subscriptions.Load() != 1 || !c.Ready.Load() || !errors.Is(second.Err(), context.Canceled) {
		t.Fatal("storage readiness did not enable subscription or release its context")
	}
	close(disconnect)
	third := nextCheck()
	if subscriptions.Load() != 1 || c.Ready.Load() {
		t.Fatal("reconnected before storage recheck succeeded")
	}
	close(reconnectReady)
	waitSignal(t, connected)
	if subscriptions.Load() != 2 || !c.Ready.Load() || !errors.Is(third.Err(), context.Canceled) {
		t.Fatal("did not recover after storage became ready")
	}
	cancel()
	if err := sessionResult(t, done); !errors.Is(err, context.Canceled) || c.Ready.Load() {
		t.Fatal("shutdown left collector running or ready", err)
	}
}

func TestRunStorageCheckDeadlineRetriesAndShutdownCancelsWaiting(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		name := "shutdown during check"
		if deadline {
			name = "check timeout then retry"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			entered := make(chan struct{}, 2)
			failures := make(chan error, 2)
			var subscriptions, checks atomic.Int32
			store := readinessStore{check: func(ctx context.Context) error {
				first := checks.Add(1) == 1
				select {
				case entered <- struct{}{}:
				case <-ctx.Done():
					return ctx.Err()
				}
				<-ctx.Done()
				if first {
					failures <- ctx.Err()
				}
				return ctx.Err()
			}}
			client := &scriptedClient{open: func(ctx context.Context) *scriptedStream {
				subscriptions.Add(1)
				return &scriptedStream{ctx: ctx}
			}}
			timeout := 3 * time.Second
			if deadline {
				timeout = 30 * time.Millisecond
			}
			c := &Collector{Client: client, Store: store, Slots: 1, ProcessTimeout: time.Second, SendTimeout: timeout, Log: testsupport.Logger()}
			done := make(chan error, 1)
			go func() { done <- c.Run(ctx) }()
			waitSignal(t, entered)
			if deadline {
				waitSignal(t, entered)
				if err := <-failures; !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("check was not bounded by its deadline", err)
				}
			}
			cancel()
			if err := sessionResult(t, done); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			if c.Ready.Load() || subscriptions.Load() != 0 {
				t.Fatal("unavailable storage allowed subscription/readiness")
			}
		})
	}
}
