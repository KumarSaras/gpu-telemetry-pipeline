package platform

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

func waitResult(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(3 * time.Second):
		t.Fatal("operation did not finish")
		return nil
	}
}
func startHTTP(t *testing.T, handler http.Handler) (*http.Server, <-chan error, string) {
	t.Helper()
	var logs bytes.Buffer
	srv, done, err := HTTP("127.0.0.1:0", handler, time.Second, slog.New(slog.NewJSONHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ShutdownHTTP(srv, time.Second) })
	var message struct {
		Address string `json:"address"`
	}
	if err := json.Unmarshal(logs.Bytes(), &message); err != nil || message.Address == "" {
		t.Fatal(logs.String(), err)
	}
	return srv, done, "http://" + message.Address
}
func TestHTTPServesAndShutsDownCleanly(t *testing.T) {
	srv, done, url := startHTTP(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, r.URL.Path) }))
	client := &http.Client{Timeout: time.Second}
	response, err := client.Get(url + "/livez")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil || response.StatusCode != 200 || string(body) != "/livez" {
		t.Fatal(string(body), err)
	}
	ShutdownHTTP(srv, time.Second)
	if err := waitResult(t, done); err != nil {
		t.Fatal(err)
	}
}
func TestHTTPBindFailure(t *testing.T) {
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	srv, done, err := HTTP(occupied.Addr().String(), http.NotFoundHandler(), time.Second, slog.Default())
	if err == nil || srv != nil || done != nil {
		t.Fatal(srv, done, err)
	}
}
func TestShutdownForcesBlockedRequestsClosedAfterGrace(t *testing.T) {
	entered := make(chan struct{})
	handlerDone := make(chan struct{})
	srv, done, url := startHTTP(t, http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done(); close(handlerDone) }))
	requestDone := make(chan error, 1)
	go func() {
		response, err := http.Get(url)
		if response != nil {
			response.Body.Close()
		}
		requestDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not start")
	}
	ShutdownHTTP(srv, 10*time.Millisecond)
	if err := waitResult(t, done); err != nil {
		t.Fatal(err)
	}
	if err := waitResult(t, requestDone); err == nil {
		t.Fatal("blocked request unexpectedly succeeded")
	}
	select {
	case <-handlerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("forced close failed to cancel handler")
	}
}
func TestManagedRunReturnsWorkerOutcome(t *testing.T) {
	failure := errors.New("worker failure")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := ManagedRun(cancel, func() error { return failure }, make(chan error), time.Second); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	if ctx.Err() != nil {
		t.Fatal("worker outcome unnecessarily canceled service")
	}
}
func TestManagedRunCancelsWorkerAndPreservesListenerFailure(t *testing.T) {
	for _, listenerError := range []error{nil, errors.New("listener failed")} {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		errorsChannel := make(chan error, 1)
		errorsChannel <- listenerError
		err := ManagedRun(cancel, func() error { <-ctx.Done(); return ctx.Err() }, errorsChannel, time.Second)
		if ctx.Err() == nil {
			t.Fatal("management failure did not stop worker")
		}
		if listenerError != nil {
			if !errors.Is(err, listenerError) {
				t.Fatal(err)
			}
		} else if err == nil || !strings.Contains(err.Error(), "management listener stopped") {
			t.Fatal(err)
		}
	}
}
func TestManagedRunBoundsAnUnresponsiveWorker(t *testing.T) {
	failure := errors.New("HTTP listener failed")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release, exited := make(chan struct{}), make(chan struct{})
	errorsChannel := make(chan error, 1)
	errorsChannel <- failure
	err := ManagedRun(cancel, func() error { defer close(exited); <-release; return nil }, errorsChannel, 10*time.Millisecond)
	close(release)
	select {
	case <-exited:
	case <-time.After(3 * time.Second):
		t.Fatal("test worker leaked")
	}
	if !errors.Is(err, failure) || !strings.Contains(err.Error(), "worker shutdown deadline exceeded") || ctx.Err() == nil {
		t.Fatal(err)
	}
}
func TestDialMakesPlaintextCallsAndEnforcesFrameLimit(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	healthpb.RegisterHealthServer(server, health.NewServer())
	go server.Serve(listener)
	t.Cleanup(func() { server.Stop(); listener.Close() })
	conn, err := Dial(listener.Addr().String(), 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	client := healthpb.NewHealthClient(conn)
	response, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil || response.Status != healthpb.HealthCheckResponse_SERVING {
		t.Fatal(response, err)
	}
	if _, err = client.Check(ctx, &healthpb.HealthCheckRequest{Service: strings.Repeat("x", 2048)}); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("oversized send accepted", err)
	}
}
func TestPauseHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Pause(ctx, 100); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}
