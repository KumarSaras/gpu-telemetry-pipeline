package observe

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestConcurrentMetricsAndReadiness(t *testing.T) {
	m := New()
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				m.Add("records_total", 1)
			}
		}()
	}
	wg.Wait()
	w := httptest.NewRecorder()
	m.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if !strings.Contains(w.Body.String(), "gpu_telemetry_records_total 1000") {
		t.Fatal(w.Body.String())
	}
	mux := http.NewServeMux()
	Health(mux, func(context.Context) error { return errors.New("down") })
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/readyz", nil))
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/livez", nil))
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
}

func TestWrapCountsHealthAndMetricsAfterResponse(t *testing.T) {
	m := New()
	mux := http.NewServeMux()
	Health(mux, func(context.Context) error { return errors.New("dependency unavailable") })
	mux.Handle("GET /metrics", m)
	handler := m.Wrap(mux, slog.New(slog.NewTextHandler(io.Discard, nil)))
	request := func(path string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		return w
	}
	first := request("/metrics")
	if first.Code != 200 || strings.Contains(first.Body.String(), "gpu_telemetry_http_requests_total") {
		t.Fatal("scrape counted before response finished", first.Body.String())
	}
	second := request("/metrics")
	if !strings.Contains(second.Body.String(), "gpu_telemetry_http_requests_total 1\n") {
		t.Fatal(second.Body.String())
	}
	if request("/livez").Code != 200 || request("/readyz").Code != 503 {
		t.Fatal("middleware changed health status")
	}
	last := request("/metrics")
	if !strings.Contains(last.Body.String(), "gpu_telemetry_http_requests_total 4\n") || !strings.Contains(last.Body.String(), "gpu_telemetry_http_request_seconds_sum ") {
		t.Fatal(last.Body.String())
	}
}

func TestWrapCountsPanicsAndPreservesAbort(t *testing.T) {
	for _, panicValue := range []any{"handler failure", http.ErrAbortHandler} {
		m := New()
		handler := m.Wrap(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic(panicValue) }), slog.New(slog.NewTextHandler(io.Discard, nil)))
		caught := func() (value any) {
			defer func() { value = recover() }()
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/panic", nil))
			return nil
		}()
		if caught != http.ErrAbortHandler {
			t.Fatal("abort contract changed", caught)
		}
		w := httptest.NewRecorder()
		m.ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
		for _, metric := range []string{"gpu_telemetry_http_aborts_total 1\n", "gpu_telemetry_http_requests_total 1\n"} {
			if !strings.Contains(w.Body.String(), metric) {
				t.Fatal(w.Body.String())
			}
		}
	}
}
