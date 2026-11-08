// Package observe provides low-cardinality metrics and process health endpoints.
package observe

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime"
	"sort"
	"sync"
	"time"
)

type Metrics struct {
	mu     sync.Mutex
	values map[string]float64
}

func New() *Metrics { return &Metrics{values: make(map[string]float64)} }
func (m *Metrics) Add(name string, value float64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	m.values[name] += value
	m.mu.Unlock()
}
func (m *Metrics) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	m.mu.Lock()
	snapshot := make(map[string]float64, len(m.values))
	for k, v := range m.values {
		snapshot[k] = v
	}
	m.mu.Unlock()
	keys := make([]string, 0, len(snapshot))
	for k := range snapshot {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "gpu_telemetry_%s %g\n", k, snapshot[k])
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	fmt.Fprintf(w, "go_goroutines %d\ngo_memstats_heap_alloc_bytes %d\n", runtime.NumGoroutine(), mem.HeapAlloc)
}
func Health(mux *http.ServeMux, ready func(context.Context) error) {
	mux.HandleFunc("GET /livez", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), time.Second)
		defer cancel()
		if err := ready(ctx); err != nil {
			http.Error(w, "not ready", http.StatusServiceUnavailable)
			return
		}
		fmt.Fprintln(w, "ready")
	})
}
func (m *Metrics) Wrap(next http.Handler, log *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		defer func() {
			m.Add("http_requests_total", 1)
			m.Add("http_request_seconds_sum", time.Since(start).Seconds())
			if v := recover(); v != nil {
				m.Add("http_aborts_total", 1)
				if v == http.ErrAbortHandler {
					panic(v)
				}
				log.Error("HTTP handler panic")
				panic(http.ErrAbortHandler)
			}
		}()
		next.ServeHTTP(w, r)
	})
}
