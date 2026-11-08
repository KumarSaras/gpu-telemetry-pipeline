package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"gpu-telemetry-pipeline/internal/telemetry"
)

type readerFuncs struct {
	list  func(context.Context) ([]string, error)
	query func(context.Context, string, telemetry.Bounds) (telemetry.Cursor, error)
}

func (r *readerFuncs) ListGPUs(ctx context.Context) ([]string, error) {
	if r.list != nil {
		return r.list(ctx)
	}
	return nil, nil
}
func (r *readerFuncs) Query(ctx context.Context, id string, b telemetry.Bounds) (telemetry.Cursor, error) {
	return r.query(ctx, id, b)
}

type streamRows struct {
	items       []telemetry.Record
	index       int
	recordError error
	onNext      func()
	closed      bool
}

func (r *streamRows) Next() bool {
	if r.onNext != nil {
		r.onNext()
	}
	r.index++
	return r.index <= len(r.items)
}
func (r *streamRows) Record() (telemetry.Record, error) {
	if r.recordError != nil {
		return telemetry.Record{}, r.recordError
	}
	return r.items[r.index-1], nil
}
func (r *streamRows) Err() error { return nil }
func (r *streamRows) Close()     { r.closed = true }
func testGateway(repo telemetry.Reader) *http.ServeMux {
	mux, _ := New(repo, Config{QueryTimeout: time.Second, MaxQueries: 1}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return mux
}
func recoveredServe(handler http.Handler, writer http.ResponseWriter, request *http.Request) (value any) {
	defer func() { value = recover() }()
	handler.ServeHTTP(writer, request)
	return nil
}

func TestQueryAndListShareCapacityAndReleaseIt(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	first := true
	repo := &readerFuncs{list: func(context.Context) ([]string, error) {
		if first {
			first = false
			close(entered)
			<-release
		}
		return nil, nil
	}, query: func(context.Context, string, telemetry.Bounds) (telemetry.Cursor, error) { return &streamRows{}, nil }}
	mux := testGateway(repo)
	done := make(chan struct{})
	go func() {
		defer close(done)
		mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/v1/gpus", nil))
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("query did not acquire capacity")
	}
	for _, path := range []string{"/api/v1/gpus", "/api/v1/gpus/GPU-a/telemetry"} {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 503 {
			t.Fatal(path, w.Code)
		}
	}
	close(release)
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("first query did not finish")
	}
	for _, path := range []string{"/api/v1/gpus", "/api/v1/gpus/GPU-a/telemetry"} {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != 200 || !json.Valid(w.Body.Bytes()) || string(bytes.TrimSpace(w.Body.Bytes())) != "[]" {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
}
func TestInitialQueryFailureReleasesCapacity(t *testing.T) {
	failure := errors.New("database unavailable")
	fail := true
	rows := &streamRows{}
	repo := &readerFuncs{query: func(context.Context, string, telemetry.Bounds) (telemetry.Cursor, error) {
		if fail {
			return nil, failure
		}
		return rows, nil
	}}
	mux := testGateway(repo)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/gpus/GPU-a/telemetry", nil))
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
	fail = false
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/gpus/GPU-a/telemetry", nil))
	if w.Code != 200 || !rows.closed {
		t.Fatal(w.Code, rows.closed)
	}
}

type failingWriter struct {
	header        http.Header
	calls, failAt int
	body          bytes.Buffer
}

func (w *failingWriter) Header() http.Header {
	if w.header == nil {
		w.header = make(http.Header)
	}
	return w.header
}
func (w *failingWriter) WriteHeader(int) {}
func (w *failingWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls == w.failAt {
		return 0, io.ErrClosedPipe
	}
	return w.body.Write(p)
}

func TestResponseWriteAndRecordFailuresAbortAndReleaseCursorAndSlot(t *testing.T) {
	for _, test := range []struct {
		name          string
		failAt        int
		recordError   bool
		invalidNumber bool
	}{
		{"open bracket", 1, false, false}, {"first row", 2, false, false}, {"separator", 3, false, false}, {"close bracket", 5, false, false},
		{"record scan", 0, true, false}, {"invalid stored number", 0, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			record := telemetry.Record{Timestamp: time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC), UUID: "GPU-a", Value: json.Number("1")}
			if test.invalidNumber {
				record.Value = json.Number("NaN")
			}
			rows := &streamRows{items: []telemetry.Record{record, record}}
			if test.recordError {
				rows.recordError = errors.New("scan failed")
			}
			repo := &readerFuncs{query: func(context.Context, string, telemetry.Bounds) (telemetry.Cursor, error) { return rows, nil }}
			mux := testGateway(repo)
			writer := &failingWriter{failAt: test.failAt}
			if got := recoveredServe(mux, writer, httptest.NewRequest("GET", "/api/v1/gpus/GPU-a/telemetry", nil)); got != http.ErrAbortHandler {
				t.Fatal("incomplete response was successful", got)
			}
			if !rows.closed {
				t.Fatal("cursor leaked")
			}
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/gpus", nil))
			if w.Code != 200 {
				t.Fatal("aborted response leaked query slot", w.Code)
			}
		})
	}
}
func TestRequestCancellationAbortsResponseAndClosesRows(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rows := &streamRows{onNext: cancel}
	repo := &readerFuncs{query: func(context.Context, string, telemetry.Bounds) (telemetry.Cursor, error) { return rows, nil }}
	mux := testGateway(repo)
	if got := recoveredServe(mux, httptest.NewRecorder(), httptest.NewRequest("GET", "/api/v1/gpus/GPU-a/telemetry", nil).WithContext(ctx)); got != http.ErrAbortHandler || !rows.closed {
		t.Fatal(got, rows.closed)
	}
}
