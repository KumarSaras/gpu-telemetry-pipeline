package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"gpu-telemetry-pipeline/internal/telemetry"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type reader struct {
	bounds  telemetry.Bounds
	id      string
	records []telemetry.Record
	err     error
	closed  bool
}

func (r *reader) ListGPUs(context.Context) ([]string, error) { return []string{"GPU-a"}, r.err }
func (r *reader) Query(_ context.Context, id string, b telemetry.Bounds) (telemetry.Cursor, error) {
	r.bounds = b
	r.id = id
	return &rows{r: r}, r.err
}

type rows struct {
	r *reader
	i int
}

func (c *rows) Next() bool                        { c.i++; return c.i <= len(c.r.records) }
func (c *rows) Record() (telemetry.Record, error) { return c.r.records[c.i-1], nil }
func (c *rows) Err() error                        { return nil }
func (c *rows) Close()                            { c.r.closed = true }
func TestPublicRoutesAndSchema(t *testing.T) {
	r := &reader{records: []telemetry.Record{{Timestamp: time.Date(2026, 10, 30, 10, 0, 0, 0, time.UTC), UUID: "GPU-a", Value: json.Number("1.2300")}}}
	mux, api := New(r, Config{QueryTimeout: time.Second, MaxQueries: 1}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	for _, tc := range []struct {
		path string
		code int
	}{{"/api/v1/gpus", 200}, {"/api/v1/gpus/GPU-a/telemetry?start_time=2026-10-30T10:00:00Z", 200}, {"/api/v1/gpus/GPU-a/telemetry?end_time=bad", 400}, {"/api/v1/gpus/GPU-a/telemetry?start_time=2026-10-31T00:00:00Z&end_time=2026-10-30T00:00:00Z", 400}} {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest("GET", tc.path, nil))
		if w.Code != tc.code {
			t.Fatal(tc.path, w.Code, w.Body.String())
		}
		if w.Code == 200 && !json.Valid(w.Body.Bytes()) {
			t.Fatal(w.Body.String())
		}
	}
	if !r.closed {
		t.Fatal("cursor not closed")
	}
	schema := api.OpenAPI().Paths["/api/v1/gpus/{id}/telemetry"].Get.Responses["200"].Content["application/json"].Schema
	if schema.Type != "array" || schema.Items == nil {
		t.Fatal(schema)
	}
	b, e := Schema()
	if e != nil || !json.Valid(b) || strings.Contains(string(b), "streamer_session") {
		t.Fatal("invalid or leaked schema", e)
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/gpus/GPU-a/telemetry", nil))
	if !strings.Contains(w.Body.String(), `"value":1.2300`) {
		t.Fatal(w.Body.String())
	}
	r.err = errors.New("database unavailable")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/gpus", nil))
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
}

type failingReader struct{ closed bool }

func (*failingReader) ListGPUs(context.Context) ([]string, error) { return nil, nil }
func (r *failingReader) Query(context.Context, string, telemetry.Bounds) (telemetry.Cursor, error) {
	return &failingRows{r: r}, nil
}

type failingRows struct{ r *failingReader }

func (*failingRows) Next() bool                        { return false }
func (*failingRows) Record() (telemetry.Record, error) { return telemetry.Record{}, nil }
func (*failingRows) Err() error                        { return errors.New("connection lost during query") }
func (r *failingRows) Close()                          { r.r.closed = true }
func TestMidStreamFailureAbortsAndClosesCursor(t *testing.T) {
	repo := &failingReader{}
	mux, _ := New(repo, Config{QueryTimeout: time.Second, MaxQueries: 1}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer func() {
		if v := recover(); v != http.ErrAbortHandler {
			t.Fatalf("expected response abort, got %v", v)
		}
		if !repo.closed {
			t.Fatal("cursor leaked")
		}
	}()
	mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/v1/gpus/GPU-a/telemetry", nil))
	t.Fatal("query failure was silently successful")
}
