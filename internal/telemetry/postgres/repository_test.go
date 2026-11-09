package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"gpu-telemetry-pipeline/internal/telemetry"
)

type fakePool struct {
	exec  func(context.Context, string, ...any) (pgconn.CommandTag, error)
	query func(context.Context, string, ...any) (pgx.Rows, error)
}

func (*fakePool) Begin(context.Context) (pgx.Tx, error) {
	panic("unexpected migration transaction in repository operation")
}

func (p *fakePool) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return p.exec(ctx, sql, args...)
}
func (p *fakePool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.query(ctx, sql, args...)
}
func (*fakePool) QueryRow(context.Context, string, ...any) pgx.Row {
	panic("unexpected follow-up row lookup")
}
func (*fakePool) Close() {}

type fakeRows struct {
	pgx.Rows
	values                    [][]any
	index                     int
	scanError, iterationError error
	closed                    bool
}

func (r *fakeRows) Next() bool {
	if r.index >= len(r.values) {
		return false
	}
	r.index++
	return true
}
func (r *fakeRows) Scan(dest ...any) error {
	if r.scanError != nil {
		return r.scanError
	}
	values := r.values[r.index-1]
	if len(values) != len(dest) {
		return errors.New("column count mismatch")
	}
	for i, value := range values {
		reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(value))
	}
	return nil
}
func (r *fakeRows) Err() error { return r.iterationError }
func (r *fakeRows) Close()     { r.closed = true }
func key() telemetry.Key {
	return telemetry.Key{Session: "0123456789abcdef0123456789abcdef", Sequence: 1}
}
func record() telemetry.Record {
	return telemetry.Record{Timestamp: time.Date(2026, 11, 9, 12, 0, 0, 123456000, time.UTC), UUID: "GPU-a", MetricName: "util", Value: json.Number("1.2300"), GPUID: "0", Device: "nvidia0", ModelName: "model", Hostname: "host", LabelsRaw: `gpu="a"`}
}

func TestInsertUsesStableKeyAndPreservesAllRecordFields(t *testing.T) {
	ctx := context.Background()
	r := record()
	k := key()
	for _, inserted := range []bool{true, false} {
		calls := 0
		repo := &Repository{Pool: &fakePool{exec: func(got context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
			calls++
			if got != ctx || !strings.Contains(sql, "ON CONFLICT(streamer_session,sequence) DO NOTHING") {
				t.Fatal(sql)
			}
			want := []any{k.Session, k.Sequence, r.Timestamp, r.UUID, r.MetricName, string(r.Value), r.GPUID, r.Device, r.ModelName, r.Hostname, r.Container, r.Pod, r.Namespace, r.LabelsRaw}
			if !reflect.DeepEqual(args, want) {
				t.Fatal(args, want)
			}
			if inserted {
				return pgconn.NewCommandTag("INSERT 0 1"), nil
			}
			return pgconn.NewCommandTag("INSERT 0 0"), nil
		}}}
		if got, err := repo.Insert(ctx, k, r); err != nil || got != inserted || calls != 1 {
			t.Fatal(got, err, calls)
		}
	}
}
func TestInsertRejectsInvalidKeysAndPropagatesDatabaseError(t *testing.T) {
	failure := errors.New("commit unavailable")
	calls := 0
	repo := &Repository{Pool: &fakePool{exec: func(context.Context, string, ...any) (pgconn.CommandTag, error) {
		calls++
		return pgconn.CommandTag{}, failure
	}}}
	bad := key()
	bad.Sequence = 0
	if ok, err := repo.Insert(context.Background(), bad, record()); err == nil || ok || calls != 0 {
		t.Fatal(ok, err, calls)
	}
	if ok, err := repo.Insert(context.Background(), key(), record()); !errors.Is(err, failure) || ok {
		t.Fatal(ok, err)
	}
}
func TestListGPUsEmptyResultsErrorsAndRowCleanup(t *testing.T) {
	failure := errors.New("read unavailable")
	for _, test := range []struct {
		name       string
		rows       *fakeRows
		queryError error
		want       []string
		wantError  bool
	}{
		{name: "empty", rows: &fakeRows{}, want: []string{}},
		{name: "ids", rows: &fakeRows{values: [][]any{{"GPU-a"}, {"GPU-b"}}}, want: []string{"GPU-a", "GPU-b"}},
		{name: "query failure", queryError: failure, wantError: true},
		{name: "scan failure", rows: &fakeRows{values: [][]any{{"GPU-a"}}, scanError: failure}, wantError: true},
		{name: "iteration failure", rows: &fakeRows{iterationError: failure}, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			repo := &Repository{Pool: &fakePool{query: func(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
				if sql != "SELECT DISTINCT uuid FROM telemetry ORDER BY uuid" || len(args) != 0 {
					t.Fatal(sql, args)
				}
				return test.rows, test.queryError
			}}}
			ids, err := repo.ListGPUs(context.Background())
			if test.wantError {
				if !errors.Is(err, failure) {
					t.Fatal(err)
				}
			} else if err != nil || !reflect.DeepEqual(ids, test.want) {
				t.Fatal(ids, err)
			}
			if test.rows != nil && !test.rows.closed {
				t.Fatal("rows leaked")
			}
		})
	}
}
func TestQueryBindsUUIDAndIndependentInclusiveBounds(t *testing.T) {
	start := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	uuid := "GPU-a' OR 1=1--"
	for _, bounds := range []telemetry.Bounds{{}, {Start: &start}, {End: &end}, {Start: &start, End: &end}} {
		rows := &fakeRows{}
		repo := &Repository{Pool: &fakePool{query: func(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
			if strings.Contains(sql, uuid) || !strings.Contains(sql, "uuid=$1") || !strings.HasSuffix(sql, "ORDER BY timestamp,streamer_session,sequence") {
				t.Fatal(sql)
			}
			want := []any{uuid}
			if bounds.Start != nil {
				want = append(want, start)
				if !strings.Contains(sql, "timestamp >= $2") {
					t.Fatal(sql)
				}
			}
			if bounds.End != nil {
				want = append(want, end)
				placeholder := "timestamp <= $2"
				if bounds.Start != nil {
					placeholder = "timestamp <= $3"
				}
				if !strings.Contains(sql, placeholder) {
					t.Fatal(sql)
				}
			}
			if !reflect.DeepEqual(args, want) {
				t.Fatal(args, want)
			}
			return rows, nil
		}}}
		cursor, err := repo.Query(context.Background(), uuid, bounds)
		if err != nil {
			t.Fatal(err)
		}
		cursor.Close()
		if !rows.closed {
			t.Fatal("cursor did not release rows")
		}
	}
}
func TestQueryEmptySubmicrosecondWindowAvoidsDatabase(t *testing.T) {
	start := time.Date(2026, 11, 9, 0, 0, 0, 1, time.UTC)
	end := start.Add(100 * time.Nanosecond)
	repo := &Repository{Pool: &fakePool{query: func(context.Context, string, ...any) (pgx.Rows, error) {
		t.Fatal("empty storage window queried database")
		return nil, nil
	}}}
	cursor, err := repo.Query(context.Background(), "GPU-a", telemetry.Bounds{Start: &start, End: &end})
	if err != nil || cursor.Next() || cursor.Err() != nil {
		t.Fatal(cursor, err)
	}
	cursor.Close()
}
func TestQueryErrorsAndCursorRecordMetadataPrecision(t *testing.T) {
	failure := errors.New("query canceled")
	repo := &Repository{Pool: &fakePool{query: func(context.Context, string, ...any) (pgx.Rows, error) { return nil, failure }}}
	if _, err := repo.Query(context.Background(), "GPU-a", telemetry.Bounds{}); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	want := record()
	offset := want.Timestamp.In(time.FixedZone("offset", 19800))
	rows := &fakeRows{values: [][]any{{offset, want.MetricName, want.GPUID, want.Device, want.UUID, want.ModelName, want.Hostname, want.Container, want.Pod, want.Namespace, string(want.Value), want.LabelsRaw}}}
	c := &cursor{Rows: rows}
	if !c.Next() {
		t.Fatal("missing row")
	}
	got, err := c.Record()
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal(got, err)
	}
	rows.scanError = failure
	if _, err := c.Record(); !errors.Is(err, failure) {
		t.Fatal(err)
	}
}
func TestReadyChecksTableAndReleasesRows(t *testing.T) {
	failure := errors.New("missing schema")
	for _, fail := range []bool{false, true} {
		rows := &fakeRows{}
		repo := &Repository{Pool: &fakePool{query: func(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
			if sql != "SELECT uuid FROM telemetry LIMIT 0" {
				t.Fatal(sql)
			}
			if fail {
				return nil, failure
			}
			return rows, nil
		}}}
		err := repo.Ready(context.Background())
		if fail {
			if !errors.Is(err, failure) {
				t.Fatal(err)
			}
		} else if err != nil || !rows.closed {
			t.Fatal(err, rows.closed)
		}
	}
}
func TestOpenRejectsBadConfigurationAndUnavailableDatabase(t *testing.T) {
	if _, err := Open(context.Background(), "://invalid", 1); err == nil || err.Error() != "invalid database configuration" {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if repo, err := Open(ctx, "postgres://postgres@127.0.0.1:1/telemetry?sslmode=disable", 1); err == nil || repo != nil {
		t.Fatal(repo, err)
	}
}
