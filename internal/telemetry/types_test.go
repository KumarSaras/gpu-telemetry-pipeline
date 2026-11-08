package telemetry

import (
	"errors"
	"math"
	"testing"
	"time"
)

func TestCounterDoesNotWrap(t *testing.T) {
	c := Counter{Last: math.MaxInt64 - 1}
	n, e := c.Next()
	if e != nil || n != math.MaxInt64 {
		t.Fatal(n, e)
	}
	for range 2 {
		n, e = c.Next()
		if !errors.Is(e, ErrSequenceExhausted) || n != 0 || c.Last != math.MaxInt64 {
			t.Fatal(n, e, c)
		}
	}
}
func TestKeyAndTelemetryValidation(t *testing.T) {
	s, e := NewSession()
	if e != nil {
		t.Fatal(e)
	}
	if e = (Key{s, 1}).Validate(); e != nil {
		t.Fatal(e)
	}
	for _, k := range []Key{{s, 0}, {s, -1}, {"bad", 1}} {
		if k.Validate() == nil {
			t.Fatal(k)
		}
	}
	raw := Raw{Timestamp: "2026-10-30T10:00:00.123456Z", UUID: "GPU-a", MetricName: "temperature", Value: "1.2300", LabelsRaw: `a="b,c"`}
	r, e := Parse(raw)
	if e != nil || r.Timestamp.Format(time.RFC3339Nano) != raw.Timestamp || string(r.Value) != "1.2300" || r.LabelsRaw != raw.LabelsRaw {
		t.Fatal(r, e)
	}
	for _, v := range []string{"NaN", "Inf", "1e9999", "01", "bad", ""} {
		bad := raw
		bad.Value = v
		if _, e = Parse(bad); e == nil {
			t.Fatalf("accepted %q", v)
		}
	}
	raw.Timestamp = "invalid"
	if _, e = Parse(raw); e == nil {
		t.Fatal("accepted invalid time")
	}
}
func TestInclusiveBoundsAtStoragePrecision(t *testing.T) {
	b, e := ParseBounds("2026-10-30T10:00:00.000000001Z", "2026-10-30T10:00:00.000000999Z")
	if e != nil {
		t.Fatal(e)
	}
	s := b.StorageBounds()
	if !s.Start.After(*s.End) {
		t.Fatal("sub-microsecond window must contain no stored times")
	}
	if _, e = ParseBounds("bad", ""); e == nil {
		t.Fatal("invalid")
	}
	if _, e = ParseBounds("2026-10-31T00:00:00Z", "2026-10-30T00:00:00Z"); e == nil {
		t.Fatal("reversed")
	}
	if _, e = ParseBounds("", "2026-10-30T10:00:00+05:30"); e != nil {
		t.Fatal(e)
	}
}

func TestParseBoundsOptionalFieldsAndErrors(t *testing.T) {
	for _, test := range []struct {
		name, start, end, wantStart, wantEnd, wantError string
	}{
		{name: "absent"},
		{name: "start only", start: "2026-10-30T10:00:00Z", wantStart: "2026-10-30T10:00:00Z"},
		{name: "end only", end: "2026-10-30T10:00:00Z", wantEnd: "2026-10-30T10:00:00Z"},
		{name: "fraction and offset", start: "2026-10-30T15:30:00.123456789+05:30", end: "2026-10-30T11:00:00.5Z", wantStart: "2026-10-30T10:00:00.123456789Z", wantEnd: "2026-10-30T11:00:00.5Z"},
		{name: "equal instants", start: "2026-10-30T15:30:00+05:30", end: "2026-10-30T10:00:00Z", wantStart: "2026-10-30T10:00:00Z", wantEnd: "2026-10-30T10:00:00Z"},
		{name: "invalid start", start: "bad", end: "2026-10-30T10:00:00Z", wantError: "time filters must use RFC3339"},
		{name: "invalid end retains start", start: "2026-10-30T10:00:00Z", end: "bad", wantStart: "2026-10-30T10:00:00Z", wantError: "time filters must use RFC3339"},
		{name: "reversed", start: "2026-10-31T10:00:00Z", end: "2026-10-30T10:00:00Z", wantStart: "2026-10-31T10:00:00Z", wantEnd: "2026-10-30T10:00:00Z", wantError: "start_time is after end_time"},
	} {
		t.Run(test.name, func(t *testing.T) {
			b, err := ParseBounds(test.start, test.end)
			if test.wantError == "" {
				if err != nil {
					t.Fatal(err)
				}
			} else if err == nil || err.Error() != test.wantError {
				t.Fatalf("got %v, want %s", err, test.wantError)
			}
			for _, field := range []struct {
				value *time.Time
				want  string
			}{{b.Start, test.wantStart}, {b.End, test.wantEnd}} {
				if field.want == "" {
					if field.value != nil {
						t.Fatal("omitted/unparsed field set", field.value)
					}
					continue
				}
				if field.value == nil || field.value.Format(time.RFC3339Nano) != field.want || field.value.Location() != time.UTC {
					t.Fatal("wrong time or non-UTC location", field.value, field.want)
				}
			}
		})
	}
}

func TestInvalidHexSessionAndRequiredTelemetryFields(t *testing.T) {
	if err := (Key{Session: "gggggggggggggggggggggggggggggggg", Sequence: 1}).Validate(); err == nil {
		t.Fatal("invalid session hex accepted")
	}
	for _, raw := range []Raw{
		{Timestamp: "0000-01-01T00:00:00Z", UUID: "GPU-a", MetricName: "util", Value: "1"},
		{Timestamp: "2026-11-09T00:00:00Z", MetricName: "util", Value: "1"},
		{Timestamp: "2026-11-09T00:00:00Z", UUID: "GPU-a", Value: "1"},
	} {
		if _, err := Parse(raw); err == nil {
			t.Fatal("invalid required field accepted", raw)
		}
	}
}
