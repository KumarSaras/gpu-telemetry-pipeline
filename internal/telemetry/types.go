// Package telemetry defines retry identity and the public telemetry record.
package telemetry

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var ErrSequenceExhausted = errors.New("sequence exhausted")

// Counter has one goroutine owner. The upper bound is PostgreSQL BIGINT's maximum.
type Counter struct{ Last int64 }

func (c *Counter) Next() (int64, error) {
	if c.Last < 0 || c.Last == math.MaxInt64 {
		return 0, ErrSequenceExhausted
	}
	// It can accommodate 2^63 - 1 counters which is a sufficient number for the project
	c.Last++
	return c.Last, nil
}

type Key struct {
	Session  string `json:"streamer_session"`
	Sequence int64  `json:"sequence"`
}

func NewSession() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func (k Key) Validate() error {
	if len(k.Session) != 32 || strings.ToLower(k.Session) != k.Session {
		return errors.New("streamer_session must be 32 lowercase hex characters")
	}
	if _, err := hex.DecodeString(k.Session); err != nil {
		return errors.New("invalid streamer_session")
	}
	if k.Sequence <= 0 {
		return errors.New("sequence must be positive")
	}
	return nil
}

type Raw struct {
	Timestamp  string `json:"timestamp"`
	MetricName string `json:"metric_name"`
	GPUID      string `json:"gpu_id"`
	Device     string `json:"device"`
	UUID       string `json:"uuid"`
	ModelName  string `json:"model_name"`
	Hostname   string `json:"hostname"`
	Container  string `json:"container"`
	Pod        string `json:"pod"`
	Namespace  string `json:"namespace"`
	Value      string `json:"value"`
	LabelsRaw  string `json:"labels_raw"`
}
type Envelope struct {
	Key       Key `json:"key"`
	Telemetry Raw `json:"telemetry"`
}

func (e Envelope) Size() int { b, _ := json.Marshal(e); return len(b) }

// Record has exactly one timestamp, assigned by the streamer. Retry identity is separate.
type Record struct {
	Timestamp  time.Time   `json:"timestamp"`
	MetricName string      `json:"metric_name"`
	GPUID      string      `json:"gpu_id"`
	Device     string      `json:"device"`
	UUID       string      `json:"uuid"`
	ModelName  string      `json:"model_name"`
	Hostname   string      `json:"hostname"`
	Container  string      `json:"container"`
	Pod        string      `json:"pod"`
	Namespace  string      `json:"namespace"`
	Value      json.Number `json:"value"`
	LabelsRaw  string      `json:"labels_raw"`
}

var number = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)

func Parse(raw Raw) (Record, error) {
	t, err := time.Parse(time.RFC3339Nano, raw.Timestamp)
	if err != nil {
		return Record{}, fmt.Errorf("invalid timestamp: %w", err)
	}
	if t.Year() < 1 {
		return Record{}, errors.New("timestamp year must be positive")
	}
	if raw.UUID == "" || raw.MetricName == "" {
		return Record{}, errors.New("uuid and metric_name are required")
	}
	if len(raw.Value) > 128 || !number.MatchString(raw.Value) {
		return Record{}, errors.New("invalid numeric value")
	}
	n, err := strconv.ParseFloat(raw.Value, 64)
	if err != nil || math.IsNaN(n) || math.IsInf(n, 0) {
		return Record{}, errors.New("numeric value outside supported finite range")
	}
	return Record{t.UTC().Truncate(time.Microsecond), raw.MetricName, raw.GPUID, raw.Device, raw.UUID, raw.ModelName, raw.Hostname, raw.Container, raw.Pod, raw.Namespace, json.Number(raw.Value), raw.LabelsRaw}, nil
}

type Bounds struct{ Start, End *time.Time }

func parseOptionalTime(value string) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return nil, errors.New("time filters must use RFC3339")
	}
	t = t.UTC()
	return &t, nil
}

func ParseBounds(start, end string) (Bounds, error) {
	var b Bounds
	var err error
	b.Start, err = parseOptionalTime(start)
	if err != nil {
		return b, err
	}
	b.End, err = parseOptionalTime(end)
	if err != nil {
		return b, err
	}
	if b.Start != nil && b.End != nil && b.Start.After(*b.End) {
		return b, errors.New("start_time is after end_time")
	}
	return b, nil
}

// StorageBounds rounds inward to microsecond resolution, preserving inclusive filtering.
func (b Bounds) StorageBounds() Bounds {
	if b.Start != nil {
		t := b.Start.Truncate(time.Microsecond)
		if t.Before(*b.Start) {
			t = t.Add(time.Microsecond)
		}
		b.Start = &t
	}
	if b.End != nil {
		t := b.End.Truncate(time.Microsecond)
		b.End = &t
	}
	return b
}
