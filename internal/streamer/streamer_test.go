package streamer

import (
	"context"
	"gpu-telemetry-pipeline/internal/telemetry"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"
)

type recordingProducer struct {
	items  []telemetry.Envelope
	cancel context.CancelFunc
}

func (p *recordingProducer) Send(_ context.Context, e telemetry.Envelope) error {
	p.items = append(p.items, e)
	if len(p.items) == 2 && p.cancel != nil {
		p.cancel()
	}
	return nil
}
func TestReplayReplacesTimestampAndPreservesCSVFields(t *testing.T) {
	p := &recordingProducer{}
	now := time.Date(2026, 10, 30, 10, 0, 0, 0, time.UTC)
	r := &Runner{Config: Config{Path: "../../testdata/telemetry.csv", MaxFileBytes: 1 << 20}, Producer: p, Log: slog.New(slog.NewTextHandler(io.Discard, nil)), Now: func() time.Time { return now }}
	if e := r.Run(context.Background()); e != nil {
		t.Fatal(e)
	}
	if len(p.items) != 2 {
		t.Fatal(len(p.items))
	}
	a, b := p.items[0], p.items[1]
	if a.Telemetry.Timestamp != now.Format(time.RFC3339Nano) || a.Key.Sequence != 1 || b.Key.Sequence != 2 || a.Key.Session != b.Key.Session || a.Telemetry.LabelsRaw != `kind="test",label="a,b"` {
		t.Fatal(a, b)
	}
}
func TestLoopGetsNewSequenceAndBoundsInput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p := &recordingProducer{cancel: cancel}
	r := &Runner{Config: Config{Path: "../../testdata/telemetry.csv", MaxFileBytes: 1 << 20, Loop: true}, Producer: p, Log: slog.Default()}
	_ = r.Run(ctx)
	if len(p.items) != 2 {
		t.Fatal(len(p.items))
	}
	r.Config.MaxFileBytes = 1
	if e := r.Run(context.Background()); e == nil {
		t.Fatal("oversized file accepted")
	}
	file := t.TempDir() + "/bad.csv"
	if e := os.WriteFile(file, []byte("timestamp,uuid\nold,GPU-a\n"), 0600); e != nil {
		t.Fatal(e)
	}
	r.Config.Path = file
	r.Config.MaxFileBytes = 1024
	if e := r.Run(context.Background()); e == nil {
		t.Fatal("missing columns accepted")
	}
}
