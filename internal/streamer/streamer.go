// Package streamer replays CSV records, assigning processing time before enqueue.
package streamer

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"google.golang.org/grpc"
	pb "gpu-telemetry-pipeline/api/queue/v1"
	"gpu-telemetry-pipeline/internal/observe"
	"gpu-telemetry-pipeline/internal/platform"
	"gpu-telemetry-pipeline/internal/queue/transport"
	"gpu-telemetry-pipeline/internal/telemetry"
	"io"
	"log/slog"
	"os"
	"time"
)

type Config struct {
	Path                     string
	Interval, RequestTimeout time.Duration
	Loop                     bool
	MaxFileBytes             int64
}
type Producer interface {
	Send(context.Context, telemetry.Envelope) error
}
type Runner struct {
	Config   Config
	Producer Producer
	Log      *slog.Logger
	Now      func() time.Time
	Metrics  *observe.Metrics
}

var columns = []string{"timestamp", "metric_name", "gpu_id", "device", "uuid", "modelName", "Hostname", "container", "pod", "namespace", "value", "labels_raw"}

func (r *Runner) Run(ctx context.Context) error {
	if r.Config.Interval < 0 || r.Config.MaxFileBytes < 1 {
		return errors.New("invalid streamer configuration")
	}
	session, err := telemetry.NewSession()
	if err != nil {
		return err
	}
	var seq telemetry.Counter
	if r.Now == nil {
		r.Now = time.Now
	}
	for {
		if err := r.pass(ctx, session, &seq); err != nil {
			return err
		}
		if !r.Config.Loop {
			return nil
		}
	}
}
func (r *Runner) pass(ctx context.Context, session string, seq *telemetry.Counter) error {
	file, err := os.Open(r.Config.Path)
	if err != nil {
		return err
	}
	defer file.Close()
	st, err := file.Stat()
	if err != nil {
		return err
	}
	if !st.Mode().IsRegular() || st.Size() > r.Config.MaxFileBytes {
		return errors.New("CSV must be a regular file within MAX_CSV_BYTES")
	}
	// Bound even a file that grows after Stat or a malformed single logical record.
	reader := csv.NewReader(io.LimitReader(file, r.Config.MaxFileBytes+1))
	header, err := reader.Read()
	if err != nil {
		return err
	}
	index := make(map[string]int, len(header))
	for i, k := range header {
		if _, ok := index[k]; ok {
			return fmt.Errorf("duplicate CSV column %s", k)
		}
		index[k] = i
	}
	for _, c := range columns {
		if _, ok := index[c]; !ok {
			return fmt.Errorf("missing CSV column %s", c)
		}
	}
	count := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if seq.Last == int64(1<<63-1) {
			return telemetry.ErrSequenceExhausted
		}
		row, err := reader.Read()
		if err == io.EOF {
			if reader.InputOffset() > r.Config.MaxFileBytes {
				return errors.New("CSV grew beyond MAX_CSV_BYTES")
			}
			break
		}
		if err != nil {
			return fmt.Errorf("read CSV: %w", err)
		}
		if reader.InputOffset() > r.Config.MaxFileBytes {
			return errors.New("CSV grew beyond MAX_CSV_BYTES")
		}
		value := func(k string) string { return row[index[k]] }
		n, err := seq.Next()
		if err != nil {
			return err
		}
		raw := telemetry.Raw{Timestamp: r.Now().UTC().Truncate(time.Microsecond).Format(time.RFC3339Nano), MetricName: value("metric_name"), GPUID: value("gpu_id"), Device: value("device"), UUID: value("uuid"), ModelName: value("modelName"), Hostname: value("Hostname"), Container: value("container"), Pod: value("pod"), Namespace: value("namespace"), Value: value("value"), LabelsRaw: value("labels_raw")}
		if err := r.Producer.Send(ctx, telemetry.Envelope{Key: telemetry.Key{Session: session, Sequence: n}, Telemetry: raw}); err != nil {
			return err
		}
		count++
		r.Metrics.Add("streamer_accepted_total", 1)
		if r.Config.Interval > 0 {
			timer := time.NewTimer(r.Config.Interval)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
		}
	}
	if count == 0 {
		return errors.New("CSV contains no records")
	}
	r.Log.Info("CSV pass accepted", "records", count)
	return nil
}

type GRPCProducer struct {
	Client  pb.QueueClient
	Timeout time.Duration
	stream  grpc.BidiStreamingClient[pb.Envelope, pb.ProduceResult]
	cancel  context.CancelFunc
}

func (p *GRPCProducer) Close() {
	if p.cancel != nil {
		p.cancel()
	}
	p.stream = nil
}
func (p *GRPCProducer) Send(ctx context.Context, e telemetry.Envelope) error {
	for attempt := 0; ; attempt = min(attempt+1, 6) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if p.stream == nil {
			streamctx, cancel := context.WithCancel(ctx)
			s, err := p.Client.Produce(streamctx)
			if err != nil {
				cancel()
				if err := platform.Pause(ctx, attempt); err != nil {
					return err
				}
				continue
			}
			p.stream = s
			p.cancel = cancel
		}
		type result struct {
			v   *pb.ProduceResult
			err error
		}
		done := make(chan result, 1)
		stream := p.stream
		go func() {
			err := stream.Send(transport.Wire(e))
			if err != nil {
				done <- result{err: err}
				return
			}
			v, err := stream.Recv()
			done <- result{v, err}
		}()
		timer := time.NewTimer(p.Timeout)
		var out result
		select {
		case out = <-done:
			timer.Stop()
		case <-timer.C:
			p.Close()
			out.err = context.DeadlineExceeded
		case <-ctx.Done():
			timer.Stop()
			p.Close()
			return ctx.Err()
		}
		if out.err != nil {
			p.Close()
			if err := platform.Pause(ctx, attempt); err != nil {
				return err
			}
			continue
		}
		if out.v.Key == nil || out.v.Key.StreamerSession != e.Key.Session || out.v.Key.Sequence != e.Key.Sequence {
			p.Close()
			return errors.New("producer acknowledgement key mismatch")
		}
		switch out.v.Status {
		case pb.ProduceStatus_PRODUCE_STATUS_ACCEPTED, pb.ProduceStatus_PRODUCE_STATUS_DUPLICATE:
			return nil
		case pb.ProduceStatus_PRODUCE_STATUS_FULL, pb.ProduceStatus_PRODUCE_STATUS_DRAINING:
			if err := platform.Pause(ctx, attempt); err != nil {
				return err
			}
		default:
			return fmt.Errorf("queue rejected envelope: %s", out.v.Reason)
		}
	}
}
