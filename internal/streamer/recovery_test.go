package streamer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	pb "gpu-telemetry-pipeline/api/queue/v1"
	"gpu-telemetry-pipeline/internal/queue/transport"
	"gpu-telemetry-pipeline/internal/telemetry"
)

type producerFunc func(context.Context, telemetry.Envelope) error

func (f producerFunc) Send(ctx context.Context, e telemetry.Envelope) error { return f(ctx, e) }
func runner(path string, p Producer) *Runner {
	return &Runner{Config: Config{Path: path, MaxFileBytes: 1 << 20}, Producer: p, Log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}
func TestRunnerInvalidFilesRecordsAndProducerFailure(t *testing.T) {
	header := strings.Join(columns, ",") + "\n"
	for _, test := range []struct{ name, data string }{
		{"empty", ""}, {"header only", header}, {"duplicate column", header[:len(header)-1] + ",timestamp\n"}, {"malformed row", header + "one,two\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "input.csv")
			if err := os.WriteFile(path, []byte(test.data), 0600); err != nil {
				t.Fatal(err)
			}
			if err := runner(path, producerFunc(func(context.Context, telemetry.Envelope) error { t.Fatal("invalid input reached producer"); return nil })).Run(context.Background()); err == nil {
				t.Fatal("invalid input accepted")
			}
		})
	}
	for _, path := range []string{filepath.Join(t.TempDir(), "missing.csv"), t.TempDir()} {
		if err := runner(path, &recordingProducer{}).Run(context.Background()); err == nil {
			t.Fatal("unreadable/nonregular input accepted", path)
		}
	}
	for _, cfg := range []Config{{MaxFileBytes: 0}, {MaxFileBytes: 1024, Interval: -time.Second}} {
		if err := (&Runner{Config: cfg}).Run(context.Background()); err == nil {
			t.Fatal("invalid config accepted")
		}
	}
	failure := errors.New("producer failed")
	calls := 0
	r := runner("../../testdata/telemetry.csv", producerFunc(func(context.Context, telemetry.Envelope) error { calls++; return failure }))
	if err := r.Run(context.Background()); !errors.Is(err, failure) || calls != 1 {
		t.Fatal(err, calls)
	}
}
func TestReplayLoopAllocatesNewOccurrenceAndInterruptsPacing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var sent []telemetry.Envelope
	r := runner("../../testdata/telemetry.csv", producerFunc(func(_ context.Context, e telemetry.Envelope) error {
		sent = append(sent, e)
		if len(sent) == 4 {
			cancel()
		}
		return nil
	}))
	r.Config.Loop = true
	r.Config.Interval = time.Millisecond
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	r.Now = func() time.Time { now = now.Add(time.Second); return now }
	if err := r.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if len(sent) != 4 {
		t.Fatal(len(sent))
	}
	for i, e := range sent {
		if e.Key.Sequence != int64(i+1) || e.Key.Session != sent[0].Key.Session {
			t.Fatal(sent)
		}
	}
	if sent[0].Telemetry.UUID != sent[2].Telemetry.UUID || sent[0].Telemetry.Timestamp == sent[2].Telemetry.Timestamp {
		t.Fatal("replay did not create a new occurrence", sent)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	r = runner("../../testdata/telemetry.csv", producerFunc(func(context.Context, telemetry.Envelope) error { cancel(); return nil }))
	r.Config.Interval = time.Hour
	if err := r.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal("pacing ignored cancellation", err)
	}
}
func TestCounterExhaustionDoesNotSendAndGrowingFilesStayBounded(t *testing.T) {
	for _, last := range []int64{math.MaxInt64, -1} {
		r := runner("../../testdata/telemetry.csv", producerFunc(func(context.Context, telemetry.Envelope) error {
			t.Fatal("invalid counter allocated a message")
			return nil
		}))
		if err := r.pass(context.Background(), "0123456789abcdef0123456789abcdef", &telemetry.Counter{Last: last}); !errors.Is(err, telemetry.ErrSequenceExhausted) {
			t.Fatal(err)
		}
	}
	fixture, err := os.ReadFile("../../testdata/telemetry.csv")
	if err != nil {
		t.Fatal(err)
	}
	for _, trim := range []bool{false, true} {
		data := string(fixture)
		if trim {
			data = strings.TrimSuffix(data, "\n")
		}
		path := filepath.Join(t.TempDir(), "growing.csv")
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
		appended := false
		r := runner(path, producerFunc(func(context.Context, telemetry.Envelope) error {
			if appended {
				return nil
			}
			appended = true
			file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
			if err != nil {
				return err
			}
			defer file.Close()
			_, err = file.WriteString("\n")
			return err
		}))
		r.Config.MaxFileBytes = int64(len(data))
		if err := r.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "CSV grew beyond") {
			t.Fatal("growing input was not bounded", trim, err)
		}
	}
}

type scriptedProducerClient struct {
	pb.QueueClient
	open func(context.Context) (pb.Queue_ProduceClient, error)
}

func (c *scriptedProducerClient) Produce(ctx context.Context, _ ...grpc.CallOption) (pb.Queue_ProduceClient, error) {
	return c.open(ctx)
}

type scriptedProducerStream struct {
	grpc.ClientStream
	ctx     context.Context
	send    func(*pb.Envelope) error
	receive func() (*pb.ProduceResult, error)
}

func (s *scriptedProducerStream) Context() context.Context         { return s.ctx }
func (s *scriptedProducerStream) Send(v *pb.Envelope) error        { return s.send(v) }
func (s *scriptedProducerStream) Recv() (*pb.ProduceResult, error) { return s.receive() }
func retryEnvelope() telemetry.Envelope {
	return telemetry.Envelope{Key: telemetry.Key{Session: "0123456789abcdef0123456789abcdef", Sequence: math.MaxInt64}, Telemetry: telemetry.Raw{Timestamp: "2026-11-09T12:00:00Z", UUID: "GPU-a", Value: "1"}}
}
func TestProducerRetriesUncertainFailuresWithUnchangedEnvelope(t *testing.T) {
	for _, mode := range []string{"open", "send", "receive", "timeout", "full then draining"} {
		t.Run(mode, func(t *testing.T) {
			e := retryEnvelope()
			sent := make(chan *pb.Envelope, 8)
			opens := 0
			var attempts atomic.Int32
			failure := errors.New("temporary network failure")
			client := &scriptedProducerClient{open: func(ctx context.Context) (pb.Queue_ProduceClient, error) {
				opens++
				number := opens
				if mode == "open" && number == 1 {
					return nil, failure
				}
				return &scriptedProducerStream{ctx: ctx, send: func(v *pb.Envelope) error {
					sent <- v
					if mode == "send" && number == 1 {
						return failure
					}
					return nil
				}, receive: func() (*pb.ProduceResult, error) {
					attempt := attempts.Add(1)
					if mode == "receive" && number == 1 {
						return nil, failure
					}
					if mode == "timeout" && number == 1 {
						<-ctx.Done()
						return nil, ctx.Err()
					}
					status := pb.ProduceStatus_PRODUCE_STATUS_ACCEPTED
					if mode == "full then draining" {
						switch attempt {
						case 1:
							status = pb.ProduceStatus_PRODUCE_STATUS_FULL
						case 2:
							status = pb.ProduceStatus_PRODUCE_STATUS_DRAINING
						}
					}
					return &pb.ProduceResult{Key: transport.Wire(e).Key, Status: status}, nil
				}}, nil
			}}
			p := &GRPCProducer{Client: client, Timeout: 50 * time.Millisecond}
			defer p.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			if err := p.Send(ctx, e); err != nil {
				t.Fatal(err)
			}
			wantSends := 2
			if mode == "open" {
				wantSends = 1
			}
			if mode == "full then draining" {
				wantSends = 3
				if opens != 1 {
					t.Fatal("backpressure recreated stream", opens)
				}
			} else if opens != 2 {
				t.Fatal(opens)
			}
			for i := 0; i < wantSends; i++ {
				select {
				case got := <-sent:
					if !proto.Equal(got, transport.Wire(e)) {
						t.Fatal("retry changed identity/time/payload", got)
					}
				default:
					t.Fatal("missing attempt")
				}
			}
		})
	}
}
func TestProducerRejectsMismatchedOrInvalidAcknowledgement(t *testing.T) {
	for _, mode := range []string{"missing key", "wrong key", "rejected"} {
		t.Run(mode, func(t *testing.T) {
			e := retryEnvelope()
			client := &scriptedProducerClient{open: func(ctx context.Context) (pb.Queue_ProduceClient, error) {
				return &scriptedProducerStream{ctx: ctx, send: func(*pb.Envelope) error { return nil }, receive: func() (*pb.ProduceResult, error) {
					result := &pb.ProduceResult{Key: transport.Wire(e).Key, Status: pb.ProduceStatus_PRODUCE_STATUS_ACCEPTED}
					switch mode {
					case "missing key":
						result.Key = nil
					case "wrong key":
						result.Key.Sequence--
					case "rejected":
						result.Status = pb.ProduceStatus_PRODUCE_STATUS_INVALID
						result.Reason = "invalid payload"
					}
					return result, nil
				}}, nil
			}}
			p := &GRPCProducer{Client: client, Timeout: time.Second}
			defer p.Close()
			if err := p.Send(context.Background(), e); err == nil {
				t.Fatal("invalid result accepted")
			}
		})
	}
}
func TestProducerCancellationBeforeOpenAndDuringReceiveOrBackoff(t *testing.T) {
	for _, mode := range []string{"before", "receive", "open backoff", "full backoff"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if mode == "before" {
				cancel()
			}
			e := retryEnvelope()
			client := &scriptedProducerClient{open: func(streamCtx context.Context) (pb.Queue_ProduceClient, error) {
				if mode == "before" {
					t.Error("canceled producer opened stream")
				}
				if mode == "open backoff" {
					cancel()
					return nil, errors.New("unavailable")
				}
				return &scriptedProducerStream{ctx: streamCtx, send: func(*pb.Envelope) error { return nil }, receive: func() (*pb.ProduceResult, error) {
					cancel()
					if mode == "full backoff" {
						return &pb.ProduceResult{Key: transport.Wire(e).Key, Status: pb.ProduceStatus_PRODUCE_STATUS_FULL}, nil
					}
					<-streamCtx.Done()
					return nil, streamCtx.Err()
				}}, nil
			}}
			p := &GRPCProducer{Client: client, Timeout: time.Second}
			defer p.Close()
			if err := p.Send(ctx, e); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
}
