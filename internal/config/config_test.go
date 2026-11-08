package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

var loaders = map[string]func() error{
	"queue":     func() error { _, err := LoadQueue(); return err },
	"streamer":  func() error { _, err := LoadStreamer(); return err },
	"collector": func() error { _, err := LoadCollector(); return err },
	"gateway":   func() error { _, err := LoadGateway(); return err },
	"migrate":   func() error { _, err := LoadMigrate(); return err },
}

func cleanEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range strings.Fields("QUEUE_URL GRPC_ADDR HTTP_ADDR DATABASE_URL CSV_PATH QUEUE_MAX_MESSAGES QUEUE_MAX_BYTES MAX_MESSAGE_BYTES QUEUE_MAX_SUBSCRIBERS QUEUE_MAX_SLOTS ACK_TIMEOUT COLLECTOR_SLOTS DB_MAX_CONNECTIONS MAX_QUERIES MAX_STREAMS PROCESS_TIMEOUT REQUEST_TIMEOUT QUERY_TIMEOUT SHUTDOWN_TIMEOUT STREAM_INTERVAL STREAM_LOOP MAX_CSV_BYTES") {
		// Setenv registers restoration; unset makes the actual default apply.
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("DATABASE_URL", "postgres://test/telemetry")
}

func TestServiceDefaultsAndOverrides(t *testing.T) {
	cleanEnvironment(t)
	q, err := LoadQueue()
	if err != nil {
		t.Fatal(err)
	}
	s, err := LoadStreamer()
	if err != nil {
		t.Fatal(err)
	}
	c, err := LoadCollector()
	if err != nil {
		t.Fatal(err)
	}
	g, err := LoadGateway()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadMigrate(); err != nil {
		t.Fatal(err)
	}
	if q.HTTPAddr != ":8081" || s.HTTPAddr != ":8083" || c.HTTPAddr != ":8082" || g.HTTPAddr != ":8080" {
		t.Fatal("management port defaults changed")
	}
	if q.Queue.MaxMessages != 10000 || q.Queue.MaxBytes != 64<<20 || q.Queue.MaxMessageBytes != 64<<10 || q.MaxStreams != 32 || q.Queue.MaxSubscribers != 10 || q.Queue.MaxSlots != 128 || q.Queue.AckTimeout != 30*time.Second {
		t.Fatal(q)
	}
	if s.Interval != 10*time.Millisecond || !s.Loop || s.MaxCSVBytes != 16<<20 || s.MaxMessageBytes != q.Queue.MaxMessageBytes {
		t.Fatal(s)
	}
	if c.CollectorSlots != 1 || c.DBConnections != 10 || c.MaxMessageBytes != q.Queue.MaxMessageBytes || c.ProcessTimeout != 10*time.Second || c.RequestTimeout != 5*time.Second || c.ShutdownTimeout != 20*time.Second {
		t.Fatal(c)
	}
	if g.MaxQueries != 8 || g.QueryTimeout != 30*time.Second || g.DBConnections != 10 {
		t.Fatal(g)
	}
	t.Setenv("COLLECTOR_SLOTS", "4")
	t.Setenv("MAX_MESSAGE_BYTES", "131072")
	c, err = LoadCollector()
	if err != nil || c.CollectorSlots != 4 {
		t.Fatal(c, err)
	}
	q, err = LoadQueue()
	if err != nil {
		t.Fatal(err)
	}
	s, err = LoadStreamer()
	if err != nil || q.Queue.MaxMessageBytes != 131072 || c.MaxMessageBytes != 131072 || s.MaxMessageBytes != 131072 {
		t.Fatal("transport overrides disagree", err)
	}
}

func TestConfigurationIsolationAndValidation(t *testing.T) {
	cleanEnvironment(t)
	for _, test := range []struct{ key, value, affected string }{
		{"STREAM_INTERVAL", "bad", "streamer"},
		{"STREAM_LOOP", "bad", "streamer"},
		{"MAX_CSV_BYTES", "0", "streamer"},
		{"QUEUE_MAX_MESSAGES", "0", "queue"},
		{"QUEUE_MAX_BYTES", "bad", "queue"},
		{"QUEUE_MAX_SLOTS", "0", "queue"},
		{"QUEUE_MAX_SUBSCRIBERS", "0", "queue"},
		{"MAX_STREAMS", "0", "queue"},
		{"ACK_TIMEOUT", "bad", "queue"},
		{"COLLECTOR_SLOTS", "4097", "collector"},
		{"PROCESS_TIMEOUT", "bad", "collector"},
		{"QUERY_TIMEOUT", "bad", "gateway"},
		{"MAX_QUERIES", "0", "gateway"},
		{"DB_MAX_CONNECTIONS", "0", "collector gateway"},
		{"MAX_MESSAGE_BYTES", "0", "queue streamer collector"},
		{"REQUEST_TIMEOUT", "bad", "streamer collector gateway"},
		{"SHUTDOWN_TIMEOUT", "bad", "queue streamer collector gateway"},
		{"DATABASE_URL", "", "collector gateway migrate"},
	} {
		t.Run(test.key, func(t *testing.T) {
			t.Setenv(test.key, test.value)
			for name, load := range loaders {
				err := load()
				wantError := strings.Contains(" "+test.affected+" ", " "+name+" ")
				if (err != nil) != wantError {
					t.Errorf("%s with %s=%q: error=%v, want error=%v", name, test.key, test.value, err, wantError)
				}
			}
		})
	}
}

func TestTimeoutRelationshipsAndBrokerSlotCeiling(t *testing.T) {
	cleanEnvironment(t)
	t.Setenv("SHUTDOWN_TIMEOUT", "1s")
	if _, err := LoadCollector(); err == nil {
		t.Fatal("collector shutdown cannot cover its operation")
	}
	for _, name := range []string{"queue", "streamer", "gateway", "migrate"} {
		if err := loaders[name](); err != nil {
			t.Fatalf("collector relationship affected %s: %v", name, err)
		}
	}
	t.Setenv("SHUTDOWN_TIMEOUT", "15s")
	t.Setenv("QUEUE_MAX_SLOTS", "2")
	t.Setenv("COLLECTOR_SLOTS", "4")
	if c, err := LoadCollector(); err != nil || c.CollectorSlots != 4 {
		t.Fatal("broker slot ceiling must be validated by broker, not duplicated client config", c, err)
	}
	if q, err := LoadQueue(); err != nil || q.Queue.MaxSlots != 2 {
		t.Fatal(q, err)
	}
}

func TestDurationBounds(t *testing.T) {
	cleanEnvironment(t)
	for _, value := range []string{"0s", "-1s", "24h1s"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("ACK_TIMEOUT", value)
			if _, err := LoadQueue(); err == nil {
				t.Fatal("out-of-bounds duration accepted")
			}
		})
	}
}
