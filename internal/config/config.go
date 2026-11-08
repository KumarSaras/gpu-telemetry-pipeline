// Package config loads and validates only the settings used by each service.
package config

import (
	"errors"
	"gpu-telemetry-pipeline/internal/queue"
	"time"
)

type QueueConfig struct {
	GRPCAddr, HTTPAddr string
	Queue              queue.Config
	MaxStreams         int
	ShutdownTimeout    time.Duration
}

type StreamerConfig struct {
	QueueURL, HTTPAddr, CSVPath               string
	MaxMessageBytes                           int
	RequestTimeout, ShutdownTimeout, Interval time.Duration
	Loop                                      bool
	MaxCSVBytes                               int64
}

type CollectorConfig struct {
	QueueURL, HTTPAddr, DatabaseURL                 string
	MaxMessageBytes, CollectorSlots, DBConnections  int
	ProcessTimeout, RequestTimeout, ShutdownTimeout time.Duration
}

type GatewayConfig struct {
	HTTPAddr, DatabaseURL                         string
	DBConnections, MaxQueries                     int
	RequestTimeout, QueryTimeout, ShutdownTimeout time.Duration
}

type MigrateConfig struct{ DatabaseURL string }

func LoadQueue() (QueueConfig, error) {
	r := new(reader)
	c := QueueConfig{
		GRPCAddr: r.str("GRPC_ADDR", ":9090"),
		HTTPAddr: r.str("HTTP_ADDR", ":8081"),
		Queue: queue.Config{
			MaxMessages:     int(r.integer("QUEUE_MAX_MESSAGES", 10000, 1, 10000000)),
			MaxBytes:        r.integer("QUEUE_MAX_BYTES", 64<<20, 1024, 1<<40),
			MaxMessageBytes: r.messageBytes(),
			MaxSubscribers:  int(r.integer("QUEUE_MAX_SUBSCRIBERS", 10, 1, 1024)),
			MaxSlots:        int(r.integer("QUEUE_MAX_SLOTS", 128, 1, 4096)),
			AckTimeout:      r.duration("ACK_TIMEOUT", 30*time.Second),
		},
		MaxStreams:      int(r.integer("MAX_STREAMS", 32, 1, 10000)),
		ShutdownTimeout: r.duration("SHUTDOWN_TIMEOUT", 20*time.Second),
	}
	if err := c.Queue.Validate(); err != nil {
		r.errs = append(r.errs, err)
	}
	return c, r.err()
}

func LoadStreamer() (StreamerConfig, error) {
	r := new(reader)
	c := StreamerConfig{
		QueueURL:        r.str("QUEUE_URL", "localhost:9090"),
		HTTPAddr:        r.str("HTTP_ADDR", ":8083"),
		CSVPath:         r.str("CSV_PATH", "data/telemetry.csv"),
		MaxMessageBytes: r.messageBytes(),
		RequestTimeout:  r.duration("REQUEST_TIMEOUT", 5*time.Second),
		ShutdownTimeout: r.duration("SHUTDOWN_TIMEOUT", 20*time.Second),
		Interval:        r.duration("STREAM_INTERVAL", 10*time.Millisecond),
		Loop:            r.boolean("STREAM_LOOP", true),
		MaxCSVBytes:     r.integer("MAX_CSV_BYTES", 16<<20, 1024, 1<<40),
	}
	return c, r.err()
}

func LoadCollector() (CollectorConfig, error) {
	r := new(reader)
	c := CollectorConfig{
		QueueURL:        r.str("QUEUE_URL", "localhost:9090"),
		HTTPAddr:        r.str("HTTP_ADDR", ":8082"),
		DatabaseURL:     r.required("DATABASE_URL"),
		MaxMessageBytes: r.messageBytes(),
		// The broker validates its own slot ceiling during subscription.
		CollectorSlots:  int(r.integer("COLLECTOR_SLOTS", 1, 1, 4096)),
		DBConnections:   int(r.integer("DB_MAX_CONNECTIONS", 10, 1, 1000)),
		ProcessTimeout:  r.duration("PROCESS_TIMEOUT", 10*time.Second),
		RequestTimeout:  r.duration("REQUEST_TIMEOUT", 5*time.Second),
		ShutdownTimeout: r.duration("SHUTDOWN_TIMEOUT", 20*time.Second),
	}
	if c.ShutdownTimeout < c.ProcessTimeout+c.RequestTimeout {
		r.errs = append(r.errs, errors.New("SHUTDOWN_TIMEOUT must cover PROCESS_TIMEOUT + REQUEST_TIMEOUT"))
	}
	// ACK_TIMEOUT is advertised by the broker and checked at the handshake.
	return c, r.err()
}

func LoadGateway() (GatewayConfig, error) {
	r := new(reader)
	c := GatewayConfig{
		HTTPAddr:        r.str("HTTP_ADDR", ":8080"),
		DatabaseURL:     r.required("DATABASE_URL"),
		DBConnections:   int(r.integer("DB_MAX_CONNECTIONS", 10, 1, 1000)),
		MaxQueries:      int(r.integer("MAX_QUERIES", 8, 1, 1000)),
		RequestTimeout:  r.duration("REQUEST_TIMEOUT", 5*time.Second),
		QueryTimeout:    r.duration("QUERY_TIMEOUT", 30*time.Second),
		ShutdownTimeout: r.duration("SHUTDOWN_TIMEOUT", 20*time.Second),
	}
	return c, r.err()
}

func LoadMigrate() (MigrateConfig, error) {
	r := new(reader)
	c := MigrateConfig{DatabaseURL: r.required("DATABASE_URL")}
	return c, r.err()
}
