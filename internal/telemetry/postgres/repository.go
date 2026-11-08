// Package postgres implements immutable telemetry persistence and indexed reads.
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"gpu-telemetry-pipeline/internal/telemetry"
	"strings"
	"time"
)

// Pool is the database boundary used by the repository and its callers.
// *pgxpool.Pool implements it; tests can exercise failures without a database.
type Pool interface {
	Begin(context.Context) (pgx.Tx, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	Close()
}

type Repository struct{ Pool Pool }

func Open(ctx context.Context, url string, max int32) (*Repository, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("invalid database configuration")
	}
	cfg.MaxConns = max
	cfg.MinConns = 0
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.ConnConfig.ConnectTimeout = 5 * time.Second
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err = p.Ping(ctx); err != nil {
		p.Close()
		return nil, err
	}
	return &Repository{p}, nil
}
func (r *Repository) Insert(ctx context.Context, k telemetry.Key, record telemetry.Record) (bool, error) {
	if err := k.Validate(); err != nil {
		return false, err
	}
	tag, err := r.Pool.Exec(ctx, `INSERT INTO telemetry(streamer_session,sequence,timestamp,uuid,metric_name,value,gpu_id,device,model_name,hostname,container,pod,namespace,labels_raw)
 VALUES($1,$2,$3,$4,$5,$6::numeric,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT(streamer_session,sequence) DO NOTHING`, k.Session, k.Sequence, record.Timestamp, record.UUID, record.MetricName, string(record.Value), record.GPUID, record.Device, record.ModelName, record.Hostname, record.Container, record.Pod, record.Namespace, record.LabelsRaw)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}
func (r *Repository) ListGPUs(ctx context.Context) ([]string, error) {
	rows, err := r.Pool.Query(ctx, "SELECT DISTINCT uuid FROM telemetry ORDER BY uuid")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []string{}
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		result = append(result, s)
	}
	return result, rows.Err()
}
func (r *Repository) Query(ctx context.Context, uuid string, b telemetry.Bounds) (telemetry.Cursor, error) {
	b = b.StorageBounds()
	if b.Start != nil && b.End != nil && b.Start.After(*b.End) {
		return &empty{}, nil
	}
	args := []any{uuid}
	where := []string{"uuid=$1"}
	if b.Start != nil {
		args = append(args, *b.Start)
		where = append(where, fmt.Sprintf("timestamp >= $%d", len(args)))
	}
	if b.End != nil {
		args = append(args, *b.End)
		where = append(where, fmt.Sprintf("timestamp <= $%d", len(args)))
	}
	rows, err := r.Pool.Query(ctx, `SELECT timestamp,metric_name,gpu_id,device,uuid,model_name,hostname,container,pod,namespace,value::text,labels_raw FROM telemetry WHERE `+strings.Join(where, " AND ")+" ORDER BY timestamp,streamer_session,sequence", args...)
	if err != nil {
		return nil, err
	}
	return &cursor{rows}, nil
}

type cursor struct{ pgx.Rows }

func (c *cursor) Record() (telemetry.Record, error) {
	var r telemetry.Record
	var value string
	err := c.Scan(&r.Timestamp, &r.MetricName, &r.GPUID, &r.Device, &r.UUID, &r.ModelName, &r.Hostname, &r.Container, &r.Pod, &r.Namespace, &value, &r.LabelsRaw)
	r.Timestamp = r.Timestamp.UTC()
	r.Value = json.Number(value)
	return r, err
}

type empty struct{}

func (*empty) Next() bool                        { return false }
func (*empty) Record() (telemetry.Record, error) { return telemetry.Record{}, nil }
func (*empty) Err() error                        { return nil }
func (*empty) Close()                            {}

// Ready checks table availability and the service role's read permission, not just TCP reachability.
func (r *Repository) Ready(ctx context.Context) error {
	rows, err := r.Pool.Query(ctx, "SELECT uuid FROM telemetry LIMIT 0")
	if err != nil {
		return err
	}
	rows.Close()
	return rows.Err()
}
