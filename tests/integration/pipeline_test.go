//go:build integration

package integration

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pb "gpu-telemetry-pipeline/api/queue/v1"
	"gpu-telemetry-pipeline/internal/collector"
	"gpu-telemetry-pipeline/internal/gateway"
	"gpu-telemetry-pipeline/internal/streamer"
	"gpu-telemetry-pipeline/internal/telemetry"
	db "gpu-telemetry-pipeline/internal/telemetry/postgres"
	"gpu-telemetry-pipeline/internal/testsupport"
	"gpu-telemetry-pipeline/migrations"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func database(t *testing.T) *db.Repository {
	t.Helper()
	pool := databasePool(t)
	ctx := context.Background()
	if e := migrations.Apply(ctx, pool); e != nil {
		t.Fatal(e)
	}
	if e := migrations.Apply(ctx, pool); e != nil {
		t.Fatal("migration not repeatable", e)
	}
	var hashColumns int
	if e := pool.QueryRow(ctx, "SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='telemetry' AND column_name='payload_hash'").Scan(&hashColumns); e != nil || hashColumns != 0 {
		t.Fatal("payload_hash column remains", hashColumns, e)
	}
	return &db.Repository{Pool: pool}
}

func databasePool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("TEST_DATABASE_URL")
	if url == "" {
		t.Fatal("TEST_DATABASE_URL must name an explicitly disposable gpu_telemetry_test database")
	}
	ctx := context.Background()
	base, e := pgxpool.New(ctx, url)
	if e != nil {
		t.Fatal(e)
	}
	defer base.Close()
	var name string
	if e = base.QueryRow(ctx, "SELECT current_database()").Scan(&name); e != nil {
		t.Fatal(e)
	}
	if len(name) < 18 || name[:18] != "gpu_telemetry_test" {
		t.Fatal("refusing non-test database")
	}
	session, _ := telemetry.NewSession()
	schema := "test_" + session
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, e = base.Exec(ctx, "CREATE SCHEMA "+identifier); e != nil {
		t.Fatal(e)
	}
	cfg, e := pgxpool.ParseConfig(url)
	if e != nil {
		t.Fatal(e)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _, _ = pool.Exec(context.Background(), "DROP SCHEMA "+identifier+" CASCADE"); pool.Close() })
	return pool
}

func TestMigrationDropsPayloadHashPreservingTelemetry(t *testing.T) {
	pool := databasePool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	legacy, err := os.ReadFile("../../migrations/001_telemetry.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, string(legacy)); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, "CREATE TABLE schema_migrations(version integer PRIMARY KEY); INSERT INTO schema_migrations VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	key := telemetry.Key{Session: "0123456789abcdef0123456789abcdef", Sequence: 1}
	if _, err = pool.Exec(ctx, `INSERT INTO telemetry
		(streamer_session,sequence,timestamp,uuid,metric_name,value,gpu_id,device,model_name,hostname,container,pod,namespace,labels_raw,payload_hash)
		VALUES($1,$2,'2026-10-30T10:00:00Z','GPU-existing','util',1.2300,'0','nvidia0','test-model','host-a','','','','',decode(repeat('00',32),'hex'))`, key.Session, key.Sequence); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err = migrations.Apply(ctx, pool); err != nil {
			t.Fatal(err)
		}
	}
	var hashColumns, versions int
	if err = pool.QueryRow(ctx, "SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='telemetry' AND column_name='payload_hash'").Scan(&hashColumns); err != nil || hashColumns != 0 {
		t.Fatal("payload_hash column remains", hashColumns, err)
	}
	if err = pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations WHERE version IN (1,2)").Scan(&versions); err != nil || versions != 2 {
		t.Fatal("missing migration versions", versions, err)
	}
	repo := &db.Repository{Pool: pool}
	rows, err := repo.Query(ctx, "GPU-existing", telemetry.Bounds{})
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		t.Fatal("existing telemetry lost", rows.Err())
	}
	record, err := rows.Record()
	if err != nil || record.UUID != "GPU-existing" || string(record.Value) != "1.2300" || record.Timestamp.Format(time.RFC3339) != "2026-10-30T10:00:00Z" || record.Hostname != "host-a" {
		t.Fatal("existing telemetry changed", record, err)
	}
	if rows.Next() || rows.Err() != nil {
		t.Fatal("unexpected additional telemetry", rows.Err())
	}
	if inserted, err := repo.Insert(ctx, key, record); err != nil || inserted {
		t.Fatal("existing retry identity lost", inserted, err)
	}
	key.Sequence++
	if inserted, err := repo.Insert(ctx, key, record); err != nil || !inserted {
		t.Fatal("insert without payload_hash failed", inserted, err)
	}
}

func TestConcurrentImmutableInsertAndCommitBeforeAckRecovery(t *testing.T) {
	repo := database(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	raw := telemetry.Raw{Timestamp: "2026-10-30T10:00:00Z", UUID: "GPU-test", MetricName: "util", Value: "1.2300"}
	record, e := telemetry.Parse(raw)
	if e != nil {
		t.Fatal(e)
	}
	key := telemetry.Key{Session: "0123456789abcdef0123456789abcdef", Sequence: 1}
	var inserted atomic.Int32
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, e := repo.Insert(ctx, key, record)
			if e != nil {
				t.Error(e)
			}
			if ok {
				inserted.Add(1)
			}
		}()
	}
	wg.Wait()
	if inserted.Load() != 1 {
		t.Fatal("duplicate persisted", inserted.Load())
	}
	changed := record
	changed.UUID = "different"
	if inserted, err := repo.Insert(ctx, key, changed); err != nil || inserted {
		t.Fatal("duplicate key must retain the existing row", inserted, err)
	}
	var storedUUID string
	if e = repo.Pool.QueryRow(ctx, "SELECT uuid FROM telemetry WHERE streamer_session=$1 AND sequence=$2", key.Session, key.Sequence).Scan(&storedUUID); e != nil || storedUUID != record.UUID {
		t.Fatal("duplicate key replaced the original payload", storedUUID, e)
	}
	b, client := testsupport.Queue(t)
	subctx, stop := context.WithCancel(ctx)
	s, e := client.Subscribe(subctx)
	if e != nil {
		t.Fatal(e)
	}
	_ = s.Send(&pb.ConsumerFrame{Body: &pb.ConsumerFrame_Subscribe{Subscribe: &pb.Subscription{Slots: 1}}})
	if _, e = s.Header(); e != nil {
		t.Fatal(e)
	}
	producer := &streamer.GRPCProducer{Client: client, Timeout: time.Second}
	defer producer.Close()
	if e = producer.Send(ctx, telemetry.Envelope{Key: key, Telemetry: raw}); e != nil {
		t.Fatal(e)
	}
	if _, e = s.Recv(); e != nil {
		t.Fatal(e)
	}
	stop() // Simulate a lost ack after the already-committed row.
	worker := &collector.Collector{Client: client, Store: repo, Slots: 1, ProcessTimeout: time.Second, SendTimeout: time.Second, Log: testsupport.Logger()}
	go func() { _ = worker.Run(ctx) }()
	testsupport.Eventually(t, func() bool { v, _ := b.Snapshot(ctx); return v.Completed == 1 })
	var count int
	if e = repo.Pool.QueryRow(ctx, "SELECT count(*) FROM telemetry").Scan(&count); e != nil || count != 1 {
		t.Fatal(count, e)
	}
}
func TestFullFixtureThroughPipelineAndPublicAPI(t *testing.T) {
	repo := database(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	fixture := os.Getenv("TELEMETRY_FIXTURE")
	if fixture == "" {
		fixture = "../../data/telemetry.csv"
	}
	fixture, e := filepath.Abs(fixture)
	if e != nil {
		t.Fatal(e)
	}
	f, e := os.Open(fixture)
	if e != nil {
		t.Fatal(e)
	}
	csvreader := csv.NewReader(f)
	all, e := csvreader.ReadAll()
	_ = f.Close()
	if e != nil || len(all) != 2471 {
		t.Fatal("full supplied fixture required", e, len(all))
	}
	b, client := testsupport.Queue(t)
	worker := &collector.Collector{Client: client, Store: repo, Slots: 1, ProcessTimeout: time.Second, SendTimeout: time.Second, Log: testsupport.Logger()}
	workerDone := make(chan error, 1)
	go func() { workerDone <- worker.Run(ctx) }()
	producer := &streamer.GRPCProducer{Client: client, Timeout: time.Second}
	defer producer.Close()
	started := time.Now().UTC().Truncate(time.Microsecond)
	runner := &streamer.Runner{Config: streamer.Config{Path: fixture, MaxFileBytes: 16 << 20}, Producer: producer, Log: testsupport.Logger()}
	if e = runner.Run(ctx); e != nil {
		t.Fatal(e)
	}
	if e = b.Drain(ctx); e != nil {
		t.Fatal(e)
	}
	var count int
	var first, last time.Time
	if e = repo.Pool.QueryRow(ctx, "SELECT count(*),min(timestamp),max(timestamp) FROM telemetry").Scan(&count, &first, &last); e != nil || count != 2470 || first.Before(started) {
		t.Fatal(count, first, last, e)
	}
	mux, _ := gateway.New(repo, gateway.Config{QueryTimeout: time.Second, MaxQueries: 2}, testsupport.Logger())
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/gpus", nil))
	var ids []string
	if e = json.Unmarshal(w.Body.Bytes(), &ids); e != nil || len(ids) != 247 {
		t.Fatal(len(ids), e, w.Body.String())
	}
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/gpus/"+ids[0]+"/telemetry", nil))
	var records []telemetry.Record
	if e = json.Unmarshal(w.Body.Bytes(), &records); e != nil || len(records) != 10 {
		t.Fatal(len(records), e, w.Body.String())
	}
	for i, r := range records {
		if r.UUID != ids[0] || r.Timestamp.Before(started) || (i > 0 && r.Timestamp.Before(records[i-1].Timestamp)) {
			t.Fatal("wrong identity/order/time")
		}
	}
	target := records[0].Timestamp.Format(time.RFC3339Nano)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/gpus/"+ids[0]+"/telemetry?start_time="+target+"&end_time="+target, nil))
	var bounded []telemetry.Record
	if e = json.Unmarshal(w.Body.Bytes(), &bounded); e != nil || len(bounded) == 0 {
		t.Fatal("inclusive boundary missing", e, w.Body.String())
	}
	for _, r := range bounded {
		if !r.Timestamp.Equal(records[0].Timestamp) {
			t.Fatal("incorrect inclusive filter")
		}
	}
	cancel()
	select {
	case <-workerDone:
	case <-time.After(3 * time.Second):
		t.Fatal("collector did not stop")
	}
}

func TestFailedMigrationRollsBackMetadataAndCanBeRetried(t *testing.T) {
	pool := databasePool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := pool.Exec(ctx, "CREATE TABLE telemetry(existing text); INSERT INTO telemetry VALUES ('keep')"); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Apply(ctx, pool); err == nil {
		t.Fatal("schema conflict did not fail")
	}
	var present bool
	if err := pool.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_schema=current_schema() AND table_name='schema_migrations')").Scan(&present); err != nil || present {
		t.Fatal("failed migration committed version metadata", present, err)
	}
	var value string
	if err := pool.QueryRow(ctx, "SELECT existing FROM telemetry").Scan(&value); err != nil || value != "keep" {
		t.Fatal("failed migration changed existing data", value, err)
	}
	if _, err := pool.Exec(ctx, "DROP TABLE telemetry"); err != nil {
		t.Fatal(err)
	}
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatal("migration could not retry", err)
	}
	var versions int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&versions); err != nil || versions != 2 {
		t.Fatal(versions, err)
	}
}

func TestConcurrentMigrationsSerializeAndApplyEachVersionOnce(t *testing.T) {
	pool := databasePool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var workers sync.WaitGroup
	for range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			if err := migrations.Apply(ctx, pool); err != nil {
				t.Error(err)
			}
		}()
	}
	workers.Wait()
	var versions, legacyColumns int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&versions); err != nil || versions != 2 {
		t.Fatal(versions, err)
	}
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM information_schema.columns WHERE table_schema=current_schema() AND table_name='telemetry' AND column_name='payload_hash'").Scan(&legacyColumns); err != nil || legacyColumns != 0 {
		t.Fatal(legacyColumns, err)
	}
}
