// Package gateway exposes the two public telemetry APIs and their generated schema.
package gateway

import (
	"context"
	"encoding/json"
	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	"gpu-telemetry-pipeline/internal/telemetry"
	"io"
	"log/slog"
	"net/http"
	"reflect"
	"time"
)

type Config struct {
	QueryTimeout time.Duration
	MaxQueries   int
}
type GPUInput struct{}
type GPUOutput struct{ Body []string }
type QueryInput struct {
	ID    string `path:"id"`
	Start string `query:"start_time" doc:"Inclusive RFC3339 lower bound"`
	End   string `query:"end_time" doc:"Inclusive RFC3339 upper bound"`
}

func New(repo telemetry.Reader, cfg Config, log *slog.Logger) (*http.ServeMux, huma.API) {
	mux := http.NewServeMux()
	config := huma.DefaultConfig("GPU Telemetry API", "1.0.0")
	config.DocsPath = ""
	config.SchemasPath = ""
	config.Transformers = nil
	api := humago.New(mux, config)
	slots := make(chan struct{}, cfg.MaxQueries)
	acquire := func() bool {
		select {
		case slots <- struct{}{}:
			return true
		default:
			return false
		}
	}
	huma.Register(api, huma.Operation{OperationID: "list-gpus", Method: "GET", Path: "/api/v1/gpus", Summary: "List unique GPU UUIDs with persisted telemetry", Errors: []int{503}}, func(ctx context.Context, _ *GPUInput) (*GPUOutput, error) {
		if !acquire() {
			return nil, huma.Error503ServiceUnavailable("query capacity reached")
		}
		defer func() { <-slots }()
		ctx, cancel := context.WithTimeout(ctx, cfg.QueryTimeout)
		defer cancel()
		ids, err := repo.ListGPUs(ctx)
		if err != nil {
			return nil, huma.Error503ServiceUnavailable("telemetry storage unavailable")
		}
		if ids == nil {
			ids = []string{}
		}
		return &GPUOutput{Body: ids}, nil
	})
	registry := api.OpenAPI().Components.Schemas
	item := registry.Schema(reflect.TypeOf(telemetry.Record{}), true, "TelemetryRecord")
	schema := registry.SchemaFromRef(item.Ref)
	schema.Properties["value"] = &huma.Schema{Type: "number"}
	responses := map[string]*huma.Response{"200": {Description: "All matching telemetry in ascending streamer timestamp order", Content: map[string]*huma.MediaType{"application/json": {Schema: &huma.Schema{Type: "array", Items: item}}}}}
	huma.Register(api, huma.Operation{OperationID: "query-gpu-telemetry", Method: "GET", Path: "/api/v1/gpus/{id}/telemetry", Summary: "Query all GPU telemetry with optional inclusive time bounds", Responses: responses, Errors: []int{400, 503}}, func(ctx context.Context, input *QueryInput) (*huma.StreamResponse, error) {
		bounds, err := telemetry.ParseBounds(input.Start, input.End)
		if err != nil {
			return nil, huma.Error400BadRequest(err.Error())
		}
		if !acquire() {
			return nil, huma.Error503ServiceUnavailable("query capacity reached")
		}
		queryctx, cancel := context.WithTimeout(ctx, cfg.QueryTimeout)
		rows, err := repo.Query(queryctx, input.ID, bounds)
		if err != nil {
			cancel()
			<-slots
			return nil, huma.Error503ServiceUnavailable("telemetry storage unavailable")
		}
		return &huma.StreamResponse{Body: func(hctx huma.Context) {
			defer cancel()
			defer rows.Close()
			defer func() { <-slots }()
			hctx.SetHeader("Content-Type", "application/json")
			w := hctx.BodyWriter()
			failed := func(err error) { log.Error("incomplete telemetry response", "error", err); panic(http.ErrAbortHandler) }
			if _, err := io.WriteString(w, "["); err != nil {
				failed(err)
			}
			first := true
			enc := json.NewEncoder(w)
			for rows.Next() {
				record, err := rows.Record()
				if err != nil {
					failed(err)
				}
				if !first {
					if _, err = io.WriteString(w, ","); err != nil {
						failed(err)
					}
				}
				first = false
				if err = enc.Encode(record); err != nil {
					failed(err)
				}
			}
			if err := rows.Err(); err != nil {
				failed(err)
			}
			if err := queryctx.Err(); err != nil {
				failed(err)
			}
			if _, err := io.WriteString(w, "]\n"); err != nil {
				failed(err)
			}
		}}, nil
	})
	return mux, api
}
func Schema() ([]byte, error) {
	_, api := New(nil, Config{QueryTimeout: time.Second, MaxQueries: 1}, slog.Default())
	return json.MarshalIndent(api.OpenAPI(), "", "  ")
}
