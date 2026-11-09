# GPU telemetry pipeline

A Go pipeline with four applications: CSV streamer, custom in-memory queue, telemetry collector, and REST gateway. PostgreSQL stores the collected telemetry. It replays GPU telemetry from a CSV file, collect it in PostgreSQL, and query it through a REST API. The sample CSV is included.

The default deployment runs one instance of each application. The collector has one worker. Its **slots** limit how many messages can be assigned to it before it acknowledges completion; the default is one. For architecture and delivery guarantees, see the [design document](docs/design.md).

## Services

| Component | Purpose |
| --- | --- |
| Streamer | Replays CSV records and replaces their timestamps with the time it processes them. |
| Queue | Holds messages in memory and pushes them to subscribed collectors. |
| Collector | Parses messages and saves them in PostgreSQL before acknowledging completion. |
| Gateway | Provides HTTP APIs to list GPUs and query their telemetry. |
| PostgreSQL | Stores collected telemetry on a persistent volume. |

Helm also runs a one-time migration Job to prepare the database schema. A queue restart can lose messages that have not yet been persisted. This project uses plain HTTP/gRPC without authentication; production security work is listed in [production TODOs](docs/production-todos.md).

## Deploy locally with Kind and Helm

Install **Docker, Make, Kind, kubectl, Helm, curl, and Python 3.9 or newer**. Start Docker and check that `docker info` succeeds. Go is not needed to build the application images; Docker compiles them. Ripgrep is not required.

These steps create a fresh local installation. Run commands from the cloned repository.

### 1. Create the cluster

```sh
cd "$(git rev-parse --show-toplevel)"
export KUBECONFIG="$PWD/.cache/kind-kubeconfig"
kind create cluster --name gpu-telemetry --kubeconfig "$KUBECONFIG" --wait 120s
kubectl get nodes
```

Create the cluster only once. In each new terminal used for kubectl or Helm, run this from inside the clone:

```sh
export KUBECONFIG="$(git rev-parse --show-toplevel)/.cache/kind-kubeconfig"
```

### 2. Build and load the images

```sh
make images
kind load docker-image \
  gpu-telemetry/queue:dev \
  gpu-telemetry/streamer:dev \
  gpu-telemetry/collector:dev \
  gpu-telemetry/gateway:dev \
  gpu-telemetry/migrate:dev \
  --name gpu-telemetry
```

Load all five images, including the migration image.

### 3. Install the application

```sh
helm upgrade --install gpu-telemetry deploy/helm/gpu-telemetry \
  --namespace gpu-telemetry --create-namespace \
  --set images.prefix=gpu-telemetry --set images.tag=dev \
  --wait --wait-for-jobs --timeout 5m
kubectl -n gpu-telemetry get pods,services,pvc
```

Expect four ready application pods, one ready PostgreSQL pod, a bound database volume, and a completed migration Job. Kind provides local storage; Helm creates the database, volume claim, services, and application workloads. No passwords, certificates, or separate database setup are needed for this fresh installation.

The streamer continuously replays the sample. One complete pass produces 2,470 records for 247 unique GPU UUIDs; the record count grows on subsequent passes.

Check ingestion directly in PostgreSQL (output: total rows, distinct GPU UUIDs):

```sh
kubectl --namespace gpu-telemetry exec statefulset/gpu-telemetry-postgres -- \
  psql -U postgres -d telemetry -Atc 'SELECT count(*), count(DISTINCT uuid) FROM telemetry;'
```

## Port forwarding

Forward the gateway to access the API. Keep this terminal running:

```sh
kubectl -n gpu-telemetry port-forward service/gpu-telemetry-gateway 8080:8080
```

For the other services' health and metrics, run each command in its own terminal with the project kubeconfig exported:

```sh
kubectl -n gpu-telemetry port-forward deployment/gpu-telemetry-queue 8081:8081
```

```sh
kubectl -n gpu-telemetry port-forward deployment/gpu-telemetry-collector 8082:8082
```

```sh
kubectl -n gpu-telemetry port-forward deployment/gpu-telemetry-streamer 8083:8083
```

Only the gateway forward is needed for telemetry queries. Stop a forward with Ctrl-C; restart it if its selected pod is replaced. If a local port is occupied, choose another local port, for example `18080:8080`.

## Query telemetry

With the gateway forward running, use another terminal:

```sh
curl --fail http://localhost:8080/readyz
curl --fail http://localhost:8080/api/v1/gpus
```

Once the GPU list is nonempty, select a returned UUID and query its records:

```sh
GPU_UUID="$(curl --fail --silent http://localhost:8080/api/v1/gpus | python3 -c 'import json,sys; print(json.load(sys.stdin)[0])')"
curl --fail "http://localhost:8080/api/v1/gpus/${GPU_UUID}/telemetry"
```

Query a record with the time filter:
```sh
GPU_UUID="$(curl --fail --silent http://localhost:8080/api/v1/gpus | python3 -c 'import json,sys; print(json.load(sys.stdin)[0])')"
curl --fail-with-body --get \
  "http://localhost:8080/api/v1/gpus/${GPU_UUID}/telemetry" \
  --data-urlencode 'start_time=2026-11-09T10:00:00Z' \
  --data-urlencode 'end_time=2026-11-09T11:00:00Z'
```

| API | Purpose |
| --- | --- |
| `GET /api/v1/gpus` | List unique GPU UUIDs with stored data. |
| `GET /api/v1/gpus/{id}/telemetry` | Return all stored telemetry for that GPU, ordered by timestamp. Use a UUID from the listing as `{id}`. |
| Optional `start_time` and `end_time` query parameters | Filter the telemetry endpoint using inclusive RFC3339 bounds. Either bound can be supplied alone. |
| `GET /openapi.json` | Retrieve the generated API specification. |

Choose time bounds from timestamps returned by your run: timestamps are generated by the streamer, rather than copied from the source CSV. An unknown GPU or a range with no data returns `[]`; invalid or reversed bounds return HTTP 400.

See the [OpenAPI specification](api/openapi.json) for request and response details.

## Configuration

Defaults work for the installation above. Applications read environment variables; Helm supplies settings from [values.yaml](deploy/helm/gpu-telemetry/values.yaml). Use the Helm keys shown below with `--set` or a values file. Shared connection and timeout settings are listed once below the service tables.

**—** means the environment variable is supported by the binary but has no Helm value in the current chart. To change it in Kubernetes, update the workload template to inject it. Only the listed Helm keys configure the deployment.

### Queue

Capacity includes both waiting and assigned messages. When capacity is full, producers back off and retry.

| Environment variable | Default | Helm key | Meaning |
| --- | --- | --- | --- |
| `GRPC_ADDR` | `:9090` | — | Producer/subscriber listener. |
| `QUEUE_MAX_MESSAGES` | `10000` | `queue.maxMessages` | Maximum unfinished messages. |
| `QUEUE_MAX_BYTES` | `67108864` (64 MiB) | `queue.maxBytes` | Accounted envelope-byte capacity; excludes runtime overhead. |
| `QUEUE_MAX_SUBSCRIBERS` | `10` | `queue.maxSubscribers` | Maximum collector subscriptions. |
| `QUEUE_MAX_SLOTS` | `128` | `queue.maxSlots` | Maximum slots allowed per collector; hard ceiling 4,096. |
| `ACK_TIMEOUT` | `30s` | `queue.ackTimeout` | Per-slot completion budget advertised to collectors. |
| `MAX_STREAMS` | `32` | — | Queue-wide active streaming RPC limit, including idle streams. |

### Streamer

| Environment variable | Default | Helm key | Meaning |
| --- | --- | --- | --- |
| `CSV_PATH` | Host: `data/telemetry.csv`; image: `/app/telemetry.csv` | — | Input CSV file; must exist inside the container for deployed runs. |
| `STREAM_INTERVAL` | `10ms` | `streamer.interval` | Delay between accepted records. |
| `STREAM_LOOP` | `true` | `streamer.loop` | Replay the file continuously. |
| `MAX_CSV_BYTES` | `16777216` (16 MiB) | — | Maximum input file size. |

The streamer is a Deployment, so Kubernetes restarts its container after it exits. Setting `streamer.loop=false` ends one process pass; it does not turn the Deployment into a one-shot Job.

### Collector

| Environment variable | Default | Helm key | Meaning |
| --- | --- | --- | --- |
| `COLLECTOR_SLOTS` | `1` | `collector.slots` | Maximum outstanding deliveries: processing plus buffered messages. |
| `PROCESS_TIMEOUT` | `10s` | — | Database write deadline for one delivery. |

The collector always has **one worker**. With four slots, it can process one message and buffer up to three more. More slots add buffering, not parallel processing. Slots cannot exceed the queue's configured slot ceiling. The collector checks the queue's advertised `ACK_TIMEOUT > PROCESS_TIMEOUT + REQUEST_TIMEOUT`. Its shutdown budget must satisfy `SHUTDOWN_TIMEOUT >= PROCESS_TIMEOUT + REQUEST_TIMEOUT`.

### Gateway

| Environment variable | Default | Helm key | Meaning |
| --- | --- | --- | --- |
| `QUERY_TIMEOUT` | `30s` | — | Deadline for a telemetry query/streamed response. |
| `MAX_QUERIES` | `8` | — | Maximum concurrent API queries. |

### Settings shared by services

Each service reads only the settings it uses.

| Environment variable | Services | Default | Helm key / meaning |
| --- | --- | --- | --- |
| `QUEUE_URL` | Streamer, collector | Host: `localhost:9090`; Helm: `gpu-telemetry-queue:9090` | Automatically generated from the release name. |
| `DATABASE_URL` | Collector, gateway, migration Job | Required for host runs; generated for the bundled database | `database.url` overrides the generated connection URL. |
| `DB_MAX_CONNECTIONS` | Collector, gateway | `10` | —; pool size per service process. |
| `REQUEST_TIMEOUT` | Streamer, collector, gateway | `5s` | —; producer acceptance, collector storage check/handshake/completion, and collector/gateway initial DB connection budget. |
| `SHUTDOWN_TIMEOUT` | All four applications | `20s` | —; bounded drain/shutdown. |
| `HTTP_ADDR` | All four applications | Queue `:8081`; streamer `:8083`; collector `:8082`; gateway `:8080` | —; application HTTP listener. |
| `MAX_MESSAGE_BYTES` | Queue, streamer, collector | `65536` (64 KiB) | `queue.maxMessageBytes`; Helm sets the same limit on the broker and both clients. |

### PostgreSQL, migration, and deployment settings

The migration Job reads only `DATABASE_URL`; it uses one connection and a fixed one-minute operation deadline. Helm runs it automatically.

| Helm key | Default | Meaning |
| --- | --- | --- |
| `database.postgresEnabled` | `true` | Run bundled PostgreSQL. When false, supply an existing database through `database.url`. |
| `database.url` | Empty | Generate the bundled database URL; required when using an external database. |
| `database.storage` | `5Gi` | PostgreSQL persistent volume claim size. |
| `database.storageClass` | Empty | Use the cluster's default StorageClass. |
| `database.image` | Pinned `postgres:18-alpine` | Database image; full digest is in values.yaml. |
| `streamer.replicas`, `collector.replicas` | `1` each | Independent instance counts; chart accepts 0–10. Zero stops that application. |
| `queue.replicas` | `1` | Fixed at one for this in-memory queue design. |
| `images.prefix`, `images.tag` | `gpu-telemetry`, `dev` | Image names used by the four applications and migration Job. |
| `images.pullPolicy` | `IfNotPresent` | Application and migration image pull policy. |
| `resources.requests` | CPU `50m`, memory `64Mi` | Resource requests for application and migration containers. |
| `resources.limits` | CPU `1`, memory `256Mi` | Resource limits for application and migration containers. |

For example, change collector buffering and replay pacing on a release installed with this guide:

```sh
helm upgrade --install gpu-telemetry deploy/helm/gpu-telemetry \
  --namespace gpu-telemetry --reuse-values \
  --set collector.slots=4 --set streamer.interval=20ms \
  --wait --wait-for-jobs --timeout 5m
```

The affected workloads restart to pick up their new settings. See the [configuration loaders](internal/config/config.go) for validation bounds and the [design](docs/design.md) for delivery/deadline details.

Pause both workers while keeping the queue and stored data:

```sh
helm upgrade gpu-telemetry deploy/helm/gpu-telemetry \
  --namespace gpu-telemetry --reuse-values \
  --set streamer.replicas=0 --set collector.replicas=0 \
  --wait --wait-for-jobs --timeout 5m
```

Set either count back to `1` to resume it; a restarted streamer begins a new CSV replay session. To drain before pausing collectors, stop streamers first and wait for `gpu_telemetry_queue_messages` to reach zero. With only collectors stopped, the queue eventually fills and backpressures streamers.

## Health and metrics

All four applications expose `/livez`, `/readyz`, and `/metrics` on their HTTP ports:

| Application | Local port after forwarding |
| --- | --- |
| Gateway | 8080 |
| Queue | 8081 |
| Collector | 8082 |
| Streamer | 8083 |

For example, with the queue forward running:

```sh
curl --fail http://localhost:8081/livez
curl --fail http://localhost:8081/readyz
curl --fail http://localhost:8081/metrics
```

Fetch queue metrics every second while the port-forward is running; stop with **Ctrl+C**. Change the port to inspect another service.

```sh
while true; do
  date
  curl -fsS http://localhost:8081/metrics
  sleep 1
done
```

`/livez` checks that the HTTP process responds. `/readyz` checks application readiness and returns HTTP 503 when unavailable. Collector readiness checks its subscription and database; gateway readiness checks the database. Streamer readiness means replay is running, so use acceptance/persistence counters to check data progress.

Collectors wait for the telemetry table before subscribing or reconnecting. While waiting, liveness stays healthy and readiness stays false; the queue retains messages until collectors can consume them.

### Available metrics

Application metric names below have the prefix **`gpu_telemetry_`**. The two Go runtime metrics in the last row have no prefix.

| Service | Metric names | Insight |
| --- | --- | --- |
| Queue | `queue_messages`, `queue_ready`, `queue_inflight` | Total unfinished, waiting, and assigned messages. |
| Queue | `queue_bytes`, `full_total` | Accounted envelope bytes and enqueue attempts refused because capacity was full. |
| Queue | `subscribers`, `subscriber_slots` | Connected collectors and their total outstanding-message capacity. |
| Queue | `accepted_total`, `completed_total` | Messages accepted into memory versus acknowledged as persisted. |
| Queue | `redelivered_total`, `duplicates_total`, `rejected_total` | Requeued assignments, duplicate active keys, and collector rejections. |
| Streamer | `streamer_accepted_total` | Records for which the streamer received queue acceptance. |
| Collector | `collector_persisted_total`, `collector_duplicates_total` | New database rows and already-stored retry identities. |
| Collector | `collector_commit_errors_total`, `collector_rejected_total`, `collector_reconnects_total` | Write errors, invalid payloads, and subscription failures followed by reconnect attempts. |
| Collector | `collector_processing_seconds_sum`, `streamer_to_persistence_seconds_sum` | Accumulated processing time for all attempts and streamer-to-commit delay for new rows. |
| All four | `http_requests_total`, `http_request_seconds_sum`, `http_aborts_total` | HTTP request count, accumulated duration, and handler aborts/panics, including management traffic. |
| All four | `go_goroutines`, `go_memstats_heap_alloc_bytes` | Goroutine count and allocated Go heap bytes. |

In a healthy replay, acceptance, completion, and persistence counters increase. Growing backlog plus increasing `full_total` indicates pressure: check collector/database errors and logs. Increasing slots adds buffering, not workers.

Counters reset on restart; some appear only after their first event. Duration sums are accumulated seconds, not current latency or percentiles. A scrape's own HTTP count appears on a later scrape. Metrics are plain text; no monitoring server or dashboard is installed.

## Logs and troubleshooting

Follow collector logs:

```sh
kubectl -n gpu-telemetry logs deployment/gpu-telemetry-collector --tail=100 --follow
```

Replace `collector` with `queue`, `streamer`, or `gateway` to inspect another application. Use `--previous` instead of `--follow` for the prior container after a restart.

For database logs:

```sh
kubectl -n gpu-telemetry logs statefulset/gpu-telemetry-postgres --tail=100
```

Start troubleshooting with:

```sh
kubectl -n gpu-telemetry get pods,pvc,jobs
kubectl -n gpu-telemetry get events --sort-by=.lastTimestamp
```

| Symptom | Check |
| --- | --- |
| `ImagePullBackOff` | Load all five images into the correct Kind cluster; confirm the image tag matches the Helm release. |
| PostgreSQL or its volume is `Pending` | Check the PVC and `kubectl get storageclass`. |
| Migration fails or database-dependent pods remain unready | Find the migration Job name with `kubectl get jobs -n gpu-telemetry`, then run `kubectl logs -n gpu-telemetry job/JOB_NAME`. |
| Connection refused from curl | Keep the relevant port-forward running and use its local port. |
| Empty GPU list or telemetry result | Allow ingestion to start; select a listed UUID and bounds matching the returned timestamps. |
| Backlog, write errors, or repeated reconnects | Check collector logs, persistence counters, and PostgreSQL pod health. A full queue is backpressure, not a reason to restart it. |
| Commands target the wrong cluster | Export the project kubeconfig again and check `kubectl config current-context`; expect `kind-gpu-telemetry`. |

## Tests and Make commands

Host-side Go commands require **Go 1.26.4 or newer**. Coverage checks and the smoke runner also require Python 3.9 or newer; macOS’s Python 3.9 is supported. Select another interpreter with `PYTHON=/path/to/python3` when invoking Make.

```sh
make test-race
make coverage
```

| Command | Purpose |
| --- | --- |
| `make build` | Build application and utility binaries into `bin/`. |
| `make test` | Run Go unit and transport tests; no database or Kubernetes required. |
| `make test-race` | Run those tests with Go's race detector. |
| `make vet` | Run Go static checks. |
| `make coverage` | Run unit coverage, generate text/HTML reports at `coverage.html`, check tooling/coverage regressions, and enforce the above-90% total. |
| `make images` | Build the five local service/migration images using Docker. Does not publish them. |
| `make helm-lint` | Validate the Helm chart. |
| `make helm-template` | Render Kubernetes manifests to `.cache/helm-rendered.yaml` without installing them. |
| `make helm-check` | Check chart defaults, supported configuration, and invalid-value rejection. |
| `make smoke` | Load already-built images into a new temporary Kind cluster, install Helm, verify the pipeline/API/metrics, and delete that test cluster and data. |
| `make integration` | Run real PostgreSQL tests. Requires `TEST_DATABASE_URL` env variable pointing to an existing disposable database named `gpu_telemetry_test*`. |
| `make openapi` | Regenerate `api/openapi.json` without a database. |
| `make generate-proto` | Regenerate gRPC code; requires protoc and Go generator plugins. Generated files are already included. |
| `make migrate` | Apply schema migrations using `DATABASE_URL`. Helm already runs migrations during installation/upgrades. |

For other local image tags, use `make images IMAGE_TAG=dev2`; pass the same tag to image loading, Helm, and `make smoke`.

For `make integration`, create a new disposable database instance and run the test and cleanup command using:
```sh
docker run --rm -d \
  --name gpu-telemetry-integration-db \
  -e POSTGRES_DB=gpu_telemetry_test \
  -e POSTGRES_PASSWORD=test-only-password \
  -p 127.0.0.1:55432:5432 \
  postgres:18-alpine

until docker exec gpu-telemetry-integration-db \
  pg_isready -U postgres -d gpu_telemetry_test
do
  sleep 1
done

TEST_DATABASE_URL='postgres://postgres:test-only-password@localhost:55432/gpu_telemetry_test?sslmode=disable' \
  make integration

docker stop gpu-telemetry-integration-db
```

## Cleanup

Stop the application while retaining its PostgreSQL volume:

```sh
helm uninstall gpu-telemetry --namespace gpu-telemetry
```

To remove the entire local Kind cluster **and its database data**:

```sh
kind delete cluster --name gpu-telemetry --kubeconfig "$KUBECONFIG"
```

## AI assistance

AI was used during design, implementation, testing, and documentation.
The [AI prompt and review record](docs/ai-prompts-info.md) contains the
prompts used, where responses fell short, and the human decisions and
follow-up work that shaped the solution.