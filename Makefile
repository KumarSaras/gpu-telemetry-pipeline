SHELL := /bin/sh
ROOT := $(CURDIR)
export GOMODCACHE := $(ROOT)/.cache/gomod
export GOCACHE := $(ROOT)/.cache/gobuild
SERVICES := queue streamer collector gateway migrate openapi
HELM ?= $(if $(wildcard $(ROOT)/.cache/tools/darwin-arm64/helm),$(ROOT)/.cache/tools/darwin-arm64/helm,helm)
KIND ?= $(if $(wildcard $(ROOT)/.cache/tools/kind),$(ROOT)/.cache/tools/kind,kind)
KUBECTL ?= $(if $(wildcard $(ROOT)/.cache/tools/kubectl),$(ROOT)/.cache/tools/kubectl,kubectl)
PROTOC ?= $(if $(wildcard $(ROOT)/.cache/tools/protoc/bin/protoc),$(ROOT)/.cache/tools/protoc/bin/protoc,protoc)
IMAGE_PREFIX ?= gpu-telemetry
IMAGE_TAG ?= dev
PYTHON ?= python3

.PHONY: build test test-race coverage vet openapi generate-proto integration images helm-lint helm-template migrate
build:
	@mkdir -p bin
	@set -e; for service in $(SERVICES); do CGO_ENABLED=0 go build -trimpath -o bin/$$service ./cmd/$$service; done
test:
	go test ./...
test-race:
	go test -race ./...
vet:
	go vet ./...
coverage:
	@set -eu; \
	packages=$$(go list ./internal/... ./migrations/...); \
	cover_pkgs=$$(printf '%s\n' "$$packages" | sed '\|/testsupport$$|d' | paste -sd, -); \
	if [ -z "$$cover_pkgs" ]; then echo "No coverage packages found." >&2; exit 1; fi; \
	go test -coverpkg="$$cover_pkgs" -coverprofile=coverage.out -covermode=atomic ./internal/... ./migrations/...
	go tool cover -func=coverage.out
	go tool cover -html=coverage.out -o coverage.html
	$(PYTHON) -B tests/test_coverage_gate.py
	$(PYTHON) -B tests/test_tooling.py
	$(PYTHON) tools/check_coverage.py coverage.out
openapi:
	@mkdir -p api
	@go run ./cmd/openapi > api/openapi.json.tmp && mv api/openapi.json.tmp api/openapi.json
generate-proto:
	PATH="$(ROOT)/.cache/tools:$$PATH" $(PROTOC) --go_out=. --go_opt=module=gpu-telemetry-pipeline --go-grpc_out=. --go-grpc_opt=module=gpu-telemetry-pipeline proto/queue.proto
integration:
	go test -tags=integration -count=1 -timeout=180s ./tests/integration/...
images:
	@set -e; for service in queue streamer collector gateway migrate; do docker build -f deploy/docker/Dockerfile --build-arg SERVICE=$$service -t $(IMAGE_PREFIX)/$$service:$(IMAGE_TAG) .; done
helm-lint:
	$(HELM) lint deploy/helm/gpu-telemetry
helm-template:
	@mkdir -p .cache
	$(HELM) template gpu-telemetry deploy/helm/gpu-telemetry > .cache/helm-rendered.yaml
migrate:
	go run ./cmd/migrate

.PHONY: smoke
smoke:
	HELM="$(HELM)" KIND="$(KIND)" KUBECTL="$(KUBECTL)" IMAGE_PREFIX="$(IMAGE_PREFIX)" IMAGE_TAG="$(IMAGE_TAG)" $(PYTHON) tools/smoke.py

.PHONY: helm-check
helm-check:
	HELM="$(HELM)" $(PYTHON) tools/check_chart.py
