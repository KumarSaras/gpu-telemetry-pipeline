# syntax=docker/dockerfile:1
FROM golang:1.26.4-alpine@sha256:3ad57304ad93bbec8548a0437ad9e06a455660655d9af011d58b993f6f615648 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY api ./api
COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations
ARG SERVICE=queue
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/service ./cmd/${SERVICE}
FROM gcr.io/distroless/static-debian13:nonroot@sha256:18f4cc26c9ffa7f20a359298be7deadac5c607c8e03a02b8119f1a5fc2f82c99
WORKDIR /app
COPY --from=build /out/service /app/service
COPY --chmod=0444 data/telemetry.csv /app/telemetry.csv
ENV CSV_PATH=/app/telemetry.csv
USER 65532:65532
ENTRYPOINT ["/app/service"]
