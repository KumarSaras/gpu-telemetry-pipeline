CREATE TABLE telemetry (
    streamer_session text NOT NULL CHECK (streamer_session ~ '^[0-9a-f]{32}$'),
    sequence bigint NOT NULL CHECK (sequence > 0),
    timestamp timestamptz NOT NULL,
    uuid text NOT NULL CHECK (uuid <> ''),
    metric_name text NOT NULL CHECK (metric_name <> ''),
    value numeric NOT NULL,
    gpu_id text NOT NULL,
    device text NOT NULL,
    model_name text NOT NULL,
    hostname text NOT NULL,
    container text NOT NULL,
    pod text NOT NULL,
    namespace text NOT NULL,
    labels_raw text NOT NULL,
    payload_hash bytea NOT NULL CHECK (octet_length(payload_hash) = 32),
    PRIMARY KEY (streamer_session, sequence)
);
CREATE INDEX telemetry_uuid_timestamp_idx ON telemetry (uuid, timestamp);
