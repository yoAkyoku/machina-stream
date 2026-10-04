CREATE TABLE IF NOT EXISTS machines (
    machine_id text PRIMARY KEY,
    factory_id text NOT NULL,
    line_id text NOT NULL,
    sequence_no bigint NOT NULL DEFAULT 0,
    latest_event_id uuid,
    event_time timestamptz,
    last_seen_at timestamptz,
    metrics jsonb,
    machine_status text,
    error_code text,
    sequence_gap_count bigint NOT NULL DEFAULT 0,
    sequence_conflict_count bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS telemetry_events (
    event_id uuid PRIMARY KEY,
    payload_hash char(64) NOT NULL,
    factory_id text NOT NULL,
    line_id text NOT NULL,
    machine_id text NOT NULL,
    sequence_no bigint NOT NULL,
    event_time timestamptz NOT NULL,
    received_at timestamptz NOT NULL,
    processed_at timestamptz NOT NULL,
    payload jsonb NOT NULL,
    sequence_outcome text NOT NULL CHECK (
        sequence_outcome IN ('projected', 'gap', 'sequence_conflict', 'out_of_order')
    ),
    replayed boolean NOT NULL DEFAULT false
);

CREATE INDEX IF NOT EXISTS telemetry_events_machine_received_idx
    ON telemetry_events (machine_id, received_at DESC, sequence_no DESC);
CREATE INDEX IF NOT EXISTS telemetry_events_machine_sequence_idx
    ON telemetry_events (machine_id, sequence_no);
CREATE INDEX IF NOT EXISTS telemetry_events_received_idx
    ON telemetry_events (received_at DESC);

CREATE TABLE IF NOT EXISTS alert_episodes (
    episode_id uuid PRIMARY KEY,
    factory_id text NOT NULL,
    line_id text NOT NULL,
    machine_id text NOT NULL,
    rule_id text NOT NULL,
    rule_version text NOT NULL,
    severity text NOT NULL CHECK (severity IN ('warning', 'critical')),
    status text NOT NULL CHECK (status IN ('OPEN', 'RESOLVED')),
    opened_at timestamptz NOT NULL,
    last_seen_at timestamptz NOT NULL,
    resolved_at timestamptz,
    occurrence_count bigint NOT NULL DEFAULT 1,
    latest_value double precision,
    latest_event_id uuid,
    diagnostic_code text,
    CHECK ((status = 'OPEN' AND resolved_at IS NULL) OR
           (status = 'RESOLVED' AND resolved_at IS NOT NULL))
);

CREATE UNIQUE INDEX IF NOT EXISTS alert_episodes_one_open_rule_per_machine_idx
    ON alert_episodes (machine_id, rule_id) WHERE status = 'OPEN';
CREATE INDEX IF NOT EXISTS alert_episodes_status_seen_idx
    ON alert_episodes (status, last_seen_at DESC);

CREATE TABLE IF NOT EXISTS alert_recovery_streaks (
    machine_id text NOT NULL,
    rule_id text NOT NULL,
    streak integer NOT NULL CHECK (streak >= 0),
    PRIMARY KEY (machine_id, rule_id)
);

CREATE TABLE IF NOT EXISTS dead_letter_audit (
    source_topic text NOT NULL CHECK (source_topic <> ''),
    source_partition integer NOT NULL CHECK (source_partition >= 0),
    source_offset bigint NOT NULL CHECK (source_offset >= 0),
    event_id uuid,
    error_code text NOT NULL,
    failed_at timestamptz NOT NULL,
    replayed_at timestamptz,
    PRIMARY KEY (source_topic, source_partition, source_offset)
);
