CREATE TABLE sync_runs (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    object_name    TEXT        NOT NULL,
    direction      TEXT        NOT NULL CHECK (direction IN ('export', 'import')),
    trigger        TEXT        NOT NULL CHECK (trigger IN ('manual', 'schedule')),
    status         TEXT        NOT NULL CHECK (status IN ('queued', 'running', 'succeeded', 'failed', 'cancelled')),
    full_sync      BOOLEAN     NOT NULL DEFAULT false,
    bulk_job_id    TEXT,
    records_ok     BIGINT      NOT NULL DEFAULT 0,
    records_failed BIGINT      NOT NULL DEFAULT 0,
    error          TEXT,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at     TIMESTAMPTZ,
    finished_at    TIMESTAMPTZ
);

-- At most one active run per object and direction: two concurrent exports of
-- the same object would only duplicate API usage and race on the watermark.
CREATE UNIQUE INDEX sync_runs_one_active
    ON sync_runs (object_name, direction)
    WHERE status IN ('queued', 'running');

CREATE INDEX sync_runs_object_created ON sync_runs (object_name, created_at DESC);

CREATE TABLE sync_state (
    object_name    TEXT PRIMARY KEY,
    -- Highest SystemModstamp of a successfully exported record.
    last_watermark TIMESTAMPTZ,
    last_run_id    BIGINT REFERENCES sync_runs (id),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Exported records of every object, as JSON keyed by field API name, so new
-- objects and fields need no schema change.
CREATE TABLE sf_records (
    object_name     TEXT        NOT NULL,
    id              TEXT        NOT NULL,
    system_modstamp TIMESTAMPTZ,
    is_deleted      BOOLEAN     NOT NULL DEFAULT false,
    data            JSONB       NOT NULL,
    synced_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (object_name, id)
);
