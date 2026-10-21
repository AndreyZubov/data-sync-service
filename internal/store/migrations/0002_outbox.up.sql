-- Changes waiting to be written to Salesforce. Other systems insert rows here;
-- import runs send them through Bulk API ingest jobs and record the outcome.
CREATE TABLE sf_outbox (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    object_name       TEXT        NOT NULL,
    operation         TEXT        NOT NULL CHECK (operation IN ('insert', 'update', 'upsert', 'delete')),
    -- Field matched by an upsert; NULL for the other operations.
    external_id_field TEXT,
    -- Field values keyed by API name; null clears a field.
    payload           JSONB       NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    status            TEXT        NOT NULL DEFAULT 'pending'
                                  CHECK (status IN ('pending', 'processing', 'sent', 'dead')),
    attempts          INTEGER     NOT NULL DEFAULT 0,
    next_attempt_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error        TEXT,
    run_id            BIGINT REFERENCES sync_runs (id),
    salesforce_id     TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((operation = 'upsert') = (external_id_field IS NOT NULL)),
    -- Identifies the Salesforce record an entry changes, so that changes to one
    -- record are sent one at a time and in order. Inserts create new records
    -- and have none.
    record_key        TEXT GENERATED ALWAYS AS (
        CASE operation
            WHEN 'upsert' THEN external_id_field || ':' || (payload ->> external_id_field)
            WHEN 'insert' THEN NULL
            ELSE 'Id:' || (payload ->> 'Id')
        END
    ) STORED
);

CREATE INDEX sf_outbox_unsent ON sf_outbox (object_name, record_key, id) WHERE status IN ('pending', 'processing');
CREATE INDEX sf_outbox_dead ON sf_outbox (object_name, id) WHERE status = 'dead';
