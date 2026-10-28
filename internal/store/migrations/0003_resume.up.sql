-- A run that already has a Salesforce job when the service stops is resumed
-- at the next start instead of being abandoned. resumes counts how often that
-- has happened, so a run that keeps crashing the service is eventually failed.
ALTER TABLE sync_runs
    ADD COLUMN resumes              INTEGER NOT NULL DEFAULT 0,
    -- Export progress: the result page to continue from, and the watermark and
    -- record count of the pages before it, all committed to sf_records.
    ADD COLUMN checkpoint_locator   TEXT,
    ADD COLUMN checkpoint_watermark TIMESTAMPTZ,
    ADD COLUMN checkpoint_records   BIGINT  NOT NULL DEFAULT 0;

-- The ingest job an entry was uploaded in, so a resumed run collects its
-- result instead of sending it again.
ALTER TABLE sf_outbox ADD COLUMN bulk_job_id TEXT;
