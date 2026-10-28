ALTER TABLE sf_outbox DROP COLUMN bulk_job_id;

ALTER TABLE sync_runs
    DROP COLUMN resumes,
    DROP COLUMN checkpoint_locator,
    DROP COLUMN checkpoint_watermark,
    DROP COLUMN checkpoint_records;
