-- Each active run is leased by the service instance executing it, which
-- renews heartbeat_at while it works. Another instance may take over a run
-- only once its lease has lapsed, so several instances can share a database.
-- owner is NULL for runs no instance holds, such as one suspended by a
-- shutdown and waiting to be resumed.
ALTER TABLE sync_runs
    ADD COLUMN owner        TEXT,
    ADD COLUMN heartbeat_at TIMESTAMPTZ;
