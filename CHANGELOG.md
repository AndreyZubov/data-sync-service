# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Resumable runs: a run that already has a Bulk API job is suspended on
  shutdown, with the job left running, and resumed at the next start, as is one
  interrupted by a crash. Exports continue from the last result page committed
  to the database; imports collect the outcomes of the ingest jobs they had
  sent instead of sending the entries again. A run is resumed at most three
  times. `GET /runs/{id}` reports `resumes`.
- Outbox entries keep their ingest job until their outcome is known, so the
  next run collects the outcome of a run that failed after closing a job
  instead of uploading the entries again: inserts are no longer duplicated.
- Several instances can share a database: every active run is leased by the
  instance executing it, renewed every 20 seconds, and taken over by another
  instance once the lease lapses. Writes of a run's state are fenced by the
  lease.
- `go.mod` requires the Go 1.26.10 toolchain, which fixes standard library
  vulnerabilities reported by `govulncheck`.
- `API_RATE_LIMIT` is enforced: Salesforce requests, retries and polling
  included, go through a token bucket.
- Org API usage tracking: the `Sforce-Limit-Info` figures of every response are
  exported as `salesforce_api_usage_requests` and `salesforce_api_usage_limit`,
  and no new run starts while usage is above `API_USAGE_THRESHOLD` (80% by
  default); the control API answers `503` with `Retry-After`.
- `salesforce_requests_total`, `salesforce_request_retries_total`,
  `sync_runs_resumed_total`, and `sync_last_success_timestamp_seconds` metrics.
- Prometheus alert rules, including one for exports that have not succeeded for
  six hours, with unit tests run by `make check-alerts` and CI; a Grafana
  dashboard; and `make compose-observability` to run both next to the service.
- Architecture documentation with diagrams, and a demo walkthrough.
- Failure-injection tests that stop the service in the middle of exports and
  imports and check that a new instance finishes them, and tests that make the
  fake org fail chosen requests (`sftest.InjectFault`) or the database refuse
  writes part-way through a run.

- Import pipeline: changes that other systems insert into the `sf_outbox` table
  (`insert`, `update`, `upsert` by external ID, or `delete`) are sent to Salesforce
  through Bulk API 2.0 ingest jobs by `POST /sync/{object}/import` and on every
  scheduled tick that finds due entries. Entries are validated against the
  object's describe result before upload, changes to one record are sent one at a
  time and in order, and per-record results are matched back to their entries.
- Retries and dead letters: rejected records are retried with exponential backoff
  up to `IMPORT_MAX_ATTEMPTS` runs, then dead-lettered. `GET
  /sync/{object}/dead-letters` lists them and `POST
  /sync/{object}/dead-letters/retry` requeues them.
- `IMPORT_MAX_RECORDS` and `IMPORT_MAX_ATTEMPTS` settings, `records_failed` in run
  responses, and the `sync_records_failed_total` and `sync_dead_letters_total`
  metrics.
- Export pipeline: `POST /sync/{object}/export` runs a Bulk API 2.0 query job over
  every queryable field of the object and upserts the results into the
  `sf_records` table as JSONB. Results are streamed page by page and written by a
  worker pool through `COPY` and `INSERT ... ON CONFLICT`, so memory use stays flat;
  newer records are never overwritten by older ones.
- Incremental sync: each successful export advances the object's `SystemModstamp`
  watermark, and later runs read only records changed since then, with a
  five-minute overlap. `?full=true` re-reads everything. Deleted records still in
  the Recycle Bin are exported with `is_deleted = true`.
- Scheduled exports from the cron expressions in `SYNC_OBJECTS`.
- Run records: `GET /runs/{id}` reports status, trigger, record count, Bulk API
  job ID, timestamps, and errors. One export per object runs at a time (`409
  Conflict` otherwise), and at most `MAX_CONCURRENT_JOBS` run at once.
- Graceful shutdown stops the schedule and active runs; runs interrupted by a
  crash before they had a Salesforce job are marked failed at startup.
- PostgreSQL schema migrations, embedded and applied at startup.
- `/readyz` checks the database as well as Salesforce authentication.
- `sync_runs_total`, `sync_records_total`, `sync_run_duration_seconds`, and
  `sync_active_runs` Prometheus metrics.
- Salesforce OAuth 2.0 client credentials token provider: caches the token,
  refreshes it a minute before expiry, shares one token request among concurrent
  callers, and never prints the token or client secret.
- Salesforce REST client: retries `429` and `503` for every method and other `5xx`
  and network errors for idempotent methods, with exponential backoff, full
  jitter, and `Retry-After`; re-authenticates once on `401`; decodes Salesforce
  error responses.
- `/readyz` reports `503 Service Unavailable` until Salesforce authentication
  succeeds, with per-check results in the body.
- `SF_TOKEN_TTL` setting for the assumed access-token lifetime.
- Control API on chi with `/healthz`, `/readyz`, and `/metrics`.
- Environment-based configuration with startup validation: required Salesforce
  credentials and database URL, an `https` My Domain `SF_LOGIN_URL` (`login.salesforce.com` and
  `test.salesforce.com` are rejected, as they do not support the client
  credentials flow), an API version of the form `60.0`, positive worker,
  concurrency, and rate-limit values, Salesforce API names, unique objects, and
  standard cron expressions in `SYNC_OBJECTS`. All problems are reported at once.
- `SHUTDOWN_TIMEOUT` setting for the graceful-shutdown grace period.
- Object allowlist: `/sync/{object}/...` accepts only objects listed in
  `SYNC_OBJECTS`, case-insensitively, and answers `404 Not Found` otherwise.
- Structured access log for every request and the `http_requests_total` and
  `http_request_duration_seconds` Prometheus metrics, labelled by route pattern.
- Read, write, and idle timeouts on the HTTP server.
- Multi-arch (`linux/amd64`, `linux/arm64`) container image on distroless.
- Release workflow: binaries and checksums via GoReleaser, and a container image
  with SBOM and provenance attestations published to GitHub Container Registry.
- CodeQL analysis and a pinned `govulncheck` in CI; `make vuln` runs it locally.
- `make compose-app` to run the service and Postgres together in containers.

[Unreleased]: https://github.com/AndreyZubov/data-sync-service/commits/main
