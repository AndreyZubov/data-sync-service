# Salesforce Bulk Data Sync Service

[![CI](https://github.com/AndreyZubov/data-sync-service/actions/workflows/ci.yml/badge.svg)](https://github.com/AndreyZubov/data-sync-service/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/AndreyZubov/data-sync-service)](https://goreportcard.com/report/github.com/AndreyZubov/data-sync-service)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

> A Go service for reliable, high-volume data synchronisation between **Salesforce**
> and **PostgreSQL** over the **Bulk API 2.0**. It runs scheduled and on-demand sync
> jobs, manages async job lifecycles, respects org API limits, and recovers cleanly
> from failures.

**Status:** active development. The export direction works end to end: the service
authenticates with OAuth client credentials, exports configured objects through
Bulk API 2.0 query jobs into PostgreSQL, streams results with flat memory use, and
syncs incrementally from `SystemModstamp` watermarks, on a cron schedule or on
demand, with every run recorded and inspectable. Import (Postgres → Salesforce) and
org API-limit awareness are next on the [roadmap](#roadmap); until import lands,
`/sync/{object}/import` answers `501 Not Implemented`.

---

## Why this project

Keeping Salesforce data in sync with a downstream store is one of the most common
reasons companies put Go next to a CRM: it is fast, concurrent, and cheap to run.
A bulk sync service also touches the parts of backend engineering that are worth
demonstrating properly — HTTP API clients, asynchronous job orchestration, worker
pools, database work, retries, and rate limiting — rather than a CRUD app.

A companion project, [pubsub-event-processor](https://github.com/AndreyZubov/pubsub-event-processor),
covers the real-time half of the same problem via the Salesforce Pub/Sub API.

---

## Architecture

```
   +------------- Scheduler / Control API -------------+
   |                                                   |
on-demand                                          cron tick
   |                                                   |
   v                                                   v
                 [ Sync Orchestrator ]
                          |
        +-----------------+-----------------+
        v                                   v
   [ Export ]                          [ Import ]
   Bulk API query job                  Bulk API ingest job
   create -> poll -> fetch             stage -> upload -> poll
        |                                   |
        v                                   v
   [ CSV parser ]                      [ Result collector ]
   worker pool                         failed-record handling
        |                                   |
        v                                   v
   [ Postgres upsert ]                 [ Dead letter ]
        |
        v
   [ Watermark store ]  <- SystemModstamp per object, for incremental runs
```

Cross-cutting: environment-based configuration, structured zap logging, a
Prometheus registry, an API rate limiter aware of org limits, and graceful
shutdown across all goroutines.

---

## Quick start

### Prerequisites

- Go 1.26.3 or newer (the version in `go.mod`).
- Docker — used for the local Postgres container, the integration tests, and
  `golangci-lint`.
- A Salesforce org (a free Developer Edition works) with a connected app set up for
  the client credentials flow — see [Salesforce setup](#salesforce-setup). The
  service refuses to start without its credentials; without a reachable org it runs,
  but `/readyz` reports not ready.

### Run it

```bash
cp .env.example .env     # fill in credentials; .env is git-ignored
make compose-up          # start local Postgres
make run                 # or: go run ./cmd/syncd
```

The control API listens on `:8080` by default:

```bash
curl localhost:8080/healthz   # liveness
curl localhost:8080/readyz    # readiness: 503 until Salesforce and Postgres are usable
curl localhost:8080/metrics   # Prometheus metrics
```

With `SYNC_OBJECTS=Account:0 * * * *`, Accounts are exported every hour; to export
now and follow the run:

```bash
curl -X POST localhost:8080/sync/Account/export   # 202 Accepted, Location: /runs/1
curl localhost:8080/runs/1                        # status and record count
```

To run the service in a container next to Postgres instead, use
`make compose-app`; it reads the same `.env`. `make compose-down` stops everything.

### Salesforce setup

1. In Setup, create a connected app (or an external client app) with OAuth enabled
   and the `api` scope.
2. Enable **Client Credentials Flow** and choose a **Run As** user. The service acts
   with that user's permissions, so give it only the object access it needs.
3. Copy the consumer key and secret into `SF_CLIENT_ID` and `SF_CLIENT_SECRET`.
4. Set `SF_LOGIN_URL` to the org's **My Domain** URL, found under Setup → My Domain,
   e.g. `https://example.my.salesforce.com`. The client credentials flow is not
   available on `login.salesforce.com` or `test.salesforce.com`, and the service
   rejects those hosts at startup.
5. Keep `SF_TOKEN_TTL` below the connected app's session timeout. Salesforce does not
   always state a token's lifetime, so the service assumes this one; a session that
   ends earlier is detected on the next `401` and replaced with a fresh token.

`make help` lists every target.

### Released builds

Tagged releases publish `linux` and `darwin` binaries for `amd64` and `arm64` on the
[releases page](https://github.com/AndreyZubov/data-sync-service/releases), and a
multi-arch image with SBOM and provenance attestations:

```bash
docker run --rm -p 8080:8080 --env-file .env ghcr.io/andreyzubov/data-sync-service:<version>
```

---

## HTTP control API

| Method | Path                    | Purpose                              |
|--------|-------------------------|--------------------------------------|
| `POST` | `/sync/{object}/export` | Start an export run (see below)      |
| `POST` | `/sync/{object}/import` | Start an import run *(not yet: 501)* |
| `GET`  | `/runs/{id}`            | Inspect a run                        |
| `GET`  | `/healthz`              | Liveness                             |
| `GET`  | `/readyz`               | Readiness (see below)                |
| `GET`  | `/metrics`              | Prometheus metrics                   |

`{object}` must be one of the objects listed in `SYNC_OBJECTS`; it is matched
case-insensitively, as Salesforce API names are. Any other value is answered with
`404 Not Found` before it can reach SOQL or SQL. Requests are bounded by a 60-second
handler timeout, after which the client receives `504 Gateway Timeout`.

`POST /sync/{object}/export` queues a run and answers `202 Accepted` with the run
and a `Location: /runs/{id}` header; the export itself runs in the background.
Add `?full=true` to ignore the watermark and re-read every record. If the object
is already being exported, the answer is `409 Conflict` with the active `run_id`.

`GET /runs/{id}` returns the run's `status` (`queued`, `running`, `succeeded`,
`failed`, or `cancelled`), `trigger` (`manual` or `schedule`), `records_ok`, the
Salesforce `bulk_job_id`, timestamps, and for unsuccessful runs an `error`.
`records_ok` grows while the run is in progress.

`/readyz` answers `200` when every dependency check passes and `503` otherwise,
with a per-check `ok` or `fail` in the body. The checks are Salesforce
authentication, which reuses the cached access token, so a healthy probe makes no
Salesforce call and spends none of the org's API quota, and a Postgres ping.
Failure details go to the log, not the response.

Every request is logged as one structured entry carrying the request ID, route
pattern, status, response size, and duration. Server errors are logged at `error`,
successful health and metrics probes at `debug`, everything else at `info`.

`/metrics` exposes Go runtime and process collectors plus:

| Metric | Labels | Meaning |
|--------|--------|---------|
| `http_requests_total` | `method`, `route`, `status` | Control API requests |
| `http_request_duration_seconds` | `method`, `route` | Control API request latency |
| `sync_runs_total` | `object`, `direction`, `status` | Finished sync runs |
| `sync_records_total` | `object`, `direction` | Records written to Postgres |
| `sync_run_duration_seconds` | `object`, `direction` | Run duration, excluding time queued |
| `sync_active_runs` | — | Runs currently executing |

The `route` label is the route pattern (for example `/runs/{id}`), and requests that
match no route share the `unmatched` label, so client-supplied paths never create new
time series.

---

## How exports work

1. **Fields.** The object's describe result decides what is read: every field
   except compound `address` and `location` fields (their components are exported
   as separate fields), `base64` fields, and deprecated ones. Bulk API queries
   cannot return the excluded types.
2. **Query job.** The service creates a Bulk API 2.0 query job and polls it with a
   doubling interval from one to thirty seconds. Objects with an `IsDeleted` field
   are read with `queryAll`, so records in the Recycle Bin arrive with
   `is_deleted = true`. Records removed from the Recycle Bin (hard-deleted or
   purged) are not detected.
3. **Streaming.** Result pages are read straight from the network and parsed as
   CSV. Records are grouped into batches of 2,000 and handed over a bounded channel
   to `WORKER_COUNT` workers. When Postgres is slow, the download slows down
   instead of buffering, so memory use stays flat whatever the result size.
4. **Upsert.** Each batch is one transaction: `COPY` into a temporary staging
   table, then `INSERT ... ON CONFLICT DO UPDATE` into `sf_records`. A stored
   record is only replaced by a version with an equal or newer `SystemModstamp`,
   so a late, stale write cannot overwrite fresher data. The run's `records_ok` is
   updated in the same transaction.
5. **Watermark.** When the run succeeds, the highest `SystemModstamp` it exported
   becomes the object's watermark, in the same transaction that marks the run
   `succeeded`. The next run selects `SystemModstamp >=` the watermark minus five
   minutes. The overlap catches records whose long-running transaction committed a
   timestamp older than one already exported; re-reading them is harmless, because
   upserts are idempotent. Objects without `SystemModstamp` are fully exported every
   time.

An object has at most one active export, and at most `MAX_CONCURRENT_JOBS` runs
execute at once; further runs wait in the `queued` state. A scheduled tick that
finds the previous run still active is skipped.

On shutdown the service stops the schedule, cancels active runs, aborts their
Salesforce jobs, and records them as `cancelled`. Runs left active by a crash are
marked `failed` at the next startup. A failed or cancelled export is safe to repeat:
its records were upserted idempotently and the watermark did not move. Runs are not
resumed yet (milestone 6). The service assumes a single instance per database.

### Data model

Migrations are embedded in the binary and applied at startup.

| Table | Contents |
|-------|----------|
| `sf_records` | One row per exported record: `object_name`, `id`, `system_modstamp`, `is_deleted`, `synced_at`, and `data`, a JSONB object keyed by field API name |
| `sync_runs` | Every run with its status, trigger, Bulk API job ID, counts, error, and timestamps |
| `sync_state` | The watermark of each object and the run that set it |

A generic JSONB table means new objects and fields need no schema change. In
`data`, empty values are `null`, checkboxes are booleans, numeric fields are JSON
numbers with their exact digits, and everything else, dates included, is a string
as Salesforce sent it:

```sql
SELECT id, data->>'Name' AS name, (data->>'AnnualRevenue')::numeric AS revenue
FROM sf_records
WHERE object_name = 'Account' AND NOT is_deleted;
```

---

## Configuration

Configuration comes from the environment only. See [`.env.example`](.env.example).
It is validated at startup; the service refuses to start and lists every invalid
value at once.

| Variable | Default | Purpose |
|----------|---------|---------|
| `HTTP_ADDR` | `:8080` | Control API listen address |
| `SHUTDOWN_TIMEOUT` | `15s` | How long in-flight requests and cancelled runs may take to stop after `SIGINT`/`SIGTERM`; a Go duration, must be positive |
| `SF_CLIENT_ID` | — | **Required.** Connected app consumer key |
| `SF_CLIENT_SECRET` | — | **Required.** Connected app consumer secret |
| `SF_LOGIN_URL` | — | **Required.** The org's My Domain URL, e.g. `https://example.my.salesforce.com`; must be `https` |
| `SF_API_VERSION` | `60.0` | Salesforce API version, in the form `60.0` |
| `SF_TOKEN_TTL` | `30m` | Assumed access-token lifetime when Salesforce does not state one; at least `5m`, and below the connected app's session timeout |
| `DATABASE_URL` | — | **Required.** Postgres DSN; the connection pool is raised to at least `MAX_CONCURRENT_JOBS × WORKER_COUNT + 4` connections |
| `SYNC_OBJECTS` | — | Objects and schedules, e.g. `Account:*/15 * * * *,Contact:0 * * * *` (see below) |
| `MAX_CONCURRENT_JOBS` | `4` | Runs executing at once; further runs wait queued. At least 1 |
| `API_RATE_LIMIT` | `10` | Request-per-second ceiling; at least 1. Not enforced yet (milestone 6) |
| `WORKER_COUNT` | `4` | Upsert workers per run; at least 1 |
| `LOG_LEVEL` | `info` | `debug`, `info`, `warn`, or `error` |

Each `SYNC_OBJECTS` entry is `Object:cron`:

- `Object` is a Salesforce API name — a letter followed by letters, digits, or
  underscores, such as `Account`, `Invoice__c`, or `ns__Order__c`. Names must be
  unique, ignoring case.
- `cron` is a standard five-field expression (minute, hour, day of month, month,
  day of week); descriptors such as `@hourly` are also accepted. Schedules use the
  server's local time zone, which is UTC in the container image.

---

## Roadmap

| Milestone | Scope | Status |
|-----------|-------|--------|
| 1 — Skeleton | Config, logging, control API, health endpoints, Docker, CI | Done |
| 2 — Auth + HTTP client | OAuth client credentials, Salesforce REST client with retries, readiness check | Done |
| 3 — Export pipeline | Bulk API query jobs, CSV streaming, Postgres upsert, run records | Done |
| 4 — Incremental sync | `SystemModstamp` watermarks, scheduled runs | Done |
| 5 — Import + dead letter | Bulk API ingest jobs, failed-record collection | Planned |
| 6 — Resilience | Rate limiting, API-limit tracking, resumable runs | Planned |
| 7 — Tests and polish | Failure-injection tests, metrics dashboard, architecture docs | Planned |

---

## Project layout

```
cmd/syncd/            entry point
internal/
  api/                chi router, middleware, control and health endpoints
  config/             environment configuration loader and validation
  logging/            zap logger constructor
  retry/              exponential backoff with jitter, Retry-After parsing
  salesforce/         OAuth token provider, REST client, describe, Bulk API query jobs
  salesforce/sftest/  in-process fake Salesforce org for tests
  store/              Postgres access: migrations, runs, watermarks, record upserts
  store/storetest/    throwaway migrated databases for tests
  syncer/             run orchestration, scheduling, and the export pipeline
scripts/hooks/        git pre-commit hook
.github/workflows/    CI, CodeQL, and the tag-triggered release
.goreleaser.yaml      release binaries and checksums
```

---

## Development

```bash
make help            # list all targets
make build           # compile into bin/syncd
make test            # go test ./... -race -count=1
make cover           # tests plus an HTML coverage report
make lint            # go vet + golangci-lint (via Docker)
make fmt             # gofumpt + gci import ordering
make config-verify   # validate .golangci.yml against the linter schema
make vuln            # govulncheck against dependencies and the standard library
make install-hooks   # pre-commit hook running lint and tests
make docker-build    # build the container image
make compose-app     # run the service and Postgres in containers
```

Tests that need PostgreSQL start a container through testcontainers, so they need
Docker. To use an existing server instead, set `TEST_DATABASE_URL` to a DSN whose
role may create databases; every test gets a fresh database. Without either, those
tests are skipped locally; in CI they fail instead.

`golangci-lint` runs from a pinned Docker image, so no local installation is
needed and CI uses the same build. Linters enabled include `errcheck`, `gosec`,
`revive`, `staticcheck`, `errorlint`, `bodyclose`, `contextcheck`, and `nilerr`;
formatting is `gofumpt` plus `gci` import grouping.

CI runs lint, a `go mod tidy` cleanliness check, tests under the race detector
with a coverage summary, `govulncheck`, and a Docker image build; CodeQL analyses
every push and pull request. Actions are pinned to commit SHAs, and Dependabot keeps
them current.

### Releasing

Pushing a tag such as `v0.1.0` runs the release workflow: tests, then GoReleaser
publishes the GitHub release with binaries and checksums, and the container image
is pushed to `ghcr.io`. Move the `Unreleased` entries in
[CHANGELOG.md](CHANGELOG.md) under the new version before tagging.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Security issues go through
[private reporting](SECURITY.md), not public issues.

## License

[MIT](LICENSE).
