# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
- Control API on chi with `/healthz`, `/readyz`, `/metrics`, and stub
  `/sync/{object}/export`, `/sync/{object}/import`, and `/runs/{id}` routes that
  answer `501 Not Implemented`.
- Environment-based configuration with startup validation: required Salesforce
  credentials, an `https` My Domain `SF_LOGIN_URL` (`login.salesforce.com` and
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
