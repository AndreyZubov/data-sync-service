# Contributing

Thanks for taking a look. This is a personal portfolio project, so the scope is
deliberately narrow — but issues, questions, and pull requests are welcome.

## Getting set up

```bash
git clone https://github.com/AndreyZubov/data-sync-service.git
cd data-sync-service

cp .env.example .env     # fill in Salesforce credentials if you need them
make compose-up          # local Postgres
make install-hooks       # pre-commit lint + test
make run
```

`make help` lists every available target.

Docker is used for linting so you do not need a local `golangci-lint`
installation; the version is pinned in the `Makefile` and in CI so both run the
same build.

## Before opening a pull request

```bash
make fmt     # gofumpt + gci
make lint    # go vet + golangci-lint
make test    # go test ./... -race
```

Tests that need PostgreSQL start a container with testcontainers, so keep Docker
running, or point `TEST_DATABASE_URL` at a server whose role may create
databases. Without either, those tests are skipped locally but fail in CI.

CI runs the same checks plus `go mod tidy` verification, `govulncheck`
(`make vuln` locally), CodeQL, and a Docker image build. The pre-commit hook installed by `make install-hooks` runs
lint and tests on any commit touching Go code.

User-visible changes get an entry under `Unreleased` in
[CHANGELOG.md](CHANGELOG.md).

## Code style

- Standard Go formatting via `gofumpt`.
- Imports grouped by `gci`: standard library, third party, then this module.
- Every exported symbol carries a doc comment; every package has a package comment.
- Errors are wrapped with `fmt.Errorf("context: %w", err)` rather than returned bare.
- New behaviour comes with table-driven tests.

## Commit messages

[Conventional Commits](https://www.conventionalcommits.org/), matching the
existing history:

```
feat(export): add Bulk API query job poller
fix(config): reject empty cron expression
docs: document the readiness probe
```

## Reporting bugs

Open an issue with the version (`syncd` logs it at startup), the configuration
involved with secrets removed, and what you expected to happen. Never paste real
Salesforce credentials, org IDs, or customer data into an issue.
