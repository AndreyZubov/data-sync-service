# Security Policy

## Supported versions

This project is in early development and has no released versions yet. Security
fixes are applied to the `main` branch.

## Reporting a vulnerability

Please **do not open a public issue** for a security problem.

Report it privately through GitHub's
[private vulnerability reporting](https://github.com/AndreyZubov/data-sync-service/security/advisories/new)
on this repository. Include what you found, how to reproduce it, and the impact
you expect. You will get an acknowledgement within a few days; since this is a
personal project maintained in spare time, a fix may take longer than that.

## Scope

This service holds Salesforce OAuth credentials and a PostgreSQL connection
string, and it moves customer data between the two. Reports about credential
handling, token lifetime, logging of sensitive fields, SQL injection, or
transport security are especially relevant.

## Handling credentials

- Configuration comes from environment variables only; `.env` is git-ignored and
  `.env.example` holds placeholders, never real values.
- Never commit a `.env`, an org ID, a Consumer Secret, or a Bulk API result file
  containing customer records.
- The container runs as a non-root user on a `distroless` base with no shell.
