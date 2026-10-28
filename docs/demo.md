# Demo walkthrough

A tour of the service against a real org, in about fifteen minutes: export,
incremental sync, import, dead letters, a restart in the middle of a run, and
the dashboard. Commands assume the repository root.

## 1. Prepare

You need Docker and a Salesforce org; a free
[Developer Edition](https://developer.salesforce.com/signup) comes with sample
Accounts and Contacts. Set up a connected app for the client credentials flow
as described in [Salesforce setup](../README.md#salesforce-setup), then:

```bash
cp .env.example .env
```

In `.env`, fill in `SF_CLIENT_ID`, `SF_CLIENT_SECRET`, and `SF_LOGIN_URL`, and set:

```bash
SYNC_OBJECTS=Account:*/30 * * * *,Contact:*/30 * * * *
# Dead-letter failed imports on the first failure, so the demo need not wait for retries.
IMPORT_MAX_ATTEMPTS=1
```

Start the service, Postgres, Prometheus, and Grafana:

```bash
make compose-observability
curl localhost:8080/readyz
# {"status":"ready","checks":{"database":"ok","salesforce":"ok"}}
```

If Salesforce reports `fail`, the service log says why:
`docker compose logs syncd`.

To query the database, open a shell in the Postgres container:

```bash
docker compose exec postgres psql -U sync -d sync
```

## 2. Export

```bash
curl -i -X POST localhost:8080/sync/Account/export
# HTTP/1.1 202 Accepted
# Location: /runs/1
curl localhost:8080/runs/1
```

The run moves from `queued` to `running` to `succeeded`, and `records_ok` counts
the Accounts. In psql:

```sql
SELECT id, data->>'Name' AS name, system_modstamp FROM sf_records
WHERE object_name = 'Account' ORDER BY system_modstamp DESC LIMIT 5;

SELECT * FROM sync_state;   -- the watermark the next run starts from
```

## 3. Incremental sync

Edit one Account in Salesforce, then export again:

```bash
curl -X POST localhost:8080/sync/Account/export
curl localhost:8080/runs/2
```

This time `records_ok` is small: the run read only records changed since the
watermark, plus a five-minute overlap. `?full=true` re-reads everything.

## 4. Import

Write a change to the outbox, as another system would, and import it:

```sql
INSERT INTO sf_outbox (object_name, operation, payload)
VALUES ('Account', 'insert', '{"Name": "Demo Industries", "Industry": "Technology"}');
```

```bash
curl -X POST localhost:8080/sync/Account/import
```

```sql
SELECT id, status, attempts, salesforce_id FROM sf_outbox ORDER BY id DESC LIMIT 1;
```

The entry is `sent`, and `salesforce_id` is the new Account's Id; open it in
Salesforce to check.

## 5. Dead letters

Two changes that cannot succeed:

```sql
INSERT INTO sf_outbox (object_name, operation, payload) VALUES
  ('Contact', 'insert', '{"FirstName": "No", "Shoe_Size__c": 42}'),  -- the field does not exist
  ('Contact', 'insert', '{"FirstName": "Nameless"}');                -- LastName is required
```

```bash
curl -X POST localhost:8080/sync/Contact/import
curl localhost:8080/sync/Contact/dead-letters
```

The first entry never reaches Salesforce: validation against the object's
describe result rejects it before upload. The second is rejected by Salesforce
with `REQUIRED_FIELD_MISSING`. Both are listed with their `last_error`. Once the
cause is fixed, `POST /sync/Contact/dead-letters/retry` sends them again.

## 6. Restart in the middle of a run

Start a full export and stop the service while it runs:

```bash
curl -X POST 'localhost:8080/sync/Contact/export?full=true'
docker compose restart syncd           # graceful: SIGTERM, then start
# or simulate a crash:
# docker compose kill syncd && docker compose start syncd
docker compose logs syncd | grep -E 'suspended|resuming'
```

If the run had already created its Bulk API job, the service leaves the job
running in Salesforce, and after the restart it resumes the run from its last
checkpoint: `GET /runs/{id}` shows `"resumes": 1`, and the run reuses the same
`bulk_job_id`. The sample data of a Developer Edition org is small, so jobs
finish within seconds and the window for the restart is short; the
failure-injection tests in `internal/syncer/resume_test.go` cover the same
paths deterministically.

## 7. API usage guard

The service stops starting runs once the org has used `API_USAGE_THRESHOLD`
percent of its daily API requests. To see it, set a threshold below the org's
current usage, for example `API_USAGE_THRESHOLD=1`, and recreate the service:

```bash
docker compose up -d syncd
curl -X POST localhost:8080/sync/Account/export   # once usage is known: 503 with Retry-After
```

The first run after a start is allowed, because the usage is unknown until
Salesforce reports it. Set the threshold back afterwards.

## 8. Metrics and dashboard

- Grafana: <http://localhost:3000>, which opens on the **Data Sync Service**
  dashboard: runs, records, dead letters, org API usage, Salesforce responses
  and retries, and control API latency.
- Prometheus: <http://localhost:9090>, with the rules from
  `deploy/prometheus/alerts.yml` under **Alerts**.
- Raw metrics: `curl localhost:8080/metrics | grep -E '^(sync|salesforce)_'`.

## 9. Clean up

```bash
make compose-clean   # stops everything and deletes the database volume
```

Records the demo created in Salesforce, such as "Demo Industries", stay there;
delete them in the org if you like.
