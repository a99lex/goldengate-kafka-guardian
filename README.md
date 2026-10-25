# GoldenGate & Kafka Guardian v0.2

Read-only live monitoring plus the original simulation demo (`demo.html`).

## What works

- Python collector reads configured GoldenGate Microservices Extract/Replicat status and lag through REST API v2.
- Kafka collector queries configured topics and consumer groups: partitions, committed offsets, end offsets and offset lag. It never subscribes, consumes or commits offsets.
- Cloudflare Worker accepts authenticated snapshots, stores them in D1 and serves authenticated snapshot/history endpoints.
- Live dashboard refreshes every 15 seconds; source failures, unavailable data and stale snapshots are visible. Tokens remain in tab memory, not localStorage.
- Source credentials stay on the collector host; separate ingest and read tokens protect the API. Static dashboard shell and simulation demo are public.

This implementation has automated mocked tests. It has NOT been tested against your GoldenGate, Kafka or Cloudflare account and is not deployed by this commit. No throughput/JMX adapter, automatic discovery, external notifications, RBAC or production start/stop controls are implemented.

## Cloudflare Worker deployment

Run from the repository root on a machine where you can sign into Cloudflare:

```bash
npm test
npm run build
npx wrangler login
npx wrangler d1 create guardian
```

Copy the actual returned database ID into `wrangler.jsonc` by adding:

```json
"d1_databases": [{"binding":"DB","database_name":"guardian","database_id":"ACTUAL_ID_FROM_CLOUDFLARE"}]
```

Keep the other existing fields. Then initialize D1 and set two DIFFERENT random secrets (at least 32 characters each), using the interactive secret prompts:

```bash
npx wrangler d1 execute guardian --remote --file schema.sql
npx wrangler secret put INGEST_TOKEN
npx wrangler secret put READ_TOKEN
npm run deploy
```

Generate secrets with a password manager. Never commit them. For Workers Builds / Git integration: build command `npm run build`, deploy command `npx wrangler deploy`, repository root `/`. Configure DB and both secrets before live collection. Use Workers, not a Pages-only deployment.

D1 stores snapshots for seven days, cleaned on ingestion; `/api/history?collector=default` returns the last 24 snapshots. Retention and D1 write/read quotas should be sized for your interval and collector count. One active collector per collector ID. Clock synchronization is required (ingest accepts timestamps within five minutes).

## Linux collector

Python 3.10+ and network access to GoldenGate Administration Service, all advertised Kafka brokers and the Worker's HTTPS URL are required. Kafka needs SSL or SASL_SSL. Configure trusted CA certificates for private PKI; certificate validation stays enabled. GoldenGate must have HTTPS configured (or a trusted internal HTTPS reverse proxy).

```bash
python3 -m venv .venv
.venv/bin/pip install -r collector/requirements.txt
cp collector/config.example.json collector/config.json
```

Edit `collector/config.json` with your deployment URLs, process names, Kafka bootstrap servers, groups and topics. These names are examples only. For Oracle private CA set `ca_file` on the deployment; for Kafka use `ssl.ca.location` in `client`. For Kafka mTLS use `ssl.certificate.location`, `ssl.key.location` and the appropriate secret environment reference if necessary.

Set `GG_USER`, `GG_PASSWORD`, `KAFKA_USER`, `KAFKA_PASSWORD` and `GUARDIAN_INGEST_TOKEN` in the collector service environment. Use the environment variable names defined in your configuration. Do not put credentials in config JSON, source control or browser code.

```bash
.venv/bin/python collector/guardian.py --config collector/config.json --dry-run
.venv/bin/python collector/guardian.py --config collector/config.json --once
.venv/bin/python collector/guardian.py --config collector/config.json
```

Dry-run prints infrastructure names and metrics locally; it never uploads. The ingest token must match the Worker's INGEST_TOKEN. A failed source is reported separately so remaining sources can still report. Failed uploads are logged without response bodies or credentials; the daemon retries with the next collection cycle.

For systemd: install the checkout at `/opt/guardian`, create a dedicated `guardian` system user, place config at `/etc/guardian/config.json`, and environment assignments in `/etc/guardian/collector.env` (root-owned, mode 0600). Install `collector/guardian.service`, then enable/start it. Make CA and client certificate paths readable by the service account. Do not enable the service until dry-run and one upload succeed.

## Permissions and interpretation

Use a GoldenGate account with only the read permissions required by your installed release. The collector queries:

- `/services/v2/extracts/{name}/info/status`
- `/services/v2/replicats/{name}/info/status`

For Kafka, grant only topic metadata/list-offset and consumer group committed-offset read permissions required by the broker's authorization policy. No produce, consume, create, delete or offset modification is needed by the collector. Consumer lag is an offset difference, not time latency or an exact count of business events. Missing commits, expired offsets and offsets beyond log end show Unknown, never zero. Kafka collection is per configured group/topic, not whole-cluster health.

## Open the live dashboard

Visit the Worker URL. Enter collector ID (default `default`) and READ_TOKEN. Click Connect. No real credentials should be entered in the simulation demo. Unavailable API responses clear old metrics; snapshots older than 120 seconds are marked STALE. Read tokens provide access to monitoring data for this deployment; this is a single-owner MVP, not tenant isolation. Revoke/rotate them through Worker secrets. No permissive CORS is enabled.

## Validation

```bash
npm test
npm run build
```

Tests cover authentication separation, missing secrets, payload limits/validation, ingest/read roundtrip, stale status, GoldenGate partial failure, credential-safe errors, Kafka lag edge cases and read-only collection behavior. Real endpoint compatibility, ACLs, TLS, D1 binding and production network reachability require environment testing.

API references:
- https://docs.oracle.com/en/middleware/goldengate/core/21.3/oggra/op-services-version-extracts-extract-info-status-get.html
- https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html
- https://developers.cloudflare.com/workers/static-assets/

Copyright (c) 2026. All rights reserved.
