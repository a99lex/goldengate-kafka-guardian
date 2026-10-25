# GoldenGate & Kafka Guardian

**Guardian v0.1.0** — an independent, demo-first observability console for Oracle GoldenGate and Apache Kafka.

![Status](https://img.shields.io/badge/status-demo%20MVP-49c5ac) ![Deployment](https://img.shields.io/badge/hosting-Cloudflare%20Pages-orange)

## Features

- Operations dashboard with replication lag, Kafka backlog and throughput
- GoldenGate Extract/Replicat inventory, search, filters and process details
- Kafka topic inventory, partitions, consumer lag and cluster health
- CDC pipeline topology
- Incident simulator: stop/start processes, inject replication lag, acknowledge alerts
- JSON snapshot export
- Local settings and automatic metric simulation
- Responsive dark UI

**Important:** This version contains only fictional/synthetic demo metrics. The app does not access any GoldenGate, Kafka or Oracle system. It has no login, server-side collector or production alerting. Never enter real credentials into the frontend.

## Run locally

No build step and no dependencies. Open `index.html` in a modern browser. For a local server:

```bash
python3 -m http.server 8787
# open http://localhost:8787
```

## Deploy to Cloudflare Pages

1. Create a **private** GitHub repository named `goldengate-kafka-guardian`.
2. Push these files to the `main` branch.
3. In Cloudflare Dashboard, open **Workers & Pages** → **Create** → **Pages** → **Connect to Git**.
4. Select the repository, use **no framework preset**, **no build command**, and `/` as the output directory (or leave the output directory blank if the dashboard supports it).
5. Deploy. Subsequent commits to `main` will automatically trigger deployments.

Alternatively, from the project directory with Wrangler installed:

```bash
npx wrangler pages deploy . --project-name goldengate-kafka-guardian
```

Cloudflare authentication and creation of the Pages project are required.

## Security and independence

- Use personal accounts and independently written code.
- Do not copy employer/customer scripts, credentials, configurations, logs, topology or proprietary materials.
- All included environment and process names are fictional.
- Production integration requires server-side secret storage, least-privilege collectors, authentication, authorization, audit logs and TLS.

## Next milestones

1. Cloudflare Workers API with D1 persistence.
2. Read-only GoldenGate Microservices REST collector running in a trusted network.
3. Kafka metrics adapter (Prometheus/JMX or Admin API), consumer group offset monitoring.
4. Authentication, tenant isolation, history, notification delivery and alert deduplication.
5. Unit/integration tests, CI, release hardening.

## License

Copyright (c) 2026. All rights reserved. No open-source license is granted by default.
