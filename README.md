# Data-in-Motion: Intelligent Multi-Cloud Storage Orchestrator

This project orchestrates object placement across multiple S3-compatible backends, with a small event stream, ML-assisted tiering/forecasting, and a dashboard.

URLs:
- Dashboard: http://localhost:8050
- API Docs (OpenAPI): http://localhost:8000/docs

## Prerequisites
- Docker and Docker Compose

## 1) Build and Run

```bash
docker compose up -d --build
```

This starts:
- Redpanda (Kafka API)
- Three MinIO endpoints (aws/azure/gcp in docker-compose env)
- The app container (FastAPI API + Dash dashboard + Kafka consumer)
- Prometheus (scrapes the API's `/metrics`)
- Grafana (pre-provisioned with a Prometheus datasource)

Environment is configured in `docker-compose.yml` (e.g., `SLA_LATENCY_MS`, `REPLICATION_FACTOR`, `S3_ENDPOINTS`).

## 2) Initialize Sample State

Seed metadata and buckets:

```bash
docker compose exec app python -m app.services.common.bootstrap
```

## 3) Generate Traffic (choose one)

- Continuous random producer (open loop):

```bash
docker compose exec app python -m app.services.stream.producer
```

- Finite, seeded simulator (reproducible datasets):

```bash
docker compose exec app python -m app.services.stream.simulate --events 1000 --rate 5 --skew 0.7 --seed 42
```

You can also use the Dashboard "Burst Selected (100)" button to increment counters for the selected file via the API.

Optional: start the decayer to cool access counters over time:

```bash
docker compose exec -d app python -m app.services.stream.decayer
```

## 4) Create a Dataset Snapshot and Train Models

Create a snapshot parquet with labels:

```bash
docker compose exec app bash -lc '
  mkdir -p /app/data /app/models /app/reports && \
  python -m app.ml.prepare_dataset --out /app/data/snapshot.parquet --label-mode fixed
'
```

Train the tier classifier and the hot-soon forecaster:

```bash
docker compose exec app bash -lc '
  python -m app.ml.train_tiers --data /app/data/snapshot.parquet --out /app/models/tier.bin --metrics /app/reports/tier_metrics.json && \
  python -m app.ml.train_forecast --data /app/data/snapshot.parquet --out /app/models/forecast.bin --metrics /app/reports/forecast_metrics.json
'
```

Load models into the running API:

```bash
curl -X POST http://localhost:8000/ml/load
```

## 5) Use the Streamlit Dashboard

Open http://localhost:8050
- Review headline metrics (dataset count, tier distribution, live heat gauges)
- Use the sidebar toggle to enforce encrypted destinations (🔒 endpoints remain eligible; 🔓 endpoints are excluded when enforcement is on)
- Pick any dataset in the selector to view the “Why this tier?” panel (heat, placement rationale, raw debug JSON)
- Use the action buttons (Burst/Chaos Spike, Re-optimize All, Migrator Tick/Drain, Clear Failed Tasks) to drive the demo live
- Try the Advanced Migration Tools section to run simulated rclone/s5cmd copies between endpoints
- Chaos Controls section lets you fail/recover endpoints (simulates MinIO outages) in seconds

## 6) Observability

- Prometheus UI: http://localhost:9090
- Grafana UI: http://localhost:3000 (login `admin` / `admin`)
- Metrics endpoint: `GET /metrics`
  - `dim_file_heat_score` gauge shows per-object heat
  - `dim_placement_evaluations_total` counter tracks optimizer cadence
  - `dim_migration_jobs_total` / `dim_migration_tasks` show mover health
- Automated alerts: the API emits alerts when SLA latency is breached, hot data sits on expensive tiers, traffic spikes, or migration backlogs form. View/ack them in Streamlit or via `/alerts`.

Grafana auto-loads the Prometheus datasource defined in `observability/grafana/provisioning/datasources/datasource.yml`. Suggested starter panels:
- Heat score by key (line graph)
- Placement evaluation rate & duration
- Migration queue depth and outcomes

## 7) Useful API Endpoints

- List files: `GET /files`
- Trigger optimizer for a key: `POST /optimize/{key}`
- Migrator one tick: `POST /migrator/tick`
- Burst counters for a key: `POST /simulate?key=...&events=100`
- MILP explain for a key: `GET /explain/{key}`
- Load ML models: `POST /ml/load`
- Security toggle: `GET /policy/security`, `POST /policy/security`
- Endpoint metadata: `GET /endpoints`
- Advanced migration tools: `POST /tools/rclone`, `POST /tools/s5cmd`
- Chaos controls: `GET /chaos/status`, `POST /chaos/fail/{name}`, `/chaos/recover/{name}`, `/chaos/clear`

OpenAPI: http://localhost:8000/docs

## 8) Chaos & Resilience Demo

You can automate a “kill/recover” flow without touching Docker manually:

```bash
python -m app.scripts.chaos --endpoint aws --duration 30
```

This CLI calls the API to fail an endpoint (all S3 calls throw errors), waits the requested duration, then restores it so you can show retries + eventual success in the dashboard. The same controls exist in Streamlit’s sidebar.

## 9) Database Migration Notes

If your SQLite DB predates the security/versioning features, run the following once:

```bash
docker compose exec api sqlite3 /app/data/state.db "ALTER TABLE file_meta ADD COLUMN version_token TEXT DEFAULT ''"
docker compose exec api sqlite3 /app/data/state.db "CREATE TABLE IF NOT EXISTS system_setting (key TEXT PRIMARY KEY, value TEXT)"
```

After that, restart the stack so the new columns/tables load.

## Configuration

Via `docker-compose.yml` environment:
- `KAFKA_BOOTSTRAP` (default `redpanda:9092`)
- `DB_URL` (default `sqlite:///./state.db` mounted in container)
- `SLA_LATENCY_MS`, `REPLICATION_FACTOR`
- `S3_ENDPOINTS` JSON array with fields: `name`, `url`, `access_key`, `secret_key`, `bucket`, `latency_ms`, `cost_per_gb`

## Rebuild After Dependency Changes

If you modify `app/requirements.txt`, rebuild the app image:

```bash
docker compose build app && docker compose up -d
```

## Troubleshooting

- Ports in use: ensure 8000/8050/9090/3000/9092/9000-9001/9100-9101/9200-9201 are free.
- Models not loaded: call `POST /ml/load` after training or check `/app/models/*` inside the container.
- No files listed: run the bootstrap step to seed metadata and buckets.
- Simulator cannot reach API: confirm the API is up (http://localhost:8000/health if you add one) and that the simulator runs in the app container.
