# Machina Stream

Machina Stream is a local-first, synthetic factory telemetry demo. It shows an event path from HTTP ingestion through Kafka to PostgreSQL projections, alert episodes, and a read-only operator view. It does not connect to PLCs, control machines, or represent safety limits.

## Quick start

Prerequisites: Docker Desktop with the Linux engine and Docker Compose v2. All application services run in containers, so the host does not need Python, Node.js, or pnpm installed.

See [docs/testing.md](docs/testing.md) for the additional tools required to run the test suite locally, [docs/release-gates.md](docs/release-gates.md) for the requirement-by-requirement release boundary, [docs/validation-status.md](docs/validation-status.md) for the current evidence details, and the [local performance baseline](docs/performance-baseline-20260929.md) for the bounded three-run k6 result.

```powershell
Copy-Item .env.example .env
docker compose up --build -d
```

Open the operator dashboard at [http://localhost:3001](http://localhost:3001), Grafana at [http://localhost:3000](http://localhost:3000), and the API docs at [http://127.0.0.1:18000/docs](http://127.0.0.1:18000/docs). The default simulator creates 1,000 synthetic machines and submits one event per machine every 10 seconds (about 100 events/second). All published ports bind to loopback. If either API or PostgreSQL host port is already in use, change `API_HOST_PORT` or `POSTGRES_HOST_PORT` in `.env`.

The defaults in `.env.example` are for a disposable local demo only. Do not expose this stack publicly; the API has no user authentication or role-based access control. The threshold values are demonstration rules, not validated machine or safety specifications.

## Event path

```text
Synthetic simulator → FastAPI → `telemetry.events.v1` → Python processor → PostgreSQL
                                                      └→ dead-letter topic
Operator UI ← read-only API ← machine/event/alert projections
Prometheus ← API + processor metrics → Grafana（Kafka lag、processor／DLQ、PostgreSQL connectivity、service CPU／memory）
```

`POST /api/v1/telemetry` returns `202` only after Kafka acknowledges the event. Bodies larger than 256 KiB return `413`, invalid JSON returns `400`, and schema/value errors return `422`. The processor persists immutable event history, the latest machine projection, and alert changes in one PostgreSQL transaction, then commits the source offset. Delivery is at-least-once with idempotent database effects; this project does not claim distributed exactly-once processing or broker high availability.

The read-only API exposes `/api/v1/machines`, `/api/v1/events`, `/api/v1/alerts`, and `/api/v1/dead-letters`. Liveness and readiness checks are `/health/live` and `/health/ready`; Prometheus scrapes `/metrics`.

## Development and verification

Python source lives in `backend/machina_stream`; schema initialization is in `db/init/001_schema.sql`. Compose starts Kafka 3.9.2, PostgreSQL 18.6, the API, processor, simulator, Prometheus, and Grafana. See `docs/testing.md` for the contract, full-stack, recovery, and load-test gates.

Stop the stack and retain local data with `docker compose down`. To remove the named demo volumes as well, use `docker compose down -v` only after confirming their contents are disposable.

## Scope

The first release is the API/streaming/reliability path plus a read-only operator dashboard and engineering observability. Predictive maintenance is explicitly a later phase. The complete domain vocabulary is in [CONTEXT.md](CONTEXT.md), and the accepted event-path rationale is in [ADR 0001](docs/adr/0001-kafka-postgres-event-path.md).
