# Release gates

This table is the release boundary for the first open-source MVP. “Implemented” means the
repository contains the agreed behavior and contract tests; it does not turn an unrun Compose,
browser, or load test into a pass claim.

| Scope | Repository evidence | Current status |
| --- | --- | --- |
| #1 project position and phase story | [README](../README.md), [CONTEXT](../CONTEXT.md), [ADR 0001](adr/0001-kafka-postgres-event-path.md) | Implemented and documented |
| #2 A → B → C portfolio phases | README scope and later-phase predictive-maintenance note | Implemented and documented |
| #3 synthetic factory, 1,000-machine demo defaults, read-only operator | `backend/machina_stream/simulator.py`, `.env.example`, `compose.yaml`, web dashboard | Implemented; Docker Desktop runtime smoke passed |
| #4 HTTP → FastAPI → Kafka → processor → PostgreSQL topology | `compose.yaml`, `Dockerfile`, [ADR 0001](adr/0001-kafka-postgres-event-path.md) | Compose build, health checks, and non-recovery flow passed |
| #5 factory dashboard and engineering observability split | `web/app/page.tsx`, Grafana dashboard, Prometheus config | Static checks and Chromium dashboard flow passed |
| #6 telemetry schema and rejection contract | `backend/machina_stream/contracts.py`, API contract tests, 63 pinned-dependency unit tests | Unit contract and live ingress integration passed |
| #7 at-least-once, retry, DLQ, replay, and idempotent projection | `worker.py`, `store.py`, `replay_dlq.py`, retry/DLQ unit contracts, integration/recovery cases | Compose recovery, DLQ replay, and crash-window cases passed |
| #8 alert lifecycle, hysteresis, and offline semantics | `rules.py`, `store.py`, event-flow integration cases | Docker Desktop event-flow and recovery cases passed |
| #9 normal-load and exploratory-load gates | [testing contract](testing.md), k6 script, [performance report](performance-baseline-20260929.md) | Three same-host baseline runs passed; local single-broker evidence only |
| #10 failure matrix and data-integrity gates | [testing contract](testing.md), 15 integration/recovery cases, CI evidence artifacts | 8 non-recovery + 7 recovery cases passed on Docker Desktop; screenshots retained |
| Open-source and security hygiene | MIT license, README, CONTRIBUTING, SECURITY, Dependabot, loopback-only ports, container hardening, credential scan | Static checks passed |

## Release decision

The repository is ready for open-source MVP review with the local runtime, recovery evidence,
and bounded #9 baseline recorded in [validation status](validation-status.md). This is not a
production-capacity or HA release: the published numbers are a single-host, single-broker
Docker Desktop baseline and must not be generalized to multi-broker or cross-host deployments.
