# Validation status

This page records the latest repository checks without turning an unrun test into a pass claim. See the [release-gates table](release-gates.md) for the requirement-by-requirement boundary. The three agreed #9 baseline runs now have a separate [local performance report](performance-baseline-20260929.md); the result is bounded to the recorded Docker Desktop host and single-broker MVP topology.

The ingress body limit now rejects an oversized streaming chunk before adding it to the retained request buffer; a regression test covers that no-`Content-Length` path. Replay source headers and DLQ audit writes now require the configured telemetry topic, strict JSON source types, and Kafka storage bounds, rejecting wrong/empty topics or negative coordinates before persistence; a replay-marked message without source coordinates is also sent to DLQ. Malformed original payload bytes are skipped by the replay CLI instead of aborting the scan. DLQ envelope serialization tests also verify binary payload preservation. Processor unit contracts now verify the five-attempt database retry budget and that a source offset remains uncommitted until DLQ acknowledgement. Kafka lag metric failures are now isolated from the processor loop, with a regression test for a transient broker error. The API and processor now expose a periodic PostgreSQL connectivity gauge for the engineering dashboard, alongside the client library's service process resource metrics. Processor readiness now waits for a Kafka telemetry partition assignment, and offline-episode scans run as a cancellable background task so a large stale-machine scan cannot block event ingestion. The 120-second processor-pause recovery test now requires both complete event history and a zero total lag metric within its 180-second drain window. The current pinned-dependency unit suite, clean Docker Desktop runtime acceptance run, and three-run #9 local baseline are recorded below.

## Source and configuration checks

The following checks passed on 2026-09-28 in the current Windows workspace:

| Check | Result |
| --- | --- |
| `docker compose config --quiet` | passed |
| `docker compose --env-file .env.example config --quiet` | passed |
| Docker image availability preflight for pinned Compose inputs | passed at the earlier local checkpoint; `node:24-alpine` (`sha256:ebfe2f90462722a7a4de65e91990e97fe0d401c70e0e762c5b53302f905ec1c1`), `postgres:18.6` (`sha256:5a5a84b19854a9ffaa54082c166ff4ec27473a361e496e5ea167f298f2da9722`), `apache/kafka:3.9.2` (`sha256:05b4616e0702ef2729327705d54ad6b50ea70b271c4b730fabd2320789fb7b02`), `prom/prometheus:v3.13.3` (`sha256:6976aa8a60fec930796ce5772b8d12da7a318a5daa8d40d69c5c7819a05eeed7`), and `grafana/grafana:13.2.1` (`sha256:f772d434e8fab0049deb2b1b30abd43342bcfca1537614aa8d36080232cf4283`) were present; no project containers were started |
| `scripts/check_compose_security.py` | passed; all published ports bind loopback, application containers drop `ALL` capabilities and enable `no-new-privileges`, Grafana anonymous access/sign-up are disabled; config-only check, no services started |
| Compose JSON assertion that every published port is bound to `127.0.0.1` | passed; API, PostgreSQL, web, Prometheus, and Grafana |
| Compose JSON check for `no-new-privileges` on application containers | passed; API, processor, simulator, and web |
| Compose JSON check for `cap_drop: ALL` on application containers | passed; API, processor, simulator, and web |
| Compose JSON check that Grafana anonymous access and sign-up are disabled | passed |
| `docker buildx build --check --network none -f Dockerfile .` | passed; no warnings |
| `docker buildx build --check --network none -f Dockerfile.web .` | passed; no warnings |
| PowerShell parse of `scripts/test_stack.ps1` including preflight/cleanup paths | passed |
| `pnpm --dir web install --frozen-lockfile` | passed; pnpm 11.19.0, lockfile supply-chain policy passed, and only the explicitly allowlisted `unrs-resolver` postinstall ran (Node system CA enabled) |
| Web ESLint and TypeScript typecheck | passed; direct project binaries, no application runtime started |
| Ruff check for `backend` and `tests` | passed; Ruff 0.16.9 cached environment |
| Ruff format check for `backend` and `tests` | passed; 16 files already formatted |
| `uv sync --locked --extra dev --system-certs` | passed; pinned 35-package graph installed into the local ignored `.venv` |
| Official dependency-backed Ruff check/format in a read-only Docker sandbox | passed; Ruff 0.16.9, no network |
| Supplemental Ruff Bandit-compatible `S` scan | triaged; 161 findings (`S101=152`, `S104=2`, `S311=2`, `S603=2`, `S607=3`) are intentional test assertions/controlled Docker subprocesses (including the Compose security checker), non-cryptographic simulator/backoff randomness, or internal service listeners; no secret/eval/shell-injection finding |
| Credential-shaped literal scan over source/configuration | passed; no private-key, cloud-key, or common token pattern found |
| `uv lock --check --offline` with cached CPython 3.12.14 | passed; lockfile resolves 35 packages |
| Current pinned-dependency `pytest tests/unit --strict-markers` in the local ignored `.venv` | passed; 63 tests, one expected Windows pytest cache warning; no network or service access |
| Earlier official dependency-backed `pytest tests/unit --strict-markers` in a read-only Docker sandbox | passed; 44 tests at that checkpoint, one expected read-only `PytestCacheWarning`, no network or service access; the later schema and retry-contract additions were rerun in the pinned local environment above |
| Constrained `pytest --collect-only` for `tests/integration` | passed; 15 integration/recovery tests collected in normal and `--strict-markers` mode, one expected `pytest-asyncio` config warning; no services were started |
| Official dependency-backed integration `pytest --collect-only --strict-markers` in a read-only Docker sandbox | passed; 15 tests collected, no services were started |
| Python syntax compilation for `backend/**/*.py` and `tests/**/*.py` in a no-network, read-only-source Docker sandbox | passed; 16 files |
| JSON parsing for `web/package.json` and `web/tsconfig.json` | passed |
| YAML parsing for `web/pnpm-workspace.yaml` | passed; `unrs-resolver` is the only explicitly allowed native dependency build |
| JSON parsing for `monitoring/grafana/dashboards/machina-stream.json` | passed; 9 panels including PostgreSQL connectivity and service CPU/memory |
| YAML parsing for `.github/workflows/ci.yml`, `compose.yaml`, and `monitoring/prometheus.yml` | passed; cached PyYAML, no network |
| YAML parsing and ecosystem assertion for `.github/dependabot.yml` | passed; weekly pip, npm, and GitHub Actions update checks |
| CI workflow structure assertion for locked sync, Python format/strict-marker gates, both jobs, browser results, Compose evidence artifacts, and always-run Compose cleanup | passed |
| Node syntax check for `web/eslint.config.mjs` and `web/scripts/next.mjs` | passed |
| Node syntax check for `tests/performance/telemetry.js` | passed |

The Python sandbox used the cached `python:3.12-slim` image with no network, a read-only repository mount, an isolated scratch mount, and CPU, memory, process, file-size, and temporary-filesystem limits. It did not receive the Docker socket.

## Docker Desktop runtime acceptance

The following checks passed on 2026-09-28 against the dedicated Compose project
`machina-stream-e2e-all-20260928d` using the `desktop-linux` Docker Desktop engine. The
stack used `SIMULATOR_MACHINE_COUNT=1`, `SIMULATOR_EVENT_INTERVAL_SECONDS=1`, and
`PROCESSOR_DATABASE_POOL_SIZE=20`; API, processor, simulator image, Kafka, PostgreSQL, web,
Prometheus, and Grafana were exercised without changing the normal development project.

| Runtime check | Result |
| --- | --- |
| Final `api`, `processor`, `simulator`, and `web` image build (BuildKit optional registry CA secret) | passed |
| Clean Compose `up -d --wait` for PostgreSQL, Kafka, Kafka init, API, processor, web, Prometheus, and Grafana | passed; all requested services healthy |
| `pytest tests/unit tests/integration --strict-markers -m "integration and not recovery" -q` | passed; 8 passed, 70 deselected in 5.75s |
| `pytest tests/integration --strict-markers -m recovery -q` | passed; 7 passed, 8 deselected in 342.52s (0:05:42) |
| Chromium Playwright dashboard flow | passed; 1 passed in 5.4s; desktop and mobile screenshots retained under `web/test-results/` |

The recovery run covered simulator/API/processor/Kafka restarts, the 12,000-event processor
backlog, a 60-second PostgreSQL outage with DLQ audit/replay, a missing-DLQ provisioning path,
and the database/offset crash window. An independent PostgreSQL-outage regression also passed
in 65.27s after the processor scheduling fix. These are local Docker Desktop evidence for the
pinned MVP topology, not a high-availability or production-capacity claim. The test script
cleaned the acceptance project and its volumes after collecting the results; Compose evidence
is retained under `artifacts/compose/machina-stream-e2e-all-20260928d/`.

## #9 local capacity baseline

The three-run baseline passed on 2026-09-29/30 (Asia/Taipei) with the same uncommitted
working-tree snapshot, host, Compose configuration, and Docker images. The test used the
official `grafana/k6:0.54.0` image (`sha256:1f40432b1cbe7234e977f96c362c9bc550a2d2b583d014dd8669fe40d3e9e755`),
5 minutes of warm-up, and 30 minutes at 100 telemetry events/s (1,000 synthetic-machine
workload). The simulator, web, and Grafana services were stopped for this API/Kafka baseline;
PostgreSQL, Kafka, API, processor, and Prometheus remained healthy. The full report is
[performance-baseline-20260929.md](performance-baseline-20260929.md).

| Run | Accepted | API p50 / p95 / p99 / max (ms) | Alert p95 / misses (ms / count) | Dropped | Non-202 | Peak lag | Drain evidence |
| --- | ---: | --- | --- | ---: | ---: | ---: | --- |
| 1 | 180,001 | 1.834 / 3.138 / 3.964 / 52.509 | 139 / 0 | 0 | 0% | 4 | final 2 samples 0 |
| 2 | 180,001 | 1.799 / 2.834 / 3.538 / 39.327 | 135 / 0 | 0 | 0% | 2 | final 2 samples 0 |
| 3 | 180,001 | 1.778 / 2.822 / 3.573 / 46.074 | 135 / 0 | 0 | 0% | 2 | final 2 samples 0 |

Each run therefore achieved 100.001 accepted events/s (180,001 / 1,800 s), stayed below
the 200 ms ingest and 2 s alert p95 gates, had zero probe misses/dropped iterations, and
showed no five-window rising-lag violation. The retained Prometheus CSVs contain 34.5–36.0
minutes of 30-second samples per run; every run has at least 30 seconds after load stopped
with lag 0. Raw JSON/CSV evidence is retained locally under
`artifacts/performance-20260929/` (the repository ignores generated artifacts).

## Not yet verified

These checks remain open and must not be described as passing:

- any production HA, multi-broker, or cross-host capacity claim beyond the local MVP topology.

The runtime and #9 rows above are evidence from one Docker Desktop host. They do not establish
production HA, multi-broker behavior, cross-host scaling, or a universal capacity guarantee.
