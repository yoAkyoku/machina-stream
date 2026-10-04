# MachinaStream #9 performance baseline

Status: PASSED for the agreed v1 normal-load gate on one recorded Docker Desktop host. This
report does not claim production capacity, broker HA, cross-host scaling, or a universal
throughput guarantee.

## Run identity

- Date/time: 2026-09-29 22:26 through 2026-09-30 00:16 (Asia/Taipei); three runs were sequential.
- Source state: one unchanged working-tree snapshot for all runs; this workspace has no Git commit yet, so no commit hash is claimed.
- Host: Windows 11 Home 10.0.26200 (build 26200), 64-bit.
- CPU: AMD Ryzen 9 8940HX with Radeon Graphics, 16 physical cores / 32 logical processors.
- Memory: 33,476,448,256 bytes (about 31.2 GiB installed).
- Docker Desktop: Engine 29.7.2; Docker Compose v5.5.1.
- k6: v0.54.0, official image digest `sha256:1f40432b1cbe7234e977f96c362c9bc550a2d2b583d014dd8669fe40d3e9e755`.
- Application images: API `sha256:fd72f8b9f17236ff3db61ec2cfeb70a6282bbe0843d51980c0e536d296cde5f4`; processor `sha256:1c0f2fbdc13f48e585fd46886744c1f92fc19fc8e76bcf9ad1eefe9d4247f75c`.
- Infrastructure images: Kafka `apache/kafka:3.9.2@sha256:05b4616e0702ef2729327705d54ad6b50ea70b271c4b730fabd2320789fb7b02`; PostgreSQL `postgres:18.6@sha256:5a5a84b19854a9ffaa54082c166ff4ec27473a361e496e5ea167f298f2da9722`; Prometheus `prom/prometheus:v3.13.3@sha256:6976aa8a60fec930796ce5772b8d12da7a318a5daa8d40d69c5c7819a05eeed7`.
- Compose project: `machina-stream-capacity-20260929a`; API host port 18000, PostgreSQL 15432, Prometheus 9090, all loopback-only. PostgreSQL pool size 20; Kafka is one broker/controller with six partitions and replication factor 1.
- Resource limits: no explicit per-container CPU or memory limits in the Compose configuration; Docker Desktop host resources above are the boundary.
- Workload command: Docker `grafana/k6:0.54.0 run --summary-export /results/run-N.json /scripts/telemetry.js`, with `MACHINA_API_URL=http://api:8000`; Prometheus query `sum(machina_kafka_consumer_lag_records)` sampled every 30 seconds.
- The simulator, web, and Grafana services were not part of the measured path; they were stopped before k6 warm-up so only API, Kafka, processor, PostgreSQL, and Prometheus served the baseline.

## Fixed workload

- 1,000 synthetic machines, 100 telemetry events/s.
- 5-minute warm-up, then 30-minute measured baseline (1,800 seconds).
- Three sequential runs on the same host, source snapshot, Compose project settings, and image set.

## Results

| Run | Accepted | Accepted/s | API p50 / p95 / p99 / max (ms) | Alert p95 / misses (ms / count) | Dropped | Non-202 | Peak lag | Rising 1m windows | Drain evidence | Result |
| --- | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | --- | --- |
| 1 | 180,001 | 100.001 | 1.834 / 3.138 / 3.964 / 52.509 | 139 / 0 | 0 | 0% | 4 | 0 observed | 34.5 min CSV; final 2 samples 0 | PASS |
| 2 | 180,001 | 100.001 | 1.799 / 2.834 / 3.538 / 39.327 | 135 / 0 | 0 | 0% | 2 | 0 observed | 36.0 min CSV; final 2 samples 0 | PASS |
| 3 | 180,001 | 100.001 | 1.778 / 2.822 / 3.573 / 46.074 | 135 / 0 | 0 | 0% | 2 | 0 observed | 36.0 min CSV; final 2 samples 0 | PASS |

The 30-second lag CSVs contain 70, 73, and 73 samples respectively. Grouping them into
one-minute windows produced a maximum of one consecutive rising transition in each run
(versus the five-transition failure condition). The last samples were recorded at least
30 seconds after the k6 load stopped and were all zero.

Raw evidence is retained locally (generated artifacts are ignored by the repository):

- `artifacts/performance-20260929/run-1.json` and `run-1-lag.csv`
- `artifacts/performance-20260929/run-2.json` and `run-2-lag.csv`
- `artifacts/performance-20260929/run-3.json` and `run-3-lag.csv`

## Interpretation

All three runs independently satisfy the agreed v1 gate: at least 180,000 accepted events,
zero dropped iterations, zero non-202 responses, ingest p95 below 200 ms, alert p95 below
2 seconds with zero misses, peak lag at most 100, no five-window rising trend, and zero lag
within 30 seconds after load stopped. The result is a local baseline for the pinned MVP
topology; production sizing, HA, multi-broker behavior, and cross-host capacity remain open.
