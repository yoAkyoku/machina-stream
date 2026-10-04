# MachinaStream performance run report

Status: NOT RUN — replace this line only after completing all three runs. Keep the original outputs for failed runs.

## Run identity

- Date/time and timezone:
- Git commit:
- OS, version, architecture:
- CPU model / physical cores / logical processors:
- Installed RAM:
- Docker Engine version:
- Docker Compose version:
- k6 version:
- Container image tags/digests:
- Compose broker/database configuration:
- Per-container CPU and memory limits:
- Commands and environment overrides:

## Fixed workload

- Telemetry arrival rate: 100 events/s
- Synthetic machines: 1,000
- Warm-up: 5 minutes
- Measured duration: 30 minutes
- Runs: three on the same host, commit, and configuration
- k6 JSON summaries and Grafana/Prometheus lag exports:

## Results

| Run | Accepted events | Achieved events/s (accepted / 1,800s) | Dropped iterations | Non-202 rate | API p50 / p95 / p99 (ms) | Alert p95 (ms) / misses | Peak lag | Rising 1m windows | Drain time | PASS/FAIL |
| --- | ---: | ---: | ---: | ---: | --- | --- | ---: | ---: | ---: | --- |
| 1 | | | | | | | | | | |
| 2 | | | | | | | | | | |
| 3 | | | | | | | | | | |

## Pass criteria (must pass independently in all three runs)

- At least 180,000 accepted events, achieved rate 100 events/s, and zero dropped iterations.
- Non-202 response rate < 0.1%; telemetry ingest p95 < 200 ms.
- Alert end-to-end p95 < 2 seconds and zero probe misses. Timing starts immediately before POST and ends when the read API first exposes that event's `latest_event_id`.
- Total Kafka consumer lag does not rise across five consecutive one-minute windows, peak lag is at most 100, and lag drains to zero within 30 seconds after load stops.
- Report p50, p95, p99, max, and throughput even though only the thresholds above determine pass/fail.

1,000, 5,000, and 10,000 events/s are exploratory only. Do not describe unrun or failed measurements as capacity results. If a run fails, preserve its artifacts and record the failure before any rerun.

## Findings / follow-up

-
