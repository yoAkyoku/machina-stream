# Contributing

Thanks for helping improve Machina Stream. This project is a synthetic, read-only monitoring demo; contributions must not add machine-control commands, real PLC connectivity, or claims that demo thresholds are safety limits.

## Before changing behavior

- Read `CONTEXT.md`, the relevant ADR, and the linked GitHub issue.
- Keep HTTP acceptance distinct from PostgreSQL projection completion.
- Preserve event history for accepted late/out-of-order events; never roll the current machine projection backward.
- Preserve the transaction-before-offset and DLQ-before-offset ordering.
- Keep the operator UI read-only; no manual ACK, resolution, or external notification in v1.

## Checks

Run Python contract tests, web lint/type checks, Compose integration/recovery tests, and the Playwright dashboard check described in [docs/testing.md](docs/testing.md). Record failures and environment details. Performance targets in that document are agreed acceptance gates, not achieved results; do not report capacity until all three baseline runs pass and their artifacts are retained.

Use small, behavior-focused changes. Tests should exercise the agreed HTTP, read API, Compose, or browser seams; only system boundaries may be replaced in unit tests.
