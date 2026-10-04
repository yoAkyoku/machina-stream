from __future__ import annotations

import hashlib
import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import asyncpg

from machina_stream.contracts import TelemetryEvent
from machina_stream.rules import NumericRule, numeric_rules

MAX_KAFKA_PARTITION = 2_147_483_647
MAX_KAFKA_OFFSET = 9_223_372_036_854_775_807


class PermanentMessageError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True)
class ApplyResult:
    event_id: UUID
    outcome: str
    sequence_gap: int = 0


def _read_received_at(value: object) -> datetime:
    if not isinstance(value, str):
        raise PermanentMessageError("missing_received_at", "received_at is required")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise PermanentMessageError("invalid_received_at", "received_at is invalid") from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise PermanentMessageError("invalid_received_at", "received_at must include a timezone")
    return parsed.astimezone(UTC)


def _identity_payload(event: TelemetryEvent) -> dict[str, object]:
    result = event.model_dump(mode="json")
    result.pop("received_at", None)
    result["event_time"] = event.event_time.astimezone(UTC).isoformat()
    return result


def _severity_level(value: str) -> int:
    return 2 if value == "critical" else 1


class ProjectionStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self.pool = pool

    @classmethod
    async def connect(cls) -> ProjectionStore:
        url = os.getenv("DATABASE_URL", "postgresql://machina:machina@postgres:5432/machina")
        pool = await asyncpg.create_pool(
            dsn=url,
            min_size=1,
            max_size=int(os.getenv("DATABASE_POOL_SIZE", "10")),
            command_timeout=30,
            server_settings={"timezone": "UTC", "application_name": "machina-stream"},
        )
        return cls(pool)

    async def close(self) -> None:
        await self.pool.close()

    async def healthy(self) -> bool:
        try:
            async with self.pool.acquire() as connection:
                await connection.fetchval("SELECT 1")
            return True
        except (asyncpg.PostgresError, OSError, TimeoutError):
            return False

    async def apply(
        self,
        raw: dict[str, object],
        *,
        replayed: bool = False,
        replay_source: tuple[str, int, int] | None = None,
    ) -> ApplyResult:
        try:
            event = TelemetryEvent.model_validate(raw)
        except Exception as error:
            raise PermanentMessageError(
                "invalid_schema", "Kafka payload failed schema validation"
            ) from error
        received_at = _read_received_at(raw.get("received_at"))
        identity_json = json.dumps(
            _identity_payload(event), sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
        payload_hash = hashlib.sha256(identity_json.encode("utf-8")).hexdigest()
        full_payload = event.model_dump(mode="json")
        full_payload["event_time"] = event.event_time.astimezone(UTC).isoformat()
        full_payload["received_at"] = received_at.isoformat()

        async with self.pool.acquire() as connection:
            async with connection.transaction():
                await connection.execute(
                    """
                    INSERT INTO machines (machine_id, factory_id, line_id)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (machine_id) DO NOTHING
                    """,
                    event.machine_id,
                    event.factory_id,
                    event.line_id,
                )
                machine = await connection.fetchrow(
                    "SELECT * FROM machines WHERE machine_id = $1 FOR UPDATE", event.machine_id
                )
                if machine["factory_id"] != event.factory_id or machine["line_id"] != event.line_id:
                    raise PermanentMessageError(
                        "machine_identity_conflict", "machine_id changed factory or line identity"
                    )

                existing_hash = await connection.fetchval(
                    "SELECT payload_hash FROM telemetry_events WHERE event_id = $1", event.event_id
                )
                if existing_hash is not None:
                    if existing_hash != payload_hash:
                        raise PermanentMessageError(
                            "event_id_conflict",
                            "event_id was previously used with different content",
                        )
                    if replay_source is not None:
                        await self._mark_replayed(connection, replay_source)
                    return ApplyResult(event.event_id, "duplicate")

                current_sequence = int(machine["sequence_no"])
                gap = 0
                existing_sequence_event = await connection.fetchval(
                    "SELECT event_id FROM telemetry_events "
                    "WHERE machine_id = $1 AND sequence_no = $2 LIMIT 1",
                    event.machine_id,
                    event.sequence_no,
                )
                if existing_sequence_event is not None or event.sequence_no == current_sequence:
                    outcome = "sequence_conflict"
                elif event.sequence_no < current_sequence:
                    outcome = "out_of_order"
                elif event.sequence_no > current_sequence + 1 and current_sequence > 0:
                    outcome = "gap"
                    gap = event.sequence_no - current_sequence - 1
                else:
                    outcome = "projected"
                processed_at = await connection.fetchval("SELECT clock_timestamp()")

                await connection.execute(
                    """
                    INSERT INTO telemetry_events (
                        event_id, payload_hash, factory_id, line_id, machine_id,
                        sequence_no, event_time, received_at, processed_at, payload,
                        sequence_outcome, replayed
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10::jsonb, $11, $12)
                    """,
                    event.event_id,
                    payload_hash,
                    event.factory_id,
                    event.line_id,
                    event.machine_id,
                    event.sequence_no,
                    event.event_time,
                    received_at,
                    processed_at,
                    json.dumps(full_payload, ensure_ascii=False, separators=(",", ":")),
                    outcome,
                    replayed,
                )

                if outcome == "sequence_conflict":
                    await connection.execute(
                        "UPDATE machines SET sequence_conflict_count = sequence_conflict_count + 1 "
                        "WHERE machine_id = $1",
                        event.machine_id,
                    )
                elif outcome in ("projected", "gap"):
                    await connection.execute(
                        """
                        UPDATE machines SET
                            sequence_no = $2,
                            latest_event_id = $3,
                            event_time = $4,
                            last_seen_at = CASE WHEN $5 THEN last_seen_at ELSE $6 END,
                            metrics = $7::jsonb,
                            machine_status = COALESCE($8, machine_status),
                            error_code = CASE WHEN $8 = 'fault' THEN $9
                                              WHEN $8 IS NOT NULL THEN NULL
                                              ELSE error_code END,
                            sequence_gap_count = sequence_gap_count + $10,
                            updated_at = now()
                        WHERE machine_id = $1
                        """,
                        event.machine_id,
                        event.sequence_no,
                        event.event_id,
                        event.event_time,
                        replayed,
                        received_at,
                        json.dumps(event.metrics.model_dump(), separators=(",", ":")),
                        event.machine_status,
                        event.error_code,
                        gap,
                    )
                    await self._evaluate_alerts(
                        connection,
                        event,
                        processed_at,
                        allow_online_transition=not replayed,
                    )

                if replay_source is not None:
                    await self._mark_replayed(connection, replay_source)

        return ApplyResult(event.event_id, outcome, gap)

    async def _mark_replayed(
        self,
        connection: asyncpg.Connection,
        source: tuple[str, int, int],
    ) -> None:
        await connection.execute(
            """
            INSERT INTO dead_letter_audit (
                source_topic, source_partition, source_offset, error_code, failed_at, replayed_at
            ) VALUES ($1, $2, $3, 'replay_pending', now(), now())
            ON CONFLICT (source_topic, source_partition, source_offset) DO UPDATE
            SET replayed_at = COALESCE(dead_letter_audit.replayed_at, now())
            """,
            source[0],
            source[1],
            source[2],
        )

    async def record_dead_letter(self, record: dict[str, object]) -> None:
        source = record.get("source")
        if not isinstance(source, dict):
            raise PermanentMessageError("invalid_dlq_record", "DLQ source coordinates are missing")
        try:
            source_topic = source["topic"]
            source_partition = source["partition"]
            source_offset = source["offset"]
            failed_at = _read_received_at(record.get("failed_at"))
        except KeyError as error:
            raise PermanentMessageError("invalid_dlq_record", "DLQ record is malformed") from error
        expected_topic = os.getenv("KAFKA_TELEMETRY_TOPIC", "telemetry.events.v1")
        if (
            not isinstance(source_topic, str)
            or not source_topic
            or source_topic != expected_topic
            or isinstance(source_partition, bool)
            or not isinstance(source_partition, int)
            or isinstance(source_offset, bool)
            or not isinstance(source_offset, int)
            or source_partition < 0
            or source_offset < 0
            or source_partition > MAX_KAFKA_PARTITION
            or source_offset > MAX_KAFKA_OFFSET
        ):
            raise PermanentMessageError("invalid_dlq_record", "DLQ source coordinates are invalid")
        candidate_id = record.get("event_id")
        try:
            event_id = UUID(str(candidate_id)) if candidate_id else None
        except ValueError:
            event_id = None

        async with self.pool.acquire() as connection:
            await connection.execute(
                """
                INSERT INTO dead_letter_audit (
                    source_topic, source_partition, source_offset, event_id, error_code, failed_at
                ) VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (source_topic, source_partition, source_offset) DO UPDATE
                SET event_id = COALESCE(dead_letter_audit.event_id, EXCLUDED.event_id),
                    error_code = CASE WHEN dead_letter_audit.error_code = 'replay_pending'
                                      THEN EXCLUDED.error_code
                                      ELSE dead_letter_audit.error_code END,
                    failed_at = LEAST(dead_letter_audit.failed_at, EXCLUDED.failed_at)
                """,
                source_topic,
                source_partition,
                source_offset,
                event_id,
                str(record.get("error", {}).get("code", "unknown"))[:100]
                if isinstance(record.get("error"), dict)
                else "unknown",
                failed_at,
            )

    async def _evaluate_alerts(
        self,
        connection: asyncpg.Connection,
        event: TelemetryEvent,
        processed_at: datetime,
        *,
        allow_online_transition: bool,
    ) -> None:
        version, rules = numeric_rules()
        for rule in rules:
            value = float(getattr(event.metrics, rule.metric_name))
            await self._evaluate_numeric(connection, event, processed_at, version, rule, value)

        if event.machine_status == "fault":
            await self._open_or_update(
                connection,
                event,
                processed_at,
                rule_id="machine_fault",
                rule_version=version,
                severity="critical",
                latest_value=None,
                diagnostic_code=event.error_code,
            )
            await self._reset_recovery(connection, event.machine_id, "machine_fault")
        elif event.machine_status is not None:
            await self._recover_fault(connection, event, processed_at)
        else:
            await self._reset_recovery(connection, event.machine_id, "machine_fault")

        if allow_online_transition:
            await connection.execute(
                """
                UPDATE alert_episodes SET status = 'RESOLVED', resolved_at = $2
                WHERE machine_id = $1 AND rule_id = 'machine_offline' AND status = 'OPEN'
                """,
                event.machine_id,
                processed_at,
            )

    async def _evaluate_numeric(
        self,
        connection: asyncpg.Connection,
        event: TelemetryEvent,
        processed_at: datetime,
        version: str,
        rule: NumericRule,
        value: float,
    ) -> None:
        row = await connection.fetchrow(
            """
            SELECT episode_id, severity FROM alert_episodes
            WHERE machine_id = $1 AND rule_id = $2 AND status = 'OPEN' FOR UPDATE
            """,
            event.machine_id,
            rule.rule_id,
        )
        if row is None:
            severity = (
                "critical"
                if value >= rule.critical
                else "warning"
                if value >= rule.warning
                else None
            )
            if severity is not None:
                await self._open_or_update(
                    connection,
                    event,
                    processed_at,
                    rule_id=rule.rule_id,
                    rule_version=version,
                    severity=severity,
                    latest_value=value,
                    diagnostic_code=None,
                )
            return

        current = str(row["severity"])
        clear_line = (rule.critical if current == "critical" else rule.warning) * 0.95
        if value < clear_line:
            streak = await self._increase_recovery(connection, event.machine_id, rule.rule_id)
            if streak >= 2:
                if current == "critical" and value >= rule.warning * 0.95:
                    await connection.execute(
                        "UPDATE alert_episodes SET severity = 'warning', last_seen_at = $2, "
                        "latest_value = $3, latest_event_id = $4, "
                        "occurrence_count = occurrence_count + "
                        "CASE WHEN $3::double precision >= $5::double precision "
                        "THEN 1 ELSE 0 END "
                        "WHERE episode_id = $1",
                        row["episode_id"],
                        processed_at,
                        value,
                        event.event_id,
                        rule.warning,
                    )
                else:
                    await connection.execute(
                        "UPDATE alert_episodes SET status = 'RESOLVED', resolved_at = $2, "
                        "last_seen_at = $2, latest_value = $3, latest_event_id = $4 "
                        "WHERE episode_id = $1",
                        row["episode_id"],
                        processed_at,
                        value,
                        event.event_id,
                    )
                await self._reset_recovery(connection, event.machine_id, rule.rule_id)
            else:
                await connection.execute(
                    "UPDATE alert_episodes SET last_seen_at = $2, latest_value = $3, "
                    "latest_event_id = $4, occurrence_count = occurrence_count + "
                    "CASE WHEN $3::double precision >= $5::double precision "
                    "THEN 1 ELSE 0 END WHERE episode_id = $1",
                    row["episode_id"],
                    processed_at,
                    value,
                    event.event_id,
                    rule.warning,
                )
            return

        await self._reset_recovery(connection, event.machine_id, rule.rule_id)
        if value >= rule.warning:
            severity = "critical" if value >= rule.critical else "warning"
            if _severity_level(severity) > _severity_level(current):
                current = severity
        await connection.execute(
            "UPDATE alert_episodes SET severity = $2, last_seen_at = $3, latest_value = $4, "
            "latest_event_id = $5, occurrence_count = occurrence_count + $6 WHERE episode_id = $1",
            row["episode_id"],
            current,
            processed_at,
            value,
            event.event_id,
            int(value >= rule.warning),
        )

    async def _open_or_update(
        self,
        connection: asyncpg.Connection,
        event: TelemetryEvent,
        processed_at: datetime,
        *,
        rule_id: str,
        rule_version: str,
        severity: str,
        latest_value: float | None,
        diagnostic_code: str | None,
    ) -> None:
        row = await connection.fetchrow(
            "SELECT episode_id, severity FROM alert_episodes "
            "WHERE machine_id = $1 AND rule_id = $2 AND status = 'OPEN' FOR UPDATE",
            event.machine_id,
            rule_id,
        )
        if row is None:
            await connection.execute(
                """
                INSERT INTO alert_episodes (
                    episode_id, factory_id, line_id, machine_id, rule_id, rule_version,
                    severity, status, opened_at, last_seen_at, latest_value,
                    latest_event_id, diagnostic_code
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'OPEN', $8, $8, $9, $10, $11)
                """,
                uuid4(),
                event.factory_id,
                event.line_id,
                event.machine_id,
                rule_id,
                rule_version,
                severity,
                processed_at,
                latest_value,
                event.event_id,
                diagnostic_code,
            )
            return

        next_severity = (
            severity
            if _severity_level(severity) > _severity_level(str(row["severity"]))
            else str(row["severity"])
        )
        await connection.execute(
            "UPDATE alert_episodes SET severity = $2, last_seen_at = $3, "
            "occurrence_count = occurrence_count + 1, latest_value = $4, "
            "latest_event_id = $5, diagnostic_code = $6 WHERE episode_id = $1",
            row["episode_id"],
            next_severity,
            processed_at,
            latest_value,
            event.event_id,
            diagnostic_code,
        )

    async def _increase_recovery(
        self, connection: asyncpg.Connection, machine_id: str, rule_id: str
    ) -> int:
        return await connection.fetchval(
            """
            INSERT INTO alert_recovery_streaks (machine_id, rule_id, streak)
            VALUES ($1, $2, 1)
            ON CONFLICT (machine_id, rule_id) DO UPDATE
            SET streak = alert_recovery_streaks.streak + 1
            RETURNING streak
            """,
            machine_id,
            rule_id,
        )

    async def _reset_recovery(
        self, connection: asyncpg.Connection, machine_id: str, rule_id: str
    ) -> None:
        await connection.execute(
            "DELETE FROM alert_recovery_streaks WHERE machine_id = $1 AND rule_id = $2",
            machine_id,
            rule_id,
        )

    async def _recover_fault(
        self, connection: asyncpg.Connection, event: TelemetryEvent, processed_at: datetime
    ) -> None:
        row = await connection.fetchrow(
            "SELECT episode_id FROM alert_episodes WHERE machine_id = $1 "
            "AND rule_id = 'machine_fault' AND status = 'OPEN' FOR UPDATE",
            event.machine_id,
        )
        if row is None:
            return
        streak = await self._increase_recovery(connection, event.machine_id, "machine_fault")
        if streak >= 2:
            await connection.execute(
                "UPDATE alert_episodes SET status = 'RESOLVED', resolved_at = $2, "
                "last_seen_at = $2, latest_event_id = $3, diagnostic_code = NULL "
                "WHERE episode_id = $1",
                row["episode_id"],
                processed_at,
                event.event_id,
            )
            await self._reset_recovery(connection, event.machine_id, "machine_fault")

    async def check_offline(self, now: datetime, *, timeout: timedelta) -> int:
        version = os.getenv("MACHINA_RULESET_VERSION", "demo-v1")
        opened = 0
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                machines = await connection.fetch(
                    """
                    SELECT * FROM machines WHERE last_seen_at IS NOT NULL
                    AND last_seen_at <= $1 FOR UPDATE SKIP LOCKED
                    """,
                    now - timeout,
                )
                for machine in machines:
                    row = await connection.fetchrow(
                        "SELECT episode_id FROM alert_episodes WHERE machine_id = $1 "
                        "AND rule_id = 'machine_offline' AND status = 'OPEN' FOR UPDATE",
                        machine["machine_id"],
                    )
                    if row is not None:
                        continue
                    await connection.execute(
                        """
                        INSERT INTO alert_episodes (
                            episode_id, factory_id, line_id, machine_id, rule_id, rule_version,
                            severity, status, opened_at, last_seen_at, latest_event_id
                        ) VALUES (
                            $1, $2, $3, $4, 'machine_offline', $5, 'warning', 'OPEN', $6, $6, $7
                        )
                        """,
                        uuid4(),
                        machine["factory_id"],
                        machine["line_id"],
                        machine["machine_id"],
                        version,
                        now,
                        machine["latest_event_id"],
                    )
                    opened += 1
        return opened

    async def list_machines(
        self,
        *,
        now: datetime,
        offline_after: timedelta,
        limit: int = 1000,
        offset: int = 0,
        factory_id: str | None = None,
    ) -> tuple[list[dict[str, object]], int]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                "SELECT * FROM machines WHERE ($3::text IS NULL OR factory_id = $3) "
                "ORDER BY factory_id, line_id, machine_id LIMIT $1 OFFSET $2",
                limit,
                offset,
                factory_id,
            )
            total = await connection.fetchval(
                "SELECT count(*) FROM machines WHERE ($1::text IS NULL OR factory_id = $1)",
                factory_id,
            )
        result = []
        for row in rows:
            metrics = row["metrics"]
            if isinstance(metrics, str):
                metrics = json.loads(metrics)
            seen = row["last_seen_at"]
            result.append(
                {
                    "factory_id": row["factory_id"],
                    "line_id": row["line_id"],
                    "machine_id": row["machine_id"],
                    "sequence_no": row["sequence_no"],
                    "latest_event_id": (
                        str(row["latest_event_id"]) if row["latest_event_id"] else None
                    ),
                    "event_time": row["event_time"].isoformat() if row["event_time"] else None,
                    "last_seen_at": seen.isoformat() if seen else None,
                    "online": seen is not None and now - seen < offline_after,
                    "metrics": metrics,
                    "machine_status": row["machine_status"],
                    "error_code": row["error_code"],
                    "sequence_gap_count": row["sequence_gap_count"],
                    "sequence_conflict_count": row["sequence_conflict_count"],
                }
            )
        return result, int(total)

    async def list_events(
        self,
        *,
        limit: int,
        offset: int = 0,
        machine_id: str | None = None,
        factory_id: str | None = None,
    ) -> tuple[list[dict[str, object]], int]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                "SELECT event_id, machine_id, sequence_no, event_time, received_at, processed_at, "
                "sequence_outcome, replayed, payload::text AS payload FROM telemetry_events "
                "WHERE ($3::text IS NULL OR machine_id = $3) "
                "AND ($4::text IS NULL OR factory_id = $4) "
                "ORDER BY received_at DESC, event_id DESC LIMIT $1 OFFSET $2",
                limit,
                offset,
                machine_id,
                factory_id,
            )
            total = await connection.fetchval(
                "SELECT count(*) FROM telemetry_events "
                "WHERE ($1::text IS NULL OR machine_id = $1) "
                "AND ($2::text IS NULL OR factory_id = $2)",
                machine_id,
                factory_id,
            )
        items = [
            {
                "event_id": str(row["event_id"]),
                "machine_id": row["machine_id"],
                "sequence_no": row["sequence_no"],
                "event_time": row["event_time"].isoformat(),
                "received_at": row["received_at"].isoformat(),
                "processed_at": row["processed_at"].isoformat(),
                "sequence_outcome": row["sequence_outcome"],
                "replayed": row["replayed"],
                "payload": json.loads(row["payload"]),
            }
            for row in rows
        ]
        return items, int(total)

    async def list_alerts(self, *, active_only: bool, limit: int) -> list[dict[str, object]]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT episode_id, factory_id, line_id, machine_id, rule_id, rule_version,
                       severity, status, opened_at, last_seen_at, resolved_at,
                       occurrence_count, latest_value, latest_event_id, diagnostic_code
                FROM alert_episodes
                WHERE ($1 = false OR status = 'OPEN')
                ORDER BY (status = 'OPEN') DESC, opened_at DESC
                LIMIT $2
                """,
                active_only,
                limit,
            )
        return [
            {
                "episode_id": str(row["episode_id"]),
                "factory_id": row["factory_id"],
                "line_id": row["line_id"],
                "machine_id": row["machine_id"],
                "rule_id": row["rule_id"],
                "rule_version": row["rule_version"],
                "severity": row["severity"],
                "status": row["status"],
                "opened_at": row["opened_at"].isoformat(),
                "last_seen_at": row["last_seen_at"].isoformat(),
                "resolved_at": row["resolved_at"].isoformat() if row["resolved_at"] else None,
                "occurrence_count": row["occurrence_count"],
                "latest_value": row["latest_value"],
                "latest_event_id": str(row["latest_event_id"]) if row["latest_event_id"] else None,
                "diagnostic_code": row["diagnostic_code"],
            }
            for row in rows
        ]

    async def list_dead_letters(self, *, limit: int) -> list[dict[str, object]]:
        async with self.pool.acquire() as connection:
            rows = await connection.fetch(
                """
                SELECT source_topic, source_partition, source_offset, event_id,
                       error_code, failed_at, replayed_at
                FROM dead_letter_audit
                ORDER BY failed_at DESC, source_topic, source_partition, source_offset DESC
                LIMIT $1
                """,
                limit,
            )
        return [
            {
                "source_topic": row["source_topic"],
                "source_partition": row["source_partition"],
                "source_offset": row["source_offset"],
                "event_id": str(row["event_id"]) if row["event_id"] else None,
                "error_code": row["error_code"],
                "failed_at": row["failed_at"].isoformat(),
                "replayed_at": row["replayed_at"].isoformat() if row["replayed_at"] else None,
            }
            for row in rows
        ]

    async def dead_letter_was_replayed(self, source: tuple[str, int, int]) -> bool:
        async with self.pool.acquire() as connection:
            return bool(
                await connection.fetchval(
                    "SELECT replayed_at IS NOT NULL FROM dead_letter_audit "
                    "WHERE source_topic = $1 AND source_partition = $2 AND source_offset = $3",
                    source[0],
                    source[1],
                    source[2],
                )
            )
