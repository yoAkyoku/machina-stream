from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import random
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from typing import Any

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError, KafkaTimeoutError, UnknownTopicOrPartitionError
from aiokafka.structs import OffsetAndMetadata, TopicPartition
from prometheus_client import Counter, Gauge, start_http_server

from machina_stream.contracts import format_utc
from machina_stream.metrics import POSTGRES_UP, observe_postgres_health
from machina_stream.store import (
    MAX_KAFKA_OFFSET,
    MAX_KAFKA_PARTITION,
    PermanentMessageError,
    ProjectionStore,
)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("machina_stream.worker")

PROCESSED = Counter("machina_processor_events_total", "Processor message outcomes", ["outcome"])
RETRIES = Counter("machina_processor_retries_total", "Transient database retries")
DLQ_PUBLISHED = Counter(
    "machina_dlq_published_total", "Poison or exhausted messages published to the DLQ", ["reason"]
)
DLQ_AUDITED = Counter("machina_dlq_audited_total", "DLQ records persisted to PostgreSQL")
CONSUMER_LAG = Gauge(
    "machina_kafka_consumer_lag_records",
    "Uncommitted telemetry records by partition",
    ["partition"],
)
PROCESSOR_READY = Gauge(
    "machina_processor_ready",
    "Processor has started Kafka consumption and owns at least one telemetry partition",
)
TELEMETRY_TOPIC = os.getenv("KAFKA_TELEMETRY_TOPIC", "telemetry.events.v1")
DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "telemetry.events.v1.dlq")
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


async def _poll_batch(consumer: AIOKafkaConsumer) -> list[Any]:
    """Poll a batch without relying on the timeout argument removed from getone()."""
    try:
        batches = await consumer.getmany(timeout_ms=1000, max_records=100)
    except KafkaTimeoutError:
        return []
    return [message for records in batches.values() for message in records]


async def _wait_for_assignment(consumer: AIOKafkaConsumer, *, timeout: float = 90.0) -> None:
    """Do not report readiness until Kafka assigned a telemetry partition."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not consumer.assignment():
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("Kafka consumer did not receive a telemetry partition assignment")
        await asyncio.sleep(0.1)


def _commit_offsets(message: Any) -> dict[TopicPartition, OffsetAndMetadata]:
    partition = TopicPartition(message.topic, message.partition)
    return {partition: OffsetAndMetadata(message.offset + 1, "")}


def _replay_source(headers: dict[str, bytes | None]) -> tuple[str, int, int] | None:
    raw = headers.get("machina-replay-source")
    if raw is None:
        return None
    try:
        topic, partition, offset = raw.decode("utf-8").rsplit(":", 2)
        parsed_partition = int(partition)
        parsed_offset = int(offset)
        if (
            topic != TELEMETRY_TOPIC
            or parsed_partition < 0
            or parsed_offset < 0
            or parsed_partition > MAX_KAFKA_PARTITION
            or parsed_offset > MAX_KAFKA_OFFSET
        ):
            raise ValueError("replay source coordinates are out of range")
        return topic, parsed_partition, parsed_offset
    except (UnicodeDecodeError, ValueError) as error:
        raise PermanentMessageError(
            "invalid_replay_source", "Replay source header is malformed"
        ) from error


def _dead_letter_record(
    message: Any,
    *,
    reason: str,
    attempts: int,
    payload: dict[str, object] | None,
) -> bytes:
    event_id = payload.get("event_id") if payload else None
    schema_version = payload.get("schema_version") if payload else None
    record: dict[str, object] = {
        "source": {
            "topic": message.topic,
            "partition": message.partition,
            "offset": message.offset,
        },
        "original_key_base64": base64.b64encode(message.key or b"").decode("ascii"),
        "original_payload_base64": base64.b64encode(message.value or b"").decode("ascii"),
        "attempts": attempts,
        "failed_at": format_utc(datetime.now(UTC)),
        "error": {
            "code": reason,
            "message": (
                "Event was not projected; inspect the source payload and replay only after repair."
            ),
        },
    }
    if event_id is not None:
        record["event_id"] = str(event_id)
    if schema_version is not None:
        record["schema_version"] = schema_version
    try:
        record["payload_text"] = (message.value or b"").decode("utf-8")
    except UnicodeDecodeError:
        record["payload_text"] = None
    return json.dumps(record, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


class EventProcessor:
    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        producer: AIOKafkaProducer,
        store: ProjectionStore,
    ) -> None:
        self.consumer = consumer
        self.producer = producer
        self.store = store
        self._commit_lock = asyncio.Lock()
        self.offline_after = timedelta(
            seconds=3 * float(os.getenv("SIMULATOR_EVENT_INTERVAL_SECONDS", "10"))
        )
        self._next_offline_scan = datetime.min.replace(tzinfo=UTC)
        self._offline_scan_task: asyncio.Task[None] | None = None

    async def run(self) -> None:
        try:
            while True:
                self._schedule_offline_scan()
                messages = await _poll_batch(self.consumer)
                if not messages:
                    await self._update_lag()
                    continue
                by_partition: dict[int, list[Any]] = {}
                for message in messages:
                    by_partition.setdefault(message.partition, []).append(message)
                await asyncio.gather(
                    *(
                        self._process_partition(partition_messages)
                        for partition_messages in by_partition.values()
                    )
                )
                self._schedule_offline_scan()
                await self._update_lag()
        finally:
            if self._offline_scan_task is not None:
                self._offline_scan_task.cancel()
                with suppress(asyncio.CancelledError):
                    await self._offline_scan_task

    def _schedule_offline_scan(self) -> None:
        if (self._offline_scan_task is None or self._offline_scan_task.done()) and datetime.now(
            UTC
        ) >= self._next_offline_scan:
            self._offline_scan_task = asyncio.create_task(self._scan_offline())

    async def _process_partition(self, messages: list[Any]) -> None:
        """Preserve Kafka order within a partition while processing partitions concurrently."""
        by_key: dict[bytes, list[Any]] = {}
        for message in messages:
            # Kafka preserves order for a key. Process different machine keys
            # concurrently, while each key remains strictly ordered.
            by_key.setdefault(message.key or b"", []).append(message)
        await asyncio.gather(*(self._process_key(key_messages) for key_messages in by_key.values()))
        if messages:
            # Projection commits are intentionally batched per poll. Every
            # database write still completes before this offset advances;
            # a crash replays the batch through event-id idempotency.
            await self._commit(messages[-1])

    async def _process_key(self, messages: list[Any]) -> None:
        for message in messages:
            await self._process(message, commit=False)

    async def _update_lag(self) -> None:
        try:
            total = 0
            for partition in self.consumer.assignment():
                highwater = self.consumer.highwater(partition)
                committed = await self.consumer.committed(partition)
                if highwater is not None:
                    lag = max(0, highwater - (committed or 0))
                    CONSUMER_LAG.labels(partition=str(partition.partition)).set(lag)
                    total += lag
            CONSUMER_LAG.labels(partition="total").set(total)
        except (KafkaError, OSError, TimeoutError):
            logger.warning("Kafka consumer lag unavailable; skipping lag update")

    async def _scan_offline(self) -> None:
        now = datetime.now(UTC)
        if now < self._next_offline_scan:
            return
        self._next_offline_scan = now + timedelta(seconds=1)
        try:
            opened = await self.store.check_offline(now, timeout=self.offline_after)
        except (
            asyncpg.PostgresError,
            asyncpg.InterfaceError,
            OSError,
            TimeoutError,
            ConnectionError,
        ):
            logger.warning("database unavailable; skipping this offline scan")
            return
        if opened:
            logger.info("opened machine offline episodes count=%d", opened)

    async def _process(self, message: Any, *, commit: bool = True) -> None:
        raw_payload: dict[str, object] | None = None
        headers = dict(message.headers or [])
        replayed = headers.get("machina-replay") == b"true"
        attempts = 0
        reason = ""

        try:
            replay_source = _replay_source(headers) if replayed else None
            if replayed and replay_source is None:
                raise PermanentMessageError(
                    "invalid_replay_source", "Replay source header is required"
                )
            decoded = json.loads((message.value or b"").decode("utf-8"))
            if not isinstance(decoded, dict):
                raise PermanentMessageError(
                    "invalid_payload", "Kafka payload must be a JSON object"
                )
            raw_payload = decoded
            for attempts in range(1, 6):
                try:
                    result = await self.store.apply(
                        raw_payload,
                        replayed=replayed,
                        replay_source=replay_source,
                    )
                    PROCESSED.labels(outcome=result.outcome).inc()
                    if commit:
                        await self._commit(message)
                    return
                except PermanentMessageError as error:
                    reason = error.code
                    break
                except (
                    asyncpg.PostgresError,
                    asyncpg.InterfaceError,
                    OSError,
                    TimeoutError,
                    ConnectionError,
                ):
                    if attempts == 5:
                        logger.exception("database projection failed after %d attempts", attempts)
                        reason = "database_retry_exhausted"
                        break
                    RETRIES.inc()
                    ceiling = min(30.0, 2 ** (attempts - 1))
                    await asyncio.sleep(random.uniform(0, ceiling))
        except (UnicodeDecodeError, json.JSONDecodeError):
            reason = "invalid_json"
            attempts = max(attempts, 1)
        except PermanentMessageError as error:
            reason = error.code
            attempts = max(attempts, 1)

        if not reason:
            raise RuntimeError("message processing ended without a durable result")

        await self._publish_dlq(message, reason=reason, attempts=attempts, payload=raw_payload)
        PROCESSED.labels(outcome="dead_letter").inc()
        if commit:
            try:
                await self._commit(message)
            except KafkaError:
                logger.exception(
                    "source offset commit failed after DLQ publish topic=%s partition=%d offset=%d",
                    message.topic,
                    message.partition,
                    message.offset,
                )
                raise
            logger.info(
                "committed source offset after DLQ publish topic=%s partition=%d offset=%d",
                message.topic,
                message.partition,
                message.offset,
            )

    async def _commit(self, message: Any) -> None:
        async with self._commit_lock:
            await self.consumer.commit(_commit_offsets(message))

    async def _publish_dlq(
        self,
        message: Any,
        *,
        reason: str,
        attempts: int,
        payload: dict[str, object] | None,
    ) -> None:
        encoded = _dead_letter_record(
            message,
            reason=reason,
            attempts=attempts,
            payload=payload,
        )
        partition = TopicPartition(message.topic, message.partition)
        self.consumer.pause(partition)
        try:
            while True:
                try:
                    await self.producer.send_and_wait(
                        DLQ_TOPIC,
                        key=message.key,
                        value=encoded,
                    )
                    DLQ_PUBLISHED.labels(reason=reason).inc()
                    logger.info(
                        "published DLQ record topic=%s source_partition=%d source_offset=%d",
                        DLQ_TOPIC,
                        message.partition,
                        message.offset,
                    )
                    return
                except Exception:
                    logger.warning(
                        "DLQ broker acknowledgement unavailable; source offset remains uncommitted"
                    )
                    await asyncio.sleep(1)
        finally:
            self.consumer.resume(partition)


class DeadLetterAuditor:
    def __init__(self, consumer: AIOKafkaConsumer, store: ProjectionStore) -> None:
        self.consumer = consumer
        self.store = store

    async def run(self) -> None:
        while True:
            messages = await _poll_batch(self.consumer)
            if not messages:
                continue
            for message in messages:
                partition = TopicPartition(message.topic, message.partition)
                self.consumer.pause(partition)
                try:
                    while True:
                        try:
                            record = json.loads((message.value or b"").decode("utf-8"))
                            if not isinstance(record, dict):
                                raise PermanentMessageError(
                                    "invalid_dlq_record", "DLQ record must be a JSON object"
                                )
                            await self.store.record_dead_letter(record)
                            await self.consumer.commit(_commit_offsets(message))
                            DLQ_AUDITED.inc()
                            break
                        except (
                            asyncpg.PostgresError,
                            asyncpg.InterfaceError,
                            OSError,
                            TimeoutError,
                            ConnectionError,
                        ):
                            logger.warning(
                                "DLQ audit database unavailable; offset remains uncommitted"
                            )
                            await asyncio.sleep(1)
                        except (UnicodeDecodeError, json.JSONDecodeError, PermanentMessageError):
                            # Keep malformed DLQ bytes at their source offset as an integrity
                            # record.
                            logger.exception("malformed DLQ entry retained at its source offset")
                            await asyncio.sleep(5)
                finally:
                    self.consumer.resume(partition)


async def _run_auditor_when_topic_is_available(
    consumer: AIOKafkaConsumer,
    store: ProjectionStore,
) -> None:
    """Keep the source processor alive while a missing DLQ topic is provisioned."""
    while True:
        try:
            await consumer.start()
            break
        except UnknownTopicOrPartitionError:
            logger.warning("DLQ topic unavailable; auditor will retry after provisioning")
            with suppress(Exception):
                await consumer.stop()
            await asyncio.sleep(1)
    await DeadLetterAuditor(consumer, store).run()


async def run_worker() -> None:
    metrics_port = int(os.getenv("PROCESSOR_METRICS_PORT", "9101"))
    start_http_server(metrics_port, addr="0.0.0.0")
    PROCESSOR_READY.set(0)
    store = await ProjectionStore.connect()
    POSTGRES_UP.labels(service="processor").set(0)
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks="all",
        enable_idempotence=True,
        # Bound a missing/unavailable DLQ broker acknowledgement so the
        # source offset remains visibly uncommitted within the recovery gate.
        request_timeout_ms=10_000,
    )
    processor_consumer = AIOKafkaConsumer(
        TELEMETRY_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=os.getenv("KAFKA_CONSUMER_GROUP", "machina-processor-v1"),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=100,
    )
    auditor_consumer = AIOKafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=os.getenv("KAFKA_DLQ_AUDIT_GROUP", "machina-dlq-audit-v1"),
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=100,
    )

    auditor_task: asyncio.Task[None] | None = None
    health_task: asyncio.Task[None] | None = None
    try:
        await producer.start()
        await processor_consumer.start()
        await _wait_for_assignment(processor_consumer)
        PROCESSOR_READY.set(1)
        auditor_task = asyncio.create_task(
            _run_auditor_when_topic_is_available(auditor_consumer, store)
        )
        health_task = asyncio.create_task(observe_postgres_health(store, service="processor"))
        await asyncio.gather(
            EventProcessor(processor_consumer, producer, store).run(),
            auditor_task,
            health_task,
        )
    finally:
        PROCESSOR_READY.set(0)
        if auditor_task is not None:
            auditor_task.cancel()
            with suppress(asyncio.CancelledError):
                await auditor_task
        if health_task is not None:
            health_task.cancel()
            with suppress(asyncio.CancelledError):
                await health_task
        await auditor_consumer.stop()
        await processor_consumer.stop()
        await producer.stop()
        await store.close()


def main() -> None:
    try:
        asyncio.run(run_worker())
    except KeyboardInterrupt:
        logger.info("processor stopped")


if __name__ == "__main__":
    main()
