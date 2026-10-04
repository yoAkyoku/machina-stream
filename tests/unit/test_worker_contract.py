import asyncio
import base64
import json
from types import SimpleNamespace

import asyncpg
import pytest
from aiokafka.errors import KafkaError
from machina_stream.replay_dlq import _decode_original_payload, _record_source
from machina_stream.store import PermanentMessageError, ProjectionStore
from machina_stream.worker import (
    EventProcessor,
    _dead_letter_record,
    _poll_batch,
    _replay_source,
    _wait_for_assignment,
)


def test_replay_source_accepts_non_negative_coordinates_for_the_telemetry_topic() -> None:
    assert _replay_source({"machina-replay-source": b"telemetry.events.v1:2:41"}) == (
        "telemetry.events.v1",
        2,
        41,
    )


@pytest.mark.parametrize(
    "header",
    [
        b"",
        b"telemetry.events.v1:partition:41",
        b"telemetry.events.v1:-1:41",
        b"telemetry.events.v1:2:-1",
        b"telemetry.events.v1:2147483648:41",
        b"telemetry.events.v1:2:9223372036854775808",
        b"other-topic:2:41",
        b"telemetry.events.v1:2",
        b"\xff:2:41",
    ],
)
def test_replay_source_rejects_malformed_or_out_of_range_coordinates(header: bytes) -> None:
    with pytest.raises(PermanentMessageError, match="Replay source header is malformed") as error:
        _replay_source({"machina-replay-source": header})

    assert error.value.code == "invalid_replay_source"


def test_replay_source_is_optional_for_non_replayed_messages() -> None:
    assert _replay_source({}) is None


@pytest.mark.parametrize(
    "source",
    [
        {"topic": None, "partition": 0, "offset": 1},
        {"topic": "telemetry.events.v1", "partition": True, "offset": 1},
        {"topic": "telemetry.events.v1", "partition": 0.5, "offset": 1},
    ],
)
def test_record_source_rejects_non_strict_json_types(source: dict[str, object]) -> None:
    with pytest.raises(ValueError, match="valid source coordinates"):
        _record_source({"source": source})


@pytest.mark.parametrize(
    "source",
    [
        {"topic": "telemetry.events.v1", "partition": 2147483648, "offset": 1},
        {"topic": "telemetry.events.v1", "partition": 0, "offset": 9223372036854775808},
    ],
)
def test_record_source_rejects_coordinates_outside_kafka_storage_bounds(
    source: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="configured telemetry topic"):
        _record_source({"source": source})


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source",
    [
        {"topic": "", "partition": 0, "offset": 1},
        {"topic": "other-topic", "partition": 0, "offset": 1},
        {"topic": "telemetry.events.v1", "partition": -1, "offset": 1},
        {"topic": "telemetry.events.v1", "partition": 0, "offset": -1},
        {"topic": None, "partition": 0, "offset": 1},
        {"topic": "telemetry.events.v1", "partition": True, "offset": 1},
        {"topic": "telemetry.events.v1", "partition": 0.5, "offset": 1},
        {"topic": "telemetry.events.v1", "partition": 2147483648, "offset": 1},
        {"topic": "telemetry.events.v1", "partition": 0, "offset": 9223372036854775808},
    ],
)
async def test_dead_letter_audit_rejects_invalid_source_coordinates(
    source: dict[str, object],
) -> None:
    store = ProjectionStore(None)  # type: ignore[arg-type]
    record = {"source": source, "failed_at": "2026-09-27T03:01:00Z"}

    with pytest.raises(PermanentMessageError, match="DLQ source coordinates are invalid") as error:
        await store.record_dead_letter(record)

    assert error.value.code == "invalid_dlq_record"


def test_decode_original_payload_preserves_key_and_value_bytes() -> None:
    record = {
        "original_key_base64": base64.b64encode(b"machine-42").decode(),
        "original_payload_base64": base64.b64encode(b'{"event_id":"x"}').decode(),
    }

    assert _decode_original_payload(record) == (b"machine-42", b'{"event_id":"x"}')


@pytest.mark.parametrize(
    "record",
    [
        {},
        {"original_key_base64": "!", "original_payload_base64": "eA=="},
        {"original_key_base64": "bQ==", "original_payload_base64": "!"},
    ],
)
def test_decode_original_payload_rejects_missing_or_invalid_base64(
    record: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="valid original payload bytes"):
        _decode_original_payload(record)


def test_dead_letter_record_preserves_source_and_binary_payload() -> None:
    message = SimpleNamespace(
        topic="telemetry.events.v1",
        partition=2,
        offset=41,
        key=b"machine-42",
        value=b"\xff\x00",
    )

    record = json.loads(
        _dead_letter_record(message, reason="invalid_json", attempts=1, payload=None)
    )

    assert record["source"] == {
        "topic": "telemetry.events.v1",
        "partition": 2,
        "offset": 41,
    }
    assert base64.b64decode(record["original_key_base64"]) == b"machine-42"
    assert base64.b64decode(record["original_payload_base64"]) == b"\xff\x00"
    assert record["payload_text"] is None
    assert record["error"]["code"] == "invalid_json"


def test_dead_letter_record_copies_event_metadata_when_payload_is_json() -> None:
    message = SimpleNamespace(
        topic="telemetry.events.v1",
        partition=0,
        offset=7,
        key=b"machine-01",
        value=b'{"event_id":"550e8400-e29b-41d4-a716-446655440000"}',
    )

    record = json.loads(
        _dead_letter_record(
            message,
            reason="invalid_schema",
            attempts=2,
            payload={
                "event_id": "550e8400-e29b-41d4-a716-446655440000",
                "schema_version": 1,
            },
        )
    )

    assert record["event_id"] == "550e8400-e29b-41d4-a716-446655440000"
    assert record["schema_version"] == 1
    assert record["payload_text"] == '{"event_id":"550e8400-e29b-41d4-a716-446655440000"}'


class FailingLagConsumer:
    def assignment(self):
        return [SimpleNamespace(partition=3)]

    def highwater(self, partition):
        return 10

    async def committed(self, partition):
        raise KafkaError("broker temporarily unavailable")


class CommitRecordingConsumer:
    def __init__(self) -> None:
        self.commits: list[object] = []

    async def commit(self, offsets) -> None:
        self.commits.append(offsets)


class TransientStore:
    def __init__(self) -> None:
        self.attempts = 0

    async def apply(self, raw, *, replayed=False, replay_source=None):
        self.attempts += 1
        raise asyncpg.PostgresError("database temporarily unavailable")


class RecordingProducer:
    def __init__(self) -> None:
        self.messages: list[tuple[object, bytes, bytes]] = []

    async def send_and_wait(self, topic, *, key, value) -> None:
        self.messages.append((topic, key, value))


class FailingProducer:
    def __init__(self) -> None:
        self.attempts = 0

    async def send_and_wait(self, topic, *, key, value) -> None:
        self.attempts += 1
        raise RuntimeError("DLQ broker unavailable")


class PauseRecordingConsumer(CommitRecordingConsumer):
    def __init__(self) -> None:
        super().__init__()
        self.paused: list[object] = []
        self.resumed: list[object] = []

    def pause(self, partition) -> None:
        self.paused.append(partition)

    def resume(self, partition) -> None:
        self.resumed.append(partition)


class BatchConsumer:
    def __init__(self, batches) -> None:
        self.batches = batches
        self.calls: list[dict[str, int]] = []

    async def getmany(self, **kwargs):
        self.calls.append(kwargs)
        return self.batches


class AssignmentConsumer:
    def __init__(self, assignments) -> None:
        self.assignments = iter(assignments)
        self.current = set()

    def assignment(self):
        self.current = next(self.assignments, self.current)
        return self.current


@pytest.mark.asyncio
async def test_poll_batch_uses_getmany_timeout_and_preserves_all_records() -> None:
    messages = [SimpleNamespace(offset=7), SimpleNamespace(offset=8)]
    consumer = BatchConsumer({"partition-2": messages})

    assert await _poll_batch(consumer) == messages
    assert consumer.calls == [{"timeout_ms": 1000, "max_records": 100}]


@pytest.mark.asyncio
async def test_poll_batch_returns_empty_for_empty_poll() -> None:
    consumer = BatchConsumer({})

    assert await _poll_batch(consumer) == []


@pytest.mark.asyncio
async def test_wait_for_assignment_does_not_report_readiness_before_kafka_assignment() -> None:
    consumer = AssignmentConsumer([set(), set(), {2}])

    await _wait_for_assignment(consumer, timeout=1)

    assert consumer.current


@pytest.mark.asyncio
async def test_database_retry_budget_routes_to_dlq_before_source_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def skip_backoff(_delay: float) -> None:
        return None

    monkeypatch.setattr("machina_stream.worker.asyncio.sleep", skip_backoff)
    consumer = PauseRecordingConsumer()
    producer = RecordingProducer()
    store = TransientStore()
    processor = EventProcessor(consumer, producer, store)
    message = SimpleNamespace(
        topic="telemetry.events.v1",
        partition=2,
        offset=41,
        key=b"machine-42",
        value=b"{}",
        headers=[],
    )

    await processor._process(message)

    assert store.attempts == 5
    assert len(producer.messages) == 1
    assert producer.messages[0][0] == "telemetry.events.v1.dlq"
    dead_letter = json.loads(producer.messages[0][2])
    assert dead_letter["attempts"] == 5
    assert dead_letter["error"]["code"] == "database_retry_exhausted"
    assert dead_letter["source"] == {"topic": "telemetry.events.v1", "partition": 2, "offset": 41}
    assert len(consumer.commits) == 1
    assert len(consumer.paused) == len(consumer.resumed) == 1


@pytest.mark.asyncio
async def test_dlq_broker_failure_keeps_source_offset_uncommitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def cancel_retry(_delay: float) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr("machina_stream.worker.asyncio.sleep", cancel_retry)
    consumer = PauseRecordingConsumer()
    producer = FailingProducer()
    processor = EventProcessor(consumer, producer, None)
    message = SimpleNamespace(
        topic="telemetry.events.v1",
        partition=0,
        offset=7,
        key=b"machine-42",
        value=b'{"event_id":',
        headers=[],
    )

    with pytest.raises(asyncio.CancelledError):
        await processor._process(message)

    assert producer.attempts == 1
    assert consumer.commits == []
    assert len(consumer.paused) == len(consumer.resumed) == 1


@pytest.mark.asyncio
async def test_lag_update_does_not_stop_processor_on_kafka_error() -> None:
    processor = EventProcessor(FailingLagConsumer(), None, None)

    await processor._update_lag()


@pytest.mark.asyncio
async def test_replayed_message_without_source_is_dead_lettered() -> None:
    consumer = CommitRecordingConsumer()
    processor = EventProcessor(consumer, None, None)
    captured: dict[str, object] = {}

    async def capture_dlq(message, *, reason, attempts, payload) -> None:
        captured.update(reason=reason, attempts=attempts, payload=payload)

    processor._publish_dlq = capture_dlq
    message = SimpleNamespace(
        topic="telemetry.events.v1",
        partition=0,
        offset=9,
        key=b"machine-42",
        value=b"{}",
        headers=[("machina-replay", b"true")],
    )

    await processor._process(message)

    assert captured == {
        "reason": "invalid_replay_source",
        "attempts": 1,
        "payload": None,
    }
    assert len(consumer.commits) == 1
