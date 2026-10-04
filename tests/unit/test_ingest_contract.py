import asyncio
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncByteStream, AsyncClient
from machina_stream.api import create_app
from machina_stream.contracts import format_utc


class RecordingPublisher:
    def __init__(self, *, fail: bool = False) -> None:
        self.messages: list[tuple[bytes, bytes]] = []
        self.fail = fail

    async def publish(self, *, key: bytes, value: bytes) -> None:
        if self.fail:
            raise RuntimeError("broker unavailable")
        self.messages.append((key, value))


class HealthyStore:
    async def healthy(self) -> bool:
        return True


class OversizedBodyStream(AsyncByteStream):
    async def __aiter__(self):
        yield b" " * (256 * 1024 + 1)


@pytest.fixture
def valid_event() -> dict[str, object]:
    return {
        "event_id": "550e8400-e29b-41d4-a716-446655440000",
        "schema_version": 1,
        "factory_id": "factory-demo",
        "line_id": "line-a",
        "machine_id": "machine-042",
        "sequence_no": 101,
        "event_time": "2026-09-27T03:00:00Z",
        "received_at": "2000-01-01T00:00:00Z",
        "metrics": {
            "temperature_c": 74.2,
            "vibration_rms_mm_s": 2.1,
            "current_a": 8.6,
        },
        "machine_status": "running",
        "error_code": None,
    }


def test_format_utc_omits_zero_fraction_and_preserves_precision() -> None:
    whole_second = datetime(2026, 9, 27, 3, 1, tzinfo=UTC)
    precise = datetime(2026, 9, 27, 3, 1, 0, 123400, tzinfo=UTC)

    assert format_utc(whole_second) == "2026-09-27T03:01:00Z"
    assert format_utc(precise) == "2026-09-27T03:01:00.1234Z"


async def post_event(publisher: RecordingPublisher, body: object) -> tuple[int, dict[str, object]]:
    app = create_app(
        publisher=publisher,
        clock=lambda: datetime(2026, 9, 27, 3, 1, tzinfo=UTC),
    )
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post("/api/v1/telemetry", json=body)
    return response.status_code, response.json()


@pytest.mark.asyncio
async def test_valid_event_is_accepted_only_after_keyed_broker_ack(
    valid_event: dict[str, object],
) -> None:
    publisher = RecordingPublisher()

    status, body = await post_event(publisher, valid_event)

    assert status == 202
    assert body["event_id"] == valid_event["event_id"]
    assert len(publisher.messages) == 1
    key, value = publisher.messages[0]
    assert key == b"machine-042"
    assert b'"received_at":"2026-09-27T03:01:00Z"' in value


@pytest.mark.asyncio
async def test_invalid_json_is_400_and_never_published() -> None:
    publisher = RecordingPublisher()
    app = create_app(publisher=publisher)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/telemetry",
            content=b'{"event_id":',
            headers={"content-type": "application/json"},
        )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "invalid_json"
    assert publisher.messages == []


@pytest.mark.asyncio
async def test_oversized_body_is_rejected_before_publication() -> None:
    publisher = RecordingPublisher()
    app = create_app(publisher=publisher)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/telemetry",
            content=b" " * (256 * 1024 + 1),
            headers={"content-type": "application/json"},
        )

    assert response.status_code == 413
    assert response.json()["error"]["code"] == "request_body_too_large"
    assert publisher.messages == []


@pytest.mark.asyncio
async def test_oversized_chunked_body_is_rejected_before_buffering_or_publication() -> None:
    publisher = RecordingPublisher()
    app = create_app(publisher=publisher)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.post(
            "/api/v1/telemetry",
            content=OversizedBodyStream(),
            headers={"content-type": "application/json"},
        )

    assert response.status_code == 413
    assert response.json()["error"]["code"] == "request_body_too_large"
    assert publisher.messages == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("change", "field"),
    [
        ({"schema_version": 2}, "schema_version"),
        ({"event_id": "550e8400-e29b-11d4-a716-446655440000"}, "event_id"),
        ({"event_time": "2026-09-27T03:00:00"}, "event_time"),
        ({"factory_id": "   "}, "factory_id"),
        ({"sequence_no": 0}, "sequence_no"),
        ({"sequence_no": True}, "sequence_no"),
        (
            {"metrics": {"temperature_c": 70, "vibration_rms_mm_s": 101, "current_a": 8}},
            "metrics.vibration_rms_mm_s",
        ),
        (
            {"metrics": {"temperature_c": True, "vibration_rms_mm_s": 2, "current_a": 8}},
            "metrics.temperature_c",
        ),
    ],
)
async def test_schema_errors_are_422_and_never_published(
    valid_event: dict[str, object], change: dict[str, object], field: str
) -> None:
    publisher = RecordingPublisher()
    event = {**valid_event, **change}

    status, body = await post_event(publisher, event)

    assert status == 422
    assert body["error"]["code"] == "schema_validation_failed"
    assert field in {issue["field"] for issue in body["error"]["issues"]}
    assert publisher.messages == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    ["event_id", "factory_id", "line_id", "machine_id", "sequence_no", "event_time", "metrics"],
)
async def test_missing_required_fields_are_422_and_never_published(
    valid_event: dict[str, object], field: str
) -> None:
    publisher = RecordingPublisher()
    event = {**valid_event}
    event.pop(field)

    status, body = await post_event(publisher, event)

    assert status == 422
    assert body["error"]["code"] == "schema_validation_failed"
    assert field in {issue["field"] for issue in body["error"]["issues"]}
    assert publisher.messages == []


@pytest.mark.asyncio
async def test_future_event_time_more_than_five_minutes_is_rejected(
    valid_event: dict[str, object],
) -> None:
    publisher = RecordingPublisher()
    valid_event["event_time"] = "2026-09-27T03:06:01Z"

    status, body = await post_event(publisher, valid_event)

    assert status == 422
    assert body["error"]["code"] == "schema_validation_failed"
    assert publisher.messages == []


@pytest.mark.asyncio
async def test_broker_failure_never_reports_acceptance(valid_event: dict[str, object]) -> None:
    status, body = await post_event(RecordingPublisher(fail=True), valid_event)

    assert status == 503
    assert body["error"]["code"] == "broker_unavailable"


@pytest.mark.asyncio
async def test_postgres_health_metric_is_exposed_during_app_lifespan(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("MACHINA_DB_HEALTH_INTERVAL_SECONDS", "0.1")
    app = create_app(publisher=RecordingPublisher(), store=HealthyStore())  # type: ignore[arg-type]

    async with app.router.lifespan_context(app):
        await asyncio.sleep(0.11)
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/metrics")

    assert response.status_code == 200
    metric_lines = [
        line
        for line in response.text.splitlines()
        if line.startswith('machina_postgres_up{service="api"}')
    ]
    assert metric_lines
    assert float(metric_lines[-1].rsplit(" ", 1)[-1]) == 1
