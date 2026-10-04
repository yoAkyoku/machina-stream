from __future__ import annotations

from datetime import UTC, datetime

import httpx
import pytest
from conftest import eventually, list_items, make_event, publish_kafka_record

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_poison_payload_is_dead_lettered_and_processor_keeps_working(
    client: httpx.AsyncClient,
) -> None:
    publish_kafka_record(
        key=f"machine-poison-{datetime.now(UTC).timestamp():.6f}".encode(),
        value=b'{"event_id":',
    )

    async def invalid_json_dead_letter():
        rows = await list_items(client, "/api/v1/dead-letters")
        if rows is None:
            return None
        return next(
            (
                row
                for row in rows
                if row["source_topic"] == "telemetry.events.v1"
                and row["error_code"] == "invalid_json"
            ),
            None,
        )

    record = await eventually(invalid_json_dead_letter)
    assert record["source_partition"] >= 0
    assert record["source_offset"] >= 0

    event = make_event()
    assert (await client.post("/api/v1/telemetry", json=event)).status_code == 202
    rows = await eventually(lambda: _event_rows(client, str(event["event_id"])))
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_http_acceptance_projects_once_even_when_event_is_retried(
    client: httpx.AsyncClient,
) -> None:
    event = make_event()

    first = await client.post("/api/v1/telemetry", json=event)
    second = await client.post("/api/v1/telemetry", json=event)

    assert first.status_code == second.status_code == 202
    rows = await eventually(
        lambda: _event_rows(client, str(event["event_id"])),
    )
    assert len(rows) == 1
    assert rows[0]["sequence_outcome"] == "projected"
    assert datetime.fromisoformat(str(rows[0]["processed_at"])).tzinfo is not None


async def _event_rows(client: httpx.AsyncClient, event_id: str):
    rows = await list_items(client, "/api/v1/events")
    if rows is None:
        return None
    matches = [row for row in rows if row["event_id"] == event_id]
    return matches if matches else None


@pytest.mark.asyncio
async def test_sequence_gaps_late_events_and_conflicts_never_roll_back_projection(
    client: httpx.AsyncClient,
) -> None:
    machine_id = f"machine-seq-{datetime.now(UTC).timestamp():.6f}"
    first = make_event(machine_id=machine_id, sequence_no=1)
    gap = make_event(machine_id=machine_id, sequence_no=3)
    late = make_event(machine_id=machine_id, sequence_no=2)
    conflict = make_event(machine_id=machine_id, sequence_no=3, temperature_c=71)
    duplicate_old_sequence = make_event(machine_id=machine_id, sequence_no=1, temperature_c=72)

    for event in (first, gap, late, conflict, duplicate_old_sequence):
        response = await client.post("/api/v1/telemetry", json=event)
        assert response.status_code == 202

    async def projected():
        rows = await list_items(client, "/api/v1/machines?factory_id=factory-e2e")
        if rows is None:
            return None
        return next(
            (row for row in rows if row["machine_id"] == machine_id and row["sequence_no"] == 3),
            None,
        )

    rows = await eventually(lambda: _all_machine_events(client, machine_id, expected=5))
    machine = await eventually(projected)
    assert machine["latest_event_id"] == gap["event_id"]
    assert machine["sequence_gap_count"] == 1
    assert machine["sequence_conflict_count"] == 2

    outcomes = {row["event_id"]: row["sequence_outcome"] for row in rows}
    assert outcomes[str(gap["event_id"])] == "gap"
    assert outcomes[str(late["event_id"])] == "out_of_order"
    assert outcomes[str(conflict["event_id"])] == "sequence_conflict"
    assert outcomes[str(duplicate_old_sequence["event_id"])] == "sequence_conflict"


async def _all_machine_events(client: httpx.AsyncClient, machine_id: str, *, expected: int):
    rows = await list_items(client, "/api/v1/events")
    if rows is None:
        return None
    matches = [row for row in rows if row["machine_id"] == machine_id]
    return matches if len(matches) == expected else None


@pytest.mark.asyncio
async def test_alert_episode_escalates_resolves_and_reopens_with_history(
    client: httpx.AsyncClient,
) -> None:
    machine_id = f"machine-alert-{datetime.now(UTC).timestamp():.6f}"
    events = [
        make_event(machine_id=machine_id, sequence_no=1, temperature_c=80),
        make_event(machine_id=machine_id, sequence_no=2, temperature_c=98),
        make_event(machine_id=machine_id, sequence_no=3, temperature_c=75),
        make_event(machine_id=machine_id, sequence_no=4, temperature_c=75),
        make_event(machine_id=machine_id, sequence_no=5, temperature_c=82),
    ]
    for event in events:
        response = await client.post("/api/v1/telemetry", json=event)
        assert response.status_code == 202

    await eventually(lambda: _all_machine_events(client, machine_id, expected=5))

    async def temperature_episodes():
        rows = await list_items(client, "/api/v1/alerts?active_only=false")
        if rows is None:
            return None
        episodes = [
            row
            for row in rows
            if row["machine_id"] == machine_id and row["rule_id"] == "temperature_high"
        ]
        return episodes if len(episodes) == 2 else None

    episodes = await eventually(temperature_episodes)
    episodes.sort(key=lambda row: row["opened_at"])
    assert episodes[0]["status"] == "RESOLVED"
    assert episodes[0]["severity"] == "critical"
    assert episodes[0]["occurrence_count"] == 2
    assert episodes[1]["status"] == "OPEN"
    assert episodes[1]["severity"] == "warning"
    assert episodes[1]["occurrence_count"] == 1


@pytest.mark.asyncio
async def test_critical_alert_downgrades_before_it_resolves(
    client: httpx.AsyncClient,
) -> None:
    machine_id = f"machine-hysteresis-{datetime.now(UTC).timestamp():.6f}"
    events = [
        make_event(machine_id=machine_id, sequence_no=1, temperature_c=98),
        make_event(machine_id=machine_id, sequence_no=2, temperature_c=90),
        make_event(machine_id=machine_id, sequence_no=3, temperature_c=85),
        make_event(machine_id=machine_id, sequence_no=4, temperature_c=75),
        make_event(machine_id=machine_id, sequence_no=5, temperature_c=75),
    ]
    for event in events:
        response = await client.post("/api/v1/telemetry", json=event)
        assert response.status_code == 202

    await eventually(lambda: _all_machine_events(client, machine_id, expected=5))

    async def episode_history():
        rows = await list_items(client, "/api/v1/alerts?active_only=false")
        if rows is None:
            return None
        matches = [
            row
            for row in rows
            if row["machine_id"] == machine_id and row["rule_id"] == "temperature_high"
        ]
        return matches if len(matches) == 1 else None

    episodes = await eventually(episode_history)
    assert episodes[0]["status"] == "RESOLVED"
    assert episodes[0]["severity"] == "warning"
    assert episodes[0]["occurrence_count"] == 3


@pytest.mark.asyncio
async def test_fault_alert_opens_immediately_and_resolves_after_two_explicit_recoveries(
    client: httpx.AsyncClient,
) -> None:
    machine_id = f"machine-fault-{datetime.now(UTC).timestamp():.6f}"
    fault = make_event(machine_id=machine_id, sequence_no=1)
    fault["machine_status"] = "fault"
    fault["error_code"] = "E-42"
    recovery_one = make_event(machine_id=machine_id, sequence_no=2)
    recovery_two = make_event(machine_id=machine_id, sequence_no=3)

    assert (await client.post("/api/v1/telemetry", json=fault)).status_code == 202

    async def active_fault():
        rows = await list_items(client, "/api/v1/alerts")
        if rows is None:
            return None
        return next(
            (
                row
                for row in rows
                if row["machine_id"] == machine_id
                and row["rule_id"] == "machine_fault"
                and row["status"] == "OPEN"
            ),
            None,
        )

    opened = await eventually(active_fault)
    assert opened["severity"] == "critical"
    assert opened["diagnostic_code"] == "E-42"

    for event in (recovery_one, recovery_two):
        assert (await client.post("/api/v1/telemetry", json=event)).status_code == 202

    await eventually(lambda: _all_machine_events(client, machine_id, expected=3))
    alerts = await list_items(client, "/api/v1/alerts?active_only=false")
    assert alerts is not None
    history = [
        row
        for row in alerts
        if row["machine_id"] == machine_id and row["rule_id"] == "machine_fault"
    ]
    assert len(history) == 1
    assert history[0]["status"] == "RESOLVED"
    assert history[0]["diagnostic_code"] is None


@pytest.mark.asyncio
async def test_offline_uses_server_freshness_and_only_a_new_sequence_restores_online(
    client: httpx.AsyncClient,
) -> None:
    machine_id = f"machine-offline-{datetime.now(UTC).timestamp():.6f}"
    old_producer_time = "2020-01-01T00:00:00Z"
    first = make_event(machine_id=machine_id, sequence_no=1, event_time=old_producer_time)
    accepted = await client.post("/api/v1/telemetry", json=first)
    assert accepted.status_code == 202
    await eventually(lambda: _event_rows(client, str(first["event_id"])))

    async def active_offline():
        rows = await list_items(client, "/api/v1/alerts")
        if rows is None:
            return None
        return next(
            (
                row
                for row in rows
                if row["machine_id"] == machine_id
                and row["rule_id"] == "machine_offline"
                and row["status"] == "OPEN"
            ),
            None,
        )

    await eventually(active_offline, timeout=12)
    stale_retry = {**first, "event_id": "b0da8b22-1ed0-42c3-a736-2613e4bbde91"}
    assert (await client.post("/api/v1/telemetry", json=stale_retry)).status_code == 202
    await eventually(lambda: _event_rows(client, str(stale_retry["event_id"])))

    async def still_offline():
        machines = await list_items(client, "/api/v1/machines?factory_id=factory-e2e")
        if machines is None:
            return None
        machine = next((row for row in machines if row["machine_id"] == machine_id), None)
        return machine if machine and not machine["online"] else None

    await eventually(still_offline)
    restored = make_event(
        machine_id=machine_id,
        sequence_no=2,
        event_time=old_producer_time,
    )
    assert (await client.post("/api/v1/telemetry", json=restored)).status_code == 202

    async def online_again():
        machines = await list_items(client, "/api/v1/machines?factory_id=factory-e2e")
        if machines is None:
            return None
        machine = next((row for row in machines if row["machine_id"] == machine_id), None)
        return machine if machine and machine["online"] and machine["sequence_no"] == 2 else None

    await eventually(online_again)
    alerts = await list_items(client, "/api/v1/alerts?active_only=false")
    assert alerts is not None
    offline_history = [
        row
        for row in alerts
        if row["machine_id"] == machine_id and row["rule_id"] == "machine_offline"
    ]
    assert len(offline_history) == 1
    assert offline_history[0]["status"] == "RESOLVED"


@pytest.mark.asyncio
async def test_event_id_with_changed_content_is_dlq_and_never_overwrites_history(
    client: httpx.AsyncClient,
) -> None:
    event = make_event()
    changed = {**event, "sequence_no": 2, "metrics": {**event["metrics"], "temperature_c": 74}}
    assert (await client.post("/api/v1/telemetry", json=event)).status_code == 202
    assert (await client.post("/api/v1/telemetry", json=changed)).status_code == 202

    async def dead_letter():
        rows = await list_items(client, "/api/v1/dead-letters")
        if rows is None:
            return None
        return next((row for row in rows if row["event_id"] == event["event_id"]), None)

    record = await eventually(dead_letter)
    assert record["error_code"] == "event_id_conflict"

    async def single_event():
        rows = await list_items(client, "/api/v1/events")
        if rows is None:
            return None
        matches = [row for row in rows if row["event_id"] == event["event_id"]]
        return matches if len(matches) == 1 else None

    rows = await eventually(single_event)
    assert rows[0]["payload"]["metrics"]["temperature_c"] == event["metrics"]["temperature_c"]
