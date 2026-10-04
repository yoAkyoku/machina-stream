from __future__ import annotations

import asyncio
import json
import subprocess
import textwrap
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest
from conftest import (
    compose,
    compose_project,
    docker,
    eventually,
    list_items,
    make_event,
    post_telemetry_with_transport_retry,
    publish_kafka_record,
    wait_for_postgres,
)

pytestmark = [pytest.mark.integration, pytest.mark.recovery]


@pytest.mark.asyncio
async def test_simulator_restart_continues_machine_sequence(client: httpx.AsyncClient) -> None:
    compose("up", "-d", "simulator")

    async def simulated_machine():
        rows = await list_items(
            client,
            "/api/v1/machines?factory_id=factory-demo",
            limit=5000,
        )
        if rows is None:
            return None
        return next((row for row in rows if row["machine_id"] == "machine-0000"), None)

    first = await eventually(
        lambda: _machine_with_sequence(simulated_machine),
        timeout=60,
    )
    previous_sequence = int(first["sequence_no"])
    compose("restart", "simulator")
    continued = await eventually(
        lambda: _machine_after_sequence(simulated_machine, previous_sequence),
        timeout=30,
    )
    assert int(continued["sequence_no"]) > previous_sequence
    compose("stop", "simulator")


@pytest.mark.asyncio
async def test_acknowledged_event_survives_api_and_processor_restart(
    client: httpx.AsyncClient,
) -> None:
    machine_id = f"machine-restart-{uuid4().hex[:10]}"
    event = make_event(machine_id=machine_id, sequence_no=1)

    compose("stop", "processor")
    accepted = await client.post("/api/v1/telemetry", json=event)
    assert accepted.status_code == 202
    compose("stop", "api")
    compose("start", "api")

    async def api_ready():
        try:
            response = await client.get("/health/ready")
        except httpx.HTTPError:
            return None
        return True if response.status_code == 200 else None

    await eventually(api_ready, timeout=60)
    compose("start", "processor")

    projected = await eventually(lambda: _event_by_id(client, str(event["event_id"])))
    assert projected["machine_id"] == machine_id
    assert await eventually(lambda: _event_count(client, str(event["event_id"]))) == 1


@pytest.mark.asyncio
async def test_kafka_restart_preserves_acknowledged_event_for_processor_recovery(
    client: httpx.AsyncClient,
) -> None:
    machine_id = f"machine-kafka-restart-{uuid4().hex[:10]}"
    factory_id = f"factory-kafka-restart-{uuid4().hex[:10]}"
    event = make_event(machine_id=machine_id, sequence_no=1)
    event["factory_id"] = factory_id
    event["line_id"] = "line-kafka-restart"
    after_restart = make_event(
        machine_id=f"machine-kafka-restart-{uuid4().hex[:10]}",
        sequence_no=1,
    )
    after_restart["factory_id"] = factory_id
    after_restart["line_id"] = "line-kafka-restart"

    compose("pause", "processor")
    processor_paused = True
    try:
        accepted = await client.post("/api/v1/telemetry", json=event)
        assert accepted.status_code == 202

        compose("restart", "kafka", timeout=90)
        compose(
            "up",
            "-d",
            "--wait",
            "--wait-timeout",
            "120",
            "kafka",
            timeout=150,
        )
        compose("unpause", "processor")
        processor_paused = False

        async def projected_event(target: dict[str, object]):
            rows = await list_items(
                client,
                f"/api/v1/events?factory_id={factory_id}",
                limit=20,
            )
            if rows is None:
                return None
            return next((row for row in rows if row["event_id"] == target["event_id"]), None)

        projected = await eventually(lambda: projected_event(event), timeout=90)
        assert projected["machine_id"] == machine_id
        async with httpx.AsyncClient(base_url=client.base_url, timeout=45) as retrying_client:
            accepted_after_restart = await retrying_client.post(
                "/api/v1/telemetry",
                json=after_restart,
            )
        assert accepted_after_restart.status_code == 202
        projected_after_restart = await eventually(
            lambda: projected_event(after_restart),
            timeout=90,
        )
        assert projected_after_restart["machine_id"] == after_restart["machine_id"]

        rows = await list_items(
            client,
            f"/api/v1/events?factory_id={factory_id}",
            limit=20,
        )
        assert rows is not None
        assert len(rows) == 2
        assert {row["event_id"] for row in rows} == {
            event["event_id"],
            after_restart["event_id"],
        }
        machines = await list_items(
            client,
            f"/api/v1/machines?factory_id={factory_id}",
            limit=20,
        )
        assert machines is not None
        assert len(machines) == 2
        assert all(machine["sequence_no"] == 1 for machine in machines)
    finally:
        if processor_paused:
            compose("unpause", "processor")


@pytest.mark.asyncio
async def test_consumer_pause_for_120_seconds_drains_accepted_backlog_once(
    client: httpx.AsyncClient,
) -> None:
    factory_id = f"factory-pause-{uuid4().hex[:8]}"
    machine_prefix = f"machine-pause-{uuid4().hex[:6]}-"
    initial_total = await _event_total(client, factory_id)
    assert initial_total == 0
    compose("stop", "processor")
    events: list[dict[str, object]] = []
    try:
        loop = asyncio.get_running_loop()
        for second in range(120):
            cycle_started = loop.time()
            batch = []
            for slot in range(100):
                machine_index = (second * 100 + slot) % 1000
                event = make_event(
                    machine_id=f"{machine_prefix}{machine_index:04d}",
                    sequence_no=second // 10 + 1,
                )
                event["factory_id"] = factory_id
                event["line_id"] = f"line-{machine_index // 100 + 1:02d}"
                batch.append(event)
            responses = await asyncio.gather(
                *(post_telemetry_with_transport_retry(client, event) for event in batch)
            )
            assert all(response.status_code == 202 for response in responses)
            events.extend(batch)
            await asyncio.sleep(max(0, 1 - (loop.time() - cycle_started)))
    finally:
        compose("start", "processor")

    async def drained_total():
        total = await _event_total(client, factory_id)
        if total != initial_total + len(events):
            return None
        lag = await _processor_total_lag()
        return total if lag == 0 else None

    await eventually(drained_total, timeout=180)
    pages = await _all_factory_events(client, factory_id)
    expected_ids = {str(event["event_id"]) for event in events}
    actual_ids = {str(row["event_id"]) for row in pages}
    assert actual_ids == expected_ids
    assert len(pages) == len(events)

    machines = await list_items(
        client,
        f"/api/v1/machines?factory_id={factory_id}",
        limit=1000,
    )
    assert machines is not None
    paused_machines = [row for row in machines if str(row["machine_id"]).startswith(machine_prefix)]
    assert len(paused_machines) == 1000
    assert all(row["sequence_no"] == 12 for row in paused_machines)
    assert all(row["sequence_gap_count"] == 0 for row in paused_machines)


@pytest.mark.asyncio
async def test_postgres_60_second_outage_recovers_through_audited_dlq_replay(
    client: httpx.AsyncClient,
) -> None:
    machine_id = f"machine-db-outage-{uuid4().hex[:10]}"
    initial = make_event(machine_id=machine_id, sequence_no=1)
    assert (await client.post("/api/v1/telemetry", json=initial)).status_code == 202
    await eventually(lambda: _event_by_id(client, str(initial["event_id"])))
    machines = await list_items(
        client,
        "/api/v1/machines?factory_id=factory-e2e",
        limit=500,
    )
    assert machines is not None
    original = next(row for row in machines if row["machine_id"] == machine_id)
    original_last_seen = original["last_seen_at"]

    compose("stop", "postgres")
    outage_event = make_event(machine_id=machine_id, sequence_no=2)
    accepted = await client.post("/api/v1/telemetry", json=outage_event)
    assert accepted.status_code == 202
    await asyncio.sleep(60)
    compose("start", "postgres")
    await wait_for_postgres()

    async def audited_record():
        rows = await list_items(client, "/api/v1/dead-letters")
        if rows is None:
            return None
        return next((row for row in rows if row["event_id"] == outage_event["event_id"]), None)

    await eventually(audited_record, timeout=90)
    compose(
        "exec",
        "-T",
        "processor",
        "python",
        "-m",
        "machina_stream.replay_dlq",
        "--event-id",
        str(outage_event["event_id"]),
        timeout=90,
    )

    persisted = await eventually(
        lambda: _event_by_id(client, str(outage_event["event_id"])),
        timeout=90,
    )
    assert persisted["replayed"] is True
    same_event = await eventually(lambda: _event_count(client, str(outage_event["event_id"])))
    assert same_event == 1

    current_machines = await list_items(
        client,
        "/api/v1/machines?factory_id=factory-e2e",
        limit=500,
    )
    assert current_machines is not None
    current = next(row for row in current_machines if row["machine_id"] == machine_id)
    assert current["sequence_no"] == 2
    assert current["last_seen_at"] == original_last_seen
    assert current["online"] is False

    final_record = await eventually(audited_record)
    assert final_record["replayed_at"] is not None


@pytest.mark.asyncio
async def test_missing_dlq_topic_keeps_source_offset_uncommitted_then_recovers(
    client: httpx.AsyncClient,
) -> None:
    dlq_topic = f"machina.test.dlq.{uuid4().hex[:12]}"
    worker_name = f"{compose_project()}-processor-dlq-{uuid4().hex[:8]}"
    compose("stop", "processor")
    try:
        compose(
            "run",
            "--detach",
            "--no-deps",
            "--name",
            worker_name,
            "--env",
            f"KAFKA_DLQ_TOPIC={dlq_topic}",
            "processor",
        )
        await eventually(lambda: _processor_metrics_ready(worker_name), timeout=45)

        partition, source_offset = publish_kafka_record(
            key=f"machine-dlq-outage-{uuid4().hex[:10]}".encode(),
            value=b'{"event_id":',
        )
        await eventually(
            lambda: _dlq_publish_is_retrying(worker_name),
            timeout=45,
        )
        assert await eventually(
            lambda: _source_offset_is_uncommitted(partition, source_offset),
            timeout=30,
        )

        compose(
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--create",
            "--if-not-exists",
            "--topic",
            dlq_topic,
            "--partitions",
            "6",
            "--replication-factor",
            "1",
            timeout=30,
        )

        async def audited_record():
            rows = await list_items(client, "/api/v1/dead-letters")
            if rows is None:
                return None
            return next(
                (
                    row
                    for row in rows
                    if row["source_topic"] == "telemetry.events.v1"
                    and row["source_partition"] == partition
                    and row["source_offset"] == source_offset
                    and row["error_code"] == "invalid_json"
                ),
                None,
            )

        record = await eventually(audited_record, timeout=60)
        assert await eventually(
            lambda: _source_offset_is_committed(partition, source_offset),
            timeout=30,
        )
        assert record["event_id"] is None
    finally:
        try:
            docker("rm", "--force", worker_name, timeout=30)
        except subprocess.CalledProcessError:
            pass
        compose("start", "processor")


@pytest.mark.asyncio
async def test_db_commit_before_offset_commit_crash_replays_without_duplicate_history(
    client: httpx.AsyncClient,
) -> None:
    factory_id = f"factory-commit-window-{uuid4().hex[:10]}"
    machine_id = f"machine-commit-window-{uuid4().hex[:10]}"
    worker_name = f"{compose_project()}-processor-commit-gate-{uuid4().hex[:8]}"
    event = make_event(machine_id=machine_id, sequence_no=1)
    event["factory_id"] = factory_id
    event["line_id"] = "line-commit-window"
    event["received_at"] = datetime.now(UTC).isoformat()
    gate_script = textwrap.dedent(
        """
        import asyncio
        import os

        from aiokafka import AIOKafkaConsumer
        from aiokafka.structs import TopicPartition
        from machina_stream.worker import run_worker

        target_partition = int(os.environ["MACHINA_TEST_GATE_PARTITION"])
        target_offset = int(os.environ["MACHINA_TEST_GATE_OFFSET"])
        original_commit = AIOKafkaConsumer.commit

        async def gated_commit(self, offsets=None):
            if offsets is not None:
                target = offsets.get(TopicPartition("telemetry.events.v1", target_partition))
                if target is not None and target.offset == target_offset:
                    print("TEST_OFFSET_COMMIT_GATE_REACHED", flush=True)
                    await asyncio.Event().wait()
            return await original_commit(self, offsets)

        AIOKafkaConsumer.commit = gated_commit
        asyncio.run(run_worker())
        """
    )

    compose("stop", "processor")
    try:
        partition, source_offset = publish_kafka_record(
            key=machine_id.encode(),
            value=json.dumps(event, separators=(",", ":")).encode(),
        )
        compose(
            "run",
            "--detach",
            "--no-deps",
            "--name",
            worker_name,
            "--env",
            f"MACHINA_TEST_GATE_PARTITION={partition}",
            "--env",
            f"MACHINA_TEST_GATE_OFFSET={source_offset + 1}",
            "processor",
            "python",
            "-c",
            gate_script,
        )
        await eventually(lambda: _processor_metrics_ready(worker_name), timeout=45)

        async def persisted_event():
            rows = await list_items(
                client,
                f"/api/v1/events?factory_id={factory_id}",
                limit=20,
            )
            if rows is None:
                return None
            return next((row for row in rows if row["event_id"] == event["event_id"]), None)

        persisted = await eventually(persisted_event, timeout=45)
        assert persisted["sequence_no"] == 1
        await eventually(
            lambda: _offset_commit_gate_is_held(worker_name),
            timeout=15,
        )
        assert await eventually(
            lambda: _source_offset_is_uncommitted(partition, source_offset),
            timeout=30,
        )

        docker("kill", worker_name, timeout=15)
        docker("rm", "--force", worker_name, timeout=15)
        compose("start", "processor")
        await eventually(
            lambda: _source_offset_is_committed(partition, source_offset),
            timeout=60,
        )

        rows = await list_items(
            client,
            f"/api/v1/events?factory_id={factory_id}",
            limit=20,
        )
        assert rows is not None
        assert [row["event_id"] for row in rows] == [event["event_id"]]
        machines = await list_items(
            client,
            f"/api/v1/machines?factory_id={factory_id}",
            limit=20,
        )
        assert machines is not None
        assert len(machines) == 1
        assert machines[0]["sequence_no"] == 1
    finally:
        try:
            docker("rm", "--force", worker_name, timeout=15)
        except subprocess.CalledProcessError:
            pass
        compose("start", "processor")


async def _event_by_id(client: httpx.AsyncClient, event_id: str):
    rows = await list_items(client, "/api/v1/events", limit=500)
    if rows is None:
        return None
    return next((row for row in rows if row["event_id"] == event_id), None)


async def _machine_with_sequence(check):
    row = await check()
    return row if row is not None and int(row["sequence_no"]) > 0 else None


async def _machine_after_sequence(check, previous_sequence: int):
    row = await check()
    return row if row is not None and int(row["sequence_no"]) > previous_sequence else None


async def _event_count(client: httpx.AsyncClient, event_id: str):
    rows = await list_items(client, "/api/v1/events", limit=500)
    if rows is None:
        return None
    matches = [row for row in rows if row["event_id"] == event_id]
    return len(matches) if matches else None


async def _event_total(client: httpx.AsyncClient, factory_id: str) -> int | None:
    response = await client.get(
        "/api/v1/events",
        params={"limit": 1, "factory_id": factory_id},
    )
    if response.status_code != 200:
        return None
    return int(response.json()["total"])


async def _all_factory_events(
    client: httpx.AsyncClient, factory_id: str
) -> list[dict[str, object]]:
    response = await client.get(
        "/api/v1/events",
        params={"limit": 1, "factory_id": factory_id},
    )
    assert response.status_code == 200
    total = int(response.json()["total"])
    rows: list[dict[str, object]] = []
    for offset in range(0, total, 500):
        page = await client.get(
            "/api/v1/events",
            params={"limit": 500, "offset": offset, "factory_id": factory_id},
        )
        assert page.status_code == 200
        rows.extend(page.json()["items"])
    return rows


async def _processor_metrics_ready(container: str) -> bool | None:
    healthcheck = (
        "import urllib.request; "
        "body=urllib.request.urlopen('http://127.0.0.1:9101/metrics', timeout=2)"
        ".read().decode(); "
        "raise SystemExit(0 if any(line.startswith('machina_processor_ready ') "
        "and line.endswith('1.0') for line in body.splitlines()) else 1)"
    )
    try:
        docker(
            "exec",
            container,
            "python",
            "-c",
            healthcheck,
            timeout=5,
        )
        return True
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None


async def _processor_total_lag() -> int | None:
    script = (
        "import urllib.request; "
        "body=urllib.request.urlopen('http://127.0.0.1:9101/metrics', timeout=2)"
        ".read().decode(); "
        "print(next((line.rsplit(' ', 1)[-1] for line in body.splitlines() "
        "if line.startswith('machina_kafka_consumer_lag_records{') "
        "and 'partition=\"total\"' in line), ''))"
    )
    try:
        output = compose("exec", "-T", "processor", "python", "-c", script, timeout=5)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    value = output.strip().splitlines()[-1] if output.strip() else ""
    try:
        return int(float(value))
    except ValueError:
        return None


async def _dlq_publish_is_retrying(container: str) -> bool | None:
    try:
        logs = docker("logs", "--tail", "200", container, timeout=5)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    return (
        True
        if "DLQ broker acknowledgement unavailable; source offset remains uncommitted" in logs
        else None
    )


async def _offset_commit_gate_is_held(container: str) -> bool | None:
    try:
        logs = docker("logs", "--tail", "200", container, timeout=5)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    return True if "TEST_OFFSET_COMMIT_GATE_REACHED" in logs else None


def _consumer_group_offset(partition: int) -> int | None:
    output = compose(
        "exec",
        "-T",
        "kafka",
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server",
        "kafka:9092",
        "--group",
        "machina-processor-v1",
        "--describe",
        timeout=20,
    )
    for line in output.splitlines():
        columns = line.split()
        if len(columns) >= 3 and columns[0] == "telemetry.events.v1":
            topic_index = 0
        elif len(columns) >= 4 and columns[1] == "telemetry.events.v1":
            # Kafka 3.9 includes the consumer group in each describe row:
            # GROUP TOPIC PARTITION CURRENT-OFFSET ...
            topic_index = 1
        else:
            continue
        partition_index = topic_index + 1
        offset_index = topic_index + 2
        if columns[partition_index].isdigit() and int(columns[partition_index]) == partition:
            return None if columns[offset_index] == "-" else int(columns[offset_index])
    return None


async def _source_offset_is_uncommitted(partition: int, source_offset: int) -> bool | None:
    try:
        committed = _consumer_group_offset(partition)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    return True if committed is None or committed <= source_offset else None


async def _source_offset_is_committed(partition: int, source_offset: int) -> bool | None:
    try:
        committed = _consumer_group_offset(partition)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
        return None
    return True if committed is not None and committed > source_offset else None
