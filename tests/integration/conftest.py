from __future__ import annotations

import asyncio
import base64
import json
import os
import subprocess
import time
from collections.abc import Awaitable, Callable
from uuid import uuid4

import httpx
import pytest


@pytest.fixture
def api_url() -> str:
    return os.getenv("MACHINA_API_URL", "http://127.0.0.1:18000")


@pytest.fixture
async def client(api_url: str):
    limits = httpx.Limits(max_connections=250, max_keepalive_connections=100)
    async with httpx.AsyncClient(base_url=api_url, timeout=10, limits=limits) as session:
        yield session


def make_event(
    *,
    machine_id: str | None = None,
    sequence_no: int = 1,
    temperature_c: float = 70.0,
    event_time: str | None = None,
    event_id: str | None = None,
) -> dict[str, object]:
    event_time = event_time or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return {
        "event_id": event_id or str(uuid4()),
        "schema_version": 1,
        "factory_id": "factory-e2e",
        "line_id": "line-e2e",
        "machine_id": machine_id or f"machine-e2e-{uuid4().hex[:12]}",
        "sequence_no": sequence_no,
        "event_time": event_time,
        "metrics": {
            "temperature_c": temperature_c,
            "vibration_rms_mm_s": 2.0,
            "current_a": 20.0,
        },
        "machine_status": "running",
        "error_code": None,
    }


async def eventually[T](
    check: Callable[[], Awaitable[T | None]],
    *,
    timeout: float = 30,
    interval: float = 0.25,
) -> T:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        value = await check()
        if value is not None:
            return value
        await asyncio.sleep(interval)
    raise AssertionError(f"condition was not met within {timeout} seconds")


async def list_items(
    client: httpx.AsyncClient, path: str, *, limit: int = 500
) -> list[dict[str, object]] | None:
    url = httpx.URL(path)
    query = dict(url.params)
    query["limit"] = str(limit)
    response = await client.get(url.path, params=query)
    if response.status_code != 200:
        return None
    return response.json()["items"]


async def post_telemetry_with_transport_retry(
    client: httpx.AsyncClient,
    event: dict[str, object],
    *,
    attempts: int = 4,
) -> httpx.Response:
    """Retry only a client transport break; the event id keeps the write idempotent."""
    for attempt in range(attempts):
        try:
            return await client.post("/api/v1/telemetry", json=event)
        except httpx.TransportError:
            if attempt == attempts - 1:
                raise
            await asyncio.sleep(0.05 * (2**attempt))
    raise AssertionError("unreachable")


def compose_project() -> str:
    project = os.getenv("MACHINA_E2E_PROJECT", "")
    if not project.startswith(("machina-stream-e2e-", "machina-stream-ci-")):
        raise RuntimeError(
            "MACHINA_E2E_PROJECT must be a dedicated machina-stream-e2e-* or "
            "machina-stream-ci-* Compose project"
        )
    return project


def compose(*args: str, timeout: int = 90) -> str:
    result = subprocess.run(
        ["docker", "compose", "-p", compose_project(), *args],
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return result.stdout


def publish_kafka_record(*, key: bytes, value: bytes) -> tuple[int, int]:
    key_base64 = base64.b64encode(key).decode("ascii")
    value_base64 = base64.b64encode(value).decode("ascii")
    script = f"""
import asyncio
import base64
import json
import os
from aiokafka import AIOKafkaProducer

async def main():
    producer = AIOKafkaProducer(
        bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        acks="all",
        enable_idempotence=True,
    )
    await producer.start()
    try:
        metadata = await producer.send_and_wait(
            os.environ.get("KAFKA_TELEMETRY_TOPIC", "telemetry.events.v1"),
            key=base64.b64decode("{key_base64}"),
            value=base64.b64decode("{value_base64}"),
        )
        produced = json.dumps({{"partition": metadata.partition, "offset": metadata.offset}})
        print(produced, flush=True)
    finally:
        await producer.stop()

asyncio.run(main())
"""
    output = compose("exec", "-T", "api", "python", "-c", script, timeout=30)
    metadata = json.loads(output.strip().splitlines()[-1])
    return int(metadata["partition"]), int(metadata["offset"])


def docker(*args: str, timeout: int = 30) -> str:
    result = subprocess.run(
        ["docker", *args],
        check=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    return result.stdout + result.stderr


async def wait_for_postgres() -> None:
    deadline = time.monotonic() + 90
    while time.monotonic() < deadline:
        try:
            compose(
                "exec",
                "-T",
                "postgres",
                "pg_isready",
                "-U",
                "machina",
                "-d",
                "machina",
                timeout=10,
            )
            return
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
            await asyncio.sleep(2)
    raise AssertionError("PostgreSQL did not become healthy after restart")
