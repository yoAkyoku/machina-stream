from __future__ import annotations

import asyncio
import logging
import os
import random
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

import httpx

from machina_stream.contracts import format_utc

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("machina_stream.simulator")


@dataclass(frozen=True)
class SimulatedMachine:
    machine_id: str
    line_id: str
    random: random.Random
    initial_sequence: int = 0


def machines(count: int, sequences: dict[str, int] | None = None) -> list[SimulatedMachine]:
    known_sequences = sequences or {}
    return [
        SimulatedMachine(
            machine_id=f"machine-{index:04d}",
            line_id=f"line-{index // 100 + 1:02d}",
            random=random.Random(index),
            initial_sequence=known_sequences.get(f"machine-{index:04d}", 0),
        )
        for index in range(count)
    ]


async def load_sequences(client: httpx.AsyncClient) -> dict[str, int]:
    sequences: dict[str, int] = {}
    offset = 0
    while True:
        try:
            response = await client.get(
                "/api/v1/machines",
                params={
                    "factory_id": "factory-demo",
                    "limit": 5000,
                    "offset": offset,
                },
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as error:
            if error.response.status_code < 500:
                raise
            logger.warning("machine projection unavailable; retrying simulator startup")
            await asyncio.sleep(2)
            continue
        except httpx.HTTPError:
            logger.warning("machine projection unavailable; retrying simulator startup")
            await asyncio.sleep(2)
            continue

        page = response.json()
        total = int(page["total"])
        items = page["items"]
        sequences.update(
            {str(machine["machine_id"]): int(machine["sequence_no"]) for machine in items}
        )
        offset += len(items)
        if offset >= total or not items:
            return sequences


def create_event(machine: SimulatedMachine, sequence_no: int) -> dict[str, object]:
    rng = machine.random
    temperature = max(30.0, min(110.0, rng.gauss(67.0, 7.0)))
    vibration = max(0.0, min(12.0, rng.gauss(2.2, 0.8)))
    current = max(0.0, min(250.0, rng.gauss(55.0, 13.0)))
    if rng.random() < 0.01:
        temperature = rng.choice([82.0, 97.0])
    if rng.random() < 0.005:
        vibration = rng.choice([5.5, 10.5])
    if rng.random() < 0.005:
        current = rng.choice([110.0, 210.0])
    status = "fault" if rng.random() < 0.0005 else "running"
    return {
        "event_id": str(uuid4()),
        "schema_version": 1,
        "factory_id": "factory-demo",
        "line_id": machine.line_id,
        "machine_id": machine.machine_id,
        "sequence_no": sequence_no,
        "event_time": format_utc(datetime.now(UTC)),
        "metrics": {
            "temperature_c": round(temperature, 2),
            "vibration_rms_mm_s": round(vibration, 2),
            "current_a": round(current, 2),
        },
        "machine_status": status,
        "error_code": "SIM-FAULT" if status == "fault" else None,
    }


async def machine_loop(
    client: httpx.AsyncClient,
    machine: SimulatedMachine,
    interval: float,
) -> None:
    sequence_no = machine.initial_sequence
    await asyncio.sleep(machine.random.uniform(0, interval))
    while True:
        sequence_no += 1
        event = create_event(machine, sequence_no)
        for attempt in range(1, 4):
            try:
                response = await client.post("/api/v1/telemetry", json=event)
                if response.status_code == 202:
                    break
                if response.status_code < 500:
                    logger.warning(
                        "event rejected machine_id=%s status=%d",
                        machine.machine_id,
                        response.status_code,
                    )
                    break
            except httpx.HTTPError:
                pass
            if attempt < 3:
                await asyncio.sleep(0.25 * attempt)
        await asyncio.sleep(interval)


async def run() -> None:
    count = int(os.getenv("SIMULATOR_MACHINE_COUNT", "1000"))
    interval = float(os.getenv("SIMULATOR_EVENT_INTERVAL_SECONDS", "10"))
    if not 1 <= count <= 10000 or interval <= 0:
        raise ValueError("machine count must be 1..10000 and event interval must be positive")
    base_url = os.getenv("MACHINA_API_URL", "http://api:8000")
    logger.info("starting synthetic factory machines=%d event_interval_seconds=%s", count, interval)
    limits = httpx.Limits(max_connections=250, max_keepalive_connections=100)
    async with httpx.AsyncClient(base_url=base_url, timeout=5, limits=limits) as client:
        sequences = await load_sequences(client)
        logger.info("resuming simulator machine sequences machines=%d", len(sequences))
        await asyncio.gather(
            *(machine_loop(client, machine, interval) for machine in machines(count, sequences))
        )


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("simulator stopped")


if __name__ == "__main__":
    main()
