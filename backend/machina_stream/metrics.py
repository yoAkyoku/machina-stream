from __future__ import annotations

import asyncio
import os
from typing import Protocol

from prometheus_client import Gauge


class HealthStore(Protocol):
    async def healthy(self) -> bool: ...


POSTGRES_UP = Gauge(
    "machina_postgres_up",
    "PostgreSQL connectivity observed by the service",
    ["service"],
)


def postgres_health_interval_seconds() -> float:
    try:
        configured = float(os.getenv("MACHINA_DB_HEALTH_INTERVAL_SECONDS", "15"))
    except ValueError:
        return 15.0
    return max(0.1, min(configured, 300.0))


async def observe_postgres_health(store: HealthStore, *, service: str) -> None:
    interval = postgres_health_interval_seconds()
    metric = POSTGRES_UP.labels(service=service)
    while True:
        try:
            healthy = await store.healthy()
        except Exception:
            healthy = False
        metric.set(1 if healthy else 0)
        await asyncio.sleep(interval)
