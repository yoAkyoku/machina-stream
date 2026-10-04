from __future__ import annotations

import json
import os

from aiokafka import AIOKafkaProducer


class KafkaTelemetryPublisher:
    def __init__(self) -> None:
        self.bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        self.topic = os.getenv("KAFKA_TELEMETRY_TOPIC", "telemetry.events.v1")
        self._producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap_servers,
            acks="all",
            enable_idempotence=True,
        )
        await self._producer.start()

    async def stop(self) -> None:
        if self._producer is not None:
            await self._producer.stop()
            self._producer = None

    async def publish(self, *, key: bytes, value: bytes) -> None:
        if self._producer is None:
            raise RuntimeError("Kafka producer is not ready")
        await self._producer.send_and_wait(self.topic, key=key, value=value)


def encode_event(event: dict[str, object]) -> bytes:
    return json.dumps(event, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode(
        "utf-8"
    )
