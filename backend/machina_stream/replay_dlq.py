from __future__ import annotations

import argparse
import asyncio
import base64
import json
from uuid import UUID

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import TopicPartition

from machina_stream.store import (
    MAX_KAFKA_OFFSET,
    MAX_KAFKA_PARTITION,
    PermanentMessageError,
    ProjectionStore,
)
from machina_stream.worker import BOOTSTRAP_SERVERS, DLQ_TOPIC, TELEMETRY_TOPIC


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manually replay selected, repaired telemetry from the Kafka DLQ."
    )
    selection = parser.add_mutually_exclusive_group(required=True)
    selection.add_argument("--event-id", type=UUID)
    selection.add_argument("--source", help="Original source coordinates: topic:partition:offset")
    selection.add_argument(
        "--all", action="store_true", help="Explicitly replay every unreplayed DLQ record"
    )
    return parser.parse_args()


def _record_source(record: dict[str, object]) -> tuple[str, int, int]:
    source = record.get("source")
    if not isinstance(source, dict):
        raise ValueError("DLQ entry does not have valid source coordinates")
    try:
        topic = source["topic"]
        partition = source["partition"]
        offset = source["offset"]
    except KeyError as error:
        raise ValueError("DLQ entry does not have valid source coordinates") from error
    if (
        not isinstance(topic, str)
        or not topic
        or isinstance(partition, bool)
        or not isinstance(partition, int)
        or isinstance(offset, bool)
        or not isinstance(offset, int)
    ):
        raise ValueError("DLQ entry does not have valid source coordinates")
    if (
        topic != TELEMETRY_TOPIC
        or partition < 0
        or offset < 0
        or partition > MAX_KAFKA_PARTITION
        or offset > MAX_KAFKA_OFFSET
    ):
        raise ValueError("DLQ entry source does not match the configured telemetry topic")
    return topic, partition, offset


def _decode_original_payload(record: dict[str, object]) -> tuple[bytes, bytes]:
    try:
        key = base64.b64decode(str(record["original_key_base64"]), validate=True)
        value = base64.b64decode(str(record["original_payload_base64"]), validate=True)
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("DLQ entry does not contain valid original payload bytes") from error
    return key, value


async def replay(arguments: argparse.Namespace) -> int:
    selected_source: tuple[str, int, int] | None = None
    if arguments.source:
        try:
            topic, partition, offset = arguments.source.rsplit(":", 2)
            selected_source = (topic, int(partition), int(offset))
        except ValueError as error:
            raise ValueError("--source must be topic:partition:offset") from error
        if (
            selected_source[0] != TELEMETRY_TOPIC
            or selected_source[1] < 0
            or selected_source[2] < 0
            or selected_source[1] > MAX_KAFKA_PARTITION
            or selected_source[2] > MAX_KAFKA_OFFSET
        ):
            raise ValueError("--source must refer to valid telemetry source coordinates")

    consumer = AIOKafkaConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks="all",
        enable_idempotence=True,
    )
    store = await ProjectionStore.connect()
    replayed_count = 0
    matched_count = 0
    await consumer.start()
    await producer.start()
    try:
        # `partitions_for_topic` is a synchronous cache lookup in aiokafka;
        # explicitly wait for this topic's metadata because this consumer
        # starts without a subscription and therefore has no topic metadata.
        await consumer._client._wait_on_metadata(DLQ_TOPIC)  # noqa: SLF001
        partitions = consumer.partitions_for_topic(DLQ_TOPIC)
        if not partitions:
            return 0
        topic_partitions = {TopicPartition(DLQ_TOPIC, partition) for partition in partitions}
        consumer.assign(topic_partitions)
        beginnings = await consumer.beginning_offsets(topic_partitions)
        endings = await consumer.end_offsets(topic_partitions)
        for partition in topic_partitions:
            consumer.seek(partition, beginnings[partition])
        complete = {
            partition
            for partition in topic_partitions
            if beginnings[partition] >= endings[partition]
        }

        while complete != topic_partitions:
            batch = await consumer.getmany(timeout_ms=1000, max_records=100)
            for partition, messages in batch.items():
                for message in messages:
                    if message.offset >= endings[partition]:
                        complete.add(partition)
                        continue
                    try:
                        record = json.loads((message.value or b"").decode("utf-8"))
                        if not isinstance(record, dict):
                            raise ValueError("DLQ record must be a JSON object")
                        source = _record_source(record)
                    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                        continue

                    matches = (
                        arguments.all
                        or (
                            arguments.event_id is not None
                            and str(record.get("event_id")) == str(arguments.event_id)
                        )
                        or (selected_source is not None and source == selected_source)
                    )
                    if matches:
                        matched_count += 1
                        try:
                            await store.record_dead_letter(record)
                            if await store.dead_letter_was_replayed(source):
                                continue
                            key, value = _decode_original_payload(record)
                        except (PermanentMessageError, ValueError):
                            continue
                        source_header = f"{source[0]}:{source[1]}:{source[2]}".encode()
                        await producer.send_and_wait(
                            source[0],
                            key=key,
                            value=value,
                            headers=[
                                ("machina-replay", b"true"),
                                ("machina-replay-source", source_header),
                            ],
                        )
                        replayed_count += 1
                    if message.offset + 1 >= endings[partition]:
                        complete.add(partition)
            # An empty poll is normal while a bounded end offset is being reached.

        print(f"matched={matched_count} replayed={replayed_count}")
        return replayed_count
    finally:
        await producer.stop()
        await consumer.stop()
        await store.close()


def main() -> None:
    arguments = _arguments()
    replayed_count = asyncio.run(replay(arguments))
    if replayed_count == 0:
        print("No matching unreplayed dead-letter records were found.")


if __name__ == "__main__":
    main()
