from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, StrictInt, StringConstraints, field_validator

NonEmptyIdentifier = Annotated[
    str,
    StringConstraints(strip_whitespace=True, min_length=1, max_length=100),
]


class Metrics(BaseModel):
    model_config = ConfigDict(extra="forbid")

    temperature_c: float = Field(ge=-40, le=300, allow_inf_nan=False)
    vibration_rms_mm_s: float = Field(ge=0, le=100, allow_inf_nan=False)
    current_a: float = Field(ge=0, le=1000, allow_inf_nan=False)

    @field_validator("temperature_c", "vibration_rms_mm_s", "current_a", mode="before")
    @classmethod
    def metrics_must_be_numeric_not_boolean(cls, value: object) -> object:
        if isinstance(value, bool):
            raise ValueError("metric values must be finite numbers")
        return value


class TelemetryEvent(BaseModel):
    model_config = ConfigDict(extra="allow")

    event_id: UUID
    schema_version: StrictInt
    factory_id: NonEmptyIdentifier
    line_id: NonEmptyIdentifier
    machine_id: NonEmptyIdentifier
    sequence_no: StrictInt = Field(gt=0)
    event_time: datetime
    metrics: Metrics
    machine_status: Literal["running", "idle", "stopped", "fault"] | None = None
    error_code: Annotated[str, StringConstraints(max_length=100)] | None = None

    @field_validator("event_id")
    @classmethod
    def event_id_must_be_uuid_v4(cls, value: UUID) -> UUID:
        if value.version != 4:
            raise ValueError("event_id must be a UUIDv4")
        return value

    @field_validator("schema_version")
    @classmethod
    def only_v1_is_supported(cls, value: int) -> int:
        if value != 1:
            raise ValueError("schema_version 1 is the only supported major version")
        return value

    @field_validator("event_time", mode="before")
    @classmethod
    def event_time_must_include_timezone(cls, value: object) -> object:
        if (
            not isinstance(value, str)
            or re.fullmatch(
                r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})",
                value,
            )
            is None
        ):
            raise ValueError("event_time must be an RFC 3339 string with a timezone")
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError("event_time must be an RFC 3339 string with a timezone") from error
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("event_time must include a timezone")
        return parsed.astimezone(UTC)


def format_utc(value: datetime) -> str:
    normalized = value.astimezone(UTC)
    return (
        normalized.replace(tzinfo=None).isoformat(timespec="microseconds").rstrip("0").rstrip(".")
        + "Z"
    )
