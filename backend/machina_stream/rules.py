from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class NumericRule:
    rule_id: str
    metric_name: str
    warning: float
    critical: float


def _number(name: str, default: float) -> float:
    raw = os.getenv(name)
    try:
        value = default if raw is None else float(raw)
    except ValueError as error:
        raise RuntimeError(f"{name} must be numeric") from error
    if not (value == value and abs(value) != float("inf")):
        raise RuntimeError(f"{name} must be finite")
    return value


def numeric_rules() -> tuple[str, tuple[NumericRule, ...]]:
    version = os.getenv("MACHINA_RULESET_VERSION", "demo-v1")
    rules = (
        NumericRule(
            "temperature_high",
            "temperature_c",
            _number("TEMPERATURE_WARNING_C", 80),
            _number("TEMPERATURE_CRITICAL_C", 95),
        ),
        NumericRule(
            "vibration_high",
            "vibration_rms_mm_s",
            _number("VIBRATION_WARNING_MM_S", 5),
            _number("VIBRATION_CRITICAL_MM_S", 10),
        ),
        NumericRule(
            "current_high",
            "current_a",
            _number("CURRENT_WARNING_A", 100),
            _number("CURRENT_CRITICAL_A", 200),
        ),
    )
    if not version.strip() or any(rule.warning >= rule.critical for rule in rules):
        raise RuntimeError("Alert rule version and warning/critical thresholds are invalid")
    return version, rules
