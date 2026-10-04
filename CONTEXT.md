# Machina Stream

Machina Stream represents telemetry and alert episodes for a single synthetic factory. This glossary keeps factory-facing terms consistent across the API, processor, tests, and operator view.

## Factory model

**Factory**:
A synthetic site containing production lines or areas and their machines.
_Avoid_: Plant, tenant

**Machine**:
A monitored asset within a factory line or area; v1 receives simulated readings and never sends control commands.
_Avoid_: PLC, device (unless referring to a physical protocol endpoint)

**Telemetry event**:
One versioned measurement record for a machine, identified by a stable `event_id` and ordered for that machine by `sequence_no`.
_Avoid_: Reading (when referring to the complete event envelope)

**Accepted event**:
A valid telemetry event acknowledged by the ingress after the event broker confirms it; acceptance is not the same as database projection completion.
_Avoid_: Persisted event, processed event

**Event history**:
The immutable record of accepted telemetry events, including late, duplicate-sequence, and out-of-order events that pass event identity rules.
_Avoid_: Current state

**Machine projection**:
The latest accepted machine view derived only from events whose `sequence_no` is greater than the current projection sequence.
_Avoid_: Event history, snapshot (unless describing a point-in-time export)

## Alert model

**Alert rule**:
A versioned condition evaluated against a machine's current projection, including a demo threshold or a machine-fault condition.
_Avoid_: Alarm (unless quoting an external system)

**Alert episode**:
One period during which a machine and rule are actively in alert; repeated triggers update that episode, while a later trigger after resolution creates a new episode.
_Avoid_: Notification, alert row (when speaking about user-visible meaning)

**Active alert**:
An alert episode in `OPEN` state, deduplicated by `machine_id` and `rule_id`.
_Avoid_: Acknowledged alert

**Resolved alert**:
An alert episode that has met its recovery condition and moved from `OPEN` to `RESOLVED`; v1 does not provide manual acknowledgement or resolution.
_Avoid_: Cleared by operator

**Online machine**:
A machine with a sufficiently fresh, newly accepted event according to the configured simulator interval; producer event time alone does not establish freshness.
_Avoid_: Connected machine (unless describing transport connectivity)
