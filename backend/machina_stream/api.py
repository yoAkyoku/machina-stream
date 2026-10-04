from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
from contextlib import asynccontextmanager, suppress
from datetime import UTC, datetime, timedelta
from time import perf_counter
from typing import Protocol

import asyncpg
from fastapi import FastAPI, Query, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest

from machina_stream.contracts import TelemetryEvent, format_utc
from machina_stream.kafka import KafkaTelemetryPublisher, encode_event
from machina_stream.metrics import POSTGRES_UP, observe_postgres_health
from machina_stream.store import ProjectionStore


class TelemetryPublisher(Protocol):
    async def publish(self, *, key: bytes, value: bytes) -> None: ...


Clock = Callable[[], datetime]

INGEST_ACCEPTED = Counter("machina_ingest_accepted_total", "Events acknowledged by Kafka")
INGEST_REJECTED = Counter(
    "machina_ingest_rejected_total", "Events rejected before acceptance", ["reason"]
)
INGEST_BROKER_ERRORS = Counter(
    "machina_ingest_broker_errors_total", "Events not acknowledged because Kafka failed"
)
HTTP_REQUESTS = Counter(
    "machina_http_requests_total",
    "HTTP requests by route and status",
    ["method", "route", "status"],
)
HTTP_LATENCY = Histogram(
    "machina_http_request_duration_seconds",
    "HTTP request latency by route",
    ["method", "route"],
    buckets=(0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1, 2, 5),
)
MAX_REQUEST_BYTES = 256 * 1024


def utc_now() -> datetime:
    return datetime.now(UTC)


def create_app(
    *,
    publisher: TelemetryPublisher | None = None,
    store: ProjectionStore | None = None,
    clock: Clock = utc_now,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        active_publisher = publisher or KafkaTelemetryPublisher()
        app.state.publisher = active_publisher
        app.state.store = store
        app.state.ready = False
        POSTGRES_UP.labels(service="api").set(0)
        health_task: asyncio.Task[None] | None = None
        try:
            starter = getattr(active_publisher, "start", None)
            if starter is not None:
                await starter()
            active_store = store or await ProjectionStore.connect()
            app.state.store = active_store
            app.state.ready = True
            if callable(getattr(active_store, "healthy", None)):
                health_task = asyncio.create_task(
                    observe_postgres_health(active_store, service="api")
                )
            yield
        finally:
            app.state.ready = False
            if health_task is not None:
                health_task.cancel()
                with suppress(asyncio.CancelledError):
                    await health_task
            stopper = getattr(active_publisher, "stop", None)
            if stopper is not None:
                await stopper()
            if store is None and app.state.store is not None:
                await app.state.store.close()

    app = FastAPI(
        title="Machina Stream API",
        version="0.1.0",
        description="Synthetic factory telemetry ingress and read-only monitoring API.",
        lifespan=lifespan,
    )
    allowed_origins = [
        origin.strip()
        for origin in os.getenv(
            "MACHINA_ALLOWED_ORIGINS", "http://localhost:3001,http://127.0.0.1:3001"
        ).split(",")
        if origin.strip()
    ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_credentials=False,
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type"],
    )
    app.state.publisher = publisher
    app.state.store = store
    app.state.ready = publisher is not None

    @app.middleware("http")
    async def record_request_metrics(request: Request, call_next):
        started = perf_counter()
        status = "500"
        try:
            response = await call_next(request)
            status = str(response.status_code)
            return response
        finally:
            route = request.scope.get("route")
            route_name = getattr(route, "path", request.url.path)
            HTTP_REQUESTS.labels(request.method, route_name, status).inc()
            HTTP_LATENCY.labels(request.method, route_name).observe(perf_counter() - started)

    @app.middleware("http")
    async def limit_request_body(request: Request, call_next):
        content_length = request.headers.get("content-length")
        if content_length is not None:
            try:
                declared_length = int(content_length)
            except ValueError:
                return JSONResponse(
                    status_code=400,
                    content={"error": {"code": "invalid_content_length"}},
                )
            if declared_length < 0:
                return JSONResponse(
                    status_code=400,
                    content={"error": {"code": "invalid_content_length"}},
                )
            if declared_length > MAX_REQUEST_BYTES:
                return JSONResponse(
                    status_code=413,
                    content={"error": {"code": "request_body_too_large"}},
                )

        body = bytearray()
        async for chunk in request.stream():
            if len(body) + len(chunk) > MAX_REQUEST_BYTES:
                return JSONResponse(
                    status_code=413,
                    content={"error": {"code": "request_body_too_large"}},
                )
            body.extend(chunk)
        request._body = bytes(body)
        original_receive = request._receive
        replayed = False

        async def receive_body_once():
            nonlocal replayed
            if not replayed:
                replayed = True
                return {"type": "http.request", "body": request._body, "more_body": False}
            return await original_receive()

        request._receive = receive_body_once
        return await call_next(request)

    @app.exception_handler(RequestValidationError)
    async def request_validation_error(request: Request, exc: RequestValidationError):
        errors = exc.errors()
        if any(error.get("type") == "json_invalid" for error in errors):
            INGEST_REJECTED.labels(reason="invalid_json").inc()
            return JSONResponse(
                status_code=400,
                content={
                    "error": {
                        "code": "invalid_json",
                        "field": "body",
                        "message": "Request body must be valid JSON.",
                    }
                },
            )

        issues = [
            {
                "field": ".".join(str(part) for part in error.get("loc", ()) if part != "body"),
                "code": str(error.get("type", "invalid_value")),
                "message": str(error.get("msg", "Invalid value.")),
            }
            for error in errors
        ]
        INGEST_REJECTED.labels(reason="schema_validation").inc()
        return JSONResponse(
            status_code=422,
            content={"error": {"code": "schema_validation_failed", "issues": issues}},
        )

    @app.get("/health/live", include_in_schema=False)
    async def live() -> dict[str, str]:
        return {"status": "live"}

    @app.get("/health/ready", include_in_schema=False)
    async def ready(request: Request):
        if not request.app.state.ready:
            return JSONResponse(status_code=503, content={"status": "not_ready"})
        return {"status": "ready"}

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    def get_store(request: Request) -> ProjectionStore | None:
        return request.app.state.store

    @app.get("/api/v1/machines")
    async def machines(
        request: Request,
        limit: int = Query(default=1000, ge=1, le=5000),
        offset: int = Query(default=0, ge=0, le=1_000_000),
        factory_id: str | None = Query(default=None, min_length=1, max_length=100),
    ) -> JSONResponse:
        projection_store = get_store(request)
        if projection_store is None:
            return JSONResponse(
                status_code=503,
                content={"error": {"code": "read_store_unavailable"}},
            )
        try:
            items, total = await projection_store.list_machines(
                now=clock().astimezone(UTC),
                offline_after=timedelta(
                    seconds=3 * float(os.getenv("SIMULATOR_EVENT_INTERVAL_SECONDS", "10"))
                ),
                limit=limit,
                offset=offset,
                factory_id=factory_id,
            )
            return JSONResponse(
                content={"items": items, "total": total, "offset": offset, "limit": limit}
            )
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError):
            return JSONResponse(
                status_code=503,
                content={"error": {"code": "read_store_unavailable"}},
            )

    @app.get("/api/v1/events")
    async def events(
        request: Request,
        limit: int = Query(default=100, ge=1, le=500),
        offset: int = Query(default=0, ge=0, le=1_000_000),
        machine_id: str | None = Query(default=None, min_length=1, max_length=100),
        factory_id: str | None = Query(default=None, min_length=1, max_length=100),
    ) -> JSONResponse:
        projection_store = get_store(request)
        if projection_store is None:
            return JSONResponse(
                status_code=503,
                content={"error": {"code": "read_store_unavailable"}},
            )
        try:
            items, total = await projection_store.list_events(
                limit=limit,
                offset=offset,
                machine_id=machine_id,
                factory_id=factory_id,
            )
            return JSONResponse(
                content={"items": items, "total": total, "offset": offset, "limit": limit}
            )
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError):
            return JSONResponse(
                status_code=503,
                content={"error": {"code": "read_store_unavailable"}},
            )

    @app.get("/api/v1/alerts")
    async def alerts(
        request: Request,
        active_only: bool = True,
        limit: int = Query(default=100, ge=1, le=500),
    ) -> JSONResponse:
        projection_store = get_store(request)
        if projection_store is None:
            return JSONResponse(
                status_code=503,
                content={"error": {"code": "read_store_unavailable"}},
            )
        try:
            items = await projection_store.list_alerts(active_only=active_only, limit=limit)
            return JSONResponse(content={"items": items, "total": len(items)})
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError):
            return JSONResponse(
                status_code=503,
                content={"error": {"code": "read_store_unavailable"}},
            )

    @app.get("/api/v1/dead-letters")
    async def dead_letters(
        request: Request,
        limit: int = Query(default=100, ge=1, le=500),
    ) -> JSONResponse:
        projection_store = get_store(request)
        if projection_store is None:
            return JSONResponse(
                status_code=503,
                content={"error": {"code": "read_store_unavailable"}},
            )
        try:
            items = await projection_store.list_dead_letters(limit=limit)
            return JSONResponse(content={"items": items, "total": len(items)})
        except (asyncpg.PostgresError, asyncpg.InterfaceError, OSError, TimeoutError):
            return JSONResponse(
                status_code=503,
                content={"error": {"code": "read_store_unavailable"}},
            )

    @app.post("/api/v1/telemetry", status_code=202)
    async def ingest(request: Request, event: TelemetryEvent) -> JSONResponse:
        accepted_at = clock().astimezone(UTC)
        if event.event_time > accepted_at + timedelta(minutes=5):
            INGEST_REJECTED.labels(reason="future_event_time").inc()
            return JSONResponse(
                status_code=422,
                content={
                    "error": {
                        "code": "schema_validation_failed",
                        "issues": [
                            {
                                "field": "event_time",
                                "code": "future_timestamp",
                                "message": "event_time must not be more than five minutes ahead.",
                            }
                        ],
                    }
                },
            )

        payload = event.model_dump(mode="json")
        payload["event_time"] = format_utc(event.event_time)
        payload["received_at"] = format_utc(accepted_at)
        try:
            await request.app.state.publisher.publish(
                key=event.machine_id.encode("utf-8"),
                value=encode_event(payload),
            )
        except Exception:
            INGEST_BROKER_ERRORS.inc()
            return JSONResponse(
                status_code=503,
                headers={"Retry-After": "1"},
                content={"error": {"code": "broker_unavailable", "message": "Event not accepted."}},
            )

        INGEST_ACCEPTED.inc()
        return JSONResponse(
            status_code=202,
            content={"event_id": str(event.event_id), "accepted_at": format_utc(accepted_at)},
        )

    return app


app = create_app()
