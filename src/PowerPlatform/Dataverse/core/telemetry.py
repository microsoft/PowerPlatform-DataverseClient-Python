# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Telemetry infrastructure for the Dataverse SDK.

Provides OpenTelemetry-based tracing, metrics, and logging with an
extensible hook system for custom telemetry providers.

All telemetry is opt-in: when :class:`TelemetryConfig` is not provided
(or all signals are disabled), a zero-overhead :class:`NoOpTelemetryManager`
is used.
"""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import (
    Any,
    Dict,
    Generator,
    List,
    Optional,
    Union,
)

from ..common.constants import (
    OTEL_ATTR_DB_SYSTEM,
    OTEL_ATTR_DB_OPERATION,
    OTEL_ATTR_HTTP_METHOD,
    OTEL_ATTR_HTTP_URL,
    OTEL_ATTR_HTTP_STATUS_CODE,
    OTEL_ATTR_DATAVERSE_TABLE,
    OTEL_ATTR_DATAVERSE_REQUEST_ID,
    OTEL_ATTR_DATAVERSE_CORRELATION_ID,
    OTEL_ATTR_DATAVERSE_SERVICE_REQUEST_ID,
)

# Optional OpenTelemetry imports -- graceful degradation when not installed
try:
    from opentelemetry import trace, metrics
    from opentelemetry.trace import Status, StatusCode

    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False
    trace = None  # type: ignore[assignment]
    metrics = None  # type: ignore[assignment]
    Status = None  # type: ignore[assignment]
    StatusCode = None  # type: ignore[assignment]


# ============================================================================
# Configuration
# ============================================================================


@dataclass(frozen=True)
class TelemetryConfig:
    """Configuration for SDK telemetry and observability.

    Telemetry is opt-in.  When enabled the SDK produces
    OpenTelemetry-compatible traces, metrics, and Python log records
    that can be exported to any OTel-compatible backend.

    Example:
        Basic tracing::

            config = DataverseConfig(
                telemetry=TelemetryConfig(enable_tracing=True)
            )

        Custom hook::

            config = DataverseConfig(
                telemetry=TelemetryConfig(hooks=[MyHook()])
            )
    """

    # Signal toggles
    enable_tracing: bool = False
    enable_metrics: bool = False
    enable_logging: bool = False

    # Service identification
    service_name: Optional[str] = None
    service_version: Optional[str] = None

    # Logging configuration
    log_level: str = "WARNING"
    logger_name: str = "PowerPlatform.Dataverse"

    # Custom hooks
    hooks: List["TelemetryHook"] = field(default_factory=list)

    # Advanced options (privacy-sensitive, disabled by default)
    capture_request_body: bool = False
    capture_response_body: bool = False
    max_body_capture_size: int = 1024


# ============================================================================
# Context Objects
# ============================================================================


@dataclass
class RequestContext:
    """Context passed to telemetry hooks for each HTTP request.

    Hook authors should treat all fields as read-only.  The
    ``custom_data`` dict can be used to share state between hooks
    within the same request lifecycle.
    """

    # Identifiers
    client_request_id: str
    correlation_id: str

    # Request details
    method: str  # GET, POST, PATCH, DELETE
    url: str
    operation: str  # e.g. "records.create", "query.sql"
    table_name: Optional[str] = None

    # Timing
    start_time: float = field(default_factory=time.perf_counter)

    # Custom data bag for hooks to share state
    custom_data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ResponseContext:
    """Response information passed to telemetry hooks."""

    status_code: int
    duration_ms: float
    service_request_id: Optional[str] = None

    # Response details
    response_size: Optional[int] = None

    # Error information
    error: Optional[Exception] = None
    retry_count: int = 0


# ============================================================================
# Internal wrapper -- keeps OTel span off the user-facing RequestContext
# ============================================================================


@dataclass
class _TrackedRequest:
    """Internal: pairs a user-facing RequestContext with an OTel span."""

    context: RequestContext
    _span: Any = field(default=None, repr=False)


# ============================================================================
# Hook Base Class
# ============================================================================


class TelemetryHook:
    """Base class for custom telemetry hooks.

    Override any subset of methods to receive telemetry events.
    All methods have default no-op implementations, so you only need to
    implement the callbacks you care about.

    Duck-typing is also supported: any object with the appropriate
    method signatures will work even without subclassing.

    Example::

        class MetricsHook(TelemetryHook):
            def on_request_end(self, request, response):
                print(f"{request.operation} -> {response.status_code} in {response.duration_ms:.0f}ms")
    """

    def on_request_start(self, context: RequestContext) -> None:
        """Called before each HTTP request is sent."""

    def on_request_end(self, request: RequestContext, response: ResponseContext) -> None:
        """Called after each HTTP request completes (success or failure)."""

    def on_request_error(self, request: RequestContext, error: Exception) -> None:
        """Called when an error occurs during an HTTP request.

        This includes both network/transport exceptions and HTTP-level
        failures (e.g. 4xx/5xx responses surfaced as ``HttpError``).
        Always fired before ``on_request_end``.
        """

    def get_additional_headers(self) -> Dict[str, str]:
        """Return additional headers to include in outbound requests."""
        return {}


# ============================================================================
# Telemetry Manager
# ============================================================================


class TelemetryManager:
    """Manages telemetry instrumentation for the Dataverse SDK.

    Internal -- not part of the public API.
    """

    def __init__(self, config: TelemetryConfig) -> None:
        self._config = config
        self._tracer: Any = None
        self._meter: Any = None
        self._logger: Optional[logging.Logger] = None
        self._log_level: int = logging.WARNING
        self._hooks = list(config.hooks)

        # Metric instruments (populated in _initialize)
        self._request_duration: Any = None
        self._request_count: Any = None
        self._error_count: Any = None
        self._retry_count: Any = None

        self._initialize()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def is_tracing_enabled(self) -> bool:
        return self._config.enable_tracing and _OTEL_AVAILABLE

    @property
    def is_metrics_enabled(self) -> bool:
        return self._config.enable_metrics and _OTEL_AVAILABLE

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    def _initialize(self) -> None:
        if self._config.enable_tracing and _OTEL_AVAILABLE:
            self._tracer = trace.get_tracer(
                "PowerPlatform.Dataverse",
                schema_url="https://opentelemetry.io/schemas/1.21.0",
            )

        if self._config.enable_metrics and _OTEL_AVAILABLE:
            self._meter = metrics.get_meter(
                "PowerPlatform.Dataverse",
                schema_url="https://opentelemetry.io/schemas/1.21.0",
            )
            self._setup_metrics()

        if self._config.enable_logging:
            self._logger = logging.getLogger(self._config.logger_name)
            self._log_level = getattr(logging, self._config.log_level.upper(), logging.WARNING)

    def _setup_metrics(self) -> None:
        if not self._meter:
            return
        self._request_duration = self._meter.create_histogram(
            name="dataverse.client.request.duration",
            description="Duration of Dataverse API requests",
            unit="ms",
        )
        self._request_count = self._meter.create_counter(
            name="dataverse.client.request.count",
            description="Number of Dataverse API requests",
            unit="1",
        )
        self._error_count = self._meter.create_counter(
            name="dataverse.client.error.count",
            description="Number of Dataverse API errors",
            unit="1",
        )
        self._retry_count = self._meter.create_counter(
            name="dataverse.client.retry.count",
            description="Number of request retries",
            unit="1",
        )

    # ------------------------------------------------------------------
    # trace_request  --  context manager yielding _TrackedRequest
    # ------------------------------------------------------------------

    @contextmanager
    def trace_request(
        self,
        operation: str,
        method: str,
        url: str,
        client_request_id: str,
        correlation_id: str,
        table_name: Optional[str] = None,
    ) -> Generator[_TrackedRequest, None, None]:
        """Create a traced request context.

        Yields a ``_TrackedRequest`` that pairs a user-visible
        :class:`RequestContext` with an internal OTel span.  The span
        is ended in the ``finally`` block -- no ``except`` block is
        present so that ``record_response`` is the single path for
        error handling (avoiding double dispatch to hooks).
        """
        ctx = RequestContext(
            client_request_id=client_request_id,
            correlation_id=correlation_id,
            method=method,
            url=url,
            operation=operation,
            table_name=table_name,
        )

        # Dispatch on_request_start to hooks
        self._dispatch_request_start(ctx)

        # Create span if tracing enabled
        span = None
        if self._tracer:
            span_name = f"Dataverse {operation}"
            if table_name:
                span_name = f"{span_name} [{table_name}]"

            span = self._tracer.start_span(
                span_name,
                kind=trace.SpanKind.CLIENT,
                attributes={
                    OTEL_ATTR_DB_SYSTEM: "dataverse",
                    OTEL_ATTR_DB_OPERATION: operation,
                    OTEL_ATTR_HTTP_METHOD: method,
                    OTEL_ATTR_HTTP_URL: url,
                    OTEL_ATTR_DATAVERSE_REQUEST_ID: client_request_id,
                    OTEL_ATTR_DATAVERSE_CORRELATION_ID: correlation_id,
                    **({OTEL_ATTR_DATAVERSE_TABLE: table_name} if table_name else {}),
                },
            )

        tracked = _TrackedRequest(context=ctx, _span=span)
        try:
            yield tracked
        finally:
            if span:
                try:
                    span.end()
                except Exception:
                    pass

    # ------------------------------------------------------------------
    # record_response  --  sets span status, records metrics/logs/hooks
    # ------------------------------------------------------------------

    def record_response(
        self,
        tracked: Any,  # _TrackedRequest or None (from NoOp)
        status_code: int,
        service_request_id: Optional[str] = None,
        error: Optional[Exception] = None,
        retry_count: int = 0,
    ) -> None:
        """Record response telemetry.

        Each subsystem (span, metrics, logging, hooks) is wrapped in
        its own ``try/except`` to guarantee that telemetry never breaks
        the request pipeline.
        """
        if tracked is None:
            return

        ctx: RequestContext = tracked.context
        duration_ms = (time.perf_counter() - ctx.start_time) * 1000

        response = ResponseContext(
            status_code=status_code,
            duration_ms=duration_ms,
            service_request_id=service_request_id,
            error=error,
            retry_count=retry_count,
        )

        # 1. Span attributes and status
        span = tracked._span
        if span:
            try:
                span.set_attribute(OTEL_ATTR_HTTP_STATUS_CODE, status_code)
                if service_request_id:
                    span.set_attribute(
                        OTEL_ATTR_DATAVERSE_SERVICE_REQUEST_ID,
                        service_request_id,
                    )
                if error is not None or status_code >= 400:
                    span.set_status(Status(StatusCode.ERROR, str(error) if error else f"HTTP {status_code}"))
                    if error is not None:
                        span.record_exception(error)
                else:
                    span.set_status(Status(StatusCode.OK))
            except Exception:
                pass

        # 2. Metrics
        if self._request_duration:
            try:
                attrs = {
                    "operation": ctx.operation,
                    "method": ctx.method,
                    "status_code": status_code,
                }
                if ctx.table_name:
                    attrs["table"] = ctx.table_name

                self._request_duration.record(duration_ms, attrs)
                self._request_count.add(1, attrs)

                if status_code >= 400 or error is not None:
                    self._error_count.add(1, attrs)

                if retry_count > 0:
                    self._retry_count.add(retry_count, attrs)
            except Exception:
                pass

        # 3. Logging (use _log_level as internal filter to avoid mutating global logger state)
        if self._logger:
            try:
                level = logging.WARNING if (status_code >= 400 or error) else logging.DEBUG
                if level >= self._log_level:
                    self._logger.log(
                        level,
                        "%s %s %s %.1fms",
                        ctx.operation,
                        ctx.method,
                        status_code,
                        duration_ms,
                        extra={
                            "client_request_id": ctx.client_request_id,
                            "correlation_id": ctx.correlation_id,
                            "service_request_id": service_request_id,
                        },
                    )
            except Exception:
                pass

        # 4. Hook dispatch (already per-hook exception safe)
        if error is not None:
            self._dispatch_request_error(ctx, error)
        self._dispatch_request_end(ctx, response)

    # ------------------------------------------------------------------
    # Hook dispatchers
    # ------------------------------------------------------------------

    def _dispatch_request_start(self, ctx: RequestContext) -> None:
        for hook in self._hooks:
            if hasattr(hook, "on_request_start"):
                try:
                    hook.on_request_start(ctx)
                except Exception:
                    pass

    def _dispatch_request_end(self, request: RequestContext, response: ResponseContext) -> None:
        for hook in self._hooks:
            if hasattr(hook, "on_request_end"):
                try:
                    hook.on_request_end(request, response)
                except Exception:
                    pass

    def _dispatch_request_error(self, request: RequestContext, error: Exception) -> None:
        for hook in self._hooks:
            if hasattr(hook, "on_request_error"):
                try:
                    hook.on_request_error(request, error)
                except Exception:
                    pass

    def get_additional_headers(self) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        for hook in self._hooks:
            if hasattr(hook, "get_additional_headers"):
                try:
                    hook_headers = hook.get_additional_headers()
                    if hook_headers:
                        headers.update(hook_headers)
                except Exception:
                    pass
        return headers


# ============================================================================
# No-op Manager  --  zero overhead when telemetry is disabled
# ============================================================================


class NoOpTelemetryManager:
    """No-op telemetry manager used when telemetry is disabled.

    ``trace_request`` yields ``None`` and ``record_response`` is a
    no-op, so there is zero allocation or timing overhead on the
    hot path.
    """

    @contextmanager
    def trace_request(
        self,
        operation: str,
        method: str,
        url: str,
        client_request_id: str,
        correlation_id: str,
        table_name: Optional[str] = None,
    ) -> Generator[None, None, None]:
        yield None

    def record_response(self, *args: Any, **kwargs: Any) -> None:
        pass

    def get_additional_headers(self) -> Dict[str, str]:
        return {}


# ============================================================================
# Factory
# ============================================================================


def create_telemetry_manager(
    config: Optional[TelemetryConfig],
) -> Union[TelemetryManager, NoOpTelemetryManager]:
    """Create the appropriate telemetry manager for *config*.

    Returns :class:`NoOpTelemetryManager` when *config* is ``None``
    or when no signals or hooks are enabled.
    """
    if config is None:
        return NoOpTelemetryManager()

    has_any = config.enable_tracing or config.enable_metrics or config.enable_logging or config.hooks
    if not has_any:
        return NoOpTelemetryManager()

    return TelemetryManager(config)


# ============================================================================
# Telemetry Capture  --  ad-hoc request inspection
# ============================================================================


@dataclass
class CapturedRequest:
    """A single captured HTTP request with its response details.

    Instances are created by :class:`TelemetryCapture` and stored in
    its :attr:`~TelemetryCapture.requests` list.
    """

    operation: str
    method: str
    url: str
    table_name: Optional[str]
    client_request_id: str
    correlation_id: str
    status_code: int
    duration_ms: float
    service_request_id: Optional[str]


class TelemetryCapture(TelemetryHook):
    """Lightweight telemetry collector for ad-hoc debugging.

    Used with :meth:`~PowerPlatform.Dataverse.client.DataverseClient.capture_telemetry`
    to inspect HTTP request details without setting up full telemetry hooks.

    Example::

        with client.capture_telemetry() as t:
            record_id = client.records.create("account", {"name": "Contoso"})

        print(t.requests[-1].service_request_id)
        print(t.requests[-1].duration_ms)
    """

    def __init__(self) -> None:
        self.requests: List[CapturedRequest] = []

    def on_request_end(self, request: RequestContext, response: ResponseContext) -> None:
        self.requests.append(
            CapturedRequest(
                operation=request.operation,
                method=request.method,
                url=request.url,
                table_name=request.table_name,
                client_request_id=request.client_request_id,
                correlation_id=request.correlation_id,
                status_code=response.status_code,
                duration_ms=response.duration_ms,
                service_request_id=response.service_request_id,
            )
        )


__all__ = [
    "TelemetryConfig",
    "TelemetryHook",
    "RequestContext",
    "ResponseContext",
    "TelemetryCapture",
    "CapturedRequest",
    "create_telemetry_manager",
]
