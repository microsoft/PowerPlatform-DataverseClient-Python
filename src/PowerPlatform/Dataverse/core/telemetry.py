# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Telemetry infrastructure for the Dataverse SDK.

Provides OpenTelemetry-based tracing, metrics, and logging with
extensible hook system for custom telemetry providers.
"""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import (
    Any,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Protocol,
    Union,
    runtime_checkable,
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

# Optional OpenTelemetry imports
try:
    from opentelemetry import trace, metrics
    from opentelemetry.trace import Tracer, Span, Status, StatusCode
    from opentelemetry.metrics import Meter

    _OTEL_AVAILABLE = True
except ImportError:
    _OTEL_AVAILABLE = False
    trace = None  # type: ignore
    metrics = None  # type: ignore
    Tracer = None  # type: ignore
    Span = None  # type: ignore
    Status = None  # type: ignore
    StatusCode = None  # type: ignore
    Meter = None  # type: ignore


# ============================================================================
# Configuration
# ============================================================================


@dataclass(frozen=True)
class TelemetryConfig:
    """Configuration for SDK telemetry and observability.

    Telemetry is opt-in. When enabled, the SDK produces OpenTelemetry-compatible
    traces, metrics, and logs that can be exported to any OTel-compatible backend.

    Example:
        Basic tracing::

            config = DataverseConfig(
                telemetry=TelemetryConfig(enable_tracing=True)
            )

        Full observability::

            config = DataverseConfig(
                telemetry=TelemetryConfig(
                    enable_tracing=True,
                    enable_metrics=True,
                    enable_logging=True,
                    service_name="my-crm-integration"
                )
            )

        Custom hook::

            config = DataverseConfig(
                telemetry=TelemetryConfig(
                    hooks=[MyCustomTelemetryHook()]
                )
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
    """Context passed to telemetry hooks for each HTTP request."""

    # Identifiers
    client_request_id: str
    correlation_id: str

    # Request details
    method: str  # GET, POST, PATCH, DELETE
    url: str
    operation: str  # e.g., "records.create", "query.sql"
    table_name: Optional[str] = None

    # Timing
    start_time: float = field(default_factory=time.perf_counter)

    # Custom data bag for hooks to share state
    custom_data: Dict[str, Any] = field(default_factory=dict)

    # Internal: span reference for adding response attributes
    _span: Any = field(default=None, repr=False)


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
    is_retry: bool = False
    retry_count: int = 0


# ============================================================================
# Hook Protocol
# ============================================================================


@runtime_checkable
class TelemetryHook(Protocol):
    """Protocol for custom telemetry hooks.

    Implement this protocol to create custom telemetry integrations.
    All methods are optional - implement only what you need.

    Example:
        class DatadogHook:
            def __init__(self, statsd):
                self.statsd = statsd

            def on_request_end(self, request: RequestContext, response: ResponseContext):
                self.statsd.timing(
                    f"dataverse.{request.operation}.duration",
                    response.duration_ms
                )
    """

    def on_request_start(self, context: RequestContext) -> None:
        """Called before each HTTP request is sent."""
        ...

    def on_request_end(
        self, request: RequestContext, response: ResponseContext
    ) -> None:
        """Called after each HTTP request completes."""
        ...

    def on_request_error(self, request: RequestContext, error: Exception) -> None:
        """Called when an unhandled exception occurs."""
        ...

    def get_additional_headers(self) -> Dict[str, str]:
        """Return additional headers to include in requests."""
        ...


# ============================================================================
# Telemetry Manager
# ============================================================================


class TelemetryManager:
    """Manages telemetry instrumentation for the Dataverse SDK.

    This class is internal and not part of the public API.
    """

    def __init__(self, config: Optional[TelemetryConfig] = None) -> None:
        self._config = config or TelemetryConfig()
        self._tracer: Optional[Any] = None  # Tracer type when available
        self._meter: Optional[Any] = None  # Meter type when available
        self._logger: Optional[logging.Logger] = None
        self._hooks = list(self._config.hooks)

        # Metric instruments
        self._request_duration: Optional[Any] = None
        self._request_count: Optional[Any] = None
        self._error_count: Optional[Any] = None
        self._retry_count: Optional[Any] = None

        self._initialize()

    @property
    def is_tracing_enabled(self) -> bool:
        """Check if tracing is enabled and available."""
        return self._config.enable_tracing and _OTEL_AVAILABLE

    @property
    def is_metrics_enabled(self) -> bool:
        """Check if metrics are enabled and available."""
        return self._config.enable_metrics and _OTEL_AVAILABLE

    def _initialize(self) -> None:
        """Initialize telemetry components based on configuration."""
        # Initialize tracer
        if self._config.enable_tracing and _OTEL_AVAILABLE:
            self._tracer = trace.get_tracer(
                "PowerPlatform.Dataverse",
                schema_url="https://opentelemetry.io/schemas/1.21.0",
            )

        # Initialize meter
        if self._config.enable_metrics and _OTEL_AVAILABLE:
            self._meter = metrics.get_meter(
                "PowerPlatform.Dataverse",
                schema_url="https://opentelemetry.io/schemas/1.21.0",
            )
            self._setup_metrics()

        # Initialize logger
        if self._config.enable_logging:
            self._logger = logging.getLogger(self._config.logger_name)
            self._logger.setLevel(getattr(logging, self._config.log_level.upper()))

    def _setup_metrics(self) -> None:
        """Create metric instruments."""
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

    @contextmanager
    def trace_request(
        self,
        operation: str,
        method: str,
        url: str,
        client_request_id: str,
        correlation_id: str,
        table_name: Optional[str] = None,
    ) -> Generator[RequestContext, None, None]:
        """Create a traced request context.

        Usage:
            with telemetry.trace_request("records.create", "POST", url, req_id, corr_id) as ctx:
                response = self._http.request(...)
                ctx.custom_data["status"] = response.status_code
        """
        ctx = RequestContext(
            client_request_id=client_request_id,
            correlation_id=correlation_id,
            method=method,
            url=url,
            operation=operation,
            table_name=table_name,
        )

        # Dispatch to hooks
        self._dispatch_request_start(ctx)

        # Create span if tracing enabled
        span = None
        if self._tracer:
            span_name = f"Dataverse {operation}"
            if table_name:
                span_name = f"{span_name} {table_name}"

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
                    **(
                        {OTEL_ATTR_DATAVERSE_TABLE: table_name}
                        if table_name
                        else {}
                    ),
                },
            )
            ctx._span = span

        try:
            yield ctx
        except Exception as e:
            if span:
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.record_exception(e)
            self._dispatch_request_error(ctx, e)
            raise
        finally:
            if span:
                span.end()

    def record_response(
        self,
        ctx: RequestContext,
        status_code: int,
        service_request_id: Optional[str] = None,
        error: Optional[Exception] = None,
        retry_count: int = 0,
    ) -> None:
        """Record response metrics and dispatch to hooks."""
        duration_ms = (time.perf_counter() - ctx.start_time) * 1000

        response = ResponseContext(
            status_code=status_code,
            duration_ms=duration_ms,
            service_request_id=service_request_id,
            error=error,
            retry_count=retry_count,
        )

        # Add response attributes to span
        if ctx._span:
            ctx._span.set_attribute(OTEL_ATTR_HTTP_STATUS_CODE, status_code)
            if service_request_id:
                ctx._span.set_attribute(OTEL_ATTR_DATAVERSE_SERVICE_REQUEST_ID, service_request_id)

        # Record metrics
        if self._request_duration:
            attributes = {
                "operation": ctx.operation,
                "method": ctx.method,
                "status_code": status_code,
            }
            if ctx.table_name:
                attributes["table"] = ctx.table_name

            self._request_duration.record(duration_ms, attributes)
            self._request_count.add(1, attributes)

            if status_code >= 400:
                self._error_count.add(1, attributes)

            if retry_count > 0:
                self._retry_count.add(retry_count, attributes)

        # Log
        if self._logger:
            level = logging.WARNING if status_code >= 400 else logging.DEBUG
            self._logger.log(
                level,
                f"{ctx.operation} {ctx.method} {status_code} {duration_ms:.1f}ms",
                extra={
                    "client_request_id": ctx.client_request_id,
                    "service_request_id": service_request_id,
                },
            )

        # Dispatch to hooks
        self._dispatch_request_end(ctx, response)

    def _dispatch_request_start(self, ctx: RequestContext) -> None:
        """Dispatch to all registered hooks."""
        for hook in self._hooks:
            if hasattr(hook, "on_request_start"):
                try:
                    hook.on_request_start(ctx)
                except Exception:
                    pass  # Hooks should not break requests

    def _dispatch_request_end(
        self, request: RequestContext, response: ResponseContext
    ) -> None:
        """Dispatch to all registered hooks."""
        for hook in self._hooks:
            if hasattr(hook, "on_request_end"):
                try:
                    hook.on_request_end(request, response)
                except Exception:
                    pass

    def _dispatch_request_error(
        self, request: RequestContext, error: Exception
    ) -> None:
        """Dispatch to all registered hooks."""
        for hook in self._hooks:
            if hasattr(hook, "on_request_error"):
                try:
                    hook.on_request_error(request, error)
                except Exception:
                    pass

    def get_additional_headers(self) -> Dict[str, str]:
        """Collect additional headers from all hooks."""
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
# No-op Manager for when telemetry is disabled
# ============================================================================


class NoOpTelemetryManager:
    """No-op telemetry manager when telemetry is disabled."""

    @contextmanager
    def trace_request(
        self,
        operation: str,
        method: str,
        url: str,
        client_request_id: str,
        correlation_id: str,
        table_name: Optional[str] = None,
    ) -> Generator[RequestContext, None, None]:
        yield RequestContext(
            client_request_id=client_request_id,
            correlation_id=correlation_id,
            method=method,
            url=url,
            operation=operation,
            table_name=table_name,
        )

    def record_response(self, *args: Any, **kwargs: Any) -> None:
        pass

    def get_additional_headers(self) -> Dict[str, str]:
        return {}


def create_telemetry_manager(
    config: Optional[TelemetryConfig],
) -> Union[TelemetryManager, NoOpTelemetryManager]:
    """Factory to create appropriate telemetry manager."""
    if config is None:
        return NoOpTelemetryManager()

    has_any_enabled = (
        config.enable_tracing
        or config.enable_metrics
        or config.enable_logging
        or config.hooks
    )

    if not has_any_enabled:
        return NoOpTelemetryManager()

    return TelemetryManager(config)


__all__ = [
    "TelemetryConfig",
    "TelemetryHook",
    "TelemetryManager",
    "NoOpTelemetryManager",
    "RequestContext",
    "ResponseContext",
    "create_telemetry_manager",
]
