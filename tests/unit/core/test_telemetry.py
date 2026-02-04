# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Tests for telemetry infrastructure."""

import pytest
from unittest.mock import MagicMock, patch

from PowerPlatform.Dataverse.core.telemetry import (
    TelemetryConfig,
    TelemetryManager,
    NoOpTelemetryManager,
    RequestContext,
    ResponseContext,
    create_telemetry_manager,
)


class TestTelemetryConfig:
    """Tests for TelemetryConfig dataclass."""

    def test_default_values(self):
        config = TelemetryConfig()
        assert config.enable_tracing is False
        assert config.enable_metrics is False
        assert config.enable_logging is False
        assert config.hooks == []

    def test_immutability(self):
        config = TelemetryConfig(enable_tracing=True)
        with pytest.raises(AttributeError):
            config.enable_tracing = False

    def test_custom_service_name(self):
        config = TelemetryConfig(service_name="my-app", service_version="1.0.0")
        assert config.service_name == "my-app"
        assert config.service_version == "1.0.0"

    def test_privacy_options_default_disabled(self):
        config = TelemetryConfig()
        assert config.capture_request_body is False
        assert config.capture_response_body is False
        assert config.max_body_capture_size == 1024


class TestTelemetryManagerFactory:
    """Tests for create_telemetry_manager factory."""

    def test_returns_noop_when_config_none(self):
        manager = create_telemetry_manager(None)
        assert isinstance(manager, NoOpTelemetryManager)

    def test_returns_noop_when_all_disabled(self):
        config = TelemetryConfig()
        manager = create_telemetry_manager(config)
        assert isinstance(manager, NoOpTelemetryManager)

    def test_returns_manager_when_tracing_enabled(self):
        config = TelemetryConfig(enable_tracing=True)
        manager = create_telemetry_manager(config)
        assert isinstance(manager, TelemetryManager)

    def test_returns_manager_when_metrics_enabled(self):
        config = TelemetryConfig(enable_metrics=True)
        manager = create_telemetry_manager(config)
        assert isinstance(manager, TelemetryManager)

    def test_returns_manager_when_logging_enabled(self):
        config = TelemetryConfig(enable_logging=True)
        manager = create_telemetry_manager(config)
        assert isinstance(manager, TelemetryManager)

    def test_returns_manager_when_hooks_provided(self):
        hook = MagicMock()
        config = TelemetryConfig(hooks=[hook])
        manager = create_telemetry_manager(config)
        assert isinstance(manager, TelemetryManager)


class TestTelemetryManager:
    """Tests for TelemetryManager."""

    def test_trace_request_creates_context(self):
        config = TelemetryConfig(enable_logging=True)
        manager = TelemetryManager(config)

        with manager.trace_request(
            operation="records.create",
            method="POST",
            url="https://test.crm.dynamics.com/api/data/v9.2/accounts",
            client_request_id="req-123",
            correlation_id="corr-456",
            table_name="account",
        ) as ctx:
            assert ctx.operation == "records.create"
            assert ctx.method == "POST"
            assert ctx.table_name == "account"
            assert ctx.client_request_id == "req-123"
            assert ctx.correlation_id == "corr-456"

    def test_hooks_dispatched_on_request_start(self):
        hook = MagicMock()
        config = TelemetryConfig(hooks=[hook])
        manager = TelemetryManager(config)

        with manager.trace_request(
            operation="test",
            method="GET",
            url="https://test.com",
            client_request_id="123",
            correlation_id="456",
        ):
            pass

        hook.on_request_start.assert_called_once()

    def test_hooks_dispatched_on_request_end(self):
        hook = MagicMock()
        config = TelemetryConfig(hooks=[hook])
        manager = TelemetryManager(config)

        with manager.trace_request(
            operation="test",
            method="GET",
            url="https://test.com",
            client_request_id="123",
            correlation_id="456",
        ) as ctx:
            manager.record_response(ctx, status_code=200)

        hook.on_request_end.assert_called_once()

    def test_hooks_dispatched_on_request_error(self):
        hook = MagicMock()
        config = TelemetryConfig(hooks=[hook])
        manager = TelemetryManager(config)

        with pytest.raises(ValueError):
            with manager.trace_request(
                operation="test",
                method="GET",
                url="https://test.com",
                client_request_id="123",
                correlation_id="456",
            ):
                raise ValueError("Test error")

        hook.on_request_error.assert_called_once()

    def test_hook_errors_do_not_break_request(self):
        hook = MagicMock()
        hook.on_request_start.side_effect = Exception("Hook error")
        config = TelemetryConfig(hooks=[hook])
        manager = TelemetryManager(config)

        # Should not raise
        with manager.trace_request(
            operation="test",
            method="GET",
            url="https://test.com",
            client_request_id="123",
            correlation_id="456",
        ):
            pass

    def test_get_additional_headers_collects_from_hooks(self):
        hook1 = MagicMock()
        hook1.get_additional_headers.return_value = {"X-Custom-1": "value1"}

        hook2 = MagicMock()
        hook2.get_additional_headers.return_value = {"X-Custom-2": "value2"}

        config = TelemetryConfig(hooks=[hook1, hook2])
        manager = TelemetryManager(config)

        headers = manager.get_additional_headers()
        assert headers == {"X-Custom-1": "value1", "X-Custom-2": "value2"}

    def test_get_additional_headers_handles_hook_errors(self):
        hook = MagicMock()
        hook.get_additional_headers.side_effect = Exception("Hook error")

        config = TelemetryConfig(hooks=[hook])
        manager = TelemetryManager(config)

        # Should not raise, returns empty dict
        headers = manager.get_additional_headers()
        assert headers == {}

    def test_record_response_dispatches_to_hooks(self):
        hook = MagicMock()
        config = TelemetryConfig(hooks=[hook])
        manager = TelemetryManager(config)

        ctx = RequestContext(
            client_request_id="req-123",
            correlation_id="corr-456",
            method="POST",
            url="https://test.com",
            operation="records.create",
        )

        manager.record_response(ctx, status_code=201, service_request_id="srv-789")

        hook.on_request_end.assert_called_once()
        call_args = hook.on_request_end.call_args
        response = call_args[0][1]
        assert isinstance(response, ResponseContext)
        assert response.status_code == 201
        assert response.service_request_id == "srv-789"


class TestNoOpTelemetryManager:
    """Tests for NoOpTelemetryManager."""

    def test_trace_request_returns_context(self):
        manager = NoOpTelemetryManager()

        with manager.trace_request(
            operation="test",
            method="GET",
            url="https://test.com",
            client_request_id="123",
            correlation_id="456",
        ) as ctx:
            assert ctx.operation == "test"
            assert ctx.method == "GET"

    def test_record_response_is_noop(self):
        manager = NoOpTelemetryManager()
        # Should not raise
        manager.record_response(None, 200)

    def test_get_additional_headers_returns_empty(self):
        manager = NoOpTelemetryManager()
        assert manager.get_additional_headers() == {}


class TestRequestContext:
    """Tests for RequestContext dataclass."""

    def test_default_start_time(self):
        ctx = RequestContext(
            client_request_id="req-1",
            correlation_id="corr-1",
            method="GET",
            url="https://test.com",
            operation="test",
        )
        assert ctx.start_time > 0

    def test_custom_data_bag(self):
        ctx = RequestContext(
            client_request_id="req-1",
            correlation_id="corr-1",
            method="GET",
            url="https://test.com",
            operation="test",
        )
        ctx.custom_data["my_key"] = "my_value"
        assert ctx.custom_data["my_key"] == "my_value"


class TestResponseContext:
    """Tests for ResponseContext dataclass."""

    def test_basic_response(self):
        response = ResponseContext(
            status_code=200,
            duration_ms=150.5,
            service_request_id="srv-123",
        )
        assert response.status_code == 200
        assert response.duration_ms == 150.5
        assert response.service_request_id == "srv-123"

    def test_error_response(self):
        error = ValueError("Test error")
        response = ResponseContext(
            status_code=500,
            duration_ms=50.0,
            error=error,
            is_retry=True,
            retry_count=2,
        )
        assert response.error is error
        assert response.is_retry is True
        assert response.retry_count == 2


class TestOpenTelemetryIntegration:
    """Tests for OpenTelemetry integration when SDK is available."""

    @pytest.fixture
    def mock_otel(self):
        """Mock OpenTelemetry SDK."""
        with patch("PowerPlatform.Dataverse.core.telemetry._OTEL_AVAILABLE", True):
            with patch("PowerPlatform.Dataverse.core.telemetry.trace") as mock_trace:
                with patch(
                    "PowerPlatform.Dataverse.core.telemetry.metrics"
                ) as mock_metrics:
                    with patch(
                        "PowerPlatform.Dataverse.core.telemetry.Status"
                    ) as mock_status:
                        with patch(
                            "PowerPlatform.Dataverse.core.telemetry.StatusCode"
                        ) as mock_status_code:
                            mock_tracer = MagicMock()
                            mock_trace.get_tracer.return_value = mock_tracer
                            mock_trace.SpanKind.CLIENT = "CLIENT"

                            mock_span = MagicMock()
                            mock_tracer.start_span.return_value = mock_span

                            mock_meter = MagicMock()
                            mock_metrics.get_meter.return_value = mock_meter

                            mock_status_code.ERROR = "ERROR"

                            yield {
                                "trace": mock_trace,
                                "metrics": mock_metrics,
                                "tracer": mock_tracer,
                                "meter": mock_meter,
                                "span": mock_span,
                                "status": mock_status,
                                "status_code": mock_status_code,
                            }

    def test_tracer_initialized_when_tracing_enabled(self, mock_otel):
        config = TelemetryConfig(enable_tracing=True)
        manager = TelemetryManager(config)

        mock_otel["trace"].get_tracer.assert_called_once()

    def test_meter_initialized_when_metrics_enabled(self, mock_otel):
        config = TelemetryConfig(enable_metrics=True)
        manager = TelemetryManager(config)

        mock_otel["metrics"].get_meter.assert_called_once()

    def test_span_created_on_trace_request(self, mock_otel):
        config = TelemetryConfig(enable_tracing=True)
        manager = TelemetryManager(config)

        with manager.trace_request(
            operation="records.create",
            method="POST",
            url="https://test.com",
            client_request_id="req-123",
            correlation_id="corr-456",
            table_name="account",
        ):
            pass

        mock_otel["tracer"].start_span.assert_called_once()
        call_args = mock_otel["tracer"].start_span.call_args
        assert "Dataverse records.create account" in call_args[0][0]

    def test_span_ended_after_request(self, mock_otel):
        config = TelemetryConfig(enable_tracing=True)
        manager = TelemetryManager(config)

        with manager.trace_request(
            operation="test",
            method="GET",
            url="https://test.com",
            client_request_id="123",
            correlation_id="456",
        ):
            pass

        mock_otel["span"].end.assert_called_once()

    def test_span_records_exception_on_error(self, mock_otel):
        config = TelemetryConfig(enable_tracing=True)
        manager = TelemetryManager(config)

        with pytest.raises(ValueError):
            with manager.trace_request(
                operation="test",
                method="GET",
                url="https://test.com",
                client_request_id="123",
                correlation_id="456",
            ):
                raise ValueError("Test error")

        mock_otel["span"].record_exception.assert_called_once()
        mock_otel["span"].set_status.assert_called_once()

    def test_metrics_created_when_enabled(self, mock_otel):
        config = TelemetryConfig(enable_metrics=True)
        manager = TelemetryManager(config)

        # Verify metrics were created
        meter = mock_otel["meter"]
        assert meter.create_histogram.called
        assert meter.create_counter.called


class TestTelemetryManagerProperties:
    """Tests for TelemetryManager property methods."""

    def test_is_tracing_enabled_false_when_otel_unavailable(self):
        with patch("PowerPlatform.Dataverse.core.telemetry._OTEL_AVAILABLE", False):
            config = TelemetryConfig(enable_tracing=True)
            manager = TelemetryManager(config)
            # Even though config says tracing enabled, OTel not available
            assert manager.is_tracing_enabled is False

    def test_is_metrics_enabled_false_when_otel_unavailable(self):
        with patch("PowerPlatform.Dataverse.core.telemetry._OTEL_AVAILABLE", False):
            config = TelemetryConfig(enable_metrics=True)
            manager = TelemetryManager(config)
            assert manager.is_metrics_enabled is False
