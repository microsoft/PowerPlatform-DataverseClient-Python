# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for the telemetry infrastructure."""

import logging
import time
import unittest
from unittest.mock import MagicMock, Mock, patch

from PowerPlatform.Dataverse.core.config import DataverseConfig
from PowerPlatform.Dataverse.core.telemetry import (
    NoOpTelemetryManager,
    RequestContext,
    ResponseContext,
    TelemetryConfig,
    TelemetryHook,
    TelemetryManager,
    _TrackedRequest,
    create_telemetry_manager,
)
from PowerPlatform.Dataverse.data._odata import (
    _OPERATION_NAME,
    _OPERATION_TABLE,
    _operation_scope,
)

# ============================================================================
# A. TelemetryConfig tests
# ============================================================================


class TestTelemetryConfig(unittest.TestCase):
    def test_defaults(self):
        cfg = TelemetryConfig()
        self.assertFalse(cfg.enable_tracing)
        self.assertFalse(cfg.enable_metrics)
        self.assertFalse(cfg.enable_logging)
        self.assertIsNone(cfg.service_name)
        self.assertEqual(cfg.log_level, "WARNING")
        self.assertEqual(cfg.logger_name, "PowerPlatform.Dataverse")
        self.assertEqual(cfg.hooks, [])
        self.assertFalse(cfg.capture_request_body)
        self.assertFalse(cfg.capture_response_body)
        self.assertEqual(cfg.max_body_capture_size, 1024)

    def test_frozen_immutability(self):
        cfg = TelemetryConfig()
        with self.assertRaises(AttributeError):
            cfg.enable_tracing = True  # type: ignore[misc]

    def test_construction_with_all_options(self):
        hook = TelemetryHook()
        cfg = TelemetryConfig(
            enable_tracing=True,
            enable_metrics=True,
            enable_logging=True,
            service_name="test-service",
            service_version="1.0",
            log_level="DEBUG",
            logger_name="custom.logger",
            hooks=[hook],
            capture_request_body=True,
            capture_response_body=True,
            max_body_capture_size=2048,
        )
        self.assertTrue(cfg.enable_tracing)
        self.assertTrue(cfg.enable_metrics)
        self.assertTrue(cfg.enable_logging)
        self.assertEqual(cfg.service_name, "test-service")
        self.assertEqual(len(cfg.hooks), 1)

    def test_hooks_populated(self):
        hook1 = TelemetryHook()
        hook2 = TelemetryHook()
        cfg = TelemetryConfig(hooks=[hook1, hook2])
        self.assertEqual(len(cfg.hooks), 2)

    def test_dataverse_config_from_env_has_no_telemetry(self):
        cfg = DataverseConfig.from_env()
        self.assertIsNone(cfg.telemetry)

    def test_dataverse_config_with_telemetry(self):
        tcfg = TelemetryConfig(enable_logging=True)
        cfg = DataverseConfig(telemetry=tcfg)
        self.assertIsNotNone(cfg.telemetry)
        self.assertTrue(cfg.telemetry.enable_logging)


# ============================================================================
# B. TelemetryHook base class tests
# ============================================================================


class TestTelemetryHook(unittest.TestCase):
    def test_subclass_override_receives_calls(self):
        calls = []

        class MyHook(TelemetryHook):
            def on_request_start(self, context):
                calls.append(("start", context.operation))

            def on_request_end(self, request, response):
                calls.append(("end", response.status_code))

        hook = MyHook()
        ctx = RequestContext(
            client_request_id="r1", correlation_id="c1", method="GET", url="http://x", operation="test.op"
        )
        hook.on_request_start(ctx)
        resp = ResponseContext(status_code=200, duration_ms=10.0)
        hook.on_request_end(ctx, resp)
        self.assertEqual(calls, [("start", "test.op"), ("end", 200)])

    def test_partial_override(self):
        class PartialHook(TelemetryHook):
            def on_request_end(self, request, response):
                pass

        hook = PartialHook()
        ctx = RequestContext(
            client_request_id="r1", correlation_id="c1", method="GET", url="http://x", operation="test.op"
        )
        # on_request_start is a no-op default -- should not raise
        hook.on_request_start(ctx)

    def test_duck_typed_hook_via_hasattr(self):
        class DuckHook:
            def __init__(self):
                self.called = False

            def on_request_end(self, request, response):
                self.called = True

        hook = DuckHook()
        self.assertTrue(hasattr(hook, "on_request_end"))
        self.assertFalse(hasattr(hook, "on_request_start"))

    def test_get_additional_headers_default(self):
        hook = TelemetryHook()
        self.assertEqual(hook.get_additional_headers(), {})


# ============================================================================
# C. NoOpTelemetryManager tests
# ============================================================================


class TestNoOpTelemetryManager(unittest.TestCase):
    def test_factory_returns_noop_for_none(self):
        mgr = create_telemetry_manager(None)
        self.assertIsInstance(mgr, NoOpTelemetryManager)

    def test_factory_returns_noop_for_all_disabled(self):
        mgr = create_telemetry_manager(TelemetryConfig())
        self.assertIsInstance(mgr, NoOpTelemetryManager)

    def test_trace_request_yields_none(self):
        mgr = NoOpTelemetryManager()
        with mgr.trace_request(
            operation="test", method="GET", url="http://x", client_request_id="r", correlation_id="c"
        ) as tracked:
            self.assertIsNone(tracked)

    def test_record_response_is_noop(self):
        mgr = NoOpTelemetryManager()
        # Should not raise with any arguments
        mgr.record_response(None, status_code=200)
        mgr.record_response(None, status_code=500, error=Exception("test"))

    def test_get_additional_headers_returns_empty(self):
        mgr = NoOpTelemetryManager()
        self.assertEqual(mgr.get_additional_headers(), {})


# ============================================================================
# D. TelemetryManager initialization tests
# ============================================================================


class TestTelemetryManagerInit(unittest.TestCase):
    def test_tracing_without_otel_installed(self):
        with patch("PowerPlatform.Dataverse.core.telemetry._OTEL_AVAILABLE", False):
            mgr = TelemetryManager(TelemetryConfig(enable_tracing=True))
            self.assertIsNone(mgr._tracer)
            self.assertFalse(mgr.is_tracing_enabled)

    def test_logging_enabled_creates_logger(self):
        mgr = TelemetryManager(TelemetryConfig(enable_logging=True, logger_name="test.telemetry.logger"))
        self.assertIsNotNone(mgr._logger)
        self.assertEqual(mgr._logger.name, "test.telemetry.logger")

    def test_invalid_log_level_falls_back_to_warning(self):
        mgr = TelemetryManager(TelemetryConfig(enable_logging=True, log_level="VERBOSE"))
        self.assertIsNotNone(mgr._logger)
        self.assertEqual(mgr._logger.level, logging.WARNING)

    def test_factory_with_hooks_only_returns_real_manager(self):
        hook = TelemetryHook()
        mgr = create_telemetry_manager(TelemetryConfig(hooks=[hook]))
        self.assertIsInstance(mgr, TelemetryManager)

    def test_is_metrics_enabled_without_otel(self):
        with patch("PowerPlatform.Dataverse.core.telemetry._OTEL_AVAILABLE", False):
            mgr = TelemetryManager(TelemetryConfig(enable_metrics=True))
            self.assertFalse(mgr.is_metrics_enabled)


# ============================================================================
# E. Hook dispatch tests
# ============================================================================


class TestHookDispatch(unittest.TestCase):
    def _make_manager(self, *hooks):
        cfg = TelemetryConfig(hooks=list(hooks))
        return TelemetryManager(cfg)

    def test_on_request_start_called(self):
        hook = MagicMock(spec=TelemetryHook)
        mgr = self._make_manager(hook)
        ctx = RequestContext(
            client_request_id="r1", correlation_id="c1", method="GET", url="http://x", operation="test.op"
        )
        mgr._dispatch_request_start(ctx)
        hook.on_request_start.assert_called_once_with(ctx)

    def test_on_request_end_called(self):
        hook = MagicMock(spec=TelemetryHook)
        mgr = self._make_manager(hook)
        ctx = RequestContext(
            client_request_id="r1", correlation_id="c1", method="GET", url="http://x", operation="test.op"
        )
        resp = ResponseContext(status_code=200, duration_ms=10.0)
        mgr._dispatch_request_end(ctx, resp)
        hook.on_request_end.assert_called_once_with(ctx, resp)

    def test_on_request_error_called(self):
        hook = MagicMock(spec=TelemetryHook)
        mgr = self._make_manager(hook)
        ctx = RequestContext(
            client_request_id="r1", correlation_id="c1", method="GET", url="http://x", operation="test.op"
        )
        err = RuntimeError("fail")
        mgr._dispatch_request_error(ctx, err)
        hook.on_request_error.assert_called_once_with(ctx, err)

    def test_hook_exception_swallowed(self):
        hook = MagicMock(spec=TelemetryHook)
        hook.on_request_start.side_effect = RuntimeError("hook crash")
        mgr = self._make_manager(hook)
        ctx = RequestContext(
            client_request_id="r1", correlation_id="c1", method="GET", url="http://x", operation="test.op"
        )
        # Should not raise
        mgr._dispatch_request_start(ctx)

    def test_get_additional_headers_merges(self):
        hook1 = MagicMock(spec=TelemetryHook)
        hook1.get_additional_headers.return_value = {"X-Hook1": "val1"}
        hook2 = MagicMock(spec=TelemetryHook)
        hook2.get_additional_headers.return_value = {"X-Hook2": "val2"}
        mgr = self._make_manager(hook1, hook2)
        headers = mgr.get_additional_headers()
        self.assertEqual(headers, {"X-Hook1": "val1", "X-Hook2": "val2"})

    def test_duck_typed_hook_with_partial_methods(self):
        class DuckHook:
            def __init__(self):
                self.called = False

            def on_request_end(self, request, response):
                self.called = True

        hook = DuckHook()
        mgr = self._make_manager(hook)
        ctx = RequestContext(
            client_request_id="r1", correlation_id="c1", method="GET", url="http://x", operation="test.op"
        )
        # on_request_start not defined on duck hook -- should not call or crash
        mgr._dispatch_request_start(ctx)
        self.assertFalse(hook.called)

        resp = ResponseContext(status_code=200, duration_ms=10.0)
        mgr._dispatch_request_end(ctx, resp)
        self.assertTrue(hook.called)

    def test_request_context_has_no_span_attribute(self):
        ctx = RequestContext(
            client_request_id="r1", correlation_id="c1", method="GET", url="http://x", operation="test.op"
        )
        self.assertFalse(hasattr(ctx, "_span"))

    def test_multiple_hooks_all_called(self):
        hook1 = MagicMock(spec=TelemetryHook)
        hook2 = MagicMock(spec=TelemetryHook)
        mgr = self._make_manager(hook1, hook2)
        ctx = RequestContext(
            client_request_id="r1", correlation_id="c1", method="GET", url="http://x", operation="test.op"
        )
        mgr._dispatch_request_start(ctx)
        hook1.on_request_start.assert_called_once()
        hook2.on_request_start.assert_called_once()


# ============================================================================
# F. trace_request + record_response tests
# ============================================================================


class TestTraceRequestAndRecordResponse(unittest.TestCase):
    def test_trace_request_yields_tracked_request(self):
        mgr = TelemetryManager(TelemetryConfig(hooks=[TelemetryHook()]))
        with mgr.trace_request(
            operation="records.create",
            method="POST",
            url="http://example.com/api/data/v9.2/accounts",
            client_request_id="req-1",
            correlation_id="corr-1",
            table_name="account",
        ) as tracked:
            self.assertIsInstance(tracked, _TrackedRequest)
            self.assertEqual(tracked.context.operation, "records.create")
            self.assertEqual(tracked.context.table_name, "account")
            self.assertEqual(tracked.context.method, "POST")
            self.assertEqual(tracked.context.client_request_id, "req-1")

    def test_span_not_created_without_otel(self):
        with patch("PowerPlatform.Dataverse.core.telemetry._OTEL_AVAILABLE", False):
            mgr = TelemetryManager(TelemetryConfig(enable_tracing=True))
            with mgr.trace_request(
                operation="test", method="GET", url="http://x", client_request_id="r", correlation_id="c"
            ) as tracked:
                self.assertIsNone(tracked._span)

    def test_span_end_called_in_finally(self):
        mock_tracer = MagicMock()
        mock_span = MagicMock()
        mock_tracer.start_span.return_value = mock_span

        mgr = TelemetryManager(TelemetryConfig(enable_tracing=True))
        mgr._tracer = mock_tracer

        with mgr.trace_request(
            operation="test", method="GET", url="http://x", client_request_id="r", correlation_id="c"
        ) as tracked:
            self.assertIs(tracked._span, mock_span)

        mock_span.end.assert_called_once()

    def test_record_response_sets_span_status_ok(self):
        from opentelemetry.trace import StatusCode

        mock_span = MagicMock()
        ctx = RequestContext(
            client_request_id="r1", correlation_id="c1", method="GET", url="http://x", operation="test.op"
        )
        tracked = _TrackedRequest(context=ctx, _span=mock_span)

        mgr = TelemetryManager(TelemetryConfig(enable_tracing=True))
        mgr.record_response(tracked, status_code=200, service_request_id="srv-1")

        # Verify span status was set to OK
        mock_span.set_status.assert_called_once()
        status_arg = mock_span.set_status.call_args[0][0]
        self.assertEqual(status_arg.status_code, StatusCode.OK)

    def test_record_response_sets_span_status_error(self):
        mock_span = MagicMock()
        ctx = RequestContext(
            client_request_id="r1", correlation_id="c1", method="GET", url="http://x", operation="test.op"
        )
        tracked = _TrackedRequest(context=ctx, _span=mock_span)

        mgr = TelemetryManager(TelemetryConfig(enable_tracing=True))
        err = RuntimeError("fail")
        mgr.record_response(tracked, status_code=500, error=err)

        mock_span.set_status.assert_called_once()
        mock_span.record_exception.assert_called_once_with(err)

    def test_record_response_logs_at_correct_level(self):
        mgr = TelemetryManager(TelemetryConfig(enable_logging=True, log_level="DEBUG"))
        ctx = RequestContext(
            client_request_id="r1", correlation_id="c1", method="GET", url="http://x", operation="test.op"
        )

        with patch.object(mgr._logger, "log") as mock_log:
            tracked = _TrackedRequest(context=ctx)
            mgr.record_response(tracked, status_code=200)
            mock_log.assert_called_once()
            self.assertEqual(mock_log.call_args[0][0], logging.DEBUG)

        with patch.object(mgr._logger, "log") as mock_log:
            tracked2 = _TrackedRequest(context=ctx)
            mgr.record_response(tracked2, status_code=404)
            mock_log.assert_called_once()
            self.assertEqual(mock_log.call_args[0][0], logging.WARNING)

    def test_record_response_exception_safe(self):
        """record_response must not raise even if span/metrics/logger crash."""
        mock_span = MagicMock()
        mock_span.set_attribute.side_effect = RuntimeError("span crash")
        ctx = RequestContext(
            client_request_id="r1", correlation_id="c1", method="GET", url="http://x", operation="test.op"
        )
        tracked = _TrackedRequest(context=ctx, _span=mock_span)

        mgr = TelemetryManager(TelemetryConfig(enable_tracing=True, enable_logging=True))
        # Should not raise even though span.set_attribute crashes
        mgr.record_response(tracked, status_code=200)

    def test_no_except_block_in_trace_request(self):
        """Verify trace_request does not catch exceptions (no double dispatch)."""
        hook = MagicMock(spec=TelemetryHook)
        mgr = TelemetryManager(TelemetryConfig(hooks=[hook]))

        with self.assertRaises(RuntimeError):
            with mgr.trace_request(
                operation="test", method="GET", url="http://x", client_request_id="r", correlation_id="c"
            ):
                raise RuntimeError("inner error")

        # on_request_error should NOT be called by trace_request
        hook.on_request_error.assert_not_called()


# ============================================================================
# G. _operation_scope ContextVar tests
# ============================================================================


class TestOperationScope(unittest.TestCase):
    def test_set_and_reset(self):
        self.assertIsNone(_OPERATION_NAME.get())
        self.assertIsNone(_OPERATION_TABLE.get())

        with _operation_scope("records.create", "account"):
            self.assertEqual(_OPERATION_NAME.get(), "records.create")
            self.assertEqual(_OPERATION_TABLE.get(), "account")

        self.assertIsNone(_OPERATION_NAME.get())
        self.assertIsNone(_OPERATION_TABLE.get())

    def test_nested_scopes(self):
        with _operation_scope("outer.op", "outer_table"):
            self.assertEqual(_OPERATION_NAME.get(), "outer.op")
            with _operation_scope("inner.op", "inner_table"):
                self.assertEqual(_OPERATION_NAME.get(), "inner.op")
                self.assertEqual(_OPERATION_TABLE.get(), "inner_table")
            self.assertEqual(_OPERATION_NAME.get(), "outer.op")
            self.assertEqual(_OPERATION_TABLE.get(), "outer_table")

    def test_default_values(self):
        self.assertIsNone(_OPERATION_NAME.get())
        self.assertIsNone(_OPERATION_TABLE.get())

    def test_cleanup_on_exception(self):
        try:
            with _operation_scope("failing.op", "fail_table"):
                self.assertEqual(_OPERATION_NAME.get(), "failing.op")
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        self.assertIsNone(_OPERATION_NAME.get())
        self.assertIsNone(_OPERATION_TABLE.get())


# ============================================================================
# H. Integration with _request() tests
# ============================================================================


class TestRequestIntegration(unittest.TestCase):
    """Integration tests verifying telemetry wiring in _ODataClient._request()."""

    def _make_odata_client(self, telemetry_config=None):
        """Create a minimal _ODataClient with mocked auth and HTTP."""
        from PowerPlatform.Dataverse.data._odata import _ODataClient

        class DummyAuth:
            def _acquire_token(self, scope):
                class Token:
                    access_token = "test_token"

                return Token()

        config = DataverseConfig(telemetry=telemetry_config)
        client = _ODataClient(DummyAuth(), "https://org.example.com", config)
        client._http = MagicMock()
        return client

    def test_telemetry_called_on_success(self):
        hook = MagicMock(spec=TelemetryHook)
        cfg = TelemetryConfig(hooks=[hook])
        client = self._make_odata_client(cfg)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {"x-ms-service-request-id": "srv-123"}
        client._http._request.return_value = mock_resp

        with _operation_scope("records.get", "account"):
            result = client._request("GET", "https://org.example.com/api/data/v9.2/accounts")

        self.assertEqual(result.status_code, 200)
        hook.on_request_start.assert_called_once()
        hook.on_request_end.assert_called_once()
        # Verify the RequestContext passed to hook
        start_ctx = hook.on_request_start.call_args[0][0]
        self.assertEqual(start_ctx.operation, "records.get")
        self.assertEqual(start_ctx.table_name, "account")

    def test_telemetry_called_on_http_error(self):
        hook = MagicMock(spec=TelemetryHook)
        cfg = TelemetryConfig(hooks=[hook])
        client = self._make_odata_client(cfg)

        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_resp.headers = {}
        mock_resp.text = '{"error": {"message": "Not Found"}}'
        mock_resp.json.return_value = {"error": {"message": "Not Found"}}
        client._http._request.return_value = mock_resp

        from PowerPlatform.Dataverse.core.errors import HttpError

        with self.assertRaises(HttpError):
            with _operation_scope("records.get", "account"):
                client._request("GET", "https://org.example.com/api/data/v9.2/accounts(bad-id)")

        hook.on_request_end.assert_called_once()
        end_args = hook.on_request_end.call_args[0]
        resp_ctx = end_args[1]
        self.assertEqual(resp_ctx.status_code, 404)
        self.assertIsNotNone(resp_ctx.error)

    def test_telemetry_called_on_network_error(self):
        hook = MagicMock(spec=TelemetryHook)
        cfg = TelemetryConfig(hooks=[hook])
        client = self._make_odata_client(cfg)

        client._http._request.side_effect = ConnectionError("network down")

        with self.assertRaises(ConnectionError):
            with _operation_scope("records.create", "account"):
                client._request("POST", "https://org.example.com/api/data/v9.2/accounts", json={"name": "Test"})

        hook.on_request_end.assert_called_once()
        end_args = hook.on_request_end.call_args[0]
        resp_ctx = end_args[1]
        self.assertEqual(resp_ctx.status_code, 0)
        self.assertIsInstance(resp_ctx.error, ConnectionError)

    def test_hook_headers_merged_into_request(self):
        hook = MagicMock(spec=TelemetryHook)
        hook.get_additional_headers.return_value = {"X-Custom-Trace": "abc123"}
        cfg = TelemetryConfig(hooks=[hook])
        client = self._make_odata_client(cfg)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {}
        client._http._request.return_value = mock_resp

        client._request("GET", "https://org.example.com/api/data/v9.2/accounts")

        # Verify the custom header was included
        call_kwargs = client._http._request.call_args
        sent_headers = (
            call_kwargs[1].get("headers", {})
            if call_kwargs[1]
            else call_kwargs[0][2] if len(call_kwargs[0]) > 2 else {}
        )
        # The headers should contain our custom header
        # Check via the actual kwargs passed to _http._request
        actual_kwargs = client._http._request.call_args
        actual_headers = actual_kwargs.kwargs.get(
            "headers", actual_kwargs.args[2] if len(actual_kwargs.args) > 2 else {}
        )
        self.assertIn("X-Custom-Trace", actual_headers)
        self.assertEqual(actual_headers["X-Custom-Trace"], "abc123")

    def test_operation_context_propagated(self):
        hook = MagicMock(spec=TelemetryHook)
        cfg = TelemetryConfig(hooks=[hook])
        client = self._make_odata_client(cfg)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {}
        client._http._request.return_value = mock_resp

        with _operation_scope("tables.list"):
            client._request("GET", "https://org.example.com/api/data/v9.2/EntityDefinitions")

        start_ctx = hook.on_request_start.call_args[0][0]
        self.assertEqual(start_ctx.operation, "tables.list")
        self.assertIsNone(start_ctx.table_name)

    def test_no_telemetry_config_uses_noop(self):
        client = self._make_odata_client(None)
        self.assertIsInstance(client._telemetry, NoOpTelemetryManager)

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.headers = {}
        client._http._request.return_value = mock_resp

        # Should work fine with no telemetry
        result = client._request("GET", "https://org.example.com/api/data/v9.2/accounts")
        self.assertEqual(result.status_code, 200)


if __name__ == "__main__":
    unittest.main()
