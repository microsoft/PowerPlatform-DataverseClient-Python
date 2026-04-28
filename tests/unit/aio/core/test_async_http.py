# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import pytest
from unittest.mock import AsyncMock, MagicMock, call, patch

import aiohttp

from PowerPlatform.Dataverse.aio.core._async_http import _AsyncHttpClient


def _make_session(status: int = 200) -> MagicMock:
    """Return a mock aiohttp.ClientSession whose request() returns a buffered response."""
    session = MagicMock(spec=aiohttp.ClientSession)
    resp = AsyncMock()
    resp.status = status
    resp.headers = {}
    resp.read = AsyncMock(return_value=b"")
    resp.text = AsyncMock(return_value="")
    session.request = AsyncMock(return_value=resp)
    return session


class TestAsyncHttpClientTimeout:
    """Tests for automatic timeout selection in _AsyncHttpClient._request."""

    async def test_get_uses_10s_default_timeout(self):
        """GET requests use 10 s default when no timeout is specified."""
        session = _make_session()
        client = _AsyncHttpClient(retries=1, session=session)
        await client._request("get", "https://example.com/data")
        _, kwargs = session.request.call_args
        assert isinstance(kwargs["timeout"], aiohttp.ClientTimeout)
        assert kwargs["timeout"].total == 10

    async def test_post_uses_120s_default_timeout(self):
        """POST requests use 120 s default when no timeout is specified."""
        session = _make_session()
        client = _AsyncHttpClient(retries=1, session=session)
        await client._request("post", "https://example.com/data")
        _, kwargs = session.request.call_args
        assert kwargs["timeout"].total == 120

    async def test_delete_uses_120s_default_timeout(self):
        """DELETE requests use 120 s default when no timeout is specified."""
        session = _make_session()
        client = _AsyncHttpClient(retries=1, session=session)
        await client._request("delete", "https://example.com/data")
        _, kwargs = session.request.call_args
        assert kwargs["timeout"].total == 120

    async def test_put_uses_10s_default_timeout(self):
        """PUT requests use 10 s default (only POST/DELETE get 120 s)."""
        session = _make_session()
        client = _AsyncHttpClient(retries=1, session=session)
        await client._request("put", "https://example.com/data")
        _, kwargs = session.request.call_args
        assert kwargs["timeout"].total == 10

    async def test_patch_uses_10s_default_timeout(self):
        """PATCH requests use 10 s default (only POST/DELETE get 120 s)."""
        session = _make_session()
        client = _AsyncHttpClient(retries=1, session=session)
        await client._request("patch", "https://example.com/data")
        _, kwargs = session.request.call_args
        assert kwargs["timeout"].total == 10

    async def test_custom_client_timeout_overrides_method_default(self):
        """Explicit default_timeout on the client overrides per-method defaults."""
        session = _make_session()
        client = _AsyncHttpClient(retries=1, timeout=30.0, session=session)
        await client._request("get", "https://example.com/data")
        _, kwargs = session.request.call_args
        assert kwargs["timeout"].total == 30.0

    async def test_explicit_timeout_kwarg_takes_precedence(self):
        """If timeout is already in kwargs it is passed through unchanged."""
        session = _make_session()
        client = _AsyncHttpClient(retries=1, timeout=30.0, session=session)
        custom_timeout = aiohttp.ClientTimeout(total=5)
        await client._request("get", "https://example.com/data", timeout=custom_timeout)
        _, kwargs = session.request.call_args
        assert kwargs["timeout"] is custom_timeout


class TestAsyncHttpClientNoSession:
    """Tests for RuntimeError when no session is provided."""

    async def test_raises_runtime_error_without_session(self):
        """_request raises RuntimeError if no session has been set."""
        client = _AsyncHttpClient(retries=1)
        with pytest.raises(RuntimeError, match="No aiohttp.ClientSession"):
            await client._request("get", "https://example.com")


class TestAsyncHttpClientRetry:
    """Tests for retry behavior on aiohttp.ClientError."""

    async def test_retries_on_client_error_and_succeeds(self):
        """Retries after a ClientError and returns response on second attempt."""
        session = MagicMock(spec=aiohttp.ClientSession)
        good_resp = AsyncMock()
        good_resp.status = 200
        good_resp.headers = {}
        good_resp.read = AsyncMock(return_value=b"")
        good_resp.text = AsyncMock(return_value="")

        session.request = AsyncMock(side_effect=[aiohttp.ClientConnectionError("timeout"), good_resp])
        client = _AsyncHttpClient(retries=2, backoff=0, session=session)
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await client._request("get", "https://example.com/data")

        assert session.request.call_count == 2
        assert result is good_resp

    async def test_raises_after_all_retries_exhausted(self):
        """Raises ClientError after all retry attempts fail."""
        session = MagicMock(spec=aiohttp.ClientSession)
        session.request = AsyncMock(side_effect=aiohttp.ClientConnectionError("timeout"))
        client = _AsyncHttpClient(retries=3, backoff=0, session=session)
        with patch("asyncio.sleep", new_callable=AsyncMock):
            with pytest.raises(aiohttp.ClientError):
                await client._request("get", "https://example.com/data")

    async def test_backoff_delay_between_retries(self):
        """Sleeps with exponential backoff between retry attempts."""
        session = MagicMock(spec=aiohttp.ClientSession)
        good_resp = AsyncMock()
        good_resp.status = 200
        good_resp.headers = {}
        good_resp.read = AsyncMock(return_value=b"")
        good_resp.text = AsyncMock(return_value="")

        session.request = AsyncMock(
            side_effect=[
                aiohttp.ClientConnectionError(),
                aiohttp.ClientConnectionError(),
                good_resp,
            ]
        )
        client = _AsyncHttpClient(retries=3, backoff=1.0, session=session)
        with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            await client._request("get", "https://example.com/data")
        # First retry: 1.0 * 2^0 = 1.0; second retry: 1.0 * 2^1 = 2.0
        mock_sleep.assert_has_calls([call(1.0), call(2.0)])

    async def test_no_retry_on_success(self):
        """Single successful response does not trigger retries."""
        session = _make_session(200)
        client = _AsyncHttpClient(retries=5, backoff=0, session=session)
        await client._request("get", "https://example.com/data")
        assert session.request.call_count == 1


class TestAsyncHttpClientClose:
    """Tests for _AsyncHttpClient.close()."""

    async def test_close_closes_session(self):
        """close() closes the session and sets _session to None."""
        session = MagicMock(spec=aiohttp.ClientSession)
        session.close = AsyncMock()
        client = _AsyncHttpClient(retries=1, session=session)
        await client.close()
        session.close.assert_called_once()
        assert client._session is None

    async def test_close_without_session_is_safe(self):
        """close() is safe to call when no session was set."""
        client = _AsyncHttpClient(retries=1)
        await client.close()  # should not raise


class TestAsyncHttpClientLogger:
    """Tests for request logging via _HttpLogger integration."""

    async def test_request_logged_when_logger_set(self):
        """Outbound request is logged once when a logger is attached."""
        session = _make_session()
        mock_logger = MagicMock()
        mock_logger.body_logging_enabled = False
        client = _AsyncHttpClient(retries=1, session=session, logger=mock_logger)
        await client._request("get", "https://example.com/data")
        mock_logger.log_request.assert_called_once()

    async def test_response_logged_when_logger_set(self):
        """HTTP response is logged when a logger is attached."""
        session = _make_session()
        mock_logger = MagicMock()
        mock_logger.body_logging_enabled = False
        client = _AsyncHttpClient(retries=1, session=session, logger=mock_logger)
        await client._request("get", "https://example.com/data")
        mock_logger.log_response.assert_called_once()

    async def test_error_logged_on_retry(self):
        """Transport errors are logged before each retry."""
        session = MagicMock(spec=aiohttp.ClientSession)
        good_resp = AsyncMock()
        good_resp.status = 200
        good_resp.headers = {}
        good_resp.read = AsyncMock(return_value=b"")
        good_resp.text = AsyncMock(return_value="")
        session.request = AsyncMock(side_effect=[aiohttp.ClientConnectionError(), good_resp])
        mock_logger = MagicMock()
        mock_logger.body_logging_enabled = False
        client = _AsyncHttpClient(retries=2, backoff=0, session=session, logger=mock_logger)
        with patch("asyncio.sleep", new_callable=AsyncMock):
            await client._request("get", "https://example.com/data")
        mock_logger.log_error.assert_called_once()
