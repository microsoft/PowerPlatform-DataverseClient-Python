# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for _AsyncHttpClient and _AsyncResponse."""

import json
import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

from PowerPlatform.Dataverse.aio.core._async_http import _AsyncHttpClient, _AsyncResponse


class TestAsyncResponse:
    """Tests for _AsyncResponse data access."""

    def test_status_code(self):
        """status_code is stored and returned correctly."""
        r = _AsyncResponse(200, {}, "")
        assert r.status_code == 200

    def test_headers(self):
        """headers is the exact dict passed at construction."""
        h = {"Content-Type": "application/json"}
        r = _AsyncResponse(200, h, "")
        assert r.headers is h

    def test_text(self):
        """text is the raw string body passed at construction."""
        r = _AsyncResponse(200, {}, "hello")
        assert r.text == "hello"

    def test_json(self):
        """json() deserializes a valid JSON text body."""
        data = {"k": "v"}
        r = _AsyncResponse(200, {}, json.dumps(data))
        assert r.json() == data

    def test_json_empty_raises(self):
        """json() raises JSONDecodeError on an empty body."""
        r = _AsyncResponse(204, {}, "")
        with pytest.raises(json.JSONDecodeError):
            r.json()

    def test_json_non_json_raises(self):
        """json() raises JSONDecodeError on a non-JSON body."""
        r = _AsyncResponse(200, {}, "multipart/mixed content")
        with pytest.raises(json.JSONDecodeError):
            r.json()


class TestAsyncHttpClientConstruction:
    """Tests for _AsyncHttpClient constructor defaults and session management."""

    def test_defaults(self):
        """Default client has 5 retries, 0.5s backoff, no timeout, and no session."""
        c = _AsyncHttpClient()
        assert c.max_attempts == 5
        assert c.base_delay == 0.5
        assert c.default_timeout is None
        assert c._session is None

    def test_external_session_stored(self):
        """An externally provided session is stored on the client."""
        session = MagicMock()
        c = _AsyncHttpClient(session=session)
        assert c._session is session

    def test_custom_params(self):
        """Constructor arguments are stored as max_attempts, base_delay, and default_timeout."""
        c = _AsyncHttpClient(retries=3, backoff=1.0, timeout=30.0)
        assert c.max_attempts == 3
        assert c.base_delay == 1.0
        assert c.default_timeout == 30.0

    async def test_get_session_lazy_create(self):
        """_get_session creates an aiohttp.ClientSession lazily on first call."""
        c = _AsyncHttpClient()
        mock_sess = MagicMock()
        with patch("aiohttp.ClientSession", return_value=mock_sess):
            sess = await c._get_session()
        assert sess is mock_sess

    async def test_get_session_reuses_existing(self):
        """_get_session returns the existing session without creating a new one."""
        existing = MagicMock()
        c = _AsyncHttpClient(session=existing)
        assert await c._get_session() is existing


class TestAsyncHttpClientClose:
    """Tests for _AsyncHttpClient.close session lifecycle."""

    async def test_close_lazily_created_session(self):
        """close() awaits session.close() and clears the session reference."""
        mock_sess = AsyncMock()
        c = _AsyncHttpClient()
        c._session = mock_sess
        await c.close()
        mock_sess.close.assert_awaited_once()
        assert c._session is None

    async def test_close_external_session(self):
        """close() closes an externally provided session and clears the reference."""
        mock_sess = AsyncMock()
        c = _AsyncHttpClient(session=mock_sess)
        await c.close()
        mock_sess.close.assert_awaited_once()
        assert c._session is None

    async def test_close_no_session_noop(self):
        """close() is safe to call when no session has been created."""
        c = _AsyncHttpClient()
        await c.close()


def _fake_session(status=200, body="{}"):
    resp = AsyncMock()
    resp.status = status
    resp.headers = {}
    resp.text = AsyncMock(return_value=body)
    captured = {}

    @asynccontextmanager
    async def _req(method, url, **kwargs):
        captured["method"] = method
        captured["url"] = url
        captured.update(kwargs)
        yield resp

    sess = MagicMock()
    sess.request = _req
    return sess, captured


class TestAsyncHttpClientRequest:
    """Tests for _AsyncHttpClient._request timeout, JSON serialization, and response handling."""

    async def test_returns_response_200(self):
        """A successful request returns an _AsyncResponse with the correct status code."""
        sess, _ = _fake_session(200, '{"value": []}')
        c = _AsyncHttpClient(session=sess)
        r = await c._request("GET", "https://x.com")
        assert isinstance(r, _AsyncResponse)
        assert r.status_code == 200

    async def test_post_timeout_120(self):
        """POST requests use 120s default when no timeout is specified."""
        sess, cap = _fake_session()
        c = _AsyncHttpClient(session=sess)
        await c._request("post", "https://x.com")
        assert cap["timeout"].total == 120.0

    async def test_delete_timeout_120(self):
        """DELETE requests use 120s default when no timeout is specified."""
        sess, cap = _fake_session()
        c = _AsyncHttpClient(session=sess)
        await c._request("delete", "https://x.com")
        assert cap["timeout"].total == 120.0

    async def test_get_timeout_10(self):
        """GET requests use 10s default when no timeout is specified."""
        sess, cap = _fake_session()
        c = _AsyncHttpClient(session=sess)
        await c._request("get", "https://x.com")
        assert cap["timeout"].total == 10.0

    async def test_global_timeout_overrides(self):
        """Explicit default_timeout on the client overrides per-method defaults."""
        sess, cap = _fake_session()
        c = _AsyncHttpClient(timeout=5.0, session=sess)
        await c._request("get", "https://x.com")
        assert cap["timeout"].total == 5.0

    async def test_float_timeout_kwarg_converted(self):
        """A float timeout kwarg is converted to an aiohttp.ClientTimeout."""
        import aiohttp

        sess, cap = _fake_session()
        c = _AsyncHttpClient(session=sess)
        await c._request("get", "https://x.com", timeout=45.0)
        assert isinstance(cap["timeout"], aiohttp.ClientTimeout)
        assert cap["timeout"].total == 45.0

    async def test_explicit_timeout_kwarg_takes_precedence(self):
        """If timeout is already in kwargs it is passed through, overriding the global default."""
        sess, cap = _fake_session()
        c = _AsyncHttpClient(timeout=30.0, session=sess)
        await c._request("get", "https://x.com", timeout=5.0)
        assert cap["timeout"].total == 5.0

    async def test_json_kwarg_to_data_bytes(self):
        """json= payload is serialized to UTF-8 bytes and sent as data= to preserve Content-Type."""
        sess, cap = _fake_session(201, "{}")
        c = _AsyncHttpClient(session=sess)
        payload = {"name": "X"}
        await c._request("POST", "https://x.com", json=payload)
        assert "json" not in cap
        assert json.loads(cap["data"].decode()) == payload

    async def test_empty_body_json_raises(self):
        """json() raises JSONDecodeError on an empty response body."""
        sess, _ = _fake_session(204, b"")
        c = _AsyncHttpClient(session=sess)
        r = await c._request("DELETE", "https://x.com")
        with pytest.raises(json.JSONDecodeError):
            r.json()

    async def test_non_json_body_json_raises(self):
        """json() raises JSONDecodeError on a non-JSON response body."""
        sess, _ = _fake_session(200, b"not json")
        c = _AsyncHttpClient(session=sess)
        r = await c._request("GET", "https://x.com")
        with pytest.raises(json.JSONDecodeError):
            r.json()

    async def test_multipart_body_text_accessible(self):
        """Multipart response bodies are returned as raw text via .text."""
        body = "--batchresponse_123\r\nContent-Type: application/http\r\n\r\nHTTP/1.1 200 OK\r\n"
        sess, _ = _fake_session(200, body)
        c = _AsyncHttpClient(session=sess)
        r = await c._request("POST", "https://x.com")
        assert r.text.startswith("--batchresponse_123")


class TestAsyncHttpClientRetry:
    """Tests for retry behavior on network errors."""

    async def test_retries_on_network_error(self):
        """Retries after a ClientConnectionError and returns the response on eventual success."""
        import aiohttp

        count = 0
        ok = AsyncMock()
        ok.status = 200
        ok.headers = {}
        ok.text = AsyncMock(return_value="{}")

        @asynccontextmanager
        async def _flaky(method, url, **kw):
            nonlocal count
            count += 1
            if count < 3:
                raise aiohttp.ClientConnectionError("err")
            yield ok

        sess = MagicMock()
        sess.request = _flaky
        c = _AsyncHttpClient(retries=5, backoff=0.0, session=sess)
        r = await c._request("GET", "https://x.com")
        assert r.status_code == 200
        assert count == 3

    async def test_raises_after_max_retries(self):
        """Raises ClientError after all retry attempts fail."""
        import aiohttp

        @asynccontextmanager
        async def _fail(method, url, **kw):
            raise aiohttp.ClientConnectionError("perm")
            yield

        sess = MagicMock()
        sess.request = _fail
        c = _AsyncHttpClient(retries=2, backoff=0.0, session=sess)
        with pytest.raises(aiohttp.ClientError):
            await c._request("GET", "https://x.com")

    async def test_backoff_delay_between_retries(self):
        """Sleeps with exponential backoff between retry attempts."""
        import aiohttp

        count = 0
        ok = AsyncMock()
        ok.status = 200
        ok.headers = {}
        ok.text = AsyncMock(return_value="{}")

        @asynccontextmanager
        async def _flaky(method, url, **kw):
            nonlocal count
            count += 1
            if count < 3:
                raise aiohttp.ClientConnectionError("err")
            yield ok

        sess = MagicMock()
        sess.request = _flaky
        c = _AsyncHttpClient(retries=5, backoff=1.0, session=sess)
        with patch("PowerPlatform.Dataverse.aio.core._async_http.asyncio.sleep") as mock_sleep:
            await c._request("GET", "https://x.com")
        # First retry: 1.0 * 2^0 = 1.0, second retry: 1.0 * 2^1 = 2.0
        mock_sleep.assert_awaited()
        calls = [c.args[0] for c in mock_sleep.await_args_list]
        assert calls == [1.0, 2.0]
