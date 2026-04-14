# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import json
import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

from PowerPlatform.Dataverse.aio.core._async_http import _AsyncHttpClient, _AsyncResponse


class TestAsyncResponse:
    def test_status_code(self):
        r = _AsyncResponse(200, {}, "", {})
        assert r.status_code == 200

    def test_headers(self):
        h = {"Content-Type": "application/json"}
        r = _AsyncResponse(200, h, "", {})
        assert r.headers is h

    def test_text(self):
        r = _AsyncResponse(200, {}, "hello", {})
        assert r.text == "hello"

    def test_json(self):
        data = {"k": "v"}
        r = _AsyncResponse(200, {}, json.dumps(data), data)
        assert r.json() == data

    def test_json_empty(self):
        r = _AsyncResponse(204, {}, "", {})
        assert r.json() == {}


class TestAsyncHttpClientConstruction:
    def test_defaults(self):
        c = _AsyncHttpClient()
        assert c.max_attempts == 5
        assert c.base_delay == 0.5
        assert c.default_timeout is None
        assert c._session is None
        assert c._owns_session is True

    def test_external_session_not_owned(self):
        session = MagicMock()
        c = _AsyncHttpClient(session=session)
        assert c._session is session
        assert c._owns_session is False

    def test_custom_params(self):
        c = _AsyncHttpClient(retries=3, backoff=1.0, timeout=30.0)
        assert c.max_attempts == 3
        assert c.base_delay == 1.0
        assert c.default_timeout == 30.0

    async def test_get_session_lazy_create(self):
        c = _AsyncHttpClient()
        mock_sess = MagicMock()
        with patch("aiohttp.ClientSession", return_value=mock_sess):
            sess = await c._get_session()
        assert sess is mock_sess

    async def test_get_session_reuses_existing(self):
        existing = MagicMock()
        c = _AsyncHttpClient(session=existing)
        assert await c._get_session() is existing


class TestAsyncHttpClientClose:
    async def test_close_owned_session(self):
        mock_sess = AsyncMock()
        c = _AsyncHttpClient()
        c._session = mock_sess
        await c.close()
        mock_sess.close.assert_awaited_once()
        assert c._session is None

    async def test_close_unowned_skips(self):
        mock_sess = AsyncMock()
        c = _AsyncHttpClient(session=mock_sess)
        await c.close()
        mock_sess.close.assert_not_awaited()

    async def test_close_no_session_noop(self):
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
    async def test_returns_response_200(self):
        sess, _ = _fake_session(200, "{\"value\": []}")
        c = _AsyncHttpClient(session=sess)
        r = await c._request("GET", "https://x.com")
        assert isinstance(r, _AsyncResponse)
        assert r.status_code == 200

    async def test_post_timeout_120(self):
        sess, cap = _fake_session()
        c = _AsyncHttpClient(session=sess)
        await c._request("post", "https://x.com")
        assert cap["timeout"].total == 120.0

    async def test_delete_timeout_120(self):
        sess, cap = _fake_session()
        c = _AsyncHttpClient(session=sess)
        await c._request("delete", "https://x.com")
        assert cap["timeout"].total == 120.0

    async def test_get_timeout_10(self):
        sess, cap = _fake_session()
        c = _AsyncHttpClient(session=sess)
        await c._request("get", "https://x.com")
        assert cap["timeout"].total == 10.0

    async def test_global_timeout_overrides(self):
        sess, cap = _fake_session()
        c = _AsyncHttpClient(timeout=5.0, session=sess)
        await c._request("get", "https://x.com")
        assert cap["timeout"].total == 5.0

    async def test_float_timeout_kwarg_converted(self):
        import aiohttp
        sess, cap = _fake_session()
        c = _AsyncHttpClient(session=sess)
        await c._request("get", "https://x.com", timeout=45.0)
        assert isinstance(cap["timeout"], aiohttp.ClientTimeout)
        assert cap["timeout"].total == 45.0

    async def test_json_kwarg_to_data_bytes(self):
        sess, cap = _fake_session(201, "{}")
        c = _AsyncHttpClient(session=sess)
        payload = {"name": "X"}
        await c._request("POST", "https://x.com", json=payload)
        assert "json" not in cap
        assert json.loads(cap["data"].decode()) == payload

    async def test_empty_body_empty_dict(self):
        sess, _ = _fake_session(204, "")
        c = _AsyncHttpClient(session=sess)
        r = await c._request("DELETE", "https://x.com")
        assert r.json() == {}

    async def test_non_json_body_raises_decode_error(self):
        sess, _ = _fake_session(200, "not json")
        c = _AsyncHttpClient(session=sess)
        with pytest.raises(json.JSONDecodeError):
            await c._request("GET", "https://x.com")


class TestAsyncHttpClientRetry:
    async def test_retries_on_network_error(self):
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