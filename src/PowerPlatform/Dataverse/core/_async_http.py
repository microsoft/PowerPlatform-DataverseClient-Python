# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Async HTTP client with automatic retry logic and timeout handling.

This module provides :class:`~PowerPlatform.Dataverse.core._async_http._AsyncHttpClient`, a
wrapper around ``aiohttp`` that mirrors the interface of
:class:`~PowerPlatform.Dataverse.core._http._HttpClient` with full async/await support.
"""

from __future__ import annotations

import asyncio
import json as _json
from typing import Any, Optional


class _AsyncResponse:
    """
    Minimal async-compatible response wrapper that mirrors the ``requests.Response`` interface.

    The response body is eagerly read so that ``.json()`` and ``.text`` behave synchronously,
    matching the pattern used throughout :class:`~PowerPlatform.Dataverse.data._odata._ODataClient`.

    :param status_code: HTTP status code.
    :type status_code: :class:`int`
    :param headers: Response headers (case-insensitive mapping).
    :param body_bytes: Raw response body bytes.
    :type body_bytes: :class:`bytes`
    """

    def __init__(self, status_code: int, headers: Any, body_bytes: bytes) -> None:
        self.status_code = status_code
        self.headers = headers
        self._body_bytes = body_bytes

    @property
    def text(self) -> str:
        """Decoded response body as a string (UTF-8 with replacement)."""
        return self._body_bytes.decode("utf-8", errors="replace")

    def json(self) -> Any:
        """Parse response body as JSON.

        :return: Parsed JSON value.
        :raises ValueError: If the body is not valid JSON.
        """
        return _json.loads(self._body_bytes)


class _AsyncHttpClient:
    """
    Async HTTP client with configurable retry logic and timeout handling.

    Uses ``aiohttp`` for non-blocking HTTP requests with the same retry semantics
    as :class:`~PowerPlatform.Dataverse.core._http._HttpClient`.

    :param retries: Maximum number of retry attempts for transient errors. Default is 5.
    :type retries: :class:`int` | None
    :param backoff: Base delay in seconds between retry attempts. Default is 0.5.
    :type backoff: :class:`float` | None
    :param timeout: Default request timeout in seconds. If None, uses per-method defaults.
    :type timeout: :class:`float` | None
    """

    def __init__(
        self,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        timeout: Optional[float] = None,
    ) -> None:
        self.max_attempts = retries if retries is not None else 5
        self.base_delay = backoff if backoff is not None else 0.5
        self.default_timeout: Optional[float] = timeout
        self._session: Any = None  # aiohttp.ClientSession, created lazily

    async def _ensure_session(self) -> Any:
        """Create the ``aiohttp.ClientSession`` if not already open."""
        try:
            import aiohttp
        except ImportError as exc:
            raise ImportError(
                "aiohttp is required for async Dataverse operations. "
                "Install it with: pip install 'PowerPlatform-Dataverse-Client[async]'"
            ) from exc
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _request(self, method: str, url: str, **kwargs: Any) -> _AsyncResponse:
        """
        Execute an async HTTP request with automatic retry logic and timeout management.

        Applies default timeouts based on HTTP method (120s for POST/DELETE, 10s for others)
        and retries on network errors with exponential backoff.

        :param method: HTTP method (GET, POST, PUT, DELETE, PATCH, etc.).
        :type method: :class:`str`
        :param url: Target URL for the request.
        :type url: :class:`str`
        :param kwargs: Additional arguments forwarded to ``aiohttp.ClientSession.request()``.
        :return: Async-compatible response with eagerly read body.
        :rtype: ~PowerPlatform.Dataverse.core._async_http._AsyncResponse
        :raises aiohttp.ClientError: If all retry attempts fail.
        """
        try:
            import aiohttp
        except ImportError as exc:
            raise ImportError(
                "aiohttp is required for async Dataverse operations. "
                "Install it with: pip install 'PowerPlatform-Dataverse-Client[async]'"
            ) from exc

        if "timeout" not in kwargs:
            if self.default_timeout is not None:
                kwargs["timeout"] = aiohttp.ClientTimeout(total=self.default_timeout)
            else:
                m = (method or "").lower()
                default_secs = 120 if m in ("post", "delete") else 10
                kwargs["timeout"] = aiohttp.ClientTimeout(total=default_secs)

        # aiohttp uses `data` for binary body; requests uses `data` too — compatible.
        # aiohttp uses `params` for query params — same as requests.
        # aiohttp uses `headers` — same as requests.
        # aiohttp uses `json` — same as requests.
        # `verify_ssl` vs `ssl` — we let aiohttp default (verify certs).

        session = await self._ensure_session()
        last_error: Optional[Exception] = None
        for attempt in range(self.max_attempts):
            try:
                async with session.request(method, url, **kwargs) as resp:
                    body_bytes = await resp.read()
                    return _AsyncResponse(
                        status_code=resp.status,
                        headers=resp.headers,
                        body_bytes=body_bytes,
                    )
            except aiohttp.ClientError as exc:
                last_error = exc
                if attempt == self.max_attempts - 1:
                    raise
                delay = self.base_delay * (2**attempt)
                await asyncio.sleep(delay)
        # Should not reach here, but satisfy the type checker
        raise RuntimeError("Unexpected exit from retry loop") from last_error

    async def close(self) -> None:
        """Close the underlying ``aiohttp.ClientSession`` and release resources.

        Safe to call multiple times.
        """
        if self._session is not None and not self._session.closed:
            await self._session.close()
            self._session = None
