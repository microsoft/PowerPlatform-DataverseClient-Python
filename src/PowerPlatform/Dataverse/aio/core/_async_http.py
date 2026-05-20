# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Async HTTP client with automatic retry logic and timeout handling.

This module provides :class:`~PowerPlatform.Dataverse.aio.core._async_http._AsyncHttpClient`,
a wrapper around the aiohttp library that adds configurable retry behavior for transient
network errors and intelligent timeout management based on HTTP method types.
"""

from __future__ import annotations

import asyncio
import json as _json
import time
from typing import TYPE_CHECKING, Any, Dict, Optional

import aiohttp

if TYPE_CHECKING:
    from ...core._http_logger import _HttpLogger


class _AsyncResponse:
    """Materialized HTTP response returned by :class:`_AsyncHttpClient._request`.

    The body is fully buffered before this object is constructed, so all
    accessors are synchronous — no ``await`` required.

    :param status: HTTP status code.
    :param headers: Response headers as a plain dict.
    :param body: Raw response body bytes.
    """

    __slots__ = ("status", "status_code", "headers", "_body")

    def __init__(self, status: int, headers: Dict[str, str], body: bytes) -> None:
        self.status = status
        self.status_code = status
        self.headers = headers
        self._body = body

    @property
    def text(self) -> str:
        """Response body decoded as UTF-8 text."""
        return self._body.decode("utf-8", errors="replace") if self._body else ""

    def json(self, content_type: Any = None) -> Any:
        """Parse and return the response body as JSON."""
        return _json.loads(self._body) if self._body else {}


class _AsyncHttpClient:
    """
    Async HTTP client with configurable retry logic and timeout handling.

    Provides automatic retry behavior for transient failures and default timeout
    management for different HTTP methods.

    :param retries: Maximum number of retry attempts for transient errors. Default is 5.
    :type retries: :class:`int` | None
    :param backoff: Base delay in seconds between retry attempts. Default is 0.5.
    :type backoff: :class:`float` | None
    :param timeout: Default request timeout in seconds. If None, uses per-method defaults.
    :type timeout: :class:`float` | None
    :param session: ``aiohttp.ClientSession`` for HTTP connection pooling.
        The session is owned by the caller (``AsyncDataverseClient``) and must remain
        open for the lifetime of this client. Unlike the sync client, there is no
        per-request fallback — a session must always be provided before making requests.
    :type session: :class:`aiohttp.ClientSession` | None
    :param logger: Optional HTTP diagnostics logger. When provided, all requests,
        responses, and transport errors are logged with automatic header redaction.
    :type logger: ~PowerPlatform.Dataverse.core._http_logger._HttpLogger | None
    """

    def __init__(
        self,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        timeout: Optional[float] = None,
        session: Optional[aiohttp.ClientSession] = None,
        logger: Optional["_HttpLogger"] = None,
    ) -> None:
        self.max_attempts = retries if retries is not None else 5
        self.base_delay = backoff if backoff is not None else 0.5
        self.default_timeout: Optional[float] = timeout
        self._session = session
        self._logger = logger

    async def _request(self, method: str, url: str, **kwargs: Any) -> _AsyncResponse:
        """
        Execute an HTTP request asynchronously with automatic retry logic and timeout management.

        Applies default timeouts based on HTTP method (120s for POST/DELETE, 10s for others)
        and retries on network errors with exponential backoff.

        The response body is fully buffered and returned as a :class:`_AsyncResponse` whose
        accessors (``.text``, ``.json()``) are synchronous — no ``await`` required on the caller side.

        :param method: HTTP method (GET, POST, PUT, DELETE, etc.).
        :type method: :class:`str`
        :param url: Target URL for the request.
        :type url: :class:`str`
        :param kwargs: Additional arguments passed to ``aiohttp.ClientSession.request()``,
            including headers, data, etc.
        :return: Materialized HTTP response with body fully buffered.
        :rtype: :class:`_AsyncResponse`
        :raises aiohttp.ClientError: If all retry attempts fail.
        :raises RuntimeError: If no session has been set.
        """
        if self._session is None:
            raise RuntimeError("No aiohttp.ClientSession set. Set _session before making requests.")

        # If no timeout is provided, use the user-specified default timeout if set;
        # otherwise, apply per-method defaults (120s for POST/DELETE, 10s for others).
        if "timeout" not in kwargs:
            if self.default_timeout is not None:
                t = self.default_timeout
            else:
                m = (method or "").lower()
                t = 120 if m in ("post", "delete") else 10
            kwargs["timeout"] = aiohttp.ClientTimeout(total=t)

        # Log outbound request once (before retry loop).
        # Use explicit key presence checks so falsy values (e.g. {}) are logged correctly.
        if self._logger is not None:
            if "json" in kwargs:
                req_body = kwargs["json"]
            elif "data" in kwargs:
                req_body = kwargs["data"]
            else:
                req_body = None
            self._logger.log_request(
                method,
                url,
                headers=kwargs.get("headers"),
                body=req_body,
            )

        # Small backoff retry on network errors only
        for attempt in range(self.max_attempts):
            try:
                t0 = time.monotonic()
                async with self._session.request(method, url, **kwargs) as resp:
                    body = await resp.read()
                    response = _AsyncResponse(resp.status, dict(resp.headers), body)
                elapsed_ms = (time.monotonic() - t0) * 1000

                if self._logger is not None:
                    # Only decode text when body logging is enabled — avoids
                    # unnecessary overhead for large payloads when max_body_bytes == 0.
                    resp_body = response.text if self._logger.body_logging_enabled else None
                    self._logger.log_response(
                        method,
                        url,
                        status_code=response.status,
                        headers=response.headers,
                        body=resp_body,
                        elapsed_ms=elapsed_ms,
                    )
                return response
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if self._logger is not None:
                    self._logger.log_error(
                        method,
                        url,
                        exc,
                        attempt=attempt + 1,
                        max_attempts=self.max_attempts,
                    )
                if attempt == self.max_attempts - 1:
                    raise
                delay = self.base_delay * (2**attempt)
                await asyncio.sleep(delay)
                continue

    async def close(self) -> None:
        """Close the HTTP client and release resources.

        If a session was provided, closes it. Safe to call multiple times.
        """
        if self._session is not None:
            await self._session.close()
            self._session = None
