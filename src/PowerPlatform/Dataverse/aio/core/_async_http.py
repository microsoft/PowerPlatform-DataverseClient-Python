# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Async HTTP client for Dataverse using aiohttp.

Provides :class:`_AsyncHttpClient` (aiohttp-based) and :class:`_AsyncResponse`,
a synchronous-looking response wrapper that eagerly materialises the aiohttp
response body so callers can use familiar ``.status_code``, ``.text``, and
``.json()`` attributes without ``await``.
"""

from __future__ import annotations

import asyncio
import json as _json
import time
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from ...core._http_logger import _HttpLogger


class _AsyncResponse:
    """Synchronous-looking response adapter built from an eagerly-read aiohttp response.

    :param status: HTTP status code.
    :param headers: Response headers (aiohttp ``CIMultiDictProxy`` — case-insensitive).
    :param text: Full response body as a decoded string (may be empty).
    :param json_data: Parsed JSON body, or ``{}`` when the body is not valid JSON.
    """

    __slots__ = ("status_code", "headers", "text", "_json_data")

    def __init__(self, status: int, headers: Any, text: str, json_data: Any) -> None:
        self.status_code: int = status
        self.headers = headers  # CIMultiDictProxy — supports case-insensitive .get()
        self.text: str = text
        self._json_data: Any = json_data

    def json(self) -> Any:
        """Return the pre-parsed JSON body."""
        return self._json_data


class _AsyncHttpClient:
    """Async HTTP client with configurable retry logic using aiohttp.

    :param retries: Maximum number of retry attempts for transient errors. Default is 5.
    :type retries: :class:`int` | None
    :param backoff: Base delay in seconds between retry attempts. Default is 0.5.
    :type backoff: :class:`float` | None
    :param timeout: Default request timeout in seconds. If None, uses per-method defaults
        (120 s for POST/DELETE, 10 s for all other methods).
    :type timeout: :class:`float` | None
    :param session: Optional ``aiohttp.ClientSession`` to use for all requests.
        When provided the session is **not** closed by :meth:`close` (caller owns it).
        When ``None`` a session is created lazily on the first request and closed by
        :meth:`close`.
    :type session: ``aiohttp.ClientSession`` | None
    """

    def __init__(
        self,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        timeout: Optional[float] = None,
        session: Any = None,  # aiohttp.ClientSession | None
        logger: Optional["_HttpLogger"] = None,
    ) -> None:
        self.max_attempts: int = retries if retries is not None else 5
        self.base_delay: float = backoff if backoff is not None else 0.5
        self.default_timeout: Optional[float] = timeout
        self._session: Any = session  # aiohttp.ClientSession | None
        self._owns_session: bool = session is None  # True → we created it; we close it
        self._logger = logger

    async def _get_session(self) -> Any:
        """Return the active session, creating one lazily if needed."""
        if self._session is None:
            import aiohttp

            self._session = aiohttp.ClientSession()
        return self._session

    async def _request(self, method: str, url: str, **kwargs: Any) -> _AsyncResponse:
        """Execute an HTTP request with automatic retry on network errors.

        :param method: HTTP method (GET, POST, PATCH, DELETE, …).
        :param url: Target URL.
        :param kwargs: Additional arguments forwarded to ``aiohttp.ClientSession.request()``.
        :return: Eagerly-read :class:`_AsyncResponse`.
        :raises aiohttp.ClientError: If all retry attempts fail.
        :raises asyncio.TimeoutError: If all retry attempts time out.
        """
        import aiohttp

        # Resolve timeout — convert float to aiohttp.ClientTimeout
        if "timeout" not in kwargs:
            if self.default_timeout is not None:
                t = self.default_timeout
            else:
                m = (method or "").lower()
                t = 120.0 if m in ("post", "delete") else 10.0
            kwargs["timeout"] = aiohttp.ClientTimeout(total=t)
        elif isinstance(kwargs["timeout"], (int, float)):
            kwargs["timeout"] = aiohttp.ClientTimeout(total=float(kwargs.pop("timeout")))

        # Convert json= to data= (bytes) so aiohttp doesn't override Content-Type,
        # which is already set in the merged request headers.
        if "json" in kwargs:
            payload = kwargs.pop("json")
            kwargs["data"] = _json.dumps(payload, ensure_ascii=False).encode("utf-8")

        # Log outbound request once (before retry loop).
        if self._logger is not None:
            req_body = kwargs.get("data")
            self._logger.log_request(method, url, headers=kwargs.get("headers"), body=req_body)

        session = await self._get_session()

        for attempt in range(self.max_attempts):
            try:
                t0 = time.monotonic()
                async with session.request(method, url, **kwargs) as resp:
                    text = await resp.text(encoding="utf-8", errors="replace")
                    elapsed_ms = (time.monotonic() - t0) * 1000
                    try:
                        json_data: Any = _json.loads(text) if text.strip() else {}
                    except (ValueError, TypeError):
                        json_data = {}
                    if self._logger is not None:
                        resp_body = text if self._logger.body_logging_enabled else None
                        self._logger.log_response(
                            method,
                            url,
                            status_code=resp.status,
                            headers=dict(resp.headers),
                            body=resp_body,
                            elapsed_ms=elapsed_ms,
                        )
                    return _AsyncResponse(resp.status, resp.headers, text, json_data)
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

        # Unreachable — loop always raises or returns inside
        raise RuntimeError("_AsyncHttpClient._request: retry loop exhausted without result")  # pragma: no cover

    async def close(self) -> None:
        """Close the underlying aiohttp session if this client owns it."""
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None
