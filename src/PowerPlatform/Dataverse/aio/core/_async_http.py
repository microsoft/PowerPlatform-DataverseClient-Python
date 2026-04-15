# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Async HTTP client for Dataverse using aiohttp.

Provides :class:`_AsyncHttpClient` (aiohttp-based) and :class:`_AsyncResponse`,
a synchronous-looking response wrapper that mirrors the interface of
``requests.Response`` used by the sync :class:`~PowerPlatform.Dataverse.core._http._HttpClient`.
"""

from __future__ import annotations

import asyncio
import json as _json
import time
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    import aiohttp
    from ...core._http_logger import _HttpLogger


class _AsyncResponse:
    """Synchronous-looking response adapter built from an eagerly-read aiohttp response.

    :param status: HTTP status code.
    :param headers: Response headers (aiohttp ``CIMultiDictProxy`` — case-insensitive).
    :param text: Response body decoded as UTF-8 (with replacement characters).
    """

    __slots__ = ("status_code", "headers", "text")

    def __init__(self, status: int, headers: Any, text: str) -> None:
        self.status_code: int = status
        self.headers = headers
        self.text: str = text

    def json(self) -> Any:
        """Parse response body as JSON.

        :raises json.JSONDecodeError: If the body is not valid JSON.
        """
        return _json.loads(self.text)


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
        When ``None`` a session is created lazily on the first request.
        :meth:`close` closes whatever session is held, whether provided or lazily created.
    :type session: ``aiohttp.ClientSession`` | None
    """

    def __init__(
        self,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        timeout: Optional[float] = None,
        session: Optional["aiohttp.ClientSession"] = None,
        logger: Optional["_HttpLogger"] = None,
    ) -> None:
        self.max_attempts: int = max(1, retries if retries is not None else 5)
        self.base_delay: float = backoff if backoff is not None else 0.5
        self.default_timeout: Optional[float] = timeout
        self._session: Optional["aiohttp.ClientSession"] = session
        self._logger = logger

    async def _get_session(self) -> "aiohttp.ClientSession":
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

        # If no timeout is provided, use the user-specified default timeout if set;
        # otherwise, apply per-method defaults (120s for POST/DELETE, 10s for others).
        # Convert the resolved float to aiohttp.ClientTimeout.
        if "timeout" not in kwargs:
            if self.default_timeout is not None:
                t = self.default_timeout
            else:
                http_method = (method or "").lower()
                t = 120.0 if http_method in ("post", "delete") else 10.0
            kwargs["timeout"] = aiohttp.ClientTimeout(total=t)
        elif isinstance(kwargs["timeout"], (int, float)):
            kwargs["timeout"] = aiohttp.ClientTimeout(total=float(kwargs.pop("timeout")))

        # Convert json= to data= (bytes) to prevent aiohttp from overriding the
        # Content-Type header that the OData layer already set in kwargs["headers"]
        # (e.g. "application/json; odata.metadata=minimal").
        if "json" in kwargs:
            payload = kwargs.pop("json")
            kwargs["data"] = _json.dumps(payload, ensure_ascii=False).encode("utf-8")

        # Log outbound request once (before retry loop).
        # Use explicit key presence checks so falsy values (e.g. {}) are logged correctly.
        if self._logger is not None:
            req_body = kwargs.get("data")
            self._logger.log_request(method, url, headers=kwargs.get("headers"), body=req_body)

        session = await self._get_session()

        # Small backoff retry on network errors only
        for attempt in range(self.max_attempts):
            try:
                t0 = time.monotonic()
                async with session.request(method, url, **kwargs) as resp:
                    text = await resp.text(encoding="utf-8", errors="replace")
                    elapsed_ms = (time.monotonic() - t0) * 1000
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
                    return _AsyncResponse(resp.status, resp.headers, text)
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

    async def close(self) -> None:
        """Close the underlying aiohttp session if one exists."""
        if self._session is not None:
            await self._session.close()
            self._session = None
