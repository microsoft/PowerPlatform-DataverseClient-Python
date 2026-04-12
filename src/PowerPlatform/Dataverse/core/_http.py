# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
HTTP client with automatic retry logic and timeout handling.

This module provides :class:`~PowerPlatform.Dataverse.core._http._HttpClient`, a wrapper
around the requests library that adds configurable retry behavior for transient
network errors and intelligent timeout management based on HTTP method types.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Optional

import requests

if TYPE_CHECKING:
    from ._http_logger import _HttpLogger


class _HttpClient:
    """
    HTTP client with configurable retry logic and timeout handling.

    Provides automatic retry behavior for transient failures and default timeout
    management for different HTTP methods.

    :param retries: Maximum number of retry attempts for transient errors. Default is 5.
    :type retries: :class:`int` | None
    :param backoff: Base delay in seconds between retry attempts. Default is 0.5.
    :type backoff: :class:`float` | None
    :param timeout: Default request timeout in seconds. If None, uses per-method defaults.
    :type timeout: :class:`float` | None
    :param session: Optional ``requests.Session`` for HTTP connection pooling.
        When provided, all requests use this session (enabling TCP/TLS reuse).
        When ``None``, each request uses ``requests.request()`` directly.
    :type session: :class:`requests.Session` | None
    :param logger: Optional HTTP diagnostics logger. When provided, all requests,
        responses, and transport errors are logged with automatic header redaction.
    :type logger: ~PowerPlatform.Dataverse.core._http_logger._HttpLogger | None
    """

    def __init__(
        self,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        timeout: Optional[float] = None,
        session: Optional[requests.Session] = None,
        logger: Optional["_HttpLogger"] = None,
    ) -> None:
        self.max_attempts = retries if retries is not None else 5
        self.base_delay = backoff if backoff is not None else 0.5
        self.default_timeout: Optional[float] = timeout
        self._session = session
        self._logger = logger

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        """
        Execute an HTTP request with automatic retry logic and timeout management.

        Applies default timeouts based on HTTP method (120s for POST/DELETE, 10s for others)
        and retries on network errors with exponential backoff.

        :param method: HTTP method (GET, POST, PUT, DELETE, etc.).
        :type method: :class:`str`
        :param url: Target URL for the request.
        :type url: :class:`str`
        :param kwargs: Additional arguments passed to ``requests.request()``, including headers, data, etc.
        :return: HTTP response object.
        :rtype: :class:`requests.Response`
        :raises requests.exceptions.RequestException: If all retry attempts fail.
        """
        # If no timeout is provided, use the user-specified default timeout if set;
        # otherwise, apply per-method defaults (120s for POST/DELETE, 10s for others).
        if "timeout" not in kwargs:
            if self.default_timeout is not None:
                kwargs["timeout"] = self.default_timeout
            else:
                m = (method or "").lower()
                kwargs["timeout"] = 120 if m in ("post", "delete") else 10

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
        requester = self._session.request if self._session is not None else requests.request
        for attempt in range(self.max_attempts):
            try:
                t0 = time.monotonic()
                resp = requester(method, url, **kwargs)
                elapsed_ms = (time.monotonic() - t0) * 1000

                if self._logger is not None:
                    # Only decode resp.text when body logging is enabled — avoids
                    # unnecessary overhead for large payloads when max_body_bytes == 0.
                    resp_body = resp.text if self._logger._config.max_body_bytes != 0 else None
                    self._logger.log_response(
                        method,
                        url,
                        status_code=resp.status_code,
                        headers=dict(resp.headers),
                        body=resp_body,
                        elapsed_ms=elapsed_ms,
                    )
                return resp
            except requests.exceptions.RequestException as exc:
                if self._logger is not None:
                    self._logger.log_error(method, url, exc)
                if attempt == self.max_attempts - 1:
                    raise
                delay = self.base_delay * (2**attempt)
                time.sleep(delay)
                continue

    def close(self) -> None:
        """Close the HTTP client and release resources.

        If a session was provided, closes it. Safe to call multiple times.
        """
        if self._session is not None:
            self._session.close()
            self._session = None
