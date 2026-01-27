# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
HTTP client with automatic retry logic, timeout handling, and optional session support.

This module provides :class:`~PowerPlatform.Dataverse.core._http._HttpClient`, a wrapper
around the requests library that adds configurable retry behavior for transient
network errors, intelligent timeout management based on HTTP method types, and
optional connection pooling via session reuse.
"""

from __future__ import annotations

import time
from typing import Any, Optional

import requests


class _HttpClient:
    """
    HTTP client with configurable retry logic, timeout handling, and optional session support.

    Provides automatic retry behavior for transient failures, default timeout
    management for different HTTP methods, and optional connection pooling
    when initialized with a session.

    :param retries: Maximum number of retry attempts for transient errors. Default is 5.
    :type retries: :class:`int` | None
    :param backoff: Base delay in seconds between retry attempts. Default is 0.5.
    :type backoff: :class:`float` | None
    :param timeout: Default request timeout in seconds. If None, uses per-method defaults.
    :type timeout: :class:`float` | None
    :param session: Optional requests.Session for connection pooling. If provided,
        all requests use this session for efficient connection reuse.
    :type session: :class:`requests.Session` | None
    """

    def __init__(
        self,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        timeout: Optional[float] = None,
        session: Optional[requests.Session] = None,
    ) -> None:
        self.max_attempts = retries if retries is not None else 5
        self.base_delay = backoff if backoff is not None else 0.5
        self.default_timeout: Optional[float] = timeout
        self._session = session

    def _request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        """
        Execute an HTTP request with automatic retry logic and timeout management.

        Applies default timeouts based on HTTP method (120s for POST/DELETE, 10s for others)
        and retries on network errors with exponential backoff. When a session is configured,
        uses the session for connection pooling; otherwise uses standalone requests.

        :param method: HTTP method (GET, POST, PUT, DELETE, etc.).
        :type method: :class:`str`
        :param url: Target URL for the request.
        :type url: :class:`str`
        :param kwargs: Additional arguments passed to ``requests.request()`` or
            ``session.request()``, including headers, data, etc.
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

        # Small backoff retry on network errors only
        for attempt in range(self.max_attempts):
            try:
                if self._session is not None:
                    return self._session.request(method, url, **kwargs)
                return requests.request(method, url, **kwargs)
            except requests.exceptions.RequestException:
                if attempt == self.max_attempts - 1:
                    raise
                delay = self.base_delay * (2**attempt)
                time.sleep(delay)
                continue

    def close(self) -> None:
        """
        Close the HTTP client and release resources.

        If a session was provided, this method closes it. Safe to call multiple times.
        After closing, the client should not be used for further requests.
        """
        if self._session is not None:
            self._session.close()
            self._session = None
