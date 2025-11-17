# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
HTTP client with automatic retry logic and timeout handling.

This module provides :class:`~PowerPlatform.Dataverse.core.http.HttpClient`, a wrapper
around the requests library that adds configurable retry behavior for transient
network errors and intelligent timeout management based on HTTP method types.
"""

from __future__ import annotations

import time
from typing import Any, Optional

import requests


class HttpClient:
    """
    HTTP client with configurable retry logic and timeout handling.

    Provides automatic retry behavior for transient failures and default timeout
    management for different HTTP methods.

    :param retries: Maximum number of retry attempts for transient errors. Default is 5.
    :type retries: ``int`` | ``None``
    :param backoff: Base delay in seconds between retry attempts. Default is 0.5.
    :type backoff: ``float`` | ``None``
    :param timeout: Default request timeout in seconds. If None, uses per-method defaults.
    :type timeout: ``float`` | ``None``
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

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        """
        Execute an HTTP request with automatic retry logic and timeout management.

        Applies default timeouts based on HTTP method (120s for POST/DELETE, 10s for others)
        and retries on network errors with exponential backoff.

        :param method: HTTP method (GET, POST, PUT, DELETE, etc.).
        :type method: ``str``
        :param url: Target URL for the request.
        :type url: ``str``
        :param kwargs: Additional arguments passed to ``requests.request()``, including headers, data, etc.
        :return: HTTP response object.
        :rtype: ``requests.Response``
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
                return requests.request(method, url, **kwargs)
            except requests.exceptions.RequestException:
                if attempt == self.max_attempts - 1:
                    raise
                delay = self.base_delay * (2**attempt)
                time.sleep(delay)
                continue
