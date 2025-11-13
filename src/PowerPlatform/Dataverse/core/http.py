# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
HTTP client with automatic retry logic and timeout handling.

This module provides :class:`HttpClient`, a wrapper around the requests library
that adds configurable retry behavior for transient network errors and
intelligent timeout management based on HTTP method types.
"""

from __future__ import annotations

import random
import time
from typing import Any, Optional

import requests


class HttpClient:
    """
    HTTP client with configurable retry logic and timeout handling.
    
    Provides automatic retry behavior for transient failures and default timeout
    management for different HTTP methods.
    
    :param retries: Maximum number of retry attempts for transient errors. Default is 5.
    :type retries: int or None
    :param backoff: Base delay in seconds between retry attempts. Default is 0.5.
    :type backoff: float or None
    :param timeout: Default request timeout in seconds. If None, uses per-method defaults.
    :type timeout: float or None
    """
    
    def __init__(
        self,
        *,
        retries: Optional[int] = None,
        backoff: Optional[float] = None,
        timeout: Optional[float] = None,
        max_backoff: Optional[float] = None,
        jitter: bool = True,
        retry_transient_errors: bool = True,
    ) -> None:
        self.max_attempts = retries if retries is not None else 5
        self.base_delay = backoff if backoff is not None else 0.5
        self.max_backoff = max_backoff if max_backoff is not None else 60.0
        self.default_timeout: Optional[float] = timeout
        self.jitter = jitter
        self.retry_transient_errors = retry_transient_errors
        
        # Transient HTTP status codes that should be retried
        self.transient_status_codes = {429, 502, 503, 504}

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        """
        Execute an HTTP request with automatic retry logic and timeout management.
        
        Applies default timeouts based on HTTP method (120s for POST/DELETE, 10s for others)
        and retries on transient network errors and HTTP status codes with exponential backoff.
        
        :param method: HTTP method (GET, POST, PUT, DELETE, etc.).
        :type method: str
        :param url: Target URL for the request.
        :type url: str
        :param kwargs: Additional arguments passed to ``requests.request()``, including headers, data, etc.
        :return: HTTP response object.
        :rtype: requests.Response
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

        # Enhanced retry logic with exponential backoff, jitter, and HTTP status retries
        for attempt in range(self.max_attempts):
            try:
                response = requests.request(method, url, **kwargs)
                
                # Check if we should retry based on HTTP status code
                if (self.retry_transient_errors and 
                    response.status_code in self.transient_status_codes and
                    attempt < self.max_attempts - 1):
                    
                    delay = self._calculate_retry_delay(attempt, response)
                    time.sleep(delay)
                    continue
                
                return response
                
            except requests.exceptions.RequestException:
                if attempt == self.max_attempts - 1:
                    raise
                delay = self._calculate_retry_delay(attempt)
                time.sleep(delay)
                
        # This should never be reached due to the logic above
        raise RuntimeError("Unexpected end of retry loop")
    
    def _calculate_retry_delay(self, attempt: int, response: Optional[requests.Response] = None) -> float:
        """
        Calculate the delay before the next retry attempt using exponential backoff with jitter.

        This method implements an intelligent retry delay strategy that prioritizes server-provided
        guidance (Retry-After header) over client-calculated delays, uses exponential backoff to
        reduce load on failing services, and applies jitter to prevent thundering herd problems
        when multiple clients retry simultaneously.

        The delay calculation follows this priority order:
        1. **Retry-After header**: If present and valid, use the server-specified delay (capped at max_backoff)
        2. **Exponential backoff**: Calculate base_delay * (2^attempt), capped at max_backoff  
        3. **Jitter**: If enabled, add ±25% random variation to prevent synchronized retries

        :param attempt: Zero-based retry attempt number (0 = first retry, 1 = second retry, etc.).
        :type attempt: int
        :param response: Optional HTTP response object containing headers. Used to extract Retry-After
            header when available. If None, only exponential backoff calculation is performed.
        :type response: requests.Response or None

        :return: Delay in seconds before the next retry attempt. Always >= 0, capped at max_backoff.
        :rtype: float

        :raises: Does not raise exceptions. Invalid Retry-After values fall back to exponential backoff.

        .. note::
            **Retry-After Header Handling (RFC 7231):**
            
            - Supports integer seconds format: ``Retry-After: 120`` 
            - Invalid formats (non-integer, HTTP-date) fall back to exponential backoff
            - Server-provided delays are capped at ``max_backoff`` to prevent excessive waits

        .. note::
            **Exponential Backoff Formula:**
            
            - Base calculation: ``delay = base_delay * (2^attempt)``
            - Example with ``base_delay=0.5``: 0.5s, 1.0s, 2.0s, 4.0s, 8.0s, 16.0s...
            - Always capped at ``max_backoff`` (default 60s) to prevent unbounded delays

        .. note::
            **Jitter Application:**
            
            - When ``jitter=True``, adds random variation of ±25% of calculated delay
            - Prevents thundering herd when multiple clients retry simultaneously  
            - Example: 4.0s delay becomes random value between 3.0s and 5.0s
            - Final result is always >= 0 even with negative jitter

        Example:
            Calculate delays for successive retry attempts::

                client = HttpClient(backoff=1.0, max_backoff=30.0, jitter=True)
                
                # Exponential backoff examples (without jitter for predictable values)
                client_no_jitter = HttpClient(backoff=0.5, jitter=False)
                
                # Attempt 0: 0.5s (0.5 * 2^0)
                delay0 = client_no_jitter._calculate_retry_delay(0)
                
                # Attempt 2: 2.0s (0.5 * 2^2) 
                delay2 = client_no_jitter._calculate_retry_delay(2)
                
                # With jitter enabled, delays vary randomly within ±25%
                # Attempt 1 with jitter: ~1.0s ± 0.25s = 0.75s - 1.25s
                delay_with_jitter = client._calculate_retry_delay(1)
        """
        
        # Check for Retry-After header (RFC 7231)
        if response and "Retry-After" in response.headers:
            try:
                retry_after = int(response.headers["Retry-After"])
                # Respect Retry-After but cap it at max_backoff
                return min(retry_after, self.max_backoff)
            except (ValueError, TypeError):
                # If Retry-After is not a valid integer, fall back to exponential backoff
                pass
        
        # Exponential backoff: base_delay * (2^attempt)
        delay = self.base_delay * (2 ** attempt)
        
        # Cap the delay at max_backoff
        delay = min(delay, self.max_backoff)
        
        # Add jitter to prevent thundering herd (±25% of delay)
        if self.jitter:
            jitter_range = delay * 0.25
            jitter_offset = random.uniform(-jitter_range, jitter_range)
            delay = max(0, delay + jitter_offset)
        
        return delay
