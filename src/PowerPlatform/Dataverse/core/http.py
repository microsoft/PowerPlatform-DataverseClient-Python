# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from __future__ import annotations

import random
import time
from typing import Any, Optional

import requests


class HttpClient:
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
        # Apply per-method default timeouts if not provided
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
                continue
                
        # This should never be reached due to the logic above
        raise RuntimeError("Unexpected end of retry loop")
    
    def _calculate_retry_delay(self, attempt: int, response: Optional[requests.Response] = None) -> float:
        """Calculate retry delay with exponential backoff, optional jitter, and Retry-After header support."""
        
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
