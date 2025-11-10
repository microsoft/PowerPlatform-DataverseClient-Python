# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from __future__ import annotations

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
    ) -> None:
        self.max_attempts = retries if retries is not None else 5
        self.base_delay = backoff if backoff is not None else 0.5
        self.default_timeout: Optional[float] = timeout

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        # Apply per-method default timeouts if not provided
        # Apply default timeout if not provided; fall back to per-method defaults
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
                delay = self.base_delay * (2 ** attempt)
                time.sleep(delay)
                continue
