"""HTTP transport for the FinOps SDK.

Single ``requests.Session`` per client, with:

  * Bearer token injection via the ``TokenProvider`` cache.
  * Bounded retry on transient failures (429 / 502 / 503 / 504 / 408)
    using exponential backoff and honoring ``Retry-After`` headers.
  * Mapping of non-2xx responses to the SDK exception hierarchy.

The retry shape intentionally mirrors the guidance captured in the
FinOps Platform memory bank (apiReference.md: transient SQL retry codes,
priority-based throttling, 5-minute token refresh).
"""
from __future__ import annotations

import logging
import random
import time
from typing import TYPE_CHECKING, Any, Dict, Mapping, Optional, Tuple

import requests

from .errors import (
    FinOpsConcurrencyError,
    FinOpsHttpError,
    FinOpsNotFoundError,
    FinOpsThrottledError,
)

if TYPE_CHECKING:  # pragma: no cover
    from ._auth import TokenProvider


logger = logging.getLogger("PowerPlatform.FinOps")

# Status codes that trigger a retry.
_RETRY_STATUS = frozenset({408, 429, 502, 503, 504})

# Headers FinOps uses to surface activity-id (for support correlation).
_ACTIVITY_HEADERS = ("ms-dyn-activityid", "request-id", "x-ms-request-id")


class HttpClient:
    """Thin retrying wrapper around ``requests.Session``."""

    def __init__(
        self,
        token_provider: "TokenProvider",
        *,
        max_retries: int = 5,
        backoff_initial: float = 0.5,
        backoff_cap: float = 30.0,
        timeout: float = 60.0,
        user_agent: str = "PowerPlatform-FinOps-Python/0.0.1",
    ) -> None:
        self._tp = token_provider
        self._max_retries = max_retries
        self._backoff_initial = backoff_initial
        self._backoff_cap = backoff_cap
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Accept": "application/json",
                "OData-Version": "4.0",
                "OData-MaxVersion": "4.0",
                "User-Agent": user_agent,
            }
        )

    def close(self) -> None:
        self._session.close()

    # -- public -----------------------------------------------------------

    def request(
        self,
        method: str,
        url: str,
        *,
        params: Optional[Mapping[str, Any]] = None,
        json: Any = None,
        headers: Optional[Mapping[str, str]] = None,
        expected: Tuple[int, ...] = (200, 201, 204),
    ) -> requests.Response:
        """Issue an HTTP call with auth + retries; raise on unexpected status."""
        attempt = 0
        while True:
            attempt += 1
            merged = self._build_headers(headers)
            try:
                resp = self._session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json,
                    headers=merged,
                    timeout=self._timeout,
                )
            except requests.RequestException as exc:
                if attempt > self._max_retries:
                    raise FinOpsHttpError(
                        f"Network error after {attempt - 1} retries: {exc}",
                        status_code=0,
                        url=url,
                    ) from exc
                self._sleep_backoff(attempt, retry_after=None)
                continue

            if resp.status_code in expected:
                return resp

            if resp.status_code == 401:
                # Token may have expired between cache check and the wire;
                # invalidate and retry once.
                if attempt == 1:
                    self._tp.invalidate()
                    continue
                self._raise(resp)

            if resp.status_code in _RETRY_STATUS and attempt <= self._max_retries:
                self._sleep_backoff(attempt, retry_after=self._retry_after(resp))
                continue

            self._raise(resp)

    # -- internals --------------------------------------------------------

    def _build_headers(
        self, extra: Optional[Mapping[str, str]]
    ) -> Dict[str, str]:
        headers = {"Authorization": f"Bearer {self._tp.get_bearer()}"}
        if extra:
            headers.update(extra)
        return headers

    def _sleep_backoff(self, attempt: int, *, retry_after: Optional[float]) -> None:
        if retry_after is not None:
            delay = retry_after
        else:
            # Exponential backoff with full jitter.
            delay = min(self._backoff_cap, self._backoff_initial * (2 ** (attempt - 1)))
            delay = random.uniform(0, delay)
        logger.debug("FinOps SDK retry attempt=%d sleeping=%.2fs", attempt, delay)
        time.sleep(delay)

    @staticmethod
    def _retry_after(resp: requests.Response) -> Optional[float]:
        ra = resp.headers.get("Retry-After")
        if not ra:
            return None
        try:
            return max(0.0, float(ra))
        except ValueError:
            return None

    @classmethod
    def _activity_id(cls, resp: requests.Response) -> Optional[str]:
        for h in _ACTIVITY_HEADERS:
            v = resp.headers.get(h)
            if v:
                return v
        return None

    @classmethod
    def _raise(cls, resp: requests.Response) -> None:
        try:
            body: Any = resp.json()
        except ValueError:
            body = resp.text
        activity = cls._activity_id(resp)
        msg = f"FinOps {resp.request.method} {resp.url} failed: {resp.status_code}"
        sc = resp.status_code
        if sc == 404:
            raise FinOpsNotFoundError(
                msg, status_code=sc, activity_id=activity, response_body=body, url=resp.url
            )
        if sc == 412:
            raise FinOpsConcurrencyError(
                msg, status_code=sc, activity_id=activity, response_body=body, url=resp.url
            )
        if sc == 429:
            raise FinOpsThrottledError(
                msg,
                status_code=sc,
                activity_id=activity,
                response_body=body,
                url=resp.url,
                retry_after=cls._retry_after(resp),
            )
        raise FinOpsHttpError(
            msg, status_code=sc, activity_id=activity, response_body=body, url=resp.url
        )
