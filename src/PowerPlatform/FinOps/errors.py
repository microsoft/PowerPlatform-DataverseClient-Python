"""Exception hierarchy for the FinOps SDK.

Mirrors the shape of the Dataverse SDK's structured-error model so callers
that already use the Dataverse client get a familiar surface.
"""
from __future__ import annotations

from typing import Any, Optional


class FinOpsError(Exception):
    """Base class for all FinOps SDK errors."""


class FinOpsAuthError(FinOpsError):
    """Raised when token acquisition or refresh fails."""


class FinOpsHttpError(FinOpsError):
    """Raised for any non-2xx HTTP response from the FinOps server.

    Carries the activity-id surfaced by FinOps for support correlation,
    when present (header ``ms-dyn-activityid`` or ``request-id``).
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: int,
        activity_id: Optional[str] = None,
        response_body: Any = None,
        url: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.activity_id = activity_id
        self.response_body = response_body
        self.url = url

    def __str__(self) -> str:  # pragma: no cover - cosmetic
        base = super().__str__()
        bits = [f"status={self.status_code}"]
        if self.activity_id:
            bits.append(f"activity_id={self.activity_id}")
        if self.url:
            bits.append(f"url={self.url}")
        return f"{base} ({', '.join(bits)})"


class FinOpsNotFoundError(FinOpsHttpError):
    """HTTP 404."""


class FinOpsConcurrencyError(FinOpsHttpError):
    """HTTP 412 — precondition failed (ETag mismatch on update/delete)."""


class FinOpsThrottledError(FinOpsHttpError):
    """HTTP 429 after retries exhausted."""

    def __init__(self, *args: Any, retry_after: Optional[float] = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.retry_after = retry_after
