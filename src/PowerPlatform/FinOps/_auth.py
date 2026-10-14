"""Token acquisition + caching for the FinOps SDK.

Wraps any ``azure.core.credentials.TokenCredential`` (e.g. the credentials in
``azure-identity``). Caches the bearer token in memory and proactively refreshes
it shortly before expiry.

Per Platform/AX.Owin/FinOpsAuthenticationOptionsProvider.cs, FinOps tokens are
short-lived and the recommended client cadence is to refresh every ~5 minutes;
we conservatively refresh when fewer than ``REFRESH_SKEW_SECONDS`` remain.
"""
from __future__ import annotations

import threading
import time
from typing import TYPE_CHECKING, Optional

from .errors import FinOpsAuthError

if TYPE_CHECKING:  # pragma: no cover
    from azure.core.credentials import AccessToken, TokenCredential


# Refresh the cached token when this many seconds (or fewer) remain on it.
REFRESH_SKEW_SECONDS = 300  # 5 min


class TokenProvider:
    """Thread-safe access-token cache for a single FinOps environment."""

    def __init__(self, credential: "TokenCredential", scope: str) -> None:
        if not scope:
            raise ValueError("scope must be a non-empty string")
        self._credential = credential
        self._scope = scope
        self._lock = threading.Lock()
        self._token: Optional["AccessToken"] = None

    @property
    def scope(self) -> str:
        return self._scope

    def get_bearer(self) -> str:
        """Return a valid bearer token, refreshing in-place if needed."""
        token = self._token
        if token is None or self._needs_refresh(token):
            with self._lock:
                token = self._token
                if token is None or self._needs_refresh(token):
                    token = self._acquire()
                    self._token = token
        return token.token

    def invalidate(self) -> None:
        """Drop the cached token (forces a fresh acquisition next call)."""
        with self._lock:
            self._token = None

    # -- internals -------------------------------------------------------

    @staticmethod
    def _needs_refresh(token: "AccessToken") -> bool:
        return token.expires_on - time.time() <= REFRESH_SKEW_SECONDS

    def _acquire(self) -> "AccessToken":
        try:
            return self._credential.get_token(self._scope)
        except Exception as exc:  # pragma: no cover - re-raised
            raise FinOpsAuthError(
                f"Failed to acquire token for scope {self._scope!r}: {exc}"
            ) from exc
