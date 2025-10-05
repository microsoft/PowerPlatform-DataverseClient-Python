from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class DataverseConfig:
    language_code: int = 1033

    # Optional HTTP tuning (not yet wired everywhere; reserved for future use)
    http_retries: Optional[int] = None
    http_backoff: Optional[float] = None
    http_timeout: Optional[float] = None

    @classmethod
    def from_env(cls) -> "DataverseConfig":
        # Environment-free defaults
        return cls(
            language_code=1033,
            http_retries=None,
            http_backoff=None,
            http_timeout=None,
        )
