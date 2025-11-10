# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class DataverseConfig:
    """
    Configuration settings for Dataverse client operations.

    :param language_code: LCID (Locale ID) for localized labels and messages. Default is 1033 (English - United States).
    :type language_code: int
    :param http_retries: Optional maximum number of retry attempts for transient HTTP errors. Reserved for future use.
    :type http_retries: int or None
    :param http_backoff: Optional backoff multiplier (in seconds) between retry attempts. Reserved for future use.
    :type http_backoff: float or None
    :param http_timeout: Optional request timeout in seconds. Reserved for future use.
    :type http_timeout: float or None
    """
    language_code: int = 1033

    # Optional HTTP tuning (not yet wired everywhere; reserved for future use)
    http_retries: Optional[int] = None
    http_backoff: Optional[float] = None
    http_timeout: Optional[float] = None

    @classmethod
    def from_env(cls) -> "DataverseConfig":
        """
        Create a configuration instance with default settings.

        :return: Configuration instance with default values.
        :rtype: ~dataverse_sdk.config.DataverseConfig
        """
        # Environment-free defaults
        return cls(
            language_code=1033,
            http_retries=None,
            http_backoff=None,
            http_timeout=None,
        )
