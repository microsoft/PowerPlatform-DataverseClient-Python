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
    :param http_retries: Maximum number of retry attempts for HTTP requests (default: 5).
    :type http_retries: int or None
    :param http_backoff: Base delay in seconds for exponential backoff (default: 0.5).
    :type http_backoff: float or None
    :param http_max_backoff: Maximum delay between retry attempts in seconds (default: 60.0).
    :type http_max_backoff: float or None
    :param http_timeout: Request timeout in seconds (default: method-dependent).
    :type http_timeout: float or None
    :param http_jitter: Whether to add jitter to retry delays to prevent thundering herd (default: True).
    :type http_jitter: bool or None
    :param http_retry_transient_errors: Whether to retry transient HTTP errors like 429, 502, 503, 504 (default: True).
    :type http_retry_transient_errors: bool or None
    """
    language_code: int = 1033

    # HTTP retry and resilience configuration
    http_retries: Optional[int] = None
    http_backoff: Optional[float] = None
    http_max_backoff: Optional[float] = None
    http_timeout: Optional[float] = None
    http_jitter: Optional[bool] = None
    http_retry_transient_errors: Optional[bool] = None

    @classmethod
    def from_env(cls) -> "DataverseConfig":
        """
        Create a configuration instance with default settings.

        :return: Configuration instance with default values.
        :rtype: ~PowerPlatform.Dataverse.core.config.DataverseConfig
        """
        # Environment-free defaults with enhanced retry configuration
        return cls(
            language_code=1033,
            http_retries=None,  # Will default to 5 in HttpClient
            http_backoff=None,  # Will default to 0.5 in HttpClient
            http_max_backoff=None,  # Will default to 60.0 in HttpClient
            http_timeout=None,  # Will use method-dependent defaults in HttpClient
            http_jitter=None,  # Will default to True in HttpClient
            http_retry_transient_errors=None,  # Will default to True in HttpClient
        )
