# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Dataverse client configuration.

Provides :class:`~PowerPlatform.Dataverse.core.config.DataverseConfig`, a lightweight
immutable container for locale and (reserved) HTTP tuning options plus the
convenience constructor :meth:`~PowerPlatform.Dataverse.core.config.DataverseConfig.from_env`.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .telemetry import TelemetryConfig


@dataclass(frozen=True)
class DataverseConfig:
    """
    Configuration settings for Dataverse client operations.

    :param language_code: LCID (Locale ID) for localized labels and messages. Default is 1033 (English - United States).
    :type language_code: :class:`int`
    :param http_retries: Optional maximum number of retry attempts for transient HTTP errors. Reserved for future use.
    :type http_retries: :class:`int` or None
    :param http_backoff: Optional backoff multiplier (in seconds) between retry attempts. Reserved for future use.
    :type http_backoff: :class:`float` or None
    :param http_timeout: Optional request timeout in seconds. Reserved for future use.
    :type http_timeout: :class:`float` or None
    :param telemetry: Optional telemetry configuration for tracing, metrics, and logging.
    :type telemetry: :class:`~PowerPlatform.Dataverse.core.telemetry.TelemetryConfig` or None
    """

    language_code: int = 1033

    # Optional HTTP tuning (not yet wired everywhere; reserved for future use)
    http_retries: Optional[int] = None
    http_backoff: Optional[float] = None
    http_timeout: Optional[float] = None

    # Telemetry configuration
    telemetry: Optional["TelemetryConfig"] = None

    @classmethod
    def from_env(cls) -> "DataverseConfig":
        """
        Create a configuration instance from environment variables.

        Environment variables:
            - DATAVERSE_LANGUAGE_CODE: LCID for localized labels (default: 1033)
            - DATAVERSE_TELEMETRY_ENABLED: Enable telemetry (default: false)
            - DATAVERSE_TRACING_ENABLED: Enable tracing when telemetry enabled (default: true)
            - DATAVERSE_METRICS_ENABLED: Enable metrics when telemetry enabled (default: false)
            - DATAVERSE_LOGGING_ENABLED: Enable logging when telemetry enabled (default: true)
            - DATAVERSE_LOG_LEVEL: Log level (default: WARNING)
            - OTEL_SERVICE_NAME: OpenTelemetry service name

        :return: Configuration instance with values from environment.
        :rtype: ~PowerPlatform.Dataverse.core.config.DataverseConfig
        """
        from .telemetry import TelemetryConfig

        telemetry = None
        if os.getenv("DATAVERSE_TELEMETRY_ENABLED", "").lower() == "true":
            telemetry = TelemetryConfig(
                enable_tracing=os.getenv("DATAVERSE_TRACING_ENABLED", "true").lower() == "true",
                enable_metrics=os.getenv("DATAVERSE_METRICS_ENABLED", "false").lower() == "true",
                enable_logging=os.getenv("DATAVERSE_LOGGING_ENABLED", "true").lower() == "true",
                log_level=os.getenv("DATAVERSE_LOG_LEVEL", "WARNING"),
                service_name=os.getenv("OTEL_SERVICE_NAME"),
            )

        return cls(
            language_code=int(os.getenv("DATAVERSE_LANGUAGE_CODE", "1033")),
            http_retries=None,
            http_backoff=None,
            http_timeout=None,
            telemetry=telemetry,
        )
