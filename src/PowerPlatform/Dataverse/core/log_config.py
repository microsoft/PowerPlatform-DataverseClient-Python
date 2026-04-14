# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Local file logging configuration for Dataverse SDK HTTP diagnostics.

Provides :class:`LogConfig`, an opt-in configuration for writing request/response
traces to ``.log`` files with automatic header redaction and timestamped filenames.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import FrozenSet

_VALID_LOG_LEVELS: FrozenSet[str] = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})

__all__ = ["LogConfig"]

# Headers whose values must never appear in log files
_DEFAULT_REDACTED_HEADERS: FrozenSet[str] = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "x-ms-authorization-auxiliary",
        "ocp-apim-subscription-key",
    }
)


def _default_redacted_headers() -> FrozenSet[str]:
    return _DEFAULT_REDACTED_HEADERS


@dataclass(frozen=True)
class LogConfig:
    """
    Configuration for local HTTP diagnostics logging.

    When provided to :class:`~PowerPlatform.Dataverse.client.DataverseClient` via
    :class:`~PowerPlatform.Dataverse.core.config.DataverseConfig`, every HTTP request
    and response is logged to timestamped ``.log`` files in the specified folder.
    Sensitive headers (e.g. ``Authorization``) are automatically redacted.

    :param log_folder: Directory path for log files. Created automatically if missing.
        Default: ``"./dataverse_logs"``
    :param log_file_prefix: Filename prefix. Timestamp is appended automatically.
        Default: ``"dataverse"``  →  ``dataverse_20260310_143022.log``
    :param max_body_bytes: Maximum bytes of request/response body to capture.
        ``0`` (default) disables body capture. Enable only for active debugging
        sessions — bodies may contain PII and sensitive business data.
    :param redacted_headers: Header names (case-insensitive) whose values are
        replaced with ``"[REDACTED]"`` in logs. Defaults include
        ``Authorization``, ``Proxy-Authorization``, etc.
    :param log_level: Python logging level name. Default: ``"DEBUG"``.
    :param max_file_bytes: Max size per log file before rotation (bytes).
        Default: ``10_485_760`` (10 MB).
    :param backup_count: Number of rotated backup files to keep. Default: ``5``.
    """

    def __post_init__(self) -> None:
        if self.log_level.upper() not in _VALID_LOG_LEVELS:
            raise ValueError(
                f"Invalid log_level {self.log_level!r}. " f"Must be one of: {', '.join(sorted(_VALID_LOG_LEVELS))}."
            )

    log_folder: str = "./dataverse_logs"
    log_file_prefix: str = "dataverse"
    max_body_bytes: int = 0  # Body capture disabled by default — opt-in only. Request URLs
    # are always logged and may contain filter values and record identifiers.
    # Enable only for active debugging sessions and treat log files as regulated data.
    redacted_headers: FrozenSet[str] = field(default_factory=_default_redacted_headers)
    log_level: str = "DEBUG"
    max_file_bytes: int = 10_485_760  # 10 MB
    backup_count: int = 5
