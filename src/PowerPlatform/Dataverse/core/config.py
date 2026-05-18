# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Dataverse client configuration.

Provides :class:`~PowerPlatform.Dataverse.core.config.DataverseConfig`, a lightweight
immutable container for locale and (reserved) HTTP tuning options plus the
convenience constructor :meth:`~PowerPlatform.Dataverse.core.config.DataverseConfig.from_env`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from .log_config import LogConfig

# key=value pairs separated by semicolons.
# Keys: alphanumeric, hyphens, underscores.
# Values: alphanumeric, hyphens, underscores, dots, slashes.
_CONTEXT_PATTERN = re.compile(r"^[a-zA-Z0-9_-]+=[a-zA-Z0-9_./-]+(;[a-zA-Z0-9_-]+=[a-zA-Z0-9_./-]+)*$")


@dataclass(frozen=True)
class OperationContext:
    """Caller-defined context appended to outbound ``User-Agent`` headers.

    The context string is validated to be semicolon-separated ``key=value`` pairs
    (e.g. ``"app=myapp/1.0;agent=claude-code"``).  Free-form text, email
    addresses, and other potentially sensitive strings are rejected.

    :param user_agent_context: Attribution string in ``key=value;key=value`` format.
    :type user_agent_context: :class:`str`

    :raises ValueError: If the string is empty, contains control characters, or
        does not match the required ``key=value`` format.
    """

    user_agent_context: str

    def __post_init__(self) -> None:
        val = self.user_agent_context
        if not val:
            raise ValueError("operation_context must not be empty.")
        if any(c in val for c in "\r\n\x00"):
            raise ValueError("operation_context must not contain CR, LF, or NUL characters.")
        if not _CONTEXT_PATTERN.match(val):
            raise ValueError(
                "operation_context must be semicolon-separated key=value pairs "
                "(e.g. 'app=myapp/1.0;agent=claude-code'). "
                "Keys and values may contain alphanumerics, hyphens, underscores, "
                "dots, and slashes."
            )


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
    :param log_config: Optional local HTTP diagnostics logging configuration.
        When provided, all HTTP requests and responses are logged to timestamped
        ``.log`` files with automatic redaction of sensitive headers.
    :type log_config: ~PowerPlatform.Dataverse.core.log_config.LogConfig or None
    :param operation_context: Optional caller-defined context object appended to the
        outbound ``User-Agent`` header as a parenthesized comment. Intended for
        plugin/tool attribution.
    :type operation_context: ~PowerPlatform.Dataverse.core.config.OperationContext or None
    """

    language_code: int = 1033

    # Optional HTTP tuning (not yet wired everywhere; reserved for future use)
    http_retries: Optional[int] = None
    http_backoff: Optional[float] = None
    http_timeout: Optional[float] = None

    log_config: Optional["LogConfig"] = None

    operation_context: Optional[OperationContext] = None

    @classmethod
    def from_env(cls) -> "DataverseConfig":
        """
        Create a configuration instance with default settings.

        :return: Configuration instance with default values.
        :rtype: ~PowerPlatform.Dataverse.core.config.DataverseConfig
        """
        # Environment-free defaults
        return cls(
            language_code=1033,
            http_retries=None,
            http_backoff=None,
            http_timeout=None,
            log_config=None,
            operation_context=None,
        )
