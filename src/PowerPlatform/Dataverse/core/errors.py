# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Structured Dataverse exception hierarchy.

This module provides :class:`~PowerPlatform.Dataverse.core.errors.DataverseError` and
specialized :class:`~PowerPlatform.Dataverse.core.errors.ValidationError`,
:class:`~PowerPlatform.Dataverse.core.errors.MetadataError`,
:class:`~PowerPlatform.Dataverse.core.errors.SQLParseError`, and
:class:`~PowerPlatform.Dataverse.core.errors.HttpError` for validation, metadata,
SQL parsing, and Web API HTTP failures.
"""

from __future__ import annotations
from typing import Any, Dict, Optional
import datetime as _dt


class DataverseError(Exception):
    """
    Base structured exception for the Dataverse SDK.

    :param message: Human-readable error message.
    :type message: :class:`str`
    :param code: Error category code (e.g. ``"validation_error"``, ``"http_error"``).
    :type code: :class:`str`
    :param subcode: Optional subcategory or specific error identifier.
    :type subcode: :class:`str` | None
    :param status_code: Optional HTTP status code if the error originated from an HTTP response.
    :type status_code: :class:`int` | None
    :param details: Optional dictionary containing additional diagnostic information.
    :type details: :class:`dict` | None
    :param source: Error source, either ``"client"`` or ``"server"``.
    :type source: :class:`str`
    :param is_transient: Whether the error is potentially transient and may succeed on retry.
    :type is_transient: :class:`bool`
    """

    def __init__(
        self,
        message: str,
        code: str,
        subcode: Optional[str] = None,
        status_code: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
        source: Optional[str] = None,
        is_transient: bool = False,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.subcode = subcode
        self.status_code = status_code
        self.details = details or {}
        self.source = source or "client"
        self.is_transient = is_transient
        self.timestamp = _dt.datetime.now(_dt.timezone.utc).isoformat().replace("+00:00", "Z")

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the error to a dictionary representation.

        :return: Dictionary containing all error properties.
        :rtype: :class:`dict`
        """
        return {
            "message": self.message,
            "code": self.code,
            "subcode": self.subcode,
            "status_code": self.status_code,
            "details": self.details,
            "source": self.source,
            "is_transient": self.is_transient,
            "timestamp": self.timestamp,
        }

    def __repr__(self) -> str:  # pragma: no cover
        return f"{self.__class__.__name__}(code={self.code!r}, subcode={self.subcode!r}, message={self.message!r})"


class ValidationError(DataverseError):
    """
    Exception raised for client-side validation failures.

    :param message: Human-readable validation error message.
    :type message: :class:`str`
    :param subcode: Optional specific validation error identifier.
    :type subcode: :class:`str` | None
    :param details: Optional dictionary with additional validation context.
    :type details: :class:`dict` | None
    """

    def __init__(self, message: str, *, subcode: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code="validation_error", subcode=subcode, details=details, source="client")


class MetadataError(DataverseError):
    """
    Exception raised for metadata operation failures.

    :param message: Human-readable metadata error message.
    :type message: :class:`str`
    :param subcode: Optional specific metadata error identifier.
    :type subcode: :class:`str` | None
    :param details: Optional dictionary with additional metadata context.
    :type details: :class:`dict` | None
    """

    def __init__(self, message: str, *, subcode: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code="metadata_error", subcode=subcode, details=details, source="client")


class SQLParseError(DataverseError):
    """
    Exception raised for SQL query parsing failures.

    :param message: Human-readable SQL parsing error message.
    :type message: :class:`str`
    :param subcode: Optional specific SQL parsing error identifier.
    :type subcode: :class:`str` | None
    :param details: Optional dictionary with SQL query context and parse information.
    :type details: :class:`dict` | None
    """

    def __init__(self, message: str, *, subcode: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code="sql_parse_error", subcode=subcode, details=details, source="client")


class HttpError(DataverseError):
    """
    Exception raised for HTTP request failures from the Dataverse Web API.

    :param message: Human-readable HTTP error message, typically from the API error response.
    :type message: :class:`str`
    :param status_code: HTTP status code (e.g. 400, 404, 500).
    :type status_code: :class:`int`
    :param is_transient: Whether the error is transient (429, 503, 504) and may succeed on retry.
    :type is_transient: :class:`bool`
    :param subcode: Optional HTTP status category (e.g. ``"4xx"``, ``"5xx"``).
    :type subcode: :class:`str` | None
    :param service_error_code: Optional Dataverse-specific error code from the API response.
    :type service_error_code: :class:`str` | None
    :param correlation_id: Optional client-generated correlation ID for tracking requests within an SDK call.
    :type correlation_id: :class:`str` | None
    :param client_request_id: Optional client-generated request ID injected into outbound headers.
    :type client_request_id: :class:`str` | None
    :param service_request_id: Optional ``x-ms-service-request-id`` value returned by Dataverse servers.
    :type service_request_id: :class:`str` | None
    :param traceparent: Optional W3C trace context for distributed tracing.
    :type traceparent: :class:`str` | None
    :param body_excerpt: Optional excerpt of the response body for diagnostics.
    :type body_excerpt: :class:`str` | None
    :param retry_after: Optional number of seconds to wait before retrying (from Retry-After header).
    :type retry_after: :class:`int` | None
    :param details: Optional additional diagnostic details.
    :type details: :class:`dict` | None
    """

    def __init__(
        self,
        message: str,
        status_code: int,
        is_transient: bool = False,
        subcode: Optional[str] = None,
        service_error_code: Optional[str] = None,
        correlation_id: Optional[str] = None,
        client_request_id: Optional[str] = None,
        service_request_id: Optional[str] = None,
        traceparent: Optional[str] = None,
        body_excerpt: Optional[str] = None,
        retry_after: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        d = details or {}
        if service_error_code is not None:
            d["service_error_code"] = service_error_code
        if correlation_id is not None:
            d["correlation_id"] = correlation_id
        if client_request_id is not None:
            d["client_request_id"] = client_request_id
        if service_request_id is not None:
            d["service_request_id"] = service_request_id
        if traceparent is not None:
            d["traceparent"] = traceparent
        if body_excerpt is not None:
            d["body_excerpt"] = body_excerpt
        if retry_after is not None:
            d["retry_after"] = retry_after
        super().__init__(
            message,
            code="http_error",
            subcode=subcode,
            status_code=status_code,
            details=d,
            source="server",
            is_transient=is_transient,
        )


__all__ = ["DataverseError", "HttpError", "ValidationError", "MetadataError", "SQLParseError"]
