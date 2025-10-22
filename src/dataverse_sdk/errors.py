from __future__ import annotations
from typing import Any, Dict, Optional
import datetime as _dt

class DataverseError(Exception):
    """Base structured error for the Dataverse SDK."""
    def __init__(
        self,
        message: str,
        *,
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
        self.timestamp = _dt.datetime.utcnow().isoformat() + "Z"

    def to_dict(self) -> Dict[str, Any]:
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
    def __init__(self, message: str, *, subcode: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code="validation_error", subcode=subcode, details=details, source="client")

class MetadataError(DataverseError):
    def __init__(self, message: str, *, subcode: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code="metadata_error", subcode=subcode, details=details, source="client")

class SQLParseError(DataverseError):
    def __init__(self, message: str, *, subcode: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message, code="sql_parse_error", subcode=subcode, details=details, source="client")

class HttpError(DataverseError):
    def __init__(
        self,
        message: str,
        status_code: int,
        is_transient: bool = False,
        subcode: Optional[str] = None,
        service_error_code: Optional[str] = None,
        correlation_id: Optional[str] = None,
        request_id: Optional[str] = None,
        traceparent: Optional[str] = None,
        body_excerpt: Optional[str] = None,
        retry_after: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        d = details or {}
        if service_error_code is not None:
            d["service_error_code"] = service_error_code
        if correlation_id is not None:
            d["correlation_id"] = correlation_id
        if request_id is not None:
            d["request_id"] = request_id
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
