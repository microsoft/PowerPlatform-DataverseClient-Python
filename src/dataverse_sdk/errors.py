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
        source: Optional[Dict[str, Any]] = None,
        is_transient: Optional[bool] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.code = code
        self.subcode = subcode
        self.status_code = status_code
        self.details = details or {}
        self.source = source or {}
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

class HttpError(DataverseError):
    def __init__(
        self,
        message: str,
        *,
        subcode: Optional[str] = None,
        status_code: Optional[int] = None,
        details: Optional[Dict[str, Any]] = None,
        source: Optional[Dict[str, Any]] = None,
        is_transient: Optional[bool] = None,
    ) -> None:
        super().__init__(
            message,
            code="http",
            subcode=subcode,
            status_code=status_code,
            details=details,
            source=source,
            is_transient=is_transient,
        )

__all__ = ["DataverseError", "HttpError"]
