# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Internal HTTP logger that writes redacted request/response diagnostics to local files.
"""

from __future__ import annotations

import json as _json
import logging
import os
import uuid
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Optional

from .log_config import LogConfig


class _HttpLogger:
    """Structured HTTP diagnostic logger with automatic header redaction."""

    def __init__(self, config: LogConfig) -> None:
        self._config = config
        self._redacted = {h.lower() for h in config.redacted_headers}

        # Ensure folder exists
        os.makedirs(config.log_folder, exist_ok=True)

        # Build timestamped filename — random suffix guarantees uniqueness even when
        # multiple clients are created within the same microsecond (e.g. in tests).
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        filename = f"{config.log_file_prefix}_{ts}_{uuid.uuid4().hex[:6]}.log"
        filepath = os.path.join(config.log_folder, filename)

        # Create a dedicated named logger (not root) to avoid side effects
        logger_name = f"PowerPlatform.Dataverse.http.{uuid.uuid4().hex[:8]}"
        self._logger = logging.getLogger(logger_name)
        self._logger.setLevel(getattr(logging, config.log_level.upper(), logging.DEBUG))
        self._logger.propagate = False  # don't bubble to root

        self._handler = RotatingFileHandler(
            filepath,
            maxBytes=config.max_file_bytes,
            backupCount=config.backup_count,
            encoding="utf-8",
        )
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S%z",
        )
        self._handler.setFormatter(formatter)
        self._logger.addHandler(self._handler)

        # Write a one-time config summary so readers know what is and isn't captured.
        body_status = (
            f"{config.max_body_bytes} bytes"
            if config.max_body_bytes > 0
            else "0 (disabled — set max_body_bytes > 0 to enable)"
        )
        redacted = ", ".join(sorted(config.redacted_headers))
        self._logger.debug(
            "\n".join(
                [
                    "=== Dataverse HTTP Diagnostics ===",
                    f"    log_level:        {config.log_level.upper()}",
                    f"    max_body_bytes:   {body_status}",
                    f"    redacted_headers: {redacted}",
                    "==================================",
                ]
            )
        )

    def log_request(
        self,
        method: str,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        body: Any = None,
    ) -> None:
        """Log an outbound HTTP request."""
        safe_headers = self._redact_headers(headers or {})
        body_text = self._truncate_body(body)
        lines = [f">>> REQUEST  {method.upper()} {url}"]
        lines += [f"    {k}: {v}" for k, v in safe_headers.items()]
        if body_text:
            lines.append(f"    Body:    {body_text}")
        self._logger.debug("\n".join(lines))

    def log_response(
        self,
        method: str,
        url: str,
        status_code: int,
        headers: Optional[Dict[str, str]] = None,
        body: Any = None,
        elapsed_ms: Optional[float] = None,
    ) -> None:
        """Log an inbound HTTP response."""
        safe_headers = self._redact_headers(headers or {})
        body_text = self._truncate_body(body)
        elapsed_str = f" ({elapsed_ms:.1f}ms)" if elapsed_ms is not None else ""
        lines = [f"<<< RESPONSE {status_code} {method.upper()} {url}{elapsed_str}"]
        lines += [f"    {k}: {v}" for k, v in safe_headers.items()]
        if body_text:
            lines.append(f"    Body:    {body_text}")
        elif not self.body_logging_enabled:
            # Body capture is disabled. Emit an explicit marker when the response
            # has a Content-Length > 0 so readers know content exists but was not captured.
            raw_headers = headers or {}
            content_length = raw_headers.get("Content-Length") or raw_headers.get("content-length", "0")
            try:
                has_body = int(content_length) > 0
            except (ValueError, TypeError):
                has_body = False
            if has_body:
                lines.append("    Body:    [not captured — set max_body_bytes > 0 to enable]")
        self._logger.debug("\n".join(lines))

    def log_error(
        self,
        method: str,
        url: str,
        error: Exception,
        attempt: Optional[int] = None,
        max_attempts: Optional[int] = None,
    ) -> None:
        """Log an HTTP transport error."""
        attempt_str = f" [attempt {attempt}/{max_attempts}]" if attempt is not None and max_attempts is not None else ""
        self._logger.error(f"!!! ERROR    {method.upper()} {url}{attempt_str} - {type(error).__name__}: {error}")

    @property
    def body_logging_enabled(self) -> bool:
        """Return True if body capture is enabled (max_body_bytes > 0)."""
        return self._config.max_body_bytes != 0

    def close(self) -> None:
        """Flush and close the underlying file handler. Safe to call multiple times."""
        self._handler.flush()
        self._handler.close()
        self._logger.removeHandler(self._handler)

    def _redact_headers(self, headers: Dict[str, str]) -> Dict[str, str]:
        return {k: ("[REDACTED]" if k.lower() in self._redacted else v) for k, v in headers.items()}

    def _truncate_body(self, body: Any) -> str:
        if body is None:
            return ""
        if isinstance(body, (bytes, bytearray)):
            text = body.decode("utf-8", errors="replace")
        elif not isinstance(body, str):
            try:
                text = _json.dumps(body, default=str, ensure_ascii=False)
            except (TypeError, ValueError):
                text = str(body)
        else:
            text = body

        limit = self._config.max_body_bytes
        if limit == 0:
            return ""
        encoded = text.encode("utf-8")
        if len(encoded) > limit:
            # Truncate on byte boundary, then decode safely to avoid splitting
            # multi-byte characters. Report the true byte length, not char count.
            truncated = encoded[:limit].decode("utf-8", errors="ignore")
            return truncated + f"... [truncated, {len(encoded)} bytes total]"
        return text
