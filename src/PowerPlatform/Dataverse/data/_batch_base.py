# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Pure serialisation/parsing base class shared by sync and async batch clients."""

from __future__ import annotations

import json
import re
import uuid
from typing import Any, Dict, List, Optional, Tuple, Union

from ..core.errors import HttpError
from ..core._error_codes import _http_subcode
from ..models.batch import BatchItemResponse, BatchResult
from ._raw_request import _RawRequest

__all__ = []

_CRLF = "\r\n"


# ---------------------------------------------------------------------------
# Module-level multipart parsing helpers
# ---------------------------------------------------------------------------


def _raise_top_level_batch_error(response: Any) -> None:
    """Parse a non-multipart batch response and raise HttpError with the service message.

    Dataverse returns ``application/json`` with an ``{"error": {...}}`` payload when
    it rejects the batch request at the HTTP level (e.g. malformed multipart body,
    missing OData headers). This helper surfaces that detail instead of silently
    returning an empty ``BatchResult``.
    """
    status_code: int = getattr(response, "status_code", 0)
    service_error_code: Optional[str] = None
    try:
        payload = response.json()
        error = payload.get("error", {})
        service_error_code = error.get("code") or None
        message: str = error.get("message") or response.text or "Unexpected non-multipart response from $batch"
    except Exception:
        message = (getattr(response, "text", None) or "") or "Unexpected non-multipart response from $batch"
    raise HttpError(
        message=f"Batch request rejected by Dataverse: {message}",
        status_code=status_code,
        subcode=_http_subcode(status_code) if status_code else None,
        service_error_code=service_error_code,
    )


_BOUNDARY_RE = re.compile(r'boundary="?([^";,\s]+)"?', re.IGNORECASE)


def _extract_boundary(content_type: str) -> Optional[str]:
    m = _BOUNDARY_RE.search(content_type)
    return m.group(1) if m else None


def _split_multipart(body: str, boundary: str) -> List[Tuple[Dict[str, str], str]]:
    delimiter = f"--{boundary}"
    parts: List[Tuple[Dict[str, str], str]] = []
    lines = body.replace("\r\n", "\n").split("\n")
    current: List[str] = []
    in_part = False
    for line in lines:
        stripped = line.rstrip("\r")
        if stripped == delimiter:
            if in_part and current:
                parts.append(_parse_mime_part("\n".join(current)))
                current = []
            in_part = True
        elif stripped == f"{delimiter}--":
            if in_part and current:
                parts.append(_parse_mime_part("\n".join(current)))
            break
        elif in_part:
            current.append(line)
    return parts


def _parse_mime_part(raw: str) -> Tuple[Dict[str, str], str]:
    if "\n\n" in raw:
        header_block, body = raw.split("\n\n", 1)
    else:
        header_block, body = raw, ""
    headers: Dict[str, str] = {}
    for line in header_block.splitlines():
        if ":" in line:
            name, _, value = line.partition(":")
            headers[name.strip().lower()] = value.strip()
    return headers, body.strip()


def _parse_http_response_part(text: str, content_id: Optional[str]) -> Optional[BatchItemResponse]:
    from ._odata import _GUID_RE  # local import to avoid circular dependency at module load

    lines = text.replace("\r\n", "\n").splitlines()
    if not lines:
        return None
    status_line = ""
    idx = 0
    for i, line in enumerate(lines):
        if line.startswith("HTTP/"):
            status_line = line
            idx = i + 1
            break
    if not status_line:
        return None
    parts = status_line.split(" ", 2)
    if len(parts) < 2:
        return None
    try:
        status_code = int(parts[1])
    except ValueError:
        return None
    resp_headers: Dict[str, str] = {}
    body_start = idx
    for i in range(idx, len(lines)):
        if lines[i] == "":
            body_start = i + 1
            break
        if ":" in lines[i]:
            name, _, value = lines[i].partition(":")
            resp_headers[name.strip().lower()] = value.strip()
    entity_id: Optional[str] = None
    odata_id = resp_headers.get("odata-entityid", "")
    if odata_id:
        m = _GUID_RE.search(odata_id)
        if m:
            entity_id = m.group(0)
    body_text = "\n".join(lines[body_start:]).strip()
    data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    error_code: Optional[str] = None
    if body_text:
        try:
            parsed = json.loads(body_text)
            if isinstance(parsed, dict):
                err = parsed.get("error")
                if isinstance(err, dict):
                    error_message = err.get("message")
                    error_code = err.get("code")
                else:
                    data = parsed
        except (json.JSONDecodeError, ValueError):
            pass
    return BatchItemResponse(
        status_code=status_code,
        content_id=content_id,
        entity_id=entity_id,
        data=data,
        error_message=error_message,
        error_code=error_code,
    )


# ---------------------------------------------------------------------------
# Pure mixin: 4 stateless serialisation/parsing methods
# ---------------------------------------------------------------------------


class _BatchBase:
    """Pure mixin providing multipart serialisation and batch response parsing.

    Contains no ``__init__`` and no instance state.  Both
    :class:`~PowerPlatform.Dataverse.data._batch._BatchClient` and
    :class:`~PowerPlatform.Dataverse.aio.data._async_batch._AsyncBatchClient`
    inherit from this class to share the 4 pure methods without creating an
    LSP-violating sync→async inheritance chain.
    """

    # ------------------------------------------------------------------
    # Multipart serialisation
    # ------------------------------------------------------------------

    def _build_batch_body(
        self,
        resolved: List[Union[_RawRequest, "Any"]],
        batch_boundary: str,
    ) -> str:
        from ._batch import _ChangeSetBatchItem  # local import to avoid circular dependency

        parts: List[str] = []
        for item in resolved:
            if isinstance(item, _ChangeSetBatchItem):
                parts.append(self._serialize_changeset_item(item, batch_boundary))
            else:
                parts.append(self._serialize_raw_request(item, batch_boundary))
        return "".join(parts) + f"--{batch_boundary}--{_CRLF}"

    def _serialize_raw_request(self, req: _RawRequest, boundary: str) -> str:
        """Serialise a single operation as a multipart/mixed part with CRLF line endings."""
        part_header_lines = [
            f"--{boundary}",
            "Content-Type: application/http",
            "Content-Transfer-Encoding: binary",
        ]
        if req.content_id is not None:
            part_header_lines.append(f"Content-ID: {req.content_id}")

        inner_lines = [f"{req.method} {req.url} HTTP/1.1"]
        if req.body is not None:
            inner_lines.append("Content-Type: application/json; type=entry")
        if req.headers:
            for k, v in req.headers.items():
                inner_lines.append(f"{k}: {v}")
        inner_lines.append("")  # blank line — end of inner headers
        if req.body is not None:
            inner_lines.append(req.body)

        part_header_str = _CRLF.join(part_header_lines) + _CRLF
        inner_str = _CRLF.join(inner_lines)
        return part_header_str + _CRLF + inner_str + _CRLF

    def _serialize_changeset_item(self, cs: Any, batch_boundary: str) -> str:
        cs_boundary = f"changeset_{uuid.uuid4()}"
        cs_parts = [self._serialize_raw_request(r, cs_boundary) for r in cs.requests]
        cs_parts.append(f"--{cs_boundary}--{_CRLF}")
        cs_body = "".join(cs_parts)

        outer = (
            f"--{batch_boundary}{_CRLF}" f'Content-Type: multipart/mixed; boundary="{cs_boundary}"{_CRLF}' f"{_CRLF}"
        )
        return outer + cs_body + _CRLF

    # ------------------------------------------------------------------
    # Response parsing (multipart/mixed)
    # ------------------------------------------------------------------

    def _parse_batch_response(self, response: Any) -> BatchResult:
        content_type = response.headers.get("Content-Type", "")
        boundary = _extract_boundary(content_type)
        if not boundary:
            # Non-multipart response: the batch request itself was rejected by Dataverse
            # (common for top-level 4xx, e.g. malformed body, missing OData headers).
            # Returning an empty BatchResult() here would silently hide the error and
            # make has_errors=False, which is actively misleading. Raise instead.
            _raise_top_level_batch_error(response)
            return BatchResult()  # unreachable; satisfies type checkers
        parts = _split_multipart(response.text or "", boundary)
        responses: List[BatchItemResponse] = []
        for part_headers, part_body in parts:
            part_ct = part_headers.get("content-type", "")
            if "multipart/mixed" in part_ct:
                inner_boundary = _extract_boundary(part_ct)
                if inner_boundary:
                    for ih, ib in _split_multipart(part_body, inner_boundary):
                        item = _parse_http_response_part(ib, ih.get("content-id"))
                        if item is not None:
                            responses.append(item)
            else:
                item = _parse_http_response_part(part_body, content_id=part_headers.get("content-id"))
                if item is not None:
                    responses.append(item)
        return BatchResult(responses=responses)
