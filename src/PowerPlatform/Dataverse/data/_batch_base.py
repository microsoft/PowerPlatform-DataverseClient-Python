# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Shared intent types, multipart helpers, and pure-logic base for the Dataverse batch client.

Contains no I/O. Subclasses add the HTTP transport layer (sync or async).
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

from ..core.errors import HttpError, ValidationError
from ..core._error_codes import _http_subcode
from ..models.batch import BatchItemResponse, BatchResult
from ..models.relationship import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
)
from ..models.upsert import UpsertItem
from ..common.constants import CASCADE_BEHAVIOR_REMOVE_LINK
from ._raw_request import _RawRequest
from ._odata_base import _GUID_RE

if TYPE_CHECKING:
    from ._odata_base import _ODataBase

__all__ = []

_CRLF = "\r\n"
_MAX_BATCH_SIZE = 1000


# ---------------------------------------------------------------------------
# Intent dataclasses — one per supported operation type
# (stored at batch-build time; resolved to _RawRequest at execute() time)
# ---------------------------------------------------------------------------

# --- Record intent types ---


@dataclass
class _RecordCreate:
    table: str
    data: Union[Dict[str, Any], List[Dict[str, Any]]]
    content_id: Optional[int] = None  # set only for changeset items


@dataclass
class _RecordUpdate:
    table: str
    ids: Union[str, List[str]]
    changes: Union[Dict[str, Any], List[Dict[str, Any]]]
    content_id: Optional[int] = None  # set only for changeset single-record updates


@dataclass
class _RecordDelete:
    table: str
    ids: Union[str, List[str]]
    use_bulk_delete: bool = True
    content_id: Optional[int] = None  # set only for changeset single-record deletes


@dataclass
class _RecordGet:
    table: str
    record_id: str
    select: Optional[List[str]] = None


@dataclass
class _RecordUpsert:
    table: str
    items: List[UpsertItem]  # always non-empty; normalised by BatchRecordOperations


# --- Table intent types ---


@dataclass
class _TableCreate:
    table: str
    columns: Dict[str, Any]
    solution: Optional[str] = None
    primary_column: Optional[str] = None
    display_name: Optional[str] = None


@dataclass
class _TableDelete:
    table: str


@dataclass
class _TableGet:
    table: str


@dataclass
class _TableList:
    filter: Optional[str] = None
    select: Optional[List[str]] = None


@dataclass
class _TableAddColumns:
    table: str
    columns: Dict[str, Any]


@dataclass
class _TableRemoveColumns:
    table: str
    columns: Union[str, List[str]]


@dataclass
class _TableCreateOneToMany:
    lookup: LookupAttributeMetadata
    relationship: OneToManyRelationshipMetadata
    solution: Optional[str] = None


@dataclass
class _TableCreateManyToMany:
    relationship: ManyToManyRelationshipMetadata
    solution: Optional[str] = None


@dataclass
class _TableDeleteRelationship:
    relationship_id: str


@dataclass
class _TableGetRelationship:
    schema_name: str


@dataclass
class _TableCreateLookupField:
    referencing_table: str
    lookup_field_name: str
    referenced_table: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    required: bool = False
    cascade_delete: str = CASCADE_BEHAVIOR_REMOVE_LINK
    solution: Optional[str] = None
    language_code: int = 1033


# --- Query intent types ---


@dataclass
class _QuerySql:
    sql: str


# ---------------------------------------------------------------------------
# Changeset container
# ---------------------------------------------------------------------------


@dataclass
class _ChangeSet:
    """Ordered group of single-record write operations that execute atomically.

    Content-IDs are allocated from ``_counter``, a single-element ``List[int]``
    that is shared across all changesets in the same batch.  Passing the same
    list object to every ``_ChangeSet`` created by a :class:`BatchRequest`
    ensures Content-ID values are unique within the entire batch request, not
    just within an individual changeset, as required by the OData spec.

    When constructed in isolation (e.g. in unit tests), ``_counter`` defaults
    to a fresh ``[1]`` so the class remains self-contained.
    """

    operations: List[Union[_RecordCreate, _RecordUpdate, _RecordDelete]] = field(default_factory=list)
    _counter: List[int] = field(default_factory=lambda: [1], repr=False)

    def add_create(self, table: str, data: Dict[str, Any]) -> str:
        """Add a single-record create; return its content-ID reference string."""
        cid = self._counter[0]
        self._counter[0] += 1
        self.operations.append(_RecordCreate(table=table, data=data, content_id=cid))
        return f"${cid}"

    def add_update(self, table: str, record_id: str, changes: Dict[str, Any]) -> None:
        """Add a single-record update (record_id may be a '$n' reference)."""
        cid = self._counter[0]
        self._counter[0] += 1
        self.operations.append(_RecordUpdate(table=table, ids=record_id, changes=changes, content_id=cid))

    def add_delete(self, table: str, record_id: str) -> None:
        """Add a single-record delete (record_id may be a '$n' reference)."""
        cid = self._counter[0]
        self._counter[0] += 1
        self.operations.append(_RecordDelete(table=table, ids=record_id, content_id=cid))


# ---------------------------------------------------------------------------
# Changeset batch item
# (_RawRequest is imported from ._raw_request — defined there so _odata.py
#  can also import it without a circular dependency)
# ---------------------------------------------------------------------------


@dataclass
class _ChangeSetBatchItem:
    """A resolved changeset — serialised as a nested multipart/mixed part."""

    requests: List[_RawRequest]


# ---------------------------------------------------------------------------
# Multipart parsing helpers
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
# Batch base: pure serialisation and pure table resolvers
# ---------------------------------------------------------------------------


class _BatchBase:
    """Pure-logic base for the Dataverse batch client.

    Provides multipart serialisation, response parsing, and the subset of
    intent resolvers that require no I/O.  Subclasses must supply ``execute``
    and the I/O-dependent resolvers.

    :param od: The active OData client (provides helpers and HTTP transport).
    """

    def __init__(self, od: "_ODataBase") -> None:
        self._od = od

    # ------------------------------------------------------------------
    # Pure table resolvers — delegate to _ODataBase._build_* methods
    # ------------------------------------------------------------------

    def _resolve_table_create(self, op: _TableCreate) -> List[_RawRequest]:
        return [self._od._build_create_entity(op.table, op.columns, op.solution, op.primary_column, op.display_name)]

    def _resolve_table_get(self, op: _TableGet) -> List[_RawRequest]:
        return [self._od._build_get_entity(op.table)]

    def _resolve_table_list(self, op: _TableList) -> List[_RawRequest]:
        return [self._od._build_list_entities(filter=op.filter, select=op.select)]

    def _resolve_table_create_one_to_many(self, op: _TableCreateOneToMany) -> List[_RawRequest]:
        body = op.relationship.to_dict()
        body["Lookup"] = op.lookup.to_dict()
        return [self._od._build_create_relationship(body, solution=op.solution)]

    def _resolve_table_create_many_to_many(self, op: _TableCreateManyToMany) -> List[_RawRequest]:
        return [self._od._build_create_relationship(op.relationship.to_dict(), solution=op.solution)]

    def _resolve_table_delete_relationship(self, op: _TableDeleteRelationship) -> List[_RawRequest]:
        return [self._od._build_delete_relationship(op.relationship_id)]

    def _resolve_table_get_relationship(self, op: _TableGetRelationship) -> List[_RawRequest]:
        return [self._od._build_get_relationship(op.schema_name)]

    def _resolve_table_create_lookup_field(self, op: _TableCreateLookupField) -> List[_RawRequest]:
        lookup, relationship = self._od._build_lookup_field_models(
            referencing_table=op.referencing_table,
            lookup_field_name=op.lookup_field_name,
            referenced_table=op.referenced_table,
            display_name=op.display_name,
            description=op.description,
            required=op.required,
            cascade_delete=op.cascade_delete,
            language_code=op.language_code,
        )
        body = relationship.to_dict()
        body["Lookup"] = lookup.to_dict()
        return [self._od._build_create_relationship(body, solution=op.solution)]

    # ------------------------------------------------------------------
    # Multipart serialisation
    # ------------------------------------------------------------------

    def _build_batch_body(
        self,
        resolved: List[Union[_RawRequest, _ChangeSetBatchItem]],
        batch_boundary: str,
    ) -> str:
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

    def _serialize_changeset_item(self, cs: _ChangeSetBatchItem, batch_boundary: str) -> str:
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
