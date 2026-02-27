# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Internal batch intent dataclasses, raw-request builder, and multipart serializer."""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from ..core.errors import MetadataError, ValidationError
from ..core._error_codes import METADATA_TABLE_NOT_FOUND, METADATA_COLUMN_NOT_FOUND
from ..models.batch import BatchItemResponse, BatchResult
from ..models.relationship import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
)
from ..models.upsert import UpsertItem
from ..common.constants import CASCADE_BEHAVIOR_REMOVE_LINK
from ._raw_request import _RawRequest
from ._odata import _GUID_RE

if TYPE_CHECKING:
    from ._odata import _ODataClient

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
    """Ordered group of single-record write operations that execute atomically."""

    operations: List[Union[_RecordCreate, _RecordUpdate, _RecordDelete]] = field(default_factory=list)
    _next_content_id: int = field(default=1, init=False, repr=False)

    def add_create(self, table: str, data: Dict[str, Any]) -> str:
        """Add a single-record create; return its content-ID reference string."""
        cid = self._next_content_id
        self._next_content_id += 1
        self.operations.append(_RecordCreate(table=table, data=data, content_id=cid))
        return f"${cid}"

    def add_update(self, table: str, record_id: str, changes: Dict[str, Any]) -> None:
        """Add a single-record update (record_id may be a '$n' reference)."""
        cid = self._next_content_id
        self._next_content_id += 1
        self.operations.append(_RecordUpdate(table=table, ids=record_id, changes=changes, content_id=cid))

    def add_delete(self, table: str, record_id: str) -> None:
        """Add a single-record delete (record_id may be a '$n' reference)."""
        cid = self._next_content_id
        self._next_content_id += 1
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
# Batch client: resolves intents → raw requests → multipart body → HTTP → result
# ---------------------------------------------------------------------------


class _BatchClient:
    """
    Serialises a list of intent objects into an OData ``$batch`` multipart/mixed
    request, dispatches it, and parses the response.

    :param od: The active OData client (provides helpers and HTTP transport).
    """

    def __init__(self, od: "_ODataClient") -> None:
        self._od = od

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def execute(
        self,
        items: List[Any],
        continue_on_error: bool = False,
    ) -> BatchResult:
        """
        Resolve all intent objects, build the batch body, send it, and return results.

        Metadata pre-resolution (GET calls for MetadataId) happens here, synchronously,
        before the multipart body is assembled.
        """
        if not items:
            return BatchResult()

        resolved = self._resolve_all(items)

        total = sum(len(r.requests) if isinstance(r, _ChangeSetBatchItem) else 1 for r in resolved)
        if total > _MAX_BATCH_SIZE:
            raise ValidationError(
                f"Batch contains {total} operations, which exceeds the limit of "
                f"{_MAX_BATCH_SIZE}. Split into multiple batches.",
                subcode="batch_size_exceeded",
                details={"count": total, "max": _MAX_BATCH_SIZE},
            )

        batch_boundary = f"batch_{uuid.uuid4()}"
        body = self._build_batch_body(resolved, batch_boundary)

        headers: Dict[str, str] = {
            "Content-Type": f'multipart/mixed; boundary="{batch_boundary}"',
        }
        if continue_on_error:
            headers["Prefer"] = "odata.continue-on-error"

        url = f"{self._od.api}/$batch"
        response = self._od._request(
            "post",
            url,
            data=body.encode("utf-8"),
            headers=headers,
            expected=(200, 400),
        )
        return self._parse_batch_response(response)

    # ------------------------------------------------------------------
    # Intent resolution dispatcher
    # ------------------------------------------------------------------

    def _resolve_all(self, items: List[Any]) -> List[Union[_RawRequest, _ChangeSetBatchItem]]:
        result: List[Union[_RawRequest, _ChangeSetBatchItem]] = []
        for item in items:
            if isinstance(item, _ChangeSet):
                cs_requests = [self._resolve_one(op) for op in item.operations]
                result.append(_ChangeSetBatchItem(requests=cs_requests))
            else:
                result.extend(self._resolve_item(item))
        return result

    def _resolve_item(self, item: Any) -> List[_RawRequest]:
        """Resolve a single intent to one or more _RawRequest objects."""
        if isinstance(item, _RecordCreate):
            return self._resolve_record_create(item)
        if isinstance(item, _RecordUpdate):
            return self._resolve_record_update(item)
        if isinstance(item, _RecordDelete):
            return self._resolve_record_delete(item)
        if isinstance(item, _RecordGet):
            return self._resolve_record_get(item)
        if isinstance(item, _RecordUpsert):
            return self._resolve_record_upsert(item)
        if isinstance(item, _TableCreate):
            return self._resolve_table_create(item)
        if isinstance(item, _TableDelete):
            return self._resolve_table_delete(item)
        if isinstance(item, _TableGet):
            return self._resolve_table_get(item)
        if isinstance(item, _TableList):
            return self._resolve_table_list(item)
        if isinstance(item, _TableAddColumns):
            return self._resolve_table_add_columns(item)
        if isinstance(item, _TableRemoveColumns):
            return self._resolve_table_remove_columns(item)
        if isinstance(item, _TableCreateOneToMany):
            return self._resolve_table_create_one_to_many(item)
        if isinstance(item, _TableCreateManyToMany):
            return self._resolve_table_create_many_to_many(item)
        if isinstance(item, _TableDeleteRelationship):
            return self._resolve_table_delete_relationship(item)
        if isinstance(item, _TableGetRelationship):
            return self._resolve_table_get_relationship(item)
        if isinstance(item, _TableCreateLookupField):
            return self._resolve_table_create_lookup_field(item)
        if isinstance(item, _QuerySql):
            return self._resolve_query_sql(item)
        raise ValidationError(
            f"Unknown batch item type: {type(item).__name__}",
            subcode="unknown_batch_item",
        )

    def _resolve_one(self, item: Any) -> _RawRequest:
        """Resolve a changeset operation to exactly one _RawRequest."""
        resolved = self._resolve_item(item)
        if len(resolved) != 1:
            raise ValidationError(
                "Changeset operations must each produce exactly one HTTP request.",
                subcode="changeset_multi_request",
            )
        return resolved[0]

    # ------------------------------------------------------------------
    # Record resolvers — delegate to _ODataClient._build_* methods
    # ------------------------------------------------------------------

    def _resolve_record_create(self, op: _RecordCreate) -> List[_RawRequest]:
        if isinstance(op.data, dict):
            entity_set = self._od._entity_set_from_schema_name(op.table)
            return [self._od._build_create(entity_set, op.table, op.data, content_id=op.content_id)]
        entity_set = self._od._entity_set_from_schema_name(op.table)
        return [self._od._build_create_multiple(entity_set, op.table, op.data)]

    def _resolve_record_update(self, op: _RecordUpdate) -> List[_RawRequest]:
        if isinstance(op.ids, str):
            if not isinstance(op.changes, dict):
                raise ValidationError(
                    "For a single-record update, changes must be a dict.",
                    subcode="invalid_changes_type",
                )
            return [self._od._build_update(op.table, op.ids, op.changes, content_id=op.content_id)]
        entity_set = self._od._entity_set_from_schema_name(op.table)
        return [self._od._build_update_multiple(entity_set, op.table, op.ids, op.changes)]

    def _resolve_record_delete(self, op: _RecordDelete) -> List[_RawRequest]:
        if isinstance(op.ids, str):
            return [self._od._build_delete(op.table, op.ids, content_id=op.content_id)]
        ids = [rid for rid in op.ids if rid]
        if not ids:
            return []
        if op.use_bulk_delete:
            return [self._od._build_delete_multiple(op.table, ids)]
        return [self._od._build_delete(op.table, rid) for rid in ids]

    def _resolve_record_get(self, op: _RecordGet) -> List[_RawRequest]:
        return [self._od._build_get(op.table, op.record_id, select=op.select)]

    def _resolve_record_upsert(self, op: _RecordUpsert) -> List[_RawRequest]:
        entity_set = self._od._entity_set_from_schema_name(op.table)
        if len(op.items) == 1:
            item = op.items[0]
            return [self._od._build_upsert(entity_set, op.table, item.alternate_key, item.record)]
        alternate_keys = [i.alternate_key for i in op.items]
        records = [i.record for i in op.items]
        return [self._od._build_upsert_multiple(entity_set, op.table, alternate_keys, records)]

    # ------------------------------------------------------------------
    # Table resolvers — delegate to _ODataClient._build_* methods
    # (pre-resolution GETs for MetadataId remain here; they are batch-
    #  specific lookups needed before the relevant _build_* call)
    # ------------------------------------------------------------------

    def _resolve_table_create(self, op: _TableCreate) -> List[_RawRequest]:
        return [self._od._build_create_entity(op.table, op.columns, op.solution, op.primary_column)]

    def _resolve_table_delete(self, op: _TableDelete) -> List[_RawRequest]:
        ent = self._od._get_entity_by_table_schema_name(op.table)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{op.table}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )
        return [self._od._build_delete_entity(ent["MetadataId"])]

    def _resolve_table_get(self, op: _TableGet) -> List[_RawRequest]:
        return [self._od._build_get_entity(op.table)]

    def _resolve_table_list(self, op: _TableList) -> List[_RawRequest]:
        return [self._od._build_list_entities(filter=op.filter, select=op.select)]

    def _resolve_table_add_columns(self, op: _TableAddColumns) -> List[_RawRequest]:
        ent = self._od._get_entity_by_table_schema_name(op.table)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{op.table}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )
        return [
            self._od._build_create_column(ent["MetadataId"], col_name, dtype) for col_name, dtype in op.columns.items()
        ]

    def _resolve_table_remove_columns(self, op: _TableRemoveColumns) -> List[_RawRequest]:
        columns = [op.columns] if isinstance(op.columns, str) else list(op.columns)
        ent = self._od._get_entity_by_table_schema_name(op.table)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{op.table}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )
        metadata_id = ent["MetadataId"]
        requests: List[_RawRequest] = []
        for col_name in columns:
            attr_meta = self._od._get_attribute_metadata(
                metadata_id, col_name, extra_select="@odata.type,AttributeType"
            )
            if not attr_meta or not attr_meta.get("MetadataId"):
                raise MetadataError(
                    f"Column '{col_name}' not found on table '{op.table}'.",
                    subcode=METADATA_COLUMN_NOT_FOUND,
                )
            requests.append(self._od._build_delete_column(metadata_id, attr_meta["MetadataId"]))
        return requests

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
    # Query resolvers — delegate to _ODataClient._build_* methods
    # ------------------------------------------------------------------

    def _resolve_query_sql(self, op: _QuerySql) -> List[_RawRequest]:
        return [self._od._build_sql(op.sql)]

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
            return BatchResult()
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
                item = _parse_http_response_part(part_body, content_id=None)
                if item is not None:
                    responses.append(item)
        return BatchResult(responses=responses)


# ---------------------------------------------------------------------------
# Multipart parsing helpers
# ---------------------------------------------------------------------------


def _extract_boundary(content_type: str) -> Optional[str]:
    m = re.search(r'boundary="?([^";,\s]+)"?', content_type, re.IGNORECASE)
    return m.group(1) if m else None


def _split_multipart(body: str, boundary: str) -> List[tuple]:
    delimiter = f"--{boundary}"
    parts: List[tuple] = []
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


def _parse_mime_part(raw: str) -> tuple:
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
