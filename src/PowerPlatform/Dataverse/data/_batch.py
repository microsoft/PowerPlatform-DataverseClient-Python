# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Internal batch intent dataclasses, raw-request builder, and multipart serializer."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from ..core.errors import MetadataError, ValidationError
from ..core._error_codes import METADATA_TABLE_NOT_FOUND, METADATA_COLUMN_NOT_FOUND
from ..models.batch import BatchResult
from ._raw_request import _RawRequest
from ._batch_base import (
    _BatchBase,
    _RecordCreate,
    _RecordUpdate,
    _RecordDelete,
    _RecordGet,
    _RecordUpsert,
    _TableCreate,
    _TableDelete,
    _TableGet,
    _TableList,
    _TableAddColumns,
    _TableRemoveColumns,
    _TableCreateOneToMany,
    _TableCreateManyToMany,
    _TableDeleteRelationship,
    _TableGetRelationship,
    _TableCreateLookupField,
    _QuerySql,
    _ChangeSet,
    _ChangeSetBatchItem,
    _CRLF,
    _MAX_BATCH_SIZE,
    _BOUNDARY_RE,
    _raise_top_level_batch_error,
    _extract_boundary,
    _split_multipart,
    _parse_mime_part,
    _parse_http_response_part,
)

if TYPE_CHECKING:
    from ._odata import _ODataClient

__all__ = []


# ---------------------------------------------------------------------------
# Batch client: resolves intents → raw requests → multipart body → HTTP → result
# ---------------------------------------------------------------------------


class _BatchClient(_BatchBase):
    """
    Serialises a list of intent objects into an OData ``$batch`` multipart/mixed
    request, dispatches it, and parses the response.

    :param od: The active OData client (provides helpers and HTTP transport).
    """

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
            # 400 is expected: Dataverse returns 400 for top-level batch
            # errors (e.g. malformed body). We parse the response body to
            # surface the service error via _parse_batch_response /
            # _raise_top_level_batch_error rather than letting _request raise.
            expected=(200, 202, 207, 400),
        )
        return self._parse_batch_response(response)

    # ------------------------------------------------------------------
    # Intent resolution dispatcher
    # ------------------------------------------------------------------

    def _resolve_all(self, items: List[Any]) -> List[Union[_RawRequest, _ChangeSetBatchItem]]:
        result: List[Union[_RawRequest, _ChangeSetBatchItem]] = []
        for item in items:
            if isinstance(item, _ChangeSet):
                if not item.operations:
                    # Empty changeset — nothing to send; skip silently.
                    continue
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
        entity_set = self._od._entity_set_from_schema_name(op.table)
        if isinstance(op.data, dict):
            return [self._od._build_create(entity_set, op.table, op.data, content_id=op.content_id)]
        return [self._od._build_create_multiple(entity_set, op.table, op.data)]

    def _resolve_record_update(self, op: _RecordUpdate) -> List[_RawRequest]:
        if isinstance(op.ids, str):
            if not isinstance(op.changes, dict):
                raise TypeError("For single id, changes must be a dict")
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

    def _require_entity_metadata(self, table: str) -> str:
        """Look up MetadataId for *table*, raising MetadataError if not found."""
        ent = self._od._get_entity_by_table_schema_name(table)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{table}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )
        return ent["MetadataId"]

    def _resolve_table_delete(self, op: _TableDelete) -> List[_RawRequest]:
        metadata_id = self._require_entity_metadata(op.table)
        return [self._od._build_delete_entity(metadata_id)]

    def _resolve_table_add_columns(self, op: _TableAddColumns) -> List[_RawRequest]:
        metadata_id = self._require_entity_metadata(op.table)
        return [self._od._build_create_column(metadata_id, col_name, dtype) for col_name, dtype in op.columns.items()]

    def _resolve_table_remove_columns(self, op: _TableRemoveColumns) -> List[_RawRequest]:
        columns = [op.columns] if isinstance(op.columns, str) else list(op.columns)
        metadata_id = self._require_entity_metadata(op.table)
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

    # ------------------------------------------------------------------
    # Query resolvers — delegate to _ODataClient._build_* methods
    # ------------------------------------------------------------------

    def _resolve_query_sql(self, op: _QuerySql) -> List[_RawRequest]:
        return [self._od._build_sql(op.sql)]
