# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Async Dataverse OData $batch client.

:class:`_AsyncBatchClient` extends :class:`~PowerPlatform.Dataverse.data._batch._BatchClient`
and overrides every method that performs HTTP I/O (or calls HTTP-touching helpers
on :class:`~PowerPlatform.Dataverse.aio.data._async_odata._AsyncODataClient`) as an
``async def`` coroutine.  Pure helpers — multipart serialisation, response
parsing, intent dataclass dispatch — are inherited unchanged from the sync parent.
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

from ...core._error_codes import METADATA_TABLE_NOT_FOUND, METADATA_COLUMN_NOT_FOUND
from ...core.errors import MetadataError, ValidationError
from ...data._batch import (
    _BatchClient,
    _ChangeSet,
    _ChangeSetBatchItem,
    _MAX_BATCH_SIZE,
    _QuerySql,
    _RecordCreate,
    _RecordDelete,
    _RecordGet,
    _RecordUpsert,
    _RecordUpdate,
    _TableAddColumns,
    _TableCreate,
    _TableCreateLookupField,
    _TableCreateManyToMany,
    _TableCreateOneToMany,
    _TableDelete,
    _TableDeleteRelationship,
    _TableGet,
    _TableGetRelationship,
    _TableList,
    _TableRemoveColumns,
)
from ...data._raw_request import _RawRequest
from ...models.batch import BatchResult

if TYPE_CHECKING:
    from ..data._async_odata import _AsyncODataClient

__all__: list[str] = []


class _AsyncBatchClient(_BatchClient):
    """Async OData ``$batch`` client.

    Serialises intent objects into a multipart request (identical logic to the
    sync :class:`~PowerPlatform.Dataverse.data._batch._BatchClient`), but sends
    the batch request and resolves all metadata dependencies via the async
    :class:`~PowerPlatform.Dataverse.aio.data._async_odata._AsyncODataClient`.

    :param od: Active async OData client instance.
    :type od: ~PowerPlatform.Dataverse.aio.data._async_odata._AsyncODataClient
    """

    def __init__(self, od: "_AsyncODataClient") -> None:
        self._od = od

    # ------------------------------------------------------------------
    # Public entry point (async override)
    # ------------------------------------------------------------------

    async def execute(  # type: ignore[override]
        self,
        items: List[Any],
        continue_on_error: bool = False,
    ) -> BatchResult:
        """Resolve all intent objects, build the batch body, send it, and return results."""
        if not items:
            return BatchResult()

        resolved = await self._resolve_all(items)

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
        response = await self._od._request(
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
    # Intent resolution dispatcher (async overrides)
    # ------------------------------------------------------------------

    async def _resolve_all(  # type: ignore[override]
        self, items: List[Any]
    ) -> List[Union[_RawRequest, _ChangeSetBatchItem]]:
        result: List[Union[_RawRequest, _ChangeSetBatchItem]] = []
        for item in items:
            if isinstance(item, _ChangeSet):
                if not item.operations:
                    # Empty changeset — nothing to send; skip silently.
                    continue
                cs_requests = [await self._resolve_one(op) for op in item.operations]
                result.append(_ChangeSetBatchItem(requests=cs_requests))
            else:
                result.extend(await self._resolve_item(item))
        return result

    async def _resolve_item(self, item: Any) -> List[_RawRequest]:  # type: ignore[override]
        if isinstance(item, _RecordCreate):
            return await self._resolve_record_create(item)
        if isinstance(item, _RecordUpdate):
            return await self._resolve_record_update(item)
        if isinstance(item, _RecordDelete):
            return await self._resolve_record_delete(item)
        if isinstance(item, _RecordGet):
            return await self._resolve_record_get(item)
        if isinstance(item, _RecordUpsert):
            return await self._resolve_record_upsert(item)
        if isinstance(item, _TableCreate):
            return await self._resolve_table_create(item)
        if isinstance(item, _TableDelete):
            return await self._resolve_table_delete(item)
        if isinstance(item, _TableGet):
            return await self._resolve_table_get(item)
        if isinstance(item, _TableList):
            return await self._resolve_table_list(item)
        if isinstance(item, _TableAddColumns):
            return await self._resolve_table_add_columns(item)
        if isinstance(item, _TableRemoveColumns):
            return await self._resolve_table_remove_columns(item)
        if isinstance(item, _TableCreateOneToMany):
            return await self._resolve_table_create_one_to_many(item)
        if isinstance(item, _TableCreateManyToMany):
            return await self._resolve_table_create_many_to_many(item)
        if isinstance(item, _TableDeleteRelationship):
            return await self._resolve_table_delete_relationship(item)
        if isinstance(item, _TableGetRelationship):
            return await self._resolve_table_get_relationship(item)
        if isinstance(item, _TableCreateLookupField):
            return await self._resolve_table_create_lookup_field(item)
        if isinstance(item, _QuerySql):
            return await self._resolve_query_sql(item)
        raise ValidationError(
            f"Unknown batch item type: {type(item).__name__}",
            subcode="unknown_batch_item",
        )

    async def _resolve_one(self, item: Any) -> _RawRequest:  # type: ignore[override]
        resolved = await self._resolve_item(item)
        if len(resolved) != 1:
            raise ValidationError(
                "Changeset operations must each produce exactly one HTTP request.",
                subcode="changeset_multi_request",
            )
        return resolved[0]

    # ------------------------------------------------------------------------
    # Record resolvers (async — inline body-building to avoid sync HTTP calls)
    # ------------------------------------------------------------------------

    async def _resolve_record_create(self, op: _RecordCreate) -> List[_RawRequest]:  # type: ignore[override]
        entity_set = await self._od._entity_set_from_schema_name(op.table)
        if isinstance(op.data, dict):
            return [await self._od._build_create(entity_set, op.table, op.data, content_id=op.content_id)]
        return [await self._od._build_create_multiple(entity_set, op.table, op.data)]

    async def _resolve_record_update(self, op: _RecordUpdate) -> List[_RawRequest]:  # type: ignore[override]
        if isinstance(op.ids, str):
            if not isinstance(op.changes, dict):
                raise TypeError("For single id, changes must be a dict")
            return [await self._od._build_update(op.table, op.ids, op.changes, content_id=op.content_id)]
        entity_set = await self._od._entity_set_from_schema_name(op.table)
        return [await self._od._build_update_multiple(entity_set, op.table, op.ids, op.changes)]

    async def _resolve_record_delete(self, op: _RecordDelete) -> List[_RawRequest]:  # type: ignore[override]
        if isinstance(op.ids, str):
            return [await self._od._build_delete(op.table, op.ids, content_id=op.content_id)]
        ids = [rid for rid in op.ids if rid]
        if not ids:
            return []
        if op.use_bulk_delete:
            return [await self._od._build_delete_multiple(op.table, ids)]
        return [await self._od._build_delete(op.table, rid) for rid in ids]

    async def _resolve_record_get(self, op: _RecordGet) -> List[_RawRequest]:  # type: ignore[override]
        return [await self._od._build_get(op.table, op.record_id, select=op.select)]

    async def _resolve_record_upsert(self, op: _RecordUpsert) -> List[_RawRequest]:  # type: ignore[override]
        entity_set = await self._od._entity_set_from_schema_name(op.table)
        if len(op.items) == 1:
            item = op.items[0]
            return [await self._od._build_upsert(entity_set, op.table, item.alternate_key, item.record)]
        alternate_keys = [i.alternate_key for i in op.items]
        records = [i.record for i in op.items]
        return [await self._od._build_upsert_multiple(entity_set, op.table, alternate_keys, records)]

    # -------------------------------------------------------------------
    # Table resolvers (async — pre-resolve MetadataId, column MetadataId)
    # -------------------------------------------------------------------

    async def _require_entity_metadata(self, table: str) -> str:  # type: ignore[override]
        ent = await self._od._get_entity_by_table_schema_name(table)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{table}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )
        return ent["MetadataId"]

    async def _resolve_table_create(self, op: _TableCreate) -> List[_RawRequest]:  # type: ignore[override]
        return [self._od._build_create_entity(op.table, op.columns, op.solution, op.primary_column)]

    async def _resolve_table_delete(self, op: _TableDelete) -> List[_RawRequest]:  # type: ignore[override]
        metadata_id = await self._require_entity_metadata(op.table)
        return [self._od._build_delete_entity(metadata_id)]

    async def _resolve_table_get(self, op: _TableGet) -> List[_RawRequest]:  # type: ignore[override]
        return [self._od._build_get_entity(op.table)]

    async def _resolve_table_list(self, op: _TableList) -> List[_RawRequest]:  # type: ignore[override]
        return [self._od._build_list_entities(filter=op.filter, select=op.select)]

    async def _resolve_table_add_columns(self, op: _TableAddColumns) -> List[_RawRequest]:  # type: ignore[override]
        metadata_id = await self._require_entity_metadata(op.table)
        return [self._od._build_create_column(metadata_id, col_name, dtype) for col_name, dtype in op.columns.items()]

    async def _resolve_table_remove_columns(self, op: _TableRemoveColumns) -> List[_RawRequest]:  # type: ignore[override]
        columns = [op.columns] if isinstance(op.columns, str) else list(op.columns)
        metadata_id = await self._require_entity_metadata(op.table)
        requests: List[_RawRequest] = []
        for col_name in columns:
            attr_meta = await self._od._get_attribute_metadata(
                metadata_id, col_name, extra_select="@odata.type,AttributeType"
            )
            if not attr_meta or not attr_meta.get("MetadataId"):
                raise MetadataError(
                    f"Column '{col_name}' not found on table '{op.table}'.",
                    subcode=METADATA_COLUMN_NOT_FOUND,
                )
            requests.append(self._od._build_delete_column(metadata_id, attr_meta["MetadataId"]))
        return requests

    async def _resolve_table_create_one_to_many(self, op: _TableCreateOneToMany) -> List[_RawRequest]:  # type: ignore[override]
        body = op.relationship.to_dict()
        body["Lookup"] = op.lookup.to_dict()
        return [self._od._build_create_relationship(body, solution=op.solution)]

    async def _resolve_table_create_many_to_many(self, op: _TableCreateManyToMany) -> List[_RawRequest]:  # type: ignore[override]
        return [self._od._build_create_relationship(op.relationship.to_dict(), solution=op.solution)]

    async def _resolve_table_delete_relationship(self, op: _TableDeleteRelationship) -> List[_RawRequest]:  # type: ignore[override]
        return [self._od._build_delete_relationship(op.relationship_id)]

    async def _resolve_table_get_relationship(self, op: _TableGetRelationship) -> List[_RawRequest]:  # type: ignore[override]
        return [self._od._build_get_relationship(op.schema_name)]

    async def _resolve_table_create_lookup_field(self, op: _TableCreateLookupField) -> List[_RawRequest]:  # type: ignore[override]
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
    # Query resolvers (async)
    # ------------------------------------------------------------------

    async def _resolve_query_sql(self, op: _QuerySql) -> List[_RawRequest]:  # type: ignore[override]
        return [await self._od._build_sql(op.sql)]
