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

import json
import uuid
from datetime import datetime, timezone
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

    # ------------------------------------------------------------------
    # Record resolvers (async — inline body-building to avoid sync HTTP calls)
    # ------------------------------------------------------------------

    async def _resolve_record_create(self, op: _RecordCreate) -> List[_RawRequest]:  # type: ignore[override]
        entity_set = await self._od._entity_set_from_schema_name(op.table)
        if isinstance(op.data, dict):
            body = self._od._lowercase_keys(op.data)
            body = await self._od._convert_labels_to_ints(op.table, body)
            return [_RawRequest(
                method="POST",
                url=f"{self._od.api}/{entity_set}",
                body=json.dumps(body, ensure_ascii=False),
                content_id=op.content_id,
            )]
        # Multiple records
        logical_name = op.table.lower()
        enriched = []
        for r in op.data:
            r = self._od._lowercase_keys(r)
            r = await self._od._convert_labels_to_ints(op.table, r)
            if "@odata.type" not in r:
                r = {**r, "@odata.type": f"Microsoft.Dynamics.CRM.{logical_name}"}
            enriched.append(r)
        return [_RawRequest(
            method="POST",
            url=f"{self._od.api}/{entity_set}/Microsoft.Dynamics.CRM.CreateMultiple",
            body=json.dumps({"Targets": enriched}, ensure_ascii=False),
        )]

    async def _resolve_record_update(self, op: _RecordUpdate) -> List[_RawRequest]:  # type: ignore[override]
        if isinstance(op.ids, str):
            if not isinstance(op.changes, dict):
                raise TypeError("For single id, changes must be a dict")
            body = self._od._lowercase_keys(op.changes)
            body = await self._od._convert_labels_to_ints(op.table, body)
            record_id = op.ids
            if record_id.startswith("$"):
                url = record_id
            else:
                entity_set = await self._od._entity_set_from_schema_name(op.table)
                url = f"{self._od.api}/{entity_set}{self._od._format_key(record_id)}"
            return [_RawRequest(
                method="PATCH",
                url=url,
                body=json.dumps(body, ensure_ascii=False),
                headers={"If-Match": "*"},
                content_id=op.content_id,
            )]
        # Multiple records
        entity_set = await self._od._entity_set_from_schema_name(op.table)
        pk_attr = await self._od._primary_id_attr(op.table)
        ids = op.ids
        changes = op.changes
        if isinstance(changes, dict):
            records = [{pk_attr: rid, **changes} for rid in ids]
        elif isinstance(changes, list):
            if len(changes) != len(ids):
                raise ValidationError(
                    "ids and changes lists must have equal length for paired update.",
                    subcode="ids_changes_length_mismatch",
                )
            records = [{pk_attr: rid, **ch} for rid, ch in zip(ids, changes)]
        else:
            raise ValidationError("changes must be a dict or list[dict].", subcode="invalid_changes_type")
        logical_name = op.table.lower()
        enriched = []
        for r in records:
            r = self._od._lowercase_keys(r)
            r = await self._od._convert_labels_to_ints(op.table, r)
            if "@odata.type" not in r:
                r = {**r, "@odata.type": f"Microsoft.Dynamics.CRM.{logical_name}"}
            enriched.append(r)
        return [_RawRequest(
            method="POST",
            url=f"{self._od.api}/{entity_set}/Microsoft.Dynamics.CRM.UpdateMultiple",
            body=json.dumps({"Targets": enriched}, ensure_ascii=False),
        )]

    async def _resolve_record_delete(self, op: _RecordDelete) -> List[_RawRequest]:  # type: ignore[override]
        if isinstance(op.ids, str):
            record_id = op.ids
            if record_id.startswith("$"):
                url = record_id
            else:
                entity_set = await self._od._entity_set_from_schema_name(op.table)
                url = f"{self._od.api}/{entity_set}{self._od._format_key(record_id)}"
            return [_RawRequest(
                method="DELETE",
                url=url,
                headers={"If-Match": "*"},
                content_id=op.content_id,
            )]
        ids = [rid for rid in op.ids if rid]
        if not ids:
            return []
        if op.use_bulk_delete:
            pk_attr = await self._od._primary_id_attr(op.table)
            logical_name = op.table.lower()
            timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
            payload = {
                "JobName": f"Bulk delete {op.table} records @ {timestamp}",
                "SendEmailNotification": False,
                "ToRecipients": [],
                "CCRecipients": [],
                "RecurrencePattern": "",
                "StartDateTime": timestamp,
                "QuerySet": [
                    {
                        "@odata.type": "Microsoft.Dynamics.CRM.QueryExpression",
                        "EntityName": logical_name,
                        "ColumnSet": {
                            "@odata.type": "Microsoft.Dynamics.CRM.ColumnSet",
                            "AllColumns": False,
                            "Columns": [],
                        },
                        "Criteria": {
                            "@odata.type": "Microsoft.Dynamics.CRM.FilterExpression",
                            "FilterOperator": "And",
                            "Conditions": [
                                {
                                    "@odata.type": "Microsoft.Dynamics.CRM.ConditionExpression",
                                    "AttributeName": pk_attr,
                                    "Operator": "In",
                                    "Values": [{"Value": rid, "Type": "System.Guid"} for rid in ids],
                                }
                            ],
                        },
                    }
                ],
            }
            return [_RawRequest(
                method="POST",
                url=f"{self._od.api}/BulkDelete",
                body=json.dumps(payload, ensure_ascii=False),
            )]
        # Sequential deletes — one _RawRequest per record
        result: List[_RawRequest] = []
        entity_set = await self._od._entity_set_from_schema_name(op.table)
        for rid in ids:
            url = f"{self._od.api}/{entity_set}{self._od._format_key(rid)}"
            result.append(_RawRequest(method="DELETE", url=url, headers={"If-Match": "*"}))
        return result

    async def _resolve_record_get(self, op: _RecordGet) -> List[_RawRequest]:  # type: ignore[override]
        entity_set = await self._od._entity_set_from_schema_name(op.table)
        url = f"{self._od.api}/{entity_set}{self._od._format_key(op.record_id)}"
        if op.select:
            url += "?$select=" + ",".join(self._od._lowercase_list(op.select))
        return [_RawRequest(method="GET", url=url)]

    async def _resolve_record_upsert(self, op: _RecordUpsert) -> List[_RawRequest]:  # type: ignore[override]
        entity_set = await self._od._entity_set_from_schema_name(op.table)
        if len(op.items) == 1:
            item = op.items[0]
            body = self._od._lowercase_keys(item.record)
            body = await self._od._convert_labels_to_ints(op.table, body)
            key_str = self._od._build_alternate_key_str(item.alternate_key)
            return [_RawRequest(
                method="PATCH",
                url=f"{self._od.api}/{entity_set}({key_str})",
                body=json.dumps(body, ensure_ascii=False),
            )]
        # Multiple records
        logical_name = op.table.lower()
        targets = []
        for item in op.items:
            alt_key_lower = self._od._lowercase_keys(item.alternate_key)
            record_processed = self._od._lowercase_keys(item.record)
            record_processed = await self._od._convert_labels_to_ints(op.table, record_processed)
            conflicting = {
                k for k in set(alt_key_lower) & set(record_processed)
                if alt_key_lower[k] != record_processed[k]
            }
            if conflicting:
                raise ValidationError(
                    f"record payload conflicts with alternate_key on fields: {sorted(conflicting)!r}",
                    subcode="upsert_key_conflict",
                )
            if "@odata.type" not in record_processed:
                record_processed["@odata.type"] = f"Microsoft.Dynamics.CRM.{logical_name}"
            key_str = self._od._build_alternate_key_str(item.alternate_key)
            record_processed["@odata.id"] = f"{entity_set}({key_str})"
            targets.append(record_processed)
        return [_RawRequest(
            method="POST",
            url=f"{self._od.api}/{entity_set}/Microsoft.Dynamics.CRM.UpsertMultiple",
            body=json.dumps({"Targets": targets}, ensure_ascii=False),
        )]

    # ------------------------------------------------------------------
    # Table resolvers (async — pre-resolve MetadataId, column MetadataId)
    # ------------------------------------------------------------------

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
        return [
            self._od._build_create_column(metadata_id, col_name, dtype)
            for col_name, dtype in op.columns.items()
        ]

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
        from urllib.parse import quote as _url_quote

        sql = op.sql
        logical = self._od._extract_logical_table(sql)
        entity_set = await self._od._entity_set_from_schema_name(logical)
        return [_RawRequest(
            method="GET",
            url=f"{self._od.api}/{entity_set}?sql={_url_quote(sql, safe='')}",
        )]
