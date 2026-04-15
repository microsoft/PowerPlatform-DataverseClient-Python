# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for _AsyncBatchClient."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from PowerPlatform.Dataverse.aio.data._async_batch import _AsyncBatchClient
from PowerPlatform.Dataverse.data._batch import (
    _ChangeSet,
    _QuerySql,
    _RecordCreate,
    _RecordDelete,
    _RecordGet,
    _RecordUpdate,
    _RecordUpsert,
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
from PowerPlatform.Dataverse.data._raw_request import _RawRequest
from PowerPlatform.Dataverse.models.batch import BatchResult
from PowerPlatform.Dataverse.models.upsert import UpsertItem
from PowerPlatform.Dataverse.core.errors import MetadataError, ValidationError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_od():
    """Return a mock _AsyncODataClient with common attributes."""
    od = AsyncMock()
    od.api = "https://example.crm.dynamics.com/api/data/v9.2"
    # sync helpers
    od._lowercase_keys = MagicMock(side_effect=lambda d: d)
    od._lowercase_list = MagicMock(side_effect=lambda lst: [s.lower() for s in lst] if lst else lst)
    od._format_key = MagicMock(side_effect=lambda k: f"({k})")
    od._build_alternate_key_str = MagicMock(return_value="accountnumber='ACC-1'")
    od._build_lookup_field_models = MagicMock(return_value=(MagicMock(), MagicMock()))
    od._build_get_entity = MagicMock(
        return_value=_RawRequest("GET", "https://example.crm.dynamics.com/api/data/v9.2/EntityDefinitions(meta-1)")
    )
    od._build_list_entities = MagicMock(
        return_value=_RawRequest("GET", "https://example.crm.dynamics.com/api/data/v9.2/EntityDefinitions")
    )
    od._build_create_entity = MagicMock(
        return_value=_RawRequest("POST", "https://example.crm.dynamics.com/api/data/v9.2/EntityDefinitions")
    )
    od._build_delete_entity = MagicMock(
        return_value=_RawRequest("DELETE", "https://example.crm.dynamics.com/api/data/v9.2/EntityDefinitions(meta-1)")
    )
    od._build_create_column = MagicMock(
        return_value=_RawRequest(
            "POST", "https://example.crm.dynamics.com/api/data/v9.2/EntityDefinitions(meta-1)/Attributes"
        )
    )
    od._build_delete_column = MagicMock(
        return_value=_RawRequest(
            "DELETE", "https://example.crm.dynamics.com/api/data/v9.2/EntityDefinitions(meta-1)/Attributes(attr-1)"
        )
    )
    od._build_create_relationship = MagicMock(
        return_value=_RawRequest("POST", "https://example.crm.dynamics.com/api/data/v9.2/RelationshipDefinitions")
    )
    od._build_create_one_to_many = MagicMock(
        return_value=_RawRequest("POST", "https://example.crm.dynamics.com/api/data/v9.2/Relationships")
    )
    od._build_create_many_to_many = MagicMock(
        return_value=_RawRequest("POST", "https://example.crm.dynamics.com/api/data/v9.2/Relationships")
    )
    od._build_delete_relationship = MagicMock(
        return_value=_RawRequest(
            "DELETE", "https://example.crm.dynamics.com/api/data/v9.2/RelationshipDefinitions(rel-1)"
        )
    )
    od._build_get_relationship = MagicMock(
        return_value=_RawRequest(
            "GET",
            "https://example.crm.dynamics.com/api/data/v9.2/RelationshipDefinitions?$filter=SchemaName eq 'schema'",
        )
    )
    od._extract_logical_table = MagicMock(return_value="account")
    # async helpers
    od._entity_set_from_schema_name = AsyncMock(return_value="accounts")
    od._primary_id_attr = AsyncMock(return_value="accountid")
    od._convert_labels_to_ints = AsyncMock(side_effect=lambda table, rec: rec)
    od._get_entity_by_table_schema_name = AsyncMock(return_value={"MetadataId": "meta-1", "EntitySetName": "accounts"})
    od._get_attribute_metadata = AsyncMock(return_value={"MetadataId": "attr-1"})

    # async _build_* methods — return realistic _RawRequest objects
    _API = od.api

    async def _build_create(entity_set, table, data, *, content_id=None):
        return _RawRequest(
            method="POST",
            url=f"{_API}/{entity_set}",
            body=json.dumps(data, ensure_ascii=False),
            content_id=content_id,
        )

    async def _build_create_multiple(entity_set, table, records):
        logical = table.lower()
        enriched = [
            {**r, "@odata.type": f"Microsoft.Dynamics.CRM.{logical}"} if "@odata.type" not in r else r for r in records
        ]
        return _RawRequest(
            method="POST",
            url=f"{_API}/{entity_set}/Microsoft.Dynamics.CRM.CreateMultiple",
            body=json.dumps({"Targets": enriched}, ensure_ascii=False),
        )

    async def _build_update(table, record_id, changes, *, content_id=None):
        url = record_id if record_id.startswith("$") else f"{_API}/accounts({record_id})"
        return _RawRequest(
            method="PATCH",
            url=url,
            body=json.dumps(changes, ensure_ascii=False),
            headers={"If-Match": "*"},
            content_id=content_id,
        )

    async def _build_update_multiple(entity_set, table, ids, changes):
        pk_attr = "accountid"
        logical = table.lower()
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
        enriched = [
            {**r, "@odata.type": f"Microsoft.Dynamics.CRM.{logical}"} if "@odata.type" not in r else r for r in records
        ]
        return _RawRequest(
            method="POST",
            url=f"{_API}/{entity_set}/Microsoft.Dynamics.CRM.UpdateMultiple",
            body=json.dumps({"Targets": enriched}, ensure_ascii=False),
        )

    async def _build_upsert(entity_set, table, alternate_key, record):
        key_str = od._build_alternate_key_str(alternate_key)
        return _RawRequest(
            method="PATCH",
            url=f"{_API}/{entity_set}({key_str})",
            body=json.dumps(record, ensure_ascii=False),
        )

    async def _build_upsert_multiple(entity_set, table, alternate_keys, records):
        logical = table.lower()
        if len(alternate_keys) != len(records):
            raise ValidationError(
                f"alternate_keys and records must have the same length " f"({len(alternate_keys)} != {len(records)})",
                subcode="upsert_length_mismatch",
            )
        targets = []
        for alt_key, record in zip(alternate_keys, records):
            alt_key_lower = {k.lower(): v for k, v in alt_key.items()}
            rec = {k.lower(): v for k, v in record.items()}
            conflicting = {k for k in set(alt_key_lower) & set(rec) if alt_key_lower[k] != rec[k]}
            if conflicting:
                raise ValidationError(
                    f"record payload conflicts with alternate_key on fields: {sorted(conflicting)!r}",
                    subcode="upsert_key_conflict",
                )
            if "@odata.type" not in rec:
                rec["@odata.type"] = f"Microsoft.Dynamics.CRM.{logical}"
            key_str = od._build_alternate_key_str(alt_key)
            rec["@odata.id"] = f"{entity_set}({key_str})"
            targets.append(rec)
        return _RawRequest(
            method="POST",
            url=f"{_API}/{entity_set}/Microsoft.Dynamics.CRM.UpsertMultiple",
            body=json.dumps({"Targets": targets}, ensure_ascii=False),
        )

    async def _build_delete(table, record_id, *, content_id=None):
        url = record_id if record_id.startswith("$") else f"{_API}/accounts({record_id})"
        return _RawRequest(
            method="DELETE",
            url=url,
            headers={"If-Match": "*"},
            content_id=content_id,
        )

    async def _build_delete_multiple(table, ids):
        return _RawRequest(
            method="POST",
            url=f"{_API}/BulkDelete",
            body=json.dumps({"ids": ids}, ensure_ascii=False),
        )

    async def _build_get(table, record_id, *, select=None):
        url = f"{_API}/accounts({record_id})"
        if select:
            url += "?$select=" + ",".join(s.lower() for s in select)
        return _RawRequest(method="GET", url=url)

    async def _build_sql(sql):
        from urllib.parse import quote as _url_quote

        return _RawRequest(
            method="GET",
            url=f"{_API}/accounts?sql={_url_quote(sql, safe='')}",
        )

    od._build_create = AsyncMock(side_effect=_build_create)
    od._build_create_multiple = AsyncMock(side_effect=_build_create_multiple)
    od._build_update = AsyncMock(side_effect=_build_update)
    od._build_update_multiple = AsyncMock(side_effect=_build_update_multiple)
    od._build_upsert = AsyncMock(side_effect=_build_upsert)
    od._build_upsert_multiple = AsyncMock(side_effect=_build_upsert_multiple)
    od._build_delete = AsyncMock(side_effect=_build_delete)
    od._build_delete_multiple = AsyncMock(side_effect=_build_delete_multiple)
    od._build_get = AsyncMock(side_effect=_build_get)
    od._build_sql = AsyncMock(side_effect=_build_sql)
    return od


def _make_client(od=None):
    if od is None:
        od = _make_od()
    return _AsyncBatchClient(od), od


# ---------------------------------------------------------------------------
# 1. TestAsyncBatchClientExecute
# ---------------------------------------------------------------------------


class TestAsyncBatchClientExecute:
    async def test_execute_empty_returns_empty_batch_result(self):
        bc, od = _make_client()
        result = await bc.execute([])
        assert isinstance(result, BatchResult)
        assert result.responses == []
        od._request.assert_not_called()

    async def test_execute_with_items_sends_post_request(self):
        bc, od = _make_client()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "--batchresponse\r\n--batchresponse--\r\n"
        od._request.return_value = mock_response

        with patch.object(bc, "_resolve_all", new=AsyncMock(return_value=[_RawRequest("POST", "http://x.com")])):
            with patch.object(bc, "_parse_batch_response", return_value=BatchResult()):
                result = await bc.execute([_RecordCreate(table="account", data={})])

        od._request.assert_called_once()
        call_args = od._request.call_args
        assert call_args[0][0] == "post"
        assert call_args[0][1].endswith("/$batch")
        assert isinstance(result, BatchResult)

    async def test_execute_continue_on_error_sets_prefer_header(self):
        bc, od = _make_client()
        mock_response = MagicMock()
        od._request.return_value = mock_response

        with patch.object(bc, "_resolve_all", new=AsyncMock(return_value=[_RawRequest("POST", "http://x.com")])):
            with patch.object(bc, "_parse_batch_response", return_value=BatchResult()):
                await bc.execute([_RecordCreate(table="account", data={})], continue_on_error=True)

        call_kwargs = od._request.call_args[1]
        assert call_kwargs.get("headers", {}).get("Prefer") == "odata.continue-on-error"

    async def test_execute_without_continue_on_error_no_prefer_header(self):
        bc, od = _make_client()
        mock_response = MagicMock()
        od._request.return_value = mock_response

        with patch.object(bc, "_resolve_all", new=AsyncMock(return_value=[_RawRequest("POST", "http://x.com")])):
            with patch.object(bc, "_parse_batch_response", return_value=BatchResult()):
                await bc.execute([_RecordCreate(table="account", data={})])

        call_kwargs = od._request.call_args[1]
        assert "Prefer" not in call_kwargs.get("headers", {})

    async def test_execute_exceeding_1000_raises_validation_error(self):
        bc, od = _make_client()
        # 1001 items resolved to 1001 raw requests
        big_list = [_RawRequest("POST", f"http://x.com/{i}") for i in range(1001)]
        with patch.object(bc, "_resolve_all", new=AsyncMock(return_value=big_list)):
            with pytest.raises(ValidationError, match="1001"):
                await bc.execute([_RecordCreate(table="account", data={})] * 1001)


# ---------------------------------------------------------------------------
# 2. TestAsyncBatchClientResolveAll
# ---------------------------------------------------------------------------


class TestAsyncBatchClientResolveAll:
    async def test_empty_changeset_is_skipped(self):
        bc, od = _make_client()
        cs = _ChangeSet()  # no operations
        result = await bc._resolve_all([cs])
        assert result == []

    async def test_changeset_with_one_op_produces_changeset_batch_item(self):
        from PowerPlatform.Dataverse.data._batch import _ChangeSetBatchItem

        bc, od = _make_client()
        cs = _ChangeSet()
        cs.add_create("account", {"name": "Test"})

        result = await bc._resolve_all([cs])

        assert len(result) == 1
        assert isinstance(result[0], _ChangeSetBatchItem)
        assert len(result[0].requests) == 1

    async def test_non_changeset_item_is_extended(self):
        bc, od = _make_client()
        op = _RecordCreate(table="account", data={"name": "Acme"})
        result = await bc._resolve_all([op])
        # Should have exactly one _RawRequest
        assert len(result) == 1
        assert isinstance(result[0], _RawRequest)

    async def test_mixed_items_both_resolved(self):
        from PowerPlatform.Dataverse.data._batch import _ChangeSetBatchItem

        bc, od = _make_client()
        cs = _ChangeSet()
        cs.add_create("account", {"name": "CS"})
        op = _RecordCreate(table="account", data={"name": "Direct"})

        result = await bc._resolve_all([cs, op])
        assert len(result) == 2
        assert isinstance(result[0], _ChangeSetBatchItem)
        assert isinstance(result[1], _RawRequest)


# ---------------------------------------------------------------------------
# 3. TestAsyncBatchClientResolveRecordCreate
# ---------------------------------------------------------------------------


class TestAsyncBatchClientResolveRecordCreate:
    async def test_single_dict_returns_post_to_entity_set(self):
        bc, od = _make_client()
        op = _RecordCreate(table="account", data={"name": "Acme"})
        result = await bc._resolve_record_create(op)

        assert len(result) == 1
        req = result[0]
        assert req.method == "POST"
        assert req.url == "https://example.crm.dynamics.com/api/data/v9.2/accounts"
        od._entity_set_from_schema_name.assert_called_once_with("account")
        od._build_create.assert_called_once_with("accounts", "account", {"name": "Acme"}, content_id=None)

    async def test_list_data_returns_create_multiple(self):
        bc, od = _make_client()
        op = _RecordCreate(table="account", data=[{"name": "A"}, {"name": "B"}])
        result = await bc._resolve_record_create(op)

        assert len(result) == 1
        req = result[0]
        assert req.method == "POST"
        assert "CreateMultiple" in req.url
        body = json.loads(req.body)
        assert "Targets" in body
        assert all("@odata.type" in t for t in body["Targets"])

    async def test_list_data_includes_odata_type(self):
        bc, od = _make_client()
        op = _RecordCreate(table="account", data=[{"name": "A"}])
        result = await bc._resolve_record_create(op)
        body = json.loads(result[0].body)
        assert body["Targets"][0]["@odata.type"] == "Microsoft.Dynamics.CRM.account"

    async def test_content_id_is_passed_through(self):
        bc, od = _make_client()
        op = _RecordCreate(table="account", data={"name": "X"}, content_id=5)
        result = await bc._resolve_record_create(op)
        assert result[0].content_id == 5


# ---------------------------------------------------------------------------
# 4. TestAsyncBatchClientResolveRecordUpdate
# ---------------------------------------------------------------------------


class TestAsyncBatchClientResolveRecordUpdate:
    async def test_single_str_id_returns_patch(self):
        bc, od = _make_client()
        op = _RecordUpdate(table="account", ids="guid-1", changes={"name": "New"})
        result = await bc._resolve_record_update(op)

        assert len(result) == 1
        req = result[0]
        assert req.method == "PATCH"
        assert "(guid-1)" in req.url
        assert req.headers.get("If-Match") == "*"

    async def test_content_id_reference_uses_content_id_as_url(self):
        bc, od = _make_client()
        op = _RecordUpdate(table="account", ids="$1", changes={"name": "New"})
        result = await bc._resolve_record_update(op)

        assert len(result) == 1
        req = result[0]
        assert req.url == "$1"
        # entity_set_from_schema_name should NOT be called for content-id refs
        od._entity_set_from_schema_name.assert_not_called()

    async def test_list_ids_broadcast_dict_returns_update_multiple(self):
        bc, od = _make_client()
        op = _RecordUpdate(table="account", ids=["id-1", "id-2"], changes={"name": "X"})
        result = await bc._resolve_record_update(op)

        assert len(result) == 1
        req = result[0]
        assert req.method == "POST"
        assert "UpdateMultiple" in req.url
        body = json.loads(req.body)
        assert len(body["Targets"]) == 2

    async def test_list_ids_paired_list_returns_update_multiple(self):
        bc, od = _make_client()
        op = _RecordUpdate(
            table="account",
            ids=["id-1", "id-2"],
            changes=[{"name": "A"}, {"name": "B"}],
        )
        result = await bc._resolve_record_update(op)

        assert len(result) == 1
        body = json.loads(result[0].body)
        assert len(body["Targets"]) == 2

    async def test_mismatched_ids_changes_lengths_raises_validation_error(self):
        bc, od = _make_client()
        op = _RecordUpdate(table="account", ids=["id-1", "id-2"], changes=[{"name": "A"}])
        with pytest.raises(ValidationError, match="equal length"):
            await bc._resolve_record_update(op)

    async def test_invalid_changes_type_raises_validation_error(self):
        bc, od = _make_client()
        op = _RecordUpdate(table="account", ids=["id-1"], changes="invalid")  # type: ignore[arg-type]
        with pytest.raises(ValidationError, match="changes must be"):
            await bc._resolve_record_update(op)


# ---------------------------------------------------------------------------
# 5. TestAsyncBatchClientResolveRecordDelete
# ---------------------------------------------------------------------------


class TestAsyncBatchClientResolveRecordDelete:
    async def test_single_str_id_returns_delete(self):
        bc, od = _make_client()
        op = _RecordDelete(table="account", ids="guid-1")
        result = await bc._resolve_record_delete(op)

        assert len(result) == 1
        req = result[0]
        assert req.method == "DELETE"
        assert "(guid-1)" in req.url
        assert req.headers.get("If-Match") == "*"

    async def test_content_id_reference_uses_content_id_as_url(self):
        bc, od = _make_client()
        op = _RecordDelete(table="account", ids="$2")
        result = await bc._resolve_record_delete(op)

        assert len(result) == 1
        assert result[0].url == "$2"

    async def test_list_use_bulk_delete_true_returns_single_bulk_delete_post(self):
        bc, od = _make_client()
        op = _RecordDelete(table="account", ids=["id-1", "id-2"], use_bulk_delete=True)
        result = await bc._resolve_record_delete(op)

        assert len(result) == 1
        req = result[0]
        assert req.method == "POST"
        assert req.url.endswith("/BulkDelete")

    async def test_list_use_bulk_delete_false_returns_one_delete_per_id(self):
        bc, od = _make_client()
        op = _RecordDelete(table="account", ids=["id-1", "id-2"], use_bulk_delete=False)
        result = await bc._resolve_record_delete(op)

        assert len(result) == 2
        assert all(r.method == "DELETE" for r in result)

    async def test_empty_list_returns_empty(self):
        bc, od = _make_client()
        op = _RecordDelete(table="account", ids=[])
        result = await bc._resolve_record_delete(op)
        assert result == []

    async def test_all_empty_string_ids_returns_empty(self):
        bc, od = _make_client()
        op = _RecordDelete(table="account", ids=["", ""])
        result = await bc._resolve_record_delete(op)
        assert result == []


# ---------------------------------------------------------------------------
# 6. TestAsyncBatchClientResolveRecordGet
# ---------------------------------------------------------------------------


class TestAsyncBatchClientResolveRecordGet:
    async def test_returns_get_request(self):
        bc, od = _make_client()
        op = _RecordGet(table="account", record_id="guid-1")
        result = await bc._resolve_record_get(op)

        assert len(result) == 1
        req = result[0]
        assert req.method == "GET"
        assert "accounts" in req.url
        assert "(guid-1)" in req.url

    async def test_with_select_includes_select_param(self):
        bc, od = _make_client()
        od._lowercase_list = MagicMock(side_effect=lambda lst: [s.lower() for s in lst])
        op = _RecordGet(table="account", record_id="guid-1", select=["Name", "Email"])
        result = await bc._resolve_record_get(op)

        assert len(result) == 1
        assert "$select=name,email" in result[0].url


# ---------------------------------------------------------------------------
# 7. TestAsyncBatchClientResolveRecordUpsert
# ---------------------------------------------------------------------------


class TestAsyncBatchClientResolveRecordUpsert:
    async def test_single_item_returns_patch_to_alternate_key(self):
        bc, od = _make_client()
        item = UpsertItem(alternate_key={"accountnumber": "ACC-1"}, record={"name": "Acme"})
        op = _RecordUpsert(table="account", items=[item])
        result = await bc._resolve_record_upsert(op)

        assert len(result) == 1
        req = result[0]
        assert req.method == "PATCH"
        assert "accountnumber='ACC-1'" in req.url
        od._build_alternate_key_str.assert_called_once()

    async def test_multiple_items_returns_upsert_multiple(self):
        bc, od = _make_client()
        items = [
            UpsertItem(alternate_key={"accountnumber": "ACC-1"}, record={"name": "Acme"}),
            UpsertItem(alternate_key={"accountnumber": "ACC-2"}, record={"name": "Contoso"}),
        ]
        op = _RecordUpsert(table="account", items=items)
        result = await bc._resolve_record_upsert(op)

        assert len(result) == 1
        req = result[0]
        assert req.method == "POST"
        assert "UpsertMultiple" in req.url
        body = json.loads(req.body)
        assert len(body["Targets"]) == 2


# ---------------------------------------------------------------------------
# 8. TestAsyncBatchClientResolveTableOps
# ---------------------------------------------------------------------------


class TestAsyncBatchClientResolveTableOps:
    async def test_table_create_calls_build_create_entity(self):
        bc, od = _make_client()
        op = _TableCreate(table="new_Account", columns={"new_Name": "string"})
        result = await bc._resolve_table_create(op)

        od._build_create_entity.assert_called_once_with("new_Account", {"new_Name": "string"}, None, None)
        assert len(result) == 1
        assert isinstance(result[0], _RawRequest)

    async def test_table_delete_calls_get_entity_and_build_delete_entity(self):
        bc, od = _make_client()
        op = _TableDelete(table="account")
        result = await bc._resolve_table_delete(op)

        od._get_entity_by_table_schema_name.assert_called_once_with("account")
        od._build_delete_entity.assert_called_once_with("meta-1")
        assert len(result) == 1

    async def test_table_delete_entity_not_found_raises_metadata_error(self):
        bc, od = _make_client()
        od._get_entity_by_table_schema_name.return_value = None
        op = _TableDelete(table="missing_table")
        with pytest.raises(MetadataError):
            await bc._resolve_table_delete(op)

    async def test_table_delete_missing_metadata_id_raises_metadata_error(self):
        bc, od = _make_client()
        od._get_entity_by_table_schema_name.return_value = {"EntitySetName": "accounts"}  # no MetadataId
        op = _TableDelete(table="account")
        with pytest.raises(MetadataError):
            await bc._resolve_table_delete(op)

    async def test_table_get_calls_build_get_entity(self):
        bc, od = _make_client()
        op = _TableGet(table="account")
        result = await bc._resolve_table_get(op)

        od._build_get_entity.assert_called_once_with("account")
        assert len(result) == 1

    async def test_table_list_calls_build_list_entities(self):
        bc, od = _make_client()
        op = _TableList(filter="IsCustomEntity eq true", select=["SchemaName"])
        result = await bc._resolve_table_list(op)

        od._build_list_entities.assert_called_once_with(filter="IsCustomEntity eq true", select=["SchemaName"])
        assert len(result) == 1

    async def test_table_add_columns_returns_list_of_requests(self):
        bc, od = _make_client()
        op = _TableAddColumns(table="account", columns={"new_col1": "string", "new_col2": "int"})
        result = await bc._resolve_table_add_columns(op)

        od._get_entity_by_table_schema_name.assert_called_once_with("account")
        assert od._build_create_column.call_count == 2
        assert len(result) == 2

    async def test_table_remove_columns_calls_get_attribute_metadata_per_column(self):
        bc, od = _make_client()
        op = _TableRemoveColumns(table="account", columns=["col1", "col2"])
        result = await bc._resolve_table_remove_columns(op)

        assert od._get_attribute_metadata.call_count == 2
        assert od._build_delete_column.call_count == 2
        assert len(result) == 2

    async def test_table_remove_columns_column_not_found_raises_metadata_error(self):
        bc, od = _make_client()
        od._get_attribute_metadata.return_value = None
        op = _TableRemoveColumns(table="account", columns=["missing_col"])
        with pytest.raises(MetadataError):
            await bc._resolve_table_remove_columns(op)

    async def test_table_remove_columns_missing_metadata_id_raises_metadata_error(self):
        bc, od = _make_client()
        od._get_attribute_metadata.return_value = {"AttributeType": "String"}  # no MetadataId
        op = _TableRemoveColumns(table="account", columns=["col1"])
        with pytest.raises(MetadataError):
            await bc._resolve_table_remove_columns(op)

    async def test_table_create_one_to_many_calls_build_create_relationship(self):
        bc, od = _make_client()
        from PowerPlatform.Dataverse.models.relationship import (
            LookupAttributeMetadata,
            OneToManyRelationshipMetadata,
        )

        lookup = MagicMock(spec=LookupAttributeMetadata)
        lookup.to_dict = MagicMock(return_value={"SchemaName": "lookup_field"})
        rel = MagicMock(spec=OneToManyRelationshipMetadata)
        rel.to_dict = MagicMock(return_value={"SchemaName": "rel_1"})
        op = _TableCreateOneToMany(lookup=lookup, relationship=rel, solution=None)
        result = await bc._resolve_table_create_one_to_many(op)

        od._build_create_relationship.assert_called_once()
        assert len(result) == 1

    async def test_table_create_many_to_many_calls_build_create_relationship(self):
        bc, od = _make_client()
        from PowerPlatform.Dataverse.models.relationship import ManyToManyRelationshipMetadata

        rel = MagicMock(spec=ManyToManyRelationshipMetadata)
        rel.to_dict = MagicMock(return_value={"SchemaName": "m2m_rel"})
        op = _TableCreateManyToMany(relationship=rel, solution=None)
        result = await bc._resolve_table_create_many_to_many(op)

        od._build_create_relationship.assert_called_once()
        assert len(result) == 1

    async def test_table_delete_relationship_calls_build_delete_relationship(self):
        bc, od = _make_client()
        op = _TableDeleteRelationship(relationship_id="rel-1")
        result = await bc._resolve_table_delete_relationship(op)

        od._build_delete_relationship.assert_called_once_with("rel-1")
        assert len(result) == 1

    async def test_table_get_relationship_calls_build_get_relationship(self):
        bc, od = _make_client()
        op = _TableGetRelationship(schema_name="schema")
        result = await bc._resolve_table_get_relationship(op)

        od._build_get_relationship.assert_called_once_with("schema")
        assert len(result) == 1

    async def test_table_create_lookup_field_calls_build_lookup_field_models(self):
        bc, od = _make_client()
        # Set up mock return values for the lookup/relationship pair
        lookup_mock = MagicMock()
        lookup_mock.to_dict = MagicMock(return_value={"SchemaName": "lookup"})
        rel_mock = MagicMock()
        rel_mock.to_dict = MagicMock(return_value={"SchemaName": "rel"})
        od._build_lookup_field_models.return_value = (lookup_mock, rel_mock)

        op = _TableCreateLookupField(
            referencing_table="contact",
            lookup_field_name="new_accountid",
            referenced_table="account",
        )
        result = await bc._resolve_table_create_lookup_field(op)

        od._build_lookup_field_models.assert_called_once()
        od._build_create_relationship.assert_called_once()
        assert len(result) == 1


# ---------------------------------------------------------------------------
# 9. TestAsyncBatchClientResolveQuerySql
# ---------------------------------------------------------------------------


class TestAsyncBatchClientResolveQuerySql:
    async def test_query_sql_returns_get_with_encoded_sql(self):
        bc, od = _make_client()
        sql = "SELECT accountid FROM account"
        op = _QuerySql(sql=sql)
        result = await bc._resolve_query_sql(op)

        od._build_sql.assert_called_once_with(sql)
        assert len(result) == 1
        req = result[0]
        assert req.method == "GET"
        assert "?sql=" in req.url
        # SQL should be URL-encoded (spaces become %20)
        assert "SELECT" in req.url
        assert " " not in req.url.split("?sql=", 1)[1]

    async def test_query_sql_url_contains_entity_set(self):
        bc, od = _make_client()
        op = _QuerySql(sql="SELECT name FROM account")
        result = await bc._resolve_query_sql(op)

        assert "accounts" in result[0].url


# ---------------------------------------------------------------------------
# 10. TestAsyncBatchClientResolveOne
# ---------------------------------------------------------------------------


class TestAsyncBatchClientResolveOne:
    async def test_single_request_item_returns_that_request(self):
        bc, od = _make_client()
        op = _RecordGet(table="account", record_id="guid-1")
        result = await bc._resolve_one(op)
        assert isinstance(result, _RawRequest)

    async def test_multi_request_item_raises_validation_error(self):
        bc, od = _make_client()
        # _TableAddColumns with two columns resolves to 2 requests
        op = _TableAddColumns(table="account", columns={"col1": "string", "col2": "string"})
        with pytest.raises(ValidationError, match="exactly one"):
            await bc._resolve_one(op)


# ---------------------------------------------------------------------------
# 11. TestAsyncBatchClientUnknownItem
# ---------------------------------------------------------------------------


class TestAsyncBatchClientUnknownItem:
    async def test_unknown_item_type_raises_validation_error(self):
        bc, od = _make_client()

        class _Sentinel:
            pass

        with pytest.raises(ValidationError, match="Unknown batch item type"):
            await bc._resolve_item(_Sentinel())
