# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for _AsyncBatchClient internals."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from PowerPlatform.Dataverse.aio.data._async_batch import _AsyncBatchClient
from PowerPlatform.Dataverse.core.errors import MetadataError, ValidationError
from PowerPlatform.Dataverse.data._batch_base import (
    _RecordCreate,
    _RecordDelete,
    _RecordGet,
    _RecordUpdate,
    _RecordUpsert,
    _TableAddColumns,
    _TableDelete,
    _TableRemoveColumns,
    _QuerySql,
    _ChangeSet,
)
from PowerPlatform.Dataverse.models.upsert import UpsertItem

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_batch_client():
    """Return _AsyncBatchClient with a fully-mocked _AsyncODataClient.

    All _build_* methods are pre-mocked so resolver tests can run without
    any real OData or HTTP logic.  Sync _build_* methods use MagicMock;
    async ones use AsyncMock.
    """
    od = AsyncMock()
    od.api = "https://example.crm.dynamics.com/api/data/v9.2"
    od._entity_set_from_schema_name = AsyncMock(return_value="accounts")
    od._primary_id_attr = AsyncMock(return_value="accountid")
    od._get_entity_by_table_schema_name = AsyncMock(
        return_value={"MetadataId": "meta-1", "LogicalName": "account", "SchemaName": "Account"}
    )
    od._get_attribute_metadata = AsyncMock(return_value={"MetadataId": "attr-1", "LogicalName": "new_notes"})
    od._build_create = AsyncMock(
        return_value=MagicMock(method="POST", url="https://x/accounts", body="{}", headers=None, content_id=None)
    )
    od._build_create_multiple = AsyncMock(
        return_value=MagicMock(
            method="POST", url="https://x/accounts/CreateMultiple", body="{}", headers=None, content_id=None
        )
    )
    od._build_update = AsyncMock(
        return_value=MagicMock(
            method="PATCH", url="https://x/accounts(g)", body="{}", headers={"If-Match": "*"}, content_id=None
        )
    )
    od._build_update_multiple = AsyncMock(
        return_value=MagicMock(
            method="POST", url="https://x/accounts/UpdateMultiple", body="{}", headers=None, content_id=None
        )
    )
    od._build_delete = AsyncMock(
        return_value=MagicMock(
            method="DELETE", url="https://x/accounts(g)", body=None, headers={"If-Match": "*"}, content_id=None
        )
    )
    od._build_delete_multiple = AsyncMock(
        return_value=MagicMock(method="POST", url="https://x/BulkDelete", body="{}", headers=None, content_id=None)
    )
    od._build_get = AsyncMock(
        return_value=MagicMock(method="GET", url="https://x/accounts(g)", body=None, headers=None, content_id=None)
    )
    od._build_upsert = AsyncMock(
        return_value=MagicMock(method="PATCH", url="https://x/accounts(k)", body="{}", headers=None, content_id=None)
    )
    od._build_upsert_multiple = AsyncMock(
        return_value=MagicMock(
            method="POST", url="https://x/accounts/UpsertMultiple", body="{}", headers=None, content_id=None
        )
    )
    od._build_sql = AsyncMock(
        return_value=MagicMock(method="GET", url="https://x/accounts?sql=...", body=None, headers=None, content_id=None)
    )
    od._build_delete_entity = MagicMock(
        return_value=MagicMock(
            method="DELETE", url="https://x/EntityDefinitions(m)", body=None, headers=None, content_id=None
        )
    )
    od._build_create_column = MagicMock(
        return_value=MagicMock(
            method="POST", url="https://x/EntityDefinitions(m)/Attributes", body="{}", headers=None, content_id=None
        )
    )
    od._build_delete_column = MagicMock(
        return_value=MagicMock(
            method="DELETE",
            url="https://x/EntityDefinitions(m)/Attributes(a)",
            body=None,
            headers=None,
            content_id=None,
        )
    )
    return _AsyncBatchClient(od), od


def _batch_resp(status=200, text="", json_payload=None):
    """Create a mock response suitable for the batch execute() path."""
    r = MagicMock()
    r.status = status
    r.headers = {"Content-Type": "application/json"}
    r.text = AsyncMock(return_value=text)
    r.json = AsyncMock(return_value=json_payload or {})
    return r


# ---------------------------------------------------------------------------
# execute()
# ---------------------------------------------------------------------------


class TestExecute:
    """Tests for execute(), the public entry point that dispatches the full batch request."""

    async def test_empty_items_returns_empty_result(self):
        """An empty items list short-circuits and returns an empty BatchResult without HTTP."""
        client, _ = _make_batch_client()
        result = await client.execute([])
        assert result is not None

    async def test_executes_single_record_create(self):
        """A single RecordCreate item causes exactly one _request call."""
        client, od = _make_batch_client()
        from PowerPlatform.Dataverse.models.batch import BatchResult

        resp_mock = _batch_resp(status=200)
        od._request = AsyncMock(return_value=resp_mock)
        item = _RecordCreate(table="account", data={"name": "X"})
        with patch.object(client, "_parse_batch_response", return_value=BatchResult()):
            await client.execute([item])
        od._request.assert_called_once()

    async def test_executes_with_continue_on_error(self):
        """The odata.continue-on-error preference is injected when continue_on_error=True."""
        client, od = _make_batch_client()
        from PowerPlatform.Dataverse.models.batch import BatchResult

        resp_mock = _batch_resp(status=200)
        od._request = AsyncMock(return_value=resp_mock)
        item = _RecordCreate(table="account", data={"name": "X"})
        with patch.object(client, "_parse_batch_response", return_value=BatchResult()):
            await client.execute([item], continue_on_error=True)
        call_kwargs = od._request.call_args.kwargs
        headers = call_kwargs.get("headers", {})
        assert "odata.continue-on-error" in headers.get("Prefer", "")


# ---------------------------------------------------------------------------
# _resolve_record_create()
# ---------------------------------------------------------------------------


class TestResolveRecordCreate:
    """Tests for _resolve_record_create() intent-to-request translation."""

    async def test_single_dict_returns_one_request(self):
        """A dict payload produces a single _build_create request."""
        client, od = _make_batch_client()
        op = _RecordCreate(table="account", data={"name": "X"})
        result = await client._resolve_record_create(op)
        assert len(result) == 1
        od._build_create.assert_called_once()

    async def test_list_returns_one_create_multiple_request(self):
        """A list payload produces a single _build_create_multiple request."""
        client, od = _make_batch_client()
        op = _RecordCreate(table="account", data=[{"name": "X"}, {"name": "Y"}])
        result = await client._resolve_record_create(op)
        assert len(result) == 1
        od._build_create_multiple.assert_called_once()


# ---------------------------------------------------------------------------
# _resolve_record_update()
# ---------------------------------------------------------------------------


class TestResolveRecordUpdate:
    """Tests for _resolve_record_update() intent-to-request translation."""

    async def test_single_id_string_returns_one_patch(self):
        """A single string ID with a dict of changes produces one _build_update request."""
        client, od = _make_batch_client()
        op = _RecordUpdate(table="account", ids="guid-1", changes={"name": "X"})
        result = await client._resolve_record_update(op)
        assert len(result) == 1
        od._build_update.assert_called_once()

    async def test_single_id_non_dict_changes_raises(self):
        """TypeError is raised when changes is not a dict for a single-ID update."""
        client, od = _make_batch_client()
        op = _RecordUpdate(table="account", ids="guid-1", changes=["invalid"])
        with pytest.raises(TypeError):
            await client._resolve_record_update(op)

    async def test_list_ids_returns_update_multiple(self):
        """A list of IDs produces a single _build_update_multiple request."""
        client, od = _make_batch_client()
        op = _RecordUpdate(table="account", ids=["id-1", "id-2"], changes={"name": "X"})
        result = await client._resolve_record_update(op)
        assert len(result) == 1
        od._build_update_multiple.assert_called_once()


# ---------------------------------------------------------------------------
# _resolve_record_delete()
# ---------------------------------------------------------------------------


class TestResolveRecordDelete:
    """Tests for _resolve_record_delete() intent-to-request translation."""

    async def test_single_id_string(self):
        """A single string ID produces one _build_delete request."""
        client, od = _make_batch_client()
        op = _RecordDelete(table="account", ids="guid-1")
        result = await client._resolve_record_delete(op)
        assert len(result) == 1
        od._build_delete.assert_called_once()

    async def test_list_ids_bulk_delete(self):
        """A list of IDs with use_bulk_delete=True produces one _build_delete_multiple request."""
        client, od = _make_batch_client()
        op = _RecordDelete(table="account", ids=["id-1", "id-2"], use_bulk_delete=True)
        result = await client._resolve_record_delete(op)
        assert len(result) == 1
        od._build_delete_multiple.assert_called_once()

    async def test_list_ids_sequential_delete(self):
        """A list of IDs with use_bulk_delete=False produces one _build_delete per ID."""
        client, od = _make_batch_client()
        op = _RecordDelete(table="account", ids=["id-1", "id-2"], use_bulk_delete=False)
        result = await client._resolve_record_delete(op)
        assert len(result) == 2
        assert od._build_delete.call_count == 2

    async def test_empty_ids_list_returns_empty(self):
        """An empty IDs list produces no requests."""
        client, od = _make_batch_client()
        op = _RecordDelete(table="account", ids=[])
        result = await client._resolve_record_delete(op)
        assert result == []


# ---------------------------------------------------------------------------
# _resolve_record_get()
# ---------------------------------------------------------------------------


class TestResolveRecordGet:
    """Tests for _resolve_record_get() intent-to-request translation."""

    async def test_single_get_request(self):
        """A RecordGet op produces one _build_get request with the correct arguments."""
        client, od = _make_batch_client()
        op = _RecordGet(table="account", record_id="guid-1", select=["name"])
        result = await client._resolve_record_get(op)
        assert len(result) == 1
        od._build_get.assert_called_once_with("account", "guid-1", select=["name"])


# ---------------------------------------------------------------------------
# _resolve_record_upsert()
# ---------------------------------------------------------------------------


class TestResolveRecordUpsert:
    """Tests for _resolve_record_upsert() intent-to-request translation."""

    async def test_single_item_calls_build_upsert(self):
        """A single UpsertItem produces one _build_upsert request."""
        client, od = _make_batch_client()
        item = UpsertItem(alternate_key={"accountnumber": "A"}, record={"name": "X"})
        op = _RecordUpsert(table="account", items=[item])
        result = await client._resolve_record_upsert(op)
        assert len(result) == 1
        od._build_upsert.assert_called_once()

    async def test_multiple_items_calls_build_upsert_multiple(self):
        """Multiple UpsertItems produce a single _build_upsert_multiple request."""
        client, od = _make_batch_client()
        items = [
            UpsertItem(alternate_key={"accountnumber": "A"}, record={"name": "X"}),
            UpsertItem(alternate_key={"accountnumber": "B"}, record={"name": "Y"}),
        ]
        op = _RecordUpsert(table="account", items=items)
        result = await client._resolve_record_upsert(op)
        assert len(result) == 1
        od._build_upsert_multiple.assert_called_once()


# ---------------------------------------------------------------------------
# _resolve_table_delete()
# ---------------------------------------------------------------------------


class TestResolveTableDelete:
    """Tests for _resolve_table_delete() intent-to-request translation."""

    async def test_resolves_to_delete_request(self):
        """A TableDelete op resolves to a _build_delete_entity call using the table's MetadataId."""
        client, od = _make_batch_client()
        op = _TableDelete(table="account")
        result = await client._resolve_table_delete(op)
        assert len(result) == 1
        od._build_delete_entity.assert_called_once_with("meta-1")

    async def test_table_not_found_raises(self):
        """MetadataError is raised when the target table does not exist in metadata."""
        client, od = _make_batch_client()
        od._get_entity_by_table_schema_name = AsyncMock(return_value=None)
        op = _TableDelete(table="nonexistent")
        with pytest.raises(MetadataError):
            await client._resolve_table_delete(op)


# ---------------------------------------------------------------------------
# _resolve_table_add_columns()
# ---------------------------------------------------------------------------


class TestResolveTableAddColumns:
    """Tests for _resolve_table_add_columns() intent-to-request translation."""

    async def test_resolves_to_create_column_requests(self):
        """Each column in the op produces one _build_create_column request."""
        client, od = _make_batch_client()
        op = _TableAddColumns(table="account", columns={"col1": "string", "col2": "decimal"})
        result = await client._resolve_table_add_columns(op)
        assert len(result) == 2
        assert od._build_create_column.call_count == 2


# ---------------------------------------------------------------------------
# _resolve_table_remove_columns()
# ---------------------------------------------------------------------------


class TestResolveTableRemoveColumns:
    """Tests for _resolve_table_remove_columns() intent-to-request translation."""

    async def test_resolves_to_delete_column_requests(self):
        """Each column in the list produces one _build_delete_column request."""
        client, od = _make_batch_client()
        op = _TableRemoveColumns(table="account", columns=["col1", "col2"])
        result = await client._resolve_table_remove_columns(op)
        assert len(result) == 2

    async def test_string_column_name(self):
        """A single column name supplied as a string produces one delete request."""
        client, od = _make_batch_client()
        op = _TableRemoveColumns(table="account", columns="col1")
        result = await client._resolve_table_remove_columns(op)
        assert len(result) == 1

    async def test_column_not_found_raises(self):
        """MetadataError is raised when attribute metadata returns None for the column."""
        client, od = _make_batch_client()
        od._get_attribute_metadata = AsyncMock(return_value=None)
        op = _TableRemoveColumns(table="account", columns=["nonexistent"])
        with pytest.raises(MetadataError):
            await client._resolve_table_remove_columns(op)

    async def test_attr_missing_metadata_id_raises(self):
        """MetadataError is raised when attribute metadata lacks a MetadataId field."""
        client, od = _make_batch_client()
        od._get_attribute_metadata = AsyncMock(return_value={"LogicalName": "col1"})
        op = _TableRemoveColumns(table="account", columns=["col1"])
        with pytest.raises(MetadataError):
            await client._resolve_table_remove_columns(op)


# ---------------------------------------------------------------------------
# _resolve_query_sql()
# ---------------------------------------------------------------------------


class TestResolveQuerySql:
    """Tests for _resolve_query_sql() intent-to-request translation."""

    async def test_resolves_to_get_request(self):
        """A QuerySql op produces one _build_sql request with the SQL statement."""
        client, od = _make_batch_client()
        op = _QuerySql(sql="SELECT name FROM account")
        result = await client._resolve_query_sql(op)
        assert len(result) == 1
        od._build_sql.assert_called_once_with("SELECT name FROM account")


# ---------------------------------------------------------------------------
# _resolve_one() — changeset item must produce exactly 1 request
# ---------------------------------------------------------------------------


class TestResolveOne:
    """Tests for _resolve_one(), which enforces the single-request contract for changeset items."""

    async def test_single_request_returned(self):
        """An op that resolves to exactly one request is returned without error."""
        client, od = _make_batch_client()
        op = _RecordGet(table="account", record_id="guid-1")
        req = await client._resolve_one(op)
        assert req is not None

    async def test_multi_request_item_raises(self):
        """ValidationError is raised when an op resolves to more than one request."""
        client, od = _make_batch_client()
        # _RecordDelete with a list produces multiple requests (one per ID)
        op = _RecordDelete(table="account", ids=["id-1", "id-2"], use_bulk_delete=False)
        with pytest.raises(ValidationError, match="exactly one"):
            await client._resolve_one(op)


# ---------------------------------------------------------------------------
# _resolve_all() — changeset handling
# ---------------------------------------------------------------------------


class TestResolveAll:
    """Tests for _resolve_all(), which dispatches items and wraps changeset ops."""

    async def test_empty_changeset_skipped(self):
        """A ChangeSet with no operations is silently skipped without error."""
        client, od = _make_batch_client()
        cs = _ChangeSet(operations=[])
        result = await client._resolve_all([cs])
        assert result == []

    async def test_changeset_with_operations(self):
        """A ChangeSet with one operation produces one _ChangeSetBatchItem in the result."""
        client, od = _make_batch_client()
        op = _RecordCreate(table="account", data={"name": "X"})
        cs = _ChangeSet(operations=[op])
        result = await client._resolve_all([cs])
        assert len(result) == 1

    async def test_unknown_item_type_raises(self):
        """ValidationError is raised when an unrecognised item type is passed to _resolve_item."""
        client, od = _make_batch_client()
        with pytest.raises(ValidationError, match="Unknown batch item type"):
            await client._resolve_item("not-a-valid-type")


# ---------------------------------------------------------------------------
# _require_entity_metadata()
# ---------------------------------------------------------------------------


class TestRequireEntityMetadata:
    """Tests for _require_entity_metadata(), which resolves a table's MetadataId or raises."""

    async def test_returns_metadata_id(self):
        """The MetadataId string is returned when the table exists in entity metadata."""
        client, od = _make_batch_client()
        meta_id = await client._require_entity_metadata("account")
        assert meta_id == "meta-1"

    async def test_not_found_raises(self):
        """MetadataError is raised when the API returns no entity definition for the table."""
        client, od = _make_batch_client()
        od._get_entity_by_table_schema_name = AsyncMock(return_value=None)
        with pytest.raises(MetadataError):
            await client._require_entity_metadata("nonexistent")
