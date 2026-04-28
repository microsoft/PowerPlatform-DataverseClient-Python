# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import pytest
import pandas as pd
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

from azure.core.credentials_async import AsyncTokenCredential

from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient
from PowerPlatform.Dataverse.aio.operations.async_batch import (
    AsyncBatchOperations,
    AsyncBatchRequest,
    AsyncBatchRecordOperations,
    AsyncBatchTableOperations,
    AsyncBatchQueryOperations,
    AsyncBatchDataFrameOperations,
    AsyncChangeSet,
    AsyncChangeSetRecordOperations,
)
from PowerPlatform.Dataverse.data._batch_base import (
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
)
from PowerPlatform.Dataverse.models.batch import BatchResult
from PowerPlatform.Dataverse.models.upsert import UpsertItem
from PowerPlatform.Dataverse.models.relationship import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
)
from PowerPlatform.Dataverse.models.labels import Label, LocalizedLabel


def _label(text: str = "Test") -> Label:
    return Label(localized_labels=[LocalizedLabel(label=text, language_code=1033)])


from PowerPlatform.Dataverse.core.errors import ValidationError

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _make_batch(async_client: AsyncDataverseClient) -> AsyncBatchRequest:
    return async_client.batch.new()


# ---------------------------------------------------------------------------
# Namespace tests
# ---------------------------------------------------------------------------


class TestAsyncBatchOperationsNamespace:
    def test_namespace_type(self, async_client):
        assert isinstance(async_client.batch, AsyncBatchOperations)

    def test_new_returns_batch_request(self, async_client):
        batch = async_client.batch.new()
        assert isinstance(batch, AsyncBatchRequest)

    def test_batch_request_namespaces(self, async_client):
        batch = async_client.batch.new()
        assert isinstance(batch.records, AsyncBatchRecordOperations)
        assert isinstance(batch.tables, AsyncBatchTableOperations)
        assert isinstance(batch.query, AsyncBatchQueryOperations)
        assert isinstance(batch.dataframe, AsyncBatchDataFrameOperations)


# ---------------------------------------------------------------------------
# AsyncBatchRecordOperations
# ---------------------------------------------------------------------------


class TestAsyncBatchRecordOperations:
    def test_create_single_appends_record_create(self, async_client):
        batch = _make_batch(async_client)
        batch.records.create("account", {"name": "Contoso"})
        assert len(batch._items) == 1
        item = batch._items[0]
        assert isinstance(item, _RecordCreate)
        assert item.table == "account"
        assert item.data == {"name": "Contoso"}

    def test_create_bulk_appends_record_create(self, async_client):
        batch = _make_batch(async_client)
        batch.records.create("account", [{"name": "A"}, {"name": "B"}])
        assert len(batch._items) == 1
        assert isinstance(batch._items[0], _RecordCreate)

    def test_update_single_appends_record_update(self, async_client):
        batch = _make_batch(async_client)
        batch.records.update("account", "guid-1", {"name": "X"})
        assert len(batch._items) == 1
        item = batch._items[0]
        assert isinstance(item, _RecordUpdate)
        assert item.table == "account"

    def test_delete_single_appends_record_delete(self, async_client):
        batch = _make_batch(async_client)
        batch.records.delete("account", "guid-1")
        assert len(batch._items) == 1
        item = batch._items[0]
        assert isinstance(item, _RecordDelete)

    def test_delete_bulk_appends_record_delete(self, async_client):
        batch = _make_batch(async_client)
        batch.records.delete("account", ["guid-1", "guid-2"])
        assert isinstance(batch._items[0], _RecordDelete)

    def test_get_appends_record_get(self, async_client):
        batch = _make_batch(async_client)
        batch.records.get("account", "guid-1", select=["name"])
        assert len(batch._items) == 1
        item = batch._items[0]
        assert isinstance(item, _RecordGet)
        assert item.table == "account"
        assert item.record_id == "guid-1"
        assert item.select == ["name"]

    def test_upsert_appends_record_upsert(self, async_client):
        batch = _make_batch(async_client)
        item = UpsertItem(alternate_key={"accountnumber": "ACC-001"}, record={"name": "X"})
        batch.records.upsert("account", [item])
        assert len(batch._items) == 1
        assert isinstance(batch._items[0], _RecordUpsert)

    def test_upsert_dict_item_normalized(self, async_client):
        batch = _make_batch(async_client)
        batch.records.upsert("account", [{"alternate_key": {"accountnumber": "ACC-001"}, "record": {"name": "X"}}])
        enqueued = batch._items[0]
        assert isinstance(enqueued, _RecordUpsert)
        assert isinstance(enqueued.items[0], UpsertItem)

    def test_upsert_empty_list_raises(self, async_client):
        batch = _make_batch(async_client)
        with pytest.raises(TypeError):
            batch.records.upsert("account", [])

    def test_upsert_invalid_item_raises(self, async_client):
        batch = _make_batch(async_client)
        with pytest.raises(TypeError):
            batch.records.upsert("account", [42])


# ---------------------------------------------------------------------------
# AsyncBatchTableOperations
# ---------------------------------------------------------------------------


class TestAsyncBatchTableOperations:
    def test_create_appends_table_create(self, async_client):
        batch = _make_batch(async_client)
        batch.tables.create("new_Product", {"new_Price": "decimal"})
        assert len(batch._items) == 1
        item = batch._items[0]
        assert isinstance(item, _TableCreate)
        assert item.table == "new_Product"

    def test_delete_appends_table_delete(self, async_client):
        batch = _make_batch(async_client)
        batch.tables.delete("new_Product")
        assert isinstance(batch._items[0], _TableDelete)

    def test_get_appends_table_get(self, async_client):
        batch = _make_batch(async_client)
        batch.tables.get("new_Product")
        assert isinstance(batch._items[0], _TableGet)

    def test_list_appends_table_list(self, async_client):
        batch = _make_batch(async_client)
        batch.tables.list()
        assert isinstance(batch._items[0], _TableList)

    def test_add_columns_appends(self, async_client):
        batch = _make_batch(async_client)
        batch.tables.add_columns("new_Product", {"new_Notes": "string"})
        assert isinstance(batch._items[0], _TableAddColumns)

    def test_remove_columns_appends(self, async_client):
        batch = _make_batch(async_client)
        batch.tables.remove_columns("new_Product", "new_Notes")
        assert isinstance(batch._items[0], _TableRemoveColumns)

    def test_create_one_to_many_appends(self, async_client):
        batch = _make_batch(async_client)
        lookup = LookupAttributeMetadata(schema_name="new_DeptId", display_name=_label("Department"))
        rel = OneToManyRelationshipMetadata(
            schema_name="new_Dept_Emp",
            referenced_entity="new_dept",
            referencing_entity="new_emp",
            referenced_attribute="new_deptid",
        )
        batch.tables.create_one_to_many_relationship(lookup, rel)
        assert isinstance(batch._items[0], _TableCreateOneToMany)

    def test_create_many_to_many_appends(self, async_client):
        batch = _make_batch(async_client)
        rel = ManyToManyRelationshipMetadata(
            schema_name="new_emp_proj",
            entity1_logical_name="new_emp",
            entity2_logical_name="new_proj",
        )
        batch.tables.create_many_to_many_relationship(rel)
        assert isinstance(batch._items[0], _TableCreateManyToMany)

    def test_delete_relationship_appends(self, async_client):
        batch = _make_batch(async_client)
        batch.tables.delete_relationship("rel-guid")
        assert isinstance(batch._items[0], _TableDeleteRelationship)

    def test_get_relationship_appends(self, async_client):
        batch = _make_batch(async_client)
        batch.tables.get_relationship("new_Dept_Emp")
        assert isinstance(batch._items[0], _TableGetRelationship)

    def test_create_lookup_field_appends(self, async_client):
        batch = _make_batch(async_client)
        batch.tables.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
        )
        assert isinstance(batch._items[0], _TableCreateLookupField)


# ---------------------------------------------------------------------------
# AsyncBatchQueryOperations
# ---------------------------------------------------------------------------


class TestAsyncBatchQueryOperations:
    def test_sql_appends_query_sql(self, async_client):
        batch = _make_batch(async_client)
        batch.query.sql("SELECT name FROM account")
        assert len(batch._items) == 1
        item = batch._items[0]
        assert isinstance(item, _QuerySql)
        assert item.sql == "SELECT name FROM account"

    def test_sql_strips_whitespace(self, async_client):
        batch = _make_batch(async_client)
        batch.query.sql("  SELECT name FROM account  ")
        assert batch._items[0].sql == "SELECT name FROM account"

    def test_sql_empty_string_raises(self, async_client):
        batch = _make_batch(async_client)
        with pytest.raises(ValidationError):
            batch.query.sql("")

    def test_sql_whitespace_only_raises(self, async_client):
        batch = _make_batch(async_client)
        with pytest.raises(ValidationError):
            batch.query.sql("   ")

    def test_sql_non_string_raises(self, async_client):
        batch = _make_batch(async_client)
        with pytest.raises(ValidationError):
            batch.query.sql(None)


# ---------------------------------------------------------------------------
# AsyncBatchDataFrameOperations
# ---------------------------------------------------------------------------


class TestAsyncBatchDataFrameOperations:
    def test_create_from_dataframe(self, async_client):
        batch = _make_batch(async_client)
        df = pd.DataFrame([{"name": "Contoso"}, {"name": "Fabrikam"}])
        batch.dataframe.create("account", df)
        assert len(batch._items) == 1
        assert isinstance(batch._items[0], _RecordCreate)

    def test_create_non_dataframe_raises(self, async_client):
        batch = _make_batch(async_client)
        with pytest.raises(TypeError):
            batch.dataframe.create("account", [{"name": "X"}])

    def test_create_empty_dataframe_raises(self, async_client):
        batch = _make_batch(async_client)
        with pytest.raises(ValueError):
            batch.dataframe.create("account", pd.DataFrame())

    def test_create_all_null_row_raises(self, async_client):
        batch = _make_batch(async_client)
        with pytest.raises(ValueError):
            batch.dataframe.create("account", pd.DataFrame([{"name": None}]))

    def test_update_from_dataframe(self, async_client):
        batch = _make_batch(async_client)
        df = pd.DataFrame([{"accountid": "guid-1", "name": "X"}])
        batch.dataframe.update("account", df, id_column="accountid")
        assert len(batch._items) == 1
        assert isinstance(batch._items[0], _RecordUpdate)

    def test_update_non_dataframe_raises(self, async_client):
        batch = _make_batch(async_client)
        with pytest.raises(TypeError):
            batch.dataframe.update("account", [{}], id_column="id")

    def test_update_empty_dataframe_raises(self, async_client):
        batch = _make_batch(async_client)
        with pytest.raises(ValueError):
            batch.dataframe.update("account", pd.DataFrame(), id_column="id")

    def test_update_missing_id_column_raises(self, async_client):
        batch = _make_batch(async_client)
        df = pd.DataFrame([{"name": "X"}])
        with pytest.raises(ValueError, match="id_column"):
            batch.dataframe.update("account", df, id_column="accountid")

    def test_update_invalid_ids_raises(self, async_client):
        batch = _make_batch(async_client)
        df = pd.DataFrame([{"accountid": None, "name": "X"}])
        with pytest.raises(ValueError):
            batch.dataframe.update("account", df, id_column="accountid")

    def test_update_no_change_columns_raises(self, async_client):
        batch = _make_batch(async_client)
        df = pd.DataFrame([{"accountid": "guid-1"}])
        with pytest.raises(ValueError):
            batch.dataframe.update("account", df, id_column="accountid")

    def test_update_all_null_rows_skipped(self, async_client):
        batch = _make_batch(async_client)
        df = pd.DataFrame([{"accountid": "guid-1", "telephone1": None}])
        batch.dataframe.update("account", df, id_column="accountid")
        # All change values null -> nothing enqueued
        assert len(batch._items) == 0

    def test_delete_from_series(self, async_client):
        batch = _make_batch(async_client)
        ids = pd.Series(["guid-1", "guid-2"])
        batch.dataframe.delete("account", ids)
        assert len(batch._items) == 1
        assert isinstance(batch._items[0], _RecordDelete)

    def test_delete_non_series_raises(self, async_client):
        batch = _make_batch(async_client)
        with pytest.raises(TypeError):
            batch.dataframe.delete("account", ["guid-1"])

    def test_delete_empty_series_no_item(self, async_client):
        batch = _make_batch(async_client)
        batch.dataframe.delete("account", pd.Series([], dtype=str))
        assert len(batch._items) == 0

    def test_delete_invalid_ids_raises(self, async_client):
        batch = _make_batch(async_client)
        ids = pd.Series(["guid-1", None])
        with pytest.raises(ValueError):
            batch.dataframe.delete("account", ids)


# ---------------------------------------------------------------------------
# AsyncChangeSet
# ---------------------------------------------------------------------------


class TestAsyncChangeSet:
    def test_changeset_returns_async_changeset(self, async_client):
        batch = _make_batch(async_client)
        cs = batch.changeset()
        assert isinstance(cs, AsyncChangeSet)

    def test_changeset_records_namespace(self, async_client):
        batch = _make_batch(async_client)
        cs = batch.changeset()
        assert isinstance(cs.records, AsyncChangeSetRecordOperations)

    def test_changeset_appended_to_items(self, async_client):
        batch = _make_batch(async_client)
        batch.changeset()
        assert len(batch._items) == 1
        assert isinstance(batch._items[0], _ChangeSet)

    async def test_changeset_async_context_manager(self, async_client):
        batch = _make_batch(async_client)
        async with batch.changeset() as cs:
            assert isinstance(cs, AsyncChangeSet)


class TestAsyncChangeSetRecordOperations:
    def test_create_adds_to_changeset(self, async_client):
        batch = _make_batch(async_client)
        cs = batch.changeset()
        ref = cs.records.create("account", {"name": "Contoso"})
        # ref should be a content-ID string like "$1"
        assert isinstance(ref, str)
        assert ref.startswith("$")

    def test_update_adds_to_changeset(self, async_client):
        batch = _make_batch(async_client)
        cs = batch.changeset()
        cs.records.update("account", "guid-1", {"name": "X"})
        internal = batch._items[0]
        assert len(internal.operations) == 1

    def test_delete_adds_to_changeset(self, async_client):
        batch = _make_batch(async_client)
        cs = batch.changeset()
        cs.records.delete("account", "guid-1")
        internal = batch._items[0]
        assert len(internal.operations) == 1

    def test_content_id_increments(self, async_client):
        batch = _make_batch(async_client)
        cs = batch.changeset()
        ref1 = cs.records.create("account", {"name": "A"})
        ref2 = cs.records.create("contact", {"firstname": "B"})
        assert ref1 != ref2


# ---------------------------------------------------------------------------
# AsyncBatchRequest.execute
# ---------------------------------------------------------------------------


class TestAsyncBatchRequestExecute:
    async def test_execute_calls_batch_client(self, async_client, mock_od):
        """execute() delegates to _AsyncBatchClient and returns BatchResult."""
        from PowerPlatform.Dataverse.models.batch import BatchResult, BatchItemResponse

        mock_result = BatchResult(responses=[BatchItemResponse(status_code=204)])

        # Patch _AsyncBatchClient so we don't need a real HTTP client
        with __import__("unittest.mock", fromlist=["patch"]).patch(
            "PowerPlatform.Dataverse.aio.operations.async_batch._AsyncBatchClient"
        ) as mock_cls:
            mock_instance = AsyncMock()
            mock_instance.execute.return_value = mock_result
            mock_cls.return_value = mock_instance

            batch = _make_batch(async_client)
            batch.records.create("account", {"name": "X"})
            result = await batch.execute()

        mock_instance.execute.assert_called_once()
        assert isinstance(result, BatchResult)

    async def test_execute_passes_continue_on_error(self, async_client, mock_od):
        """execute() passes continue_on_error to _AsyncBatchClient.execute."""
        from PowerPlatform.Dataverse.models.batch import BatchResult

        mock_result = BatchResult()

        with __import__("unittest.mock", fromlist=["patch"]).patch(
            "PowerPlatform.Dataverse.aio.operations.async_batch._AsyncBatchClient"
        ) as mock_cls:
            mock_instance = AsyncMock()
            mock_instance.execute.return_value = mock_result
            mock_cls.return_value = mock_instance

            batch = _make_batch(async_client)
            await batch.execute(continue_on_error=True)

        _, kwargs = mock_instance.execute.call_args
        assert kwargs["continue_on_error"] is True

    async def test_execute_empty_batch_ok(self, async_client, mock_od):
        """execute() with an empty batch does not raise."""
        from PowerPlatform.Dataverse.models.batch import BatchResult

        mock_result = BatchResult()

        with __import__("unittest.mock", fromlist=["patch"]).patch(
            "PowerPlatform.Dataverse.aio.operations.async_batch._AsyncBatchClient"
        ) as mock_cls:
            mock_instance = AsyncMock()
            mock_instance.execute.return_value = mock_result
            mock_cls.return_value = mock_instance

            batch = _make_batch(async_client)
            result = await batch.execute()

        assert isinstance(result, BatchResult)


# ---------------------------------------------------------------------------
# Multiple operations in one batch
# ---------------------------------------------------------------------------


class TestAsyncBatchMultipleOperations:
    def test_multiple_items_accumulated(self, async_client):
        batch = _make_batch(async_client)
        batch.records.create("account", {"name": "A"})
        batch.records.get("account", "guid-1")
        batch.tables.get("account")
        batch.query.sql("SELECT name FROM account")
        assert len(batch._items) == 4
