# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for async batch operations (AsyncBatchRequest, AsyncBatchOperations, etc.)."""

import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock, patch

from azure.core.credentials_async import AsyncTokenCredential

from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient
from PowerPlatform.Dataverse.aio.operations.async_batch import (
    AsyncBatchOperations,
    AsyncBatchRequest,
)
from PowerPlatform.Dataverse.operations.batch import (
    BatchDataFrameOperations as AsyncBatchDataFrameOperations,
    BatchQueryOperations as AsyncBatchQueryOperations,
    BatchRecordOperations as AsyncBatchRecordOperations,
    BatchTableOperations as AsyncBatchTableOperations,
    ChangeSet as AsyncChangeSet,
    ChangeSetRecordOperations as AsyncChangeSetRecordOperations,
)
from PowerPlatform.Dataverse.data._batch import (
    _RecordCreate,
    _RecordDelete,
    _RecordGet,
    _RecordUpdate,
    _RecordUpsert,
    _ChangeSet,
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
)
from PowerPlatform.Dataverse.models.batch import BatchResult
from PowerPlatform.Dataverse.models.upsert import UpsertItem


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client_with_mock_odata():
    """Return (client, mock_od) with _scoped_odata patched."""
    credential = AsyncMock(spec=AsyncTokenCredential)
    client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
    od = AsyncMock()

    @asynccontextmanager
    async def _fake_scoped_odata():
        yield od

    client._scoped_odata = _fake_scoped_odata
    return client, od


# ---------------------------------------------------------------------------
# AsyncBatchOperations / AsyncBatchRequest construction
# ---------------------------------------------------------------------------

class TestAsyncBatchOperationsNamespace:
    def test_namespace_exists(self):
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        assert isinstance(client.batch, AsyncBatchOperations)

    def test_new_returns_batch_request(self):
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        batch = client.batch.new()
        assert isinstance(batch, AsyncBatchRequest)

    def test_batch_request_has_namespaces(self):
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        batch = client.batch.new()
        assert isinstance(batch.records, AsyncBatchRecordOperations)
        assert isinstance(batch.tables, AsyncBatchTableOperations)
        assert isinstance(batch.query, AsyncBatchQueryOperations)
        assert isinstance(batch.dataframe, AsyncBatchDataFrameOperations)


# ---------------------------------------------------------------------------
# AsyncBatchRecordOperations — local assembly (no HTTP)
# ---------------------------------------------------------------------------

class TestAsyncBatchRecordOperations:
    def _make_batch(self):
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        return client.batch.new()

    def test_create_single_appends_record_create(self):
        batch = self._make_batch()
        batch.records.create("account", {"name": "Contoso"})
        assert len(batch._items) == 1
        item = batch._items[0]
        assert isinstance(item, _RecordCreate)
        assert item.table == "account"
        assert item.data == {"name": "Contoso"}

    def test_create_list_appends_record_create(self):
        batch = self._make_batch()
        payloads = [{"name": "A"}, {"name": "B"}]
        batch.records.create("account", payloads)
        assert isinstance(batch._items[0], _RecordCreate)
        assert batch._items[0].data == payloads

    def test_update_appends_record_update(self):
        batch = self._make_batch()
        batch.records.update("account", "guid-1", {"name": "Updated"})
        assert len(batch._items) == 1
        item = batch._items[0]
        assert isinstance(item, _RecordUpdate)
        assert item.table == "account"
        assert item.ids == "guid-1"

    def test_delete_appends_record_delete(self):
        batch = self._make_batch()
        batch.records.delete("account", "guid-1")
        assert len(batch._items) == 1
        item = batch._items[0]
        assert isinstance(item, _RecordDelete)
        assert item.table == "account"
        assert item.ids == "guid-1"

    def test_get_appends_record_get(self):
        batch = self._make_batch()
        batch.records.get("account", "guid-1", select=["name"])
        assert len(batch._items) == 1
        item = batch._items[0]
        assert isinstance(item, _RecordGet)
        assert item.table == "account"
        assert item.record_id == "guid-1"
        assert item.select == ["name"]

    def test_upsert_appends_record_upsert(self):
        batch = self._make_batch()
        items = [UpsertItem(alternate_key={"accountnumber": "ACC-1"}, record={"name": "Contoso"})]
        batch.records.upsert("account", items)
        assert len(batch._items) == 1
        item = batch._items[0]
        assert isinstance(item, _RecordUpsert)
        assert item.table == "account"

    def test_upsert_empty_list_raises(self):
        batch = self._make_batch()
        with pytest.raises(TypeError):
            batch.records.upsert("account", [])

    def test_multiple_operations_append_in_order(self):
        batch = self._make_batch()
        batch.records.create("account", {"name": "A"})
        batch.records.get("account", "guid-1")
        batch.records.delete("account", "guid-2")
        assert len(batch._items) == 3
        assert isinstance(batch._items[0], _RecordCreate)
        assert isinstance(batch._items[1], _RecordGet)
        assert isinstance(batch._items[2], _RecordDelete)


# ---------------------------------------------------------------------------
# AsyncChangeSet / AsyncChangeSetRecordOperations
# ---------------------------------------------------------------------------

class TestAsyncChangeSet:
    def _make_batch(self):
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        return client.batch.new()

    def test_changeset_returns_async_change_set(self):
        batch = self._make_batch()
        cs = batch.changeset()
        assert isinstance(cs, AsyncChangeSet)

    def test_changeset_appended_to_items(self):
        batch = self._make_batch()
        batch.changeset()
        assert len(batch._items) == 1
        assert isinstance(batch._items[0], _ChangeSet)

    def test_changeset_is_context_manager(self):
        batch = self._make_batch()
        with batch.changeset() as cs:
            assert isinstance(cs, AsyncChangeSet)
            assert isinstance(cs.records, AsyncChangeSetRecordOperations)

    def test_changeset_records_create_adds_to_changeset(self):
        batch = self._make_batch()
        with batch.changeset() as cs:
            ref = cs.records.create("contact", {"firstname": "Alice"})
        # The _ChangeSet internal should have one op
        internal_cs = batch._items[0]
        assert isinstance(internal_cs, _ChangeSet)
        assert len(internal_cs.operations) == 1

    def test_changeset_records_update_adds_to_changeset(self):
        batch = self._make_batch()
        with batch.changeset() as cs:
            cs.records.update("account", "guid-1", {"name": "New"})
        internal_cs = batch._items[0]
        assert len(internal_cs.operations) == 1

    def test_changeset_records_delete_adds_to_changeset(self):
        batch = self._make_batch()
        with batch.changeset() as cs:
            cs.records.delete("account", "guid-1")
        internal_cs = batch._items[0]
        assert len(internal_cs.operations) == 1

    def test_multiple_changesets_are_separate(self):
        batch = self._make_batch()
        with batch.changeset() as cs1:
            cs1.records.create("account", {"name": "A"})
        with batch.changeset() as cs2:
            cs2.records.create("contact", {"firstname": "B"})
        assert len(batch._items) == 2
        assert len(batch._items[0].operations) == 1
        assert len(batch._items[1].operations) == 1


# ---------------------------------------------------------------------------
# AsyncBatchQueryOperations
# ---------------------------------------------------------------------------

class TestAsyncBatchQueryOperations:
    def _make_batch(self):
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        return client.batch.new()

    def test_sql_empty_raises(self):
        from PowerPlatform.Dataverse.core.errors import ValidationError
        batch = self._make_batch()
        with pytest.raises(ValidationError):
            batch.query.sql("")

    def test_sql_non_string_raises(self):
        from PowerPlatform.Dataverse.core.errors import ValidationError
        batch = self._make_batch()
        with pytest.raises((ValidationError, TypeError)):
            batch.query.sql(123)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# AsyncBatchRequest.execute — delegates to _AsyncBatchClient
# ---------------------------------------------------------------------------

class TestAsyncBatchRequestExecute:
    async def test_execute_calls_batch_client_execute(self):
        client, od = _make_client_with_mock_odata()
        batch = client.batch.new()
        batch.records.create("account", {"name": "Contoso"})

        expected_result = BatchResult()
        mock_bc_instance = AsyncMock()
        mock_bc_instance.execute.return_value = expected_result

        with patch(
            "PowerPlatform.Dataverse.aio.operations.async_batch._AsyncBatchClient",
            return_value=mock_bc_instance,
        ) as MockBatchClient:
            result = await batch.execute()

        MockBatchClient.assert_called_once_with(od)
        mock_bc_instance.execute.assert_awaited_once_with(batch._items, continue_on_error=False)
        assert result is expected_result

    async def test_execute_passes_continue_on_error(self):
        client, od = _make_client_with_mock_odata()
        batch = client.batch.new()

        mock_bc_instance = AsyncMock()
        mock_bc_instance.execute.return_value = BatchResult()

        with patch(
            "PowerPlatform.Dataverse.aio.operations.async_batch._AsyncBatchClient",
            return_value=mock_bc_instance,
        ):
            await batch.execute(continue_on_error=True)

        mock_bc_instance.execute.assert_awaited_once_with(batch._items, continue_on_error=True)

    async def test_execute_empty_batch(self):
        """Executing an empty batch should still call the batch client."""
        client, od = _make_client_with_mock_odata()
        batch = client.batch.new()

        mock_bc_instance = AsyncMock()
        mock_bc_instance.execute.return_value = BatchResult()

        with patch(
            "PowerPlatform.Dataverse.aio.operations.async_batch._AsyncBatchClient",
            return_value=mock_bc_instance,
        ):
            result = await batch.execute()

        mock_bc_instance.execute.assert_awaited_once()
        assert isinstance(result, BatchResult)


# ---------------------------------------------------------------------------
# AsyncBatchRecordOperations — upsert dict path
# ---------------------------------------------------------------------------

class TestAsyncBatchRecordUpsertDict:
    def _make_batch(self):
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        return client.batch.new()

    def test_upsert_dict_form_normalised(self):
        batch = self._make_batch()
        batch.records.upsert("account", [
            {"alternate_key": {"accountnumber": "ACC-1"}, "record": {"name": "Contoso"}},
        ])
        item = batch._items[0]
        assert isinstance(item, _RecordUpsert)
        assert item.items[0].alternate_key == {"accountnumber": "ACC-1"}

    def test_upsert_invalid_dict_raises(self):
        batch = self._make_batch()
        with pytest.raises(TypeError):
            batch.records.upsert("account", [{"bad": "shape"}])


# ---------------------------------------------------------------------------
# AsyncBatchQueryOperations — sql success path
# ---------------------------------------------------------------------------

class TestAsyncBatchQuerySqlSuccess:
    def _make_batch(self):
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        return client.batch.new()

    def test_sql_success_appends_query_sql(self):
        batch = self._make_batch()
        batch.query.sql("SELECT name FROM account")
        assert len(batch._items) == 1
        item = batch._items[0]
        assert isinstance(item, _QuerySql)
        assert item.sql == "SELECT name FROM account"

    def test_sql_strips_whitespace(self):
        batch = self._make_batch()
        batch.query.sql("  SELECT name FROM account  ")
        assert batch._items[0].sql == "SELECT name FROM account"


# ---------------------------------------------------------------------------
# AsyncBatchTableOperations — all methods
# ---------------------------------------------------------------------------

class TestAsyncBatchTableOperations:
    def _make_batch(self):
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        return client.batch.new()

    def test_create_appends_table_create(self):
        batch = self._make_batch()
        batch.tables.create("new_Product", {"new_Title": "string"})
        assert isinstance(batch._items[0], _TableCreate)
        assert batch._items[0].table == "new_Product"

    def test_create_with_solution_and_primary(self):
        batch = self._make_batch()
        batch.tables.create("new_T", {}, solution="MySolution", primary_column="new_id")
        item = batch._items[0]
        assert item.solution == "MySolution"
        assert item.primary_column == "new_id"

    def test_delete_appends_table_delete(self):
        batch = self._make_batch()
        batch.tables.delete("new_Product")
        assert isinstance(batch._items[0], _TableDelete)
        assert batch._items[0].table == "new_Product"

    def test_get_appends_table_get(self):
        batch = self._make_batch()
        batch.tables.get("new_Product")
        assert isinstance(batch._items[0], _TableGet)
        assert batch._items[0].table == "new_Product"

    def test_list_appends_table_list(self):
        batch = self._make_batch()
        batch.tables.list()
        assert isinstance(batch._items[0], _TableList)

    def test_list_passes_filter_and_select(self):
        batch = self._make_batch()
        batch.tables.list(filter="IsCustomEntity eq true", select=["LogicalName"])
        item = batch._items[0]
        assert item.filter == "IsCustomEntity eq true"
        assert item.select == ["LogicalName"]

    def test_add_columns_appends_table_add_columns(self):
        batch = self._make_batch()
        batch.tables.add_columns("new_Product", {"new_Desc": "string"})
        assert isinstance(batch._items[0], _TableAddColumns)
        assert batch._items[0].table == "new_Product"

    def test_remove_columns_appends_table_remove_columns(self):
        batch = self._make_batch()
        batch.tables.remove_columns("new_Product", ["new_Desc"])
        assert isinstance(batch._items[0], _TableRemoveColumns)

    def test_delete_relationship_appends(self):
        batch = self._make_batch()
        batch.tables.delete_relationship("rel-guid-123")
        assert isinstance(batch._items[0], _TableDeleteRelationship)
        assert batch._items[0].relationship_id == "rel-guid-123"

    def test_get_relationship_appends(self):
        batch = self._make_batch()
        batch.tables.get_relationship("new_account_contact")
        assert isinstance(batch._items[0], _TableGetRelationship)
        assert batch._items[0].schema_name == "new_account_contact"

    def test_create_lookup_field_appends(self):
        batch = self._make_batch()
        batch.tables.create_lookup_field("contact", "new_accountid", "account")
        assert isinstance(batch._items[0], _TableCreateLookupField)
        assert batch._items[0].referencing_table == "contact"
        assert batch._items[0].referenced_table == "account"

    def test_create_one_to_many_appends(self):
        batch = self._make_batch()
        lookup = MagicMock()
        relationship = MagicMock()
        batch.tables.create_one_to_many_relationship(lookup, relationship)
        assert isinstance(batch._items[0], _TableCreateOneToMany)

    def test_create_many_to_many_appends(self):
        batch = self._make_batch()
        relationship = MagicMock()
        batch.tables.create_many_to_many_relationship(relationship)
        assert isinstance(batch._items[0], _TableCreateManyToMany)

    def test_multiple_table_ops_in_order(self):
        batch = self._make_batch()
        batch.tables.get("new_A")
        batch.tables.delete("new_B")
        batch.tables.list()
        assert len(batch._items) == 3
        assert isinstance(batch._items[0], _TableGet)
        assert isinstance(batch._items[1], _TableDelete)
        assert isinstance(batch._items[2], _TableList)


# ---------------------------------------------------------------------------
# AsyncBatchDataFrameOperations
# ---------------------------------------------------------------------------

class TestAsyncBatchDataFrameOperations:
    def _make_batch(self):
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        return client.batch.new()

    def test_create_from_dataframe(self):
        import pandas as pd
        batch = self._make_batch()
        df = pd.DataFrame([{"name": "Contoso"}, {"name": "Fabrikam"}])
        batch.dataframe.create("account", df)
        # Should have appended a _RecordCreate with the list of dicts
        assert len(batch._items) == 1
        item = batch._items[0]
        assert isinstance(item, _RecordCreate)
        assert item.table == "account"
        assert isinstance(item.data, list)
        assert len(item.data) == 2

    def test_create_non_dataframe_raises(self):
        batch = self._make_batch()
        with pytest.raises(TypeError):
            batch.dataframe.create("account", [{"name": "X"}])

    def test_create_empty_dataframe_raises(self):
        import pandas as pd
        batch = self._make_batch()
        with pytest.raises(ValueError):
            batch.dataframe.create("account", pd.DataFrame())

    def test_update_from_dataframe(self):
        import pandas as pd
        batch = self._make_batch()
        df = pd.DataFrame([
            {"accountid": "guid-1", "name": "New Name"},
            {"accountid": "guid-2", "name": "Other Name"},
        ])
        batch.dataframe.update("account", df, id_column="accountid")
        assert len(batch._items) == 1
        item = batch._items[0]
        assert isinstance(item, _RecordUpdate)

    def test_update_non_dataframe_raises(self):
        batch = self._make_batch()
        with pytest.raises(TypeError):
            batch.dataframe.update("account", "bad", "accountid")

    def test_update_empty_dataframe_raises(self):
        import pandas as pd
        batch = self._make_batch()
        with pytest.raises(ValueError):
            batch.dataframe.update("account", pd.DataFrame(), "accountid")

    def test_update_missing_id_column_raises(self):
        import pandas as pd
        batch = self._make_batch()
        df = pd.DataFrame([{"name": "X"}])
        with pytest.raises(ValueError):
            batch.dataframe.update("account", df, "accountid")

    def test_delete_from_series(self):
        import pandas as pd
        batch = self._make_batch()
        ids = pd.Series(["guid-1", "guid-2"])
        batch.dataframe.delete("account", ids)
        assert len(batch._items) == 1
        item = batch._items[0]
        assert isinstance(item, _RecordDelete)

    def test_delete_non_series_raises(self):
        batch = self._make_batch()
        with pytest.raises(TypeError):
            batch.dataframe.delete("account", ["guid-1"])

    def test_delete_empty_series_is_noop(self):
        import pandas as pd
        batch = self._make_batch()
        batch.dataframe.delete("account", pd.Series([], dtype=str))
        assert len(batch._items) == 0
