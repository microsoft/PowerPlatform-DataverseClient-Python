# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock, patch

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.operations.batch import (
    BatchOperations,
    BatchRequest,
    BatchRecordOperations,
    BatchTableOperations,
    BatchQueryOperations,
    ChangeSet,
    ChangeSetRecordOperations,
)
from PowerPlatform.Dataverse.data._batch import (
    _RecordCreate,
    _RecordUpdate,
    _RecordDelete,
    _RecordGet,
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
from PowerPlatform.Dataverse.models.batch import BatchResult, BatchItemResponse
from PowerPlatform.Dataverse.core.errors import ValidationError


class TestBatchOperationsNamespace(unittest.TestCase):
    """Tests for the client.batch namespace."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)

    def test_namespace_exists(self):
        """client.batch should be a BatchOperations instance."""
        self.assertIsInstance(self.client.batch, BatchOperations)

    def test_new_returns_batch_request(self):
        """client.batch.new() should return a BatchRequest."""
        batch = self.client.batch.new()
        self.assertIsInstance(batch, BatchRequest)

    def test_new_returns_new_instance_each_call(self):
        """Each call to new() should return a distinct BatchRequest."""
        b1 = self.client.batch.new()
        b2 = self.client.batch.new()
        self.assertIsNot(b1, b2)


class TestBatchRequest(unittest.TestCase):
    """Tests for BatchRequest builder."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.batch = self.client.batch.new()

    def test_has_records_namespace(self):
        self.assertIsInstance(self.batch.records, BatchRecordOperations)

    def test_has_tables_namespace(self):
        self.assertIsInstance(self.batch.tables, BatchTableOperations)

    def test_has_query_namespace(self):
        self.assertIsInstance(self.batch.query, BatchQueryOperations)

    def test_changeset_returns_changeset(self):
        cs = self.batch.changeset()
        self.assertIsInstance(cs, ChangeSet)

    def test_changeset_added_to_items(self):
        cs = self.batch.changeset()
        self.assertEqual(len(self.batch._items), 1)
        self.assertIsInstance(self.batch._items[0], _ChangeSet)

    def test_execute_calls_batch_client(self):
        """execute() should call _BatchClient.execute via _scoped_odata."""
        mock_od = MagicMock()
        mock_result = BatchResult(responses=[BatchItemResponse(status_code=204)])
        mock_od._entity_set_from_schema_name.return_value = "accounts"
        mock_od._build_create.return_value = MagicMock()
        mock_od._request.return_value = MagicMock(
            headers={"Content-Type": "multipart/mixed; boundary=batch_xyz"},
            text="",
        )

        self.client._odata = mock_od
        self.batch.records.create("account", {"name": "Contoso"})

        with patch(
            "PowerPlatform.Dataverse.data._batch._BatchClient.execute",
            return_value=mock_result,
        ) as mock_exec:
            result = self.batch.execute()
            mock_exec.assert_called_once_with(self.batch._items, continue_on_error=False)
            self.assertIs(result, mock_result)

    def test_execute_continue_on_error_passed_through(self):
        """continue_on_error=True should be forwarded to _BatchClient.execute."""
        mock_result = BatchResult()
        with patch(
            "PowerPlatform.Dataverse.data._batch._BatchClient.execute",
            return_value=mock_result,
        ) as mock_exec:
            self.client._odata = MagicMock()
            self.batch.execute(continue_on_error=True)
            mock_exec.assert_called_once_with(self.batch._items, continue_on_error=True)


class TestBatchRecordOperations(unittest.TestCase):
    """Tests that BatchRecordOperations appends the correct intent objects."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.batch = self.client.batch.new()

    def test_create_single_appends_record_create(self):
        self.batch.records.create("account", {"name": "Contoso"})
        self.assertEqual(len(self.batch._items), 1)
        item = self.batch._items[0]
        self.assertIsInstance(item, _RecordCreate)
        self.assertEqual(item.table, "account")
        self.assertEqual(item.data, {"name": "Contoso"})

    def test_create_list_appends_record_create(self):
        data = [{"name": "A"}, {"name": "B"}]
        self.batch.records.create("account", data)
        item = self.batch._items[0]
        self.assertIsInstance(item, _RecordCreate)
        self.assertIs(item.data, data)

    def test_update_single_appends_record_update(self):
        self.batch.records.update("account", "guid-1", {"name": "X"})
        item = self.batch._items[0]
        self.assertIsInstance(item, _RecordUpdate)
        self.assertEqual(item.table, "account")
        self.assertEqual(item.ids, "guid-1")
        self.assertEqual(item.changes, {"name": "X"})

    def test_update_list_appends_record_update(self):
        ids = ["guid-1", "guid-2"]
        changes = {"statecode": 0}
        self.batch.records.update("account", ids, changes)
        item = self.batch._items[0]
        self.assertIsInstance(item, _RecordUpdate)
        self.assertIs(item.ids, ids)

    def test_delete_single_appends_record_delete(self):
        self.batch.records.delete("account", "guid-to-del")
        item = self.batch._items[0]
        self.assertIsInstance(item, _RecordDelete)
        self.assertEqual(item.table, "account")
        self.assertEqual(item.ids, "guid-to-del")
        self.assertTrue(item.use_bulk_delete)

    def test_delete_list_use_bulk_delete_false(self):
        self.batch.records.delete("account", ["g1", "g2"], use_bulk_delete=False)
        item = self.batch._items[0]
        self.assertIsInstance(item, _RecordDelete)
        self.assertFalse(item.use_bulk_delete)

    def test_get_single_appends_record_get(self):
        self.batch.records.get("account", "guid-1", select=["name"])
        item = self.batch._items[0]
        self.assertIsInstance(item, _RecordGet)
        self.assertEqual(item.table, "account")
        self.assertEqual(item.record_id, "guid-1")
        self.assertEqual(item.select, ["name"])

    def test_get_single_no_select(self):
        self.batch.records.get("account", "guid-1")
        item = self.batch._items[0]
        self.assertIsNone(item.select)

    def test_multiple_ops_appended_in_order(self):
        self.batch.records.create("account", {"name": "X"})
        self.batch.records.delete("account", "g1")
        self.batch.records.get("account", "g2")
        self.assertEqual(len(self.batch._items), 3)
        self.assertIsInstance(self.batch._items[0], _RecordCreate)
        self.assertIsInstance(self.batch._items[1], _RecordDelete)
        self.assertIsInstance(self.batch._items[2], _RecordGet)


class TestBatchTableOperations(unittest.TestCase):
    """Tests that BatchTableOperations appends the correct intent objects."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.batch = self.client.batch.new()

    def test_create_appends_table_create(self):
        cols = {"new_Price": "decimal"}
        self.batch.tables.create("new_Product", cols, solution="Sol", primary_column="new_Name")
        item = self.batch._items[0]
        self.assertIsInstance(item, _TableCreate)
        self.assertEqual(item.table, "new_Product")
        self.assertIs(item.columns, cols)
        self.assertEqual(item.solution, "Sol")
        self.assertEqual(item.primary_column, "new_Name")

    def test_delete_appends_table_delete(self):
        self.batch.tables.delete("new_Product")
        item = self.batch._items[0]
        self.assertIsInstance(item, _TableDelete)
        self.assertEqual(item.table, "new_Product")

    def test_get_appends_table_get(self):
        self.batch.tables.get("new_Product")
        item = self.batch._items[0]
        self.assertIsInstance(item, _TableGet)
        self.assertEqual(item.table, "new_Product")

    def test_list_appends_table_list(self):
        self.batch.tables.list()
        item = self.batch._items[0]
        self.assertIsInstance(item, _TableList)

    def test_add_columns_appends_table_add_columns(self):
        cols = {"new_Notes": "string"}
        self.batch.tables.add_columns("new_Product", cols)
        item = self.batch._items[0]
        self.assertIsInstance(item, _TableAddColumns)
        self.assertEqual(item.table, "new_Product")
        self.assertIs(item.columns, cols)

    def test_remove_columns_single_string(self):
        self.batch.tables.remove_columns("new_Product", "new_Notes")
        item = self.batch._items[0]
        self.assertIsInstance(item, _TableRemoveColumns)
        self.assertEqual(item.columns, "new_Notes")

    def test_remove_columns_list(self):
        self.batch.tables.remove_columns("new_Product", ["new_A", "new_B"])
        item = self.batch._items[0]
        self.assertIsInstance(item, _TableRemoveColumns)
        self.assertEqual(item.columns, ["new_A", "new_B"])

    def test_create_one_to_many_appends_intent(self):
        lookup = MagicMock()
        relationship = MagicMock()
        self.batch.tables.create_one_to_many_relationship(lookup, relationship, solution="Sol")
        item = self.batch._items[0]
        self.assertIsInstance(item, _TableCreateOneToMany)
        self.assertIs(item.lookup, lookup)
        self.assertIs(item.relationship, relationship)
        self.assertEqual(item.solution, "Sol")

    def test_create_many_to_many_appends_intent(self):
        relationship = MagicMock()
        self.batch.tables.create_many_to_many_relationship(relationship)
        item = self.batch._items[0]
        self.assertIsInstance(item, _TableCreateManyToMany)
        self.assertIs(item.relationship, relationship)
        self.assertIsNone(item.solution)

    def test_delete_relationship_appends_intent(self):
        self.batch.tables.delete_relationship("rel-guid-1")
        item = self.batch._items[0]
        self.assertIsInstance(item, _TableDeleteRelationship)
        self.assertEqual(item.relationship_id, "rel-guid-1")

    def test_get_relationship_appends_intent(self):
        self.batch.tables.get_relationship("new_Dept_Emp")
        item = self.batch._items[0]
        self.assertIsInstance(item, _TableGetRelationship)
        self.assertEqual(item.schema_name, "new_Dept_Emp")

    def test_create_lookup_field_appends_intent(self):
        self.batch.tables.create_lookup_field(
            "new_order",
            "new_ProductId",
            "new_product",
            display_name="Product",
            solution="Sol",
        )
        item = self.batch._items[0]
        self.assertIsInstance(item, _TableCreateLookupField)
        self.assertEqual(item.referencing_table, "new_order")
        self.assertEqual(item.lookup_field_name, "new_ProductId")
        self.assertEqual(item.referenced_table, "new_product")
        self.assertEqual(item.display_name, "Product")
        self.assertEqual(item.solution, "Sol")


class TestBatchQueryOperations(unittest.TestCase):
    """Tests that BatchQueryOperations appends the correct intent objects."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.batch = self.client.batch.new()

    def test_sql_appends_query_sql(self):
        self.batch.query.sql("SELECT accountid FROM account")
        item = self.batch._items[0]
        self.assertIsInstance(item, _QuerySql)
        self.assertEqual(item.sql, "SELECT accountid FROM account")

    def test_sql_strips_whitespace(self):
        self.batch.query.sql("  SELECT name FROM account  ")
        item = self.batch._items[0]
        self.assertEqual(item.sql, "SELECT name FROM account")

    def test_sql_empty_raises(self):
        with self.assertRaises(ValidationError):
            self.batch.query.sql("")

    def test_sql_whitespace_only_raises(self):
        with self.assertRaises(ValidationError):
            self.batch.query.sql("   ")

    def test_sql_non_string_raises(self):
        with self.assertRaises((ValidationError, AttributeError)):
            self.batch.query.sql(None)


class TestChangeSet(unittest.TestCase):
    """Tests for ChangeSet context-manager and ChangeSetRecordOperations."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.batch = self.client.batch.new()

    def test_changeset_records_is_change_set_record_ops(self):
        cs = self.batch.changeset()
        self.assertIsInstance(cs.records, ChangeSetRecordOperations)

    def test_changeset_create_returns_content_id_ref(self):
        cs = self.batch.changeset()
        ref = cs.records.create("account", {"name": "X"})
        self.assertIsInstance(ref, str)
        self.assertTrue(ref.startswith("$"))

    def test_changeset_create_content_ids_increment(self):
        cs = self.batch.changeset()
        ref1 = cs.records.create("account", {"name": "A"})
        ref2 = cs.records.create("contact", {"firstname": "B"})
        self.assertNotEqual(ref1, ref2)
        n1 = int(ref1[1:])
        n2 = int(ref2[1:])
        self.assertGreater(n2, n1)

    def test_changeset_update_adds_operation(self):
        cs = self.batch.changeset()
        cs.records.update("account", "guid-1", {"name": "Y"})
        internal = self.batch._items[0]
        self.assertIsInstance(internal, _ChangeSet)
        self.assertEqual(len(internal.operations), 1)

    def test_changeset_delete_adds_operation(self):
        cs = self.batch.changeset()
        cs.records.delete("account", "guid-del")
        internal = self.batch._items[0]
        self.assertEqual(len(internal.operations), 1)

    def test_changeset_as_context_manager(self):
        with self.batch.changeset() as cs:
            cs.records.create("account", {"name": "ACME"})
        internal = self.batch._items[0]
        self.assertEqual(len(internal.operations), 1)

    def test_changeset_ops_in_order(self):
        cs = self.batch.changeset()
        ref = cs.records.create("lead", {"firstname": "Ada"})
        cs.records.update("contact", ref, {"lastname": "L"})
        cs.records.delete("task", "task-guid")
        internal = self.batch._items[0]
        self.assertEqual(len(internal.operations), 3)
        self.assertIsInstance(internal.operations[0], _RecordCreate)
        self.assertIsInstance(internal.operations[1], _RecordUpdate)
        self.assertIsInstance(internal.operations[2], _RecordDelete)


class TestBatchItemResponse(unittest.TestCase):
    """Tests for BatchItemResponse model."""

    def test_is_success_2xx(self):
        for code in (200, 201, 204):
            item = BatchItemResponse(status_code=code)
            self.assertTrue(item.is_success, f"Expected is_success for {code}")

    def test_is_not_success_4xx(self):
        for code in (400, 404, 409):
            item = BatchItemResponse(status_code=code)
            self.assertFalse(item.is_success, f"Expected not is_success for {code}")

    def test_is_not_success_5xx(self):
        item = BatchItemResponse(status_code=500)
        self.assertFalse(item.is_success)


class TestBatchResult(unittest.TestCase):
    """Tests for BatchResult model."""

    def test_succeeded(self):
        responses = [
            BatchItemResponse(status_code=204),
            BatchItemResponse(status_code=400),
            BatchItemResponse(status_code=201, entity_id="guid-1"),
        ]
        result = BatchResult(responses=responses)
        self.assertEqual(len(result.succeeded), 2)
        self.assertEqual(len(result.failed), 1)

    def test_has_errors_true(self):
        result = BatchResult(
            responses=[
                BatchItemResponse(status_code=204),
                BatchItemResponse(status_code=404),
            ]
        )
        self.assertTrue(result.has_errors)

    def test_has_errors_false(self):
        result = BatchResult(
            responses=[
                BatchItemResponse(status_code=204),
                BatchItemResponse(status_code=201),
            ]
        )
        self.assertFalse(result.has_errors)

    def test_entity_ids(self):
        result = BatchResult(
            responses=[
                BatchItemResponse(status_code=201, entity_id="guid-1"),
                BatchItemResponse(status_code=204),
                BatchItemResponse(status_code=201, entity_id="guid-2"),
                BatchItemResponse(status_code=400),
            ]
        )
        self.assertEqual(result.entity_ids, ["guid-1", "guid-2"])

    def test_empty_result(self):
        result = BatchResult()
        self.assertEqual(result.responses, [])
        self.assertEqual(result.succeeded, [])
        self.assertEqual(result.failed, [])
        self.assertFalse(result.has_errors)
        self.assertEqual(result.entity_ids, [])


if __name__ == "__main__":
    unittest.main()
