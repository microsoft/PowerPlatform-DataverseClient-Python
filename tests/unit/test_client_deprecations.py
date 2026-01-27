# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for DataverseClient deprecated methods and backward compatibility."""

import unittest
import warnings
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.results import RequestTelemetryData


class TestDeprecationWarnings(unittest.TestCase):
    """Test that deprecated methods emit DeprecationWarning."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"
        self.client = DataverseClient(self.base_url, self.mock_credential)
        self.client._odata = MagicMock()

        # Setup common mock returns
        mock_metadata = RequestTelemetryData(client_request_id="test")
        self.client._odata._create.return_value = ("guid", mock_metadata)
        self.client._odata._create_multiple.return_value = (["guid1", "guid2"], mock_metadata)
        self.client._odata._update.return_value = (None, mock_metadata)
        self.client._odata._update_by_ids.return_value = (None, mock_metadata)
        self.client._odata._delete.return_value = (None, mock_metadata)
        self.client._odata._delete_multiple.return_value = ("job-id", mock_metadata)
        self.client._odata._get.return_value = ({"id": "1"}, mock_metadata)
        self.client._odata._get_multiple.return_value = iter([([{"id": "1"}], mock_metadata)])
        self.client._odata._query_sql.return_value = ([{"name": "Test"}], mock_metadata)
        self.client._odata._get_table_info.return_value = ({"table": "info"}, mock_metadata)
        self.client._odata._create_table.return_value = ({"table": "created"}, mock_metadata)
        self.client._odata._delete_table.return_value = (None, mock_metadata)
        self.client._odata._list_tables.return_value = ([{"table": "1"}], mock_metadata)
        self.client._odata._create_columns.return_value = (["col1"], mock_metadata)
        self.client._odata._delete_columns.return_value = (["col1"], mock_metadata)
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

    def test_create_deprecation_warning(self):
        """Test that client.create() emits DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.client.create("account", {"name": "Test"})

            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))
            self.assertIn("client.records.create()", str(w[0].message))

    def test_update_deprecation_warning(self):
        """Test that client.update() emits DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.client.update("account", "guid", {"name": "Test"})

            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))
            self.assertIn("client.records.update()", str(w[0].message))

    def test_delete_deprecation_warning(self):
        """Test that client.delete() emits DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.client.delete("account", "guid")

            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))
            self.assertIn("client.records.delete()", str(w[0].message))

    def test_get_single_deprecation_warning(self):
        """Test that client.get() with record_id emits DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.client.get("account", record_id="guid")

            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))
            self.assertIn("client.records.get()", str(w[0].message))

    def test_get_multiple_deprecation_warning(self):
        """Test that client.get() without record_id emits DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            list(self.client.get("account", filter="statecode eq 0"))

            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))
            self.assertIn("client.query.get()", str(w[0].message))

    def test_query_sql_deprecation_warning(self):
        """Test that client.query_sql() emits DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.client.query_sql("SELECT name FROM account")

            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))
            self.assertIn("client.query.sql()", str(w[0].message))

    def test_get_table_info_deprecation_warning(self):
        """Test that client.get_table_info() emits DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.client.get_table_info("account")

            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))
            self.assertIn("client.tables.info()", str(w[0].message))

    def test_create_table_deprecation_warning(self):
        """Test that client.create_table() emits DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.client.create_table("new_Test", {"new_Col": "string"})

            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))
            self.assertIn("client.tables.create()", str(w[0].message))

    def test_delete_table_deprecation_warning(self):
        """Test that client.delete_table() emits DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.client.delete_table("new_Test")

            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))
            self.assertIn("client.tables.delete()", str(w[0].message))

    def test_list_tables_deprecation_warning(self):
        """Test that client.list_tables() emits DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.client.list_tables()

            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))
            self.assertIn("client.tables.list()", str(w[0].message))

    def test_create_columns_deprecation_warning(self):
        """Test that client.create_columns() emits DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.client.create_columns("new_Test", {"new_Col": "string"})

            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))
            self.assertIn("client.tables.add_columns()", str(w[0].message))

    def test_delete_columns_deprecation_warning(self):
        """Test that client.delete_columns() emits DeprecationWarning."""
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            self.client.delete_columns("new_Test", ["new_Col"])

            self.assertEqual(len(w), 1)
            self.assertTrue(issubclass(w[0].category, DeprecationWarning))
            self.assertIn("client.tables.remove_columns()", str(w[0].message))


class TestBackwardCompatibility(unittest.TestCase):
    """Test that legacy methods produce identical results to namespace methods."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"
        self.client = DataverseClient(self.base_url, self.mock_credential)
        self.client._odata = MagicMock()

    def test_create_single_backward_compatible(self):
        """Test legacy create() returns same result as records.create()."""
        mock_metadata = RequestTelemetryData(client_request_id="test-123")
        self.client._odata._create.return_value = ("guid-abc", mock_metadata)
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            legacy_result = self.client.create("account", {"name": "Test"})

        # Reset mock to test namespace method
        self.client._odata._create.reset_mock()
        self.client._odata._create.return_value = ("guid-abc", mock_metadata)

        namespace_result = self.client.records.create("account", {"name": "Test"})

        # Results should be equivalent
        self.assertEqual(legacy_result[0], namespace_result[0])
        self.assertEqual(legacy_result.value, namespace_result.value)

    def test_create_multiple_backward_compatible(self):
        """Test legacy create() with list returns same result as records.create()."""
        mock_metadata = RequestTelemetryData(client_request_id="test-456")
        self.client._odata._create_multiple.return_value = (["guid-1", "guid-2"], mock_metadata)
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            legacy_result = self.client.create("account", [{"name": "A"}, {"name": "B"}])

        self.client._odata._create_multiple.reset_mock()
        self.client._odata._create_multiple.return_value = (["guid-1", "guid-2"], mock_metadata)

        namespace_result = self.client.records.create("account", [{"name": "A"}, {"name": "B"}])

        self.assertEqual(list(legacy_result), list(namespace_result))
        self.assertEqual(legacy_result.value, namespace_result.value)

    def test_update_single_backward_compatible(self):
        """Test legacy update() returns same result as records.update()."""
        mock_metadata = RequestTelemetryData(client_request_id="test-789")
        self.client._odata._update.return_value = (None, mock_metadata)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            legacy_result = self.client.update("account", "guid", {"name": "Updated"})

        self.client._odata._update.reset_mock()
        self.client._odata._update.return_value = (None, mock_metadata)

        namespace_result = self.client.records.update("account", "guid", {"name": "Updated"})

        self.assertEqual(legacy_result.value, namespace_result.value)

    def test_delete_single_backward_compatible(self):
        """Test legacy delete() returns same result as records.delete()."""
        mock_metadata = RequestTelemetryData(client_request_id="test-delete")
        self.client._odata._delete.return_value = (None, mock_metadata)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            legacy_result = self.client.delete("account", "guid")

        self.client._odata._delete.reset_mock()
        self.client._odata._delete.return_value = (None, mock_metadata)

        namespace_result = self.client.records.delete("account", "guid")

        self.assertEqual(legacy_result.value, namespace_result.value)

    def test_delete_multiple_backward_compatible(self):
        """Test legacy delete() with list returns same result as records.delete()."""
        mock_metadata = RequestTelemetryData(client_request_id="test-bulk")
        self.client._odata._delete_multiple.return_value = ("job-123", mock_metadata)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            legacy_result = self.client.delete("account", ["guid-1", "guid-2"])

        self.client._odata._delete_multiple.reset_mock()
        self.client._odata._delete_multiple.return_value = ("job-123", mock_metadata)

        namespace_result = self.client.records.delete("account", ["guid-1", "guid-2"])

        self.assertEqual(legacy_result.value, namespace_result.value)

    def test_get_single_backward_compatible(self):
        """Test legacy get() with record_id returns same result as records.get()."""
        expected_record = {"accountid": "guid", "name": "Contoso"}
        mock_metadata = RequestTelemetryData(client_request_id="test-get")
        self.client._odata._get.return_value = (expected_record, mock_metadata)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            legacy_result = self.client.get("account", record_id="guid")

        self.client._odata._get.reset_mock()
        self.client._odata._get.return_value = (expected_record, mock_metadata)

        namespace_result = self.client.records.get("account", "guid")

        self.assertEqual(legacy_result.value, namespace_result.value)
        self.assertEqual(legacy_result["name"], namespace_result["name"])

    def test_get_multiple_backward_compatible(self):
        """Test legacy get() without record_id returns same result as query.get()."""
        expected_batch = [{"accountid": "1"}, {"accountid": "2"}]
        mock_metadata = RequestTelemetryData(client_request_id="test-query")
        self.client._odata._get_multiple.return_value = iter([(expected_batch, mock_metadata)])

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            legacy_result = list(self.client.get("account", filter="statecode eq 0"))

        self.client._odata._get_multiple.return_value = iter([(expected_batch, mock_metadata)])

        namespace_result = list(self.client.query.get("account", filter="statecode eq 0"))

        self.assertEqual(len(legacy_result), len(namespace_result))
        self.assertEqual(legacy_result[0].value, namespace_result[0].value)

    def test_query_sql_backward_compatible(self):
        """Test legacy query_sql() returns same result as query.sql()."""
        expected_results = [{"name": "Test"}]
        mock_metadata = RequestTelemetryData(client_request_id="test-sql")
        self.client._odata._query_sql.return_value = (expected_results, mock_metadata)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            legacy_result = self.client.query_sql("SELECT name FROM account")

        self.client._odata._query_sql.reset_mock()
        self.client._odata._query_sql.return_value = (expected_results, mock_metadata)

        namespace_result = self.client.query.sql("SELECT name FROM account")

        self.assertEqual(legacy_result.value, namespace_result.value)

    def test_get_table_info_backward_compatible(self):
        """Test legacy get_table_info() returns same result as tables.info()."""
        expected_info = {"table_schema_name": "account"}
        mock_metadata = RequestTelemetryData(client_request_id="test-info")
        self.client._odata._get_table_info.return_value = (expected_info, mock_metadata)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            legacy_result = self.client.get_table_info("account")

        self.client._odata._get_table_info.reset_mock()
        self.client._odata._get_table_info.return_value = (expected_info, mock_metadata)

        namespace_result = self.client.tables.info("account")

        self.assertEqual(legacy_result.value, namespace_result.value)

    def test_create_table_backward_compatible(self):
        """Test legacy create_table() returns same result as tables.create()."""
        expected_result = {"table_schema_name": "new_Test"}
        mock_metadata = RequestTelemetryData(client_request_id="test-create")
        self.client._odata._create_table.return_value = (expected_result, mock_metadata)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            legacy_result = self.client.create_table("new_Test", {"new_Col": "string"})

        self.client._odata._create_table.reset_mock()
        self.client._odata._create_table.return_value = (expected_result, mock_metadata)

        namespace_result = self.client.tables.create("new_Test", {"new_Col": "string"})

        self.assertEqual(legacy_result.value, namespace_result.value)

    def test_delete_table_backward_compatible(self):
        """Test legacy delete_table() returns same result as tables.delete()."""
        mock_metadata = RequestTelemetryData(client_request_id="test-delete")
        self.client._odata._delete_table.return_value = (None, mock_metadata)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            legacy_result = self.client.delete_table("new_Test")

        self.client._odata._delete_table.reset_mock()
        self.client._odata._delete_table.return_value = (None, mock_metadata)

        namespace_result = self.client.tables.delete("new_Test")

        self.assertEqual(legacy_result.value, namespace_result.value)

    def test_list_tables_backward_compatible(self):
        """Test legacy list_tables() returns same result as tables.list()."""
        expected_result = [{"table_schema_name": "new_Table1"}]
        mock_metadata = RequestTelemetryData(client_request_id="test-list")
        self.client._odata._list_tables.return_value = (expected_result, mock_metadata)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            legacy_result = self.client.list_tables()

        self.client._odata._list_tables.reset_mock()
        self.client._odata._list_tables.return_value = (expected_result, mock_metadata)

        namespace_result = self.client.tables.list()

        self.assertEqual(legacy_result.value, namespace_result.value)

    def test_create_columns_backward_compatible(self):
        """Test legacy create_columns() returns same result as tables.add_columns()."""
        expected_result = ["new_Col"]
        mock_metadata = RequestTelemetryData(client_request_id="test-cols")
        self.client._odata._create_columns.return_value = (expected_result, mock_metadata)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            legacy_result = self.client.create_columns("new_Test", {"new_Col": "string"})

        self.client._odata._create_columns.reset_mock()
        self.client._odata._create_columns.return_value = (expected_result, mock_metadata)

        namespace_result = self.client.tables.add_columns("new_Test", {"new_Col": "string"})

        self.assertEqual(legacy_result.value, namespace_result.value)

    def test_delete_columns_backward_compatible(self):
        """Test legacy delete_columns() returns same result as tables.remove_columns()."""
        expected_result = ["new_Col"]
        mock_metadata = RequestTelemetryData(client_request_id="test-del-cols")
        self.client._odata._delete_columns.return_value = (expected_result, mock_metadata)

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            legacy_result = self.client.delete_columns("new_Test", ["new_Col"])

        self.client._odata._delete_columns.reset_mock()
        self.client._odata._delete_columns.return_value = (expected_result, mock_metadata)

        namespace_result = self.client.tables.remove_columns("new_Test", ["new_Col"])

        self.assertEqual(legacy_result.value, namespace_result.value)


if __name__ == "__main__":
    unittest.main()
