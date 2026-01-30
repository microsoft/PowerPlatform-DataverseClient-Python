# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for TableOperations namespace class."""

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.results import RequestTelemetryData


class TestTableOperations(unittest.TestCase):
    """Test cases for the TableOperations namespace class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create mock credential
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"

        # Initialize the client under test
        self.client = DataverseClient(self.base_url, self.mock_credential)

        # Mock the internal _odata client
        self.client._odata = MagicMock()

    def test_tables_namespace_exists(self):
        """Test that tables namespace is accessible on the client."""
        self.assertIsNotNone(self.client.tables)
        self.assertEqual(self.client.tables._client, self.client)

    def test_tables_create_basic(self):
        """Test tables.create() with basic parameters."""
        expected_result = {
            "table_schema_name": "new_Product",
            "entity_set_name": "new_products",
            "table_logical_name": "new_product",
            "metadata_id": "meta-123",
            "columns_created": ["new_Name", "new_Price"],
        }
        mock_metadata = RequestTelemetryData(client_request_id="test-create-table")
        self.client._odata._create_table.return_value = (expected_result, mock_metadata)

        result = self.client.tables.create(
            "new_Product",
            {"new_Name": "string", "new_Price": "decimal"},
        )

        self.client._odata._create_table.assert_called_once_with(
            "new_Product",
            {"new_Name": "string", "new_Price": "decimal"},
            None,  # solution
            None,  # primary_column
        )
        self.assertEqual(result["table_schema_name"], "new_Product")
        self.assertEqual(result["columns_created"], ["new_Name", "new_Price"])

    def test_tables_create_with_solution(self):
        """Test tables.create() with solution parameter."""
        expected_result = {"table_schema_name": "new_Product"}
        mock_metadata = RequestTelemetryData()
        self.client._odata._create_table.return_value = (expected_result, mock_metadata)

        result = self.client.tables.create(
            "new_Product",
            {"new_Name": "string"},
            solution="MySolution",
        )

        self.client._odata._create_table.assert_called_once_with(
            "new_Product",
            {"new_Name": "string"},
            "MySolution",
            None,
        )

    def test_tables_create_with_primary_column(self):
        """Test tables.create() with primary_column parameter."""
        expected_result = {"table_schema_name": "new_Product"}
        mock_metadata = RequestTelemetryData()
        self.client._odata._create_table.return_value = (expected_result, mock_metadata)

        result = self.client.tables.create(
            "new_Product",
            {"new_Title": "string"},
            primary_column="new_Title",
        )

        self.client._odata._create_table.assert_called_once_with(
            "new_Product",
            {"new_Title": "string"},
            None,
            "new_Title",
        )

    def test_tables_create_with_telemetry(self):
        """Test tables.create() with telemetry access."""
        expected_result = {"table_schema_name": "new_Test"}
        mock_metadata = RequestTelemetryData(client_request_id="create-123", service_request_id="svc-456")
        self.client._odata._create_table.return_value = (expected_result, mock_metadata)

        result = self.client.tables.create("new_Test", {"new_Col": "string"})

        response = result.with_response_details()
        self.assertEqual(response.telemetry["client_request_id"], "create-123")
        self.assertEqual(response.telemetry["service_request_id"], "svc-456")

    def test_tables_delete_basic(self):
        """Test tables.delete() basic functionality."""
        mock_metadata = RequestTelemetryData(client_request_id="test-delete")
        self.client._odata._delete_table.return_value = (None, mock_metadata)

        result = self.client.tables.delete("new_MyTestTable")

        self.client._odata._delete_table.assert_called_once_with("new_MyTestTable")
        self.assertIsNone(result.value)

    def test_tables_delete_with_telemetry(self):
        """Test tables.delete() with telemetry access."""
        mock_metadata = RequestTelemetryData(client_request_id="delete-123")
        self.client._odata._delete_table.return_value = (None, mock_metadata)

        result = self.client.tables.delete("new_Test")

        response = result.with_response_details()
        self.assertEqual(response.telemetry["client_request_id"], "delete-123")

    def test_tables_info_found(self):
        """Test tables.get() when table exists."""
        expected_result = {
            "table_schema_name": "account",
            "table_logical_name": "account",
            "entity_set_name": "accounts",
            "metadata_id": "meta-abc",
        }
        mock_metadata = RequestTelemetryData(client_request_id="test-info")
        self.client._odata._get_table_info.return_value = (expected_result, mock_metadata)

        result = self.client.tables.get("account")

        self.client._odata._get_table_info.assert_called_once_with("account")
        self.assertEqual(result["table_logical_name"], "account")
        self.assertEqual(result["entity_set_name"], "accounts")

    def test_tables_info_not_found(self):
        """Test tables.get() when table doesn't exist."""
        mock_metadata = RequestTelemetryData()
        self.client._odata._get_table_info.return_value = (None, mock_metadata)

        result = self.client.tables.get("nonexistent_table")

        self.assertIsNone(result.value)

    def test_tables_info_with_telemetry(self):
        """Test tables.get() with telemetry access."""
        expected_result = {"table_schema_name": "account"}
        mock_metadata = RequestTelemetryData(client_request_id="info-123")
        self.client._odata._get_table_info.return_value = (expected_result, mock_metadata)

        result = self.client.tables.get("account")

        response = result.with_response_details()
        self.assertEqual(response.telemetry["client_request_id"], "info-123")

    def test_tables_list_basic(self):
        """Test tables.list() basic functionality."""
        expected_result = [
            {"table_schema_name": "new_Table1"},
            {"table_schema_name": "new_Table2"},
        ]
        mock_metadata = RequestTelemetryData(client_request_id="test-list")
        self.client._odata._list_tables.return_value = (expected_result, mock_metadata)

        result = self.client.tables.list()

        self.client._odata._list_tables.assert_called_once()
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["table_schema_name"], "new_Table1")

    def test_tables_list_empty(self):
        """Test tables.list() with no custom tables."""
        mock_metadata = RequestTelemetryData()
        self.client._odata._list_tables.return_value = ([], mock_metadata)

        result = self.client.tables.list()

        self.assertEqual(result.value, [])
        self.assertEqual(len(result), 0)

    def test_tables_list_iteration(self):
        """Test tables.list() results support iteration."""
        expected_result = [
            {"table_schema_name": "new_A"},
            {"table_schema_name": "new_B"},
            {"table_schema_name": "new_C"},
        ]
        mock_metadata = RequestTelemetryData()
        self.client._odata._list_tables.return_value = (expected_result, mock_metadata)

        result = self.client.tables.list()

        names = [t["table_schema_name"] for t in result]
        self.assertEqual(names, ["new_A", "new_B", "new_C"])

    def test_tables_add_columns_basic(self):
        """Test tables.add_columns() basic functionality."""
        expected_result = ["new_Description", "new_InStock"]
        mock_metadata = RequestTelemetryData(client_request_id="test-add-cols")
        self.client._odata._create_columns.return_value = (expected_result, mock_metadata)

        result = self.client.tables.add_columns(
            "new_Product",
            {"new_Description": "string", "new_InStock": "bool"},
        )

        self.client._odata._create_columns.assert_called_once_with(
            "new_Product",
            {"new_Description": "string", "new_InStock": "bool"},
        )
        self.assertEqual(result.value, ["new_Description", "new_InStock"])

    def test_tables_add_columns_with_telemetry(self):
        """Test tables.add_columns() with telemetry access."""
        expected_result = ["new_Col"]
        mock_metadata = RequestTelemetryData(client_request_id="add-cols-123")
        self.client._odata._create_columns.return_value = (expected_result, mock_metadata)

        result = self.client.tables.add_columns("new_Test", {"new_Col": "string"})

        response = result.with_response_details()
        self.assertEqual(response.telemetry["client_request_id"], "add-cols-123")

    def test_tables_remove_columns_single(self):
        """Test tables.remove_columns() with single column."""
        expected_result = ["new_Scratch"]
        mock_metadata = RequestTelemetryData(client_request_id="test-remove-col")
        self.client._odata._delete_columns.return_value = (expected_result, mock_metadata)

        result = self.client.tables.remove_columns("new_Product", "new_Scratch")

        self.client._odata._delete_columns.assert_called_once_with("new_Product", "new_Scratch")
        self.assertEqual(result.value, ["new_Scratch"])

    def test_tables_remove_columns_multiple(self):
        """Test tables.remove_columns() with multiple columns."""
        expected_result = ["new_Scratch", "new_Flags"]
        mock_metadata = RequestTelemetryData()
        self.client._odata._delete_columns.return_value = (expected_result, mock_metadata)

        result = self.client.tables.remove_columns(
            "new_Product",
            ["new_Scratch", "new_Flags"],
        )

        self.client._odata._delete_columns.assert_called_once_with(
            "new_Product",
            ["new_Scratch", "new_Flags"],
        )
        self.assertEqual(result.value, ["new_Scratch", "new_Flags"])

    def test_tables_remove_columns_with_telemetry(self):
        """Test tables.remove_columns() with telemetry access."""
        expected_result = ["new_Col"]
        mock_metadata = RequestTelemetryData(client_request_id="remove-cols-123")
        self.client._odata._delete_columns.return_value = (expected_result, mock_metadata)

        result = self.client.tables.remove_columns("new_Test", ["new_Col"])

        response = result.with_response_details()
        self.assertEqual(response.telemetry["client_request_id"], "remove-cols-123")


if __name__ == "__main__":
    unittest.main()
