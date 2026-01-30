# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for RecordOperations namespace class."""

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.results import RequestTelemetryData


class TestRecordOperations(unittest.TestCase):
    """Test cases for the RecordOperations namespace class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create mock credential
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"

        # Initialize the client under test
        self.client = DataverseClient(self.base_url, self.mock_credential)

        # Mock the internal _odata client
        self.client._odata = MagicMock()

    def test_records_namespace_exists(self):
        """Test that records namespace is accessible on the client."""
        self.assertIsNotNone(self.client.records)
        self.assertEqual(self.client.records._client, self.client)

    def test_records_create_single(self):
        """Test records.create() with a single record returns single ID."""
        # Setup mock return values
        mock_metadata = RequestTelemetryData(client_request_id="test-123")
        self.client._odata._create.return_value = ("00000000-0000-0000-0000-000000000000", mock_metadata)
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

        # Execute test
        result = self.client.records.create("account", {"name": "Contoso Ltd"})

        # Verify - single record returns single ID string, not list
        self.client._odata._create.assert_called_once_with("accounts", "account", {"name": "Contoso Ltd"})
        self.assertEqual(result.value, "00000000-0000-0000-0000-000000000000")
        self.assertIsInstance(result.value, str)
        # Verify telemetry access
        response = result.with_response_details()
        self.assertEqual(response.telemetry["client_request_id"], "test-123")

    def test_records_create_multiple(self):
        """Test records.create() with multiple records."""
        payloads = [{"name": "Company A"}, {"name": "Company B"}, {"name": "Company C"}]

        # Setup mock return values
        mock_metadata = RequestTelemetryData(client_request_id="test-456")
        self.client._odata._create_multiple.return_value = (
            [
                "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000002",
                "00000000-0000-0000-0000-000000000003",
            ],
            mock_metadata,
        )
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

        # Execute test
        result = self.client.records.create("account", payloads)

        # Verify
        self.client._odata._create_multiple.assert_called_once_with("accounts", "account", payloads)
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], "00000000-0000-0000-0000-000000000001")

    def test_records_create_invalid_type(self):
        """Test records.create() with invalid data type."""
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

        with self.assertRaises(TypeError):
            self.client.records.create("account", "invalid")

    def test_records_update_single(self):
        """Test records.update() with a single record."""
        mock_metadata = RequestTelemetryData(client_request_id="test-789")
        self.client._odata._update.return_value = (None, mock_metadata)

        result = self.client.records.update(
            "account", "00000000-0000-0000-0000-000000000000", {"telephone1": "555-0199"}
        )

        self.client._odata._update.assert_called_once_with(
            "account", "00000000-0000-0000-0000-000000000000", {"telephone1": "555-0199"}
        )
        self.assertIsNone(result.value)
        response = result.with_response_details()
        self.assertEqual(response.telemetry["client_request_id"], "test-789")

    def test_records_update_multiple(self):
        """Test records.update() with multiple records (broadcast)."""
        ids = [
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ]
        changes = {"statecode": 1}

        mock_metadata = RequestTelemetryData(client_request_id="test-update-multi")
        self.client._odata._update_by_ids.return_value = (None, mock_metadata)

        result = self.client.records.update("account", ids, changes)

        self.client._odata._update_by_ids.assert_called_once_with("account", ids, changes)
        self.assertIsNone(result.value)

    def test_records_update_invalid_single_id_with_list_changes(self):
        """Test records.update() with single id but list changes raises TypeError."""
        with self.assertRaises(TypeError):
            self.client.records.update("account", "some-id", [{"change1": 1}])

    def test_records_delete_single(self):
        """Test records.delete() with a single record."""
        mock_metadata = RequestTelemetryData(client_request_id="test-delete")
        self.client._odata._delete.return_value = (None, mock_metadata)

        result = self.client.records.delete("account", "00000000-0000-0000-0000-000000000000")

        self.client._odata._delete.assert_called_once_with("account", "00000000-0000-0000-0000-000000000000")
        self.assertIsNone(result.value)

    def test_records_delete_multiple_bulk(self):
        """Test records.delete() with multiple records using bulk delete."""
        ids = [
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ]
        mock_metadata = RequestTelemetryData(client_request_id="test-bulk-delete")
        self.client._odata._delete_multiple.return_value = ("job-guid-123", mock_metadata)

        result = self.client.records.delete("account", ids)

        self.client._odata._delete_multiple.assert_called_once_with("account", ids)
        self.assertEqual(result.value, "job-guid-123")

    def test_records_delete_multiple_sequential(self):
        """Test records.delete() with multiple records using sequential delete."""
        ids = [
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ]
        mock_metadata = RequestTelemetryData(client_request_id="test-seq-delete")
        self.client._odata._delete.return_value = (None, mock_metadata)

        result = self.client.records.delete("account", ids, use_bulk_delete=False)

        # Should have called _delete twice (once per id)
        self.assertEqual(self.client._odata._delete.call_count, 2)
        self.assertIsNone(result.value)

    def test_records_delete_empty_list(self):
        """Test records.delete() with empty list returns early."""
        result = self.client.records.delete("account", [])

        self.client._odata._delete.assert_not_called()
        self.client._odata._delete_multiple.assert_not_called()
        self.assertIsNone(result.value)

    def test_records_get_single(self):
        """Test records.get() with a single record ID."""
        expected_record = {"accountid": "00000000-0000-0000-0000-000000000000", "name": "Contoso"}
        mock_metadata = RequestTelemetryData(client_request_id="test-get")
        self.client._odata._get.return_value = (expected_record, mock_metadata)

        result = self.client.records.get("account", "00000000-0000-0000-0000-000000000000")

        self.client._odata._get.assert_called_once_with("account", "00000000-0000-0000-0000-000000000000", select=None)
        self.assertEqual(result["accountid"], "00000000-0000-0000-0000-000000000000")
        self.assertEqual(result["name"], "Contoso")

    def test_records_get_with_select(self):
        """Test records.get() with select parameter."""
        expected_record = {"accountid": "guid", "name": "Contoso"}
        mock_metadata = RequestTelemetryData()
        self.client._odata._get.return_value = (expected_record, mock_metadata)

        result = self.client.records.get("account", "guid", select=["name", "telephone1"])

        self.client._odata._get.assert_called_once_with("account", "guid", select=["name", "telephone1"])
        self.assertEqual(result.value, expected_record)

    def test_records_get_invalid_record_id_type(self):
        """Test records.get() with invalid record_id type raises TypeError."""
        with self.assertRaises(TypeError):
            self.client.records.get("account", 12345)

    def test_records_upsert_not_implemented(self):
        """Test records.upsert() raises NotImplementedError."""
        with self.assertRaises(NotImplementedError):
            self.client.records.upsert("account", {"name": "Test"})


if __name__ == "__main__":
    unittest.main()
