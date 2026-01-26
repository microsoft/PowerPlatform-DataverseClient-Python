# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.results import RequestTelemetryData


class TestDataverseClient(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create mock credential
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"

        # Initialize the client under test
        self.client = DataverseClient(self.base_url, self.mock_credential)

        # Mock the internal _odata client
        # This ensures we verify logic without making actual HTTP calls
        self.client._odata = MagicMock()

    def test_create_single(self):
        """Test create method with a single record."""
        # Setup mock return values
        # _create must return a (GUID, RequestTelemetryData) tuple
        mock_metadata = RequestTelemetryData(client_request_id="test-123")
        self.client._odata._create.return_value = ("00000000-0000-0000-0000-000000000000", mock_metadata)
        # _entity_set_from_schema_name should return the plural entity set name
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

        # Execute test
        result = self.client.create("account", {"name": "Contoso Ltd"})

        # Verify
        # Ensure _entity_set_from_schema_name was called and its result ("accounts") was passed to _create
        self.client._odata._create.assert_called_once_with("accounts", "account", {"name": "Contoso Ltd"})
        # Result should be OperationResult that acts like a list
        self.assertEqual(result[0], "00000000-0000-0000-0000-000000000000")
        # Can also access telemetry via with_response_details()
        response = result.with_response_details()
        self.assertEqual(response.telemetry["client_request_id"], "test-123")

    def test_create_multiple(self):
        """Test create method with multiple records."""
        payloads = [{"name": "Company A"}, {"name": "Company B"}, {"name": "Company C"}]

        # Setup mock return values
        # _create_multiple must return a (list of GUID strings, RequestTelemetryData) tuple
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
        result = self.client.create("account", payloads)

        # Verify
        self.client._odata._create_multiple.assert_called_once_with("accounts", "account", payloads)
        # Result should be OperationResult that acts like a list
        self.assertEqual(len(result), 3)
        self.assertEqual(result[0], "00000000-0000-0000-0000-000000000001")
        # Can iterate
        ids = list(result)
        self.assertEqual(len(ids), 3)

    def test_update_single(self):
        """Test update method with a single record."""
        # _update returns (None, RequestTelemetryData) tuple
        mock_metadata = RequestTelemetryData(client_request_id="test-789")
        self.client._odata._update.return_value = (None, mock_metadata)

        result = self.client.update("account", "00000000-0000-0000-0000-000000000000", {"telephone1": "555-0199"})

        self.client._odata._update.assert_called_once_with(
            "account", "00000000-0000-0000-0000-000000000000", {"telephone1": "555-0199"}
        )
        # Result is OperationResult with None value
        self.assertIsNone(result.value)
        # Can access telemetry
        response = result.with_response_details()
        self.assertEqual(response.telemetry["client_request_id"], "test-789")

    def test_update_multiple(self):
        """Test update method with multiple records (broadcast)."""
        ids = [
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ]
        changes = {"statecode": 1}

        # _update_by_ids returns (None, RequestTelemetryData) tuple
        mock_metadata = RequestTelemetryData(client_request_id="test-update-multi")
        self.client._odata._update_by_ids.return_value = (None, mock_metadata)

        result = self.client.update("account", ids, changes)

        self.client._odata._update_by_ids.assert_called_once_with("account", ids, changes)
        self.assertIsNone(result.value)

    def test_delete_single(self):
        """Test delete method with a single record."""
        # _delete returns (None, RequestTelemetryData) tuple
        mock_metadata = RequestTelemetryData(client_request_id="test-delete")
        self.client._odata._delete.return_value = (None, mock_metadata)

        result = self.client.delete("account", "00000000-0000-0000-0000-000000000000")

        self.client._odata._delete.assert_called_once_with("account", "00000000-0000-0000-0000-000000000000")
        self.assertIsNone(result.value)

    def test_delete_multiple(self):
        """Test delete method with multiple records."""
        ids = [
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ]
        # Mock return value for bulk delete: (job_id, RequestTelemetryData) tuple
        mock_metadata = RequestTelemetryData(client_request_id="test-bulk-delete")
        self.client._odata._delete_multiple.return_value = ("job-guid-123", mock_metadata)

        result = self.client.delete("account", ids)

        self.client._odata._delete_multiple.assert_called_once_with("account", ids)
        # Result is OperationResult containing the job ID
        self.assertEqual(result.value, "job-guid-123")
        # Can compare directly with raw value
        self.assertEqual(result, "job-guid-123")

    def test_get_single(self):
        """Test get method with a single record ID."""
        # Setup mock return value: (record, RequestTelemetryData) tuple
        expected_record = {"accountid": "00000000-0000-0000-0000-000000000000", "name": "Contoso"}
        mock_metadata = RequestTelemetryData(client_request_id="test-get")
        self.client._odata._get.return_value = (expected_record, mock_metadata)

        result = self.client.get("account", "00000000-0000-0000-0000-000000000000")

        self.client._odata._get.assert_called_once_with("account", "00000000-0000-0000-0000-000000000000", select=None)
        # Result is OperationResult that supports dict-like access
        self.assertEqual(result["accountid"], "00000000-0000-0000-0000-000000000000")
        self.assertEqual(result["name"], "Contoso")
        # Can also access the full value
        self.assertEqual(result.value, expected_record)
        # Can access telemetry
        response = result.with_response_details()
        self.assertEqual(response.telemetry["client_request_id"], "test-get")

    def test_get_multiple(self):
        """Test get method for querying multiple records."""
        # Setup mock return value (iterator of (batch, metadata) tuples)
        expected_batch = [{"accountid": "1", "name": "A"}, {"accountid": "2", "name": "B"}]
        mock_metadata = RequestTelemetryData(client_request_id="test-page-1")
        self.client._odata._get_multiple.return_value = iter([(expected_batch, mock_metadata)])

        # Execute query
        result_iterator = self.client.get("account", filter="statecode eq 0", top=10)

        # Consume iterator to verify content
        results = list(result_iterator)

        self.client._odata._get_multiple.assert_called_once_with(
            "account",
            select=None,
            filter="statecode eq 0",
            orderby=None,
            top=10,
            expand=None,
            page_size=None,
        )
        # Each batch is now wrapped in OperationResult
        self.assertEqual(len(results), 1)
        # Can iterate/index the batch directly (OperationResult delegates)
        self.assertEqual(results[0][0], {"accountid": "1", "name": "A"})
        self.assertEqual(list(results[0]), expected_batch)
        # Can access telemetry via with_response_details()
        response = results[0].with_response_details()
        self.assertEqual(response.telemetry["client_request_id"], "test-page-1")

    def test_get_multiple_pagination_with_telemetry(self):
        """Test get method returns per-page telemetry for paginated results."""
        # Setup mock with multiple pages
        batch1 = [{"accountid": "1"}, {"accountid": "2"}]
        batch2 = [{"accountid": "3"}, {"accountid": "4"}]
        metadata1 = RequestTelemetryData(client_request_id="page-1", service_request_id="svc-1")
        metadata2 = RequestTelemetryData(client_request_id="page-2", service_request_id="svc-2")
        self.client._odata._get_multiple.return_value = iter(
            [
                (batch1, metadata1),
                (batch2, metadata2),
            ]
        )

        # Execute query
        results = list(self.client.get("account"))

        # Verify we got two pages
        self.assertEqual(len(results), 2)

        # First page telemetry
        response1 = results[0].with_response_details()
        self.assertEqual(response1.result, batch1)
        self.assertEqual(response1.telemetry["client_request_id"], "page-1")
        self.assertEqual(response1.telemetry["service_request_id"], "svc-1")

        # Second page telemetry
        response2 = results[1].with_response_details()
        self.assertEqual(response2.result, batch2)
        self.assertEqual(response2.telemetry["client_request_id"], "page-2")
        self.assertEqual(response2.telemetry["service_request_id"], "svc-2")

    def test_get_multiple_batch_concatenation(self):
        """Test that batches can be concatenated with + operator."""
        # Setup mock with multiple pages
        batch1 = [{"id": "1"}, {"id": "2"}]
        batch2 = [{"id": "3"}, {"id": "4"}]
        metadata = RequestTelemetryData()
        self.client._odata._get_multiple.return_value = iter(
            [
                (batch1, metadata),
                (batch2, metadata),
            ]
        )

        # Execute query and concatenate batches
        batches = list(self.client.get("account"))
        all_records = batches[0] + batches[1]

        # Verify concatenation works
        self.assertEqual(len(all_records), 4)
        self.assertEqual(all_records[0]["id"], "1")
        self.assertEqual(all_records[3]["id"], "4")
        # Result is raw list, not OperationResult
        self.assertIsInstance(all_records, list)
