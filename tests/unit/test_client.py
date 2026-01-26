# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.results import RequestMetadata, FluentResult


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

        # Create a sample metadata object for mocking
        self.sample_metadata = RequestMetadata(
            client_request_id="test-client-123",
            correlation_id="test-corr-456",
            http_status_code=200,
            timing_ms=100.0
        )

    def test_create_single(self):
        """Test create method with a single record."""
        # Setup mock return values for _with_metadata variant
        self.client._odata._create_with_metadata.return_value = (
            "00000000-0000-0000-0000-000000000000",
            self.sample_metadata
        )
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

        # Execute test
        result = self.client.create("account", {"name": "Contoso Ltd"})

        # Verify - now uses _create_with_metadata
        self.client._odata._create_with_metadata.assert_called_once_with(
            "accounts", "account", {"name": "Contoso Ltd"}
        )
        # Result should be a FluentResult that behaves like a list
        self.assertIsInstance(result, FluentResult)
        self.assertEqual(result[0], "00000000-0000-0000-0000-000000000000")
        self.assertEqual(len(result), 1)

    def test_create_multiple(self):
        """Test create method with multiple records."""
        payloads = [{"name": "Company A"}, {"name": "Company B"}, {"name": "Company C"}]

        # Setup mock return values for _with_metadata variant
        self.client._odata._create_multiple_with_metadata.return_value = (
            [
                "00000000-0000-0000-0000-000000000001",
                "00000000-0000-0000-0000-000000000002",
                "00000000-0000-0000-0000-000000000003",
            ],
            self.sample_metadata,
            {"total": 3, "success": 3, "failures": 0}
        )
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

        # Execute test
        result = self.client.create("account", payloads)

        # Verify - now uses _create_multiple_with_metadata
        self.client._odata._create_multiple_with_metadata.assert_called_once_with(
            "accounts", "account", payloads
        )
        # Result should be a FluentResult that behaves like a list
        self.assertIsInstance(result, FluentResult)
        self.assertEqual(len(result), 3)

    def test_create_with_detail_response(self):
        """Test that create() supports .with_detail_response() for telemetry."""
        self.client._odata._create_with_metadata.return_value = (
            "00000000-0000-0000-0000-000000000000",
            self.sample_metadata
        )
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

        result = self.client.create("account", {"name": "Test"})
        response = result.with_detail_response()

        # Verify telemetry is available
        self.assertEqual(response.result, ["00000000-0000-0000-0000-000000000000"])
        self.assertEqual(response.telemetry["client_request_id"], "test-client-123")
        self.assertEqual(response.telemetry["timing_ms"], 100.0)

    def test_update_single(self):
        """Test update method with a single record."""
        # Setup mock return value for _with_metadata variant
        self.client._odata._update_with_metadata.return_value = (None, self.sample_metadata)

        result = self.client.update("account", "00000000-0000-0000-0000-000000000000", {"telephone1": "555-0199"})

        self.client._odata._update_with_metadata.assert_called_once_with(
            "account", "00000000-0000-0000-0000-000000000000", {"telephone1": "555-0199"}
        )
        # Result should be a FluentResult wrapping None
        self.assertIsInstance(result, FluentResult)
        self.assertIsNone(result.value)

    def test_update_multiple(self):
        """Test update method with multiple records (broadcast)."""
        ids = [
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ]
        changes = {"statecode": 1}

        result = self.client.update("account", ids, changes)

        # Bulk updates still use _update_by_ids (no _with_metadata variant yet)
        self.client._odata._update_by_ids.assert_called_once_with("account", ids, changes)
        self.assertIsInstance(result, FluentResult)

    def test_delete_single(self):
        """Test delete method with a single record."""
        # Setup mock return value for _with_metadata variant
        self.client._odata._delete_with_metadata.return_value = (None, self.sample_metadata)

        result = self.client.delete("account", "00000000-0000-0000-0000-000000000000")

        self.client._odata._delete_with_metadata.assert_called_once_with(
            "account", "00000000-0000-0000-0000-000000000000"
        )
        # Result should be a FluentResult wrapping None
        self.assertIsInstance(result, FluentResult)

    def test_delete_multiple(self):
        """Test delete method with multiple records."""
        ids = [
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ]
        # Mock return value for bulk delete job ID
        self.client._odata._delete_multiple.return_value = "job-guid-123"

        result = self.client.delete("account", ids)

        self.client._odata._delete_multiple.assert_called_once_with("account", ids)
        # Result should be a FluentResult wrapping the job ID
        self.assertIsInstance(result, FluentResult)
        self.assertEqual(result.value, "job-guid-123")

    def test_get_single(self):
        """Test get method with a single record ID."""
        # Setup mock return value for _with_metadata variant
        expected_record = {"accountid": "00000000-0000-0000-0000-000000000000", "name": "Contoso"}
        self.client._odata._get_with_metadata.return_value = (expected_record, self.sample_metadata)

        result = self.client.get("account", "00000000-0000-0000-0000-000000000000")

        self.client._odata._get_with_metadata.assert_called_once_with(
            "account", "00000000-0000-0000-0000-000000000000", select=None
        )
        # Result should be a FluentResult that behaves like a dict
        self.assertIsInstance(result, FluentResult)
        self.assertEqual(result["name"], "Contoso")

    def test_get_single_with_detail_response(self):
        """Test that get() for single record supports .with_detail_response()."""
        expected_record = {"accountid": "00000000-0000-0000-0000-000000000000", "name": "Contoso"}
        self.client._odata._get_with_metadata.return_value = (expected_record, self.sample_metadata)

        result = self.client.get("account", "00000000-0000-0000-0000-000000000000")
        response = result.with_detail_response()

        # Verify telemetry is available
        self.assertEqual(response.result["name"], "Contoso")
        self.assertEqual(response.telemetry["http_status_code"], 200)

    def test_get_multiple(self):
        """Test get method for querying multiple records."""
        # Setup mock return value (iterator)
        expected_batch = [{"accountid": "1", "name": "A"}, {"accountid": "2", "name": "B"}]
        self.client._odata._get_multiple.return_value = iter([expected_batch])

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
        self.assertEqual(results, [expected_batch])
