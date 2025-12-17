# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient


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
        # _create must return a GUID string
        self.client._odata._create.return_value = "00000000-0000-0000-0000-000000000000"
        # _entity_set_from_schema_name should return the plural entity set name
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

        # Execute test
        self.client.create("account", {"name": "Contoso Ltd"})

        # Verify
        # Ensure _entity_set_from_schema_name was called and its result ("accounts") was passed to _create
        self.client._odata._create.assert_called_once_with("accounts", "account", {"name": "Contoso Ltd"})

    def test_create_multiple(self):
        """Test create method with multiple records."""
        payloads = [{"name": "Company A"}, {"name": "Company B"}, {"name": "Company C"}]

        # Setup mock return values
        # _create_multiple must return a list of GUID strings
        self.client._odata._create_multiple.return_value = [
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
            "00000000-0000-0000-0000-000000000003",
        ]
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

        # Execute test
        self.client.create("account", payloads)

        # Verify
        self.client._odata._create_multiple.assert_called_once_with("accounts", "account", payloads)

    def test_update_single(self):
        """Test update method with a single record."""
        self.client.update("account", "00000000-0000-0000-0000-000000000000", {"telephone1": "555-0199"})
        self.client._odata._update.assert_called_once_with(
            "account", "00000000-0000-0000-0000-000000000000", {"telephone1": "555-0199"}
        )

    def test_update_multiple(self):
        """Test update method with multiple records (broadcast)."""
        ids = [
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ]
        changes = {"statecode": 1}

        self.client.update("account", ids, changes)
        self.client._odata._update_by_ids.assert_called_once_with("account", ids, changes)

    def test_delete_single(self):
        """Test delete method with a single record."""
        self.client.delete("account", "00000000-0000-0000-0000-000000000000")
        self.client._odata._delete.assert_called_once_with("account", "00000000-0000-0000-0000-000000000000")

    def test_delete_multiple(self):
        """Test delete method with multiple records."""
        ids = [
            "00000000-0000-0000-0000-000000000001",
            "00000000-0000-0000-0000-000000000002",
        ]
        # Mock return value for bulk delete job ID
        self.client._odata._delete_multiple.return_value = "job-guid-123"

        job_id = self.client.delete("account", ids)

        self.client._odata._delete_multiple.assert_called_once_with("account", ids)
        self.assertEqual(job_id, "job-guid-123")

    def test_get_single(self):
        """Test get method with a single record ID."""
        # Setup mock return value
        expected_record = {"accountid": "00000000-0000-0000-0000-000000000000", "name": "Contoso"}
        self.client._odata._get.return_value = expected_record

        result = self.client.get("account", "00000000-0000-0000-0000-000000000000")

        self.client._odata._get.assert_called_once_with("account", "00000000-0000-0000-0000-000000000000", select=None)
        self.assertEqual(result, expected_record)

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
