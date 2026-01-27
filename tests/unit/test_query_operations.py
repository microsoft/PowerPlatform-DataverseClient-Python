# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for QueryOperations namespace class."""

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.results import RequestTelemetryData
from PowerPlatform.Dataverse.models.record import Record


class TestQueryOperations(unittest.TestCase):
    """Test cases for the QueryOperations namespace class."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create mock credential
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"

        # Initialize the client under test
        self.client = DataverseClient(self.base_url, self.mock_credential)

        # Mock the internal _odata client
        self.client._odata = MagicMock()

    def test_query_namespace_exists(self):
        """Test that query namespace is accessible on the client."""
        self.assertIsNotNone(self.client.query)
        self.assertEqual(self.client.query._client, self.client)

    def test_query_get_basic(self):
        """Test query.get() with basic parameters."""
        expected_batch = [{"accountid": "1", "name": "A"}, {"accountid": "2", "name": "B"}]
        mock_metadata = RequestTelemetryData(client_request_id="test-page-1")
        self.client._odata._get_multiple.return_value = iter([(expected_batch, mock_metadata)])

        # Execute query
        result_iterator = self.client.query.get("account", filter="statecode eq 0", top=10)

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
        self.assertEqual(len(results), 1)
        # Results are now Record objects with dict-like access
        self.assertIsInstance(results[0][0], Record)
        self.assertEqual(results[0][0]["accountid"], "1")
        self.assertEqual(results[0][0]["name"], "A")

    def test_query_get_with_all_parameters(self):
        """Test query.get() with all parameters."""
        expected_batch = [{"name": "Test"}]
        mock_metadata = RequestTelemetryData()
        self.client._odata._get_multiple.return_value = iter([(expected_batch, mock_metadata)])

        result_iterator = self.client.query.get(
            "account",
            select=["name", "telephone1"],
            filter="statecode eq 0",
            orderby=["name asc"],
            top=100,
            expand=["primarycontactid"],
            page_size=50,
        )

        results = list(result_iterator)

        self.client._odata._get_multiple.assert_called_once_with(
            "account",
            select=["name", "telephone1"],
            filter="statecode eq 0",
            orderby=["name asc"],
            top=100,
            expand=["primarycontactid"],
            page_size=50,
        )
        self.assertEqual(len(results), 1)

    def test_query_get_pagination_with_telemetry(self):
        """Test query.get() returns per-page telemetry for paginated results."""
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

        results = list(self.client.query.get("account"))

        # Verify we got two pages
        self.assertEqual(len(results), 2)

        # First page telemetry - results are now Record objects
        response1 = results[0].with_response_details()
        self.assertEqual(len(response1.result), 2)
        self.assertIsInstance(response1.result[0], Record)
        self.assertEqual(response1.result[0]["accountid"], "1")
        self.assertEqual(response1.telemetry["client_request_id"], "page-1")
        self.assertEqual(response1.telemetry["service_request_id"], "svc-1")

        # Second page telemetry
        response2 = results[1].with_response_details()
        self.assertEqual(len(response2.result), 2)
        self.assertEqual(response2.result[0]["accountid"], "3")
        self.assertEqual(response2.telemetry["client_request_id"], "page-2")
        self.assertEqual(response2.telemetry["service_request_id"], "svc-2")

    def test_query_get_batch_iteration(self):
        """Test that query.get() batches support iteration."""
        expected_batch = [{"id": "1"}, {"id": "2"}]
        mock_metadata = RequestTelemetryData()
        self.client._odata._get_multiple.return_value = iter([(expected_batch, mock_metadata)])

        results = list(self.client.query.get("account"))

        # The batch should support iteration - items are now Record objects
        batch_items = list(results[0])
        self.assertEqual(len(batch_items), 2)
        self.assertIsInstance(batch_items[0], Record)
        self.assertEqual(batch_items[0]["id"], "1")
        self.assertEqual(batch_items[1]["id"], "2")

    def test_query_get_batch_indexing(self):
        """Test that query.get() batches support indexing."""
        expected_batch = [{"id": "1"}, {"id": "2"}]
        mock_metadata = RequestTelemetryData()
        self.client._odata._get_multiple.return_value = iter([(expected_batch, mock_metadata)])

        results = list(self.client.query.get("account"))

        # The batch should support indexing - items are now Record objects
        self.assertIsInstance(results[0][0], Record)
        self.assertEqual(results[0][0]["id"], "1")
        self.assertEqual(results[0][1]["id"], "2")

    def test_query_get_batch_concatenation(self):
        """Test that query.get() batches can be concatenated with + operator."""
        batch1 = [{"id": "1"}, {"id": "2"}]
        batch2 = [{"id": "3"}, {"id": "4"}]
        metadata = RequestTelemetryData()
        self.client._odata._get_multiple.return_value = iter(
            [
                (batch1, metadata),
                (batch2, metadata),
            ]
        )

        batches = list(self.client.query.get("account"))
        all_records = batches[0] + batches[1]

        self.assertEqual(len(all_records), 4)
        # Records support dict-like access
        self.assertEqual(all_records[0]["id"], "1")
        self.assertEqual(all_records[3]["id"], "4")

    def test_query_sql_basic(self):
        """Test query.sql() with basic SQL query."""
        expected_results = [{"name": "Contoso", "revenue": 1000000}]
        mock_metadata = RequestTelemetryData(client_request_id="test-sql")
        self.client._odata._query_sql.return_value = (expected_results, mock_metadata)

        sql = "SELECT TOP 10 name, revenue FROM account ORDER BY revenue DESC"
        result = self.client.query.sql(sql)

        self.client._odata._query_sql.assert_called_once_with(sql)
        # Results are now Record objects
        self.assertEqual(len(result.value), 1)
        self.assertIsInstance(result.value[0], Record)
        self.assertEqual(result[0]["name"], "Contoso")

    def test_query_sql_with_telemetry(self):
        """Test query.sql() with telemetry access."""
        expected_results = [{"name": "Test"}]
        mock_metadata = RequestTelemetryData(
            client_request_id="sql-123",
            service_request_id="svc-456",
        )
        self.client._odata._query_sql.return_value = (expected_results, mock_metadata)

        result = self.client.query.sql("SELECT name FROM account")

        response = result.with_response_details()
        # Results are now Record objects
        self.assertEqual(len(response.result), 1)
        self.assertEqual(response.result[0]["name"], "Test")
        self.assertEqual(response.telemetry["client_request_id"], "sql-123")
        self.assertEqual(response.telemetry["service_request_id"], "svc-456")

    def test_query_sql_empty_results(self):
        """Test query.sql() with no matching rows."""
        mock_metadata = RequestTelemetryData()
        self.client._odata._query_sql.return_value = ([], mock_metadata)

        result = self.client.query.sql("SELECT name FROM account WHERE name = 'NonExistent'")

        self.assertEqual(result.value, [])
        self.assertEqual(len(result), 0)

    def test_query_sql_iteration(self):
        """Test query.sql() results support iteration."""
        expected_results = [{"name": "A"}, {"name": "B"}, {"name": "C"}]
        mock_metadata = RequestTelemetryData()
        self.client._odata._query_sql.return_value = (expected_results, mock_metadata)

        result = self.client.query.sql("SELECT name FROM account")

        names = [row["name"] for row in result]
        self.assertEqual(names, ["A", "B", "C"])


if __name__ == "__main__":
    unittest.main()
