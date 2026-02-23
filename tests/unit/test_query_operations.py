# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.models.record import Record
from PowerPlatform.Dataverse.operations.query import QueryOperations


class TestQueryOperations(unittest.TestCase):
    """Unit tests for the client.query namespace (QueryOperations)."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()

    # ---------------------------------------------------------------- namespace

    def test_namespace_exists(self):
        """The client.query attribute should be a QueryOperations instance."""
        self.assertIsInstance(self.client.query, QueryOperations)

    # -------------------------------------------------------------------- sql

    def test_sql(self):
        """sql() should return Record objects with dict-like access."""
        raw_rows = [
            {"accountid": "1", "name": "Contoso"},
            {"accountid": "2", "name": "Fabrikam"},
        ]
        self.client._odata._query_sql.return_value = raw_rows

        result = self.client.query.sql("SELECT accountid, name FROM account")

        self.client._odata._query_sql.assert_called_once_with("SELECT accountid, name FROM account")
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], Record)
        self.assertEqual(result[0]["name"], "Contoso")
        self.assertEqual(result[1]["name"], "Fabrikam")

    def test_sql_empty_result(self):
        """sql() should return an empty list when _query_sql returns no rows."""
        self.client._odata._query_sql.return_value = []

        result = self.client.query.sql("SELECT name FROM account WHERE name = 'NonExistent'")

        self.client._odata._query_sql.assert_called_once_with("SELECT name FROM account WHERE name = 'NonExistent'")
        self.assertIsInstance(result, list)
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
