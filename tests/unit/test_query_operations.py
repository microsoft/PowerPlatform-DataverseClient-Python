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

    # -------------------------------------------------------------- fetchxml

    def test_fetchxml_basic(self):
        """fetchxml() should call _query_fetchxml and return results."""
        expected_pages = [
            [{"accountid": "1", "name": "Contoso"}, {"accountid": "2", "name": "Fabrikam"}],
        ]
        self.client._odata._query_fetchxml.return_value = iter(expected_pages)

        fetchxml = "<fetch><entity name='account'><attribute name='name' /></entity></fetch>"
        result = list(self.client.query.fetchxml(fetchxml))

        self.client._odata._query_fetchxml.assert_called_once_with(fetchxml, page_size=None)
        self.assertEqual(result, expected_pages)

    def test_fetchxml_with_page_size(self):
        """fetchxml() should pass page_size through to _query_fetchxml."""
        self.client._odata._query_fetchxml.return_value = iter([])

        fetchxml = "<fetch><entity name='account' /></fetch>"
        list(self.client.query.fetchxml(fetchxml, page_size=50))

        self.client._odata._query_fetchxml.assert_called_once_with(fetchxml, page_size=50)

    def test_fetchxml_empty_result(self):
        """fetchxml() should return empty generator when no results."""
        self.client._odata._query_fetchxml.return_value = iter([])

        fetchxml = "<fetch><entity name='account' /></fetch>"
        result = list(self.client.query.fetchxml(fetchxml))

        self.assertEqual(result, [])

    def test_fetchxml_returns_iterable(self):
        """fetchxml() should return an iterable (generator)."""
        self.client._odata._query_fetchxml.return_value = iter([[{"name": "A"}]])

        fetchxml = "<fetch><entity name='account' /></fetch>"
        result = self.client.query.fetchxml(fetchxml)

        self.assertIsNotNone(iter(result))
        self.assertEqual(list(result), [[{"name": "A"}]])


if __name__ == "__main__":
    unittest.main()
