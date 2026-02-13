# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
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

    # ----------------------------------------------------------- get (paginated)

    def test_get_paginated(self):
        """get() should yield pages from the underlying _get_multiple generator."""
        page_1 = [{"accountid": "1", "name": "A"}]
        page_2 = [{"accountid": "2", "name": "B"}]
        self.client._odata._get_multiple.return_value = iter([page_1, page_2])

        pages = list(self.client.query.get("account"))

        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0], page_1)
        self.assertEqual(pages[1], page_2)

    def test_get_with_all_params(self):
        """get() should pass all keyword arguments through to _get_multiple."""
        self.client._odata._get_multiple.return_value = iter([])

        # Consume the generator so the call actually happens
        list(
            self.client.query.get(
                "account",
                select=["name", "telephone1"],
                filter="statecode eq 0",
                orderby=["name asc", "createdon desc"],
                top=50,
                expand=["primarycontactid"],
                page_size=25,
            )
        )

        self.client._odata._get_multiple.assert_called_once_with(
            "account",
            select=["name", "telephone1"],
            filter="statecode eq 0",
            orderby=["name asc", "createdon desc"],
            top=50,
            expand=["primarycontactid"],
            page_size=25,
        )

    # -------------------------------------------------------------------- sql

    def test_sql(self):
        """sql() should call _query_sql and return the result list."""
        expected_rows = [
            {"accountid": "1", "name": "Contoso"},
            {"accountid": "2", "name": "Fabrikam"},
        ]
        self.client._odata._query_sql.return_value = expected_rows

        result = self.client.query.sql("SELECT accountid, name FROM account")

        self.client._odata._query_sql.assert_called_once_with("SELECT accountid, name FROM account")
        self.assertIsInstance(result, list)
        self.assertEqual(result, expected_rows)

    def test_sql_empty_result(self):
        """sql() should return an empty list when _query_sql returns no rows."""
        self.client._odata._query_sql.return_value = []

        result = self.client.query.sql("SELECT name FROM account WHERE name = 'NonExistent'")

        self.client._odata._query_sql.assert_called_once_with("SELECT name FROM account WHERE name = 'NonExistent'")
        self.assertIsInstance(result, list)
        self.assertEqual(result, [])


if __name__ == "__main__":
    unittest.main()
