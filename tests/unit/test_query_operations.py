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

    # ----------------------------------------------------------------- builder

    def test_builder_returns_query_builder(self):
        """builder() should return a QueryBuilder with _query_ops set."""
        from PowerPlatform.Dataverse.models.query_builder import QueryBuilder

        qb = self.client.query.builder("account")

        self.assertIsInstance(qb, QueryBuilder)
        self.assertEqual(qb.table, "account")
        self.assertIs(qb._query_ops, self.client.query)

    def test_builder_execute_flat_default(self):
        """builder().execute() should return flat records by default."""
        self.client._odata._get_multiple.return_value = iter([[{"accountid": "1", "name": "Test"}]])

        records = list(self.client.query.builder("account").select("name").filter_eq("statecode", 0).top(10).execute())

        self.client._odata._get_multiple.assert_called_once_with(
            "account",
            select=["name"],
            filter="statecode eq 0",
            orderby=None,
            top=10,
            expand=None,
            page_size=None,
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["name"], "Test")

    def test_builder_execute_flat_multiple_pages(self):
        """execute() should flatten records from multiple pages."""
        self.client._odata._get_multiple.return_value = iter([[{"accountid": "1"}], [{"accountid": "2"}]])

        records = list(self.client.query.builder("account").execute())

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["accountid"], "1")
        self.assertEqual(records[1]["accountid"], "2")

    def test_builder_execute_by_page(self):
        """execute(by_page=True) should yield pages."""
        self.client._odata._get_multiple.return_value = iter([[{"accountid": "1"}], [{"accountid": "2"}]])

        pages = list(self.client.query.builder("account").execute(by_page=True))

        self.assertEqual(len(pages), 2)
        self.assertEqual(len(pages[0]), 1)
        self.assertEqual(pages[0][0]["accountid"], "1")
        self.assertEqual(pages[1][0]["accountid"], "2")

    def test_builder_execute_all_params(self):
        """builder().execute() should forward all parameters."""
        self.client._odata._get_multiple.return_value = iter([[{"name": "Test"}]])

        list(
            self.client.query.builder("account")
            .select("name", "revenue")
            .filter_eq("statecode", 0)
            .filter_gt("revenue", 1000000)
            .order_by("revenue", descending=True)
            .expand("primarycontactid")
            .top(50)
            .page_size(25)
            .execute()
        )

        self.client._odata._get_multiple.assert_called_once_with(
            "account",
            select=["name", "revenue"],
            filter="statecode eq 0 and revenue gt 1000000",
            orderby=["revenue desc"],
            top=50,
            expand=["primarycontactid"],
            page_size=25,
        )

    def test_builder_execute_with_where(self):
        """builder().where().execute() should compile expression to filter."""
        from PowerPlatform.Dataverse.models.filters import eq, gt

        self.client._odata._get_multiple.return_value = iter([[{"name": "Test"}]])

        list(
            self.client.query.builder("account")
            .where((eq("statecode", 0) | eq("statecode", 1)) & gt("revenue", 100000))
            .execute()
        )

        call_kwargs = self.client._odata._get_multiple.call_args
        self.assertEqual(
            call_kwargs.kwargs["filter"],
            "((statecode eq 0 or statecode eq 1) and revenue gt 100000)",
        )

    def test_builder_full_fluent_workflow(self):
        """End-to-end test of the fluent query workflow."""
        expected_records = [
            {"accountid": "1", "name": "Big Corp", "revenue": 5000000},
            {"accountid": "2", "name": "Mega Inc", "revenue": 4000000},
        ]
        self.client._odata._get_multiple.return_value = iter([expected_records])

        records = list(
            self.client.query.builder("account")
            .select("name", "revenue")
            .filter_eq("statecode", 0)
            .filter_gt("revenue", 1000000)
            .order_by("revenue", descending=True)
            .expand("primarycontactid")
            .top(10)
            .page_size(5)
            .execute()
        )

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["name"], "Big Corp")
        self.assertEqual(records[1]["name"], "Mega Inc")


if __name__ == "__main__":
    unittest.main()
