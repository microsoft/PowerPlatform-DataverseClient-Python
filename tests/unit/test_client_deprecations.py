# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Tests for deprecated flat methods on DataverseClient.

Each deprecated method on the client should:
1. Emit a DeprecationWarning.
2. Delegate to the correct namespace method (records / query / tables).
3. Return the expected value, including any backward-compatibility shims.
"""

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient


class TestClientDeprecations(unittest.TestCase):
    """Verify every deprecated flat method warns and delegates correctly."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        # Mock the internal OData client so namespace methods resolve without
        # making real HTTP calls.
        self.client._odata = MagicMock()

    # ---------------------------------------------------------------- create

    def test_create_warns(self):
        """client.create() emits a DeprecationWarning."""
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"
        self.client._odata._create.return_value = "guid-123"

        with self.assertWarns(DeprecationWarning):
            self.client.create("account", {"name": "Test"})

    def test_create_single_returns_list(self):
        """client.create() wraps a single GUID in a list for backward compat.

        records.create() returns a bare string for a single dict, but the
        deprecated client.create() always returned list[str].
        """
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"
        self.client._odata._create.return_value = "guid-123"

        with self.assertWarns(DeprecationWarning):
            result = self.client.create("account", {"name": "A"})

        self.assertIsInstance(result, list)
        self.assertEqual(result, ["guid-123"])

    def test_create_bulk_returns_list(self):
        """client.create() with a list payload returns list[str] directly."""
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"
        self.client._odata._create_multiple.return_value = ["guid-1", "guid-2"]

        with self.assertWarns(DeprecationWarning):
            result = self.client.create("account", [{"name": "A"}, {"name": "B"}])

        self.assertIsInstance(result, list)
        self.assertEqual(result, ["guid-1", "guid-2"])

    # ---------------------------------------------------------------- update

    def test_update_warns_and_delegates(self):
        """client.update() emits a DeprecationWarning and delegates to records.update."""
        with self.assertWarns(DeprecationWarning):
            self.client.update(
                "account",
                "00000000-0000-0000-0000-000000000001",
                {"telephone1": "555-0199"},
            )

        self.client._odata._update.assert_called_once_with(
            "account",
            "00000000-0000-0000-0000-000000000001",
            {"telephone1": "555-0199"},
        )

    # ---------------------------------------------------------------- delete

    def test_delete_warns_and_delegates(self):
        """client.delete() emits a DeprecationWarning and delegates to records.delete."""
        with self.assertWarns(DeprecationWarning):
            self.client.delete("account", "00000000-0000-0000-0000-000000000001")

        self.client._odata._delete.assert_called_once_with("account", "00000000-0000-0000-0000-000000000001")

    # ------------------------------------------------------------------- get

    def test_get_single_warns(self):
        """client.get() with record_id emits a DeprecationWarning and delegates
        to records.get.
        """
        expected = {"accountid": "guid-1", "name": "Contoso"}
        self.client._odata._get.return_value = expected

        with self.assertWarns(DeprecationWarning):
            result = self.client.get("account", record_id="guid-1")

        self.client._odata._get.assert_called_once_with("account", "guid-1", select=None)
        self.assertEqual(result, expected)

    def test_get_multiple_warns(self):
        """client.get() without record_id emits a DeprecationWarning and delegates
        to query.get.
        """
        page = [{"accountid": "1", "name": "A"}, {"accountid": "2", "name": "B"}]
        self.client._odata._get_multiple.return_value = iter([page])

        with self.assertWarns(DeprecationWarning):
            result = self.client.get("account", filter="statecode eq 0", top=10)

        # The result is a generator; consume it.
        pages = list(result)
        self.assertEqual(pages, [page])

        self.client._odata._get_multiple.assert_called_once_with(
            "account",
            select=None,
            filter="statecode eq 0",
            orderby=None,
            top=10,
            expand=None,
            page_size=None,
        )

    # ------------------------------------------------------------- query_sql

    def test_query_sql_warns(self):
        """client.query_sql() emits a DeprecationWarning and delegates to
        query.sql.
        """
        expected_rows = [{"name": "Contoso"}, {"name": "Fabrikam"}]
        self.client._odata._query_sql.return_value = expected_rows

        with self.assertWarns(DeprecationWarning):
            result = self.client.query_sql("SELECT name FROM account")

        self.client._odata._query_sql.assert_called_once_with("SELECT name FROM account")
        self.assertEqual(result, expected_rows)

    # -------------------------------------------------------- get_table_info

    def test_get_table_info_warns(self):
        """client.get_table_info() emits a DeprecationWarning and delegates to
        tables.get.
        """
        expected_info = {
            "table_schema_name": "new_MyTable",
            "table_logical_name": "new_mytable",
            "entity_set_name": "new_mytables",
            "metadata_id": "meta-guid",
        }
        self.client._odata._get_table_info.return_value = expected_info

        with self.assertWarns(DeprecationWarning):
            result = self.client.get_table_info("new_MyTable")

        self.client._odata._get_table_info.assert_called_once_with("new_MyTable")
        self.assertEqual(result, expected_info)

    # --------------------------------------------------------- create_table

    def test_create_table_warns(self):
        """client.create_table() emits a DeprecationWarning and maps legacy
        parameter names (solution_unique_name -> solution,
        primary_column_schema_name -> primary_column) when delegating to
        tables.create.
        """
        expected = {
            "table_schema_name": "new_Product",
            "entity_set_name": "new_products",
            "table_logical_name": "new_product",
            "metadata_id": "meta-guid",
            "columns_created": ["new_Price"],
        }
        self.client._odata._create_table.return_value = expected

        with self.assertWarns(DeprecationWarning):
            result = self.client.create_table(
                "new_Product",
                {"new_Price": "decimal"},
                solution_unique_name="MySolution",
                primary_column_schema_name="new_ProductName",
            )

        # Verify that the internal _create_table received the mapped params.
        self.client._odata._create_table.assert_called_once_with(
            "new_Product",
            {"new_Price": "decimal"},
            "MySolution",
            "new_ProductName",
        )
        self.assertEqual(result, expected)

    # --------------------------------------------------------- delete_table

    def test_delete_table_warns(self):
        """client.delete_table() emits a DeprecationWarning and delegates to
        tables.delete.
        """
        with self.assertWarns(DeprecationWarning):
            self.client.delete_table("new_MyTestTable")

        self.client._odata._delete_table.assert_called_once_with("new_MyTestTable")

    # ---------------------------------------------------------- list_tables

    def test_list_tables_warns(self):
        """client.list_tables() emits a DeprecationWarning and delegates to
        tables.list.
        """
        expected = [{"LogicalName": "account"}, {"LogicalName": "contact"}]
        self.client._odata._list_tables.return_value = expected

        with self.assertWarns(DeprecationWarning):
            result = self.client.list_tables()

        self.client._odata._list_tables.assert_called_once()
        self.assertEqual(result, expected)

    # ------------------------------------------------------- create_columns

    def test_create_columns_warns(self):
        """client.create_columns() emits a DeprecationWarning and delegates to
        tables.add_columns.
        """
        self.client._odata._create_columns.return_value = ["new_Notes", "new_Active"]

        with self.assertWarns(DeprecationWarning):
            result = self.client.create_columns(
                "new_MyTestTable",
                {"new_Notes": "string", "new_Active": "bool"},
            )

        self.client._odata._create_columns.assert_called_once_with(
            "new_MyTestTable",
            {"new_Notes": "string", "new_Active": "bool"},
        )
        self.assertEqual(result, ["new_Notes", "new_Active"])

    # ------------------------------------------------------- delete_columns

    def test_delete_columns_warns(self):
        """client.delete_columns() emits a DeprecationWarning and delegates to
        tables.remove_columns.
        """
        self.client._odata._delete_columns.return_value = ["new_Notes", "new_Active"]

        with self.assertWarns(DeprecationWarning):
            result = self.client.delete_columns(
                "new_MyTestTable",
                ["new_Notes", "new_Active"],
            )

        self.client._odata._delete_columns.assert_called_once_with(
            "new_MyTestTable",
            ["new_Notes", "new_Active"],
        )
        self.assertEqual(result, ["new_Notes", "new_Active"])


if __name__ == "__main__":
    unittest.main()
