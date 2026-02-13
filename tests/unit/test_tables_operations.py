# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.operations.tables import TableOperations


class TestTableOperations(unittest.TestCase):
    """Unit tests for the client.tables namespace (TableOperations)."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()

    # ---------------------------------------------------------------- namespace

    def test_namespace_exists(self):
        """The client.tables attribute should be a TableOperations instance."""
        self.assertIsInstance(self.client.tables, TableOperations)

    # ------------------------------------------------------------------ create

    def test_create(self):
        """create() should call _create_table with correct positional args including renamed kwargs."""
        expected_result = {
            "table_schema_name": "new_Product",
            "entity_set_name": "new_products",
            "table_logical_name": "new_product",
            "metadata_id": "meta-guid-1",
            "columns_created": ["new_Price", "new_InStock"],
        }
        self.client._odata._create_table.return_value = expected_result

        columns = {"new_Price": "decimal", "new_InStock": "bool"}
        result = self.client.tables.create(
            "new_Product",
            columns,
            solution="MySolution",
            primary_column="new_ProductName",
        )

        self.client._odata._create_table.assert_called_once_with(
            "new_Product",
            columns,
            "MySolution",
            "new_ProductName",
        )
        self.assertEqual(result, expected_result)

    # ------------------------------------------------------------------ delete

    def test_delete(self):
        """delete() should call _delete_table with the table schema name."""
        self.client.tables.delete("new_Product")

        self.client._odata._delete_table.assert_called_once_with("new_Product")

    # --------------------------------------------------------------------- get

    def test_get(self):
        """get() should call _get_table_info and return the metadata dict."""
        expected_info = {
            "table_schema_name": "new_Product",
            "table_logical_name": "new_product",
            "entity_set_name": "new_products",
            "metadata_id": "meta-guid-1",
        }
        self.client._odata._get_table_info.return_value = expected_info

        result = self.client.tables.get("new_Product")

        self.client._odata._get_table_info.assert_called_once_with("new_Product")
        self.assertEqual(result, expected_info)

    def test_get_returns_none(self):
        """get() should return None when _get_table_info returns None (table not found)."""
        self.client._odata._get_table_info.return_value = None

        result = self.client.tables.get("nonexistent_Table")

        self.client._odata._get_table_info.assert_called_once_with("nonexistent_Table")
        self.assertIsNone(result)

    # ------------------------------------------------------------------- list

    def test_list(self):
        """list() should call _list_tables and return the list of metadata dicts."""
        expected_tables = [
            {"LogicalName": "account", "SchemaName": "Account"},
            {"LogicalName": "contact", "SchemaName": "Contact"},
        ]
        self.client._odata._list_tables.return_value = expected_tables

        result = self.client.tables.list()

        self.client._odata._list_tables.assert_called_once()
        self.assertIsInstance(result, list)
        self.assertEqual(result, expected_tables)

    # ------------------------------------------------------------ add_columns

    def test_add_columns(self):
        """add_columns() should call _create_columns with correct args."""
        self.client._odata._create_columns.return_value = ["new_Notes", "new_Active"]

        columns = {"new_Notes": "string", "new_Active": "bool"}
        result = self.client.tables.add_columns("new_Product", columns)

        self.client._odata._create_columns.assert_called_once_with("new_Product", columns)
        self.assertEqual(result, ["new_Notes", "new_Active"])

    # --------------------------------------------------------- remove_columns

    def test_remove_columns_single(self):
        """remove_columns() with a single string should pass it through to _delete_columns."""
        self.client._odata._delete_columns.return_value = ["new_Notes"]

        result = self.client.tables.remove_columns("new_Product", "new_Notes")

        self.client._odata._delete_columns.assert_called_once_with("new_Product", "new_Notes")
        self.assertEqual(result, ["new_Notes"])

    def test_remove_columns_list(self):
        """remove_columns() with a list of strings should pass it through to _delete_columns."""
        self.client._odata._delete_columns.return_value = ["new_Notes", "new_Active"]

        result = self.client.tables.remove_columns("new_Product", ["new_Notes", "new_Active"])

        self.client._odata._delete_columns.assert_called_once_with("new_Product", ["new_Notes", "new_Active"])
        self.assertEqual(result, ["new_Notes", "new_Active"])


if __name__ == "__main__":
    unittest.main()
