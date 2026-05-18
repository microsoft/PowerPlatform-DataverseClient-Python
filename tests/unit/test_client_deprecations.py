# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Tests confirming all 12 deprecated flat methods were removed from DataverseClient.

These methods previously delegated to namespace equivalents with a DeprecationWarning.
In 1.0 GA they are fully removed; each call now raises AttributeError.
Callers must use the operation namespaces directly (records.*, query.*, tables.*, files.*).
"""

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient


class TestClientDeprecations(unittest.TestCase):
    """All formerly-deprecated flat methods are now removed and raise AttributeError."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)

    # ---------------------------------------------------------------- records

    def test_create_removed(self):
        """client.create() → use client.records.create()"""
        with self.assertRaises(AttributeError):
            self.client.create("account", {"name": "Test"})

    def test_create_single_returns_list(self):
        """client.create() single-dict shim is gone; client.records.create() returns str."""
        with self.assertRaises(AttributeError):
            self.client.create("account", {"name": "A"})

    def test_create_bulk_returns_list(self):
        """client.create() list-payload shim is gone; client.records.create() returns list[str]."""
        with self.assertRaises(AttributeError):
            self.client.create("account", [{"name": "A"}, {"name": "B"}])

    def test_update_warns_and_delegates(self):
        """client.update() → use client.records.update()"""
        with self.assertRaises(AttributeError):
            self.client.update("account", "guid-1", {"telephone1": "555-0199"})

    def test_delete_warns_and_delegates(self):
        """client.delete() → use client.records.delete()"""
        with self.assertRaises(AttributeError):
            self.client.delete("account", "guid-1")

    def test_get_single_warns(self):
        """client.get(record_id=...) → use client.records.get()"""
        with self.assertRaises(AttributeError):
            self.client.get("account", record_id="guid-1")

    def test_get_multiple_warns(self):
        """client.get(filter=...) → use client.records.get()"""
        with self.assertRaises(AttributeError):
            self.client.get("account", filter="statecode eq 0", top=10)

    # ----------------------------------------------------------------- query

    def test_query_sql_warns(self):
        """client.query_sql() → use client.query.sql()"""
        with self.assertRaises(AttributeError):
            self.client.query_sql("SELECT name FROM account")

    # --------------------------------------------------------------- tables

    def test_get_table_info_warns(self):
        """client.get_table_info() → use client.tables.get()"""
        with self.assertRaises(AttributeError):
            self.client.get_table_info("new_MyTable")

    def test_create_table_warns(self):
        """client.create_table() → use client.tables.create()"""
        with self.assertRaises(AttributeError):
            self.client.create_table("new_Product", {"new_Price": "decimal"})

    def test_delete_table_warns(self):
        """client.delete_table() → use client.tables.delete()"""
        with self.assertRaises(AttributeError):
            self.client.delete_table("new_MyTestTable")

    def test_list_tables_warns(self):
        """client.list_tables() → use client.tables.list()"""
        with self.assertRaises(AttributeError):
            self.client.list_tables()

    def test_create_columns_warns(self):
        """client.create_columns() → use client.tables.add_columns()"""
        with self.assertRaises(AttributeError):
            self.client.create_columns("new_MyTestTable", {"new_Notes": "string"})

    def test_delete_columns_warns(self):
        """client.delete_columns() → use client.tables.remove_columns()"""
        with self.assertRaises(AttributeError):
            self.client.delete_columns("new_MyTestTable", ["new_Notes"])

    # ----------------------------------------------------------------- files

    def test_upload_file_warns(self):
        """client.upload_file() → use client.files.upload()"""
        with self.assertRaises(AttributeError):
            self.client.upload_file("account", "guid-1", "new_Document", "/path/to/file.pdf")


if __name__ == "__main__":
    unittest.main()
