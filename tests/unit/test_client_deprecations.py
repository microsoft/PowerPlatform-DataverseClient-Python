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


class TestRemovedBetaMethodMigrationHint(unittest.TestCase):
    """Removed beta methods raise AttributeError with an actionable migration hint.

    Without the hint the call raises a bare ``AttributeError`` ("'DataverseClient'
    has no attribute 'create'") and the user has no idea their code is calling a
    v0 method that was removed at 1.0 GA. Each hint must:

    1. name the removed method,
    2. point at the GA replacement (``client.records.create(...)`` etc.), and
    3. mention the codemod so they can migrate automatically.
    """

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)

    # Expected GA replacement substring per removed method. Aligned with
    # DataverseClient._REMOVED_BETA_METHODS and migrate_v0_to_v1._CLIENT_SHORTCUTS.
    EXPECTED_HINTS = {
        "create": "client.records.create",
        "update": "client.records.update",
        "delete": "client.records.delete",
        "get": "client.records.get",
        "query_sql": "client.query.sql",
        "get_table_info": "client.tables.get",
        "create_table": "client.tables.create",
        "delete_table": "client.tables.delete",
        "list_tables": "client.tables.list",
        "create_columns": "client.tables.add_columns",
        "delete_columns": "client.tables.remove_columns",
        "upload_file": "client.files.upload",
    }

    def test_every_removed_method_has_a_migration_hint(self):
        """Bug-report condition: bare ``AttributeError('DataverseClient has no
        attribute create')`` had no migration hint. Every removed method must
        produce a message that names both the removed name and the GA path."""
        for old_name, ga_path in self.EXPECTED_HINTS.items():
            with self.subTest(method=old_name):
                with self.assertRaises(AttributeError) as ctx:
                    getattr(self.client, old_name)
                msg = str(ctx.exception)
                self.assertIn(repr(old_name), msg, f"removed name {old_name!r} missing from message: {msg!r}")
                self.assertIn(ga_path, msg, f"GA replacement {ga_path!r} missing from message: {msg!r}")
                self.assertIn("v0 beta", msg, f"v0/beta context missing from message: {msg!r}")

    def test_hint_mentions_the_codemod(self):
        """The bug suggestion explicitly asks for the codemod command in the
        hint so users can migrate without searching docs first."""
        with self.assertRaises(AttributeError) as ctx:
            self.client.create("account", {"name": "X"})
        msg = str(ctx.exception)
        self.assertIn("migrate_v0_to_v1", msg, f"codemod hint missing: {msg!r}")

    def test_bug_repro_create_emits_records_create_hint(self):
        """Exact bug-report repro: ``client.create('account', {...})``."""
        with self.assertRaises(AttributeError) as ctx:
            self.client.create("account", {"name": "Contoso"})
        msg = str(ctx.exception)
        self.assertIn("'create'", msg)
        self.assertIn("client.records.create(table, data)", msg)

    def test_bug_repro_query_sql_emits_query_sql_hint(self):
        """Second bug-report repro: ``client.query_sql(...)``."""
        with self.assertRaises(AttributeError) as ctx:
            self.client.query_sql("SELECT name FROM account")
        msg = str(ctx.exception)
        self.assertIn("'query_sql'", msg)
        self.assertIn("client.query.sql(sql)", msg)

    def test_unknown_attribute_does_not_get_migration_hint(self):
        """Truly unrelated attribute names must NOT pretend to be removed beta
        methods -- otherwise typos like ``client.creat`` give misleading advice."""
        with self.assertRaises(AttributeError) as ctx:
            self.client.totally_made_up_method  # noqa: B018  # attribute access for its side effect
        msg = str(ctx.exception)
        self.assertNotIn("v0 beta", msg, f"unrelated attr wrongly tagged as removed: {msg!r}")
        self.assertNotIn("migrate_v0_to_v1", msg, f"unrelated attr wrongly references codemod: {msg!r}")

    def test_dunder_attribute_does_not_get_migration_hint(self):
        """Dunder/private lookups (pickle, deepcopy, IDE introspection) must
        not route through the hint path -- protocol probes expect plain
        AttributeError, not multi-line migration text."""
        with self.assertRaises(AttributeError) as ctx:
            self.client.__definitely_not_a_real_dunder__  # noqa: B018
        msg = str(ctx.exception)
        self.assertNotIn("v0 beta", msg)
        self.assertNotIn("migrate_v0_to_v1", msg)

    def test_hasattr_still_returns_false_for_removed_method(self):
        """``hasattr`` semantics must not change: feature-detection of a
        removed method should report False, not crash on the hint."""
        self.assertFalse(hasattr(self.client, "create"))
        self.assertFalse(hasattr(self.client, "query_sql"))

    def test_existing_namespaces_unaffected_by_getattr(self):
        """The namespaces installed in __init__ (records, query, tables, files,
        dataframe, batch) must still resolve via normal lookup and never reach
        ``__getattr__``. Otherwise the migration hint would shadow real API."""
        from PowerPlatform.Dataverse.operations.records import RecordOperations
        from PowerPlatform.Dataverse.operations.query import QueryOperations
        from PowerPlatform.Dataverse.operations.tables import TableOperations
        from PowerPlatform.Dataverse.operations.files import FileOperations

        self.assertIsInstance(self.client.records, RecordOperations)
        self.assertIsInstance(self.client.query, QueryOperations)
        self.assertIsInstance(self.client.tables, TableOperations)
        self.assertIsInstance(self.client.files, FileOperations)

    def test_runtime_mapping_matches_codemod_shortcuts(self):
        """Drift guard: every method the codemod rewrites must also produce a
        runtime hint, and vice versa. If someone adds a new shortcut to the
        codemod they must remember to add it here too (or callers of the new
        beta name will silently get a bare AttributeError again)."""
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import _CLIENT_SHORTCUTS

        self.assertEqual(
            set(DataverseClient._REMOVED_BETA_METHODS.keys()),
            set(_CLIENT_SHORTCUTS.keys()),
            "DataverseClient._REMOVED_BETA_METHODS drifted from migrate_v0_to_v1._CLIENT_SHORTCUTS",
        )


if __name__ == "__main__":
    unittest.main()
