# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for PowerPlatform/Dataverse/migration/migrate_v0_to_v1.py.

Covers:
- QueryBuilder.to_dataframe() -> .execute().to_dataframe()  (auto-rewrite)
- QueryResult.to_dataframe() left untouched (receiver is .execute())
- QueryBuilder chain via .select(), .where(), .filter_eq() all trigger the rewrite
- client.get(t, id) -> client.records.get(t, id)  (top-level shortcut)
- batch.records.get(t, id) -> batch.records.retrieve(t, id)
- .filter_eq / .filter_ne / .filter_gt  ->  .where(col(...) OP v)
- .filter_null / .filter_not_null  ->  .where(col(...).is_null/is_not_null())
- .filter_raw / .filter  ->  .where(raw(...))
- .execute(by_page=True)  ->  .execute_pages()
- .execute(by_page=False)  ->  .execute() with flag stripped
- find_manual_patterns: flags client.records.get(), execute(by_page=variable), client.dataframe.get()
"""

import textwrap
import unittest

try:
    import libcst  # noqa: F401

    _LIBCST_AVAILABLE = True
except ImportError:
    _LIBCST_AVAILABLE = False

_skip_no_libcst = unittest.skipUnless(_LIBCST_AVAILABLE, "libcst not installed")


def _migrate(source: str, *, client_var: str = "client") -> str:
    from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import migrate_source

    return migrate_source(textwrap.dedent(source), client_var=client_var)


def _find_manual(source: str, *, client_var: str = "client") -> list:
    from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import find_manual_patterns

    return find_manual_patterns(textwrap.dedent(source), client_var=client_var)


# ---------------------------------------------------------------------------
# QueryBuilder.to_dataframe()  ->  .execute().to_dataframe()
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestToDataframeRewrite(unittest.TestCase):
    """QueryBuilder.to_dataframe() receives .execute() insertion."""

    def test_builder_chain_gets_execute_inserted(self):
        src = "df = client.query.builder('account').select('name').to_dataframe()\n"
        out = _migrate(src)
        self.assertIn(".execute().to_dataframe()", out)
        self.assertNotIn(".to_dataframe().to_dataframe()", out)

    def test_where_chain_triggers_rewrite(self):
        src = "df = q.where(col('statecode') == 0).to_dataframe()\n"
        out = _migrate(src)
        self.assertIn(".execute().to_dataframe()", out)

    def test_filter_eq_chain_triggers_rewrite(self):
        src = "df = q.filter_eq('statecode', 0).to_dataframe()\n"
        out = _migrate(src)
        self.assertIn(".execute().to_dataframe()", out)

    def test_select_alone_triggers_rewrite(self):
        src = "df = q.select('name', 'revenue').to_dataframe()\n"
        out = _migrate(src)
        self.assertIn(".execute().to_dataframe()", out)

    def test_already_executed_not_double_wrapped(self):
        src = "df = q.select('name').execute().to_dataframe()\n"
        out = _migrate(src)
        self.assertNotIn(".execute().execute()", out)
        self.assertIn(".execute().to_dataframe()", out)

    def test_unrelated_to_dataframe_not_rewritten(self):
        src = "df = some_result.to_dataframe()\n"
        out = _migrate(src)
        self.assertNotIn(".execute()", out)
        self.assertIn("some_result.to_dataframe()", out)

    def test_full_chain_structure_preserved(self):
        src = "df = client.query.builder('account')\\\n" "        .select('name')\\\n" "        .to_dataframe()\n"
        out = _migrate(src)
        # .execute() is inserted before .to_dataframe(); a line-continuation may separate them
        self.assertIn(".execute()", out)
        self.assertIn(".to_dataframe()", out)
        self.assertNotIn(".get(", out)

    def test_rewrite_inside_assignment(self):
        src = "result = builder.select('name').to_dataframe()\n"
        out = _migrate(src)
        self.assertIn(".execute().to_dataframe()", out)


# ---------------------------------------------------------------------------
# Top-level shortcut rewrites
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestClientShortcutRewrites(unittest.TestCase):
    def test_client_get_becomes_records_get(self):
        src = "r = client.get('account', 'abc')\n"
        out = _migrate(src)
        self.assertIn("client.records.get(", out)
        self.assertNotIn("client.get(", out)

    def test_client_create_becomes_records_create(self):
        src = "client.create('account', {'name': 'X'})\n"
        out = _migrate(src)
        self.assertIn("client.records.create(", out)

    def test_client_delete_becomes_records_delete(self):
        src = "client.delete('account', 'abc')\n"
        out = _migrate(src)
        self.assertIn("client.records.delete(", out)

    def test_client_update_becomes_records_update(self):
        src = "client.update('account', 'abc', {'name': 'Y'})\n"
        out = _migrate(src)
        self.assertIn("client.records.update(", out)

    def test_client_query_sql_becomes_query_sql(self):
        src = "rows = client.query_sql('SELECT * FROM account')\n"
        out = _migrate(src)
        self.assertIn("client.query.sql(", out)

    def test_client_get_table_info_becomes_tables_get(self):
        src = "info = client.get_table_info('account')\n"
        out = _migrate(src)
        self.assertIn("client.tables.get(", out)

    def test_client_list_tables_becomes_tables_list(self):
        src = "tables = client.list_tables()\n"
        out = _migrate(src)
        self.assertIn("client.tables.list(", out)

    def test_client_var_override(self):
        src = "r = svc.get('account', 'abc')\n"
        out = _migrate(src, client_var="svc")
        self.assertIn("svc.records.get(", out)

    def test_client_get_not_matched_on_other_receiver(self):
        src = "v = record.get('name')\n"
        out = _migrate(src)
        self.assertIn("record.get(", out)
        self.assertNotIn("record.records.get(", out)


# ---------------------------------------------------------------------------
# batch.records.get() -> batch.records.retrieve()
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestBatchRecordsGetRewrite(unittest.TestCase):
    def test_batch_records_get_becomes_retrieve(self):
        src = "batch.records.get('account', 'abc')\n"
        out = _migrate(src)
        self.assertIn("batch.records.retrieve(", out)
        self.assertNotIn("batch.records.get(", out)

    def test_client_records_get_not_rewritten(self):
        src = "client.records.get('account', 'abc')\n"
        out = _migrate(src)
        self.assertIn("client.records.get(", out)
        self.assertNotIn("client.records.retrieve(", out)


# ---------------------------------------------------------------------------
# .filter_*() -> .where(col(...) ...) rewrites
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestFilterMethodRewrites(unittest.TestCase):
    def test_filter_eq(self):
        src = "q.filter_eq('statecode', 0)\n"
        out = _migrate(src)
        self.assertIn(".where(", out)
        self.assertIn("col(", out)

    def test_filter_ne(self):
        src = "q.filter_ne('statecode', 0)\n"
        out = _migrate(src)
        self.assertIn(".where(", out)

    def test_filter_gt(self):
        src = "q.filter_gt('revenue', 1000)\n"
        out = _migrate(src)
        self.assertIn(".where(", out)

    def test_filter_null(self):
        src = "q.filter_null('email')\n"
        out = _migrate(src)
        self.assertIn(".is_null()", out)

    def test_filter_not_null(self):
        src = "q.filter_not_null('email')\n"
        out = _migrate(src)
        self.assertIn(".is_not_null()", out)

    def test_filter_raw(self):
        src = "q.filter_raw('statecode eq 0')\n"
        out = _migrate(src)
        self.assertIn("raw(", out)

    def test_filter_string_literal(self):
        src = "q.filter('statecode eq 0')\n"
        out = _migrate(src)
        self.assertIn(".where(raw(", out)

    def test_filter_between(self):
        src = "q.filter_between('revenue', 1000, 5000)\n"
        out = _migrate(src)
        self.assertIn(".between(", out)

    def test_filter_in(self):
        src = "q.filter_in('statecode', [0, 1])\n"
        out = _migrate(src)
        self.assertIn(".in_(", out)


# ---------------------------------------------------------------------------
# .execute(by_page=...) -> .execute_pages() / .execute()
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestExecuteByPageRewrite(unittest.TestCase):
    def test_execute_by_page_true_becomes_execute_pages(self):
        src = "result = q.execute(by_page=True)\n"
        out = _migrate(src)
        self.assertIn(".execute_pages()", out)
        self.assertNotIn("by_page", out)

    def test_execute_by_page_false_strips_flag(self):
        src = "result = q.execute(by_page=False)\n"
        out = _migrate(src)
        self.assertIn(".execute()", out)
        self.assertNotIn("by_page", out)
        self.assertNotIn("execute_pages", out)

    def test_execute_no_args_unchanged(self):
        src = "result = q.execute()\n"
        out = _migrate(src)
        self.assertIn(".execute()", out)
        self.assertNotIn("execute_pages", out)


# ---------------------------------------------------------------------------
# find_manual_patterns
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestFindManualPatterns(unittest.TestCase):
    def test_client_records_get_flagged(self):
        src = "client.records.get('account', 'abc')\n"
        findings = _find_manual(src)
        self.assertTrue(any("records.get" in f for f in findings))

    def test_execute_by_page_variable_flagged(self):
        src = "q.execute(by_page=flag)\n"
        findings = _find_manual(src)
        self.assertTrue(any("by_page" in f for f in findings))

    def test_execute_by_page_literal_not_flagged(self):
        src = "q.execute(by_page=True)\n"
        findings = _find_manual(src)
        self.assertFalse(any("by_page" in f for f in findings))

    def test_client_dataframe_get_flagged(self):
        src = "client.dataframe.get('account')\n"
        findings = _find_manual(src)
        self.assertTrue(any("dataframe.get" in f for f in findings))

    def test_query_sql_select_flagged(self):
        src = "client.query.sql_select('account', ['name'])\n"
        findings = _find_manual(src)
        self.assertTrue(any("sql_select" in f for f in findings))

    def test_clean_code_has_no_findings(self):
        src = (
            "result = client.records.retrieve('account', 'abc')\n" "pages = client.records.list('account').execute()\n"
        )
        findings = _find_manual(src)
        self.assertEqual(findings, [])

    def test_batch_records_get_not_flagged(self):
        src = "batch.records.get('account', 'abc')\n"
        findings = _find_manual(src)
        self.assertFalse(any("records.get" in f for f in findings))


# ---------------------------------------------------------------------------
# CLI: --help / -h handling
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestMainHelp(unittest.TestCase):
    """``main()`` returns 0 and prints usage when --help / -h is passed.

    Regression guard for the UX gap where ``--help`` was treated as a positional
    path argument and produced ``[WARN] Not a file or directory: --help``.
    """

    def _run_main_capture(self, argv):
        import io
        import contextlib
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import main

        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            rc = main(argv)
        return rc, buf.getvalue()

    def test_long_help_flag_returns_zero(self):
        rc, _ = self._run_main_capture(["--help"])
        self.assertEqual(rc, 0)

    def test_short_help_flag_returns_zero(self):
        rc, _ = self._run_main_capture(["-h"])
        self.assertEqual(rc, 0)

    def test_help_prints_usage_line(self):
        _, out = self._run_main_capture(["--help"])
        self.assertIn("Usage:", out)
        self.assertIn("dataverse-migrate", out)

    def test_help_takes_precedence_over_other_flags(self):
        """--help with other flags still exits 0 without processing paths."""
        rc, _ = self._run_main_capture(["--dry-run", "--help", "/nonexistent/path"])
        self.assertEqual(rc, 0)

    def test_no_args_returns_one(self):
        """No arguments still prints usage but returns 1 (error)."""
        rc, out = self._run_main_capture([])
        self.assertEqual(rc, 1)
        self.assertIn("Usage:", out)


if __name__ == "__main__":
    unittest.main()
