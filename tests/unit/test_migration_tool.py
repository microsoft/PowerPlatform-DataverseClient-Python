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


# ---------------------------------------------------------------------------
# Keyword-argument forms of removed v0 filter methods (bug repair: ADO 6410701)
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestFilterKwargsRewrites(unittest.TestCase):
    """Kwarg and mixed positional/kwarg forms of .filter_*() rewrite to .where(col(...))."""

    def test_filter_eq_full_kwargs_rewrites(self):
        src = "q = client.query.builder('account').filter_eq(column='name', value='X')\n"
        out = _migrate(src)
        self.assertIn(".where(col('name') == 'X')", out)
        self.assertNotIn("filter_eq", out)

    def test_filter_between_mixed_positional_and_kwargs_rewrites(self):
        src = "q = client.query.builder('account').filter_between('revenue', low=1000, high=9999)\n"
        out = _migrate(src)
        self.assertIn(".where(col('revenue').between(1000, 9999))", out)
        self.assertNotIn("filter_between", out)

    def test_filter_between_full_kwargs_rewrites(self):
        src = "q = b.filter_between(column='revenue', low=1, high=10)\n"
        out = _migrate(src)
        self.assertIn(".where(col('revenue').between(1, 10))", out)

    def test_filter_between_reversed_kwarg_order_preserves_slot_meaning(self):
        # kwargs name the slot, not the position -- high=10/low=1 must still produce between(1, 10).
        src = "q = b.filter_between('revenue', high=10, low=1)\n"
        out = _migrate(src)
        self.assertIn(".where(col('revenue').between(1, 10))", out)

    def test_filter_in_uses_values_plural_kwarg(self):
        src = "q = b.filter_in(column='status', values=[1, 2, 3])\n"
        out = _migrate(src)
        self.assertIn(".where(col('status').in_([1, 2, 3]))", out)

    def test_filter_not_in_uses_values_plural_kwarg(self):
        src = "q = b.filter_not_in('status', values=[4, 5])\n"
        out = _migrate(src)
        self.assertIn(".where(col('status').not_in([4, 5]))", out)

    def test_filter_null_with_column_kwarg(self):
        src = "q = b.filter_null(column='deleted_on')\n"
        out = _migrate(src)
        self.assertIn(".where(col('deleted_on').is_null())", out)

    def test_filter_not_null_with_column_kwarg(self):
        src = "q = b.filter_not_null(column='deleted_on')\n"
        out = _migrate(src)
        self.assertIn(".where(col('deleted_on').is_not_null())", out)

    def test_filter_contains_full_kwargs(self):
        src = "q = b.filter_contains(column='name', value='Contoso')\n"
        out = _migrate(src)
        self.assertIn(".where(col('name').contains('Contoso'))", out)

    def test_filter_startswith_full_kwargs(self):
        src = "q = b.filter_startswith(column='name', value='Con')\n"
        out = _migrate(src)
        self.assertIn(".where(col('name').startswith('Con'))", out)

    def test_filter_raw_with_filter_string_kwarg(self):
        src = "q = b.filter_raw(filter_string=\"name eq 'X'\")\n"
        out = _migrate(src)
        self.assertIn(".where(raw(\"name eq 'X'\"))", out)

    def test_filter_method_with_filter_string_kwarg(self):
        src = "q = b.filter(filter_string=\"name eq 'X'\")\n"
        out = _migrate(src)
        self.assertIn(".where(raw(\"name eq 'X'\"))", out)

    def test_kwargs_form_triggers_col_import(self):
        # The kwargs path must register _needs_col so the import is injected.
        src = (
            "from PowerPlatform.Dataverse.client import DataverseClient\n"
            "q = client.query.builder('account').filter_eq(column='name', value='X')\n"
        )
        out = _migrate(src)
        self.assertIn("from PowerPlatform.Dataverse.models.filters import col", out)

    def test_kwargs_form_in_chain_with_to_dataframe(self):
        # Confirm the .to_dataframe() chain detector still recognises kwargs-form filter calls.
        src = "df = client.query.builder('account').filter_eq(column='name', value='X').to_dataframe()\n"
        out = _migrate(src)
        self.assertIn(".execute().to_dataframe()", out)
        self.assertIn(".where(col('name') == 'X')", out)


@_skip_no_libcst
class TestFilterKwargsBugReproADO6410701(unittest.TestCase):
    """Exact two-line repro from the ADO 6410701 repair item.

    Prior to the fix the codemod left the file completely unchanged, mis-counting
    it as "0 changed" -- which gave the user every reason to believe the file was
    already v1-compatible. After the fix, both lines are rewritten and the
    resulting source uses only GA-supported APIs.
    """

    def test_repro_exact_input_is_fully_rewritten(self):
        src = (
            "q = client.query.builder('account').filter_eq(column='name', value='X')\n"
            "q2 = client.query.builder('account').filter_between('revenue', low=1000, high=9999)\n"
        )
        out = _migrate(src)
        self.assertNotIn("filter_eq", out)
        self.assertNotIn("filter_between", out)
        self.assertIn(".where(col('name') == 'X')", out)
        self.assertIn(".where(col('revenue').between(1000, 9999))", out)


# ---------------------------------------------------------------------------
# Standalone filter functions: kwargs form
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestStandaloneFilterFuncsKwargs(unittest.TestCase):
    """Standalone filter functions (eq, between, ...) accept kwarg forms too."""

    def test_eq_func_kwargs(self):
        src = "from PowerPlatform.Dataverse.models.filters import eq\n" "f = eq(column='name', value='X')\n"
        out = _migrate(src)
        self.assertIn("(col('name') == 'X')", out)

    def test_between_func_kwargs(self):
        src = (
            "from PowerPlatform.Dataverse.models.filters import between\n"
            "f = between(column='revenue', low=1, high=10)\n"
        )
        out = _migrate(src)
        self.assertIn("col('revenue').between(1, 10)", out)

    def test_filter_in_func_kwargs(self):
        src = (
            "from PowerPlatform.Dataverse.models.filters import filter_in\n"
            "f = filter_in(column='status', values=[1, 2])\n"
        )
        out = _migrate(src)
        self.assertIn("col('status').in_([1, 2])", out)


# ---------------------------------------------------------------------------
# Manual-review safety net for unrecognized .filter_*() arg shapes
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestUnrecognizedFilterKwargsEmitManualNote(unittest.TestCase):
    """Calls with non-canonical kwargs are flagged for manual review instead of being silently passed."""

    def test_unrecognized_kwargs_emit_manual_finding(self):
        # `field=`/`val=` are NOT the v0 kwarg names -- the codemod must not rewrite,
        # and the manual-review finder must surface this so the user knows to act.
        src = "q = b.filter_eq(field='name', val='X')\n"
        out = _migrate(src)
        # Source is unchanged because no canonical arg shape matched.
        self.assertEqual(out, src)
        notes = _find_manual(src)
        self.assertTrue(
            any(".filter_eq" in n for n in notes),
            f"expected a .filter_eq manual-review note, got {notes!r}",
        )

    def test_unrecognized_between_kwargs_emit_manual_finding(self):
        src = "q = b.filter_between(column='revenue', lo=1, hi=10)\n"
        out = _migrate(src)
        self.assertEqual(out, src)
        notes = _find_manual(src)
        self.assertTrue(
            any(".filter_between" in n for n in notes),
            f"expected a .filter_between manual-review note, got {notes!r}",
        )

    def test_recognized_kwargs_do_not_emit_manual_finding(self):
        # If the migrator CAN rewrite, the finder must not also flag the same call.
        src = "q = b.filter_eq(column='name', value='X')\n"
        notes = _find_manual(src)
        self.assertFalse(
            any(".filter_eq" in n for n in notes),
            f"unexpected .filter_eq manual-review note for a rewriteable call: {notes!r}",
        )

    def test_positional_only_does_not_emit_manual_finding(self):
        src = "q = b.filter_eq('name', 'X')\n"
        notes = _find_manual(src)
        self.assertFalse(
            any(".filter_eq" in n for n in notes),
            f"unexpected .filter_eq manual-review note for positional call: {notes!r}",
        )

    # ------------------------------------------------------------------
    # Bug raised in PR #184 review (Copilot on _can_extract_filter_method_args):
    # arity-violating calls were treated as "extractable" as soon as the first
    # arg was present. Migrator would then rewrite to .where(...) dropping the
    # extras silently. With the shape gate tightened both predicates agree:
    # malformed calls stay untouched AND get a [MANUAL] note.
    # ------------------------------------------------------------------

    def test_filter_raw_with_extra_arg_is_not_rewritten(self):
        src = "q = b.filter_raw('a eq 1', 'EXTRA')\n"
        out = _migrate(src)
        self.assertEqual(out, src, "filter_raw with extra arg must not be rewritten")
        notes = _find_manual(src)
        self.assertTrue(
            any(".filter_raw" in n for n in notes),
            f"expected a .filter_raw manual-review note, got {notes!r}",
        )

    def test_unary_filter_with_extra_arg_is_not_rewritten(self):
        # filter_null is documented as 1-arity; an extra positional is malformed
        # and must surface to the user, not be silently dropped.
        for unary in ("filter_null", "filter_not_null"):
            with self.subTest(method=unary):
                src = f"q = b.{unary}('col', 'EXTRA')\n"
                out = _migrate(src)
                self.assertEqual(out, src, f"{unary} with extra arg must not be rewritten")
                notes = _find_manual(src)
                self.assertTrue(
                    any(f".{unary}" in n for n in notes),
                    f"expected a .{unary} manual-review note, got {notes!r}",
                )

    def test_binary_filter_with_extra_arg_is_not_rewritten(self):
        src = "q = b.filter_eq('a', 1, 'EXTRA')\n"
        out = _migrate(src)
        self.assertEqual(out, src, "filter_eq with extra arg must not be rewritten")
        notes = _find_manual(src)
        self.assertTrue(
            any(".filter_eq" in n for n in notes),
            f"expected a .filter_eq manual-review note, got {notes!r}",
        )

    def test_between_with_extra_arg_is_not_rewritten(self):
        src = "q = b.filter_between('col', 1, 10, 'EXTRA')\n"
        out = _migrate(src)
        self.assertEqual(out, src, "filter_between with extra arg must not be rewritten")
        notes = _find_manual(src)
        self.assertTrue(
            any(".filter_between" in n for n in notes),
            f"expected a .filter_between manual-review note, got {notes!r}",
        )

    def test_canonical_arity_still_rewrites_cleanly(self):
        # Regression guard: the shape tightening must not break legitimate calls.
        for src, expected_sub in (
            ("q = b.filter_eq('a', 1)\n", "where(col('a') == 1)"),
            ("q = b.filter_raw('a eq 1')\n", "where(raw('a eq 1'))"),
            ("q = b.filter_null('c')\n", "where(col('c').is_null())"),
            ("q = b.filter_between('c', 1, 10)\n", "where(col('c').between(1, 10))"),
        ):
            with self.subTest(src=src.strip()):
                out = _migrate(src)
                self.assertIn(expected_sub, out, f"canonical call broken by tightening: {src!r} -> {out!r}")
                notes = _find_manual(src)
                self.assertFalse(
                    any("kwargs/argument shape not recognized" in n for n in notes),
                    f"unexpected shape-not-recognized note for canonical call: {notes!r}",
                )


# ---------------------------------------------------------------------------
# Manual-review notes carry source line:col (bug repair: sub-finding A)
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestManualFindingsHaveLineNumbers(unittest.TestCase):
    """Every [MANUAL] note is prefixed with ``<line>:<col>: `` so editors can jump to it.

    libcst's PositionProvider gives 1-based lines and 0-based columns. The
    finder formats the prefix as ``"<line>:<col>: "``; :func:`migrate_file`
    prepends the path to produce the canonical
    ``"<path>:<line>:<col>: <message>"`` shape.
    """

    @staticmethod
    def _is_lineprefixed(note: str) -> bool:
        # ``<line>:<col>: <message>`` -- line and col are digits, separated by ``:``,
        # followed by a trailing ``: `` and then the human-readable message.
        head = note.split(":", 2)
        return len(head) >= 3 and head[0].isdigit() and head[1].isdigit() and head[2].startswith(" ")

    def test_records_get_finding_has_line_col_prefix(self):
        src = "x = client.records.get('account', 'abc')\n"
        notes = _find_manual(src)
        self.assertEqual(len(notes), 1)
        self.assertTrue(
            self._is_lineprefixed(notes[0]),
            f"expected line:col prefix on note, got {notes[0]!r}",
        )
        # The call is on line 1.
        self.assertTrue(notes[0].startswith("1:"), f"expected line=1 prefix, got {notes[0]!r}")

    def test_finding_reports_correct_line_for_call_on_line_three(self):
        src = "# header comment\n" "x = 1\n" "y = client.records.get('account', 'abc')\n"
        notes = _find_manual(src)
        self.assertEqual(len(notes), 1)
        self.assertTrue(notes[0].startswith("3:"), f"expected line=3 prefix, got {notes[0]!r}")

    def test_multiple_findings_carry_distinct_line_numbers(self):
        src = (
            "a = client.records.get('account', '1')\n"
            "b = client.dataframe.get('account', 'guid-1')\n"
            "c = q.execute(by_page=flag)\n"
        )
        notes = _find_manual(src)
        self.assertEqual(len(notes), 3)
        # Each note must carry the line number of the call that produced it.
        line_prefixes = sorted(int(n.split(":", 1)[0]) for n in notes)
        self.assertEqual(line_prefixes, [1, 2, 3])

    def test_unrecognized_filter_kwargs_finding_has_line_prefix(self):
        src = "\n\nq = b.filter_eq(field='name', val='X')\n"
        notes = _find_manual(src)
        self.assertEqual(len(notes), 1)
        # The call is on line 3.
        self.assertTrue(notes[0].startswith("3:"), f"expected line=3 prefix, got {notes[0]!r}")
        self.assertIn(".filter_eq", notes[0])


@_skip_no_libcst
class TestMigrateFilePrependsPath(unittest.TestCase):
    """`migrate_file` returns notes with full ``<path>:<line>:<col>: <message>`` shape."""

    def setUp(self):
        import tempfile

        self._tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False, encoding="utf-8")
        self._tmp.write("x = client.records.get('account', 'abc')\n")
        self._tmp.close()

    def tearDown(self):
        import os

        os.unlink(self._tmp.name)

    def test_migrate_file_notes_include_path_line_col(self):
        from pathlib import Path
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import migrate_file

        path = Path(self._tmp.name)
        _, _, notes = migrate_file(path, dry_run=True)
        self.assertEqual(len(notes), 1)
        # Expected shape: "<path>:1:<col>: <message>"
        self.assertTrue(
            notes[0].startswith(f"{path}:1:"),
            f"expected note to start with '{path}:1:', got {notes[0]!r}",
        )
        # The message body still mentions the API.
        self.assertIn("records.get", notes[0])

    def test_note_line_col_match_migrated_output_after_import_insertion(self):
        """Bug raised in PR #184 review (Copilot on migrate_file):
        when the codemod inserts a new ``from ...filters import col`` line at
        the top, every subsequent line shifts by +1. find_manual_patterns()
        used to run against the *original* source, so a note that should
        point at the actual records.get() line in the post-migration file
        pointed one line too high. Editors / CI annotations then jumped to
        the wrong line."""
        import os
        import tempfile
        from pathlib import Path
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import migrate_file

        src = (
            "import os\n"  # line 1 in both original and migrated
            "q = client.query.builder('account').filter_eq('name', 'X')\n"  # line 2 in original
            "client.records.get('a', 'b')\n"  # line 3 in original, becomes line 4 in migrated
        )
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False, encoding="utf-8")
        try:
            tmp.write(src)
            tmp.close()
            path = Path(tmp.name)
            _, _, notes = migrate_file(path, dry_run=False)

            # Locate the records.get note. There may be multiple notes; pick this one.
            records_get_notes = [n for n in notes if "records.get" in n and "retrieve" in n]
            self.assertEqual(len(records_get_notes), 1, f"expected exactly one records.get note, got {notes!r}")

            # Verify the migrated file actually placed records.get on line 4.
            migrated_lines = path.read_text(encoding="utf-8").splitlines()
            records_get_line = next(i + 1 for i, line in enumerate(migrated_lines) if "records.get" in line)
            self.assertEqual(
                records_get_line,
                4,
                f"sanity check on migrated layout (col-import added at line 2); got {migrated_lines!r}",
            )

            # Now the note's "<path>:<line>:<col>:" prefix must match line 4,
            # not the original line 3.
            self.assertTrue(
                records_get_notes[0].startswith(f"{path}:{records_get_line}:"),
                f"note's line must match migrated output ({records_get_line}); got {records_get_notes[0]!r}",
            )
        finally:
            os.unlink(tmp.name)

    def test_rewritten_call_does_not_produce_stale_manual_note(self):
        """A call the codemod successfully rewrote (e.g. ``client.create(...)``
        → ``client.records.create(...)``) must not also produce a [MANUAL] note
        about its v0 shape. Running find_manual_patterns on the *migrated*
        source guarantees this because the v0 shape no longer exists there."""
        import os
        import tempfile
        from pathlib import Path
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import migrate_file

        src = "client.create('account', {'name': 'X'})\n"
        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False, encoding="utf-8")
        try:
            tmp.write(src)
            tmp.close()
            path = Path(tmp.name)
            _, _, notes = migrate_file(path, dry_run=False)
            # The rewrite produced ``client.records.create(...)`` -- which is
            # the canonical v1 form -- so no [MANUAL] notes should fire about it.
            self.assertEqual(
                notes,
                [],
                f"rewritten client.create should not produce a stale [MANUAL] note: {notes!r}",
            )
            self.assertIn("client.records.create", path.read_text(encoding="utf-8"))
        finally:
            os.unlink(tmp.name)


# ---------------------------------------------------------------------------
# dataframe.get(record_id=...) emits records.retrieve recipe (sub-finding B)
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestDataframeGetRecordIdRecipe(unittest.TestCase):
    """`client.dataframe.get(table, record_id=...)` recommends `records.retrieve()`."""

    def test_record_id_kwarg_recommends_records_retrieve(self):
        src = "row = client.dataframe.get('account', record_id=account_id, select=['name'])\n"
        notes = _find_manual(src)
        self.assertEqual(len(notes), 1)
        self.assertIn("records.retrieve", notes[0])
        self.assertNotIn("query.builder", notes[0])

    def test_record_id_positional_recommends_records_retrieve(self):
        # v0 signature: dataframe.get(table, record_id=None, ...) -- 2nd positional is record_id.
        src = "row = client.dataframe.get('account', 'guid-1', select=['name'])\n"
        notes = _find_manual(src)
        self.assertEqual(len(notes), 1)
        self.assertIn("records.retrieve", notes[0])
        self.assertNotIn("query.builder", notes[0])

    def test_no_record_id_recommends_query_builder(self):
        # Multi-record fetch -- keep the query.builder recipe.
        src = "df = client.dataframe.get('account', select=['name'], filter='statecode eq 0')\n"
        notes = _find_manual(src)
        self.assertEqual(len(notes), 1)
        self.assertIn("query.builder", notes[0])
        self.assertNotIn("records.retrieve", notes[0])

    def test_record_id_kwarg_recipe_mentions_semantic_shift(self):
        # The recipe must warn callers that retrieve() returns Record | None, not a DataFrame.
        src = "row = client.dataframe.get('account', record_id=x)\n"
        notes = _find_manual(src)
        self.assertEqual(len(notes), 1)
        self.assertIn("Record | None", notes[0])
        self.assertIn("DataFrame", notes[0])

    def test_record_id_recipe_honors_client_var_override(self):
        src = "row = svc.dataframe.get('account', record_id=x)\n"
        notes = _find_manual(src, client_var="svc")
        self.assertEqual(len(notes), 1)
        self.assertIn("svc.records.retrieve", notes[0])

    def test_table_only_form_recommends_query_builder(self):
        # dataframe.get("account") with nothing else -- caller wants all rows.
        src = "df = client.dataframe.get('account')\n"
        notes = _find_manual(src)
        self.assertEqual(len(notes), 1)
        self.assertIn("query.builder", notes[0])


# ---------------------------------------------------------------------------
# Re-run stability: [MIGRATED] does not flip to [NEEDS-MANUAL] on second run
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestRerunStability(unittest.TestCase):
    """A file the codemod migrated on run 1 must not flip to [NEEDS-MANUAL] on run 2.

    Root cause was that ``migrate_file`` only signalled ``was_changed``; on re-run
    the file was byte-identical to its prior output (``was_changed=False``) but
    advisory [MANUAL] notes still re-fired, so the per-file label flipped. Fix
    adds an ``already_migrated`` signal driven by v1-exclusive sentinels.
    """

    def _migrate_file_once(self, content: str):
        """Write *content* to a temp .py file, run migrate_file, return (was_changed, already_migrated, notes, new_content)."""
        import os
        import tempfile
        from pathlib import Path
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import migrate_file

        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False, encoding="utf-8")
        try:
            tmp.write(content)
            tmp.close()
            path = Path(tmp.name)
            was_changed, already_migrated, notes = migrate_file(path)
            new_content = path.read_text(encoding="utf-8")
            return was_changed, already_migrated, notes, new_content
        finally:
            os.unlink(tmp.name)

    def test_v0_file_first_run_then_rerun_is_idempotent_label(self):
        # Run 1: v0 source containing a filter the codemod rewrites AND a [MANUAL]-only
        # advisory call. Should be (changed=True, already_migrated=False).
        import os
        import tempfile
        from pathlib import Path
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import migrate_file

        v0_source = (
            "q = client.query.builder('account').filter_eq('name', 'X')\n"
            "row = client.records.get('account', 'guid-1')\n"
        )

        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False, encoding="utf-8")
        try:
            tmp.write(v0_source)
            tmp.close()
            path = Path(tmp.name)

            # Run 1: codemod rewrites filter_eq and emits manual note for records.get.
            was_changed_1, already_migrated_1, notes_1 = migrate_file(path)
            self.assertTrue(was_changed_1, "run 1 should rewrite filter_eq")
            self.assertFalse(already_migrated_1, "run 1 is not a re-run")
            self.assertTrue(any("records.get" in n for n in notes_1))
            after_run_1 = path.read_text(encoding="utf-8")

            # Run 2: same path, no edits. File should be byte-identical and labeled re-run.
            was_changed_2, already_migrated_2, notes_2 = migrate_file(path)
            after_run_2 = path.read_text(encoding="utf-8")
            self.assertEqual(after_run_1, after_run_2, "re-run output must be byte-identical")
            self.assertFalse(was_changed_2, "re-run on migrated file should not change it")
            self.assertTrue(already_migrated_2, "re-run on migrated file must signal already_migrated")
            # Manual notes still re-fire -- that's expected, they're advisory.
            self.assertTrue(any("records.get" in n for n in notes_2))
        finally:
            os.unlink(tmp.name)

    def test_pure_manual_v0_file_stays_needs_manual_across_reruns(self):
        # A file with only [MANUAL]-flagged v0 patterns (no auto-rewriteable v0 forms,
        # no v1 sentinels) must keep its [NEEDS-MANUAL] semantics on every run, not
        # be misclassified as already_migrated.
        v0_only = "row = client.records.get('account', 'guid-1')\n"
        was_changed, already_migrated, notes, after = self._migrate_file_once(v0_only)
        self.assertFalse(was_changed)
        self.assertFalse(already_migrated, "file has no v1 sentinels -- must not be ALREADY-MIGRATED")
        self.assertTrue(any("records.get" in n for n in notes))
        self.assertEqual(after, v0_only)

    def test_clean_file_with_no_patterns_is_unchanged(self):
        # No Dataverse APIs at all -- neither MIGRATED, ALREADY-MIGRATED, nor NEEDS-MANUAL.
        clean = "x = 1\nprint('hello')\n"
        was_changed, already_migrated, notes, after = self._migrate_file_once(clean)
        self.assertFalse(was_changed)
        self.assertFalse(already_migrated)
        self.assertEqual(notes, [])
        self.assertEqual(after, clean)

    def test_migrate_source_is_idempotent_on_its_own_output(self):
        # Foundational guarantee: feeding the codemod its own output is a no-op.
        # If this ever regresses, every re-run becomes a real edit and the
        # already_migrated heuristic is meaningless.
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import migrate_source

        v0_source = (
            "q = client.query.builder('account').filter_eq('name', 'X')\n"
            "row = client.records.get('account', 'guid-1')\n"
            "client.delete('account', 'guid-2')\n"
        )
        once = migrate_source(v0_source)
        twice = migrate_source(once)
        self.assertEqual(once, twice, "migrate_source must be idempotent on its own output")


@_skip_no_libcst
class TestHasV1Sentinels(unittest.TestCase):
    """``_has_v1_sentinels`` detects v1-exclusive forms; ignores v0-shared forms."""

    def test_detects_records_retrieve(self):
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import _has_v1_sentinels

        self.assertTrue(_has_v1_sentinels("x = client.records.retrieve('account', 'g')\n"))

    def test_detects_records_list(self):
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import _has_v1_sentinels

        self.assertTrue(_has_v1_sentinels("for r in client.records.list('account'): pass\n"))

    def test_detects_records_list_pages(self):
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import _has_v1_sentinels

        self.assertTrue(_has_v1_sentinels("for page in client.records.list_pages('account'): pass\n"))

    def test_detects_execute_pages(self):
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import _has_v1_sentinels

        self.assertTrue(_has_v1_sentinels("for page in q.execute_pages(): pass\n"))

    def test_detects_where_col_pattern(self):
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import _has_v1_sentinels

        self.assertTrue(_has_v1_sentinels("q = b.where(col('name') == 'X')\n"))

    def test_detects_tables_add_columns(self):
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import _has_v1_sentinels

        self.assertTrue(_has_v1_sentinels("client.tables.add_columns('account', [...])\n"))

    def test_does_not_match_records_create_alone(self):
        # .records.create existed in v0 alongside the now-removed client.create shortcut,
        # so its presence does not prove the codemod ran.
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import _has_v1_sentinels

        self.assertFalse(_has_v1_sentinels("client.records.create('account', {'name': 'X'})\n"))

    def test_does_not_match_records_get_alone(self):
        # .records.get(...) is deliberately not a sentinel -- it's the v0 form the codemod
        # flags as [MANUAL] but never auto-rewrites.
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import _has_v1_sentinels

        self.assertFalse(_has_v1_sentinels("x = client.records.get('account', 'g')\n"))

    def test_does_not_match_clean_code(self):
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import _has_v1_sentinels

        self.assertFalse(_has_v1_sentinels("x = 1\nprint('hi')\n"))


# ---------------------------------------------------------------------------
# Targeted note for nested-for paging over .records.get(...)
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestNestedForPagingDetection(unittest.TestCase):
    """The ``for page in records.get(...): for rec in page:`` idiom gets a targeted note.

    Under v0 ``records.get(table, kw=...)`` returns ``Iterable[List[Record]]`` so
    the nested-for-rec-in-page pattern works. Under v1 the mechanical replacement
    ``records.list(table, ...)`` returns a flat ``QueryResult`` -- the inner loop
    will iterate Record attributes / fail. The generic ``records.get()`` note
    fires for every call site; this targeted note specifically flags the nested
    loop shape with the concrete v1 rewrite alongside it.
    """

    def test_bug_repro_emits_targeted_paging_note(self):
        src = (
            'for page in client.records.get("account", select=["name"], top=100):\n'
            "    for rec in page:\n"
            '        print(rec["name"])\n'
        )
        notes = _find_manual(src)
        # The targeted note points at the outer ``for`` (line 1) and mentions
        # both v1 options the dev can pick from.
        paging_notes = [n for n in notes if "nested-for paging" in n]
        self.assertEqual(len(paging_notes), 1, f"expected one paging note, got {notes!r}")
        self.assertTrue(
            paging_notes[0].startswith("1:"),
            f"expected line=1 prefix, got {paging_notes[0]!r}",
        )
        self.assertIn("records.list(", paging_notes[0])
        self.assertIn("records.list_pages(", paging_notes[0])

    def test_targeted_note_fires_alongside_generic_records_get_note(self):
        # Both notes are valuable -- the generic one flags the call site, the
        # targeted one explains why the nested loop is the breaking shape.
        src = 'for page in client.records.get("account", top=100):\n' "    for rec in page:\n" "        pass\n"
        notes = _find_manual(src)
        self.assertTrue(any("nested-for paging" in n for n in notes))
        self.assertTrue(any("records.get() -- use retrieve()" in n for n in notes))

    def test_single_level_for_over_records_get_does_not_trigger_paging_note(self):
        # Only the existing generic records.get() note should fire here -- no
        # inner-for-over-page means no breaking iteration pattern to flag.
        src = 'for rec in client.records.get("account"):\n    print(rec)\n'
        notes = _find_manual(src)
        self.assertFalse(
            any("nested-for paging" in n for n in notes),
            f"expected no nested-for paging note, got {notes!r}",
        )

    def test_inner_for_over_different_name_does_not_trigger_paging_note(self):
        # The inner For iterates over ``batch``, not the outer ``page`` variable,
        # so it isn't the canonical paging shape -- don't flag.
        src = 'for page in client.records.get("account"):\n' "    for rec in batch:\n" "        pass\n"
        notes = _find_manual(src)
        self.assertFalse(
            any("nested-for paging" in n for n in notes),
            f"expected no nested-for paging note, got {notes!r}",
        )

    def test_nested_for_over_list_pages_does_not_trigger_paging_note(self):
        # list_pages() is the v1 paged API -- nested-for-rec-in-page is the
        # correct iteration pattern for it, so no note should fire.
        src = 'for page in client.records.list_pages("account"):\n' "    for rec in page:\n" "        pass\n"
        notes = _find_manual(src)
        self.assertFalse(
            any("nested-for paging" in n for n in notes),
            f"expected no paging note over list_pages(), got {notes!r}",
        )

    # ------------------------------------------------------------------
    # Bug raised in PR #184 review (Copilot on visit_For):
    # the detector matched any ``<x>.records.get(...)``, even when ``<x>``
    # was completely unrelated to the configured client (e.g. a third-party
    # object with its own .records attribute). The note would then advise
    # the user to switch to ``client.records.list(...)``, replacing the
    # wrong receiver name. Receiver must be the configured client_var or
    # a known alias of it.
    # ------------------------------------------------------------------

    def test_unrelated_receiver_does_not_trigger_paging_note(self):
        # database_pool is not the client, and not aliased to one -- the note
        # must not fire here, even though the .records.get + nested-for shape
        # matches structurally.
        src = (
            "database_pool = get_pool()\n"
            "for page in database_pool.records.get(table):\n"
            "    for row in page:\n"
            "        print(row)\n"
        )
        notes = _find_manual(src)
        self.assertFalse(
            any("nested-for paging" in n for n in notes),
            f"unrelated .records.get must not be flagged, got {notes!r}",
        )

    def test_alias_of_client_var_does_trigger_paging_note(self):
        # When the user has aliased the client (``my_client = client``), the
        # codemod tracks ``my_client`` in _known_client_names and the
        # nested-for over ``my_client.records.get(...)`` is real v0 paging.
        # Use ``--client-var=my_client`` so the alias's call sites match the
        # rewrite path (otherwise only the constructor/alias note fires).
        src = (
            "client = DataverseClient(url, cred)\n"
            "my_client = client\n"
            'for page in my_client.records.get("account"):\n'
            "    for rec in page:\n"
            "        print(rec)\n"
        )
        notes = _find_manual(src, client_var="my_client")
        self.assertTrue(
            any("nested-for paging" in n for n in notes),
            f"alias's nested-for must still flag, got {notes!r}",
        )


# ---------------------------------------------------------------------------
# --dry-run prints a unified diff for files that would change
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestDryRunPrintsDiff(unittest.TestCase):
    """``--dry-run`` previews the proposed edits as a unified diff.

    Before the fix the dry-run output was only ``[DRY-RUN] <path>`` with no way
    to see what the codemod planned to do without first running for real and
    relying on a backup. Now the diff prints alongside the per-file label so
    users can audit the rewrites before letting the codemod touch the source.
    """

    def _run_dry_run_and_capture_stdout(self, content: str) -> tuple:
        """Write *content* to a temp file, call ``migrate_file`` with ``dry_run=True``,
        and return ``(was_changed, on_disk_content, captured_stdout)``."""
        import contextlib
        import io
        import os
        import tempfile
        from pathlib import Path
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import migrate_file

        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False, encoding="utf-8")
        try:
            tmp.write(content)
            tmp.close()
            path = Path(tmp.name)
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                was_changed, _, _ = migrate_file(path, dry_run=True)
            on_disk = path.read_text(encoding="utf-8")
            return was_changed, on_disk, buf.getvalue()
        finally:
            os.unlink(tmp.name)

    def test_dry_run_prints_unified_diff_when_changes_would_apply(self):
        src = 'q = client.query.builder("account").filter_eq("name", "X")\n'
        was_changed, on_disk, out = self._run_dry_run_and_capture_stdout(src)
        self.assertTrue(was_changed)
        # File untouched on disk (was_changed is in-memory only under dry_run).
        self.assertEqual(on_disk, src)
        # Standard unified-diff anchors: --- / +++ file headers and at least one
        # @@ hunk header.
        self.assertIn("---", out)
        self.assertIn("+++", out)
        self.assertIn("@@", out)
        # The actual rewrite is visible in the diff: filter_eq leaves, where(col enters.
        self.assertIn("-q = client.query.builder", out)
        self.assertIn(".where(col(", out)

    def test_dry_run_diff_includes_added_import_line(self):
        # filter_eq rewriting causes ``col`` to be auto-imported. The diff must
        # show the new import line so the user can audit it.
        src = 'q = client.query.builder("account").filter_eq("name", "X")\n'
        _, _, out = self._run_dry_run_and_capture_stdout(src)
        self.assertIn("+from PowerPlatform.Dataverse.models.filters import col", out)

    def test_dry_run_emits_no_diff_when_file_would_not_change(self):
        # Pure-manual-only file (records.get is never auto-rewritten) → no diff,
        # only the [MANUAL] note path. The diff section must be empty.
        src = "row = client.records.get('account', 'abc')\n"
        was_changed, _, out = self._run_dry_run_and_capture_stdout(src)
        self.assertFalse(was_changed)
        # No diff anchors should appear.
        self.assertNotIn("@@", out)
        # Headers also absent (only call site is the diff helper, which doesn't fire).
        self.assertNotIn("(migrated)", out)

    def test_dry_run_filename_in_diff_headers_matches_input_path(self):
        # The fromfile/tofile labels in the unified diff carry the file path so
        # the user can identify which file each hunk belongs to in multi-file runs.
        import contextlib
        import io
        import os
        import tempfile
        from pathlib import Path
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import migrate_file

        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False, encoding="utf-8")
        try:
            tmp.write("client.create('account', {'name': 'X'})\n")
            tmp.close()
            path = Path(tmp.name)
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                migrate_file(path, dry_run=True)
            out = buf.getvalue()
            self.assertIn(str(path), out)
            self.assertIn(f"{path} (migrated)", out)
        finally:
            os.unlink(tmp.name)

    def test_non_dry_run_does_not_print_diff(self):
        # Sanity: a normal (non-dry-run) invocation writes the file and does not
        # print a diff -- that would be noise in CI logs.
        import contextlib
        import io
        import os
        import tempfile
        from pathlib import Path
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import migrate_file

        tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False, encoding="utf-8")
        try:
            tmp.write("client.create('account', {'name': 'X'})\n")
            tmp.close()
            path = Path(tmp.name)
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                was_changed, _, _ = migrate_file(path, dry_run=False)
            out = buf.getvalue()
            self.assertTrue(was_changed)
            self.assertNotIn("@@", out)
            self.assertNotIn("(migrated)", out)
            # And the file should be rewritten on disk now.
            self.assertIn("client.records.create(", path.read_text(encoding="utf-8"))
        finally:
            os.unlink(tmp.name)


# ---------------------------------------------------------------------------
# Line-ending fidelity: LF stays LF, CRLF stays CRLF (cross-platform regression)
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestLineEndingFidelity(unittest.TestCase):
    """The codemod preserves the source file's dominant newline style on write.

    Python's default ``path.write_text`` translates every ``\\n`` to ``os.linesep``,
    which silently CRLF-converts LF-only sources on Windows. ``git diff`` then
    shows every line as changed and the real codemod edits are buried in noise.
    Fix: detect the source's dominant newline and pass it to ``write_text``.
    """

    def _write_bytes_and_migrate(self, raw: bytes) -> bytes:
        """Write *raw* to a temp .py file, migrate in place, return the new bytes."""
        import os
        import tempfile
        from pathlib import Path
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import migrate_file

        fd, tmp_path = tempfile.mkstemp(suffix=".py")
        os.close(fd)
        try:
            Path(tmp_path).write_bytes(raw)
            migrate_file(Path(tmp_path))
            return Path(tmp_path).read_bytes()
        finally:
            os.unlink(tmp_path)

    def test_lf_only_source_stays_lf_only(self):
        # Build LF-only source with exactly 2 newlines and no CR anywhere.
        src = (
            b'q = client.query.builder("account").filter_eq("name", "X")\n' b'client.create("account", {"name": "Y"})\n'
        )
        self.assertEqual(src.count(b"\n"), 2)
        self.assertEqual(src.count(b"\r"), 0)
        out = self._write_bytes_and_migrate(src)
        # Codemod adds at least one new line (the auto-imported ``col``), so the
        # LF count may grow -- but CR count MUST stay 0.
        self.assertEqual(out.count(b"\r"), 0, f"LF source must not gain CR bytes; out={out!r}")
        self.assertGreaterEqual(out.count(b"\n"), 2)

    def test_crlf_source_stays_crlf(self):
        # Same content as the LF test but with CRLF endings.
        src = (
            b'q = client.query.builder("account").filter_eq("name", "X")\r\n'
            b'client.create("account", {"name": "Y"})\r\n'
        )
        # Every newline is a CRLF: LF count == CR count.
        self.assertEqual(src.count(b"\r\n"), 2)
        out = self._write_bytes_and_migrate(src)
        # Every newline in the output must still be CRLF: every \n preceded by \r,
        # equivalently CR count == LF count.
        self.assertEqual(
            out.count(b"\r"),
            out.count(b"\n"),
            f"CRLF source must stay CRLF; got CR={out.count(b'\\r')} LF={out.count(b'\\n')}",
        )
        # Sanity: the actual rewrite is visible.
        self.assertIn(b".where(col(", out)

    def test_dominant_newline_detection_prefers_majority_style(self):
        # Three CRLF lines, one bare LF -- dominant style is CRLF, output should
        # render all newlines as CRLF.
        src = (
            b'q = client.query.builder("account").filter_eq("name", "X")\r\n'
            b'client.create("account", {"name": "Y"})\r\n'
            b'client.update("account", "id", {"name": "Z"})\r\n'
            b"x = 1\n"
        )
        # 3 \r\n + 1 lone \n = 4 LF total, 3 CR.
        self.assertEqual(src.count(b"\r\n"), 3)
        self.assertEqual(src.count(b"\n") - src.count(b"\r\n"), 1)
        out = self._write_bytes_and_migrate(src)
        # Dominant CRLF means output should have no bare LFs (every \n preceded by \r).
        self.assertEqual(
            out.count(b"\r"),
            out.count(b"\n"),
            "dominant-CRLF source must produce all-CRLF output",
        )


# ---------------------------------------------------------------------------
# .execute() insertion preserves multi-line fluent-chain layout
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestToDataframeChainLayout(unittest.TestCase):
    """Inserting ``.execute()`` before ``.to_dataframe()`` keeps the chain's per-line layout.

    Without trivia transfer the new ``.execute()`` lands inline on the receiver's
    line ('.select(...).execute()'), destroying readable per-method-per-line
    chains. Fix copies the .to_dataframe() Attribute's dot.whitespace_before onto
    the inserted .execute() so both calls keep their own line.
    """

    def test_parenthesized_chain_preserves_per_method_per_line_layout(self):
        src = (
            "df = (\n"
            "    client.query\n"
            "    .builder('account')\n"
            "    .filter_eq('statecode', 0)\n"
            "    .select('name', 'new_completed')\n"
            "    .to_dataframe()\n"
            ")\n"
        )
        out = _migrate(src)
        # .execute() and .to_dataframe() each on their own line at the same indent.
        self.assertIn("    .execute()", out)
        self.assertIn("    .to_dataframe()", out)
        # And NOT jammed onto the preceding .select(...) line.
        self.assertNotIn(".select('name', 'new_completed').execute()", out)
        # Also not chained inline.
        self.assertNotIn(".execute().to_dataframe()", out)

    def test_inline_chain_stays_inline(self):
        # Sanity: when the source has no newline before .to_dataframe(), the
        # inserted .execute() must NOT introduce one. Output should chain inline.
        src = "df = q.select('name').to_dataframe()\n"
        out = _migrate(src)
        self.assertIn(".execute().to_dataframe()", out)
        # And no spurious newline+indent before .execute().
        self.assertNotIn("\n    .execute()", out)

    def test_line_continuation_chain_preserves_continuation(self):
        # Backslash-continued chain: .execute() inherits the receiver's trailing
        # trivia, so it sits where the original .to_dataframe() did.
        src = "df = client.query.builder('account')\\\n" "        .select('name')\\\n" "        .to_dataframe()\n"
        out = _migrate(src)
        # Both calls still present; .execute() at the original to_dataframe indent.
        self.assertIn(".execute()", out)
        self.assertIn(".to_dataframe()", out)
        # No layout-destroying inline collapse of select+execute.
        self.assertNotIn(".select('name').execute()", out)


# ---------------------------------------------------------------------------
# Import injection lands BEFORE first reference (blocker: NameError otherwise)
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestImportInjectionPlacement(unittest.TestCase):
    """Auto-injected ``from ... import col`` must precede the first reference.

    Root cause was ``last_import_idx = 0`` defaulting to "after the first
    statement" when no imports existed in the file. The new import landed on
    line 2, after the rewritten line that already referenced ``col`` --
    NameError at runtime. Fix initializes to -1 and inserts at position 0 (or 1
    if a module docstring is present) when no imports exist.
    """

    def test_no_imports_no_docstring_injected_at_top(self):
        src = "q = client.query.builder('account').filter_eq('name', 'X')\n"
        out = _migrate(src)
        lines = out.splitlines()
        import_idx = next(i for i, l in enumerate(lines) if "import col" in l)
        ref_idx = next(i for i, l in enumerate(lines) if "col(" in l)
        self.assertLess(
            import_idx,
            ref_idx,
            f"import (line {import_idx}) must precede first col() reference (line {ref_idx}): {out!r}",
        )
        # Specifically: import goes at the very top when no docstring exists.
        self.assertTrue(lines[0].startswith("from PowerPlatform.Dataverse.models.filters import col"))

    def test_migrated_file_is_py_compileable(self):
        # py_compile catches the NameError-on-import-after-reference case
        # because the source becomes a syntactically valid but semantically
        # broken file. py_compile is a parse check; the exec check below catches
        # the semantic failure.
        import os
        import py_compile
        import tempfile
        from pathlib import Path
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import migrate_file

        fd, tmp_path = tempfile.mkstemp(suffix=".py")
        os.close(fd)
        try:
            Path(tmp_path).write_text(
                "q = client.query.builder('account').filter_eq('name', 'X')\n",
                encoding="utf-8",
            )
            migrate_file(Path(tmp_path))
            # Raises py_compile.PyCompileError on syntax error.
            py_compile.compile(tmp_path, doraise=True)
        finally:
            os.unlink(tmp_path)
            # Best-effort cleanup of py_compile's __pycache__ artifact.
            pyc = Path(tmp_path).with_suffix(".pyc")
            if pyc.exists():
                os.unlink(pyc)

    def test_migrated_file_execs_without_nameerror(self):
        # The blocker symptom: ``NameError: name 'col' is not defined`` at exec.
        # exec the migrated source under a namespace that stubs out ``client``,
        # and assert no NameError. If the import lands after the reference, this
        # raises -- exactly what real users hit.
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import migrate_source

        src = "q = client.query.builder('account').filter_eq('name', 'X')\n"
        migrated = migrate_source(src)

        class _StubQuery:
            def builder(self, *a, **kw):
                return _StubBuilder()

        class _StubBuilder:
            def where(self, *a, **kw):
                return self

        class _StubClient:
            query = _StubQuery()

        # Provide ``client`` so we don't NameError on the unrelated symbol --
        # we're specifically testing for NameError on ``col``.
        ns = {"client": _StubClient()}
        try:
            exec(compile(migrated, "<migrated>", "exec"), ns)
        except NameError as exc:
            self.fail(f"migrated file raised NameError at exec: {exc}; source:\n{migrated}")

    def test_existing_import_at_top_keeps_filters_import_at_top(self):
        # Sanity: file with an unrelated existing import. The new filters import
        # should land AFTER the existing import block, not above it.
        src = "import os\n" "q = client.query.builder('account').filter_eq('name', 'X')\n"
        out = _migrate(src)
        lines = out.splitlines()
        os_idx = next(i for i, l in enumerate(lines) if l.startswith("import os"))
        filt_idx = next(i for i, l in enumerate(lines) if "import col" in l)
        ref_idx = next(i for i, l in enumerate(lines) if "col(" in l)
        self.assertLess(os_idx, filt_idx, "filters import should land after existing imports")
        self.assertLess(filt_idx, ref_idx, "filters import must still precede first col() reference")

    def test_module_docstring_preserved_above_injected_import(self):
        # A leading module docstring should stay at position 0; the new import
        # goes at position 1.
        src = '"""Module docstring."""\n' "q = client.query.builder('account').filter_eq('name', 'X')\n"
        out = _migrate(src)
        lines = out.splitlines()
        # Docstring stays first.
        self.assertEqual(lines[0], '"""Module docstring."""')
        # Import lands on line 2 (index 1).
        self.assertIn("import col", lines[1])
        # Reference still after import.
        ref_idx = next(i for i, l in enumerate(lines) if "col(" in l)
        self.assertGreater(ref_idx, 1)

    def test_chained_filters_no_imports_runs(self):
        # The 17_chained_filters.py worst case from the bug: multi-line chain
        # ending in execute(by_page=True). The codemod must still place the
        # filters import above the chain so the file is runnable.
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import migrate_source

        src = (
            "q = (\n"
            "    client.query.builder('account')\n"
            "    .filter_eq('name', 'X')\n"
            "    .filter_gt('revenue', 1000)\n"
            "    .filter_in('status', [1, 2, 3])\n"
            "    .execute(by_page=True)\n"
            ")\n"
        )
        migrated = migrate_source(src)
        lines = migrated.splitlines()
        import_idx = next(i for i, l in enumerate(lines) if "import col" in l)
        first_ref_idx = next(i for i, l in enumerate(lines) if "col(" in l)
        self.assertLess(
            import_idx,
            first_ref_idx,
            f"chained-filter file's import must precede first col() reference; got:\n{migrated}",
        )


# ---------------------------------------------------------------------------
# Orphaned filter-module imports get pruned after standalone-function rewrite
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestPruneUnusedFilterImports(unittest.TestCase):
    """Standalone-filter rewrites no longer leave dead ``from filters import eq`` lines.

    Before the fix, ``eq("col", v)`` was rewritten to ``col("col") == v`` but the
    original ``from PowerPlatform.Dataverse.models.filters import eq`` line stayed,
    producing F401 unused-import errors on every codemod run.
    """

    def test_rewritten_eq_gt_between_imports_are_pruned(self):
        src = (
            "from PowerPlatform.Dataverse.models.filters import eq, gt, between, col\n"
            "q = (\n"
            "    client.query.builder('account')\n"
            "    .where(eq('statecode', 0))\n"
            "    .where(gt('revenue', 1000))\n"
            ")\n"
        )
        out = _migrate(src)
        # The three rewritten names must NOT remain in any import line.
        import_lines = [l for l in out.splitlines() if "import" in l and "filters" in l]
        joined = "\n".join(import_lines)
        for name in ("eq", "gt", "between"):
            self.assertNotIn(
                f" {name}",
                joined,
                f"expected ``{name}`` pruned from filters import; got:\n{joined}",
            )
        # ``col`` is still referenced by the rewrite, so its import stays.
        self.assertIn("col", joined)

    def test_lone_unused_eq_import_statement_is_removed(self):
        # When the only thing imported from filters is rewritten away, the
        # whole import statement should disappear -- not just the alias.
        src = "from PowerPlatform.Dataverse.models.filters import eq\n" "f = eq('name', 'X')\n"
        out = _migrate(src)
        # Original eq import line must be gone.
        self.assertNotIn("import eq", out)
        # The rewrite itself is still present.
        self.assertIn("(col('name') == 'X')", out)
        # And ``col`` got auto-imported.
        self.assertIn("import col", out)

    def test_eq_still_referenced_elsewhere_keeps_its_import(self):
        # If ``eq`` is also assigned to a variable (not just called), the
        # rewrite of the direct call doesn't make ``eq`` unused. The pruner
        # must keep the import.
        src = "from PowerPlatform.Dataverse.models.filters import eq\n" "alias = eq\n" "f = eq('name', 'X')\n"
        out = _migrate(src)
        self.assertIn("import eq", out)

    def test_unrelated_imports_are_not_touched(self):
        # The pruner only touches filters-module imports. Standard-library
        # unused imports stay (that's outside this codemod's scope).
        src = "import os\n" "from PowerPlatform.Dataverse.models.filters import eq\n" "f = eq('name', 'X')\n"
        out = _migrate(src)
        # ``os`` import remains untouched even though it was never used.
        self.assertIn("import os", out)
        # ``eq`` import is gone.
        self.assertNotIn("import eq", out)

    def test_existing_col_import_preserved_through_pruner(self):
        # When the user already imported ``col`` explicitly, the pruner must
        # not strip it just because the rewrite of ``eq`` injected another col
        # reference. ``col`` is still genuinely used.
        src = (
            "from PowerPlatform.Dataverse.models.filters import eq, col\n"
            "q = b.where(eq('a', 1)).where(col('b') == 2)\n"
        )
        out = _migrate(src)
        self.assertIn("col", out)
        self.assertNotIn(" eq", out.split("\n", 1)[0] if "filters" in out.split("\n", 1)[0] else "")
        # And ``col`` is still referenced in the body of the file.
        self.assertIn("col(", out)

    def test_no_new_f401_after_migration(self):
        # End-to-end: take a file that ``ruff --select F401`` finds clean, run
        # the codemod, parse the output with ``ast``, and assert that every
        # local binding introduced by an ImportFrom from the filters module is
        # actually referenced somewhere outside the import.
        import ast
        from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import migrate_source, _FILTERS_MODULE

        src = (
            "from PowerPlatform.Dataverse.models.filters import eq, gt, between, col\n"
            "q = (\n"
            "    client.query.builder('account')\n"
            "    .where(eq('statecode', 0))\n"
            "    .where(gt('revenue', 1000))\n"
            ")\n"
        )
        out = migrate_source(src)
        tree = ast.parse(out)

        # Collect every local binding name introduced by ``from FILTERS import ...``.
        filters_bindings = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module == _FILTERS_MODULE:
                for alias in node.names:
                    filters_bindings.add(alias.asname or alias.name)

        # Collect every Name referenced outside ImportFrom nodes.
        referenced = set()

        class _NameVisitor(ast.NodeVisitor):
            def visit_ImportFrom(self, node):
                return  # skip names *inside* imports

            def visit_Import(self, node):
                return

            def visit_Name(self, node):
                referenced.add(node.id)

        _NameVisitor().visit(tree)

        unused = filters_bindings - referenced
        self.assertEqual(
            unused,
            set(),
            f"new F401 leftovers from filters module: {sorted(unused)}; migrated:\n{out}",
        )


# ---------------------------------------------------------------------------
# Aliased / renamed client detection (--client-var foot-gun)
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestAliasedClientDetection(unittest.TestCase):
    """Direct ``<var> = DataverseClient(...)`` to a non-default name emits a [MANUAL] note.

    Without the warning the user has no idea ``cl.create()`` / ``svc.update()``
    weren't migrated -- they only find out at runtime when the v0 shortcut is
    gone. Note tells them which ``--client-var`` value to re-run with.
    """

    def test_bug_repro_cl_assignment_emits_note(self):
        src = (
            "client = DataverseClient(url, cred)\n"
            "my_client = client\n"
            "my_client.create('account', {'name': 'X'})\n"
            "cl = DataverseClient(url, cred)\n"
            "cl.create('account', {'name': 'X'})\n"
        )
        notes = _find_manual(src)
        # Direct ``cl = DataverseClient(...)`` must be flagged.
        cl_notes = [n for n in notes if "cl = DataverseClient" in n]
        self.assertEqual(len(cl_notes), 1, f"expected one cl note, got {notes!r}")
        self.assertIn("--client-var=cl", cl_notes[0])
        # And the note must carry a line:col prefix.
        self.assertTrue(cl_notes[0].split(":", 2)[0].isdigit())

    def test_default_client_name_assignment_does_not_emit_note(self):
        # ``client = DataverseClient(...)`` matches the default --client-var, so
        # the codemod can rewrite the calls -- no warning needed.
        src = "client = DataverseClient(url, cred)\n"
        notes = _find_manual(src)
        self.assertFalse(
            any("DataverseClient(...)" in n for n in notes),
            f"unexpected note for default-named client: {notes!r}",
        )

    def test_custom_client_var_suppresses_note_for_matching_name(self):
        # If the user explicitly runs --client-var=svc, ``svc = DataverseClient(...)``
        # is no longer the foot-gun case.
        src = "svc = DataverseClient(url, cred)\nsvc.create('account', {'name': 'X'})\n"
        notes = _find_manual(src, client_var="svc")
        self.assertFalse(
            any("DataverseClient(...)" in n for n in notes),
            f"unexpected note when --client-var matches: {notes!r}",
        )

    def test_annotated_assignment_is_flagged(self):
        # ``cl: DataverseClient = DataverseClient(...)`` -- AnnAssign form.
        src = "cl: DataverseClient = DataverseClient(url, cred)\n"
        notes = _find_manual(src)
        self.assertTrue(
            any("cl = DataverseClient" in n and "--client-var=cl" in n for n in notes),
            f"expected annotated-assignment to flag, got {notes!r}",
        )

    def test_dotted_constructor_form_is_flagged(self):
        # ``cl = PowerPlatform.Dataverse.client.DataverseClient(...)`` -- the
        # codemod looks at the right-most attribute name, not the bare Name.
        src = "cl = PowerPlatform.Dataverse.client.DataverseClient(url, cred)\n"
        notes = _find_manual(src)
        self.assertTrue(
            any("cl = DataverseClient" in n for n in notes),
            f"expected dotted-constructor to flag, got {notes!r}",
        )

    def test_unrelated_assignment_does_not_emit_note(self):
        # ``data = SomethingElse(...)`` must not be flagged.
        src = "data = SomeOtherClass(url, cred)\n"
        notes = _find_manual(src)
        self.assertFalse(
            any("DataverseClient" in n for n in notes),
            f"unexpected note for unrelated assignment: {notes!r}",
        )

    def test_multi_target_assignment_skipped(self):
        # ``a = b = DataverseClient(...)`` is ambiguous re: which name should
        # be the client_var -- skip silently rather than flag both.
        src = "a = b = DataverseClient(url, cred)\n"
        notes = _find_manual(src)
        self.assertFalse(
            any("DataverseClient" in n for n in notes),
            f"unexpected note for multi-target assign: {notes!r}",
        )

    def test_note_carries_line_col_prefix(self):
        # Sanity check on the location annotation -- the note for ``cl`` should
        # point at line 3 (1-based).
        src = "# header\n" "x = 1\n" "cl = DataverseClient(url, cred)\n"
        notes = _find_manual(src)
        cl_notes = [n for n in notes if "cl = DataverseClient" in n]
        self.assertEqual(len(cl_notes), 1)
        self.assertTrue(cl_notes[0].startswith("3:"), f"expected line=3, got {cl_notes[0]!r}")

    def test_name_alias_of_client_var_emits_note(self):
        # ``my_client = client`` -- the original bug-report repro line. The RHS
        # is a Name (not a Call), so the constructor-only detector missed it
        # and ``my_client.create(...)`` silently went unmigrated.
        src = (
            "client = DataverseClient(url, cred)\n"
            "my_client = client\n"
            "my_client.create('account', {'name': 'X'})\n"
        )
        notes = _find_manual(src)
        alias_notes = [n for n in notes if "my_client = client" in n]
        self.assertEqual(len(alias_notes), 1, f"expected one alias note, got {notes!r}")
        self.assertIn("aliases a Dataverse client", alias_notes[0])
        self.assertIn("--client-var=my_client", alias_notes[0])

    def test_name_alias_chained_transitively(self):
        # ``dv = my_client`` where ``my_client`` is itself an alias of ``client``.
        # Both alias hops must be flagged so the full chain is visible.
        src = (
            "client = DataverseClient(url, cred)\n"
            "my_client = client\n"
            "dv = my_client\n"
            "dv.create('account', {'name': 'X'})\n"
        )
        notes = _find_manual(src)
        self.assertTrue(
            any("my_client = client" in n for n in notes),
            f"expected first-hop alias note, got {notes!r}",
        )
        self.assertTrue(
            any("dv = my_client" in n and "--client-var=dv" in n for n in notes),
            f"expected chained alias note, got {notes!r}",
        )

    def test_name_alias_of_non_default_constructor_is_flagged(self):
        # ``cl = DataverseClient(...)`` already warns; a subsequent ``dv = cl``
        # alias of that same non-default client must also warn so chained
        # call sites aren't silently missed.
        src = "cl = DataverseClient(url, cred)\n" "dv = cl\n" "dv.create('account', {'name': 'X'})\n"
        notes = _find_manual(src)
        self.assertTrue(
            any("dv = cl" in n and "--client-var=dv" in n for n in notes),
            f"expected alias-of-non-default note, got {notes!r}",
        )

    def test_name_alias_to_unrelated_name_does_not_emit_note(self):
        # ``data = something_else`` -- RHS is a Name but is not a known client,
        # so no note should be emitted.
        src = "client = DataverseClient(url, cred)\n" "something_else = 42\n" "data = something_else\n"
        notes = _find_manual(src)
        self.assertFalse(
            any("data = " in n for n in notes),
            f"unexpected note for unrelated Name alias: {notes!r}",
        )

    def test_alias_matching_client_var_suppressed(self):
        # If the user runs ``--client-var=dv`` and the file does ``dv = client``,
        # the alias target itself matches client_var -- no warning needed for
        # the assignment line (``dv.create(...)`` will be rewritten directly).
        src = "client = DataverseClient(url, cred)\ndv = client\ndv.create('a', {})\n"
        notes = _find_manual(src, client_var="dv")
        self.assertFalse(
            any(" = client" in n and "aliases a Dataverse client" in n for n in notes),
            f"unexpected alias note when target matches --client-var: {notes!r}",
        )

    def test_annotated_alias_assignment_is_flagged(self):
        # AnnAssign form: ``my_client: DataverseClient = client``.
        src = "client = DataverseClient(url, cred)\n" "my_client: DataverseClient = client\n"
        notes = _find_manual(src)
        self.assertTrue(
            any("my_client = client" in n and "aliases a Dataverse client" in n for n in notes),
            f"expected annotated alias to flag, got {notes!r}",
        )


# ---------------------------------------------------------------------------
# Multi-line import layout survives both augmentation and pruning (B1 fix)
# ---------------------------------------------------------------------------


@_skip_no_libcst
class TestImportLayoutPreservation(unittest.TestCase):
    """Multi-line ``from filters import (...)`` blocks keep their layout through both
    the V1Migrator's auto-augmentation (adding ``col``/``raw``) and the pruner's
    removal of unused names. Previously both code paths called ``_comma_separated``
    which rebuilt every alias's comma with a single space, silently collapsing a
    multi-line import block onto one long line.
    """

    def test_multiline_import_augmented_with_col_stays_multiline(self):
        src = (
            "from PowerPlatform.Dataverse.models.filters import (\n"
            "    between,\n"
            "    raw,\n"
            ")\n"
            "q = client.query.builder('account').filter_eq('name', 'X')\n"
        )
        out = _migrate(src)
        # ``col`` was added by the V1Migrator -- and must land on its own line at
        # the original indent, not be jammed onto the existing line.
        self.assertIn("    col", out)
        # The single-line collapse symptom: every name on one line.
        self.assertNotIn("between, raw, col", out)
        self.assertNotIn("between, raw,col", out)

    def test_single_alias_multiline_import_augmented_stays_multiline(self):
        # Bug raised in PR #184 review (Copilot comment on _append_aliases_preserving_layout):
        # a single-alias parenthesized multi-line import was flattened to one line
        # because the separator template was only derived from existing[0].comma
        # when len(existing) >= 2. The trailing comma of the only alias carries
        # the newline trivia and must be promoted to the separator template.
        src = (
            "from PowerPlatform.Dataverse.models.filters import (\n"
            "    raw,\n"
            ")\n"
            'rule = raw("a eq 1")\n'
            "q = client.query.builder('account').filter_eq('name', 'X')\n"
        )
        out = _migrate(src)
        # ``col`` should be on its own line at the same 4-space indent as ``raw``.
        self.assertIn("    raw,", out)
        self.assertIn("    col,", out)
        # Symptom of the bug: both names jammed onto one line.
        self.assertNotIn("raw, col,", out)
        self.assertNotIn("raw,col,", out)
        # And the closing paren must still appear on its own line.
        self.assertIn("\n)", out)

    def test_multiline_import_pruned_stays_multiline(self):
        # Original imports include unused names; the codemod removes them but the
        # surviving aliases must keep their per-line layout.
        src = (
            "from PowerPlatform.Dataverse.models.filters import (\n"
            "    eq,\n"
            "    gt,\n"
            "    col,\n"
            ")\n"
            "q = b.where(eq('name', 'X')).where(col('y') == 1)\n"
        )
        out = _migrate(src)
        # ``gt`` and ``eq`` are pruned; ``col`` remains and must still be on its
        # own line under the opening parenthesis.
        self.assertIn("    col", out)
        # If the pruner had collapsed the layout we'd see ``col)`` directly.
        self.assertNotIn("import (col)", out)
        self.assertNotIn("import ( col )", out)

    def test_inline_import_augmented_stays_inline(self):
        # Inline imports don't have multi-line trivia -- they should also keep
        # their single-line shape (with single-space separators, no newlines).
        # Use ``raw`` because the codemod doesn't rewrite standalone raw(...)
        # calls, so it stays referenced after migration.
        src = (
            "from PowerPlatform.Dataverse.models.filters import raw\n"
            "rule = raw('a eq 1')\n"
            "q = client.query.builder('account').filter_eq('name', 'X')\n"
        )
        out = _migrate(src)
        filt_line = next(l for l in out.splitlines() if "filters import" in l)
        # Should be a single line with both names, comma-space separated.
        self.assertIn("raw", filt_line)
        self.assertIn("col", filt_line)
        self.assertNotIn("\n", filt_line)

    def test_inline_import_pruned_stays_inline(self):
        src = (
            "from PowerPlatform.Dataverse.models.filters import eq, col\n"
            "q = b.where(eq('a', 1)).where(col('b') == 2)\n"
        )
        out = _migrate(src)
        # ``eq`` is pruned; ``col`` remains. Output stays inline.
        filt_lines = [l for l in out.splitlines() if "filters import" in l]
        self.assertEqual(len(filt_lines), 1)
        self.assertIn("col", filt_lines[0])
        self.assertNotIn("eq", filt_lines[0])


if __name__ == "__main__":
    unittest.main()
