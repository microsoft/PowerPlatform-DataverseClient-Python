# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Phase 2 GA regression tests.

Covers:
- QueryResult class (models/record.py)
- execute() returns QueryResult in flat mode
- execute(by_page=True) still returns Iterable[list[Record]]
- col, raw, QueryResult re-exports from models/__init__ and package root
- pyproject.toml migration optional dep
"""

import unittest
import warnings
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.models.record import QueryResult, Record
from PowerPlatform.Dataverse.models.query_builder import QueryBuilder


def _make_client():
    cred = MagicMock(spec=TokenCredential)
    from PowerPlatform.Dataverse.client import DataverseClient

    client = DataverseClient("https://example.crm.dynamics.com", cred)
    client._odata = MagicMock()
    client._odata._get_multiple = MagicMock()
    client._odata._get_single = MagicMock()
    return client


class TestQueryResultClass(unittest.TestCase):
    """Unit tests for QueryResult."""

    def _records(self, n=3):
        return [Record(id=f"id-{i}", table="account", data={"name": f"R{i}"}) for i in range(n)]

    # ----- construction / dunder

    def test_init_stores_records(self):
        recs = self._records(2)
        qr = QueryResult(recs)
        self.assertIs(qr.records, recs)

    def test_empty_result(self):
        qr = QueryResult([])
        self.assertEqual(len(qr), 0)
        self.assertFalse(bool(qr))
        self.assertIsNone(qr.first())

    def test_len(self):
        self.assertEqual(len(QueryResult(self._records(5))), 5)

    def test_bool_true_when_nonempty(self):
        self.assertTrue(bool(QueryResult(self._records(1))))

    def test_bool_false_when_empty(self):
        self.assertFalse(bool(QueryResult([])))

    def test_iter_yields_records(self):
        recs = self._records(3)
        qr = QueryResult(recs)
        self.assertEqual(list(qr), recs)

    def test_iter_multiple_times(self):
        recs = self._records(2)
        qr = QueryResult(recs)
        self.assertEqual(list(qr), list(qr))

    def test_repr_contains_count(self):
        qr = QueryResult(self._records(7))
        self.assertIn("7", repr(qr))

    # ----- first()

    def test_first_returns_first_record(self):
        recs = self._records(3)
        qr = QueryResult(recs)
        self.assertIs(qr.first(), recs[0])

    def test_first_returns_none_when_empty(self):
        self.assertIsNone(QueryResult([]).first())

    # ----- __getitem__

    def test_getitem_int_returns_record(self):
        recs = self._records(3)
        qr = QueryResult(recs)
        self.assertIs(qr[0], recs[0])
        self.assertIs(qr[2], recs[2])

    def test_getitem_negative_index(self):
        recs = self._records(3)
        qr = QueryResult(recs)
        self.assertIs(qr[-1], recs[-1])

    def test_getitem_out_of_range_raises(self):
        qr = QueryResult(self._records(2))
        with self.assertRaises(IndexError):
            _ = qr[99]

    def test_getitem_slice_returns_query_result(self):
        recs = self._records(5)
        qr = QueryResult(recs)
        sliced = qr[1:3]
        self.assertIsInstance(sliced, QueryResult)
        self.assertEqual(list(sliced), recs[1:3])

    def test_getitem_slice_empty(self):
        qr = QueryResult(self._records(3))
        sliced = qr[10:]
        self.assertIsInstance(sliced, QueryResult)
        self.assertEqual(len(sliced), 0)

    # ----- to_dataframe()

    def test_to_dataframe_nonempty(self):
        import pandas as pd

        recs = [
            Record(id="id-1", table="account", data={"name": "Contoso", "revenue": 1000}),
            Record(id="id-2", table="account", data={"name": "Fabrikam", "revenue": 2000}),
        ]
        qr = QueryResult(recs)
        df = qr.to_dataframe()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertIn("name", df.columns)
        self.assertIn("revenue", df.columns)
        self.assertEqual(df.iloc[0]["name"], "Contoso")
        self.assertEqual(df.iloc[1]["revenue"], 2000)

    def test_to_dataframe_empty_returns_empty_df(self):
        import pandas as pd

        df = QueryResult([]).to_dataframe()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 0)

    def test_to_dataframe_handles_plain_dicts(self):
        """QueryResult.to_dataframe() handles plain dicts (no .data attribute)."""
        import pandas as pd

        qr = QueryResult([{"name": "A"}, {"name": "B"}])  # type: ignore[arg-type]
        df = qr.to_dataframe()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)

    # ----- for r in result (backward compat)

    def test_backward_compat_for_loop(self):
        recs = self._records(3)
        qr = QueryResult(recs)
        collected = []
        for r in qr:
            collected.append(r)
        self.assertEqual(collected, recs)

    def test_list_conversion(self):
        recs = self._records(4)
        qr = QueryResult(recs)
        self.assertEqual(list(qr), recs)


class TestQueryResultListLikeContract(unittest.TestCase):
    """Contract tests: QueryResult must be substitutable for ``list[Record]``
    in the patterns shown in the class docstring, examples, and skill docs.

    These tests exist to catch the gap from PR #175 where ``__getitem__`` was
    missing despite docs and examples treating ``QueryResult`` as list-like.
    They drive the contract from the *caller's* perspective rather than
    introspecting which dunders are implemented, so removing any single dunder
    breaks at least one assertion here with a clear signal.
    """

    def _records(self, n=3):
        return [Record(id=f"id-{i}", table="account", data={"name": f"R{i}"}) for i in range(n)]

    def test_contract_index_then_field_access(self):
        """Pattern from examples/advanced/fetchxml.py: ``row = result[0]; row.get(...)``."""
        qr = QueryResult(self._records(3))
        row = qr[0]
        self.assertEqual(row.get("name"), "R0")

    def test_contract_single_loop_field_access(self):
        """Pattern from examples/basic/installation_example.py: ``for r in result: r["name"]``."""
        qr = QueryResult(self._records(3))
        names = [r["name"] for r in qr]
        self.assertEqual(names, ["R0", "R1", "R2"])

    def test_contract_first_with_none_guard(self):
        """Pattern recommended by Copilot review: ``result.first()`` with None-check."""
        empty = QueryResult([])
        nonempty = QueryResult(self._records(2))
        self.assertIsNone(empty.first())
        first = nonempty.first()
        self.assertIsNotNone(first)
        self.assertEqual(first.get("name"), "R0")  # type: ignore[union-attr]

    def test_contract_truthy_guard(self):
        """Pattern: ``if result: ...`` to skip empty results before indexing."""
        if QueryResult(self._records(1)):
            ok = True
        else:
            ok = False
        self.assertTrue(ok)
        self.assertFalse(bool(QueryResult([])))

    def test_contract_len_for_size_check(self):
        """Pattern: ``f"{len(result)} rows"`` in log/print statements."""
        self.assertEqual(len(QueryResult(self._records(7))), 7)
        self.assertEqual(len(QueryResult([])), 0)

    def test_contract_slice_returns_list_like(self):
        """Slicing must yield something that supports iteration and len()."""
        qr = QueryResult(self._records(5))
        page = qr[1:4]
        self.assertEqual(len(page), 3)
        self.assertEqual([r.get("name") for r in page], ["R1", "R2", "R3"])

    def test_contract_negative_index(self):
        """``result[-1]`` for "last record" is a common Python idiom."""
        qr = QueryResult(self._records(3))
        self.assertEqual(qr[-1].get("name"), "R2")

    def test_contract_list_conversion_round_trip(self):
        """``list(result)`` must yield the same records iteration yields."""
        recs = self._records(4)
        qr = QueryResult(recs)
        self.assertEqual(list(qr), recs)
        self.assertEqual(list(qr), [r for r in qr])

    def test_contract_iteration_does_not_consume(self):
        """Multiple ``for`` loops over the same result must all see records.

        Guards against an accidental refactor to a single-shot iterator.
        """
        qr = QueryResult(self._records(3))
        first_pass = list(qr)
        second_pass = list(qr)
        self.assertEqual(first_pass, second_pass)
        self.assertEqual(len(first_pass), 3)

    def test_contract_nested_loop_iterates_records_not_fields(self):
        """Regression guard for the bug in installation_example.py (PR #175 review #3).

        ``for page in result: for r in page: ...`` would iterate Record keys
        if QueryResult were a flat iterable of Records (each Record is also
        iterable over its keys). This test makes it explicit that the outer
        loop yields Records, not pages — so callers know to use a single loop.
        """
        qr = QueryResult(self._records(2))
        outer = list(qr)
        self.assertTrue(all(isinstance(r, Record) for r in outer))

    def test_contract_records_attribute_is_underlying_list(self):
        """``result.records`` is the documented escape hatch for list-only APIs."""
        recs = self._records(3)
        qr = QueryResult(recs)
        self.assertIsInstance(qr.records, list)
        self.assertIs(qr.records, recs)


class TestExecuteReturnsQueryResult(unittest.TestCase):
    """execute() flat mode returns QueryResult."""

    def setUp(self):
        self.client = _make_client()

    def test_execute_flat_returns_query_result(self):
        self.client._odata._get_multiple.return_value = iter(
            [
                [{"name": "A", "accountid": "1"}],
                [{"name": "B", "accountid": "2"}],
            ]
        )
        result = self.client.query.builder("account").select("name").execute()
        self.assertIsInstance(result, QueryResult)

    def test_execute_flat_collects_all_pages(self):
        self.client._odata._get_multiple.return_value = iter(
            [
                [{"name": "A", "accountid": "1"}],
                [{"name": "B", "accountid": "2"}, {"name": "C", "accountid": "3"}],
            ]
        )
        result = self.client.query.builder("account").select("name").execute()
        self.assertEqual(len(result), 3)

    def test_execute_flat_records_accessible(self):
        self.client._odata._get_multiple.return_value = iter(
            [
                [{"name": "Contoso", "accountid": "abc"}],
            ]
        )
        result = self.client.query.builder("account").select("name").execute()
        first = result.first()
        self.assertIsNotNone(first)
        self.assertEqual(first["name"], "Contoso")

    def test_execute_flat_for_loop_backward_compat(self):
        """for r in execute() still works — backward-compatible iteration."""
        self.client._odata._get_multiple.return_value = iter(
            [
                [{"name": "A", "accountid": "1"}, {"name": "B", "accountid": "2"}],
            ]
        )
        records = []
        for r in self.client.query.builder("account").select("name").execute():
            records.append(r)
        self.assertEqual(len(records), 2)

    def test_execute_flat_list_backward_compat(self):
        """list(execute()) still works."""
        self.client._odata._get_multiple.return_value = iter(
            [
                [{"name": "X", "accountid": "x"}],
            ]
        )
        records = list(self.client.query.builder("account").select("name").execute())
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["name"], "X")

    def test_execute_by_page_not_query_result(self):
        """execute(by_page=True) still returns page iterator, not QueryResult."""
        self.client._odata._get_multiple.return_value = iter(
            [
                [{"name": "A", "accountid": "1"}],
                [{"name": "B", "accountid": "2"}],
            ]
        )
        result = self.client.query.builder("account").select("name").execute(by_page=True)
        self.assertNotIsInstance(result, QueryResult)
        pages = list(result)
        self.assertEqual(len(pages), 2)

    def test_execute_empty_returns_empty_query_result(self):
        self.client._odata._get_multiple.return_value = iter([])
        result = self.client.query.builder("account").select("name").execute()
        self.assertIsInstance(result, QueryResult)
        self.assertEqual(len(result), 0)
        self.assertFalse(bool(result))
        self.assertIsNone(result.first())

    def test_execute_result_to_dataframe(self):
        """execute().to_dataframe() works end-to-end."""
        import pandas as pd

        self.client._odata._get_multiple.return_value = iter(
            [
                [{"name": "Contoso", "accountid": "1"}, {"name": "Fabrikam", "accountid": "2"}],
            ]
        )
        df = self.client.query.builder("account").select("name").execute().to_dataframe()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)

    def test_execute_no_deprecation_warnings(self):
        """execute() flat mode emits no DeprecationWarnings."""
        from PowerPlatform.Dataverse.models.filters import col

        self.client._odata._get_multiple.return_value = iter([])
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            (self.client.query.builder("account").select("name").where(col("statecode") == 0).execute())
        dep = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(dep, [], f"Unexpected warnings: {dep}")


class TestQueryBuilderToDataframe(unittest.TestCase):
    """to_dataframe() delegates to QueryResult.to_dataframe() after execute()."""

    def setUp(self):
        self.client = _make_client()

    def test_to_dataframe_returns_dataframe(self):
        import pandas as pd

        self.client._odata._get_multiple.return_value = iter(
            [
                [{"name": "A", "accountid": "1"}],
            ]
        )
        df = self.client.query.builder("account").select("name").to_dataframe()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)

    def test_to_dataframe_empty_preserves_select_columns(self):
        """to_dataframe() on empty result keeps column names from select()."""
        import pandas as pd

        self.client._odata._get_multiple.return_value = iter([])
        df = self.client.query.builder("account").select("name", "revenue").to_dataframe()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 0)
        self.assertListEqual(list(df.columns), ["name", "revenue"])

    def test_to_dataframe_empty_no_select(self):
        """to_dataframe() on empty result with no select() returns bare empty DataFrame."""
        import pandas as pd

        self.client._odata._get_multiple.return_value = iter([])
        df = self.client.query.builder("account").top(10).to_dataframe()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 0)


class TestExports(unittest.TestCase):
    """col, raw, QueryResult are importable from models and package root."""

    def test_col_importable_from_models(self):
        from PowerPlatform.Dataverse.models import col

        self.assertIsNotNone(col)

    def test_raw_importable_from_models(self):
        from PowerPlatform.Dataverse.models import raw

        self.assertIsNotNone(raw)

    def test_query_result_importable_from_models(self):
        from PowerPlatform.Dataverse.models import QueryResult

        self.assertIsNotNone(QueryResult)

    def test_col_importable_from_package_root(self):
        from PowerPlatform.Dataverse import col

        self.assertIsNotNone(col)

    def test_raw_importable_from_package_root(self):
        from PowerPlatform.Dataverse import raw

        self.assertIsNotNone(raw)

    def test_query_result_importable_from_package_root(self):
        from PowerPlatform.Dataverse import QueryResult

        self.assertIsNotNone(QueryResult)

    def test_col_from_root_produces_filter_expression(self):
        from PowerPlatform.Dataverse import col as root_col
        from PowerPlatform.Dataverse.models.filters import FilterExpression

        expr = root_col("statecode") == 0
        self.assertIsInstance(expr, FilterExpression)

    def test_raw_from_root_produces_filter_expression(self):
        from PowerPlatform.Dataverse import raw as root_raw
        from PowerPlatform.Dataverse.models.filters import FilterExpression

        expr = root_raw("statecode eq 0")
        self.assertIsInstance(expr, FilterExpression)

    def test_query_result_from_root_is_correct_class(self):
        from PowerPlatform.Dataverse import QueryResult as root_qr

        qr = root_qr([])
        self.assertIsInstance(qr, root_qr)
        self.assertEqual(len(qr), 0)

    def test_col_from_package_root_no_warning(self):
        """Importing col from package root fires no DeprecationWarning."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            from PowerPlatform.Dataverse import col  # noqa: F401
        dep = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(dep, [], f"Unexpected warnings: {dep}")

    def test_col_call_no_warning(self):
        """col() emits no DeprecationWarning."""
        from PowerPlatform.Dataverse import col

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = col("statecode") == 0
        dep = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(dep, [], f"Unexpected warnings: {dep}")
        self.assertIsNotNone(result)

    def test_raw_call_no_warning(self):
        """raw() emits no DeprecationWarning."""
        from PowerPlatform.Dataverse import raw

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = raw("statecode eq 0")
        dep = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(dep, [], f"Unexpected warnings: {dep}")
        self.assertIsNotNone(result)


class TestQueryResultAcceptanceCriteria(unittest.TestCase):
    """Verify all QueryResult acceptance criteria from the spec."""

    def setUp(self):
        self.client = _make_client()
        self.client._odata._get_multiple.return_value = iter(
            [
                [{"name": "Contoso", "accountid": "id-1"}],
            ]
        )

    def _execute(self):
        return self.client.query.builder("account").select("name").execute()

    def test_result_is_query_result(self):
        result = self._execute()
        self.assertIsInstance(result, QueryResult)

    def test_for_loop_still_works(self):
        result = self._execute()
        records = [r for r in result]
        self.assertEqual(len(records), 1)

    def test_first_returns_record_or_none(self):
        result = self._execute()
        r = result.first()
        self.assertIsNotNone(r)
        self.assertIsInstance(r, Record)

    def test_to_dataframe_returns_dataframe(self):
        import pandas as pd

        result = self._execute()
        df = result.to_dataframe()
        self.assertIsInstance(df, pd.DataFrame)

    def test_builder_to_dataframe_returns_dataframe(self):
        import pandas as pd

        self.client._odata._get_multiple.return_value = iter(
            [
                [{"name": "Contoso", "accountid": "id-1"}],
            ]
        )
        df = self.client.query.builder("account").select("name").to_dataframe()
        self.assertIsInstance(df, pd.DataFrame)


if __name__ == "__main__":
    unittest.main()
