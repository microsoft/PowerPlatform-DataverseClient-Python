# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Comprehensive Phase 1 GA regression tests.

Verifies all Phase 1 breaking changes and deprecations in one place:

1. All 12 deprecated client flat methods raise AttributeError (removed).
2. All 15 deprecated filter factory functions emit DeprecationWarning on CALL
   (not on import).
3. GA filter functions col() and raw() emit NO deprecation warning.
4. dataframe.get() emits DeprecationWarning on call.
5. All deprecated factories remain functional (correct OData output).
6. All 16 filter_* builder methods raise AttributeError on QueryBuilder (removed).
7. No DeprecationWarning is emitted at module import time.
"""

import unittest
import warnings
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.models.query_builder import QueryBuilder

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client():
    """Create a DataverseClient with a mock credential and mock _odata."""
    credential = MagicMock(spec=TokenCredential)
    client = DataverseClient("https://example.crm.dynamics.com", credential)
    client._odata = MagicMock()
    return client


def _catch_warnings(*categories):
    """Context manager: catch all warnings, return the list."""
    return warnings.catch_warnings(record=True)


# ---------------------------------------------------------------------------
# 1. Removed client flat methods raise AttributeError
# ---------------------------------------------------------------------------


class TestRemovedClientFlatMethods(unittest.TestCase):
    """All 12 formerly-deprecated client flat methods are removed in GA."""

    def setUp(self):
        self.client = _make_client()

    def _assert_removed(self, method_name, *args, **kwargs):
        with self.assertRaises(AttributeError, msg=f"client.{method_name} should not exist"):
            getattr(self.client, method_name)(*args, **kwargs)

    def test_create_removed(self):
        self._assert_removed("create", "account", {"name": "Test"})

    def test_update_removed(self):
        self._assert_removed("update", "account", "guid-1", {"name": "Test"})

    def test_delete_removed(self):
        self._assert_removed("delete", "account", "guid-1")

    def test_get_removed(self):
        self._assert_removed("get", "account", "guid-1")

    def test_query_sql_removed(self):
        self._assert_removed("query_sql", "SELECT name FROM account")

    def test_get_table_info_removed(self):
        self._assert_removed("get_table_info", "account")

    def test_create_table_removed(self):
        self._assert_removed("create_table", "new_Test", {})

    def test_delete_table_removed(self):
        self._assert_removed("delete_table", "new_Test")

    def test_list_tables_removed(self):
        self._assert_removed("list_tables")

    def test_create_columns_removed(self):
        self._assert_removed("create_columns", "account", {})

    def test_delete_columns_removed(self):
        self._assert_removed("delete_columns", "account", [])

    def test_upload_file_removed(self):
        self._assert_removed("upload_file", "account", "guid-1", "col", "/path")


# ---------------------------------------------------------------------------
# 2. Deprecated filter factories emit DeprecationWarning on CALL (not import)
# ---------------------------------------------------------------------------


class TestDeprecatedFilterFactoriesWarnOnCall(unittest.TestCase):
    """All 15 deprecated filter factories emit DeprecationWarning when called.

    The warning must fire on CALL, not on import — importing the module must
    be warning-free.
    """

    def _assert_warns_on_call(self, func, *args, **kwargs):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = func(*args, **kwargs)
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertGreater(len(dep_warnings), 0, f"{func.__name__}() did not emit DeprecationWarning")
        return result, dep_warnings

    def _assert_single_warning(self, func, *args, **kwargs):
        """Verify exactly one DeprecationWarning is emitted (no chained warnings)."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            func(*args, **kwargs)
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(
            len(dep_warnings), 1, f"{func.__name__}() emitted {len(dep_warnings)} warnings (expected exactly 1)"
        )

    def test_no_warning_on_import(self):
        """Importing the filters module emits no DeprecationWarning."""
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            import importlib
            import PowerPlatform.Dataverse.models.filters as _f

            importlib.reload(_f)
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(dep_warnings), 0, "Import must not emit DeprecationWarning")

    def test_eq_warns(self):
        from PowerPlatform.Dataverse.models.filters import eq

        self._assert_warns_on_call(eq, "name", "Contoso")

    def test_ne_warns(self):
        from PowerPlatform.Dataverse.models.filters import ne

        self._assert_warns_on_call(ne, "statecode", 1)

    def test_gt_warns(self):
        from PowerPlatform.Dataverse.models.filters import gt

        self._assert_warns_on_call(gt, "revenue", 1000000)

    def test_ge_warns(self):
        from PowerPlatform.Dataverse.models.filters import ge

        self._assert_warns_on_call(ge, "revenue", 1000000)

    def test_lt_warns(self):
        from PowerPlatform.Dataverse.models.filters import lt

        self._assert_warns_on_call(lt, "revenue", 500000)

    def test_le_warns(self):
        from PowerPlatform.Dataverse.models.filters import le

        self._assert_warns_on_call(le, "revenue", 500000)

    def test_contains_warns(self):
        from PowerPlatform.Dataverse.models.filters import contains

        self._assert_warns_on_call(contains, "name", "Corp")

    def test_startswith_warns(self):
        from PowerPlatform.Dataverse.models.filters import startswith

        self._assert_warns_on_call(startswith, "name", "Con")

    def test_endswith_warns(self):
        from PowerPlatform.Dataverse.models.filters import endswith

        self._assert_warns_on_call(endswith, "name", "Ltd")

    def test_between_warns(self):
        from PowerPlatform.Dataverse.models.filters import between

        self._assert_warns_on_call(between, "revenue", 100000, 500000)

    def test_is_null_warns(self):
        from PowerPlatform.Dataverse.models.filters import is_null

        self._assert_warns_on_call(is_null, "telephone1")

    def test_is_not_null_warns(self):
        from PowerPlatform.Dataverse.models.filters import is_not_null

        self._assert_warns_on_call(is_not_null, "telephone1")

    def test_filter_in_warns(self):
        from PowerPlatform.Dataverse.models.filters import filter_in

        self._assert_warns_on_call(filter_in, "statecode", [0, 1])

    def test_not_in_warns(self):
        from PowerPlatform.Dataverse.models.filters import not_in

        self._assert_warns_on_call(not_in, "statecode", [2, 3])

    def test_not_between_warns(self):
        from PowerPlatform.Dataverse.models.filters import not_between

        self._assert_warns_on_call(not_between, "revenue", 100000, 500000)

    def test_between_emits_exactly_one_warning(self):
        """between() must not chain through deprecated ge/le (would emit 3 warnings)."""
        from PowerPlatform.Dataverse.models.filters import between

        self._assert_single_warning(between, "revenue", 100000, 500000)

    def test_not_between_emits_exactly_one_warning(self):
        """not_between() must not chain through deprecated ge/le."""
        from PowerPlatform.Dataverse.models.filters import not_between

        self._assert_single_warning(not_between, "revenue", 100000, 500000)

    def test_warning_is_deprecation_warning_class(self):
        """Each factory's warning category must be DeprecationWarning, not its subclass."""
        from PowerPlatform.Dataverse.models.filters import eq

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            eq("name", "Test")
        w = caught[0]
        self.assertIs(w.category, DeprecationWarning)

    def test_warning_message_names_replacement(self):
        """Each warning message should name the col()-based replacement."""
        from PowerPlatform.Dataverse.models.filters import eq

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            eq("name", "Test")
        self.assertIn("col(", str(caught[0].message))


# ---------------------------------------------------------------------------
# 3. GA functions col() and raw() emit NO deprecation warning
# ---------------------------------------------------------------------------


class TestGAFunctionsNoWarning(unittest.TestCase):
    """col() and raw() are GA — must never emit DeprecationWarning."""

    def _assert_no_dep_warning(self, func, *args, **kwargs):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            func(*args, **kwargs)
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(
            len(dep_warnings), 0, f"{func.__name__}() emitted unexpected DeprecationWarning: {dep_warnings}"
        )

    def test_col_no_warning(self):
        from PowerPlatform.Dataverse.models.filters import col

        self._assert_no_dep_warning(col, "statecode")

    def test_raw_no_warning(self):
        from PowerPlatform.Dataverse.models.filters import raw

        self._assert_no_dep_warning(raw, "statecode eq 0")

    def test_col_eq_no_warning(self):
        from PowerPlatform.Dataverse.models.filters import col

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            expr = col("statecode") == 0
            _ = expr.to_odata()
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(dep_warnings), 0)

    def test_col_comparison_chain_no_warning(self):
        from PowerPlatform.Dataverse.models.filters import col

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            expr = (col("statecode") == 0) & (col("revenue") > 100000)
            _ = expr.to_odata()
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(dep_warnings), 0)

    def test_col_between_no_warning(self):
        from PowerPlatform.Dataverse.models.filters import col

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            expr = col("revenue").between(100000, 500000)
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(dep_warnings), 0)

    def test_col_not_between_no_warning(self):
        from PowerPlatform.Dataverse.models.filters import col

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            expr = col("revenue").not_between(100000, 500000)
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(dep_warnings), 0)

    def test_col_in_no_warning(self):
        from PowerPlatform.Dataverse.models.filters import col

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            expr = col("statecode").in_([0, 1])
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(dep_warnings), 0)

    def test_col_not_in_no_warning(self):
        from PowerPlatform.Dataverse.models.filters import col

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            expr = col("statecode").not_in([2, 3])
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(dep_warnings), 0)

    def test_col_like_no_warning(self):
        from PowerPlatform.Dataverse.models.filters import col

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            expr = col("name").like("Contoso%")
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(dep_warnings), 0)

    def test_col_not_like_no_warning(self):
        from PowerPlatform.Dataverse.models.filters import col

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            expr = col("name").not_like("%Corp%")
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(dep_warnings), 0)

    def test_where_with_col_no_warning(self):
        from PowerPlatform.Dataverse.models.filters import col

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            QueryBuilder("account").where(col("statecode") == 0)
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(dep_warnings), 0)


# ---------------------------------------------------------------------------
# 4. dataframe.get() emits DeprecationWarning on call
# ---------------------------------------------------------------------------


class TestDataframeGetDeprecation(unittest.TestCase):
    """dataframe.get() must emit DeprecationWarning on call (not on import)."""

    def setUp(self):
        self.client = _make_client()
        # Set up _odata so records.get() can be called
        self.client._odata._get_multiple.return_value = iter([])
        self.client._odata._get.return_value = MagicMock(data={})

    def test_dataframe_get_warns_on_call(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            try:
                self.client.dataframe.get("account", select=["name"], top=10)
            except Exception:
                pass
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertGreater(len(dep_warnings), 0, "dataframe.get() did not emit DeprecationWarning")

    def test_dataframe_get_warning_message(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            try:
                self.client.dataframe.get("account", select=["name"], top=10)
            except Exception:
                pass
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        msg = str(dep_warnings[0].message)
        self.assertIn("dataframe.get()", msg)
        self.assertIn("builder", msg)

    def test_dataframe_other_methods_no_warning(self):
        """dataframe.sql(), dataframe.create(), etc. must NOT warn."""
        import pandas as pd
        from PowerPlatform.Dataverse.models.record import Record

        self.client._odata._query_sql.return_value = []

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.client.dataframe.sql("SELECT name FROM account")
        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(dep_warnings), 0, "dataframe.sql() must not warn")


# ---------------------------------------------------------------------------
# 5. Deprecated factories remain functional (correct OData output)
# ---------------------------------------------------------------------------


class TestDeprecatedFactoriesStillFunctional(unittest.TestCase):
    """Despite the warning, deprecated factories produce correct OData strings."""

    def _odata(self, func, *args):
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            return func(*args).to_odata()

    def test_eq_functional(self):
        from PowerPlatform.Dataverse.models.filters import eq

        self.assertEqual(self._odata(eq, "name", "Contoso"), "name eq 'Contoso'")

    def test_ne_functional(self):
        from PowerPlatform.Dataverse.models.filters import ne

        self.assertEqual(self._odata(ne, "statecode", 1), "statecode ne 1")

    def test_gt_functional(self):
        from PowerPlatform.Dataverse.models.filters import gt

        self.assertEqual(self._odata(gt, "revenue", 1000000), "revenue gt 1000000")

    def test_ge_functional(self):
        from PowerPlatform.Dataverse.models.filters import ge

        self.assertEqual(self._odata(ge, "revenue", 1000000), "revenue ge 1000000")

    def test_lt_functional(self):
        from PowerPlatform.Dataverse.models.filters import lt

        self.assertEqual(self._odata(lt, "revenue", 500000), "revenue lt 500000")

    def test_le_functional(self):
        from PowerPlatform.Dataverse.models.filters import le

        self.assertEqual(self._odata(le, "revenue", 500000), "revenue le 500000")

    def test_contains_functional(self):
        from PowerPlatform.Dataverse.models.filters import contains

        self.assertEqual(self._odata(contains, "name", "Corp"), "contains(name, 'Corp')")

    def test_startswith_functional(self):
        from PowerPlatform.Dataverse.models.filters import startswith

        self.assertEqual(self._odata(startswith, "name", "Con"), "startswith(name, 'Con')")

    def test_endswith_functional(self):
        from PowerPlatform.Dataverse.models.filters import endswith

        self.assertEqual(self._odata(endswith, "name", "Ltd"), "endswith(name, 'Ltd')")

    def test_between_functional(self):
        from PowerPlatform.Dataverse.models.filters import between

        self.assertEqual(
            self._odata(between, "revenue", 100000, 500000),
            "(revenue ge 100000 and revenue le 500000)",
        )

    def test_is_null_functional(self):
        from PowerPlatform.Dataverse.models.filters import is_null

        self.assertEqual(self._odata(is_null, "telephone1"), "telephone1 eq null")

    def test_is_not_null_functional(self):
        from PowerPlatform.Dataverse.models.filters import is_not_null

        self.assertEqual(self._odata(is_not_null, "telephone1"), "telephone1 ne null")

    def test_filter_in_functional(self):
        from PowerPlatform.Dataverse.models.filters import filter_in

        self.assertEqual(
            self._odata(filter_in, "statecode", [0, 1]),
            'Microsoft.Dynamics.CRM.In(PropertyName=\'statecode\',PropertyValues=["0","1"])',
        )

    def test_not_in_functional(self):
        from PowerPlatform.Dataverse.models.filters import not_in

        self.assertEqual(
            self._odata(not_in, "statecode", [2, 3]),
            'Microsoft.Dynamics.CRM.NotIn(PropertyName=\'statecode\',PropertyValues=["2","3"])',
        )

    def test_not_between_functional(self):
        from PowerPlatform.Dataverse.models.filters import not_between

        self.assertEqual(
            self._odata(not_between, "revenue", 100000, 500000),
            "not ((revenue ge 100000 and revenue le 500000))",
        )

    def test_deprecated_factory_usable_in_where(self):
        """Deprecated factories still produce valid FilterExpression for where()."""
        from PowerPlatform.Dataverse.models.filters import eq

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            expr = eq("statecode", 0)
        qb = QueryBuilder("account").where(expr)
        self.assertEqual(qb.build()["filter"], "statecode eq 0")

    def test_deprecated_factories_composable(self):
        """Deprecated factories still compose correctly with & and |."""
        from PowerPlatform.Dataverse.models.filters import eq, gt

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            expr = (eq("statecode", 0) | eq("statecode", 1)) & gt("revenue", 100000)
        self.assertEqual(
            expr.to_odata(),
            "((statecode eq 0 or statecode eq 1) and revenue gt 100000)",
        )


# ---------------------------------------------------------------------------
# 6. All 16 filter_* builder methods raise AttributeError on QueryBuilder
# ---------------------------------------------------------------------------


class TestRemovedBuilderFilterMethods(unittest.TestCase):
    """All 16 filter_* methods were removed from QueryBuilder in GA."""

    def setUp(self):
        self.qb = QueryBuilder("account")

    def _assert_removed(self, method_name, *args):
        with self.assertRaises(AttributeError, msg=f"QueryBuilder.{method_name} should not exist"):
            getattr(self.qb, method_name)(*args)

    def test_filter_eq_removed(self):
        self._assert_removed("filter_eq", "name", "Contoso")

    def test_filter_ne_removed(self):
        self._assert_removed("filter_ne", "statecode", 1)

    def test_filter_gt_removed(self):
        self._assert_removed("filter_gt", "revenue", 0)

    def test_filter_ge_removed(self):
        self._assert_removed("filter_ge", "revenue", 0)

    def test_filter_lt_removed(self):
        self._assert_removed("filter_lt", "revenue", 0)

    def test_filter_le_removed(self):
        self._assert_removed("filter_le", "revenue", 0)

    def test_filter_contains_removed(self):
        self._assert_removed("filter_contains", "name", "Corp")

    def test_filter_startswith_removed(self):
        self._assert_removed("filter_startswith", "name", "Con")

    def test_filter_endswith_removed(self):
        self._assert_removed("filter_endswith", "name", "Ltd")

    def test_filter_null_removed(self):
        self._assert_removed("filter_null", "telephone1")

    def test_filter_not_null_removed(self):
        self._assert_removed("filter_not_null", "telephone1")

    def test_filter_in_removed(self):
        self._assert_removed("filter_in", "statecode", [0, 1])

    def test_filter_not_in_removed(self):
        self._assert_removed("filter_not_in", "statecode", [0, 1])

    def test_filter_between_removed(self):
        self._assert_removed("filter_between", "revenue", 100, 500)

    def test_filter_not_between_removed(self):
        self._assert_removed("filter_not_between", "revenue", 100, 500)

    def test_filter_raw_removed(self):
        self._assert_removed("filter_raw", "statecode eq 0")


# ---------------------------------------------------------------------------
# 7. ColumnProxy (col()) correctness — all operators and methods
# ---------------------------------------------------------------------------


class TestColumnProxyOperators(unittest.TestCase):
    """col() proxy covers all operators and methods correctly."""

    def test_eq(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual((col("name") == "Contoso").to_odata(), "name eq 'Contoso'")

    def test_ne(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual((col("statecode") != 1).to_odata(), "statecode ne 1")

    def test_gt(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual((col("revenue") > 1000000).to_odata(), "revenue gt 1000000")

    def test_ge(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual((col("revenue") >= 1000000).to_odata(), "revenue ge 1000000")

    def test_lt(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual((col("revenue") < 500000).to_odata(), "revenue lt 500000")

    def test_le(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual((col("revenue") <= 500000).to_odata(), "revenue le 500000")

    def test_is_null(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual(col("telephone1").is_null().to_odata(), "telephone1 eq null")

    def test_is_not_null(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual(col("telephone1").is_not_null().to_odata(), "telephone1 ne null")

    def test_in_(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual(
            col("statecode").in_([0, 1]).to_odata(),
            'Microsoft.Dynamics.CRM.In(PropertyName=\'statecode\',PropertyValues=["0","1"])',
        )

    def test_not_in(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual(
            col("statecode").not_in([2, 3]).to_odata(),
            'Microsoft.Dynamics.CRM.NotIn(PropertyName=\'statecode\',PropertyValues=["2","3"])',
        )

    def test_between(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual(
            col("revenue").between(100000, 500000).to_odata(),
            "(revenue ge 100000 and revenue le 500000)",
        )

    def test_not_between(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual(
            col("revenue").not_between(100000, 500000).to_odata(),
            "not ((revenue ge 100000 and revenue le 500000))",
        )

    def test_contains(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual(col("name").contains("Corp").to_odata(), "contains(name, 'Corp')")

    def test_startswith(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual(col("name").startswith("Con").to_odata(), "startswith(name, 'Con')")

    def test_endswith(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual(col("name").endswith("Ltd").to_odata(), "endswith(name, 'Ltd')")

    def test_like_startswith_pattern(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual(col("name").like("Contoso%").to_odata(), "startswith(name, 'Contoso')")

    def test_like_endswith_pattern(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual(col("name").like("%Ltd").to_odata(), "endswith(name, 'Ltd')")

    def test_like_contains_pattern(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual(col("name").like("%Corp%").to_odata(), "contains(name, 'Corp')")

    def test_like_no_wildcard_equality(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual(col("name").like("Contoso").to_odata(), "name eq 'Contoso'")

    def test_like_interior_wildcard_raises(self):
        from PowerPlatform.Dataverse.models.filters import col

        with self.assertRaises(ValueError):
            col("name").like("Con%oso")

    def test_not_like_startswith_pattern(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual(
            col("name").not_like("Contoso%").to_odata(),
            "not (startswith(name, 'Contoso'))",
        )

    def test_not_like_endswith_pattern(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual(
            col("name").not_like("%Ltd").to_odata(),
            "not (endswith(name, 'Ltd'))",
        )

    def test_not_like_contains_pattern(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual(
            col("name").not_like("%Corp%").to_odata(),
            "not (contains(name, 'Corp'))",
        )

    def test_not_like_no_wildcard_negated_equality(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual(
            col("name").not_like("Contoso").to_odata(),
            "not (name eq 'Contoso')",
        )

    def test_not_like_interior_wildcard_raises(self):
        from PowerPlatform.Dataverse.models.filters import col

        with self.assertRaises(ValueError):
            col("name").not_like("Con%oso")

    def test_column_name_lowercased(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual((col("StateCode") == 0).to_odata(), "statecode eq 0")

    def test_empty_column_name_raises(self):
        from PowerPlatform.Dataverse.models.filters import col

        with self.assertRaises(ValueError):
            col("")

    def test_whitespace_column_name_raises(self):
        from PowerPlatform.Dataverse.models.filters import col

        with self.assertRaises(ValueError):
            col("   ")

    def test_boolean_eq(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual((col("active") == True).to_odata(), "active eq true")  # noqa: E712
        self.assertEqual((col("active") == False).to_odata(), "active eq false")  # noqa: E712

    def test_none_eq(self):
        from PowerPlatform.Dataverse.models.filters import col

        self.assertEqual((col("telephone1") == None).to_odata(), "telephone1 eq null")  # noqa: E711

    def test_and_composition(self):
        from PowerPlatform.Dataverse.models.filters import col

        expr = (col("statecode") == 0) & (col("revenue") > 100000)
        self.assertEqual(expr.to_odata(), "(statecode eq 0 and revenue gt 100000)")

    def test_or_composition(self):
        from PowerPlatform.Dataverse.models.filters import col

        expr = (col("statecode") == 0) | (col("statecode") == 1)
        self.assertEqual(expr.to_odata(), "(statecode eq 0 or statecode eq 1)")

    def test_not_composition(self):
        from PowerPlatform.Dataverse.models.filters import col

        expr = ~(col("statecode") == 1)
        self.assertEqual(expr.to_odata(), "not (statecode eq 1)")

    def test_in_empty_raises(self):
        from PowerPlatform.Dataverse.models.filters import col

        with self.assertRaises(ValueError):
            col("statecode").in_([])

    def test_not_in_empty_raises(self):
        from PowerPlatform.Dataverse.models.filters import col

        with self.assertRaises(ValueError):
            col("statecode").not_in([])


# ---------------------------------------------------------------------------
# 8. GA namespace-level fluent builder (where + col) end-to-end
# ---------------------------------------------------------------------------


class TestGABuilderEndToEnd(unittest.TestCase):
    """End-to-end verification of the GA query builder without any deprecated APIs."""

    def setUp(self):
        self.client = _make_client()
        self.client._odata._get_multiple.return_value = iter([])

    def test_builder_with_col_exprs_no_warnings(self):
        from PowerPlatform.Dataverse.models.filters import col

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            qb = (
                self.client.query.builder("account")
                .select("name", "revenue")
                .where((col("statecode") == 0) | (col("statecode") == 1))
                .where(col("revenue") > 100000)
                .order_by("revenue", descending=True)
                .top(50)
            )
            params = qb.build()

        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(dep_warnings), 0, f"GA API emitted warnings: {dep_warnings}")
        self.assertEqual(
            params["filter"],
            "(statecode eq 0 or statecode eq 1) and revenue gt 100000",
        )
        self.assertEqual(params["select"], ["name", "revenue"])
        self.assertEqual(params["top"], 50)

    def test_builder_with_raw_no_warnings(self):
        from PowerPlatform.Dataverse.models.filters import raw

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            qb = self.client.query.builder("account").select("name").where(raw("statecode eq 0")).top(10)
            params = qb.build()

        dep_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(dep_warnings), 0)
        self.assertEqual(params["filter"], "statecode eq 0")


if __name__ == "__main__":
    unittest.main()
