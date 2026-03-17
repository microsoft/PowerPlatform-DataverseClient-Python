# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for QueryBuilder class."""

import unittest
from unittest.mock import MagicMock

from PowerPlatform.Dataverse.models.query_builder import QueryBuilder


class TestQueryBuilderConstruction(unittest.TestCase):
    """Tests for QueryBuilder construction and validation."""

    def test_basic_construction(self):
        qb = QueryBuilder("account")
        self.assertEqual(qb.table, "account")
        self.assertEqual(qb.build(), {"table": "account"})

    def test_empty_table_raises(self):
        with self.assertRaises(ValueError):
            QueryBuilder("")

    def test_whitespace_table_raises(self):
        with self.assertRaises(ValueError):
            QueryBuilder("   ")

    def test_internal_state_not_exposed_as_constructor_params(self):
        """Unlike a dataclass, internal state should not be settable via constructor."""
        with self.assertRaises(TypeError):
            QueryBuilder("account", _select=["name"])  # type: ignore


class TestSelect(unittest.TestCase):
    """Tests for the select() method."""

    def test_select_single(self):
        qb = QueryBuilder("account").select("name")
        self.assertEqual(qb.build()["select"], ["name"])

    def test_select_multiple(self):
        qb = QueryBuilder("account").select("name", "revenue", "telephone1")
        self.assertEqual(qb.build()["select"], ["name", "revenue", "telephone1"])

    def test_select_chained(self):
        qb = QueryBuilder("account").select("name").select("revenue")
        self.assertEqual(qb.build()["select"], ["name", "revenue"])

    def test_select_returns_self(self):
        qb = QueryBuilder("account")
        self.assertIs(qb.select("name"), qb)


class TestComparisonFilters(unittest.TestCase):
    """Tests for comparison filter methods."""

    def test_filter_eq_string(self):
        qb = QueryBuilder("account").filter_eq("name", "Contoso")
        self.assertEqual(qb.build()["filter"], "name eq 'Contoso'")

    def test_filter_eq_integer(self):
        qb = QueryBuilder("account").filter_eq("statecode", 0)
        self.assertEqual(qb.build()["filter"], "statecode eq 0")

    def test_filter_eq_boolean_true(self):
        qb = QueryBuilder("account").filter_eq("active", True)
        self.assertEqual(qb.build()["filter"], "active eq true")

    def test_filter_eq_boolean_false(self):
        qb = QueryBuilder("account").filter_eq("active", False)
        self.assertEqual(qb.build()["filter"], "active eq false")

    def test_filter_eq_none(self):
        qb = QueryBuilder("account").filter_eq("telephone1", None)
        self.assertEqual(qb.build()["filter"], "telephone1 eq null")

    def test_filter_eq_float(self):
        qb = QueryBuilder("account").filter_eq("revenue", 1000000.5)
        self.assertEqual(qb.build()["filter"], "revenue eq 1000000.5")

    def test_filter_eq_datetime(self):
        from datetime import datetime, timezone

        dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        qb = QueryBuilder("account").filter_eq("createdon", dt)
        self.assertEqual(qb.build()["filter"], "createdon eq 2024-01-15T10:30:00Z")


class TestFilterIn(unittest.TestCase):
    """Tests for the filter_in() method."""

    def test_filter_in_integers(self):
        qb = QueryBuilder("account").filter_in("statecode", [0, 1, 2])
        self.assertEqual(
            qb.build()["filter"],
            'Microsoft.Dynamics.CRM.In(PropertyName=\'statecode\',PropertyValues=["0","1","2"])',
        )

    def test_filter_in_strings(self):
        qb = QueryBuilder("account").filter_in("name", ["Contoso", "Fabrikam"])
        self.assertEqual(
            qb.build()["filter"],
            'Microsoft.Dynamics.CRM.In(PropertyName=\'name\',PropertyValues=["Contoso","Fabrikam"])',
        )

    def test_filter_in_single_value(self):
        qb = QueryBuilder("account").filter_in("statecode", [0])
        self.assertEqual(
            qb.build()["filter"],
            "Microsoft.Dynamics.CRM.In(PropertyName='statecode',PropertyValues=[\"0\"])",
        )

    def test_filter_in_column_lowercased(self):
        qb = QueryBuilder("account").filter_in("StateCode", [0, 1])
        self.assertEqual(
            qb.build()["filter"],
            'Microsoft.Dynamics.CRM.In(PropertyName=\'statecode\',PropertyValues=["0","1"])',
        )

    def test_filter_in_empty_raises(self):
        with self.assertRaises(ValueError):
            QueryBuilder("account").filter_in("statecode", [])

    def test_filter_in_returns_self(self):
        qb = QueryBuilder("account")
        self.assertIs(qb.filter_in("statecode", [0, 1]), qb)

    def test_filter_in_accepts_set(self):
        qb = QueryBuilder("account").filter_in("statecode", {0, 1})
        result = qb.build()["filter"]
        self.assertIn("Microsoft.Dynamics.CRM.In", result)
        self.assertIn("statecode", result)

    def test_filter_in_accepts_tuple(self):
        qb = QueryBuilder("account").filter_in("statecode", (0, 1, 2))
        self.assertEqual(
            qb.build()["filter"],
            'Microsoft.Dynamics.CRM.In(PropertyName=\'statecode\',PropertyValues=["0","1","2"])',
        )

    def test_filter_in_int_enum(self):
        from enum import IntEnum

        class Priority(IntEnum):
            LOW = 1
            HIGH = 3

        qb = QueryBuilder("account").filter_in("priority", [Priority.LOW, Priority.HIGH])
        self.assertEqual(
            qb.build()["filter"],
            'Microsoft.Dynamics.CRM.In(PropertyName=\'priority\',PropertyValues=["1","3"])',
        )

    def test_filter_in_combined_with_other_filters(self):
        qb = QueryBuilder("account").filter_eq("statecode", 0).filter_in("priority", [1, 2, 3])
        self.assertEqual(
            qb.build()["filter"],
            'statecode eq 0 and Microsoft.Dynamics.CRM.In(PropertyName=\'priority\',PropertyValues=["1","2","3"])',
        )

    def test_filter_ne(self):
        qb = QueryBuilder("account").filter_ne("statecode", 1)
        self.assertEqual(qb.build()["filter"], "statecode ne 1")

    def test_filter_gt(self):
        qb = QueryBuilder("account").filter_gt("revenue", 1000000)
        self.assertEqual(qb.build()["filter"], "revenue gt 1000000")

    def test_filter_ge(self):
        qb = QueryBuilder("account").filter_ge("revenue", 1000000)
        self.assertEqual(qb.build()["filter"], "revenue ge 1000000")

    def test_filter_lt(self):
        qb = QueryBuilder("account").filter_lt("revenue", 500000)
        self.assertEqual(qb.build()["filter"], "revenue lt 500000")

    def test_filter_le(self):
        qb = QueryBuilder("account").filter_le("revenue", 500000)
        self.assertEqual(qb.build()["filter"], "revenue le 500000")

    def test_column_names_lowercased(self):
        qb = QueryBuilder("account").filter_eq("StateCode", 0).order_by("Revenue")
        params = qb.build()
        self.assertEqual(params["filter"], "statecode eq 0")
        self.assertEqual(params["orderby"], ["revenue"])

    def test_string_with_quotes_escaped(self):
        qb = QueryBuilder("account").filter_eq("name", "O'Brien's Corp")
        self.assertEqual(qb.build()["filter"], "name eq 'O''Brien''s Corp'")

    def test_multiple_filters_and_joined(self):
        qb = QueryBuilder("account").filter_eq("statecode", 0).filter_gt("revenue", 1000000)
        self.assertEqual(qb.build()["filter"], "statecode eq 0 and revenue gt 1000000")


class TestStringFunctionFilters(unittest.TestCase):
    """Tests for string function filter methods."""

    def test_filter_contains(self):
        qb = QueryBuilder("account").filter_contains("name", "Corp")
        self.assertEqual(qb.build()["filter"], "contains(name, 'Corp')")

    def test_filter_startswith(self):
        qb = QueryBuilder("account").filter_startswith("name", "Con")
        self.assertEqual(qb.build()["filter"], "startswith(name, 'Con')")

    def test_filter_endswith(self):
        qb = QueryBuilder("account").filter_endswith("name", "Ltd")
        self.assertEqual(qb.build()["filter"], "endswith(name, 'Ltd')")

    def test_filter_contains_single_quotes(self):
        qb = QueryBuilder("account").filter_contains("name", "O'Brien")
        self.assertEqual(qb.build()["filter"], "contains(name, 'O''Brien')")


class TestNullFilters(unittest.TestCase):
    """Tests for null/not-null filter methods."""

    def test_filter_null(self):
        qb = QueryBuilder("account").filter_null("telephone1")
        self.assertEqual(qb.build()["filter"], "telephone1 eq null")

    def test_filter_not_null(self):
        qb = QueryBuilder("account").filter_not_null("telephone1")
        self.assertEqual(qb.build()["filter"], "telephone1 ne null")


class TestFilterBetween(unittest.TestCase):
    """Tests for the filter_between() method."""

    def test_filter_between_parenthesized(self):
        qb = QueryBuilder("account").filter_between("revenue", 100000, 500000)
        self.assertEqual(
            qb.build()["filter"],
            "(revenue ge 100000 and revenue le 500000)",
        )

    def test_filter_between_column_lowercased(self):
        qb = QueryBuilder("account").filter_between("Revenue", 100, 500)
        self.assertEqual(
            qb.build()["filter"],
            "(revenue ge 100 and revenue le 500)",
        )

    def test_filter_between_returns_self(self):
        qb = QueryBuilder("account")
        self.assertIs(qb.filter_between("revenue", 100, 500), qb)

    def test_filter_between_combined_with_other_filters(self):
        qb = QueryBuilder("account").filter_eq("statecode", 0).filter_between("revenue", 100000, 500000)
        self.assertEqual(
            qb.build()["filter"],
            "statecode eq 0 and (revenue ge 100000 and revenue le 500000)",
        )

    def test_filter_between_datetimes(self):
        from datetime import datetime, timezone

        start = datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end = datetime(2024, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
        qb = QueryBuilder("account").filter_between("createdon", start, end)
        self.assertEqual(
            qb.build()["filter"],
            "(createdon ge 2024-01-01T00:00:00Z and createdon le 2024-12-31T23:59:59Z)",
        )


class TestFilterNotIn(unittest.TestCase):
    """Tests for the filter_not_in() method."""

    def test_filter_not_in_ints(self):
        qb = QueryBuilder("account").filter_not_in("statecode", [2, 3])
        self.assertEqual(
            qb.build()["filter"],
            'Microsoft.Dynamics.CRM.NotIn(PropertyName=\'statecode\',PropertyValues=["2","3"])',
        )

    def test_filter_not_in_strings(self):
        qb = QueryBuilder("account").filter_not_in("name", ["Contoso", "Fabrikam"])
        self.assertEqual(
            qb.build()["filter"],
            'Microsoft.Dynamics.CRM.NotIn(PropertyName=\'name\',PropertyValues=["Contoso","Fabrikam"])',
        )

    def test_filter_not_in_empty_raises(self):
        with self.assertRaises(ValueError):
            QueryBuilder("account").filter_not_in("statecode", [])

    def test_filter_not_in_returns_self(self):
        qb = QueryBuilder("account")
        self.assertIs(qb.filter_not_in("statecode", [0, 1]), qb)

    def test_filter_not_in_combined_with_other_filters(self):
        qb = QueryBuilder("account").filter_eq("statecode", 0).filter_not_in("priority", [1, 2])
        self.assertEqual(
            qb.build()["filter"],
            'statecode eq 0 and Microsoft.Dynamics.CRM.NotIn(PropertyName=\'priority\',PropertyValues=["1","2"])',
        )


class TestFilterNotBetween(unittest.TestCase):
    """Tests for the filter_not_between() method."""

    def test_filter_not_between_ints(self):
        qb = QueryBuilder("account").filter_not_between("revenue", 100000, 500000)
        self.assertEqual(
            qb.build()["filter"],
            "not ((revenue ge 100000 and revenue le 500000))",
        )

    def test_filter_not_between_returns_self(self):
        qb = QueryBuilder("account")
        self.assertIs(qb.filter_not_between("revenue", 100, 500), qb)

    def test_filter_not_between_combined_with_other_filters(self):
        qb = QueryBuilder("account").filter_eq("statecode", 0).filter_not_between("revenue", 100000, 500000)
        self.assertEqual(
            qb.build()["filter"],
            "statecode eq 0 and not ((revenue ge 100000 and revenue le 500000))",
        )


class TestFilterRaw(unittest.TestCase):
    """Tests for the filter_raw() method."""

    def test_filter_raw(self):
        qb = QueryBuilder("account").filter_raw("(statecode eq 0 or statecode eq 1)")
        self.assertEqual(qb.build()["filter"], "(statecode eq 0 or statecode eq 1)")

    def test_filter_raw_returns_self(self):
        qb = QueryBuilder("account")
        self.assertIs(qb.filter_raw("a eq 1"), qb)


class TestWhere(unittest.TestCase):
    """Tests for the where() method with composable expressions."""

    def test_where_simple(self):
        from PowerPlatform.Dataverse.models.filters import eq

        qb = QueryBuilder("account").where(eq("statecode", 0))
        self.assertEqual(qb.build()["filter"], "statecode eq 0")

    def test_where_complex(self):
        from PowerPlatform.Dataverse.models.filters import eq, gt

        expr = (eq("statecode", 0) | eq("statecode", 1)) & gt("revenue", 100000)
        qb = QueryBuilder("account").where(expr)
        self.assertEqual(
            qb.build()["filter"],
            "((statecode eq 0 or statecode eq 1) and revenue gt 100000)",
        )

    def test_where_combined_with_filter_methods(self):
        from PowerPlatform.Dataverse.models.filters import gt

        qb = QueryBuilder("account").filter_eq("statecode", 0).where(gt("revenue", 100000))
        self.assertEqual(qb.build()["filter"], "statecode eq 0 and revenue gt 100000")

    def test_where_multiple_calls(self):
        from PowerPlatform.Dataverse.models.filters import eq, gt

        qb = QueryBuilder("account").where(eq("statecode", 0)).where(gt("revenue", 100000))
        self.assertEqual(qb.build()["filter"], "statecode eq 0 and revenue gt 100000")

    def test_where_preserves_call_order(self):
        """Interleaved filter_*() and where() should preserve call order."""
        from PowerPlatform.Dataverse.models.filters import eq, gt

        qb = QueryBuilder("account").where(eq("a", 1)).filter_eq("b", 2).where(gt("c", 3))
        self.assertEqual(qb.build()["filter"], "a eq 1 and b eq 2 and c gt 3")

    def test_where_returns_self(self):
        from PowerPlatform.Dataverse.models.filters import eq

        qb = QueryBuilder("account")
        self.assertIs(qb.where(eq("statecode", 0)), qb)

    def test_where_non_expression_raises(self):
        qb = QueryBuilder("account")
        with self.assertRaises(TypeError):
            qb.where("statecode eq 0")  # type: ignore

    def test_where_with_not(self):
        from PowerPlatform.Dataverse.models.filters import eq

        qb = QueryBuilder("account").where(~eq("statecode", 1))
        self.assertEqual(qb.build()["filter"], "not (statecode eq 1)")

    def test_where_with_filter_in(self):
        from PowerPlatform.Dataverse.models.filters import filter_in, gt

        expr = filter_in("statecode", [0, 1]) & gt("revenue", 100000)
        qb = QueryBuilder("account").where(expr)
        self.assertEqual(
            qb.build()["filter"],
            '(Microsoft.Dynamics.CRM.In(PropertyName=\'statecode\',PropertyValues=["0","1"]) and revenue gt 100000)',
        )


class TestOrderBy(unittest.TestCase):
    """Tests for the order_by() method."""

    def test_ascending(self):
        qb = QueryBuilder("account").order_by("name")
        self.assertEqual(qb.build()["orderby"], ["name"])

    def test_descending(self):
        qb = QueryBuilder("account").order_by("revenue", descending=True)
        self.assertEqual(qb.build()["orderby"], ["revenue desc"])

    def test_multiple(self):
        qb = QueryBuilder("account").order_by("revenue", descending=True).order_by("name")
        self.assertEqual(qb.build()["orderby"], ["revenue desc", "name"])

    def test_returns_self(self):
        qb = QueryBuilder("account")
        self.assertIs(qb.order_by("name"), qb)


class TestTopAndPageSize(unittest.TestCase):
    """Tests for top() and page_size() methods."""

    def test_top(self):
        qb = QueryBuilder("account").top(10)
        self.assertEqual(qb.build()["top"], 10)

    def test_top_invalid_raises(self):
        with self.assertRaises(ValueError):
            QueryBuilder("account").top(0)
        with self.assertRaises(ValueError):
            QueryBuilder("account").top(-1)

    def test_top_returns_self(self):
        qb = QueryBuilder("account")
        self.assertIs(qb.top(10), qb)

    def test_page_size(self):
        qb = QueryBuilder("account").page_size(50)
        self.assertEqual(qb.build()["page_size"], 50)

    def test_page_size_invalid_raises(self):
        with self.assertRaises(ValueError):
            QueryBuilder("account").page_size(0)
        with self.assertRaises(ValueError):
            QueryBuilder("account").page_size(-1)

    def test_page_size_returns_self(self):
        qb = QueryBuilder("account")
        self.assertIs(qb.page_size(50), qb)


class TestExpand(unittest.TestCase):
    """Tests for the expand() method."""

    def test_expand_single(self):
        qb = QueryBuilder("account").expand("primarycontactid")
        self.assertEqual(qb.build()["expand"], ["primarycontactid"])

    def test_expand_multiple(self):
        qb = QueryBuilder("account").expand("primarycontactid", "ownerid")
        self.assertEqual(qb.build()["expand"], ["primarycontactid", "ownerid"])

    def test_expand_returns_self(self):
        qb = QueryBuilder("account")
        self.assertIs(qb.expand("primarycontactid"), qb)

    def test_expand_with_expand_option(self):
        from PowerPlatform.Dataverse.models.query_builder import ExpandOption

        opt = ExpandOption("Account_Tasks").select("subject", "createdon").top(5)
        qb = QueryBuilder("account").expand(opt)
        self.assertEqual(
            qb.build()["expand"],
            ["Account_Tasks($select=subject,createdon;$top=5)"],
        )

    def test_expand_option_with_filter_and_orderby(self):
        from PowerPlatform.Dataverse.models.query_builder import ExpandOption

        opt = (ExpandOption("Account_Tasks")
               .select("subject")
               .filter("contains(subject,'Task')")
               .order_by("createdon", descending=True)
               .top(10))
        self.assertEqual(
            opt.to_odata(),
            "Account_Tasks($select=subject;$filter=contains(subject,'Task');$orderby=createdon desc;$top=10)",
        )

    def test_expand_option_no_options_returns_plain_name(self):
        from PowerPlatform.Dataverse.models.query_builder import ExpandOption

        opt = ExpandOption("primarycontactid")
        self.assertEqual(opt.to_odata(), "primarycontactid")

    def test_expand_mixed_strings_and_options(self):
        from PowerPlatform.Dataverse.models.query_builder import ExpandOption

        opt = ExpandOption("Account_Tasks").select("subject")
        qb = QueryBuilder("account").expand("primarycontactid", opt)
        self.assertEqual(
            qb.build()["expand"],
            ["primarycontactid", "Account_Tasks($select=subject)"],
        )


class TestCount(unittest.TestCase):
    """Tests for the count() method."""

    def test_count_sets_flag(self):
        qb = QueryBuilder("account").count()
        self.assertTrue(qb.build()["count"])

    def test_count_not_in_build_by_default(self):
        params = QueryBuilder("account").build()
        self.assertNotIn("count", params)

    def test_count_returns_self(self):
        qb = QueryBuilder("account")
        self.assertIs(qb.count(), qb)


class TestIncludeAnnotations(unittest.TestCase):
    """Tests for include_formatted_values() and include_annotations()."""

    def test_include_formatted_values(self):
        qb = QueryBuilder("account").include_formatted_values()
        self.assertEqual(
            qb.build()["include_annotations"],
            "OData.Community.Display.V1.FormattedValue",
        )

    def test_include_annotations_default_wildcard(self):
        qb = QueryBuilder("account").include_annotations()
        self.assertEqual(qb.build()["include_annotations"], "*")

    def test_include_annotations_custom(self):
        qb = QueryBuilder("account").include_annotations(
            "Microsoft.Dynamics.CRM.lookuplogicalname"
        )
        self.assertEqual(
            qb.build()["include_annotations"],
            "Microsoft.Dynamics.CRM.lookuplogicalname",
        )

    def test_annotations_not_in_build_by_default(self):
        params = QueryBuilder("account").build()
        self.assertNotIn("include_annotations", params)

    def test_include_formatted_values_returns_self(self):
        qb = QueryBuilder("account")
        self.assertIs(qb.include_formatted_values(), qb)

    def test_include_annotations_returns_self(self):
        qb = QueryBuilder("account")
        self.assertIs(qb.include_annotations(), qb)


class TestBuild(unittest.TestCase):
    """Tests for the build() method."""

    def test_empty_builder_only_has_table(self):
        params = QueryBuilder("account").build()
        self.assertEqual(params, {"table": "account"})
        self.assertNotIn("select", params)
        self.assertNotIn("filter", params)
        self.assertNotIn("orderby", params)
        self.assertNotIn("expand", params)
        self.assertNotIn("top", params)
        self.assertNotIn("page_size", params)

    def test_full_query_build(self):
        qb = (
            QueryBuilder("account")
            .select("name", "revenue", "telephone1")
            .filter_eq("statecode", 0)
            .filter_gt("revenue", 1000000)
            .order_by("revenue", descending=True)
            .order_by("name")
            .expand("primarycontactid")
            .top(50)
            .page_size(25)
        )
        params = qb.build()
        self.assertEqual(params["table"], "account")
        self.assertEqual(params["select"], ["name", "revenue", "telephone1"])
        self.assertEqual(params["filter"], "statecode eq 0 and revenue gt 1000000")
        self.assertEqual(params["orderby"], ["revenue desc", "name"])
        self.assertEqual(params["expand"], ["primarycontactid"])
        self.assertEqual(params["top"], 50)
        self.assertEqual(params["page_size"], 25)

    def test_build_returns_fresh_lists(self):
        """build() should return copies of internal lists."""
        qb = QueryBuilder("account").select("name")
        params1 = qb.build()
        params2 = qb.build()
        self.assertEqual(params1["select"], params2["select"])
        self.assertIsNot(params1["select"], params2["select"])


class TestMethodChainingReturnsSelf(unittest.TestCase):
    """Verify all methods return self for chaining."""

    def test_all_methods_return_self(self):
        from PowerPlatform.Dataverse.models.filters import eq

        qb = QueryBuilder("account")

        self.assertIs(qb.select("name"), qb)
        self.assertIs(qb.filter_eq("a", 1), qb)
        self.assertIs(qb.filter_ne("b", 2), qb)
        self.assertIs(qb.filter_gt("c", 3), qb)
        self.assertIs(qb.filter_ge("d", 4), qb)
        self.assertIs(qb.filter_lt("e", 5), qb)
        self.assertIs(qb.filter_le("f", 6), qb)
        self.assertIs(qb.filter_contains("g", "x"), qb)
        self.assertIs(qb.filter_startswith("h", "y"), qb)
        self.assertIs(qb.filter_endswith("i", "z"), qb)
        self.assertIs(qb.filter_null("j"), qb)
        self.assertIs(qb.filter_not_null("k"), qb)
        self.assertIs(qb.filter_raw("l eq 1"), qb)
        self.assertIs(qb.filter_in("m", [1, 2]), qb)
        self.assertIs(qb.filter_between("n", 1, 10), qb)
        self.assertIs(qb.where(eq("o", 1)), qb)
        self.assertIs(qb.order_by("p"), qb)
        self.assertIs(qb.expand("q"), qb)
        self.assertIs(qb.top(10), qb)
        self.assertIs(qb.page_size(5), qb)
        self.assertIs(qb.count(), qb)
        self.assertIs(qb.include_formatted_values(), qb)
        self.assertIs(qb.include_annotations(), qb)


class TestExecute(unittest.TestCase):
    """Tests for the execute() terminal method."""

    def test_execute_without_query_ops_raises(self):
        qb = QueryBuilder("account").filter_eq("statecode", 0)
        with self.assertRaises(RuntimeError) as ctx:
            qb.execute()
        self.assertIn("client.query.builder()", str(ctx.exception))

    def test_execute_calls_records_get(self):
        """execute() should delegate to client.records.get() with built params."""
        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        mock_client.records.get.return_value = iter([[{"name": "Test"}]])

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.select("name", "revenue").filter_eq("statecode", 0).order_by("revenue", descending=True).top(100).page_size(
            50
        ).expand("primarycontactid")

        list(qb.execute())

        mock_client.records.get.assert_called_once_with(
            "account",
            select=["name", "revenue"],
            filter="statecode eq 0",
            orderby=["revenue desc"],
            top=100,
            expand=["primarycontactid"],
            page_size=50,
            count=False,
            include_annotations=None,
        )

    def test_execute_returns_flat_records_by_default(self):
        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        mock_client.records.get.return_value = iter([[{"name": "A"}, {"name": "B"}], [{"name": "C"}]])

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        records = list(qb.execute())

        self.assertEqual(len(records), 3)
        self.assertEqual(records[0]["name"], "A")
        self.assertEqual(records[1]["name"], "B")
        self.assertEqual(records[2]["name"], "C")

    def test_execute_by_page_returns_pages(self):
        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client

        page1 = [{"name": "A"}, {"name": "B"}]
        page2 = [{"name": "C"}]
        mock_client.records.get.return_value = iter([page1, page2])

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        pages = list(qb.execute(by_page=True))

        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0], page1)
        self.assertEqual(pages[1], page2)

    def test_execute_passes_none_for_empty_options(self):
        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        mock_client.records.get.return_value = iter([])

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        list(qb.execute())

        mock_client.records.get.assert_called_once_with(
            "account",
            select=None,
            filter=None,
            orderby=None,
            top=None,
            expand=None,
            page_size=None,
            count=False,
            include_annotations=None,
        )

    def test_execute_with_where_expressions(self):
        from PowerPlatform.Dataverse.models.filters import eq, gt

        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        mock_client.records.get.return_value = iter([])

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.where((eq("statecode", 0) | eq("statecode", 1)) & gt("revenue", 100000))
        list(qb.execute())

        call_args = mock_client.records.get.call_args
        self.assertEqual(
            call_args.kwargs["filter"],
            "((statecode eq 0 or statecode eq 1) and revenue gt 100000)",
        )

    def test_execute_with_filter_in(self):
        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        mock_client.records.get.return_value = iter([])

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.filter_in("statecode", [0, 1, 2])
        list(qb.execute())

        call_args = mock_client.records.get.call_args
        self.assertEqual(
            call_args.kwargs["filter"],
            'Microsoft.Dynamics.CRM.In(PropertyName=\'statecode\',PropertyValues=["0","1","2"])',
        )

    def test_execute_passes_count_and_annotations(self):
        """execute() should forward count and include_annotations when set."""
        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        mock_client.records.get.return_value = iter([])

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.count().include_formatted_values()
        list(qb.execute())

        mock_client.records.get.assert_called_once_with(
            "account",
            select=None,
            filter=None,
            orderby=None,
            top=None,
            expand=None,
            page_size=None,
            count=True,
            include_annotations="OData.Community.Display.V1.FormattedValue",
        )


class TestToDataframe(unittest.TestCase):
    """Tests for the to_dataframe() terminal method."""

    def test_to_dataframe_without_query_ops_raises(self):
        qb = QueryBuilder("account").filter_eq("statecode", 0)
        with self.assertRaises(RuntimeError) as ctx:
            qb.to_dataframe()
        self.assertIn("client.query.builder()", str(ctx.exception))

    def test_to_dataframe_delegates_to_dataframe_get(self):
        """to_dataframe() should delegate to client.dataframe.get() with built params."""
        import pandas as pd

        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        expected_df = pd.DataFrame([{"name": "Contoso", "revenue": 1000}])
        mock_client.dataframe.get.return_value = expected_df

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.select("name", "revenue").filter_eq("statecode", 0).order_by("revenue", descending=True).top(100).page_size(
            50
        ).expand("primarycontactid")

        result = qb.to_dataframe()

        mock_client.dataframe.get.assert_called_once_with(
            "account",
            select=["name", "revenue"],
            filter="statecode eq 0",
            orderby=["revenue desc"],
            top=100,
            expand=["primarycontactid"],
            page_size=50,
            count=False,
            include_annotations=None,
        )
        pd.testing.assert_frame_equal(result, expected_df)

    def test_to_dataframe_passes_none_for_empty_options(self):
        """to_dataframe() with no options should pass None for all optional params."""
        import pandas as pd

        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        mock_client.dataframe.get.return_value = pd.DataFrame()

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.to_dataframe()

        mock_client.dataframe.get.assert_called_once_with(
            "account",
            select=None,
            filter=None,
            orderby=None,
            top=None,
            expand=None,
            page_size=None,
            count=False,
            include_annotations=None,
        )

    def test_to_dataframe_returns_dataframe(self):
        """to_dataframe() should return a pandas DataFrame."""
        import pandas as pd

        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        mock_client.dataframe.get.return_value = pd.DataFrame(
            [{"name": "A", "revenue": 100}, {"name": "B", "revenue": 200}]
        )

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.select("name", "revenue")

        result = qb.to_dataframe()

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        self.assertListEqual(list(result.columns), ["name", "revenue"])


if __name__ == "__main__":
    unittest.main()
