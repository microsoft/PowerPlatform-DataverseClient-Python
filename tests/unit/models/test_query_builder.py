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


class TestRemovedFilterMethods(unittest.TestCase):
    """Verify all 16 filter_* builder methods were removed in 1.0 GA."""

    def setUp(self):
        self.qb = QueryBuilder("account")

    def test_filter_eq_removed(self):
        with self.assertRaises(AttributeError):
            self.qb.filter_eq("name", "Contoso")

    def test_filter_ne_removed(self):
        with self.assertRaises(AttributeError):
            self.qb.filter_ne("statecode", 1)

    def test_filter_gt_removed(self):
        with self.assertRaises(AttributeError):
            self.qb.filter_gt("revenue", 0)

    def test_filter_ge_removed(self):
        with self.assertRaises(AttributeError):
            self.qb.filter_ge("revenue", 0)

    def test_filter_lt_removed(self):
        with self.assertRaises(AttributeError):
            self.qb.filter_lt("revenue", 0)

    def test_filter_le_removed(self):
        with self.assertRaises(AttributeError):
            self.qb.filter_le("revenue", 0)

    def test_filter_contains_removed(self):
        with self.assertRaises(AttributeError):
            self.qb.filter_contains("name", "Corp")

    def test_filter_startswith_removed(self):
        with self.assertRaises(AttributeError):
            self.qb.filter_startswith("name", "Con")

    def test_filter_endswith_removed(self):
        with self.assertRaises(AttributeError):
            self.qb.filter_endswith("name", "Ltd")

    def test_filter_null_removed(self):
        with self.assertRaises(AttributeError):
            self.qb.filter_null("telephone1")

    def test_filter_not_null_removed(self):
        with self.assertRaises(AttributeError):
            self.qb.filter_not_null("telephone1")

    def test_filter_in_removed(self):
        with self.assertRaises(AttributeError):
            self.qb.filter_in("statecode", [0, 1])

    def test_filter_not_in_removed(self):
        with self.assertRaises(AttributeError):
            self.qb.filter_not_in("statecode", [0, 1])

    def test_filter_between_removed(self):
        with self.assertRaises(AttributeError):
            self.qb.filter_between("revenue", 100, 500)

    def test_filter_not_between_removed(self):
        with self.assertRaises(AttributeError):
            self.qb.filter_not_between("revenue", 100, 500)

    def test_filter_raw_removed(self):
        with self.assertRaises(AttributeError):
            self.qb.filter_raw("statecode eq 0")


class TestWhere(unittest.TestCase):
    """Tests for the where() method with composable expressions."""

    def test_where_simple(self):
        from PowerPlatform.Dataverse.models.filters import col

        qb = QueryBuilder("account").where(col("statecode") == 0)
        self.assertEqual(qb.build()["filter"], "statecode eq 0")

    def test_where_complex(self):
        from PowerPlatform.Dataverse.models.filters import col

        expr = ((col("statecode") == 0) | (col("statecode") == 1)) & (col("revenue") > 100000)
        qb = QueryBuilder("account").where(expr)
        self.assertEqual(
            qb.build()["filter"],
            "((statecode eq 0 or statecode eq 1) and revenue gt 100000)",
        )

    def test_where_combined_with_raw(self):
        from PowerPlatform.Dataverse.models.filters import col, raw

        qb = QueryBuilder("account").where(raw("statecode eq 0")).where(col("revenue") > 100000)
        self.assertEqual(qb.build()["filter"], "statecode eq 0 and revenue gt 100000")

    def test_where_multiple_calls(self):
        from PowerPlatform.Dataverse.models.filters import col

        qb = QueryBuilder("account").where(col("statecode") == 0).where(col("revenue") > 100000)
        self.assertEqual(qb.build()["filter"], "statecode eq 0 and revenue gt 100000")

    def test_where_preserves_call_order(self):
        """Multiple where() calls preserve insertion order."""
        from PowerPlatform.Dataverse.models.filters import col

        qb = QueryBuilder("account").where(col("a") == 1).where(col("b") == 2).where(col("c") > 3)
        self.assertEqual(qb.build()["filter"], "a eq 1 and b eq 2 and c gt 3")

    def test_where_returns_self(self):
        from PowerPlatform.Dataverse.models.filters import col

        qb = QueryBuilder("account")
        self.assertIs(qb.where(col("statecode") == 0), qb)

    def test_where_non_expression_raises(self):
        qb = QueryBuilder("account")
        with self.assertRaises(TypeError):
            qb.where("statecode eq 0")  # type: ignore

    def test_where_with_not(self):
        from PowerPlatform.Dataverse.models.filters import col

        qb = QueryBuilder("account").where(~(col("statecode") == 1))
        self.assertEqual(qb.build()["filter"], "not (statecode eq 1)")

    def test_where_with_filter_in(self):
        from PowerPlatform.Dataverse.models.filters import col

        expr = col("statecode").in_([0, 1]) & (col("revenue") > 100000)
        qb = QueryBuilder("account").where(expr)
        self.assertEqual(
            qb.build()["filter"],
            '(Microsoft.Dynamics.CRM.In(PropertyName=\'statecode\',PropertyValues=["0","1"]) and revenue gt 100000)',
        )

    def test_where_with_raw_preserves_string(self):
        from PowerPlatform.Dataverse.models.filters import raw

        qb = QueryBuilder("account").where(raw("(statecode eq 0 or statecode eq 1)"))
        self.assertEqual(qb.build()["filter"], "(statecode eq 0 or statecode eq 1)")

    def test_where_negation_of_in(self):
        from PowerPlatform.Dataverse.models.filters import col

        qb = QueryBuilder("account").where(col("statecode").not_in([2, 3]))
        self.assertEqual(
            qb.build()["filter"],
            'Microsoft.Dynamics.CRM.NotIn(PropertyName=\'statecode\',PropertyValues=["2","3"])',
        )

    def test_where_between(self):
        from PowerPlatform.Dataverse.models.filters import col

        qb = QueryBuilder("account").where(col("revenue").between(100000, 500000))
        self.assertEqual(
            qb.build()["filter"],
            "(revenue ge 100000 and revenue le 500000)",
        )

    def test_where_not_between(self):
        from PowerPlatform.Dataverse.models.filters import col

        qb = QueryBuilder("account").where(col("revenue").not_between(100000, 500000))
        self.assertEqual(
            qb.build()["filter"],
            "not ((revenue ge 100000 and revenue le 500000))",
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

        opt = (
            ExpandOption("Account_Tasks")
            .select("subject")
            .filter("contains(subject,'Task')")
            .order_by("createdon", descending=True)
            .top(10)
        )
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

    def test_expand_option_chained_select_accumulates(self):
        """Calling select() multiple times should accumulate columns."""
        from PowerPlatform.Dataverse.models.query_builder import ExpandOption

        opt = ExpandOption("Account_Tasks").select("subject").select("createdon")
        self.assertEqual(
            opt.to_odata(),
            "Account_Tasks($select=subject,createdon)",
        )

    def test_expand_option_multiple_order_by(self):
        """Calling order_by() multiple times should accumulate sort clauses."""
        from PowerPlatform.Dataverse.models.query_builder import ExpandOption

        opt = (
            ExpandOption("Account_Tasks").select("subject").order_by("priority", descending=True).order_by("createdon")
        )
        self.assertEqual(
            opt.to_odata(),
            "Account_Tasks($select=subject;$orderby=priority desc,createdon)",
        )

    def test_expand_option_filter_last_wins(self):
        """Calling filter() multiple times should use the last value."""
        from PowerPlatform.Dataverse.models.query_builder import ExpandOption

        opt = ExpandOption("Account_Tasks").filter("statecode eq 0").filter("contains(subject,'Task')")
        self.assertEqual(
            opt.to_odata(),
            "Account_Tasks($filter=contains(subject,'Task'))",
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
        qb = QueryBuilder("account").include_annotations("Microsoft.Dynamics.CRM.lookuplogicalname")
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

    def test_include_annotations_overrides_formatted_values(self):
        """Last annotation call should win."""
        qb = QueryBuilder("account").include_formatted_values().include_annotations("*")
        self.assertEqual(qb.build()["include_annotations"], "*")

    def test_include_formatted_values_overrides_annotations(self):
        """Last annotation call should win (reverse order)."""
        qb = QueryBuilder("account").include_annotations("*").include_formatted_values()
        self.assertEqual(
            qb.build()["include_annotations"],
            "OData.Community.Display.V1.FormattedValue",
        )


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
        from PowerPlatform.Dataverse.models.filters import col, raw

        qb = (
            QueryBuilder("account")
            .select("name", "revenue", "telephone1")
            .where(raw("statecode eq 0"))
            .where(col("revenue") > 1000000)
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

    def test_build_with_plain_string_filter_part(self):
        """build() handles plain string entries in _filter_parts (internal path)."""
        qb = QueryBuilder("account")
        qb._filter_parts.append("name eq 'Contoso'")
        self.assertEqual(qb.build()["filter"], "name eq 'Contoso'")

    def test_build_mixed_string_and_expression_filter_parts(self):
        """build() AND-joins raw strings and FilterExpression objects in order."""
        from PowerPlatform.Dataverse.models.filters import col

        qb = QueryBuilder("account")
        qb._filter_parts.append("statecode eq 0")
        qb.where(col("revenue") > 100000)
        self.assertEqual(qb.build()["filter"], "statecode eq 0 and revenue gt 100000")


class TestMethodChainingReturnsSelf(unittest.TestCase):
    """Verify all public methods return self for chaining."""

    def test_all_methods_return_self(self):
        from PowerPlatform.Dataverse.models.filters import col

        qb = QueryBuilder("account")

        self.assertIs(qb.select("name"), qb)
        self.assertIs(qb.where(col("statecode") == 0), qb)
        self.assertIs(qb.order_by("name"), qb)
        self.assertIs(qb.expand("primarycontactid"), qb)
        self.assertIs(qb.top(10), qb)
        self.assertIs(qb.page_size(5), qb)
        self.assertIs(qb.count(), qb)
        self.assertIs(qb.include_formatted_values(), qb)
        self.assertIs(qb.include_annotations(), qb)


class TestExecute(unittest.TestCase):
    """Tests for the execute() terminal method."""

    def test_execute_without_query_ops_raises(self):
        from PowerPlatform.Dataverse.models.filters import raw

        qb = QueryBuilder("account").where(raw("statecode eq 0"))
        with self.assertRaises(RuntimeError) as ctx:
            qb.execute()
        self.assertIn("client.query.builder()", str(ctx.exception))

    def test_execute_calls_records_get(self):
        """execute() should delegate to client.records.get() with built params."""
        from PowerPlatform.Dataverse.models.filters import raw

        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        mock_client.records.get.return_value = iter([[{"name": "Test"}]])

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        (
            qb.select("name", "revenue")
            .where(raw("statecode eq 0"))
            .order_by("revenue", descending=True)
            .top(100)
            .page_size(50)
            .expand("primarycontactid")
        )

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
        qb.select("name")
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
        qb.select("name")
        pages = list(qb.execute(by_page=True))

        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0], page1)
        self.assertEqual(pages[1], page2)

    def test_execute_with_only_select_succeeds(self):
        """execute() with select only should not raise."""
        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        mock_client.records.get.return_value = iter([])

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.select("name")
        list(qb.execute())  # should not raise
        mock_client.records.get.assert_called_once()

    def test_execute_with_only_filter_succeeds(self):
        """execute() with filter only should not raise."""
        from PowerPlatform.Dataverse.models.filters import raw

        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        mock_client.records.get.return_value = iter([])

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.where(raw("statecode eq 0"))
        list(qb.execute())  # should not raise
        mock_client.records.get.assert_called_once()

    def test_execute_with_only_top_succeeds(self):
        """execute() with top only should not raise."""
        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        mock_client.records.get.return_value = iter([])

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.top(10)
        list(qb.execute())  # should not raise
        mock_client.records.get.assert_called_once()

    def test_execute_with_only_expand_raises(self):
        """expand alone is not a sufficient constraint."""
        mock_query_ops = MagicMock()
        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.expand("primarycontactid")
        with self.assertRaises(ValueError):
            qb.execute()

    def test_execute_with_only_count_raises(self):
        """count alone is not a sufficient constraint."""
        mock_query_ops = MagicMock()
        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.count()
        with self.assertRaises(ValueError):
            qb.execute()

    def test_execute_with_where_expressions(self):
        from PowerPlatform.Dataverse.models.filters import col

        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        mock_client.records.get.return_value = iter([])

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.where(((col("statecode") == 0) | (col("statecode") == 1)) & (col("revenue") > 100000))
        list(qb.execute())

        call_args = mock_client.records.get.call_args
        self.assertEqual(
            call_args.kwargs["filter"],
            "((statecode eq 0 or statecode eq 1) and revenue gt 100000)",
        )

    def test_execute_with_filter_in(self):
        from PowerPlatform.Dataverse.models.filters import col

        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        mock_client.records.get.return_value = iter([])

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.where(col("statecode").in_([0, 1, 2]))
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
        qb.select("name").count().include_formatted_values()
        list(qb.execute())

        mock_client.records.get.assert_called_once_with(
            "account",
            select=["name"],
            filter=None,
            orderby=None,
            top=None,
            expand=None,
            page_size=None,
            count=True,
            include_annotations="OData.Community.Display.V1.FormattedValue",
        )


class TestExecutePages(unittest.TestCase):
    """Tests for execute_pages() — lazy per-page QueryResult iterator."""

    def _make_qb(self):
        mock_query_ops = MagicMock()
        mock_query_ops._client.records.get.return_value = iter([[{"name": "A"}], [{"name": "B"}]])
        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.select("name")
        return qb, mock_query_ops

    def test_execute_pages_returns_iterator(self):
        qb, _ = self._make_qb()
        result = qb.execute_pages()
        import types

        self.assertIsInstance(result, types.GeneratorType)

    def test_execute_pages_yields_query_result_per_page(self):
        from PowerPlatform.Dataverse.models.record import QueryResult

        qb, _ = self._make_qb()
        pages = list(qb.execute_pages())
        self.assertEqual(len(pages), 2)
        for page in pages:
            self.assertIsInstance(page, QueryResult)

    def test_execute_pages_page_contents(self):
        qb, _ = self._make_qb()
        pages = list(qb.execute_pages())
        self.assertEqual(pages[0].first()["name"], "A")
        self.assertEqual(pages[1].first()["name"], "B")

    def test_execute_pages_without_query_ops_raises(self):
        from PowerPlatform.Dataverse.models.filters import raw

        qb = QueryBuilder("account").where(raw("statecode eq 0"))
        with self.assertRaises(RuntimeError):
            list(qb.execute_pages())

    def test_execute_pages_without_constraints_raises(self):
        mock_query_ops = MagicMock()
        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        with self.assertRaises(ValueError):
            list(qb.execute_pages())


class TestByPageWarning(unittest.TestCase):
    """execute(by_page=...) fires UserWarning; plain execute() does not."""

    def _make_qb(self):
        mock_query_ops = MagicMock()
        mock_query_ops._client.records.get.return_value = iter([[{"name": "A"}]])
        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.select("name")
        return qb

    def test_execute_no_flag_no_warning(self):
        """execute() with no by_page argument fires no warning."""
        import warnings

        qb = self._make_qb()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            qb.execute()
        user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        self.assertEqual(len(user_warnings), 0)

    def test_execute_by_page_true_fires_user_warning(self):
        """execute(by_page=True) fires UserWarning pointing to execute_pages()."""
        qb = self._make_qb()
        with self.assertWarns(UserWarning) as ctx:
            list(qb.execute(by_page=True))
        self.assertIn("execute_pages()", str(ctx.warning))

    def test_execute_by_page_false_fires_user_warning(self):
        """execute(by_page=False) fires UserWarning — redundant flag."""
        qb = self._make_qb()
        with self.assertWarns(UserWarning) as ctx:
            qb.execute(by_page=False)
        self.assertIn("redundant", str(ctx.warning))

    def test_execute_by_page_true_still_functional(self):
        """execute(by_page=True) still returns the raw page iterator."""
        import warnings

        qb = self._make_qb()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            result = qb.execute(by_page=True)
        pages = list(result)
        self.assertEqual(len(pages), 1)

    def test_execute_by_page_false_still_returns_query_result(self):
        """execute(by_page=False) still returns QueryResult."""
        import warnings

        from PowerPlatform.Dataverse.models.record import QueryResult

        qb = self._make_qb()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            result = qb.execute(by_page=False)
        self.assertIsInstance(result, QueryResult)


class TestToDataframe(unittest.TestCase):
    """Tests for the to_dataframe() terminal method."""

    def test_to_dataframe_without_query_ops_raises(self):
        from PowerPlatform.Dataverse.models.filters import raw

        qb = QueryBuilder("account").where(raw("statecode eq 0"))
        with self.assertRaises(RuntimeError) as ctx:
            qb.to_dataframe()
        self.assertIn("client.query.builder()", str(ctx.exception))

    def test_to_dataframe_returns_dataframe(self):
        """to_dataframe() collects execute() results into a DataFrame."""
        import pandas as pd

        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        mock_client.records.get.return_value = iter([[{"name": "A", "revenue": 100}, {"name": "B", "revenue": 200}]])

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.select("name", "revenue")

        result = qb.to_dataframe()

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        self.assertListEqual(list(result.columns), ["name", "revenue"])

    def test_to_dataframe_empty_result_returns_empty_dataframe(self):
        """to_dataframe() with no matching records returns empty DataFrame."""
        import pandas as pd

        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        mock_client.records.get.return_value = iter([])

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.select("name", "revenue")

        result = qb.to_dataframe()

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 0)

    def test_to_dataframe_calls_records_get_with_params(self):
        """to_dataframe() should call records.get() with the built query params."""
        import pandas as pd
        from PowerPlatform.Dataverse.models.filters import raw

        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        mock_client.records.get.return_value = iter([[{"name": "Contoso", "revenue": 1000}]])

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        (
            qb.select("name", "revenue")
            .where(raw("statecode eq 0"))
            .order_by("revenue", descending=True)
            .top(100)
            .page_size(50)
            .expand("primarycontactid")
        )

        result = qb.to_dataframe()

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
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]["name"], "Contoso")

    def test_to_dataframe_forwards_count_and_annotations(self):
        """to_dataframe() should forward count and include_annotations when set."""
        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        mock_client.records.get.return_value = iter([])

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.select("name").count().include_formatted_values()
        qb.to_dataframe()

        mock_client.records.get.assert_called_once_with(
            "account",
            select=["name"],
            filter=None,
            orderby=None,
            top=None,
            expand=None,
            page_size=None,
            count=True,
            include_annotations="OData.Community.Display.V1.FormattedValue",
        )

    def test_to_dataframe_with_record_objects(self):
        """to_dataframe() handles Record objects (with .data attribute)."""
        import pandas as pd
        from PowerPlatform.Dataverse.models.record import Record

        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        records = [
            Record(id="id-1", table="account", data={"name": "Contoso", "revenue": 1000}),
            Record(id="id-2", table="account", data={"name": "Fabrikam", "revenue": 2000}),
        ]
        mock_client.records.get.return_value = iter([records])

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.select("name", "revenue")

        result = qb.to_dataframe()

        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 2)
        self.assertEqual(result.iloc[0]["name"], "Contoso")
        self.assertEqual(result.iloc[1]["revenue"], 2000)

    def test_to_dataframe_emits_deprecation_warning(self):
        """QueryBuilder.to_dataframe() fires DeprecationWarning; use execute().to_dataframe() instead."""
        mock_query_ops = MagicMock()
        mock_client = mock_query_ops._client
        mock_client.records.get.return_value = iter([])

        qb = QueryBuilder("account")
        qb._query_ops = mock_query_ops
        qb.select("name")

        with self.assertWarns(DeprecationWarning) as ctx:
            qb.to_dataframe()

        self.assertIn("QueryBuilder.to_dataframe()", str(ctx.warning))
        self.assertIn("execute().to_dataframe()", str(ctx.warning))


if __name__ == "__main__":
    unittest.main()
