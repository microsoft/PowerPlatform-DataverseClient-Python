# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for QueryBuilder class."""

import unittest

from PowerPlatform.Dataverse.models.query_builder import QueryBuilder, BoundQueryBuilder


class TestQueryBuilder(unittest.TestCase):
    """Test cases for the QueryBuilder class."""

    def test_basic_construction(self):
        """Test basic QueryBuilder construction."""
        qb = QueryBuilder("account")
        self.assertEqual(qb.table, "account")
        self.assertEqual(qb.build(), {"table": "account"})

    def test_select_single_column(self):
        """Test selecting a single column."""
        qb = QueryBuilder("account").select("name")
        params = qb.build()
        self.assertEqual(params["select"], ["name"])

    def test_select_multiple_columns(self):
        """Test selecting multiple columns."""
        qb = QueryBuilder("account").select("name", "revenue", "telephone1")
        params = qb.build()
        self.assertEqual(params["select"], ["name", "revenue", "telephone1"])

    def test_select_chained(self):
        """Test chained select calls."""
        qb = QueryBuilder("account").select("name").select("revenue")
        params = qb.build()
        self.assertEqual(params["select"], ["name", "revenue"])

    def test_filter_eq_string(self):
        """Test equality filter with string value."""
        qb = QueryBuilder("account").filter_eq("name", "Contoso")
        params = qb.build()
        self.assertEqual(params["filter"], "name eq 'Contoso'")

    def test_filter_eq_integer(self):
        """Test equality filter with integer value."""
        qb = QueryBuilder("account").filter_eq("statecode", 0)
        params = qb.build()
        self.assertEqual(params["filter"], "statecode eq 0")

    def test_filter_eq_boolean(self):
        """Test equality filter with boolean value."""
        qb = QueryBuilder("account").filter_eq("emailaddress1_valid", True)
        params = qb.build()
        self.assertEqual(params["filter"], "emailaddress1_valid eq true")

    def test_filter_eq_boolean_false(self):
        """Test equality filter with boolean false value."""
        qb = QueryBuilder("account").filter_eq("emailaddress1_valid", False)
        params = qb.build()
        self.assertEqual(params["filter"], "emailaddress1_valid eq false")

    def test_filter_ne(self):
        """Test not-equal filter."""
        qb = QueryBuilder("account").filter_ne("statecode", 1)
        params = qb.build()
        self.assertEqual(params["filter"], "statecode ne 1")

    def test_filter_gt(self):
        """Test greater-than filter."""
        qb = QueryBuilder("account").filter_gt("revenue", 1000000)
        params = qb.build()
        self.assertEqual(params["filter"], "revenue gt 1000000")

    def test_filter_ge(self):
        """Test greater-than-or-equal filter."""
        qb = QueryBuilder("account").filter_ge("revenue", 1000000)
        params = qb.build()
        self.assertEqual(params["filter"], "revenue ge 1000000")

    def test_filter_lt(self):
        """Test less-than filter."""
        qb = QueryBuilder("account").filter_lt("revenue", 500000)
        params = qb.build()
        self.assertEqual(params["filter"], "revenue lt 500000")

    def test_filter_le(self):
        """Test less-than-or-equal filter."""
        qb = QueryBuilder("account").filter_le("revenue", 500000)
        params = qb.build()
        self.assertEqual(params["filter"], "revenue le 500000")

    def test_filter_contains(self):
        """Test contains filter."""
        qb = QueryBuilder("account").filter_contains("name", "Corp")
        params = qb.build()
        self.assertEqual(params["filter"], "contains(name, 'Corp')")

    def test_filter_startswith(self):
        """Test startswith filter."""
        qb = QueryBuilder("account").filter_startswith("name", "Con")
        params = qb.build()
        self.assertEqual(params["filter"], "startswith(name, 'Con')")

    def test_filter_endswith(self):
        """Test endswith filter."""
        qb = QueryBuilder("account").filter_endswith("name", "Ltd")
        params = qb.build()
        self.assertEqual(params["filter"], "endswith(name, 'Ltd')")

    def test_filter_null(self):
        """Test null filter."""
        qb = QueryBuilder("account").filter_null("telephone1")
        params = qb.build()
        self.assertEqual(params["filter"], "telephone1 eq null")

    def test_filter_not_null(self):
        """Test not-null filter."""
        qb = QueryBuilder("account").filter_not_null("telephone1")
        params = qb.build()
        self.assertEqual(params["filter"], "telephone1 ne null")

    def test_filter_raw(self):
        """Test raw filter string."""
        qb = QueryBuilder("account").filter_raw("(statecode eq 0 or statecode eq 1)")
        params = qb.build()
        self.assertEqual(params["filter"], "(statecode eq 0 or statecode eq 1)")

    def test_multiple_filters_and_joined(self):
        """Test multiple filters are AND-joined."""
        qb = QueryBuilder("account").filter_eq("statecode", 0).filter_gt("revenue", 1000000)
        params = qb.build()
        self.assertEqual(params["filter"], "statecode eq 0 and revenue gt 1000000")

    def test_order_by_ascending(self):
        """Test ascending order."""
        qb = QueryBuilder("account").order_by("name")
        params = qb.build()
        self.assertEqual(params["orderby"], ["name"])

    def test_order_by_descending(self):
        """Test descending order."""
        qb = QueryBuilder("account").order_by("revenue", descending=True)
        params = qb.build()
        self.assertEqual(params["orderby"], ["revenue desc"])

    def test_order_by_multiple(self):
        """Test multiple order by clauses."""
        qb = QueryBuilder("account").order_by("revenue", descending=True).order_by("name")
        params = qb.build()
        self.assertEqual(params["orderby"], ["revenue desc", "name"])

    def test_top(self):
        """Test top limit."""
        qb = QueryBuilder("account").top(10)
        params = qb.build()
        self.assertEqual(params["top"], 10)

    def test_top_invalid_raises(self):
        """Test that top with invalid value raises."""
        qb = QueryBuilder("account")
        with self.assertRaises(ValueError):
            qb.top(0)
        with self.assertRaises(ValueError):
            qb.top(-1)

    def test_expand_single(self):
        """Test expanding single navigation property."""
        qb = QueryBuilder("account").expand("primarycontactid")
        params = qb.build()
        self.assertEqual(params["expand"], ["primarycontactid"])

    def test_expand_multiple(self):
        """Test expanding multiple navigation properties."""
        qb = QueryBuilder("account").expand("primarycontactid", "ownerid")
        params = qb.build()
        self.assertEqual(params["expand"], ["primarycontactid", "ownerid"])

    def test_column_names_lowercased(self):
        """Test that column names are lowercased in filters and orderby."""
        qb = QueryBuilder("account").filter_eq("StateCode", 0).order_by("Revenue")
        params = qb.build()
        self.assertEqual(params["filter"], "statecode eq 0")
        self.assertEqual(params["orderby"], ["revenue"])

    def test_string_value_with_quotes_escaped(self):
        """Test that string values with quotes are properly escaped."""
        qb = QueryBuilder("account").filter_eq("name", "O'Brien's Corp")
        params = qb.build()
        self.assertEqual(params["filter"], "name eq 'O''Brien''s Corp'")

    def test_filter_eq_none(self):
        """Test equality filter with None value formats as null."""
        qb = QueryBuilder("account").filter_eq("telephone1", None)
        params = qb.build()
        self.assertEqual(params["filter"], "telephone1 eq null")

    def test_filter_eq_float(self):
        """Test equality filter with float value."""
        qb = QueryBuilder("account").filter_eq("revenue", 1000000.50)
        params = qb.build()
        self.assertEqual(params["filter"], "revenue eq 1000000.5")

    def test_full_query_build(self):
        """Test building a complete query with all options."""
        qb = (
            QueryBuilder("account")
            .select("name", "revenue", "telephone1")
            .filter_eq("statecode", 0)
            .filter_gt("revenue", 1000000)
            .order_by("revenue", descending=True)
            .order_by("name")
            .expand("primarycontactid")
            .top(50)
        )
        params = qb.build()

        self.assertEqual(params["table"], "account")
        self.assertEqual(params["select"], ["name", "revenue", "telephone1"])
        self.assertEqual(params["filter"], "statecode eq 0 and revenue gt 1000000")
        self.assertEqual(params["orderby"], ["revenue desc", "name"])
        self.assertEqual(params["expand"], ["primarycontactid"])
        self.assertEqual(params["top"], 50)

    def test_method_chaining_returns_self(self):
        """Test that all methods return self for chaining."""
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
        self.assertIs(qb.order_by("m"), qb)
        self.assertIs(qb.expand("n"), qb)
        self.assertIs(qb.top(10), qb)

    def test_build_does_not_include_empty_params(self):
        """Test that build() omits empty optional parameters."""
        qb = QueryBuilder("account")
        params = qb.build()
        self.assertNotIn("select", params)
        self.assertNotIn("filter", params)
        self.assertNotIn("orderby", params)
        self.assertNotIn("expand", params)
        self.assertNotIn("top", params)

    def test_build_returns_new_lists(self):
        """Test that build() returns copies of internal lists."""
        qb = QueryBuilder("account").select("name")
        params1 = qb.build()
        params2 = qb.build()
        # Should be equal but not the same object
        self.assertEqual(params1["select"], params2["select"])
        self.assertIsNot(params1["select"], params2["select"])

    def test_page_size(self):
        """Test page_size setting."""
        qb = QueryBuilder("account").page_size(50)
        params = qb.build()
        self.assertEqual(params["page_size"], 50)

    def test_page_size_invalid_raises(self):
        """Test that page_size with invalid value raises."""
        qb = QueryBuilder("account")
        with self.assertRaises(ValueError):
            qb.page_size(0)
        with self.assertRaises(ValueError):
            qb.page_size(-1)

    def test_page_size_chaining(self):
        """Test that page_size returns self for chaining."""
        qb = QueryBuilder("account")
        self.assertIs(qb.page_size(50), qb)

    def test_page_size_not_included_when_not_set(self):
        """Test that page_size is omitted from build() when not set."""
        qb = QueryBuilder("account")
        params = qb.build()
        self.assertNotIn("page_size", params)

    def test_full_query_with_page_size(self):
        """Test building a complete query including page_size."""
        qb = (
            QueryBuilder("account")
            .select("name")
            .filter_eq("statecode", 0)
            .top(100)
            .page_size(50)
        )
        params = qb.build()
        self.assertEqual(params["top"], 100)
        self.assertEqual(params["page_size"], 50)


class TestBoundQueryBuilder(unittest.TestCase):
    """Test cases for the BoundQueryBuilder class."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock query_ops for testing
        self.mock_query_ops = unittest.mock.MagicMock()

    def test_basic_construction(self):
        """Test basic BoundQueryBuilder construction."""
        bqb = BoundQueryBuilder("account", self.mock_query_ops)
        self.assertEqual(bqb.table, "account")

    def test_select(self):
        """Test select method delegates to internal builder."""
        bqb = BoundQueryBuilder("account", self.mock_query_ops)
        result = bqb.select("name", "revenue")
        self.assertIs(result, bqb)
        params = bqb.build()
        self.assertEqual(params["select"], ["name", "revenue"])

    def test_filter_eq(self):
        """Test filter_eq method delegates to internal builder."""
        bqb = BoundQueryBuilder("account", self.mock_query_ops)
        result = bqb.filter_eq("statecode", 0)
        self.assertIs(result, bqb)
        params = bqb.build()
        self.assertEqual(params["filter"], "statecode eq 0")

    def test_filter_ne(self):
        """Test filter_ne method."""
        bqb = BoundQueryBuilder("account", self.mock_query_ops).filter_ne("statecode", 1)
        self.assertEqual(bqb.build()["filter"], "statecode ne 1")

    def test_filter_gt(self):
        """Test filter_gt method."""
        bqb = BoundQueryBuilder("account", self.mock_query_ops).filter_gt("revenue", 1000)
        self.assertEqual(bqb.build()["filter"], "revenue gt 1000")

    def test_filter_ge(self):
        """Test filter_ge method."""
        bqb = BoundQueryBuilder("account", self.mock_query_ops).filter_ge("revenue", 1000)
        self.assertEqual(bqb.build()["filter"], "revenue ge 1000")

    def test_filter_lt(self):
        """Test filter_lt method."""
        bqb = BoundQueryBuilder("account", self.mock_query_ops).filter_lt("revenue", 1000)
        self.assertEqual(bqb.build()["filter"], "revenue lt 1000")

    def test_filter_le(self):
        """Test filter_le method."""
        bqb = BoundQueryBuilder("account", self.mock_query_ops).filter_le("revenue", 1000)
        self.assertEqual(bqb.build()["filter"], "revenue le 1000")

    def test_filter_contains(self):
        """Test filter_contains method."""
        bqb = BoundQueryBuilder("account", self.mock_query_ops).filter_contains("name", "Corp")
        self.assertEqual(bqb.build()["filter"], "contains(name, 'Corp')")

    def test_filter_startswith(self):
        """Test filter_startswith method."""
        bqb = BoundQueryBuilder("account", self.mock_query_ops).filter_startswith("name", "Con")
        self.assertEqual(bqb.build()["filter"], "startswith(name, 'Con')")

    def test_filter_endswith(self):
        """Test filter_endswith method."""
        bqb = BoundQueryBuilder("account", self.mock_query_ops).filter_endswith("name", "Ltd")
        self.assertEqual(bqb.build()["filter"], "endswith(name, 'Ltd')")

    def test_filter_null(self):
        """Test filter_null method."""
        bqb = BoundQueryBuilder("account", self.mock_query_ops).filter_null("telephone1")
        self.assertEqual(bqb.build()["filter"], "telephone1 eq null")

    def test_filter_not_null(self):
        """Test filter_not_null method."""
        bqb = BoundQueryBuilder("account", self.mock_query_ops).filter_not_null("telephone1")
        self.assertEqual(bqb.build()["filter"], "telephone1 ne null")

    def test_filter_raw(self):
        """Test filter_raw method."""
        bqb = BoundQueryBuilder("account", self.mock_query_ops).filter_raw("(a eq 1 or b eq 2)")
        self.assertEqual(bqb.build()["filter"], "(a eq 1 or b eq 2)")

    def test_order_by(self):
        """Test order_by method."""
        bqb = BoundQueryBuilder("account", self.mock_query_ops).order_by("revenue", descending=True)
        self.assertEqual(bqb.build()["orderby"], ["revenue desc"])

    def test_top(self):
        """Test top method."""
        bqb = BoundQueryBuilder("account", self.mock_query_ops).top(10)
        self.assertEqual(bqb.build()["top"], 10)

    def test_page_size(self):
        """Test page_size method."""
        bqb = BoundQueryBuilder("account", self.mock_query_ops).page_size(50)
        self.assertEqual(bqb.build()["page_size"], 50)

    def test_expand(self):
        """Test expand method."""
        bqb = BoundQueryBuilder("account", self.mock_query_ops).expand("primarycontactid")
        self.assertEqual(bqb.build()["expand"], ["primarycontactid"])

    def test_method_chaining(self):
        """Test that all methods return self for chaining."""
        bqb = BoundQueryBuilder("account", self.mock_query_ops)

        self.assertIs(bqb.select("name"), bqb)
        self.assertIs(bqb.filter_eq("a", 1), bqb)
        self.assertIs(bqb.filter_ne("b", 2), bqb)
        self.assertIs(bqb.filter_gt("c", 3), bqb)
        self.assertIs(bqb.filter_ge("d", 4), bqb)
        self.assertIs(bqb.filter_lt("e", 5), bqb)
        self.assertIs(bqb.filter_le("f", 6), bqb)
        self.assertIs(bqb.filter_contains("g", "x"), bqb)
        self.assertIs(bqb.filter_startswith("h", "y"), bqb)
        self.assertIs(bqb.filter_endswith("i", "z"), bqb)
        self.assertIs(bqb.filter_null("j"), bqb)
        self.assertIs(bqb.filter_not_null("k"), bqb)
        self.assertIs(bqb.filter_raw("l eq 1"), bqb)
        self.assertIs(bqb.order_by("m"), bqb)
        self.assertIs(bqb.expand("n"), bqb)
        self.assertIs(bqb.top(10), bqb)
        self.assertIs(bqb.page_size(50), bqb)

    def test_execute_calls_query_ops_get(self):
        """Test that execute() calls query_ops.get() with correct parameters."""
        bqb = (
            BoundQueryBuilder("account", self.mock_query_ops)
            .select("name", "revenue")
            .filter_eq("statecode", 0)
            .order_by("revenue", descending=True)
            .top(100)
            .page_size(50)
            .expand("primarycontactid")
        )

        bqb.execute()

        self.mock_query_ops.get.assert_called_once_with(
            "account",
            select=["name", "revenue"],
            filter="statecode eq 0",
            orderby=["revenue desc"],
            top=100,
            expand=["primarycontactid"],
            page_size=50,
        )

    def test_execute_returns_query_ops_result(self):
        """Test that execute() returns the result from query_ops.get()."""
        mock_result = [{"name": "Test"}]
        self.mock_query_ops.get.return_value = mock_result

        bqb = BoundQueryBuilder("account", self.mock_query_ops)
        result = bqb.execute()

        self.assertEqual(result, mock_result)


if __name__ == "__main__":
    unittest.main()
