# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for composable OData filter expressions."""

import unittest
import uuid
from datetime import date, datetime, timezone, timedelta

from PowerPlatform.Dataverse.models.filters import (
    FilterExpression,
    _format_value,
    eq,
    ne,
    gt,
    ge,
    lt,
    le,
    contains,
    startswith,
    endswith,
    between,
    is_null,
    is_not_null,
    filter_in,
    raw,
)


class TestFormatValue(unittest.TestCase):
    """Tests for _format_value()."""

    def test_none(self):
        self.assertEqual(_format_value(None), "null")

    def test_bool_true(self):
        self.assertEqual(_format_value(True), "true")

    def test_bool_false(self):
        self.assertEqual(_format_value(False), "false")

    def test_bool_before_int(self):
        """bool is a subclass of int; must format as true/false, not 1/0."""
        self.assertEqual(_format_value(True), "true")
        self.assertNotEqual(_format_value(True), "1")

    def test_int(self):
        self.assertEqual(_format_value(42), "42")

    def test_int_zero(self):
        self.assertEqual(_format_value(0), "0")

    def test_int_negative(self):
        self.assertEqual(_format_value(-5), "-5")

    def test_float(self):
        self.assertEqual(_format_value(3.14), "3.14")

    def test_float_integer_value(self):
        self.assertEqual(_format_value(1000000.0), "1000000.0")

    def test_int_enum(self):
        from enum import IntEnum

        class Priority(IntEnum):
            LOW = 1
            HIGH = 2

        self.assertEqual(_format_value(Priority.HIGH), "2")
        self.assertEqual(_format_value(Priority.LOW), "1")

    def test_str_enum(self):
        from enum import Enum

        class Color(Enum):
            RED = "red"

        self.assertEqual(_format_value(Color.RED), "'red'")

    def test_string(self):
        self.assertEqual(_format_value("hello"), "'hello'")

    def test_string_with_single_quotes(self):
        self.assertEqual(_format_value("O'Brien"), "'O''Brien'")

    def test_string_with_multiple_quotes(self):
        self.assertEqual(_format_value("O'Brien's Corp"), "'O''Brien''s Corp'")

    def test_string_empty(self):
        self.assertEqual(_format_value(""), "''")

    def test_datetime_naive(self):
        """Naive datetimes are assumed UTC."""
        dt = datetime(2024, 1, 15, 10, 30, 0)
        self.assertEqual(_format_value(dt), "2024-01-15T10:30:00Z")

    def test_datetime_utc(self):
        dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=timezone.utc)
        self.assertEqual(_format_value(dt), "2024-01-15T10:30:00Z")

    def test_datetime_with_microseconds(self):
        """Microseconds should be preserved when non-zero."""
        dt = datetime(2024, 1, 15, 10, 30, 0, 123456)
        self.assertEqual(_format_value(dt), "2024-01-15T10:30:00.123456Z")

    def test_datetime_non_utc_converted(self):
        """Timezone-aware non-UTC datetimes should be converted to UTC."""
        eastern = timezone(timedelta(hours=-5))
        dt = datetime(2024, 1, 15, 10, 30, 0, tzinfo=eastern)
        # 10:30 EST = 15:30 UTC
        self.assertEqual(_format_value(dt), "2024-01-15T15:30:00Z")

    def test_date(self):
        d = date(2024, 1, 15)
        self.assertEqual(_format_value(d), "2024-01-15")

    def test_uuid(self):
        uid = uuid.UUID("12345678-1234-1234-1234-123456789abc")
        self.assertEqual(_format_value(uid), "12345678-1234-1234-1234-123456789abc")

    def test_fallback_converts_to_string(self):
        """Unknown types fall back to str()."""

        class Custom:
            def __str__(self):
                return "custom-value"

        self.assertEqual(_format_value(Custom()), "custom-value")


class TestComparisonFilters(unittest.TestCase):
    """Tests for comparison filter factory functions."""

    def test_eq_string(self):
        self.assertEqual(eq("name", "Contoso").to_odata(), "name eq 'Contoso'")

    def test_eq_int(self):
        self.assertEqual(eq("statecode", 0).to_odata(), "statecode eq 0")

    def test_eq_none(self):
        self.assertEqual(eq("phone", None).to_odata(), "phone eq null")

    def test_eq_bool(self):
        self.assertEqual(eq("active", True).to_odata(), "active eq true")

    def test_ne(self):
        self.assertEqual(ne("statecode", 1).to_odata(), "statecode ne 1")

    def test_gt(self):
        self.assertEqual(gt("revenue", 1000000).to_odata(), "revenue gt 1000000")

    def test_ge(self):
        self.assertEqual(ge("revenue", 1000000).to_odata(), "revenue ge 1000000")

    def test_lt(self):
        self.assertEqual(lt("revenue", 500000).to_odata(), "revenue lt 500000")

    def test_le(self):
        self.assertEqual(le("revenue", 500000).to_odata(), "revenue le 500000")

    def test_column_name_lowercased(self):
        self.assertEqual(eq("StateCode", 0).to_odata(), "statecode eq 0")

    def test_eq_float(self):
        self.assertEqual(eq("revenue", 1000000.5).to_odata(), "revenue eq 1000000.5")

    def test_eq_uuid(self):
        uid = uuid.UUID("12345678-1234-1234-1234-123456789abc")
        self.assertEqual(
            eq("accountid", uid).to_odata(),
            "accountid eq 12345678-1234-1234-1234-123456789abc",
        )


class TestFunctionFilters(unittest.TestCase):
    """Tests for string function filter factory functions."""

    def test_contains(self):
        self.assertEqual(contains("name", "Corp").to_odata(), "contains(name, 'Corp')")

    def test_startswith(self):
        self.assertEqual(startswith("name", "Con").to_odata(), "startswith(name, 'Con')")

    def test_endswith(self):
        self.assertEqual(endswith("name", "Ltd").to_odata(), "endswith(name, 'Ltd')")

    def test_function_column_lowercased(self):
        self.assertEqual(contains("Name", "Corp").to_odata(), "contains(name, 'Corp')")


class TestBetween(unittest.TestCase):
    """Tests for the between factory function."""

    def test_between_ints(self):
        self.assertEqual(
            between("revenue", 100000, 500000).to_odata(),
            "(revenue ge 100000 and revenue le 500000)",
        )

    def test_between_dates(self):
        result = between("createdon", date(2024, 1, 1), date(2024, 12, 31)).to_odata()
        self.assertEqual(result, "(createdon ge 2024-01-01 and createdon le 2024-12-31)")


class TestNullChecks(unittest.TestCase):
    """Tests for is_null and is_not_null."""

    def test_is_null(self):
        self.assertEqual(is_null("phone").to_odata(), "phone eq null")

    def test_is_not_null(self):
        self.assertEqual(is_not_null("phone").to_odata(), "phone ne null")


class TestRawFilter(unittest.TestCase):
    """Tests for the raw filter function."""

    def test_raw(self):
        expr = raw("Microsoft.Dynamics.CRM.Today(PropertyName='createdon')")
        self.assertEqual(
            expr.to_odata(),
            "Microsoft.Dynamics.CRM.Today(PropertyName='createdon')",
        )

    def test_raw_passthrough(self):
        """Raw filter should pass through exactly as given."""
        text = "(statecode eq 0 or statecode eq 1)"
        self.assertEqual(raw(text).to_odata(), text)


class TestInFilter(unittest.TestCase):
    """Tests for the filter_in factory function."""

    def test_filter_in_ints(self):
        self.assertEqual(
            filter_in("statecode", [0, 1, 2]).to_odata(),
            "Microsoft.Dynamics.CRM.In(PropertyName='statecode',PropertyValues=[0, 1, 2])",
        )

    def test_filter_in_strings(self):
        self.assertEqual(
            filter_in("name", ["Contoso", "Fabrikam"]).to_odata(),
            "Microsoft.Dynamics.CRM.In(PropertyName='name',PropertyValues=['Contoso', 'Fabrikam'])",
        )

    def test_filter_in_single_value(self):
        self.assertEqual(
            filter_in("statecode", [0]).to_odata(),
            "Microsoft.Dynamics.CRM.In(PropertyName='statecode',PropertyValues=[0])",
        )

    def test_filter_in_column_lowercased(self):
        self.assertEqual(
            filter_in("StateCode", [0, 1]).to_odata(),
            "Microsoft.Dynamics.CRM.In(PropertyName='statecode',PropertyValues=[0, 1])",
        )

    def test_filter_in_empty_raises(self):
        with self.assertRaises(ValueError):
            filter_in("statecode", [])


class TestLogicalOperators(unittest.TestCase):
    """Tests for &, |, ~ operator overloads."""

    def test_and_operator(self):
        self.assertEqual(
            (eq("a", 1) & eq("b", 2)).to_odata(),
            "(a eq 1 and b eq 2)",
        )

    def test_or_operator(self):
        self.assertEqual(
            (eq("a", 1) | eq("b", 2)).to_odata(),
            "(a eq 1 or b eq 2)",
        )

    def test_not_operator(self):
        self.assertEqual(
            (~eq("a", 1)).to_odata(),
            "not (a eq 1)",
        )

    def test_complex_composition(self):
        """(statecode in {0,1}) AND (revenue > 100k)"""
        expr = (eq("statecode", 0) | eq("statecode", 1)) & gt("revenue", 100000)
        self.assertEqual(
            expr.to_odata(),
            "((statecode eq 0 or statecode eq 1) and revenue gt 100000)",
        )

    def test_triple_and(self):
        expr = eq("a", 1) & eq("b", 2) & eq("c", 3)
        self.assertEqual(
            expr.to_odata(),
            "((a eq 1 and b eq 2) and c eq 3)",
        )

    def test_not_or(self):
        expr = ~(eq("a", 1) | eq("b", 2))
        self.assertEqual(
            expr.to_odata(),
            "not ((a eq 1 or b eq 2))",
        )

    def test_and_with_non_expression_returns_not_implemented(self):
        result = eq("a", 1).__and__("not an expression")
        self.assertIs(result, NotImplemented)

    def test_or_with_non_expression_returns_not_implemented(self):
        result = eq("a", 1).__or__("not an expression")
        self.assertIs(result, NotImplemented)


class TestStrAndRepr(unittest.TestCase):
    """Tests for __str__ and __repr__."""

    def test_str_delegates_to_odata(self):
        self.assertEqual(str(eq("a", 1)), "a eq 1")

    def test_repr_includes_class_name(self):
        r = repr(eq("a", 1))
        self.assertIn("_ComparisonFilter", r)
        self.assertIn("a eq 1", r)


class TestFilterExpressionBase(unittest.TestCase):
    """Tests for the FilterExpression base class."""

    def test_base_to_odata_raises(self):
        with self.assertRaises(NotImplementedError):
            FilterExpression().to_odata()


if __name__ == "__main__":
    unittest.main()
