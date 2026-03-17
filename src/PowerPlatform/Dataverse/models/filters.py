# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Composable OData filter expressions for the Dataverse SDK.

Provides an expression tree that compiles to OData ``$filter`` strings,
with Python operator overloads (``&``, ``|``, ``~``) for composing
complex filter conditions.

Example::

    from PowerPlatform.Dataverse.models.filters import eq, gt, filter_in

    # Simple comparison
    expr = eq("statecode", 0)
    print(expr.to_odata())  # statecode eq 0

    # Complex composition with OR and AND
    expr = (eq("statecode", 0) | eq("statecode", 1)) & gt("revenue", 100000)
    print(expr.to_odata())
    # ((statecode eq 0 or statecode eq 1) and revenue gt 100000)

    # In operator (Dataverse function)
    expr = filter_in("statecode", [0, 1, 2])
    print(expr.to_odata())
    # Microsoft.Dynamics.CRM.In(PropertyName='statecode',PropertyValues=["0","1","2"])

    # Negation
    expr = ~eq("statecode", 1)
    print(expr.to_odata())  # not (statecode eq 1)
"""

from __future__ import annotations

import enum
import uuid
from datetime import date, datetime, timezone
from typing import Any, Sequence

__all__ = [
    "FilterExpression",
    "eq",
    "ne",
    "gt",
    "ge",
    "lt",
    "le",
    "contains",
    "startswith",
    "endswith",
    "between",
    "is_null",
    "is_not_null",
    "filter_in",
    "raw",
]


# ---------------------------------------------------------------------------
# Value formatting
# ---------------------------------------------------------------------------


def _format_value(value: Any) -> str:
    """Format a Python value for OData query syntax.

    Handles: ``None``, ``bool``, ``int``, ``float``, ``str``,
    ``datetime``, ``date``, ``uuid.UUID``.

    .. note::
        ``bool`` is checked before ``int`` because ``bool`` is a subclass
        of ``int`` in Python.  Without this ordering ``True`` would format
        as ``1`` instead of ``true``.
    """
    if value is None:
        return "null"
    # bool MUST be checked before int (bool is a subclass of int)
    if isinstance(value, bool):
        return "true" if value else "false"
    # Enum/IntEnum MUST be checked before int (IntEnum is a subclass of int)
    if isinstance(value, enum.Enum):
        return _format_value(value.value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(value)
    if isinstance(value, str):
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    if isinstance(value, datetime):
        # Convert timezone-aware datetimes to UTC; assume naive datetimes are UTC
        if value.tzinfo is not None:
            value = value.astimezone(timezone.utc)
        if value.microsecond:
            return value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        return value.strftime("%Y-%m-%dT%H:%M:%SZ")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, uuid.UUID):
        return str(value)
    # Fallback
    return str(value)


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------


class FilterExpression:
    """Base class for composable OData filter expressions.

    Supports Python operator overloads for logical composition:

    - ``expr1 & expr2`` produces ``(expr1 and expr2)``
    - ``expr1 | expr2`` produces ``(expr1 or expr2)``
    - ``~expr`` produces ``not (expr)``
    """

    def to_odata(self) -> str:
        """Compile this expression to an OData ``$filter`` string."""
        raise NotImplementedError

    def __and__(self, other: FilterExpression) -> FilterExpression:
        if not isinstance(other, FilterExpression):
            return NotImplemented
        return _AndFilter(self, other)

    def __or__(self, other: FilterExpression) -> FilterExpression:
        if not isinstance(other, FilterExpression):
            return NotImplemented
        return _OrFilter(self, other)

    def __invert__(self) -> FilterExpression:
        return _NotFilter(self)

    def __str__(self) -> str:
        return self.to_odata()

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.to_odata()!r})"


# ---------------------------------------------------------------------------
# Internal expression classes
# ---------------------------------------------------------------------------


class _ComparisonFilter(FilterExpression):
    """Comparison filter: ``column op value``."""

    __slots__ = ("column", "op", "value")

    def __init__(self, column: str, op: str, value: Any) -> None:
        self.column = column.lower()
        self.op = op
        self.value = value

    def to_odata(self) -> str:
        return f"{self.column} {self.op} {_format_value(self.value)}"


class _FunctionFilter(FilterExpression):
    """Function filter: ``func(column, value)``."""

    __slots__ = ("func_name", "column", "value")

    def __init__(self, func_name: str, column: str, value: Any) -> None:
        self.func_name = func_name
        self.column = column.lower()
        self.value = value

    def to_odata(self) -> str:
        return f"{self.func_name}({self.column}, {_format_value(self.value)})"


class _AndFilter(FilterExpression):
    """Logical AND: ``(left and right)``."""

    __slots__ = ("left", "right")

    def __init__(self, left: FilterExpression, right: FilterExpression) -> None:
        self.left = left
        self.right = right

    def to_odata(self) -> str:
        return f"({self.left.to_odata()} and {self.right.to_odata()})"


class _OrFilter(FilterExpression):
    """Logical OR: ``(left or right)``."""

    __slots__ = ("left", "right")

    def __init__(self, left: FilterExpression, right: FilterExpression) -> None:
        self.left = left
        self.right = right

    def to_odata(self) -> str:
        return f"({self.left.to_odata()} or {self.right.to_odata()})"


class _NotFilter(FilterExpression):
    """Logical NOT: ``not (expr)``."""

    __slots__ = ("expr",)

    def __init__(self, expr: FilterExpression) -> None:
        self.expr = expr

    def to_odata(self) -> str:
        return f"not ({self.expr.to_odata()})"


class _InFilter(FilterExpression):
    """In filter using ``Microsoft.Dynamics.CRM.In``."""

    __slots__ = ("column", "values")

    def __init__(self, column: str, values: Sequence[Any]) -> None:
        if not values:
            raise ValueError("filter_in requires at least one value")
        self.column = column.lower()
        self.values = list(values)

    def to_odata(self) -> str:
        # PropertyValues is Collection(Edm.String)
        formatted = ",".join(
            f'"{v.value if isinstance(v, enum.Enum) else v}"'
            for v in self.values
        )
        return (
            f"Microsoft.Dynamics.CRM.In"
            f"(PropertyName='{self.column}',PropertyValues=[{formatted}])"
        )


class _RawFilter(FilterExpression):
    """Raw verbatim OData filter expression."""

    __slots__ = ("filter_string",)

    def __init__(self, filter_string: str) -> None:
        self.filter_string = filter_string

    def to_odata(self) -> str:
        return self.filter_string


# ---------------------------------------------------------------------------
# Public factory functions
# ---------------------------------------------------------------------------


def eq(column: str, value: Any) -> FilterExpression:
    """Equality filter: ``column eq value``.

    :param column: Column name (will be lowercased).
    :param value: Value to compare against.
    :return: A filter expression.

    Example::

        eq("statecode", 0).to_odata()  # "statecode eq 0"
    """
    return _ComparisonFilter(column, "eq", value)


def ne(column: str, value: Any) -> FilterExpression:
    """Not-equal filter: ``column ne value``.

    :param column: Column name (will be lowercased).
    :param value: Value to compare against.
    :return: A filter expression.
    """
    return _ComparisonFilter(column, "ne", value)


def gt(column: str, value: Any) -> FilterExpression:
    """Greater-than filter: ``column gt value``.

    :param column: Column name (will be lowercased).
    :param value: Value to compare against.
    :return: A filter expression.
    """
    return _ComparisonFilter(column, "gt", value)


def ge(column: str, value: Any) -> FilterExpression:
    """Greater-than-or-equal filter: ``column ge value``.

    :param column: Column name (will be lowercased).
    :param value: Value to compare against.
    :return: A filter expression.
    """
    return _ComparisonFilter(column, "ge", value)


def lt(column: str, value: Any) -> FilterExpression:
    """Less-than filter: ``column lt value``.

    :param column: Column name (will be lowercased).
    :param value: Value to compare against.
    :return: A filter expression.
    """
    return _ComparisonFilter(column, "lt", value)


def le(column: str, value: Any) -> FilterExpression:
    """Less-than-or-equal filter: ``column le value``.

    :param column: Column name (will be lowercased).
    :param value: Value to compare against.
    :return: A filter expression.
    """
    return _ComparisonFilter(column, "le", value)


def contains(column: str, value: str) -> FilterExpression:
    """Contains filter: ``contains(column, value)``.

    :param column: Column name (will be lowercased).
    :param value: Substring to search for.
    :return: A filter expression.
    """
    return _FunctionFilter("contains", column, value)


def startswith(column: str, value: str) -> FilterExpression:
    """Startswith filter: ``startswith(column, value)``.

    :param column: Column name (will be lowercased).
    :param value: Prefix to match.
    :return: A filter expression.
    """
    return _FunctionFilter("startswith", column, value)


def endswith(column: str, value: str) -> FilterExpression:
    """Endswith filter: ``endswith(column, value)``.

    :param column: Column name (will be lowercased).
    :param value: Suffix to match.
    :return: A filter expression.
    """
    return _FunctionFilter("endswith", column, value)


def between(column: str, low: Any, high: Any) -> FilterExpression:
    """Between filter: ``(column ge low and column le high)``.

    Syntactic sugar that composes :func:`ge` and :func:`le` with ``&``.

    :param column: Column name (will be lowercased).
    :param low: Lower bound (inclusive).
    :param high: Upper bound (inclusive).
    :return: A composed filter expression.

    Example::

        between("revenue", 100000, 500000).to_odata()
        # "(revenue ge 100000 and revenue le 500000)"
    """
    return ge(column, low) & le(column, high)


def is_null(column: str) -> FilterExpression:
    """Null check: ``column eq null``.

    :param column: Column name (will be lowercased).
    :return: A filter expression.
    """
    return _ComparisonFilter(column, "eq", None)


def is_not_null(column: str) -> FilterExpression:
    """Not-null check: ``column ne null``.

    :param column: Column name (will be lowercased).
    :return: A filter expression.
    """
    return _ComparisonFilter(column, "ne", None)


def filter_in(column: str, values: Sequence[Any]) -> FilterExpression:
    """In filter using ``Microsoft.Dynamics.CRM.In``.

    Named ``filter_in`` because ``in`` is a Python keyword.

    :param column: Column name (will be lowercased).
    :param values: Non-empty sequence of values.
    :return: A filter expression.
    :raises ValueError: If ``values`` is empty.

    Example::

        filter_in("statecode", [0, 1, 2]).to_odata()
        # "Microsoft.Dynamics.CRM.In(PropertyName='statecode',PropertyValues=["0","1","2"])"
    """
    return _InFilter(column, values)


def raw(filter_string: str) -> FilterExpression:
    """Verbatim OData filter expression (passed through unchanged).

    :param filter_string: Raw OData filter string.
    :return: A filter expression.

    Example::

        raw("Microsoft.Dynamics.CRM.Today(PropertyName='createdon')")
    """
    return _RawFilter(filter_string)
