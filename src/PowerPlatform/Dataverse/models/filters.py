# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Composable OData filter expressions for the Dataverse SDK.

Provides an expression tree that compiles to OData ``$filter`` strings,
with Python operator overloads (``&``, ``|``, ``~``) for composing
complex filter conditions.

Example::

    from PowerPlatform.Dataverse.models.filters import col, raw

    # Preferred GA idiom — col() proxy
    expr = col("statecode") == 0
    print(expr.to_odata())  # statecode eq 0

    # Complex composition with OR and AND
    expr = (col("statecode") == 0) | (col("statecode") == 1) & (col("revenue") > 100000)
    print(expr.to_odata())

    # In / not-in
    expr = col("statecode").in_([0, 1, 2])
    print(expr.to_odata())
    # Microsoft.Dynamics.CRM.In(PropertyName='statecode',PropertyValues=["0","1","2"])

    # Raw OData escape hatch (no deprecation warning)
    expr = raw("Microsoft.Dynamics.CRM.Today(PropertyName='createdon')")

    # Negation
    expr = ~(col("statecode") == 1)
    print(expr.to_odata())  # not (statecode eq 1)
"""

from __future__ import annotations

import enum
import uuid
import warnings
from datetime import date, datetime, timezone
from typing import Any, Collection, List

__all__ = [
    "FilterExpression",
    "ColumnProxy",
    "col",
    "raw",
    # Deprecated factories — still functional, fire DeprecationWarning on call:
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
    "not_in",
    "not_between",
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

    def __init__(self, column: str, values: Collection[Any]) -> None:
        if not values:
            raise ValueError("filter_in requires at least one value")
        self.column = column.lower()
        self.values = list(values)

    def to_odata(self) -> str:
        # PropertyValues is Collection(Edm.String)
        parts = [f'"{_format_value(v).strip("'")}"' for v in self.values]
        formatted = ",".join(parts)
        return f"Microsoft.Dynamics.CRM.In" f"(PropertyName='{self.column}',PropertyValues=[{formatted}])"


class _NotInFilter(FilterExpression):
    """Not-in filter using ``Microsoft.Dynamics.CRM.NotIn``."""

    __slots__ = ("column", "values")

    def __init__(self, column: str, values: Collection[Any]) -> None:
        if not values:
            raise ValueError("not_in requires at least one value")
        self.column = column.lower()
        self.values = list(values)

    def to_odata(self) -> str:
        # Same Collection(Edm.String) rules as _InFilter.
        parts = [f'"{_format_value(v).strip("'")}"' for v in self.values]
        formatted = ",".join(parts)
        return f"Microsoft.Dynamics.CRM.NotIn" f"(PropertyName='{self.column}',PropertyValues=[{formatted}])"


class _RawFilter(FilterExpression):
    """Raw verbatim OData filter expression."""

    __slots__ = ("filter_string",)

    def __init__(self, filter_string: str) -> None:
        self.filter_string = filter_string

    def to_odata(self) -> str:
        return self.filter_string


# ---------------------------------------------------------------------------
# Private implementation helpers (no warnings — used internally and by col())
# ---------------------------------------------------------------------------


def _eq_impl(column: str, value: Any) -> FilterExpression:
    return _ComparisonFilter(column, "eq", value)


def _ne_impl(column: str, value: Any) -> FilterExpression:
    return _ComparisonFilter(column, "ne", value)


def _gt_impl(column: str, value: Any) -> FilterExpression:
    return _ComparisonFilter(column, "gt", value)


def _ge_impl(column: str, value: Any) -> FilterExpression:
    return _ComparisonFilter(column, "ge", value)


def _lt_impl(column: str, value: Any) -> FilterExpression:
    return _ComparisonFilter(column, "lt", value)


def _le_impl(column: str, value: Any) -> FilterExpression:
    return _ComparisonFilter(column, "le", value)


def _contains_impl(column: str, value: str) -> FilterExpression:
    return _FunctionFilter("contains", column, value)


def _startswith_impl(column: str, value: str) -> FilterExpression:
    return _FunctionFilter("startswith", column, value)


def _endswith_impl(column: str, value: str) -> FilterExpression:
    return _FunctionFilter("endswith", column, value)


def _in_impl(column: str, values: Collection[Any]) -> FilterExpression:
    return _InFilter(column, values)


def _not_in_impl(column: str, values: Collection[Any]) -> FilterExpression:
    return _NotInFilter(column, values)


# ---------------------------------------------------------------------------
# ColumnProxy — GA idiom for building filter expressions
# ---------------------------------------------------------------------------

_LIKE_WILDCARD = "%"


def _compile_like(column: str, pattern: str) -> FilterExpression:
    """Compile a LIKE-style pattern to an OData FilterExpression.

    Pattern rules:
    - ``val%``   → ``startswith(column, 'val')``
    - ``%val``   → ``endswith(column, 'val')``
    - ``%val%``  → ``contains(column, 'val')``
    - ``val``    (no wildcard) → ``column eq 'val'`` (equality)
    - Anything else  → :class:`ValueError`

    :param column: Lowercased column name.
    :param pattern: The LIKE pattern string.
    :raises ValueError: If the pattern contains wildcards in unsupported positions.
    """
    has_start = pattern.startswith(_LIKE_WILDCARD)
    has_end = pattern.endswith(_LIKE_WILDCARD)
    inner = pattern.strip(_LIKE_WILDCARD)

    # Detect non-reducible interior wildcards: after stripping the leading/trailing
    # % the inner value must contain no further % characters.
    if _LIKE_WILDCARD in inner:
        raise ValueError(
            f"like() pattern {pattern!r} is not reducible to a single OData function. "
            "Use raw(), fetch_xml(), or query.sql() for complex wildcard patterns."
        )

    if not has_start and has_end:
        # "val%" — startswith
        return _startswith_impl(column, inner)
    if has_start and not has_end:
        # "%val" — endswith
        return _endswith_impl(column, inner)
    if has_start and has_end:
        # "%val%" — contains
        return _contains_impl(column, inner)
    # No wildcard at all — exact equality
    return _eq_impl(column, pattern)


class ColumnProxy:
    """Fluent proxy for building OData filter expressions from a column name.

    Returned by :func:`col`. Operator overloads and methods produce
    :class:`FilterExpression` instances that can be passed to
    ``QueryBuilder.where()``.

    Example::

        from PowerPlatform.Dataverse.models.filters import col

        expr = col("statecode") == 0               # equality
        expr = col("revenue") > 1_000_000          # comparison
        expr = col("name").like("Contoso%")        # startswith
        expr = col("name").is_null()               # null check
        expr = col("statecode").in_([0, 1])        # in
    """

    __slots__ = ("_column",)

    def __init__(self, name: str) -> None:
        if not name or not name.strip():
            raise ValueError("col() requires a non-empty column name")
        self._column = name.strip().lower()

    # ---------------------------------------------------------------- comparisons

    def __eq__(self, other: Any) -> FilterExpression:  # type: ignore[override]
        return _eq_impl(self._column, other)

    def __ne__(self, other: Any) -> FilterExpression:  # type: ignore[override]
        return _ne_impl(self._column, other)

    def __gt__(self, other: Any) -> FilterExpression:
        return _gt_impl(self._column, other)

    def __ge__(self, other: Any) -> FilterExpression:
        return _ge_impl(self._column, other)

    def __lt__(self, other: Any) -> FilterExpression:
        return _lt_impl(self._column, other)

    def __le__(self, other: Any) -> FilterExpression:
        return _le_impl(self._column, other)

    # ---------------------------------------------------------------- null checks

    def is_null(self) -> FilterExpression:
        """Column equals null: ``column eq null``."""
        return _eq_impl(self._column, None)

    def is_not_null(self) -> FilterExpression:
        """Column not null: ``column ne null``."""
        return _ne_impl(self._column, None)

    # ---------------------------------------------------------------- in / not-in

    def in_(self, values: Collection[Any]) -> FilterExpression:
        """In filter using ``Microsoft.Dynamics.CRM.In``.

        :param values: Non-empty collection of values.
        :raises ValueError: If ``values`` is empty.
        """
        return _in_impl(self._column, values)

    def not_in(self, values: Collection[Any]) -> FilterExpression:
        """Not-in filter using ``Microsoft.Dynamics.CRM.NotIn``.

        :param values: Non-empty collection of values.
        :raises ValueError: If ``values`` is empty.
        """
        return _not_in_impl(self._column, values)

    # ---------------------------------------------------------------- range

    def between(self, lo: Any, hi: Any) -> FilterExpression:
        """Between filter: ``(column ge lo and column le hi)``."""
        return _ge_impl(self._column, lo) & _le_impl(self._column, hi)

    def not_between(self, lo: Any, hi: Any) -> FilterExpression:
        """Not-between filter: ``not (column ge lo and column le hi)``."""
        return ~(self.between(lo, hi))

    # ---------------------------------------------------------------- string functions

    def contains(self, value: str) -> FilterExpression:
        """Contains filter: ``contains(column, value)``."""
        return _contains_impl(self._column, value)

    def startswith(self, value: str) -> FilterExpression:
        """Startswith filter: ``startswith(column, value)``."""
        return _startswith_impl(self._column, value)

    def endswith(self, value: str) -> FilterExpression:
        """Endswith filter: ``endswith(column, value)``."""
        return _endswith_impl(self._column, value)

    # ---------------------------------------------------------------- like / not_like

    def like(self, pattern: str) -> FilterExpression:
        """Pattern-match filter compiled to the closest OData equivalent.

        +-----------------+-----------------------------+-------------------------------------+
        | Pattern form    | Example                     | Compiles to                         |
        +=================+=============================+=====================================+
        | ``val%``        | ``like("Contoso%")``        | ``startswith(column,'Contoso')``     |
        +-----------------+-----------------------------+-------------------------------------+
        | ``%val``        | ``like("%Ltd")``            | ``endswith(column,'Ltd')``          |
        +-----------------+-----------------------------+-------------------------------------+
        | ``%val%``       | ``like("%Corp%")``          | ``contains(column,'Corp')``         |
        +-----------------+-----------------------------+-------------------------------------+
        | No wildcard     | ``like("Contoso")``         | ``column eq 'Contoso'``             |
        +-----------------+-----------------------------+-------------------------------------+
        | Other           | ``like("Con%oso")``         | :class:`ValueError`                 |
        +-----------------+-----------------------------+-------------------------------------+

        :param pattern: LIKE-style pattern string.
        :raises ValueError: If the pattern cannot be reduced to a single OData function.
        """
        return _compile_like(self._column, pattern)

    def not_like(self, pattern: str) -> FilterExpression:
        """Negated pattern-match filter; mirrors :meth:`like` rules then negates.

        :param pattern: LIKE-style pattern string (same rules as :meth:`like`).
        :raises ValueError: If the pattern cannot be reduced to a single OData function.
        """
        return ~_compile_like(self._column, pattern)

    # ---------------------------------------------------------------- hash / repr

    def __hash__(self) -> int:
        return hash(self._column)

    def __repr__(self) -> str:
        return f"ColumnProxy({self._column!r})"


# ---------------------------------------------------------------------------
# Public factory: col() — no deprecation warning
# ---------------------------------------------------------------------------


def col(name: str) -> ColumnProxy:
    """Return a :class:`ColumnProxy` for building filter expressions.

    This is the preferred GA idiom for constructing filter expressions::

        from PowerPlatform.Dataverse.models.filters import col

        expr = col("statecode") == 0
        expr = col("revenue") > 1_000_000
        expr = col("name").like("Contoso%")
        expr = col("statecode").in_([0, 1])
        expr = col("parentaccountid").is_null()

    :param name: Column logical name (case-insensitive, will be lowercased).
    :return: A :class:`ColumnProxy` bound to the column.
    :raises ValueError: If ``name`` is empty.
    """
    return ColumnProxy(name)


# ---------------------------------------------------------------------------
# Public factory: raw() — no deprecation warning (OData escape hatch)
# ---------------------------------------------------------------------------


def raw(filter_string: str) -> FilterExpression:
    """Verbatim OData filter expression (passed through unchanged).

    This function is **not** deprecated — it is the OData escape hatch with
    no typed replacement.

    :param filter_string: Raw OData filter string.
    :return: A :class:`FilterExpression`.

    Example::

        raw("Microsoft.Dynamics.CRM.Today(PropertyName='createdon')")
    """
    return _RawFilter(filter_string)


# ---------------------------------------------------------------------------
# Deprecated public factory functions — fire DeprecationWarning on CALL
# ---------------------------------------------------------------------------

_DEP_MSG = "'{name}' is deprecated and will be removed in a future release. " "Use {replacement} instead."


def eq(column: str, value: Any) -> FilterExpression:
    """Equality filter: ``column eq value``.

    .. deprecated::
        Use ``col(column) == value`` instead.
    """
    warnings.warn(
        _DEP_MSG.format(name="eq", replacement="col('column') == value"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _eq_impl(column, value)


def ne(column: str, value: Any) -> FilterExpression:
    """Not-equal filter: ``column ne value``.

    .. deprecated::
        Use ``col(column) != value`` instead.
    """
    warnings.warn(
        _DEP_MSG.format(name="ne", replacement="col('column') != value"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _ne_impl(column, value)


def gt(column: str, value: Any) -> FilterExpression:
    """Greater-than filter: ``column gt value``.

    .. deprecated::
        Use ``col(column) > value`` instead.
    """
    warnings.warn(
        _DEP_MSG.format(name="gt", replacement="col('column') > value"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _gt_impl(column, value)


def ge(column: str, value: Any) -> FilterExpression:
    """Greater-than-or-equal filter: ``column ge value``.

    .. deprecated::
        Use ``col(column) >= value`` instead.
    """
    warnings.warn(
        _DEP_MSG.format(name="ge", replacement="col('column') >= value"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _ge_impl(column, value)


def lt(column: str, value: Any) -> FilterExpression:
    """Less-than filter: ``column lt value``.

    .. deprecated::
        Use ``col(column) < value`` instead.
    """
    warnings.warn(
        _DEP_MSG.format(name="lt", replacement="col('column') < value"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _lt_impl(column, value)


def le(column: str, value: Any) -> FilterExpression:
    """Less-than-or-equal filter: ``column le value``.

    .. deprecated::
        Use ``col(column) <= value`` instead.
    """
    warnings.warn(
        _DEP_MSG.format(name="le", replacement="col('column') <= value"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _le_impl(column, value)


def contains(column: str, value: str) -> FilterExpression:
    """Contains filter: ``contains(column, value)``.

    .. deprecated::
        Use ``col(column).contains(value)`` instead.
    """
    warnings.warn(
        _DEP_MSG.format(name="contains", replacement="col('column').contains(value)"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _contains_impl(column, value)


def startswith(column: str, value: str) -> FilterExpression:
    """Startswith filter: ``startswith(column, value)``.

    .. deprecated::
        Use ``col(column).startswith(value)`` instead.
    """
    warnings.warn(
        _DEP_MSG.format(name="startswith", replacement="col('column').startswith(value)"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _startswith_impl(column, value)


def endswith(column: str, value: str) -> FilterExpression:
    """Endswith filter: ``endswith(column, value)``.

    .. deprecated::
        Use ``col(column).endswith(value)`` instead.
    """
    warnings.warn(
        _DEP_MSG.format(name="endswith", replacement="col('column').endswith(value)"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _endswith_impl(column, value)


def between(column: str, low: Any, high: Any) -> FilterExpression:
    """Between filter: ``(column ge low and column le high)``.

    .. deprecated::
        Use ``col(column).between(low, high)`` instead.
    """
    warnings.warn(
        _DEP_MSG.format(name="between", replacement="col('column').between(low, high)"),
        DeprecationWarning,
        stacklevel=2,
    )
    # Use private helpers to avoid chaining through the deprecated ge/le wrappers
    return _ge_impl(column, low) & _le_impl(column, high)


def is_null(column: str) -> FilterExpression:
    """Null check: ``column eq null``.

    .. deprecated::
        Use ``col(column).is_null()`` instead.
    """
    warnings.warn(
        _DEP_MSG.format(name="is_null", replacement="col('column').is_null()"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _eq_impl(column, None)


def is_not_null(column: str) -> FilterExpression:
    """Not-null check: ``column ne null``.

    .. deprecated::
        Use ``col(column).is_not_null()`` instead.
    """
    warnings.warn(
        _DEP_MSG.format(name="is_not_null", replacement="col('column').is_not_null()"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _ne_impl(column, None)


def filter_in(column: str, values: Collection[Any]) -> FilterExpression:
    """In filter using ``Microsoft.Dynamics.CRM.In``.

    Named ``filter_in`` because ``in`` is a Python keyword.

    .. deprecated::
        Use ``col(column).in_(values)`` instead.

    :raises ValueError: If ``values`` is empty.
    """
    warnings.warn(
        _DEP_MSG.format(name="filter_in", replacement="col('column').in_(values)"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _in_impl(column, values)


def not_in(column: str, values: Collection[Any]) -> FilterExpression:
    """Not-in filter using ``Microsoft.Dynamics.CRM.NotIn``.

    .. deprecated::
        Use ``col(column).not_in(values)`` instead.

    :raises ValueError: If ``values`` is empty.
    """
    warnings.warn(
        _DEP_MSG.format(name="not_in", replacement="col('column').not_in(values)"),
        DeprecationWarning,
        stacklevel=2,
    )
    return _not_in_impl(column, values)


def not_between(column: str, low: Any, high: Any) -> FilterExpression:
    """Not-between filter: ``not (column ge low and column le high)``.

    .. deprecated::
        Use ``col(column).not_between(low, high)`` instead.
    """
    warnings.warn(
        _DEP_MSG.format(name="not_between", replacement="col('column').not_between(low, high)"),
        DeprecationWarning,
        stacklevel=2,
    )
    # Use private helpers to avoid chaining through deprecated ge/le wrappers
    return ~(_ge_impl(column, low) & _le_impl(column, high))
