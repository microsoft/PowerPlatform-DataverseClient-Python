# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Fluent query builder for constructing OData queries.

Provides a type-safe, discoverable interface for building complex queries
against Dataverse tables with method chaining.

Example::

    # Via client (recommended)
    for page in (client.query.builder("account")
                 .select("name", "revenue")
                 .filter_eq("statecode", 0)
                 .filter_gt("revenue", 1000000)
                 .order_by("revenue", descending=True)
                 .top(100)
                 .page_size(50)
                 .execute()):
        for record in page:
            print(record["name"])

    # With composable expression tree
    from PowerPlatform.Dataverse.models.filters import eq, gt

    for page in (client.query.builder("account")
                 .select("name", "revenue")
                 .where((eq("statecode", 0) | eq("statecode", 1))
                        & gt("revenue", 100000))
                 .top(100)
                 .execute()):
        for record in page:
            print(record["name"])
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Union

from .filters import FilterExpression, _format_value


__all__ = ["QueryBuilder"]


class QueryBuilder:
    """Fluent interface for building OData queries.

    Provides method chaining for constructing complex queries with
    type-safe filter operations. Can be used standalone (via :meth:`build`)
    or bound to a client (via :meth:`execute`).

    :param table: Table schema name to query.
    :type table: str
    :raises ValueError: If ``table`` is empty.

    Example:
        Standalone query construction::

            query = (QueryBuilder("account")
                     .select("name")
                     .filter_eq("statecode", 0)
                     .top(10))
            params = query.build()
            # {"table": "account", "select": ["name"],
            #  "filter": "statecode eq 0", "top": 10}
    """

    def __init__(self, table: str) -> None:
        if not table or not table.strip():
            raise ValueError("table name is required")
        self.table = table
        self._select: List[str] = []
        self._filter_parts: List[Union[str, FilterExpression]] = []
        self._orderby: List[str] = []
        self._expand: List[str] = []
        self._top: Optional[int] = None
        self._page_size: Optional[int] = None
        self._query_ops: Optional[Any] = None  # Set by QueryOperations.builder()

    # ----------------------------------------------------------------- select

    def select(self, *columns: str) -> QueryBuilder:
        """Select specific columns to retrieve.

        Column names are passed as-is; the OData layer lowercases them
        automatically.  Can be called multiple times (additive).

        :param columns: Column names to select.
        :return: Self for method chaining.

        Example::

            query = QueryBuilder("account").select("name", "telephone1", "revenue")
        """
        self._select.extend(columns)
        return self

    # ----------------------------------------------------------- filter: comparison

    def filter_eq(self, column: str, value: Any) -> QueryBuilder:
        """Add equality filter: ``column eq value``.

        :param column: Column name (will be lowercased).
        :param value: Value to compare against.
        :return: Self for method chaining.
        """
        self._filter_parts.append(f"{column.lower()} eq {_format_value(value)}")
        return self

    def filter_ne(self, column: str, value: Any) -> QueryBuilder:
        """Add not-equal filter: ``column ne value``.

        :param column: Column name (will be lowercased).
        :param value: Value to compare against.
        :return: Self for method chaining.
        """
        self._filter_parts.append(f"{column.lower()} ne {_format_value(value)}")
        return self

    def filter_gt(self, column: str, value: Any) -> QueryBuilder:
        """Add greater-than filter: ``column gt value``.

        :param column: Column name (will be lowercased).
        :param value: Value to compare against.
        :return: Self for method chaining.
        """
        self._filter_parts.append(f"{column.lower()} gt {_format_value(value)}")
        return self

    def filter_ge(self, column: str, value: Any) -> QueryBuilder:
        """Add greater-than-or-equal filter: ``column ge value``.

        :param column: Column name (will be lowercased).
        :param value: Value to compare against.
        :return: Self for method chaining.
        """
        self._filter_parts.append(f"{column.lower()} ge {_format_value(value)}")
        return self

    def filter_lt(self, column: str, value: Any) -> QueryBuilder:
        """Add less-than filter: ``column lt value``.

        :param column: Column name (will be lowercased).
        :param value: Value to compare against.
        :return: Self for method chaining.
        """
        self._filter_parts.append(f"{column.lower()} lt {_format_value(value)}")
        return self

    def filter_le(self, column: str, value: Any) -> QueryBuilder:
        """Add less-than-or-equal filter: ``column le value``.

        :param column: Column name (will be lowercased).
        :param value: Value to compare against.
        :return: Self for method chaining.
        """
        self._filter_parts.append(f"{column.lower()} le {_format_value(value)}")
        return self

    # --------------------------------------------------------- filter: string functions

    def filter_contains(self, column: str, value: str) -> QueryBuilder:
        """Add contains filter: ``contains(column, value)``.

        :param column: Column name (will be lowercased).
        :param value: Substring to search for.
        :return: Self for method chaining.
        """
        self._filter_parts.append(f"contains({column.lower()}, {_format_value(value)})")
        return self

    def filter_startswith(self, column: str, value: str) -> QueryBuilder:
        """Add startswith filter: ``startswith(column, value)``.

        :param column: Column name (will be lowercased).
        :param value: Prefix to match.
        :return: Self for method chaining.
        """
        self._filter_parts.append(f"startswith({column.lower()}, {_format_value(value)})")
        return self

    def filter_endswith(self, column: str, value: str) -> QueryBuilder:
        """Add endswith filter: ``endswith(column, value)``.

        :param column: Column name (will be lowercased).
        :param value: Suffix to match.
        :return: Self for method chaining.
        """
        self._filter_parts.append(f"endswith({column.lower()}, {_format_value(value)})")
        return self

    # --------------------------------------------------------- filter: null checks

    def filter_null(self, column: str) -> QueryBuilder:
        """Add null check: ``column eq null``.

        :param column: Column name (will be lowercased).
        :return: Self for method chaining.
        """
        self._filter_parts.append(f"{column.lower()} eq null")
        return self

    def filter_not_null(self, column: str) -> QueryBuilder:
        """Add not-null check: ``column ne null``.

        :param column: Column name (will be lowercased).
        :return: Self for method chaining.
        """
        self._filter_parts.append(f"{column.lower()} ne null")
        return self

    # --------------------------------------------------------- filter: special

    def filter_in(self, column: str, values: list) -> QueryBuilder:
        """Add an ``in`` filter: ``column in (val1, val2, ...)``.

        :param column: Column name (will be lowercased).
        :param values: Non-empty list of values for the ``in`` clause.
        :return: Self for method chaining.
        :raises ValueError: If ``values`` is empty.

        Example::

            query = QueryBuilder("account").filter_in("statecode", [0, 1, 2])
            # Produces: statecode in (0, 1, 2)
        """
        if not values:
            raise ValueError("filter_in requires at least one value")
        formatted = ", ".join(_format_value(v) for v in values)
        self._filter_parts.append(f"{column.lower()} in ({formatted})")
        return self

    def filter_between(self, column: str, low: Any, high: Any) -> QueryBuilder:
        """Add a between filter: ``(column ge low and column le high)``.

        :param column: Column name (will be lowercased).
        :param low: Lower bound (inclusive).
        :param high: Upper bound (inclusive).
        :return: Self for method chaining.

        Example::

            query = QueryBuilder("account").filter_between("revenue", 100000, 500000)
            # Produces: (revenue ge 100000 and revenue le 500000)
        """
        col = column.lower()
        self._filter_parts.append(
            f"({col} ge {_format_value(low)} and {col} le {_format_value(high)})"
        )
        return self

    def filter_raw(self, filter_string: str) -> QueryBuilder:
        """Add a raw OData filter string.

        Use this for complex filters not covered by other methods.
        Column names in the filter string should be lowercase.

        :param filter_string: Raw OData filter expression.
        :return: Self for method chaining.

        Example::

            query = QueryBuilder("account").filter_raw(
                "(statecode eq 0 or statecode eq 1)"
            )
        """
        self._filter_parts.append(filter_string)
        return self

    # ------------------------------------------------------ filter: expression tree

    def where(self, expression: FilterExpression) -> QueryBuilder:
        """Add a composable filter expression.

        Accepts a :class:`~PowerPlatform.Dataverse.models.filters.FilterExpression`
        built using the convenience functions from
        :mod:`~PowerPlatform.Dataverse.models.filters`.

        Multiple ``where()`` calls and ``filter_*()`` calls are all
        AND-joined together in the order they were called.

        :param expression: A composable filter expression.
        :type expression: FilterExpression
        :return: Self for method chaining.
        :raises TypeError: If ``expression`` is not a FilterExpression.

        Example::

            from PowerPlatform.Dataverse.models.filters import eq, gt

            query = (QueryBuilder("account")
                     .where((eq("statecode", 0) | eq("statecode", 1))
                            & gt("revenue", 100000)))
        """
        if not isinstance(expression, FilterExpression):
            raise TypeError(
                f"where() requires a FilterExpression, got {type(expression).__name__}"
            )
        self._filter_parts.append(expression)
        return self

    # --------------------------------------------------------------- ordering

    def order_by(self, column: str, descending: bool = False) -> QueryBuilder:
        """Add sorting order.

        Can be called multiple times for multi-column sorting.

        :param column: Column name to sort by (will be lowercased).
        :param descending: Sort in descending order.
        :return: Self for method chaining.
        """
        order = f"{column.lower()} desc" if descending else column.lower()
        self._orderby.append(order)
        return self

    # --------------------------------------------------------------- pagination

    def top(self, count: int) -> QueryBuilder:
        """Limit the total number of results.

        :param count: Maximum number of records to return (must be >= 1).
        :return: Self for method chaining.
        :raises ValueError: If ``count`` is less than 1.
        """
        if count < 1:
            raise ValueError("top count must be at least 1")
        self._top = count
        return self

    def page_size(self, size: int) -> QueryBuilder:
        """Set the number of records per page.

        Controls how many records are returned in each page/batch
        via the ``Prefer: odata.maxpagesize`` header.

        :param size: Number of records per page (must be >= 1).
        :return: Self for method chaining.
        :raises ValueError: If ``size`` is less than 1.
        """
        if size < 1:
            raise ValueError("page_size must be at least 1")
        self._page_size = size
        return self

    # --------------------------------------------------------------- expand

    def expand(self, *relations: str) -> QueryBuilder:
        """Expand navigation properties.

        Navigation property names are case-sensitive and passed as-is.

        :param relations: Navigation property names to expand.
        :return: Self for method chaining.
        """
        self._expand.extend(relations)
        return self

    # --------------------------------------------------------------- build

    def build(self) -> dict:
        """Build query parameters dictionary.

        Returns a dictionary suitable for passing to the OData layer.
        All ``filter_*()`` and ``where()`` clauses are AND-joined into
        a single ``filter`` string in call order.

        :return: Dictionary with ``table`` and optionally ``select``,
            ``filter``, ``orderby``, ``expand``, ``top``, ``page_size``.
        :rtype: dict
        """
        params: dict = {"table": self.table}
        if self._select:
            params["select"] = list(self._select)
        if self._filter_parts:
            parts: List[str] = []
            for part in self._filter_parts:
                if isinstance(part, FilterExpression):
                    parts.append(part.to_odata())
                else:
                    parts.append(part)
            params["filter"] = " and ".join(parts)
        if self._orderby:
            params["orderby"] = list(self._orderby)
        if self._expand:
            params["expand"] = list(self._expand)
        if self._top is not None:
            params["top"] = self._top
        if self._page_size is not None:
            params["page_size"] = self._page_size
        return params

    # --------------------------------------------------------------- execute

    def execute(self) -> Iterable[List[Dict[str, Any]]]:
        """Execute the query and return paginated results.

        This method is only available when the QueryBuilder was created
        via ``client.query.builder(table)``.  Standalone ``QueryBuilder``
        instances should use :meth:`build` to get parameters and pass them
        to ``client.records.get()`` manually.

        :return: Generator yielding pages, where each page is a list of
            record dictionaries.
        :rtype: Iterable[List[Dict[str, Any]]]
        :raises RuntimeError: If the query was not created via
            ``client.query.builder()``.

        Example::

            for page in (client.query.builder("account")
                         .select("name")
                         .filter_eq("statecode", 0)
                         .execute()):
                for record in page:
                    print(record["name"])
        """
        if self._query_ops is None:
            raise RuntimeError(
                "Cannot execute: query was not created via client.query.builder(). "
                "Use build() and pass parameters to client.records.get() instead."
            )
        params = self.build()
        client = self._query_ops._client

        def _paged() -> Iterable[List[Dict[str, Any]]]:
            with client._scoped_odata() as od:
                yield from od._get_multiple(
                    params["table"],
                    select=params.get("select"),
                    filter=params.get("filter"),
                    orderby=params.get("orderby"),
                    top=params.get("top"),
                    expand=params.get("expand"),
                    page_size=params.get("page_size"),
                )

        return _paged()
