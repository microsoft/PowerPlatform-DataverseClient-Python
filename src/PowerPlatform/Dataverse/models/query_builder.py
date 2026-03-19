# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Fluent query builder for constructing OData queries.

Provides a type-safe, discoverable interface for building complex queries
against Dataverse tables with method chaining.

Example::

    # Via client (recommended) -- flat iteration over records
    for record in (client.query.builder("account")
                   .select("name", "revenue")
                   .filter_eq("statecode", 0)
                   .filter_gt("revenue", 1000000)
                   .order_by("revenue", descending=True)
                   .top(100)
                   .execute()):
        print(record["name"])

    # With composable expression tree
    from PowerPlatform.Dataverse.models.filters import eq, gt

    for record in (client.query.builder("account")
                   .select("name", "revenue")
                   .where((eq("statecode", 0) | eq("statecode", 1))
                          & gt("revenue", 100000))
                   .top(100)
                   .execute()):
        print(record["name"])

    # Opt-in paged iteration (for batch processing)
    for page in (client.query.builder("account")
                 .select("name")
                 .execute(by_page=True)):
        process_batch(page)

    # Get results as a pandas DataFrame
    df = (client.query.builder("account")
          .select("name", "telephone1")
          .filter_eq("statecode", 0)
          .top(100)
          .to_dataframe())
"""

from __future__ import annotations

from typing import Any, Collection, Dict, Iterable, List, Optional, Sequence, TypedDict, Union

import pandas as pd

from . import filters
from .record import Record

__all__ = ["QueryBuilder", "QueryParams", "ExpandOption"]


class QueryParams(TypedDict, total=False):
    """Typed dictionary returned by :meth:`QueryBuilder.build`.

    Provides IDE autocomplete when passing build results to
    ``client.records.get()`` manually.
    """

    table: str
    select: List[str]
    filter: str
    orderby: List[str]
    expand: List[str]
    top: int
    page_size: int
    count: bool
    include_annotations: str


class ExpandOption:
    """Structured options for an ``$expand`` navigation property.

    Allows specifying nested ``$select``, ``$filter``, ``$orderby``, and
    ``$top`` options for a single navigation property expansion, following
    the OData ``$expand`` syntax.

    :param relation: Navigation property name (case-sensitive).
    :type relation: str

    Example::

        # Expand Account_Tasks with nested options
        opt = (ExpandOption("Account_Tasks")
               .select("subject", "createdon")
               .filter("contains(subject,'Task')")
               .order_by("createdon", descending=True)
               .top(5))

        query = (client.query.builder("account")
                 .select("name")
                 .expand(opt)
                 .execute())
    """

    def __init__(self, relation: str) -> None:
        self.relation = relation
        self._select: List[str] = []
        self._filter: Optional[str] = None
        self._orderby: List[str] = []
        self._top: Optional[int] = None

    def select(self, *columns: str) -> ExpandOption:
        """Select specific columns from the expanded entity.

        :param columns: Column names to select.
        :return: Self for method chaining.
        """
        self._select.extend(columns)
        return self

    def filter(self, filter_str: str) -> ExpandOption:
        """Filter the expanded collection.

        :param filter_str: OData ``$filter`` expression.
        :return: Self for method chaining.
        """
        self._filter = filter_str
        return self

    def order_by(self, column: str, descending: bool = False) -> ExpandOption:
        """Sort the expanded collection.

        :param column: Column name to sort by.
        :param descending: Sort descending if ``True``.
        :return: Self for method chaining.
        """
        order = f"{column} desc" if descending else column
        self._orderby.append(order)
        return self

    def top(self, count: int) -> ExpandOption:
        """Limit expanded results.

        :param count: Maximum number of expanded records.
        :return: Self for method chaining.
        """
        self._top = count
        return self

    def to_odata(self) -> str:
        """Compile to OData ``$expand`` syntax.

        :return: OData expand string like ``"Nav($select=col1,col2;$filter=...)"``
        :rtype: str
        """
        options: List[str] = []
        if self._select:
            options.append(f"$select={','.join(self._select)}")
        if self._filter:
            options.append(f"$filter={self._filter}")
        if self._orderby:
            options.append(f"$orderby={','.join(self._orderby)}")
        if self._top is not None:
            options.append(f"$top={self._top}")
        if options:
            return f"{self.relation}({';'.join(options)})"
        return self.relation


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
        table = table.strip() if table else ""
        if not table:
            raise ValueError("table name is required")
        self.table = table
        self._select: List[str] = []
        self._filter_parts: List[Union[str, filters.FilterExpression]] = []
        self._orderby: List[str] = []
        self._expand: List[str] = []
        self._top: Optional[int] = None
        self._page_size: Optional[int] = None
        self._count: bool = False
        self._include_annotations: Optional[str] = None
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
        self._filter_parts.append(filters.eq(column, value))
        return self

    def filter_ne(self, column: str, value: Any) -> QueryBuilder:
        """Add not-equal filter: ``column ne value``.

        :param column: Column name (will be lowercased).
        :param value: Value to compare against.
        :return: Self for method chaining.
        """
        self._filter_parts.append(filters.ne(column, value))
        return self

    def filter_gt(self, column: str, value: Any) -> QueryBuilder:
        """Add greater-than filter: ``column gt value``.

        :param column: Column name (will be lowercased).
        :param value: Value to compare against.
        :return: Self for method chaining.
        """
        self._filter_parts.append(filters.gt(column, value))
        return self

    def filter_ge(self, column: str, value: Any) -> QueryBuilder:
        """Add greater-than-or-equal filter: ``column ge value``.

        :param column: Column name (will be lowercased).
        :param value: Value to compare against.
        :return: Self for method chaining.
        """
        self._filter_parts.append(filters.ge(column, value))
        return self

    def filter_lt(self, column: str, value: Any) -> QueryBuilder:
        """Add less-than filter: ``column lt value``.

        :param column: Column name (will be lowercased).
        :param value: Value to compare against.
        :return: Self for method chaining.
        """
        self._filter_parts.append(filters.lt(column, value))
        return self

    def filter_le(self, column: str, value: Any) -> QueryBuilder:
        """Add less-than-or-equal filter: ``column le value``.

        :param column: Column name (will be lowercased).
        :param value: Value to compare against.
        :return: Self for method chaining.
        """
        self._filter_parts.append(filters.le(column, value))
        return self

    # --------------------------------------------------------- filter: string functions

    def filter_contains(self, column: str, value: str) -> QueryBuilder:
        """Add contains filter: ``contains(column, value)``.

        :param column: Column name (will be lowercased).
        :param value: Substring to search for.
        :return: Self for method chaining.
        """
        self._filter_parts.append(filters.contains(column, value))
        return self

    def filter_startswith(self, column: str, value: str) -> QueryBuilder:
        """Add startswith filter: ``startswith(column, value)``.

        :param column: Column name (will be lowercased).
        :param value: Prefix to match.
        :return: Self for method chaining.
        """
        self._filter_parts.append(filters.startswith(column, value))
        return self

    def filter_endswith(self, column: str, value: str) -> QueryBuilder:
        """Add endswith filter: ``endswith(column, value)``.

        :param column: Column name (will be lowercased).
        :param value: Suffix to match.
        :return: Self for method chaining.
        """
        self._filter_parts.append(filters.endswith(column, value))
        return self

    # --------------------------------------------------------- filter: null checks

    def filter_null(self, column: str) -> QueryBuilder:
        """Add null check: ``column eq null``.

        :param column: Column name (will be lowercased).
        :return: Self for method chaining.
        """
        self._filter_parts.append(filters.is_null(column))
        return self

    def filter_not_null(self, column: str) -> QueryBuilder:
        """Add not-null check: ``column ne null``.

        :param column: Column name (will be lowercased).
        :return: Self for method chaining.
        """
        self._filter_parts.append(filters.is_not_null(column))
        return self

    # --------------------------------------------------------- filter: special

    def filter_in(self, column: str, values: Collection[Any]) -> QueryBuilder:
        """Add an ``in`` filter using ``Microsoft.Dynamics.CRM.In``.

        :param column: Column name (will be lowercased).
        :param values: Non-empty list of values for the ``in`` clause.
        :return: Self for method chaining.
        :raises ValueError: If ``values`` is empty.

        Example::

            query = QueryBuilder("account").filter_in("statecode", [0, 1, 2])
            # Produces: Microsoft.Dynamics.CRM.In(
            #     PropertyName='statecode',PropertyValues=["0","1","2"])
        """
        self._filter_parts.append(filters.filter_in(column, values))
        return self

    def filter_not_in(self, column: str, values: Collection[Any]) -> QueryBuilder:
        """Add a ``not in`` filter using ``Microsoft.Dynamics.CRM.NotIn``.

        :param column: Column name (will be lowercased).
        :param values: Non-empty list of values to exclude.
        :return: Self for method chaining.
        :raises ValueError: If ``values`` is empty.

        Example::

            query = QueryBuilder("account").filter_not_in("statecode", [2, 3])
            # Produces: Microsoft.Dynamics.CRM.NotIn(
            #     PropertyName='statecode',PropertyValues=["2","3"])
        """
        self._filter_parts.append(filters.not_in(column, values))
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
        self._filter_parts.append(filters.between(column, low, high))
        return self

    def filter_not_between(self, column: str, low: Any, high: Any) -> QueryBuilder:
        """Add a not-between filter: ``not (column ge low and column le high)``.

        :param column: Column name (will be lowercased).
        :param low: Lower bound (inclusive, will be excluded).
        :param high: Upper bound (inclusive, will be excluded).
        :return: Self for method chaining.

        Example::

            query = QueryBuilder("account").filter_not_between("revenue", 100000, 500000)
            # Produces: not ((revenue ge 100000 and revenue le 500000))
        """
        self._filter_parts.append(filters.not_between(column, low, high))
        return self

    def filter_raw(self, filter_string: str) -> QueryBuilder:
        """Add a raw OData filter string.

        Use this for complex filters not covered by other methods.
        Column names in the filter string should be lowercase.

        .. warning::
            The filter string is passed directly to Dataverse without validation.
            Ensure it follows OData filter syntax; a malformed expression will result
            in a ``400 Bad Request`` error from the server.

        :param filter_string: Raw OData filter expression.
        :return: Self for method chaining.

        Example::

            query = QueryBuilder("account").filter_raw(
                "(statecode eq 0 or statecode eq 1)"
            )
        """
        self._filter_parts.append(filters.raw(filter_string))
        return self

    # ------------------------------------------------------ filter: expression tree

    def where(self, expression: filters.FilterExpression) -> QueryBuilder:
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
        if not isinstance(expression, filters.FilterExpression):
            raise TypeError(f"where() requires a FilterExpression, got {type(expression).__name__}")
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

    def count(self) -> QueryBuilder:
        """Request a count of matching records in the response.

        Adds ``$count=true`` to the query, causing the server to include
        an ``@odata.count`` annotation in the response with the total
        number of matching records (up to 5,000).

        :return: Self for method chaining.

        Example::

            results = (client.query.builder("account")
                       .filter_eq("statecode", 0)
                       .count()
                       .execute())
        """
        self._count = True
        return self

    def include_formatted_values(self) -> QueryBuilder:
        """Request formatted values in the response.

        Adds ``Prefer: odata.include-annotations="OData.Community.Display.V1.FormattedValue"``
        to the request, causing the server to return formatted string
        representations alongside raw values. This includes:

        - Localized labels for choice, yes/no, status, and status reason columns
        - Primary name values for lookup and owner properties
        - Currency values with currency symbols
        - Formatted dates in the user's time zone

        Access formatted values in the response via the annotation key::

            record["statecode@OData.Community.Display.V1.FormattedValue"]

        :return: Self for method chaining.

        Example::

            for record in (client.query.builder("account")
                           .select("name", "statecode")
                           .include_formatted_values()
                           .execute()):
                label = record["statecode@OData.Community.Display.V1.FormattedValue"]
                print(f"{record['name']}: {label}")
        """
        self._include_annotations = "OData.Community.Display.V1.FormattedValue"
        return self

    def include_annotations(self, annotation: str = "*") -> QueryBuilder:
        """Request specific OData annotations in the response.

        Sets the ``Prefer: odata.include-annotations`` header. Use ``"*"``
        to request all annotations, or specify a particular annotation
        pattern.

        :param annotation: Annotation pattern to request. Defaults to
            ``"*"`` (all annotations).
        :return: Self for method chaining.

        Example::

            # Request all annotations
            builder = (client.query.builder("account")
                       .select("name", "_ownerid_value")
                       .include_annotations("*"))

            # Request only lookup metadata
            builder = (client.query.builder("account")
                       .include_annotations(
                           "Microsoft.Dynamics.CRM.lookuplogicalname"))
        """
        self._include_annotations = annotation
        return self

    # --------------------------------------------------------------- expand

    def expand(self, *relations: Union[str, ExpandOption]) -> QueryBuilder:
        """Expand navigation properties.

        Accepts plain navigation property names (case-sensitive, passed
        as-is) or :class:`ExpandOption` objects for nested options like
        ``$select``, ``$filter``, ``$orderby``, and ``$top``.

        :param relations: Navigation property names or
            :class:`ExpandOption` objects.
        :return: Self for method chaining.

        Example::

            # Simple expand
            query = QueryBuilder("account").expand("primarycontactid")

            # Nested expand with options
            query = (QueryBuilder("account")
                     .expand(ExpandOption("Account_Tasks")
                             .select("subject")
                             .filter("contains(subject,'Task')")
                             .top(5)))
        """
        for rel in relations:
            if isinstance(rel, ExpandOption):
                self._expand.append(rel.to_odata())
            else:
                self._expand.append(rel)
        return self

    # --------------------------------------------------------------- build

    def build(self) -> QueryParams:
        """Build query parameters dictionary.

        Returns a :class:`QueryParams` dictionary suitable for passing to
        the OData layer.  All ``filter_*()`` and ``where()`` clauses are
        AND-joined into a single ``filter`` string in call order.

        :return: Dictionary with ``table`` and optionally ``select``,
            ``filter``, ``orderby``, ``expand``, ``top``, ``page_size``,
            ``count``, ``include_annotations``.
        :rtype: QueryParams
        """
        params: QueryParams = {"table": self.table}
        if self._select:
            params["select"] = list(self._select)
        if self._filter_parts:
            parts: List[str] = []
            for part in self._filter_parts:
                if isinstance(part, filters.FilterExpression):
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
        if self._count:
            params["count"] = True
        if self._include_annotations is not None:
            params["include_annotations"] = self._include_annotations
        return params

    # --------------------------------------------------------------- guards

    def _validate_constraints(self) -> None:
        """Raise if the query has no limiting constraints.

        At least one of ``select``, ``filter``, or ``top`` must be set
        before executing a query to prevent accidental full-table scans.

        :raises ValueError: If none of ``select()``, ``filter_*()``,
            ``where()``, or ``top()`` has been called.
        """
        if not (self._select or self._filter_parts or self._top is not None):
            raise ValueError(
                "Unbounded query: set at least one of select(), filter_*(), "
                "where(), or top() before calling execute() or to_dataframe()."
            )

    # --------------------------------------------------------------- execute

    def execute(self, *, by_page: bool = False) -> Union[Iterable[Record], Iterable[List[Record]]]:
        """Execute the query and return results.

        By default, returns a flat iterator over individual records,
        abstracting away OData paging.  Pass ``by_page=True`` to get
        page-level iteration instead (useful for batch processing).

        This method is only available when the QueryBuilder was created
        via ``client.query.builder(table)``.  Standalone ``QueryBuilder``
        instances should use :meth:`build` to get parameters and pass them
        to ``client.records.get()`` manually.

        At least one of ``select()``, ``filter_*()``, ``where()``, or
        ``top()`` must be called before ``execute()``; otherwise a
        :class:`ValueError` is raised to prevent accidental full-table
        scans.

        :param by_page: If ``True``, yield pages (lists of
            :class:`~PowerPlatform.Dataverse.models.record.Record` objects)
            instead of individual records. Defaults to ``False``.
        :type by_page: bool
        :return: Generator yielding individual
            :class:`~PowerPlatform.Dataverse.models.record.Record` objects
            (default) or pages of records (when ``by_page=True``).
        :rtype: Iterable[Record] or Iterable[List[Record]]
        :raises ValueError: If no ``select``, ``filter``, or ``top``
            constraint has been set.
        :raises RuntimeError: If the query was not created via
            ``client.query.builder()``.

        Example:
            Flat iteration (default)::

                for record in (client.query.builder("account")
                               .select("name")
                               .filter_eq("statecode", 0)
                               .execute()):
                    print(record["name"])

            Paged iteration::

                for page in (client.query.builder("account")
                             .select("name")
                             .execute(by_page=True)):
                    process_batch(page)
        """
        if self._query_ops is None:
            raise RuntimeError(
                "Cannot execute: query was not created via client.query.builder(). "
                "Use build() and pass parameters to client.records.get() instead."
            )
        self._validate_constraints()
        params = self.build()
        client = self._query_ops._client

        pages = client.records.get(
            params["table"],
            select=params.get("select"),
            filter=params.get("filter"),
            orderby=params.get("orderby"),
            top=params.get("top"),
            expand=params.get("expand"),
            page_size=params.get("page_size"),
            count=params.get("count", False),
            include_annotations=params.get("include_annotations"),
        )

        if by_page:
            return pages

        def _flat() -> Iterable[Record]:
            for page in pages:
                yield from page

        return _flat()

    # ----------------------------------------------------------- to_dataframe

    def to_dataframe(self) -> pd.DataFrame:
        """Execute the query and return results as a pandas DataFrame.

        All pages are consolidated into a single DataFrame, matching
        the behavior of ``client.dataframe.get()``.

        This method is only available when the QueryBuilder was created
        via ``client.query.builder(table)``.

        At least one of ``select()``, ``filter_*()``, ``where()``, or
        ``top()`` must be called before ``to_dataframe()``; otherwise a
        :class:`ValueError` is raised to prevent accidental full-table
        scans.

        :return: DataFrame containing all matching records. Returns an empty
            DataFrame when no records match.
        :rtype: ~pandas.DataFrame
        :raises ValueError: If no ``select``, ``filter``, or ``top``
            constraint has been set.
        :raises RuntimeError: If the query was not created via
            ``client.query.builder()``.

        Example::

            df = (client.query.builder("account")
                  .select("name", "telephone1")
                  .filter_eq("statecode", 0)
                  .top(100)
                  .to_dataframe())
        """
        if self._query_ops is None:
            raise RuntimeError(
                "Cannot execute: query was not created via client.query.builder(). "
                "Use build() and pass parameters to client.dataframe.get() instead."
            )
        self._validate_constraints()
        params = self.build()
        return self._query_ops._client.dataframe.get(
            params["table"],
            select=params.get("select"),
            filter=params.get("filter"),
            orderby=params.get("orderby"),
            top=params.get("top"),
            expand=params.get("expand"),
            page_size=params.get("page_size"),
            count=params.get("count", False),
            include_annotations=params.get("include_annotations"),
        )
