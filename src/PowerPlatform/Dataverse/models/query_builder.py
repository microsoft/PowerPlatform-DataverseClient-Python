# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Fluent query builder for constructing OData queries.

Provides a type-safe, discoverable interface for building complex queries
against Dataverse tables with method chaining.

Example::

    # Via client (recommended) -- flat iteration over records
    from PowerPlatform.Dataverse.models.filters import col

    for record in (client.query.builder("account")
                   .select("name", "revenue")
                   .where(col("statecode") == 0)
                   .where(col("revenue") > 1_000_000)
                   .order_by("revenue", descending=True)
                   .top(100)
                   .execute()):
        print(record["name"])

    # With composable expression tree
    from PowerPlatform.Dataverse.models.filters import col, raw

    for record in (client.query.builder("account")
                   .select("name", "revenue")
                   .where((col("statecode") == 0) | (col("statecode") == 1))
                   .where(col("revenue") > 100000)
                   .top(100)
                   .execute()):
        print(record["name"])

    # Lazy paged iteration (one QueryResult per HTTP page)
    for page in (client.query.builder("account")
                 .select("name")
                 .execute_pages()):
        process_batch(page)

    # Get results as a pandas DataFrame
    df = (client.query.builder("account")
          .select("name", "telephone1")
          .where(col("statecode") == 0)
          .top(100)
          .execute()
          .to_dataframe())
"""

from __future__ import annotations

import warnings
from typing import Any, Dict, Iterator, List, Optional, TypedDict, Union

import pandas as pd

from . import filters
from .record import QueryResult, Record

__all__ = ["QueryBuilder", "QueryParams", "ExpandOption"]

# Sentinel for detecting when by_page is explicitly passed to execute()
_BY_PAGE_UNSET = object()


class QueryParams(TypedDict, total=False):
    """Typed dictionary returned by :meth:`QueryBuilder.build`.

    Provides IDE autocomplete when passing build results to
    ``client.records.list()`` manually.
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
    composable filter expressions. Can be used standalone (via :meth:`build`)
    or bound to a client (via :meth:`execute`).

    :param table: Table schema name to query.
    :type table: str
    :raises ValueError: If ``table`` is empty.

    Example:
        Standalone query construction::

            from PowerPlatform.Dataverse.models.filters import col

            query = (QueryBuilder("account")
                     .select("name")
                     .where(col("statecode") == 0)
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

    # ------------------------------------------------------ filter: expression tree

    def where(self, expression: filters.FilterExpression) -> QueryBuilder:
        """Add a composable filter expression.

        Accepts a :class:`~PowerPlatform.Dataverse.models.filters.FilterExpression`
        built using :func:`~PowerPlatform.Dataverse.models.filters.col` or
        :func:`~PowerPlatform.Dataverse.models.filters.raw`.

        Multiple ``where()`` calls are AND-joined together in call order.

        :param expression: A composable filter expression.
        :type expression: FilterExpression
        :return: Self for method chaining.
        :raises TypeError: If ``expression`` is not a FilterExpression.

        Example::

            from PowerPlatform.Dataverse.models.filters import col

            query = (QueryBuilder("account")
                     .where((col("statecode") == 0) | (col("statecode") == 1))
                     .where(col("revenue") > 100000))
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
                       .where(col("statecode") == 0)
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
        the OData layer.  All ``where()`` clauses are AND-joined into a
        single ``filter`` string in call order.

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

    # --------------------------------------------------------------- execute

    def execute(self, *, by_page=_BY_PAGE_UNSET) -> Union[QueryResult, Iterator[QueryResult]]:
        """Execute the query and return results.

        Returns a :class:`~PowerPlatform.Dataverse.models.record.QueryResult`
        with all pages collected. Use :meth:`execute_pages` for lazy per-page
        iteration.

        This method is only available when the QueryBuilder was created
        via ``client.query.builder(table)``.  Standalone ``QueryBuilder``
        instances should use :meth:`build` to get parameters and pass them
        to ``client.records.list()`` manually.

        At least one of ``select()``, ``where()``, or ``top()`` must be
        called before ``execute()``; otherwise a :class:`ValueError` is
        raised to prevent accidental full-table scans.

        .. deprecated::
            The ``by_page`` parameter is deprecated. Use :meth:`execute_pages`
            for lazy per-page iteration, or plain ``execute()`` (no flag) for
            the default eager result.

        :return: :class:`~PowerPlatform.Dataverse.models.record.QueryResult`
            with all pages collected (default), or page iterator (deprecated
            ``by_page=True``).
        :rtype: QueryResult or Iterator[QueryResult]
        :raises ValueError: If no ``select``, ``where``, or ``top``
            constraint has been set.
        :raises RuntimeError: If the query was not created via
            ``client.query.builder()``.

        Example::

            from PowerPlatform.Dataverse.models.filters import col

            for record in (client.query.builder("account")
                           .select("name")
                           .where(col("statecode") == 0)
                           .execute()):
                print(record["name"])
        """
        use_by_page = False
        if by_page is not _BY_PAGE_UNSET:
            use_by_page = bool(by_page)
            if use_by_page:
                warnings.warn(
                    "'execute(by_page=True)' is deprecated; use 'execute_pages()' instead.",
                    UserWarning,
                    stacklevel=2,
                )
            else:
                warnings.warn(
                    "'execute(by_page=False)' is deprecated; "
                    "the by_page flag is redundant — use plain 'execute()' instead.",
                    UserWarning,
                    stacklevel=2,
                )

        if self._query_ops is None:
            raise RuntimeError(
                "Cannot execute: query was not created via client.query.builder(). "
                "Use build() and pass parameters to client.records.list() instead."
            )

        if not self._select and not self._filter_parts and self._top is None:
            raise ValueError(
                "At least one of select(), where(), or top() must be called before "
                "execute() to prevent accidental full-table scans."
            )

        params = self.build()
        client = self._query_ops._client

        if use_by_page:
            return self.execute_pages()

        all_records: List[Record] = []
        with client._scoped_odata() as od:
            for page in od._get_multiple(
                params["table"],
                select=params.get("select"),
                filter=params.get("filter"),
                orderby=params.get("orderby"),
                top=params.get("top"),
                expand=params.get("expand"),
                page_size=params.get("page_size"),
                count=params.get("count", False),
                include_annotations=params.get("include_annotations"),
            ):
                all_records.extend(Record.from_api_response(params["table"], row) for row in page)
        return QueryResult(all_records)

    # ---------------------------------------------------------- execute_pages

    def execute_pages(self) -> Iterator[QueryResult]:
        """Lazily yield one :class:`~PowerPlatform.Dataverse.models.record.QueryResult`
        per HTTP page.

        Each iteration triggers a network request via ``@odata.nextLink``.
        One-shot — do not iterate more than once.

        At least one of ``select()``, ``where()``, or ``top()`` must be
        called before ``execute_pages()``; otherwise a :class:`ValueError` is
        raised to prevent accidental full-table scans.

        :return: Iterator of per-page :class:`~PowerPlatform.Dataverse.models.record.QueryResult`.
        :rtype: Iterator[QueryResult]
        :raises ValueError: If no ``select``, ``where``, or ``top``
            constraint has been set.
        :raises RuntimeError: If the query was not created via
            ``client.query.builder()``.

        Example::

            from PowerPlatform.Dataverse.models.filters import col

            for page in (client.query.builder("account")
                         .select("name")
                         .where(col("statecode") == 0)
                         .execute_pages()):
                process(page.to_dataframe())
        """
        if self._query_ops is None:
            raise RuntimeError(
                "Cannot execute: query was not created via client.query.builder(). "
                "Use build() and pass parameters to client.records.list() instead."
            )

        if not self._select and not self._filter_parts and self._top is None:
            raise ValueError(
                "At least one of select(), where(), or top() must be called before "
                "execute_pages() to prevent accidental full-table scans."
            )

        params = self.build()
        client = self._query_ops._client

        with client._scoped_odata() as od:
            for page in od._get_multiple(
                params["table"],
                select=params.get("select"),
                filter=params.get("filter"),
                orderby=params.get("orderby"),
                top=params.get("top"),
                expand=params.get("expand"),
                page_size=params.get("page_size"),
                count=params.get("count", False),
                include_annotations=params.get("include_annotations"),
            ):
                yield QueryResult([Record.from_api_response(params["table"], row) for row in page])

    # ----------------------------------------------------------- to_dataframe

    def to_dataframe(self) -> pd.DataFrame:
        """Execute the query and return results as a pandas DataFrame.

        .. deprecated::
            Use ``QueryBuilder.execute().to_dataframe()`` instead.
            ``QueryBuilder.to_dataframe()`` will be removed in a future release.

        All pages are consolidated into a single DataFrame.

        This method is only available when the QueryBuilder was created
        via ``client.query.builder(table)``.

        At least one of ``select()``, ``where()``, or ``top()`` must be
        called before ``to_dataframe()``; otherwise a :class:`ValueError`
        is raised to prevent accidental full-table scans.

        :return: DataFrame containing all matching records. Returns an empty
            DataFrame when no records match.
        :rtype: ~pandas.DataFrame
        :raises ValueError: If no ``select``, ``where``, or ``top``
            constraint has been set.
        :raises RuntimeError: If the query was not created via
            ``client.query.builder()``.

        Example::

            from PowerPlatform.Dataverse.models.filters import col

            df = (client.query.builder("account")
                  .select("name", "telephone1")
                  .where(col("statecode") == 0)
                  .top(100)
                  .execute()
                  .to_dataframe())
        """
        warnings.warn(
            "'QueryBuilder.to_dataframe()' is deprecated; use " "'QueryBuilder.execute().to_dataframe()' instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if self._query_ops is None:
            raise RuntimeError(
                "Cannot execute: query was not created via client.query.builder(). "
                "Use build() and pass parameters to client.records.list() instead."
            )

        result = self.execute()
        if not result:
            return pd.DataFrame(columns=self._select) if self._select else pd.DataFrame()
        return result.to_dataframe()
