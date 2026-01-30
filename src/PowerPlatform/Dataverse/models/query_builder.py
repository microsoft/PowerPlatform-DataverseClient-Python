# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Fluent query builder for constructing OData queries.

Provides a type-safe, discoverable interface for building complex queries
against Dataverse tables.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..core.results import OperationResult
    from .record import Record


@dataclass
class QueryBuilder:
    """
    Fluent interface for building OData queries.

    Provides method chaining for constructing complex queries with
    type-safe filter operations.

    :param table: Table schema name to query.
    :type table: str

    Example:
        Build and execute a query (via client)::

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

        Build a standalone query::

            query = (QueryBuilder("account")
                     .select("name")
                     .filter_eq("statecode", 0)
                     .top(10))
            params = query.build()
    """

    table: str
    _select: List[str] = field(default_factory=list)
    _filter: List[str] = field(default_factory=list)
    _orderby: List[str] = field(default_factory=list)
    _expand: List[str] = field(default_factory=list)
    _top: Optional[int] = None
    _page_size: Optional[int] = None
    _query_ops: Any = field(default=None, compare=False, repr=False)

    def select(self, *columns: str) -> "QueryBuilder":
        """
        Select specific columns to retrieve.

        Column names are automatically lowercased per Dataverse conventions.

        :param columns: Column names to select.
        :type columns: str
        :return: Self for method chaining.
        :rtype: QueryBuilder

        Example::

            query = QueryBuilder("account").select("name", "telephone1", "revenue")
        """
        self._select.extend(columns)
        return self

    def filter_eq(self, column: str, value: Any) -> "QueryBuilder":
        """
        Add equality filter (column eq value).

        :param column: Column name (will be lowercased).
        :type column: str
        :param value: Value to compare against.
        :return: Self for method chaining.
        :rtype: QueryBuilder

        Example::

            query = QueryBuilder("account").filter_eq("statecode", 0)
        """
        self._filter.append(f"{column.lower()} eq {self._format_value(value)}")
        return self

    def filter_ne(self, column: str, value: Any) -> "QueryBuilder":
        """
        Add not-equal filter (column ne value).

        :param column: Column name.
        :type column: str
        :param value: Value to compare against.
        :return: Self for method chaining.
        :rtype: QueryBuilder
        """
        self._filter.append(f"{column.lower()} ne {self._format_value(value)}")
        return self

    def filter_gt(self, column: str, value: Any) -> "QueryBuilder":
        """
        Add greater-than filter (column gt value).

        :param column: Column name.
        :type column: str
        :param value: Value to compare against.
        :return: Self for method chaining.
        :rtype: QueryBuilder
        """
        self._filter.append(f"{column.lower()} gt {self._format_value(value)}")
        return self

    def filter_ge(self, column: str, value: Any) -> "QueryBuilder":
        """
        Add greater-than-or-equal filter (column ge value).

        :param column: Column name.
        :type column: str
        :param value: Value to compare against.
        :return: Self for method chaining.
        :rtype: QueryBuilder
        """
        self._filter.append(f"{column.lower()} ge {self._format_value(value)}")
        return self

    def filter_lt(self, column: str, value: Any) -> "QueryBuilder":
        """
        Add less-than filter (column lt value).

        :param column: Column name.
        :type column: str
        :param value: Value to compare against.
        :return: Self for method chaining.
        :rtype: QueryBuilder
        """
        self._filter.append(f"{column.lower()} lt {self._format_value(value)}")
        return self

    def filter_le(self, column: str, value: Any) -> "QueryBuilder":
        """
        Add less-than-or-equal filter (column le value).

        :param column: Column name.
        :type column: str
        :param value: Value to compare against.
        :return: Self for method chaining.
        :rtype: QueryBuilder
        """
        self._filter.append(f"{column.lower()} le {self._format_value(value)}")
        return self

    def filter_contains(self, column: str, value: str) -> "QueryBuilder":
        """
        Add contains filter (contains(column, value)).

        :param column: Column name.
        :type column: str
        :param value: Substring to search for.
        :type value: str
        :return: Self for method chaining.
        :rtype: QueryBuilder

        Example::

            query = QueryBuilder("account").filter_contains("name", "Contoso")
        """
        self._filter.append(f"contains({column.lower()}, {self._format_value(value)})")
        return self

    def filter_startswith(self, column: str, value: str) -> "QueryBuilder":
        """
        Add startswith filter (startswith(column, value)).

        :param column: Column name.
        :type column: str
        :param value: Prefix to match.
        :type value: str
        :return: Self for method chaining.
        :rtype: QueryBuilder
        """
        self._filter.append(f"startswith({column.lower()}, {self._format_value(value)})")
        return self

    def filter_endswith(self, column: str, value: str) -> "QueryBuilder":
        """
        Add endswith filter (endswith(column, value)).

        :param column: Column name.
        :type column: str
        :param value: Suffix to match.
        :type value: str
        :return: Self for method chaining.
        :rtype: QueryBuilder
        """
        self._filter.append(f"endswith({column.lower()}, {self._format_value(value)})")
        return self

    def filter_null(self, column: str) -> "QueryBuilder":
        """
        Add null check filter (column eq null).

        :param column: Column name.
        :type column: str
        :return: Self for method chaining.
        :rtype: QueryBuilder
        """
        self._filter.append(f"{column.lower()} eq null")
        return self

    def filter_not_null(self, column: str) -> "QueryBuilder":
        """
        Add not-null check filter (column ne null).

        :param column: Column name.
        :type column: str
        :return: Self for method chaining.
        :rtype: QueryBuilder
        """
        self._filter.append(f"{column.lower()} ne null")
        return self

    def filter_raw(self, filter_string: str) -> "QueryBuilder":
        """
        Add a raw OData filter string.

        Use this for complex filters not covered by other methods.
        Column names in the filter string should be lowercase.

        :param filter_string: Raw OData filter expression.
        :type filter_string: str
        :return: Self for method chaining.
        :rtype: QueryBuilder

        Example::

            # Complex OR condition
            query = QueryBuilder("account").filter_raw(
                "(statecode eq 0 or statecode eq 1)"
            )
        """
        self._filter.append(filter_string)
        return self

    def order_by(self, column: str, descending: bool = False) -> "QueryBuilder":
        """
        Add sorting order.

        Can be called multiple times for multi-column sorting.

        :param column: Column name to sort by.
        :type column: str
        :param descending: Sort in descending order.
        :type descending: bool
        :return: Self for method chaining.
        :rtype: QueryBuilder

        Example::

            query = (QueryBuilder("account")
                     .order_by("revenue", descending=True)
                     .order_by("name"))
        """
        order = f"{column.lower()} desc" if descending else column.lower()
        self._orderby.append(order)
        return self

    def top(self, count: int) -> "QueryBuilder":
        """
        Limit the total number of results.

        :param count: Maximum number of records to return.
        :type count: int
        :return: Self for method chaining.
        :rtype: QueryBuilder

        Example::

            query = QueryBuilder("account").top(10)
        """
        if count < 1:
            raise ValueError("top count must be at least 1")
        self._top = count
        return self

    def page_size(self, size: int) -> "QueryBuilder":
        """
        Set the number of records per page.

        Controls how many records are returned in each page/batch
        when iterating through results.

        :param size: Number of records per page.
        :type size: int
        :return: Self for method chaining.
        :rtype: QueryBuilder

        Example::

            query = (QueryBuilder("account")
                     .top(100)       # Total limit
                     .page_size(50)) # 50 records per page
        """
        if size < 1:
            raise ValueError("page_size must be at least 1")
        self._page_size = size
        return self

    def expand(self, *relations: str) -> "QueryBuilder":
        """
        Expand navigation properties.

        :param relations: Navigation property names to expand.
        :type relations: str
        :return: Self for method chaining.
        :rtype: QueryBuilder

        Example::

            query = QueryBuilder("account").expand("primarycontactid")
        """
        self._expand.extend(relations)
        return self

    @staticmethod
    def _format_value(value: Any) -> str:
        """
        Format a value for OData query syntax.

        :param value: Value to format.
        :return: OData-formatted value string.
        :rtype: str
        """
        if value is None:
            return "null"
        if isinstance(value, str):
            # Escape single quotes by doubling them
            escaped = value.replace("'", "''")
            return f"'{escaped}'"
        if isinstance(value, bool):
            return str(value).lower()
        if isinstance(value, (int, float)):
            return str(value)
        # For GUIDs and other types, convert to string
        return str(value)

    def build(self) -> dict:
        """
        Build query parameters dictionary.

        Returns a dictionary that can be passed to the OData client
        for query execution.

        :return: Dictionary with table, select, filter, orderby, expand, top, page_size keys.
        :rtype: dict

        Example::

            query = QueryBuilder("account").filter_eq("statecode", 0).top(10)
            params = query.build()
            # {'table': 'account', 'filter': 'statecode eq 0', 'top': 10}
        """
        params: dict = {"table": self.table}
        if self._select:
            params["select"] = list(self._select)
        if self._filter:
            params["filter"] = " and ".join(self._filter)
        if self._orderby:
            params["orderby"] = list(self._orderby)
        if self._expand:
            params["expand"] = list(self._expand)
        if self._top is not None:
            params["top"] = self._top
        if self._page_size is not None:
            params["page_size"] = self._page_size
        return params

    def execute(self) -> "Iterable[OperationResult[List[Record]]]":
        """
        Execute the query and return an iterator of record pages.

        This method is only available when the QueryBuilder was created via
        ``client.query.builder(table)``. Standalone QueryBuilder instances
        must use ``client.query.execute(query)`` instead.

        Returns a generator yielding pages of records. Each page is an
        OperationResult containing a list of Record objects.

        :return: Generator yielding OperationResult pages of Record objects.
        :raises RuntimeError: If the query was not created via client.query.builder().

        Example::

            for page in (client.query.builder("account")
                         .select("name")
                         .filter_eq("statecode", 0)
                         .execute()):
                for record in page:
                    print(record["name"])

            # With per-page telemetry
            for page in query.execute():
                response = page.with_response_details()
                print(f"Request ID: {response.telemetry['service_request_id']}")
        """
        if self._query_ops is None:
            raise RuntimeError(
                "Cannot execute: query was not created via client.query.builder(). "
                "Use client.query.execute(query) instead."
            )
        return self._query_ops.get(
            self.table,
            select=self._select if self._select else None,
            filter=" and ".join(self._filter) if self._filter else None,
            orderby=list(self._orderby) if self._orderby else None,
            top=self._top,
            expand=list(self._expand) if self._expand else None,
            page_size=self._page_size,
        )


__all__ = ["QueryBuilder"]
