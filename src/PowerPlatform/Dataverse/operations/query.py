# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Query and SQL operations namespace."""

from __future__ import annotations

from typing import Any, Dict, Optional, List, Iterable, TYPE_CHECKING

from ..core.results import OperationResult
from ..models.record import Record
from ..models.query_builder import QueryBuilder

if TYPE_CHECKING:
    from ..client import DataverseClient


class QueryOperations:
    """
    Query operations for retrieving records.

    Accessed via ``client.query``. Provides methods for OData queries,
    SQL-based queries, and fluent query building.

    Example:
        Fluent query builder (recommended)::

            query = (client.query.builder("account")
                     .select("name", "revenue")
                     .filter_eq("statecode", 0)
                     .filter_gt("revenue", 1000000)
                     .order_by("revenue", descending=True)
                     .top(10))

            for record in client.query.iterate_query(query):
                print(f"{record['name']}: ${record['revenue']}")

        OData query with string filter::

            for batch in client.query.get("account", filter="statecode eq 0"):
                for record in batch:
                    print(record["name"])

        SQL query::

            results = client.query.sql("SELECT name FROM account WHERE statecode = 0")
            for row in results:
                print(row["name"])

        With telemetry::

            response = client.query.sql(sql).with_response_details()
            print(response.telemetry["service_request_id"])
    """

    def __init__(self, client: "DataverseClient") -> None:
        """
        Initialize QueryOperations.

        :param client: Parent DataverseClient instance.
        :type client: DataverseClient
        """
        self._client = client

    def get(
        self,
        table: str,
        *,
        select: Optional[List[str]] = None,
        filter: Optional[str] = None,
        orderby: Optional[List[str]] = None,
        top: Optional[int] = None,
        expand: Optional[List[str]] = None,
        page_size: Optional[int] = None,
    ) -> Iterable[OperationResult[List[Record]]]:
        """
        Query records with OData filtering and pagination.

        Returns a generator yielding batches of records. Each batch is an
        OperationResult containing a list of Record objects.

        :param table: Table schema name.
        :type table: str
        :param select: Columns to retrieve (auto-lowercased).
        :type select: list[str] or None
        :param filter: OData filter string (column names must be lowercase).
        :type filter: str or None
        :param orderby: Sort order (e.g., ["name asc", "createdon desc"]).
        :type orderby: list[str] or None
        :param top: Maximum records to return.
        :type top: int or None
        :param expand: Navigation properties to expand.
        :type expand: list[str] or None
        :param page_size: Records per page.
        :type page_size: int or None
        :return: Generator yielding OperationResult batches of Record objects.
        :rtype: Iterable[OperationResult[List[Record]]]

        Example:
            Basic query::

                for batch in client.query.get("account", filter="statecode eq 0"):
                    for record in batch:
                        print(record["name"])  # Dict-like access (backward compatible)
                        print(record.id)       # Structured access to record GUID

            With pagination control::

                for batch in client.query.get(
                    "contact",
                    select=["fullname", "emailaddress1"],
                    filter="statecode eq 0",
                    orderby=["createdon desc"],
                    top=100,
                    page_size=50
                ):
                    print(f"Batch of {len(batch)} records")

            With per-page telemetry access::

                for batch in client.query.get("account", filter="statecode eq 0"):
                    response = batch.with_response_details()
                    print(f"Page request ID: {response.telemetry['service_request_id']}")
                    for account in response.result:
                        print(account["name"])
                        print(account.id)  # Access record GUID
        """

        def _paged() -> Iterable[OperationResult[List[Record]]]:
            with self._client._scoped_odata() as od:
                for batch, metadata in od._get_multiple(
                    table,
                    select=select,
                    filter=filter,
                    orderby=orderby,
                    top=top,
                    expand=expand,
                    page_size=page_size,
                ):
                    records = [Record.from_api_response(table, record_data) for record_data in batch]
                    yield OperationResult(records, metadata)

        return _paged()

    def sql(self, sql: str) -> OperationResult[List[Record]]:
        """
        Execute a read-only SQL query.

        :param sql: SQL SELECT statement.
        :type sql: str
        :return: OperationResult containing list of Record objects.
        :rtype: OperationResult[List[Record]]

        :raises ~PowerPlatform.Dataverse.core.errors.SQLParseError: If the SQL query uses
            unsupported syntax.
        :raises ~PowerPlatform.Dataverse.core.errors.HttpError: If the Web API returns an error.

        Example:
            Basic SQL query::

                results = client.query.sql(
                    "SELECT TOP 10 name, revenue FROM account ORDER BY revenue DESC"
                )
                for row in results:
                    print(f"{row['name']}: {row['revenue']}")  # Dict-like access
                    print(f"Record ID: {row.id}")              # Structured access

            Access telemetry data::

                response = client.query.sql(sql).with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._client._scoped_odata() as od:
            result, metadata = od._query_sql(sql)
            # SQL queries may not have a single table context, use empty string
            # The Record.from_api_response will extract the table from the result if available
            records = [Record.from_api_response("", record_data) for record_data in result]
            return OperationResult(records, metadata)

    def builder(self, table: str) -> QueryBuilder:
        """
        Create a fluent query builder for the specified table.

        :param table: Table schema name.
        :type table: str
        :return: QueryBuilder instance for fluent query construction.
        :rtype: QueryBuilder

        Example:
            Build and execute a query::

                query = (client.query.builder("account")
                         .select("name", "revenue")
                         .filter_eq("statecode", 0)
                         .filter_gt("revenue", 1000000)
                         .order_by("revenue", descending=True)
                         .top(10))

                for record in client.query.iterate_query(query):
                    print(record["name"])
        """
        return QueryBuilder(table)

    def execute(
        self,
        query: QueryBuilder,
        *,
        page_size: Optional[int] = None,
    ) -> Iterable[OperationResult[List[Record]]]:
        """
        Execute a QueryBuilder query and return iterator of record batches.

        Returns a generator yielding batches of records. Each batch is an
        OperationResult containing a list of Record objects.

        :param query: QueryBuilder instance with query configuration.
        :type query: QueryBuilder
        :param page_size: Records per page (uses default from config if not specified).
        :type page_size: int or None
        :return: Generator yielding OperationResult batches of Record objects.
        :rtype: Iterable[OperationResult[List[Record]]]

        Example:
            Execute a fluent query::

                query = (client.query.builder("account")
                         .select("name", "revenue")
                         .filter_gt("revenue", 500000)
                         .order_by("revenue", descending=True)
                         .top(100))

                for batch in client.query.execute(query):
                    for record in batch:
                        print(f"{record['name']}: ${record['revenue']}")

            With per-page telemetry::

                for batch in client.query.execute(query, page_size=50):
                    response = batch.with_response_details()
                    print(f"Page request: {response.telemetry['service_request_id']}")
                    for record in response.result:
                        print(record["name"])
        """
        params = query.build()
        return self.get(
            params["table"],
            select=params.get("select"),
            filter=params.get("filter"),
            orderby=params.get("orderby"),
            top=params.get("top"),
            expand=params.get("expand"),
            page_size=page_size,
        )

    def iterate(
        self,
        table: str,
        *,
        select: Optional[List[str]] = None,
        filter: Optional[str] = None,
        orderby: Optional[List[str]] = None,
        top: Optional[int] = None,
        expand: Optional[List[str]] = None,
        page_size: Optional[int] = None,
    ) -> Iterable[Record]:
        """
        Iterate records one at a time with automatic pagination.

        This is a convenience wrapper over get() that yields individual
        Record objects instead of batches.

        :param table: Table schema name.
        :type table: str
        :param select: Columns to retrieve (auto-lowercased).
        :type select: list[str] or None
        :param filter: OData filter string (column names must be lowercase).
        :type filter: str or None
        :param orderby: Sort order (e.g., ["name asc", "createdon desc"]).
        :type orderby: list[str] or None
        :param top: Maximum records to return.
        :type top: int or None
        :param expand: Navigation properties to expand.
        :type expand: list[str] or None
        :param page_size: Records per page.
        :type page_size: int or None
        :return: Generator yielding individual Record objects.
        :rtype: Iterable[Record]

        Example:
            Simple iteration::

                for account in client.query.iterate(
                    "account",
                    filter="statecode eq 0",
                    select=["name", "revenue"]
                ):
                    print(f"{account['name']}: ${account['revenue']}")

            With QueryBuilder (use iterate_query instead)::

                query = client.query.builder("account").filter_eq("statecode", 0).top(100)
                for record in client.query.iterate_query(query):
                    print(record["name"])
        """
        for batch in self.get(
            table,
            select=select,
            filter=filter,
            orderby=orderby,
            top=top,
            expand=expand,
            page_size=page_size,
        ):
            for record in batch:
                yield record

    def iterate_query(
        self,
        query: QueryBuilder,
        *,
        page_size: Optional[int] = None,
    ) -> Iterable[Record]:
        """
        Iterate records from a QueryBuilder one at a time.

        This is a convenience wrapper over execute() that yields individual
        Record objects instead of batches.

        :param query: QueryBuilder instance with query configuration.
        :type query: QueryBuilder
        :param page_size: Records per page.
        :type page_size: int or None
        :return: Generator yielding individual Record objects.
        :rtype: Iterable[Record]

        Example::

            query = (client.query.builder("account")
                     .filter_gt("revenue", 1000000)
                     .order_by("revenue", descending=True))

            for account in client.query.iterate_query(query):
                print(f"{account['name']}: ${account['revenue']}")
        """
        for batch in self.execute(query, page_size=page_size):
            for record in batch:
                yield record


__all__ = ["QueryOperations"]
