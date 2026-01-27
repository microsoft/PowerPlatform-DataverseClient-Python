# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Query and SQL operations namespace."""

from __future__ import annotations

from typing import Any, Dict, Optional, List, Iterable, TYPE_CHECKING

from ..core.results import OperationResult

if TYPE_CHECKING:
    from ..client import DataverseClient


class QueryOperations:
    """
    Query operations for retrieving records.

    Accessed via ``client.query``. Provides methods for OData queries
    and SQL-based queries.

    Example:
        OData query with pagination::

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
    ) -> Iterable[OperationResult[List[Dict[str, Any]]]]:
        """
        Query records with OData filtering and pagination.

        Returns a generator yielding batches of records. Each batch is an
        OperationResult containing a list of record dicts.

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
        :return: Generator yielding OperationResult batches.
        :rtype: Iterable[OperationResult[List[Dict[str, Any]]]]

        Example:
            Basic query::

                for batch in client.query.get("account", filter="statecode eq 0"):
                    for record in batch:
                        print(record["name"])

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
        """

        def _paged() -> Iterable[OperationResult[List[Dict[str, Any]]]]:
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
                    yield OperationResult(batch, metadata)

        return _paged()

    def sql(self, sql: str) -> OperationResult[List[Dict[str, Any]]]:
        """
        Execute a read-only SQL query.

        :param sql: SQL SELECT statement.
        :type sql: str
        :return: OperationResult containing list of result rows.
        :rtype: OperationResult[List[Dict[str, Any]]]

        :raises ~PowerPlatform.Dataverse.core.errors.SQLParseError: If the SQL query uses
            unsupported syntax.
        :raises ~PowerPlatform.Dataverse.core.errors.HttpError: If the Web API returns an error.

        Example:
            Basic SQL query::

                results = client.query.sql(
                    "SELECT TOP 10 name, revenue FROM account ORDER BY revenue DESC"
                )
                for row in results:
                    print(f"{row['name']}: {row['revenue']}")

            Access telemetry data::

                response = client.query.sql(sql).with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._client._scoped_odata() as od:
            result, metadata = od._query_sql(sql)
            return OperationResult(result, metadata)

    # Future: QueryBuilder support (Priority 4)
    # def builder(self, table: str) -> "QueryBuilder":
    #     """Create a fluent query builder."""
    #     pass


__all__ = ["QueryOperations"]
