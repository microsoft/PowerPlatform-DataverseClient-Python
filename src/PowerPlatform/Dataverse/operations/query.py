# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Query operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import List, TYPE_CHECKING

from ..models.record import Record

from ..models.query_builder import QueryBuilder

if TYPE_CHECKING:
    from ..client import DataverseClient


__all__ = ["QueryOperations"]


class QueryOperations:
    """Namespace for query operations.

    Accessed via ``client.query``. Provides query and search operations
    against Dataverse tables.

    :param client: The parent :class:`~PowerPlatform.Dataverse.client.DataverseClient` instance.
    :type client: ~PowerPlatform.Dataverse.client.DataverseClient

    Example::

        client = DataverseClient(base_url, credential)

        # Fluent query builder (recommended)
        for record in (client.query.builder("account")
                       .select("name", "revenue")
                       .filter_eq("statecode", 0)
                       .order_by("revenue", descending=True)
                       .top(100)
                       .execute()):
            print(record["name"])

        # SQL query
        rows = client.query.sql("SELECT TOP 10 name FROM account ORDER BY name")
        for row in rows:
            print(row["name"])
    """

    def __init__(self, client: DataverseClient) -> None:
        self._client = client

    # ----------------------------------------------------------------- builder

    def builder(self, table: str) -> QueryBuilder:
        """Create a fluent query builder for the specified table.

        Returns a :class:`~PowerPlatform.Dataverse.models.query_builder.QueryBuilder`
        that can be chained with filter, select, and order methods, then
        executed directly via ``.execute()``.

        :param table: Table schema name (e.g. ``"account"``).
        :type table: :class:`str`
        :return: A QueryBuilder instance bound to this client.
        :rtype: ~PowerPlatform.Dataverse.models.query_builder.QueryBuilder

        Example:
            Build and execute a query fluently::

                for record in (client.query.builder("account")
                               .select("name", "revenue")
                               .filter_eq("statecode", 0)
                               .filter_gt("revenue", 1000000)
                               .order_by("revenue", descending=True)
                               .top(100)
                               .page_size(50)
                               .execute()):
                    print(record["name"])

            With composable expression tree::

                from PowerPlatform.Dataverse.models.filters import eq, gt

                for record in (client.query.builder("account")
                               .where((eq("statecode", 0) | eq("statecode", 1))
                                      & gt("revenue", 100000))
                               .execute()):
                    print(record["name"])
        """
        qb = QueryBuilder(table)
        qb._query_ops = self
        return qb

    # -------------------------------------------------------------------- sql

    def sql(self, sql: str) -> List[Record]:
        """Execute a read-only SQL query using the Dataverse Web API.

        The SQL query must follow the supported subset: a single SELECT
        statement with optional WHERE, TOP (integer literal), ORDER BY (column
        names only), and a simple table alias after FROM.

        :param sql: Supported SQL SELECT statement.
        :type sql: :class:`str`

        :return: List of :class:`~PowerPlatform.Dataverse.models.record.Record`
            objects. Returns an empty list when no rows match.
        :rtype: :class:`list` of
            :class:`~PowerPlatform.Dataverse.models.record.Record`

        :raises ~PowerPlatform.Dataverse.core.errors.ValidationError:
            If ``sql`` is not a string or is empty.

        Example:
            Basic SQL query::

                rows = client.query.sql(
                    "SELECT TOP 10 accountid, name FROM account "
                    "WHERE name LIKE 'C%' ORDER BY name"
                )
                for row in rows:
                    print(row["name"])

            Query with alias::

                rows = client.query.sql(
                    "SELECT a.name, a.telephone1 FROM account AS a "
                    "WHERE a.statecode = 0"
                )
        """
        with self._client._scoped_odata() as od:
            rows = od._query_sql(sql)
            return [Record.from_api_response("", row) for row in rows]
