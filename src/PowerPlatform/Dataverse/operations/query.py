# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Query operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import Iterable, List, Optional, TYPE_CHECKING

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
        :rtype: list[~PowerPlatform.Dataverse.models.record.Record]

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

    # -------------------------------------------------------------------- fetchxml

    def fetchxml(
        self,
        fetchxml: str,
        *,
        page_size: Optional[int] = None,
    ) -> Iterable[List[Record]]:
        """Execute a FetchXML query with automatic paging cookie handling.

        Executes a FetchXML query against Dataverse via the Web API. The FetchXML
        string must contain a root ``<fetch>`` element with a child ``<entity name='...'>``
        element. Results are yielded as pages (lists of
        :class:`~PowerPlatform.Dataverse.models.record.Record` objects).

        :param fetchxml: Raw FetchXML string (e.g. ``<fetch><entity name='account'>...</entity></fetch>``).
        :type fetchxml: :class:`str`
        :param page_size: Optional page size override. Sets the ``count`` attribute on
            ``<fetch>`` if not already present.
        :type page_size: :class:`int` | ``None``

        .. note::
            The ``page_size`` parameter is always validated, but only applied (sets the
            ``count`` attribute) when the FetchXML does not already contain one.
            If the FetchXML already has a ``count`` attribute, it takes precedence.

        .. note::
            Raw FetchXML input is limited to 16,000 characters (security cap before XML
            parsing). The final encoded request URL is limited to 32,768 bytes. Paging
            cookies can increase URL size on subsequent pages.

        :return: Generator yielding pages, where each page is a list of
            :class:`~PowerPlatform.Dataverse.models.record.Record` objects.
        :rtype: :class:`~typing.Iterable` of :class:`list` of
            :class:`~PowerPlatform.Dataverse.models.record.Record`

        :raises ~PowerPlatform.Dataverse.core.errors.ValidationError:
            If ``fetchxml`` is not a string, is empty, or has invalid structure.

        Example:
            Basic FetchXML query::

                fetchxml = '''
                <fetch top='5'>
                  <entity name='account'>
                    <attribute name='name' />
                  </entity>
                </fetch>
                '''
                for page in client.query.fetchxml(fetchxml):
                    for record in page:
                        print(record["name"])

            Paginated query with page size::

                fetchxml = '''
                <fetch>
                  <entity name='contact'>
                    <attribute name='fullname' />
                    <filter type='and'>
                      <condition attribute='statecode' operator='eq' value='0' />
                    </filter>
                  </entity>
                </fetch>
                '''
                for page in client.query.fetchxml(fetchxml, page_size=50):
                    for record in page:
                        print(record["fullname"])
        """

        def _paged() -> Iterable[List[Record]]:
            with self._client._scoped_odata() as od:
                entity_name = ""
                for page in od._query_fetchxml(fetchxml, page_size=page_size):
                    if not entity_name:
                        entity_name = od._extract_entity_from_fetchxml(fetchxml)
                    yield [Record.from_api_response(entity_name, row) for row in page]

        return _paged()
