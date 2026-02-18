# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Query operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..client import DataverseClient


__all__ = ["QueryOperations"]


class QueryOperations:
    """Namespace for query operations (multi-record retrieval and SQL).

    Accessed via ``client.query``. Provides paginated OData queries and SQL
    query execution against Dataverse tables.

    :param client: The parent :class:`~PowerPlatform.Dataverse.client.DataverseClient` instance.
    :type client: ~PowerPlatform.Dataverse.client.DataverseClient

    Example::

        client = DataverseClient(base_url, credential)

        # Paginated query
        for page in client.query.get("account", select=["name"], top=100):
            for record in page:
                print(record["name"])

        # SQL query
        rows = client.query.sql("SELECT TOP 10 name FROM account ORDER BY name")
        for row in rows:
            print(row["name"])
    """

    def __init__(self, client: DataverseClient) -> None:
        self._client = client

    # -------------------------------------------------------------------- get

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
    ) -> Iterable[List[Dict[str, Any]]]:
        """Query multiple records from a Dataverse table with pagination.

        Returns a generator that yields one page (list of record dicts) at a
        time. Automatically follows ``@odata.nextLink`` for server-side paging.

        :param table: Schema name of the table (e.g. ``"account"`` or
            ``"new_MyTestTable"``).
        :type table: :class:`str`
        :param select: Optional list of column logical names to include.
            Column names are automatically lowercased.
        :type select: :class:`list` of :class:`str` or None
        :param filter: Optional OData ``$filter`` expression (e.g.
            ``"name eq 'Contoso'"``). Column names in filter expressions must
            use exact lowercase logical names. Passed directly without
            transformation.
        :type filter: :class:`str` or None
        :param orderby: Optional list of sort expressions (e.g.
            ``["name asc", "createdon desc"]``). Column names are automatically
            lowercased.
        :type orderby: :class:`list` of :class:`str` or None
        :param top: Optional maximum total number of records to return.
        :type top: :class:`int` or None
        :param expand: Optional list of navigation properties to expand (e.g.
            ``["primarycontactid"]``). Case-sensitive; must match server-defined
            names exactly.
        :type expand: :class:`list` of :class:`str` or None
        :param page_size: Optional per-page size hint sent via
            ``Prefer: odata.maxpagesize``.
        :type page_size: :class:`int` or None

        :return: Generator yielding pages, where each page is a list of record
            dictionaries.
        :rtype: :class:`collections.abc.Iterable` of :class:`list` of :class:`dict`

        Example:
            Query with filtering and pagination::

                for page in client.query.get(
                    "account",
                    filter="statecode eq 0",
                    select=["name", "telephone1"],
                    page_size=50,
                ):
                    for record in page:
                        print(record["name"])

            Query with sorting and limit::

                for page in client.query.get(
                    "account",
                    orderby=["createdon desc"],
                    top=100,
                ):
                    print(f"Page size: {len(page)}")
        """

        def _paged() -> Iterable[List[Dict[str, Any]]]:
            with self._client._scoped_odata() as od:
                yield from od._get_multiple(
                    table,
                    select=select,
                    filter=filter,
                    orderby=orderby,
                    top=top,
                    expand=expand,
                    page_size=page_size,
                )

        return _paged()

    # -------------------------------------------------------------------- sql

    def sql(self, sql: str) -> List[Dict[str, Any]]:
        """Execute a read-only SQL query using the Dataverse Web API.

        The SQL query must follow the supported subset: a single SELECT
        statement with optional WHERE, TOP (integer literal), ORDER BY (column
        names only), and a simple table alias after FROM.

        :param sql: Supported SQL SELECT statement.
        :type sql: :class:`str`

        :return: List of result row dictionaries. Returns an empty list when no
            rows match.
        :rtype: :class:`list` of :class:`dict`

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
            return od._query_sql(sql)
