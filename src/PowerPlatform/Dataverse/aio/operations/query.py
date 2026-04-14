# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Async query operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import Any, AsyncGenerator, List, Optional

from ...models.record import Record
from ...models.query_builder import QueryBuilder

__all__ = ["AsyncQueryOperations", "AsyncQueryBuilder"]


class AsyncQueryBuilder(QueryBuilder):
    """Async query builder.

    Extends :class:`~PowerPlatform.Dataverse.models.query_builder.QueryBuilder`
    with ``async def execute()`` and ``async def to_dataframe()`` that call
    the async client instead of the sync client.

    Do not instantiate directly; use ``client.query.builder(table)``.
    """

    async def execute(  # type: ignore[override]
        self,
        *,
        by_page: bool = False,
    ) -> Any:
        """Execute the query and return an async iterator of results.

        :param by_page: If ``True``, yields pages (lists of
            :class:`~PowerPlatform.Dataverse.models.record.Record` objects).
            If ``False`` (default), yields individual records.
        :type by_page: :class:`bool`

        :return: Async generator yielding individual records or pages.

        :raises RuntimeError: If the query was not created via
            ``client.query.builder()``.
        :raises ValueError: If no ``select``, ``filter``, or ``top``
            constraint has been set.

        Example::

            async for record in await (
                client.query.builder("account")
                .select("name")
                .filter_eq("statecode", 0)
                .execute()
            ):
                print(record["name"])
        """
        if self._query_ops is None:
            raise RuntimeError(
                "Cannot execute: query was not created via client.query.builder(). "
                "Use build() and pass parameters to client.records.get() instead."
            )
        self._validate_constraints()
        params = self.build()
        client = self._query_ops._client

        pages: AsyncGenerator[List[Record], None] = await client.records.get(
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

        async def _flat() -> AsyncGenerator[Record, None]:
            async for page in pages:
                for rec in page:
                    yield rec

        return _flat()

    async def to_dataframe(self) -> Any:  # type: ignore[override]
        """Execute the query and return results as a pandas DataFrame.

        :return: DataFrame containing all matching records.
        :rtype: ~pandas.DataFrame

        :raises RuntimeError: If the query was not created via
            ``client.query.builder()``.
        :raises ValueError: If no ``select``, ``filter``, or ``top``
            constraint has been set.

        Example::

            df = await (
                client.query.builder("account")
                .select("name", "telephone1")
                .filter_eq("statecode", 0)
                .top(100)
                .to_dataframe()
            )
        """
        if self._query_ops is None:
            raise RuntimeError(
                "Cannot execute: query was not created via client.query.builder(). "
                "Use build() and pass parameters to client.dataframe.get() instead."
            )
        self._validate_constraints()
        params = self.build()
        return await self._query_ops._client.dataframe.get(
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


class AsyncQueryOperations:
    """Async namespace for query operations.

    Accessed via ``client.query``.  Async counterpart of
    :class:`~PowerPlatform.Dataverse.operations.query.QueryOperations`.

    :param client: The parent
        :class:`~PowerPlatform.Dataverse.aio.AsyncDataverseClient` instance.

    Example::

        # Fluent async query builder
        async for record in await (
            client.query.builder("account")
            .select("name", "revenue")
            .filter_eq("statecode", 0)
            .execute()
        ):
            print(record["name"])

        # SQL query
        rows = await client.query.sql("SELECT TOP 10 name FROM account ORDER BY name")
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    def builder(self, table: str) -> AsyncQueryBuilder:
        """Create a fluent async query builder for the specified table.

        :param table: Table schema name (e.g. ``"account"``).
        :type table: :class:`str`

        :return: An :class:`AsyncQueryBuilder` bound to this client.
        :rtype: AsyncQueryBuilder

        Example::

            async for record in await (
                client.query.builder("account")
                .select("name", "revenue")
                .filter_eq("statecode", 0)
                .execute()
            ):
                print(record["name"])
        """
        qb = AsyncQueryBuilder(table)
        qb._query_ops = self
        return qb

    async def sql(self, sql: str) -> List[Record]:
        """Execute a read-only SQL query using the Dataverse Web API.

        :param sql: Supported SQL SELECT statement.
        :type sql: :class:`str`

        :return: List of :class:`~PowerPlatform.Dataverse.models.record.Record`
            objects. Returns an empty list when no rows match.
        :rtype: list[~PowerPlatform.Dataverse.models.record.Record]

        :raises ~PowerPlatform.Dataverse.core.errors.ValidationError:
            If ``sql`` is not a string or is empty.

        Example::

            rows = await client.query.sql(
                "SELECT TOP 10 accountid, name FROM account WHERE name LIKE 'C%'"
            )
            for row in rows:
                print(row["name"])
        """
        async with self._client._scoped_odata() as od:
            rows = await od._query_sql(sql)
            return [Record.from_api_response("", row) for row in rows]
