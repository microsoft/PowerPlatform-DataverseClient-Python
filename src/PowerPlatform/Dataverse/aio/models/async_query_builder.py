# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""AsyncQueryBuilder — async execution layer over the shared QueryBuilder."""

from __future__ import annotations

from typing import AsyncIterator, List

from ...models.query_builder import _QueryBuilderBase
from ...models.record import QueryResult, Record

__all__ = ["AsyncQueryBuilder"]


class AsyncQueryBuilder(_QueryBuilderBase):
    """Async-capable QueryBuilder.

    Identical fluent interface to :class:`~PowerPlatform.Dataverse.models.query_builder.QueryBuilder`
    — all chaining methods (``select``, ``where``, ``order_by``, ``top``, ``page_size``,
    ``count``, ``expand``, ``include_annotations``, ``include_formatted_values``) are
    inherited unchanged.  Only the execution methods are overridden as coroutines.

    Obtained via ``client.query.builder(table)`` on an async client.

    Example::

        from PowerPlatform.Dataverse.models.filters import col

        result = await (client.query.builder("account")
                        .select("name", "revenue")
                        .where(col("statecode") == 0)
                        .order_by("revenue", descending=True)
                        .top(100)
                        .execute())
        for record in result:
            print(record["name"])
    """

    async def execute(self) -> QueryResult:
        """Execute the query and return all results as a :class:`QueryResult`.

        Awaitable — fetches all pages and holds every record in memory before
        returning. Use :meth:`execute_pages` for lazy per-page streaming.

        At least one of ``select()``, ``where()``, ``top()``, or
        ``page_size()`` must be called first to prevent accidental full-table
        scans.

        :return: All matching records across all pages.
        :rtype: :class:`~PowerPlatform.Dataverse.models.record.QueryResult`
        :raises ValueError: If no scope constraint has been set.
        :raises RuntimeError: If the builder was not created via
            ``client.query.builder()``.

        Example::

            result = await (client.query.builder("account")
                            .select("name")
                            .where(col("statecode") == 0)
                            .execute())
            for record in result:
                print(record["name"])
        """
        if self._query_ops is None:
            raise RuntimeError(
                "Cannot execute: query was not created via client.query.builder(). "
                "Use build() and pass parameters to client.records.list() instead."
            )
        if not self._select and not self._filter_parts and self._top is None and self._page_size is None:
            raise ValueError(
                "At least one of select(), where(), top(), or page_size() must be called before "
                "execute() to prevent accidental full-table scans."
            )
        params = self.build()
        client = self._query_ops._client
        all_records: List[Record] = []
        async with client._scoped_odata() as od:
            async for page in od._get_multiple(
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

    async def execute_pages(self) -> AsyncIterator[QueryResult]:
        """Lazily yield one :class:`QueryResult` per HTTP page.

        Each iteration triggers one network request. One-shot — do not
        iterate more than once.

        At least one of ``select()``, ``where()``, ``top()``, or
        ``page_size()`` must be called first to prevent accidental full-table
        scans.

        :return: Async iterator of per-page
            :class:`~PowerPlatform.Dataverse.models.record.QueryResult` objects.
        :rtype: AsyncIterator[:class:`~PowerPlatform.Dataverse.models.record.QueryResult`]
        :raises ValueError: If no scope constraint has been set.
        :raises RuntimeError: If the builder was not created via
            ``client.query.builder()``.

        Example::

            async for page in (client.query.builder("account")
                               .select("name")
                               .execute_pages()):
                process(page.to_dataframe())
        """
        if self._query_ops is None:
            raise RuntimeError(
                "Cannot execute: query was not created via client.query.builder(). "
                "Use build() and pass parameters to client.records.list() instead."
            )
        if not self._select and not self._filter_parts and self._top is None and self._page_size is None:
            raise ValueError(
                "At least one of select(), where(), top(), or page_size() must be called before "
                "execute_pages() to prevent accidental full-table scans."
            )
        params = self.build()
        client = self._query_ops._client
        async with client._scoped_odata() as od:
            async for page in od._get_multiple(
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
