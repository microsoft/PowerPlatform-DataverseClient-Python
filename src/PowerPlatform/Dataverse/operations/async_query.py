# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Async query operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import List, TYPE_CHECKING

from ..models.record import Record

if TYPE_CHECKING:
    from ..async_client import AsyncDataverseClient


__all__ = ["AsyncQueryOperations"]


class AsyncQueryOperations:
    """Async namespace for query operations.

    Accessed via ``client.query`` on
    :class:`~PowerPlatform.Dataverse.async_client.AsyncDataverseClient`.

    :param client: The parent async client instance.
    :type client: ~PowerPlatform.Dataverse.async_client.AsyncDataverseClient

    Example::

        async with AsyncDataverseClient(base_url, credential) as client:
            rows = await client.query.sql("SELECT TOP 10 name FROM account ORDER BY name")
            for row in rows:
                print(row["name"])
    """

    def __init__(self, client: AsyncDataverseClient) -> None:
        self._client = client

    # -------------------------------------------------------------------- sql

    async def sql(self, sql: str) -> List[Record]:
        """Execute a read-only SQL query using the Dataverse Web API.

        :param sql: Supported SQL SELECT statement.
        :type sql: :class:`str`

        :return: List of :class:`~PowerPlatform.Dataverse.models.record.Record` objects.
        :rtype: :class:`list` of :class:`~PowerPlatform.Dataverse.models.record.Record`

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
