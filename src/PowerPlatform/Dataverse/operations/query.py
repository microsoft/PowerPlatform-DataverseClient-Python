# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Query operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import Any, Dict, List, TYPE_CHECKING

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

        rows = client.query.sql("SELECT TOP 10 name FROM account ORDER BY name")
        for row in rows:
            print(row["name"])
    """

    def __init__(self, client: DataverseClient) -> None:
        self._client = client

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
