# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Async DataFrame CRUD operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, List, Optional

import pandas as pd

from ...utils._pandas import dataframe_to_records

if TYPE_CHECKING:
    from ..async_client import AsyncDataverseClient


__all__ = ["AsyncDataFrameOperations"]


class AsyncDataFrameOperations:
    """Async namespace for pandas DataFrame CRUD operations.

    Accessed via ``client.dataframe``. Provides DataFrame-oriented wrappers
    around the async record-level CRUD operations.

    :param client: The parent :class:`~PowerPlatform.Dataverse.aio.async_client.AsyncDataverseClient` instance.
    :type client: ~PowerPlatform.Dataverse.aio.async_client.AsyncDataverseClient

    Example::

        import pandas as pd

        async with AsyncDataverseClient(base_url, credential) as client:

            # Query records as a DataFrame
            df = await client.dataframe.get("account", select=["name"], top=100)

            # Create records from a DataFrame
            new_df = pd.DataFrame([{"name": "Contoso"}, {"name": "Fabrikam"}])
            new_df["accountid"] = await client.dataframe.create("account", new_df)

            # Update records
            new_df["telephone1"] = ["555-0100", "555-0200"]
            await client.dataframe.update("account", new_df, id_column="accountid")

            # Delete records
            await client.dataframe.delete("account", new_df["accountid"])
    """

    def __init__(self, client: "AsyncDataverseClient") -> None:
        self._client = client

    # --------------------------------------------------------------------- sql

    async def sql(self, sql: str) -> pd.DataFrame:
        """Execute a SQL query and return the results as a pandas DataFrame.

        Delegates to :meth:`~PowerPlatform.Dataverse.aio.operations.async_query.AsyncQueryOperations.sql`
        and converts the list of records into a single DataFrame.

        :param sql: Supported SQL SELECT statement.
        :type sql: :class:`str`

        :return: DataFrame containing all result rows. Returns an empty
            DataFrame when no rows match.
        :rtype: ~pandas.DataFrame

        :raises ~PowerPlatform.Dataverse.core.errors.ValidationError:
            If ``sql`` is not a string or is empty.

        Example:
            SQL query to DataFrame::

                df = await client.dataframe.sql(
                    "SELECT TOP 100 name, revenue FROM account "
                    "WHERE statecode = 0 ORDER BY revenue"
                )
                print(f"Got {len(df)} rows")
                print(df.head())
        """
        rows = await self._client.query.sql(sql)
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame.from_records([r.data for r in rows])

    # -------------------------------------------------------------------- get

    async def get(
        self,
        table: str,
        record_id: Optional[str] = None,
        select: Optional[List[str]] = None,
        filter: Optional[str] = None,
        orderby: Optional[List[str]] = None,
        top: Optional[int] = None,
        expand: Optional[List[str]] = None,
        page_size: Optional[int] = None,
        count: bool = False,
        include_annotations: Optional[str] = None,
    ) -> pd.DataFrame:
        """Fetch records and return as a single pandas DataFrame.

        When ``record_id`` is provided, returns a single-row DataFrame.
        When ``record_id`` is None, internally iterates all pages and returns one
        consolidated DataFrame.

        :param table: Schema name of the table (e.g. ``"account"`` or ``"new_MyTestTable"``).
        :type table: :class:`str`
        :param record_id: Optional GUID to fetch a specific record. If None, queries multiple records.
        :type record_id: :class:`str` or None
        :param select: Optional list of attribute logical names to retrieve.
        :type select: list[str] or None
        :param filter: Optional OData filter string. Column names must use exact lowercase logical names.
        :type filter: :class:`str` or None
        :param orderby: Optional list of attributes to sort by.
        :type orderby: list[str] or None
        :param top: Optional maximum number of records to return.
        :type top: :class:`int` or None
        :param expand: Optional list of navigation properties to expand (case-sensitive).
        :type expand: list[str] or None
        :param page_size: Optional number of records per page for pagination.
        :type page_size: :class:`int` or None
        :param count: If ``True``, adds ``$count=true`` to include a total
            record count in the response.
        :type count: :class:`bool`
        :param include_annotations: OData annotation pattern for the
            ``Prefer: odata.include-annotations`` header (e.g. ``"*"`` or
            ``"OData.Community.Display.V1.FormattedValue"``), or ``None``.
        :type include_annotations: :class:`str` or None

        :return: DataFrame containing all matching records. Returns an empty DataFrame
            when no records match.
        :rtype: ~pandas.DataFrame

        :raises ValueError: If ``record_id`` is not a non-empty string, or if
            query parameters (``filter``, ``orderby``, ``top``, ``expand``,
            ``page_size``) are provided alongside ``record_id``.

        Example:
            Fetch a single record as a DataFrame::

                df = await client.dataframe.get("account", record_id=account_id, select=["name"])
                print(df)

            Query with filtering::

                df = await client.dataframe.get("account", filter="statecode eq 0", select=["name"])
                print(f"Got {len(df)} active accounts")
        """
        if record_id is not None:
            if not isinstance(record_id, str) or not record_id.strip():
                raise ValueError("record_id must be a non-empty string")
            record_id = record_id.strip()
            if any(p is not None for p in (filter, orderby, top, expand, page_size)):
                raise ValueError(
                    "Cannot specify query parameters (filter, orderby, top, "
                    "expand, page_size) when fetching a single record by ID"
                )
            result = await self._client.records.get(
                table,
                record_id,
                select=select,
            )
            return pd.DataFrame([result.data])

        rows: List[dict] = []
        async for batch in await self._client.records.get(
            table,
            select=select,
            filter=filter,
            orderby=orderby,
            top=top,
            expand=expand,
            page_size=page_size,
            count=count,
            include_annotations=include_annotations,
        ):
            rows.extend(row.data for row in batch)

        if not rows:
            return pd.DataFrame(columns=select) if select else pd.DataFrame()
        return pd.DataFrame.from_records(rows)

    # ----------------------------------------------------------------- create

    async def create(
        self,
        table: str,
        records: pd.DataFrame,
    ) -> pd.Series:
        """Create records from a pandas DataFrame.

        :param table: Schema name of the table (e.g. ``"account"`` or ``"new_MyTestTable"``).
        :type table: :class:`str`
        :param records: DataFrame where each row is a record to create.
        :type records: ~pandas.DataFrame

        :return: Series of created record GUIDs, aligned with the input DataFrame index.
        :rtype: ~pandas.Series

        :raises TypeError: If ``records`` is not a pandas DataFrame.
        :raises ValueError: If ``records`` is empty or the number of returned
            IDs does not match the number of input rows.

        Example:
            Create records from a DataFrame::

                import pandas as pd

                df = pd.DataFrame([
                    {"name": "Contoso", "telephone1": "555-0100"},
                    {"name": "Fabrikam", "telephone1": "555-0200"},
                ])
                df["accountid"] = await client.dataframe.create("account", df)
        """
        if not isinstance(records, pd.DataFrame):
            raise TypeError("records must be a pandas DataFrame")

        if records.empty:
            raise ValueError("records must be a non-empty DataFrame")

        record_list = dataframe_to_records(records)

        # Detect rows where all values were NaN/None (empty dicts after normalization)
        empty_rows = [records.index[i] for i, r in enumerate(record_list) if not r]
        if empty_rows:
            raise ValueError(
                f"Records at index(es) {empty_rows} have no non-null values. "
                "All rows must contain at least one field to create."
            )

        ids = await self._client.records.create(table, record_list)

        if len(ids) != len(records):
            raise ValueError(f"Server returned {len(ids)} IDs for {len(records)} input rows")

        return pd.Series(ids, index=records.index)

    # ----------------------------------------------------------------- update

    async def update(
        self,
        table: str,
        changes: pd.DataFrame,
        id_column: str,
        clear_nulls: bool = False,
    ) -> None:
        """Update records from a pandas DataFrame.

        Each row in the DataFrame represents an update. The ``id_column`` specifies which
        column contains the record GUIDs.

        :param table: Schema name of the table (e.g. ``"account"`` or ``"new_MyTestTable"``).
        :type table: :class:`str`
        :param changes: DataFrame where each row contains a record GUID and the fields to update.
        :type changes: ~pandas.DataFrame
        :param id_column: Name of the DataFrame column containing record GUIDs.
        :type id_column: :class:`str`
        :param clear_nulls: When ``False`` (default), missing values (NaN/None) are skipped
            (the field is left unchanged on the server). When ``True``, missing values are sent
            as ``null`` to Dataverse, clearing the field.
        :type clear_nulls: :class:`bool`

        :raises TypeError: If ``changes`` is not a pandas DataFrame.
        :raises ValueError: If ``changes`` is empty, ``id_column`` is not found in the
            DataFrame, ``id_column`` contains invalid values, or no updatable columns exist.

        Example:
            Update records with different values per row::

                import pandas as pd

                df = pd.DataFrame([
                    {"accountid": "guid-1", "telephone1": "555-0100"},
                    {"accountid": "guid-2", "telephone1": "555-0200"},
                ])
                await client.dataframe.update("account", df, id_column="accountid")
        """
        if not isinstance(changes, pd.DataFrame):
            raise TypeError("changes must be a pandas DataFrame")
        if changes.empty:
            raise ValueError("changes must be a non-empty DataFrame")
        if id_column not in changes.columns:
            raise ValueError(f"id_column '{id_column}' not found in DataFrame columns")

        raw_ids = changes[id_column].tolist()
        invalid = [changes.index[i] for i, v in enumerate(raw_ids) if not isinstance(v, str) or not v.strip()]
        if invalid:
            raise ValueError(
                f"id_column '{id_column}' contains invalid values at row index(es) {invalid}. "
                "All IDs must be non-empty strings."
            )
        ids = [v.strip() for v in raw_ids]

        change_columns = [column for column in changes.columns if column != id_column]
        if not change_columns:
            raise ValueError(
                "No columns to update. The DataFrame must contain at least one column besides the id_column."
            )
        change_list = dataframe_to_records(changes[change_columns], na_as_null=clear_nulls)

        # Filter out rows where all change values were NaN/None (empty dicts)
        paired = [(rid, patch) for rid, patch in zip(ids, change_list) if patch]
        if not paired:
            return
        ids_filtered: List[str] = [p[0] for p in paired]
        change_filtered: List[Dict[str, Any]] = [p[1] for p in paired]

        if len(ids_filtered) == 1:
            await self._client.records.update(table, ids_filtered[0], change_filtered[0])
        else:
            await self._client.records.update(table, ids_filtered, change_filtered)

    # ----------------------------------------------------------------- delete

    async def delete(
        self,
        table: str,
        ids: pd.Series,
        use_bulk_delete: bool = True,
    ) -> Optional[str]:
        """Delete records by passing a pandas Series of GUIDs.

        :param table: Schema name of the table (e.g. ``"account"`` or ``"new_MyTestTable"``).
        :type table: :class:`str`
        :param ids: Series of record GUIDs to delete.
        :type ids: ~pandas.Series
        :param use_bulk_delete: When ``True`` (default) and ``ids`` contains multiple values,
            execute the BulkDelete action and return its async job identifier.
            When ``False`` each record is deleted sequentially.
        :type use_bulk_delete: :class:`bool`

        :raises TypeError: If ``ids`` is not a pandas Series.
        :raises ValueError: If ``ids`` contains invalid values.

        :return: BulkDelete job ID when deleting multiple records via BulkDelete;
            ``None`` when deleting a single record, using sequential deletion, or
            when ``ids`` is empty.
        :rtype: :class:`str` or None

        Example:
            Delete records using a Series::

                import pandas as pd

                ids = pd.Series(["guid-1", "guid-2", "guid-3"])
                await client.dataframe.delete("account", ids)
        """
        if not isinstance(ids, pd.Series):
            raise TypeError("ids must be a pandas Series")

        raw_list = ids.tolist()
        if not raw_list:
            return None

        invalid = [ids.index[i] for i, v in enumerate(raw_list) if not isinstance(v, str) or not v.strip()]
        if invalid:
            raise ValueError(
                f"ids Series contains invalid values at index(es) {invalid}. " f"All IDs must be non-empty strings."
            )
        id_list = [v.strip() for v in raw_list]

        if len(id_list) == 1:
            await self._client.records.delete(table, id_list[0])
            return None
        return await self._client.records.delete(table, id_list, use_bulk_delete=use_bulk_delete)
