# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""DataFrame CRUD operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import List, Optional, TYPE_CHECKING

import pandas as pd

from ..utils._pandas import dataframe_to_records

if TYPE_CHECKING:
    from ..client import DataverseClient


__all__ = ["DataFrameOperations"]


class DataFrameOperations:
    """Namespace for pandas DataFrame CRUD operations.

    Accessed via ``client.dataframe``. Provides DataFrame-oriented wrappers
    around the record-level CRUD operations.

    :param client: The parent :class:`~PowerPlatform.Dataverse.client.DataverseClient` instance.
    :type client: ~PowerPlatform.Dataverse.client.DataverseClient

    Example::

        import pandas as pd

        client = DataverseClient(base_url, credential)

        # Query records as a DataFrame
        df = client.dataframe.get("account", select=["name"], top=100)

        # Create records from a DataFrame
        new_df = pd.DataFrame([{"name": "Contoso"}, {"name": "Fabrikam"}])
        new_df["accountid"] = client.dataframe.create("account", new_df)

        # Update records
        new_df["telephone1"] = ["555-0100", "555-0200"]
        client.dataframe.update("account", new_df, id_column="accountid")

        # Delete records
        client.dataframe.delete("account", new_df["accountid"])
    """

    def __init__(self, client: DataverseClient) -> None:
        self._client = client

    # -------------------------------------------------------------------- get

    def get(
        self,
        table: str,
        record_id: Optional[str] = None,
        select: Optional[List[str]] = None,
        filter: Optional[str] = None,
        orderby: Optional[List[str]] = None,
        top: Optional[int] = None,
        expand: Optional[List[str]] = None,
        page_size: Optional[int] = None,
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
        :type select: :class:`list` of :class:`str` or None
        :param filter: Optional OData filter string. Column names must use exact lowercase logical names.
        :type filter: :class:`str` or None
        :param orderby: Optional list of attributes to sort by.
        :type orderby: :class:`list` of :class:`str` or None
        :param top: Optional maximum number of records to return.
        :type top: :class:`int` or None
        :param expand: Optional list of navigation properties to expand (case-sensitive).
        :type expand: :class:`list` of :class:`str` or None
        :param page_size: Optional number of records per page for pagination.
        :type page_size: :class:`int` or None

        :return: DataFrame containing all matching records. Returns an empty DataFrame
            when no records match.
        :rtype: ~pandas.DataFrame

        .. tip::
            For large tables, use ``top`` or ``filter`` to limit the result set.

        Example:
            Fetch a single record as a DataFrame::

                df = client.dataframe.get("account", record_id=account_id, select=["name", "telephone1"])
                print(df)

            Query with filtering::

                df = client.dataframe.get("account", filter="statecode eq 0", select=["name"])
                print(f"Got {len(df)} active accounts")

            Limit result size::

                df = client.dataframe.get("account", select=["name"], top=100)
        """
        if record_id is not None:
            result = self._client.records.get(
                table,
                record_id,
                select=select,
            )
            return pd.DataFrame([result.data])

        frames: List[pd.DataFrame] = []
        for batch in self._client.records.get(
            table,
            select=select,
            filter=filter,
            orderby=orderby,
            top=top,
            expand=expand,
            page_size=page_size,
        ):
            frames.append(pd.DataFrame([row.data for row in batch]))

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    # ----------------------------------------------------------------- create

    def create(
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
                df["accountid"] = client.dataframe.create("account", df)
        """
        if not isinstance(records, pd.DataFrame):
            raise TypeError("records must be a pandas DataFrame")

        if records.empty:
            raise ValueError("records must be a non-empty DataFrame")

        record_list = dataframe_to_records(records)
        ids = self._client.records.create(table, record_list)

        if len(ids) != len(records):
            raise ValueError(f"Server returned {len(ids)} IDs for {len(records)} input rows")

        return pd.Series(ids, index=records.index)

    # ----------------------------------------------------------------- update

    def update(
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
            as ``null`` to Dataverse, clearing the field. Use ``True`` only when you intentionally
            want NaN/None values to clear fields.
        :type clear_nulls: :class:`bool`

        :raises TypeError: If ``changes`` is not a pandas DataFrame.
        :raises ValueError: If ``id_column`` is not found in the DataFrame.

        Example:
            Update records with different values per row::

                import pandas as pd

                df = pd.DataFrame([
                    {"accountid": "guid-1", "telephone1": "555-0100"},
                    {"accountid": "guid-2", "telephone1": "555-0200"},
                ])
                client.dataframe.update("account", df, id_column="accountid")

            Broadcast the same change to all records::

                df = pd.DataFrame({"accountid": ["guid-1", "guid-2", "guid-3"]})
                df["websiteurl"] = "https://example.com"
                client.dataframe.update("account", df, id_column="accountid")

            Clear a field by setting clear_nulls=True::

                df = pd.DataFrame([{"accountid": "guid-1", "websiteurl": None}])
                client.dataframe.update("account", df, id_column="accountid", clear_nulls=True)
        """
        if not isinstance(changes, pd.DataFrame):
            raise TypeError("changes must be a pandas DataFrame")
        if id_column not in changes.columns:
            raise ValueError(f"id_column '{id_column}' not found in DataFrame columns")

        ids = changes[id_column].tolist()
        invalid = [i for i, v in enumerate(ids) if not isinstance(v, str) or not v.strip()]
        if invalid:
            raise ValueError(
                f"id_column '{id_column}' contains invalid values at row index(es) {invalid}. "
                "All IDs must be non-empty strings."
            )

        change_columns = [column for column in changes.columns if column != id_column]
        if not change_columns:
            raise ValueError(
                "No columns to update. The DataFrame must contain at least one column " "besides the id_column."
            )
        change_list = dataframe_to_records(changes[change_columns], na_as_null=clear_nulls)

        if len(ids) == 1:
            self._client.records.update(table, ids[0], change_list[0])
        else:
            self._client.records.update(table, ids, change_list)

    # ----------------------------------------------------------------- delete

    def delete(
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
        :param use_bulk_delete: When ``True`` (default) and ``ids`` contains multiple values, execute the BulkDelete
            action and return its async job identifier. When ``False`` each record is deleted sequentially.
        :type use_bulk_delete: :class:`bool`

        :raises TypeError: If ``ids`` is not a pandas Series.

        :return: BulkDelete job ID when deleting multiple records via BulkDelete; otherwise ``None``.
        :rtype: :class:`str` or None

        Example:
            Delete records using a Series::

                import pandas as pd

                ids = pd.Series(["guid-1", "guid-2", "guid-3"])
                client.dataframe.delete("account", ids)
        """
        if not isinstance(ids, pd.Series):
            raise TypeError("ids must be a pandas Series")

        id_list = ids.tolist()
        if not id_list:
            return None

        invalid = [i for i, v in enumerate(id_list) if not isinstance(v, str) or not v.strip()]
        if invalid:
            raise ValueError(
                f"ids Series contains invalid values at index(es) {invalid}. " "All IDs must be non-empty strings."
            )

        if len(id_list) == 1:
            return self._client.records.delete(table, id_list[0])
        else:
            return self._client.records.delete(table, id_list, use_bulk_delete=use_bulk_delete)
