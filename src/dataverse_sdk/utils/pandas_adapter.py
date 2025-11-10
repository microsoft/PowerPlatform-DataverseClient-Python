# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Pandas-friendly wrappers for Dataverse OData operations.

This module provides :class:`PandasODataClient`, a high-level wrapper that enables
DataFrame-based CRUD and query operations.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Any
import re
import json

import pandas as pd

from ..data.odata import ODataClient


@dataclass
class RowError:
    """
    Container for row-level error information.

    :param index: Zero-based row index where the error occurred.
    :type index: int
    :param message: Error message describing the failure.
    :type message: str
    """
    index: int
    message: str


class PandasODataClient:
    """
    High-level pandas-friendly wrapper for Dataverse OData operations.

    :param odata_client: Initialized low-level OData client with authentication configured.
    :type odata_client: ~dataverse_sdk.odata.ODataClient
    """

    def __init__(self, odata_client: ODataClient) -> None:
        self._c = odata_client

    # ---------------------------- Create ---------------------------------
    def create_df(self, logical_name: str, record: pd.Series) -> str:
        """
        Create a single record from a pandas Series and return the GUID.

        :param logical_name: Logical (singular) entity name, e.g. ``"account"``.
        :type logical_name: str
        :param record: Series whose index labels are field logical names and values are field values.
        :type record: pandas.Series
        :return: The created record's GUID.
        :rtype: str
        :raises TypeError: If ``record`` is not a pandas Series.
        :raises RuntimeError: If the internal create operation returns an unexpected format.
        """
        if not isinstance(record, pd.Series):
            raise TypeError("record must be a pandas Series")
        payload = {k: v for k, v in record.items()}
        created_ids = self._c.create(logical_name, payload)
        if not isinstance(created_ids, list) or len(created_ids) != 1 or not isinstance(created_ids[0], str):
            raise RuntimeError("Unexpected create return shape (expected single-element list of GUID str)")
        return created_ids[0]

    # ---------------------------- Update ---------------------------------
    def update(self, logical_name: str, record_id: str, entity_data: pd.Series) -> None:
        """
        Update a single record with values from a pandas Series.

        :param logical_name: Logical (singular) entity name, e.g. ``"account"``.
        :type logical_name: str
        :param record_id: GUID of the record to update.
        :type record_id: str
        :param entity_data: Series whose index labels are field logical names. NaN values are ignored.
        :type entity_data: pandas.Series
        :raises TypeError: If ``entity_data`` is not a pandas Series.
        """
        if not isinstance(entity_data, pd.Series):
            raise TypeError("entity_data must be a pandas Series")
        payload = {k: v for k, v in entity_data.items()}
        if not payload:
            return  # nothing to send
        self._c.update(logical_name, record_id, payload)

    # ---------------------------- Delete ---------------------------------
    def delete_ids(self, logical_name: str, ids: Sequence[str] | pd.Series | pd.Index) -> pd.DataFrame:
        """
        Delete a collection of record IDs and return a summary DataFrame.

        :param logical_name: Logical (singular) entity name, e.g. ``"account"``.
        :type logical_name: str
        :param ids: Collection of GUIDs to delete. Can be a list, pandas Series, or pandas Index.
        :type ids: Sequence[str] or pandas.Series or pandas.Index
        :return: DataFrame with columns: ``id`` (str), ``success`` (bool), ``error`` (str or None).
        :rtype: pandas.DataFrame
        """
        if isinstance(ids, (pd.Series, pd.Index)):
            id_list = [str(x) for x in ids.tolist()]
        else:
            id_list = [str(x) for x in ids]
        results = []
        for rid in id_list:
            try:
                self._c.delete(logical_name, rid)
                results.append({"id": rid, "success": True, "error": None})
            except Exception as e:  # noqa: BLE001
                results.append({"id": rid, "success": False, "error": str(e)})
        return pd.DataFrame(results)

    # ------------------------------ Get ----------------------------------
    def get_ids(self, logical_name: str, ids: Sequence[str] | pd.Series | pd.Index, select: Optional[Iterable[str]] = None) -> pd.DataFrame:
        """
        Fetch multiple records by ID and return a DataFrame.

        :param logical_name: Logical (singular) entity name, e.g. ``"account"``.
        :type logical_name: str
        :param ids: Collection of GUIDs to fetch. Can be a list, pandas Series, or pandas Index.
        :type ids: Sequence[str] or pandas.Series or pandas.Index
        :param select: Optional iterable of field logical names to retrieve. If None, all fields are returned.
        :type select: Iterable[str] or None
        :return: DataFrame containing fetched records. Failed fetches will have an ``error`` column.
        :rtype: pandas.DataFrame
        """
        if isinstance(ids, (pd.Series, pd.Index)):
            id_list = [str(x) for x in ids.tolist()]
        else:
            id_list = [str(x) for x in ids]
        rows = []
        any_errors = False
        select_arg = None
        if select:
            # ensure iterable of strings -> list -> join
            select_list = [str(c) for c in select]
            if select_list:
                select_arg = ",".join(select_list)
        for rec_id in id_list:
            try:
                data = self._c.get(logical_name, rec_id, select=select_arg)
                rows.append(data)
            except Exception as e:  # noqa: BLE001
                any_errors = True
                rows.append({"id": rec_id, "error": str(e)})
        if not rows:
            return pd.DataFrame(columns=["id"])
        return pd.DataFrame(rows)

    # --------------------------- Query SQL -------------------------------
    def query_sql_df(self, sql: str) -> pd.DataFrame:
        """
        Execute a SQL query via the Dataverse Web API and return a DataFrame.

        :param sql: SQL SELECT statement following Dataverse Web API SQL syntax.
        :type sql: str
        :return: DataFrame containing query results. Returns an empty DataFrame if no rows match.
        :rtype: pandas.DataFrame
        :raises ValueError: If the API returns a malformed JSON response.
        """
        rows: Any = self._c.query_sql(sql)

        # If API returned a JSON string, parse it
        if isinstance(rows, str):
            try:
                rows = json.loads(rows)
            except json.JSONDecodeError as e:  # noqa: BLE001
                raise ValueError("query_sql returned a string that is not valid JSON") from e

        # If a dict wrapper came back, try common shapes
        if isinstance(rows, dict):
            # Shape: {"rows": [...], "columns": [...]} (some APIs)
            if "rows" in rows and "columns" in rows and isinstance(rows["rows"], list):
                return pd.DataFrame(rows["rows"], columns=rows.get("columns"))
            # Shape: {"value": [...]}
            if "value" in rows and isinstance(rows["value"], list):
                rows = rows["value"]
            else:
                # Treat single dict payload as one-row result
                rows = [rows]

        # Now rows should ideally be a list
        if not rows:
            return pd.DataFrame()

        if isinstance(rows, list):
            if len(rows) == 0:
                return pd.DataFrame()
            # All dicts -> normal tabular expansion
            if all(isinstance(r, dict) for r in rows):
                return pd.DataFrame(rows)
            # Mixed or scalar list -> single column DataFrame
            return pd.DataFrame({"value": rows})

        # Fallback: wrap anything else
        return pd.DataFrame({"value": [rows]})

__all__ = ["PandasODataClient"]
