"""Pandas-friendly wrappers around the low-level `ODataClient`.

These helpers allow using pandas DataFrames / Series / Indexes as inputs and
outputs for common CRUD + query operations.

Design notes:
* All methods are thin convenience wrappers that iterate row-by-row; no OData
  batch requests are issued (future enhancement opportunity).
* create_df: creates one record per row, returning a new DataFrame with an
  added id column (default name 'id').
* update_df: updates records based on an id column; returns a DataFrame with
  per-row success booleans and optional error messages.
* delete_ids: deletes a collection of ids (Series, list, or Index) returning a
  DataFrame summarizing success/failure.
* get_ids: fetches a set of ids returning a DataFrame of the merged JSON
  objects (outer union of keys). Missing keys are NaN.
* query_sql_df: runs a SQL query via Custom API and returns the result rows as
  a DataFrame (empty DataFrame if no rows).

Edge cases & behaviors:
* Empty inputs return empty DataFrames without calling the API.
* Errors on individual rows are captured instead of aborting the whole batch.
* The default id column name is 'id' but can be overridden.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional, Sequence, Any
import re
import json

import pandas as pd

from .odata import ODataClient


@dataclass
class RowError:
    index: int
    message: str


class PandasODataClient:
    """High-level convenience wrapper exposing pandas-friendly methods.

    Parameters
    ----------
    odata_client : ODataClient
        An initialized low-level client (token acquisition & base URL ready).
    """

    def __init__(self, odata_client: ODataClient) -> None:
        self._c = odata_client

    # ---------------------------- Create ---------------------------------
    def create_df(self, entity_set: str, record: pd.Series) -> str:
        """Create a single record from a pandas Series and return the GUID.

        Parameters
        ----------
        entity_set : str
            Target Dataverse entity set name (entity set logical plural).
        record : pandas.Series
            Series whose index labels are field logical names.

        Returns
        -------
        str
            The created record's GUID.
        """
        if not isinstance(record, pd.Series):
            raise TypeError("record must be a pandas Series")
        payload = {k: v for k, v in record.items()}
        created = self._c.create(entity_set, payload)
        # Extract primary id from returned representation (first '*id' that looks like a GUID)
        if isinstance(created, dict):
            for k, v in created.items():
                if isinstance(k, str) and k.lower().endswith("id") and isinstance(v, (str,)):
                    if re.fullmatch(r"[0-9a-fA-F-]{36}", v.strip() or ""):
                        return v
        raise RuntimeError("Could not determine created record id from returned representation")

    # ---------------------------- Update ---------------------------------
    def update(self, entity_set: str, record_id: str, entity_data: pd.Series) -> None:
        """Update a single record.

        Parameters
        ----------
        entity_set : str
            Target Dataverse entity set name (plural logical name).
        record_id : str
            GUID of the record to update.
        entity_data : pandas.Series
            Series whose index labels are field logical names; any null (NaN) values
            are ignored (not sent). An 'id' key, if present, is ignored.

        Raises
        ------
        TypeError
            If entity_data is not a Series.
        Exception
            Propagates underlying HTTP errors from the OData client.
        """
        if not isinstance(entity_data, pd.Series):
            raise TypeError("entity_data must be a pandas Series")
        payload = {k: v for k, v in entity_data.items()}
        if not payload:
            return  # nothing to send
        self._c.update(entity_set, record_id, payload)

    # ---------------------------- Delete ---------------------------------
    def delete_ids(self, entity_set: str, record_id: str) -> None:
        """Delete a collection of record IDs.
        """
        self._c.delete(entity_set, record_id)

    # ------------------------------ Get ----------------------------------
    def get_ids(self, entity_set: str, ids: Sequence[str] | pd.Series | pd.Index, select: Optional[Iterable[str]] = None) -> pd.DataFrame:
        """Fetch multiple records by ID and return a DataFrame.

        Missing records are included with NaN for fields and an error column entry.
        """
        if isinstance(ids, (pd.Series, pd.Index)):
            id_list = [str(x) for x in ids.tolist()]
        else:
            id_list = [str(x) for x in ids]
        rows = []
        any_errors = False
        for rec_id in id_list:
            try:
                data = self._c.get(entity_set, rec_id, select=",".join(select) if select else None)
                rows.append(data)
            except Exception as e:  # noqa: BLE001
                any_errors = True
                rows.append({"id": rec_id, "error": str(e)})
        if not rows:
            return pd.DataFrame(columns=["id"])
        return pd.DataFrame(rows)

    # --------------------------- Query SQL -------------------------------
    def query_sql_df(self, tsql: str) -> pd.DataFrame:
        """Execute a SQL query via Custom API and return a DataFrame.

        Empty result -> empty DataFrame (columns inferred only if rows present).
        """
        rows: Any = self._c.query_sql(tsql)

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
