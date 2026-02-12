# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Internal pandas helpers"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd


def dataframe_to_records(df: pd.DataFrame, na_as_null: bool = False) -> List[Dict[str, Any]]:
    """Convert a DataFrame to a list of dicts, converting Timestamps to ISO strings.

    :param df: Input DataFrame.
    :param na_as_null: When False (default), missing values are omitted from each dict.
        When True, missing values are included as None (sends null to Dataverse, clearing the field).
    """
    records = []
    for row in df.to_dict(orient="records"):
        clean = {}
        for k, v in row.items():
            if pd.notna(v):
                clean[k] = v.isoformat() if isinstance(v, pd.Timestamp) else v
            elif na_as_null:
                clean[k] = None
        records.append(clean)
    return records


def strip_odata_keys(record: Dict[str, Any]) -> Dict[str, Any]:
    """Remove OData metadata keys (keys containing '@') from a record dict."""
    return {k: v for k, v in record.items() if "@" not in k}
