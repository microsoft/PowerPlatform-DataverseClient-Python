# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Internal pandas helpers"""

from __future__ import annotations

from typing import Any, Dict, List

import numpy as np
import pandas as pd


def _normalize_scalar(v: Any) -> Any:
    """Convert numpy scalar types to their Python native equivalents.

    :param v: A scalar value to normalize.
    :return: The value converted to a JSON-serializable Python type.
    """
    if isinstance(v, pd.Timestamp):
        return v.isoformat()
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        return float(v)
    if isinstance(v, np.bool_):
        return bool(v)
    return v


def dataframe_to_records(df: pd.DataFrame, na_as_null: bool = False) -> List[Dict[str, Any]]:
    """Convert a DataFrame to a list of dicts, normalizing values for JSON serialization.

    :param df: Input DataFrame.
    :param na_as_null: When False (default), missing values are omitted from each dict.
        When True, missing values are included as None (sends null to Dataverse, clearing the field).
    """
    records = []
    for row in df.to_dict(orient="records"):
        clean = {}
        for k, v in row.items():
            if pd.api.types.is_scalar(v):
                if pd.notna(v):
                    clean[k] = _normalize_scalar(v)
                elif na_as_null:
                    clean[k] = None
            else:
                clean[k] = v  # pass through lists, dicts, arrays, etc.
        records.append(clean)
    return records
