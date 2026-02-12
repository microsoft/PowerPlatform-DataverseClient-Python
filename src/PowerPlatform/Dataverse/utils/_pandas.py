# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Internal pandas helpers"""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd


def dataframe_to_records(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """Convert a DataFrame to a list of dicts, converting missing values (e.g. NaN, None, NaT, pd.NA) to None and Timestamps to ISO strings."""
    records = []
    for row in df.to_dict(orient="records"):
        clean = {}
        for k, v in row.items():
            if pd.notna(v):
                clean[k] = v.isoformat() if isinstance(v, pd.Timestamp) else v
            else:
                clean[k] = None
        records.append(clean)
    return records
