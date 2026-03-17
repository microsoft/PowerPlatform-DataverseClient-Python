# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for the dataframe_to_records() helper in utils/_pandas.py."""

import unittest

import numpy as np
import pandas as pd

from PowerPlatform.Dataverse.utils._pandas import dataframe_to_records


class TestDataframeToRecords(unittest.TestCase):
    """Unit tests for dataframe_to_records()."""

    def test_basic(self):
        """Basic DataFrame with string values is converted correctly."""
        df = pd.DataFrame([{"name": "Contoso", "city": "Seattle"}])
        result = dataframe_to_records(df)
        self.assertEqual(result, [{"name": "Contoso", "city": "Seattle"}])

    def test_nan_dropped(self):
        """NaN values are omitted from records when na_as_null=False (default)."""
        df = pd.DataFrame([{"name": "Contoso", "telephone1": None}])
        result = dataframe_to_records(df)
        self.assertEqual(result, [{"name": "Contoso"}])
        self.assertNotIn("telephone1", result[0])

    def test_nan_as_null(self):
        """NaN values become None when na_as_null=True."""
        df = pd.DataFrame([{"name": "Contoso", "telephone1": None}])
        result = dataframe_to_records(df, na_as_null=True)
        self.assertEqual(result, [{"name": "Contoso", "telephone1": None}])
        self.assertIn("telephone1", result[0])
        self.assertIsNone(result[0]["telephone1"])

    def test_timestamp_conversion(self):
        """pd.Timestamp values are converted to ISO 8601 strings."""
        ts = pd.Timestamp("2024-01-15 10:30:00")
        df = pd.DataFrame([{"name": "Contoso", "createdon": ts}])
        result = dataframe_to_records(df)
        self.assertEqual(result[0]["createdon"], "2024-01-15T10:30:00")

    def test_numpy_int(self):
        """np.int64 values are converted to Python int."""
        df = pd.DataFrame([{"priority": np.int64(42)}])
        result = dataframe_to_records(df)
        self.assertIsInstance(result[0]["priority"], int)
        self.assertEqual(result[0]["priority"], 42)

    def test_numpy_float(self):
        """np.float64 values are converted to Python float."""
        df = pd.DataFrame([{"score": np.float64(3.14)}])
        result = dataframe_to_records(df)
        self.assertIsInstance(result[0]["score"], float)
        self.assertAlmostEqual(result[0]["score"], 3.14)

    def test_numpy_bool(self):
        """np.bool_ values are converted to Python bool."""
        df = pd.DataFrame([{"active": np.bool_(True)}])
        result = dataframe_to_records(df)
        self.assertIsInstance(result[0]["active"], bool)
        self.assertTrue(result[0]["active"])

    def test_list_value(self):
        """Cells containing lists pass through without raising ValueError."""
        df = pd.DataFrame([{"tags": ["a", "b", "c"]}])
        result = dataframe_to_records(df)
        self.assertEqual(result[0]["tags"], ["a", "b", "c"])

    def test_dict_value(self):
        """Cells containing dicts pass through without raising ValueError."""
        df = pd.DataFrame([{"metadata": {"key": "value"}}])
        result = dataframe_to_records(df)
        self.assertEqual(result[0]["metadata"], {"key": "value"})

    def test_empty_dataframe(self):
        """Empty DataFrame returns an empty list."""
        df = pd.DataFrame(columns=["name", "telephone1"])
        result = dataframe_to_records(df)
        self.assertEqual(result, [])

    def test_mixed_types(self):
        """DataFrame with mixed types (str, int, float, None, Timestamp) converts correctly."""
        ts = pd.Timestamp("2024-06-01")
        df = pd.DataFrame(
            [
                {
                    "name": "Contoso",
                    "count": np.int64(5),
                    "score": np.float64(9.8),
                    "active": np.bool_(True),
                    "createdon": ts,
                    "notes": None,
                }
            ]
        )
        result = dataframe_to_records(df)
        self.assertEqual(len(result), 1)
        rec = result[0]
        self.assertEqual(rec["name"], "Contoso")
        self.assertIsInstance(rec["count"], int)
        self.assertEqual(rec["count"], 5)
        self.assertIsInstance(rec["score"], float)
        self.assertAlmostEqual(rec["score"], 9.8)
        self.assertIsInstance(rec["active"], bool)
        self.assertTrue(rec["active"])
        self.assertEqual(rec["createdon"], "2024-06-01T00:00:00")
        self.assertNotIn("notes", rec)


if __name__ == "__main__":
    unittest.main()
