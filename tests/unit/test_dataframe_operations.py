# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Comprehensive unit tests for the DataFrameOperations namespace (client.dataframe)."""

import unittest
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.operations.dataframe import DataFrameOperations
from PowerPlatform.Dataverse.utils._pandas import dataframe_to_records


class TestDataframeToRecordsHelper(unittest.TestCase):
    """Unit tests for the dataframe_to_records() helper in isolation."""

    def test_dataframe_to_records_basic(self):
        """Basic DataFrame with string values is converted correctly."""
        df = pd.DataFrame([{"name": "Contoso", "city": "Seattle"}])
        result = dataframe_to_records(df)
        self.assertEqual(result, [{"name": "Contoso", "city": "Seattle"}])

    def test_dataframe_to_records_nan_dropped(self):
        """NaN values are omitted from records when na_as_null=False (default)."""
        df = pd.DataFrame([{"name": "Contoso", "telephone1": None}])
        result = dataframe_to_records(df)
        self.assertNotIn("telephone1", result[0])

    def test_dataframe_to_records_nan_as_null(self):
        """NaN values become None when na_as_null=True."""
        df = pd.DataFrame([{"name": "Contoso", "telephone1": None}])
        result = dataframe_to_records(df, na_as_null=True)
        self.assertIn("telephone1", result[0])
        self.assertIsNone(result[0]["telephone1"])

    def test_dataframe_to_records_timestamp_conversion(self):
        """pd.Timestamp values are converted to ISO 8601 strings."""
        ts = pd.Timestamp("2024-01-15 10:30:00")
        df = pd.DataFrame([{"createdon": ts}])
        result = dataframe_to_records(df)
        self.assertEqual(result[0]["createdon"], "2024-01-15T10:30:00")

    def test_dataframe_to_records_numpy_int(self):
        """np.int64 values are converted to Python int."""
        df = pd.DataFrame([{"priority": np.int64(42)}])
        result = dataframe_to_records(df)
        self.assertIsInstance(result[0]["priority"], int)
        self.assertEqual(result[0]["priority"], 42)

    def test_dataframe_to_records_numpy_float(self):
        """np.float64 values are converted to Python float."""
        df = pd.DataFrame([{"score": np.float64(3.14)}])
        result = dataframe_to_records(df)
        self.assertIsInstance(result[0]["score"], float)
        self.assertAlmostEqual(result[0]["score"], 3.14)

    def test_dataframe_to_records_numpy_bool(self):
        """np.bool_ values are converted to Python bool."""
        df = pd.DataFrame([{"active": np.bool_(True)}])
        result = dataframe_to_records(df)
        self.assertIsInstance(result[0]["active"], bool)
        self.assertTrue(result[0]["active"])

    def test_dataframe_to_records_list_value(self):
        """Cells containing lists pass through without crashing."""
        df = pd.DataFrame([{"tags": ["a", "b", "c"]}])
        result = dataframe_to_records(df)
        self.assertEqual(result[0]["tags"], ["a", "b", "c"])

    def test_dataframe_to_records_dict_value(self):
        """Cells containing dicts pass through without crashing."""
        df = pd.DataFrame([{"metadata": {"key": "value"}}])
        result = dataframe_to_records(df)
        self.assertEqual(result[0]["metadata"], {"key": "value"})

    def test_dataframe_to_records_empty_dataframe(self):
        """Empty DataFrame returns an empty list."""
        df = pd.DataFrame(columns=["name", "telephone1"])
        result = dataframe_to_records(df)
        self.assertEqual(result, [])

    def test_dataframe_to_records_mixed_types(self):
        """DataFrame with mixed types converts all values correctly."""
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
        rec = result[0]
        self.assertEqual(rec["name"], "Contoso")
        self.assertIsInstance(rec["count"], int)
        self.assertIsInstance(rec["score"], float)
        self.assertIsInstance(rec["active"], bool)
        self.assertEqual(rec["createdon"], "2024-06-01T00:00:00")
        self.assertNotIn("notes", rec)


class TestDataFrameOperationsNamespace(unittest.TestCase):
    """Tests for the DataFrameOperations namespace itself."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()

    def test_namespace_exists(self):
        """client.dataframe is a DataFrameOperations instance."""
        self.assertIsInstance(self.client.dataframe, DataFrameOperations)


class TestDataFrameGet(unittest.TestCase):
    """Tests for client.dataframe.get()."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()

    def test_get_single_record(self):
        """record_id returns a one-row DataFrame using result.data."""
        self.client._odata._get.return_value = {"accountid": "guid-1", "name": "Contoso"}
        df = self.client.dataframe.get("account", record_id="guid-1")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["name"], "Contoso")

    def test_get_multiple_records(self):
        """Without record_id, pages are iterated and consolidated into one DataFrame."""
        page1 = [{"accountid": "guid-1", "name": "A"}]
        page2 = [{"accountid": "guid-2", "name": "B"}]
        self.client._odata._get_multiple.return_value = iter([page1, page2])
        df = self.client.dataframe.get("account")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)

    def test_get_no_results(self):
        """Empty result set returns an empty DataFrame."""
        self.client._odata._get_multiple.return_value = iter([])
        df = self.client.dataframe.get("account")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 0)

    def test_get_passes_all_params(self):
        """All OData parameters are forwarded to the underlying API call."""
        self.client._odata._get_multiple.return_value = iter([])
        self.client.dataframe.get(
            "account",
            select=["name"],
            filter="statecode eq 0",
            orderby=["name asc"],
            top=50,
            expand=["primarycontactid"],
            page_size=25,
        )
        self.client._odata._get_multiple.assert_called_once_with(
            "account",
            select=["name"],
            filter="statecode eq 0",
            orderby=["name asc"],
            top=50,
            expand=["primarycontactid"],
            page_size=25,
        )


class TestDataFrameCreate(unittest.TestCase):
    """Tests for client.dataframe.create()."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

    def test_create_returns_series(self):
        """Returns a Series of GUIDs aligned with the input DataFrame index."""
        df = pd.DataFrame([{"name": "Contoso"}, {"name": "Fabrikam"}])
        self.client._odata._create_multiple.return_value = ["guid-1", "guid-2"]
        ids = self.client.dataframe.create("account", df)
        self.assertIsInstance(ids, pd.Series)
        self.assertListEqual(ids.tolist(), ["guid-1", "guid-2"])

    def test_create_type_error(self):
        """Non-DataFrame input raises TypeError."""
        with self.assertRaises(TypeError) as ctx:
            self.client.dataframe.create("account", [{"name": "Contoso"}])
        self.assertIn("pandas DataFrame", str(ctx.exception))

    def test_create_empty_dataframe_raises(self):
        """Empty DataFrame raises ValueError without calling the API."""
        df = pd.DataFrame(columns=["name"])
        with self.assertRaises(ValueError) as ctx:
            self.client.dataframe.create("account", df)
        self.assertIn("non-empty", str(ctx.exception))
        self.client._odata._create_multiple.assert_not_called()

    def test_create_id_count_mismatch_raises(self):
        """ValueError raised when returned IDs count doesn't match input row count."""
        df = pd.DataFrame([{"name": "Contoso"}, {"name": "Fabrikam"}])
        self.client._odata._create_multiple.return_value = ["guid-1"]
        with self.assertRaises(ValueError) as ctx:
            self.client.dataframe.create("account", df)
        self.assertIn("1 IDs for 2 input rows", str(ctx.exception))

    def test_create_normalizes_values(self):
        """NumPy types and Timestamps are normalized before sending to the API."""
        ts = pd.Timestamp("2024-01-15 10:30:00")
        df = pd.DataFrame([{"count": np.int64(5), "score": np.float64(9.8), "createdon": ts}])
        self.client._odata._create_multiple.return_value = ["guid-1"]
        self.client.dataframe.create("account", df)
        records_arg = self.client._odata._create_multiple.call_args[0][2]
        rec = records_arg[0]
        self.assertIsInstance(rec["count"], int)
        self.assertIsInstance(rec["score"], float)
        self.assertEqual(rec["createdon"], "2024-01-15T10:30:00")


class TestDataFrameUpdate(unittest.TestCase):
    """Tests for client.dataframe.update()."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()

    def test_update_single_record(self):
        """Single-row DataFrame calls single-record update path."""
        df = pd.DataFrame([{"accountid": "guid-1", "name": "New Name"}])
        self.client.dataframe.update("account", df, id_column="accountid")
        self.client._odata._update.assert_called_once_with("account", "guid-1", {"name": "New Name"})

    def test_update_multiple_records(self):
        """Multi-row DataFrame calls batch update path."""
        df = pd.DataFrame(
            [
                {"accountid": "guid-1", "telephone1": "555-0100"},
                {"accountid": "guid-2", "telephone1": "555-0200"},
            ]
        )
        self.client.dataframe.update("account", df, id_column="accountid")
        self.client._odata._update_by_ids.assert_called_once_with(
            "account",
            ["guid-1", "guid-2"],
            [{"telephone1": "555-0100"}, {"telephone1": "555-0200"}],
        )

    def test_update_type_error(self):
        """Non-DataFrame input raises TypeError."""
        with self.assertRaises(TypeError) as ctx:
            self.client.dataframe.update("account", {"id": "guid-1"}, id_column="id")
        self.assertIn("pandas DataFrame", str(ctx.exception))

    def test_update_missing_id_column(self):
        """ValueError raised when id_column is not in DataFrame columns."""
        df = pd.DataFrame([{"name": "Contoso"}])
        with self.assertRaises(ValueError) as ctx:
            self.client.dataframe.update("account", df, id_column="accountid")
        self.assertIn("accountid", str(ctx.exception))

    def test_update_invalid_id_values(self):
        """ValueError raised when id_column contains NaN or non-string values."""
        df = pd.DataFrame(
            [
                {"accountid": "guid-1", "name": "A"},
                {"accountid": None, "name": "B"},
            ]
        )
        with self.assertRaises(ValueError) as ctx:
            self.client.dataframe.update("account", df, id_column="accountid")
        self.assertIn("invalid values", str(ctx.exception))
        self.assertIn("[1]", str(ctx.exception))

    def test_update_empty_change_columns(self):
        """ValueError raised when DataFrame contains only the id_column."""
        df = pd.DataFrame([{"accountid": "guid-1"}])
        with self.assertRaises(ValueError) as ctx:
            self.client.dataframe.update("account", df, id_column="accountid")
        self.assertIn("No columns to update", str(ctx.exception))

    def test_update_clear_nulls_false(self):
        """NaN values are omitted from the update payload when clear_nulls=False."""
        df = pd.DataFrame([{"accountid": "guid-1", "name": "New Name", "telephone1": None}])
        self.client.dataframe.update("account", df, id_column="accountid")
        call_args = self.client._odata._update.call_args[0]
        changes = call_args[2]
        self.assertIn("name", changes)
        self.assertNotIn("telephone1", changes)

    def test_update_clear_nulls_true(self):
        """NaN values are sent as None in the update payload when clear_nulls=True."""
        df = pd.DataFrame([{"accountid": "guid-1", "name": "New Name", "telephone1": None}])
        self.client.dataframe.update("account", df, id_column="accountid", clear_nulls=True)
        call_args = self.client._odata._update.call_args[0]
        changes = call_args[2]
        self.assertIn("name", changes)
        self.assertIn("telephone1", changes)
        self.assertIsNone(changes["telephone1"])


class TestDataFrameDelete(unittest.TestCase):
    """Tests for client.dataframe.delete()."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()

    def test_delete_single_record(self):
        """Single-element Series calls single-record delete."""
        ids = pd.Series(["guid-1"])
        self.client.dataframe.delete("account", ids)
        self.client._odata._delete.assert_called_once_with("account", "guid-1")

    def test_delete_multiple_records(self):
        """Multi-element Series calls bulk delete."""
        ids = pd.Series(["guid-1", "guid-2", "guid-3"])
        self.client._odata._delete_multiple.return_value = "job-123"
        job_id = self.client.dataframe.delete("account", ids)
        self.assertEqual(job_id, "job-123")
        self.client._odata._delete_multiple.assert_called_once_with("account", ["guid-1", "guid-2", "guid-3"])

    def test_delete_type_error(self):
        """Non-Series input raises TypeError."""
        with self.assertRaises(TypeError) as ctx:
            self.client.dataframe.delete("account", ["guid-1"])
        self.assertIn("pandas Series", str(ctx.exception))

    def test_delete_empty_series(self):
        """Empty Series returns None without calling delete."""
        ids = pd.Series([], dtype="str")
        result = self.client.dataframe.delete("account", ids)
        self.assertIsNone(result)
        self.client._odata._delete.assert_not_called()
        self.client._odata._delete_multiple.assert_not_called()

    def test_delete_invalid_ids(self):
        """ValueError raised when Series contains NaN or non-string values."""
        ids = pd.Series(["guid-1", None, "  "])
        with self.assertRaises(ValueError) as ctx:
            self.client.dataframe.delete("account", ids)
        self.assertIn("invalid values", str(ctx.exception))

    def test_delete_with_bulk_delete_false(self):
        """use_bulk_delete=False passes through to the underlying delete call."""
        ids = pd.Series(["guid-1", "guid-2"])
        result = self.client.dataframe.delete("account", ids, use_bulk_delete=False)
        self.assertIsNone(result)
        self.assertEqual(self.client._odata._delete.call_count, 2)


class TestDataFrameEndToEnd(unittest.TestCase):
    """End-to-end mocked flow: create -> get -> update -> delete."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

    def test_create_get_update_delete_flow(self):
        """Full CRUD cycle works end-to-end through the dataframe namespace."""
        # Step 1: create
        df = pd.DataFrame(
            [{"name": "Contoso", "telephone1": "555-0100"}, {"name": "Fabrikam", "telephone1": "555-0200"}]
        )
        self.client._odata._create_multiple.return_value = ["guid-1", "guid-2"]

        ids = self.client.dataframe.create("account", df)

        self.assertIsInstance(ids, pd.Series)
        self.assertListEqual(ids.tolist(), ["guid-1", "guid-2"])

        # Step 2: get
        df["accountid"] = ids
        self.client._odata._get_multiple.return_value = iter(
            [[{"accountid": "guid-1", "name": "Contoso"}, {"accountid": "guid-2", "name": "Fabrikam"}]]
        )

        result_df = self.client.dataframe.get("account", select=["accountid", "name"])

        self.assertIsInstance(result_df, pd.DataFrame)
        self.assertEqual(len(result_df), 2)

        # Step 3: update
        df["telephone1"] = ["555-9999", "555-8888"]

        self.client.dataframe.update("account", df, id_column="accountid")

        self.client._odata._update_by_ids.assert_called_once()

        # Step 4: delete
        self.client._odata._delete_multiple.return_value = "job-abc"

        job_id = self.client.dataframe.delete("account", df["accountid"])

        self.assertEqual(job_id, "job-abc")
        self.client._odata._delete_multiple.assert_called_once_with("account", ["guid-1", "guid-2"])

    def test_create_normalizes_numpy_types_before_api(self):
        """NumPy types in DataFrame cells are normalized to Python types before the API call."""
        df = pd.DataFrame(
            [
                {
                    "count": np.int64(10),
                    "score": np.float64(9.5),
                    "active": np.bool_(True),
                    "createdon": pd.Timestamp("2024-06-01"),
                }
            ]
        )
        self.client._odata._create_multiple.return_value = ["guid-1"]

        self.client.dataframe.create("account", df)

        records_arg = self.client._odata._create_multiple.call_args[0][2]
        rec = records_arg[0]
        self.assertIsInstance(rec["count"], int)
        self.assertIsInstance(rec["score"], float)
        self.assertIsInstance(rec["active"], bool)
        self.assertIsInstance(rec["createdon"], str)
        self.assertEqual(rec["createdon"], "2024-06-01T00:00:00")


if __name__ == "__main__":
    unittest.main()
