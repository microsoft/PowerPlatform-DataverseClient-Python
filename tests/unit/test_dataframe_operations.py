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


class TestDataFrameSql(unittest.TestCase):
    """Tests for client.dataframe.sql()."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()

    def test_sql_returns_dataframe(self):
        """sql() should return a DataFrame from SQL results."""
        raw_rows = [
            {"accountid": "1", "name": "Contoso"},
            {"accountid": "2", "name": "Fabrikam"},
        ]
        self.client._odata._query_sql.return_value = raw_rows
        df = self.client.dataframe.sql("SELECT accountid, name FROM account")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertEqual(df.iloc[0]["name"], "Contoso")
        self.assertEqual(df.iloc[1]["name"], "Fabrikam")

    def test_sql_empty_result(self):
        """sql() should return an empty DataFrame when no rows match."""
        self.client._odata._query_sql.return_value = []
        df = self.client.dataframe.sql("SELECT name FROM account WHERE name = 'None'")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 0)

    def test_sql_aggregate(self):
        """sql() should handle aggregate results as DataFrame."""
        self.client._odata._query_sql.return_value = [{"cnt": 42}]
        df = self.client.dataframe.sql("SELECT COUNT(*) as cnt FROM account")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["cnt"], 42)

    def test_sql_join(self):
        """sql() should handle JOIN results as DataFrame."""
        raw = [
            {"name": "Contoso", "fullname": "John Doe"},
            {"name": "Fabrikam", "fullname": "Jane Smith"},
        ]
        self.client._odata._query_sql.return_value = raw
        df = self.client.dataframe.sql(
            "SELECT a.name, c.fullname FROM account a " "JOIN contact c ON a.accountid = c.parentcustomerid"
        )
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertIn("name", df.columns)
        self.assertIn("fullname", df.columns)

    def test_sql_group_by(self):
        """sql() should handle GROUP BY results as DataFrame."""
        raw = [
            {"new_region": 1, "cnt": 3, "total": 167000},
            {"new_region": 2, "cnt": 1, "total": 75000},
        ]
        self.client._odata._query_sql.return_value = raw
        df = self.client.dataframe.sql(
            "SELECT new_region, COUNT(*) as cnt, SUM(new_budget) as total " "FROM new_sqldemoteam GROUP BY new_region"
        )
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertIn("new_region", df.columns)
        self.assertIn("cnt", df.columns)
        self.assertIn("total", df.columns)

    def test_sql_distinct(self):
        """sql() should handle DISTINCT results as DataFrame."""
        raw = [{"name": "Contoso"}, {"name": "Fabrikam"}]
        self.client._odata._query_sql.return_value = raw
        df = self.client.dataframe.sql("SELECT DISTINCT name FROM account")
        self.assertEqual(len(df), 2)

    def test_sql_select_star(self):
        """sql() should handle SELECT * (auto-expanded) as DataFrame."""
        raw = [{"accountid": "1", "name": "Contoso", "revenue": 1000}]
        self.client._odata._query_sql.return_value = raw
        df = self.client.dataframe.sql("SELECT * FROM account")
        self.assertEqual(len(df), 1)
        self.assertIn("accountid", df.columns)
        self.assertIn("name", df.columns)

    def test_sql_polymorphic_owner_join(self):
        """sql() should handle polymorphic lookup JOIN to DataFrame."""
        raw = [
            {"name": "Contoso", "owner_name": "Admin"},
            {"name": "Fabrikam", "owner_name": "Manager"},
        ]
        self.client._odata._query_sql.return_value = raw
        df = self.client.dataframe.sql(
            "SELECT a.name, su.fullname as owner_name "
            "FROM account a "
            "JOIN systemuser su ON a._ownerid_value = su.systemuserid"
        )
        self.assertEqual(len(df), 2)
        self.assertIn("owner_name", df.columns)

    def test_sql_multi_aggregate(self):
        """sql() should handle all 5 aggregate functions."""
        raw = [{"cnt": 10, "total": 500, "avg_v": 50.0, "min_v": 10, "max_v": 100}]
        self.client._odata._query_sql.return_value = raw
        df = self.client.dataframe.sql(
            "SELECT COUNT(*) as cnt, SUM(revenue) as total, "
            "AVG(revenue) as avg_v, MIN(revenue) as min_v, MAX(revenue) as max_v "
            "FROM account"
        )
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["cnt"], 10)
        self.assertEqual(df.iloc[0]["max_v"], 100)

    def test_sql_offset_fetch(self):
        """sql() should handle OFFSET FETCH pagination results."""
        raw = [{"name": "Row1"}, {"name": "Row2"}]
        self.client._odata._query_sql.return_value = raw
        df = self.client.dataframe.sql("SELECT name FROM account ORDER BY name OFFSET 10 ROWS FETCH NEXT 2 ROWS ONLY")
        self.assertEqual(len(df), 2)

    def test_sql_join_with_group_by(self):
        """sql() should handle JOIN + GROUP BY + aggregates."""
        raw = [
            {"name": "Contoso", "contact_count": 5},
            {"name": "Fabrikam", "contact_count": 3},
        ]
        self.client._odata._query_sql.return_value = raw
        df = self.client.dataframe.sql(
            "SELECT a.name, COUNT(c.contactid) as contact_count "
            "FROM account a "
            "JOIN contact c ON a.accountid = c.parentcustomerid "
            "GROUP BY a.name"
        )
        self.assertEqual(len(df), 2)
        self.assertIn("contact_count", df.columns)

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

    def test_get_no_results_with_select_preserves_columns(self):
        """Empty result with select returns DataFrame with expected columns."""
        self.client._odata._get_multiple.return_value = iter([])
        df = self.client.dataframe.get("account", select=["name", "telephone1"])
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 0)
        self.assertListEqual(list(df.columns), ["name", "telephone1"])

    def test_create_all_nan_rows_raises(self):
        """DataFrame where all values are NaN raises ValueError."""
        df = pd.DataFrame([{"name": None, "phone": None}])
        with self.assertRaises(ValueError) as ctx:
            self.client.dataframe.create("account", df)
        self.assertIn("no non-null values", str(ctx.exception))

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
            count=False,
            include_annotations=None,
        )

    def test_get_record_id_with_query_params_raises(self):
        """ValueError raised when record_id is provided with query params."""
        with self.assertRaises(ValueError) as ctx:
            self.client.dataframe.get("account", record_id="guid-1", filter="name eq 'X'")
        self.assertIn("Cannot specify query parameters", str(ctx.exception))

    def test_get_record_id_with_top_raises(self):
        """ValueError raised when record_id is provided with top."""
        with self.assertRaises(ValueError) as ctx:
            self.client.dataframe.get("account", record_id="guid-1", top=10)
        self.assertIn("Cannot specify query parameters", str(ctx.exception))

    def test_get_empty_record_id_raises(self):
        """ValueError raised when record_id is empty or whitespace."""
        with self.assertRaises(ValueError) as ctx:
            self.client.dataframe.get("account", record_id="  ")
        self.assertIn("non-empty string", str(ctx.exception))

    def test_get_record_id_stripped(self):
        """Leading/trailing whitespace in record_id is stripped."""
        self.client._odata._get.return_value = {"accountid": "guid-1", "name": "Contoso"}
        self.client.dataframe.get("account", record_id="  guid-1  ")
        self.client._odata._get.assert_called_once_with("account", "guid-1", select=None)


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

    def test_update_empty_dataframe_raises(self):
        """Empty DataFrame raises ValueError without calling the API."""
        df = pd.DataFrame(columns=["accountid", "name"])
        with self.assertRaises(ValueError) as ctx:
            self.client.dataframe.update("account", df, id_column="accountid")
        self.assertIn("non-empty", str(ctx.exception))
        self.client._odata._update.assert_not_called()

    def test_update_clear_nulls_false(self):
        """NaN values are omitted from the update payload when clear_nulls=False."""
        df = pd.DataFrame([{"accountid": "guid-1", "name": "New Name", "telephone1": None}])
        self.client.dataframe.update("account", df, id_column="accountid")
        call_args = self.client._odata._update.call_args[0]
        changes = call_args[2]
        self.assertIn("name", changes)
        self.assertNotIn("telephone1", changes)

    def test_update_all_nan_rows_skipped(self):
        """When all change values are NaN for every row, no API call is made."""
        df = pd.DataFrame(
            [
                {"accountid": "guid-1", "telephone1": None, "websiteurl": None},
                {"accountid": "guid-2", "telephone1": None, "websiteurl": None},
            ]
        )
        self.client.dataframe.update("account", df, id_column="accountid")
        self.client._odata._update.assert_not_called()
        self.client._odata._update_by_ids.assert_not_called()

    def test_update_partial_nan_rows_filtered(self):
        """Rows where all changes are NaN are filtered; remaining rows proceed."""
        df = pd.DataFrame(
            [
                {"accountid": "guid-1", "name": "Updated", "telephone1": None},
                {"accountid": "guid-2", "name": None, "telephone1": None},
            ]
        )
        self.client.dataframe.update("account", df, id_column="accountid")
        self.client._odata._update.assert_called_once_with("account", "guid-1", {"name": "Updated"})

    def test_update_invalid_ids_reports_index_labels(self):
        """Error message reports DataFrame index labels, not positional indices."""
        df = pd.DataFrame(
            [
                {"accountid": "guid-1", "name": "A"},
                {"accountid": None, "name": "B"},
            ],
            index=["row_a", "row_b"],
        )
        with self.assertRaises(ValueError) as ctx:
            self.client.dataframe.update("account", df, id_column="accountid")
        self.assertIn("row_b", str(ctx.exception))

    def test_update_strips_whitespace_from_ids(self):
        """Leading/trailing whitespace in IDs is stripped before API call."""
        df = pd.DataFrame([{"accountid": "  guid-1  ", "name": "Contoso"}])
        self.client.dataframe.update("account", df, id_column="accountid")
        call_args = self.client._odata._update.call_args[0]
        self.assertEqual(call_args[1], "guid-1")

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

    def test_delete_invalid_ids_reports_index_labels(self):
        """Error message reports Series index labels, not positional indices."""
        ids = pd.Series(["guid-1", None], index=["row_x", "row_y"])
        with self.assertRaises(ValueError) as ctx:
            self.client.dataframe.delete("account", ids)
        self.assertIn("row_y", str(ctx.exception))

    def test_delete_strips_whitespace_from_ids(self):
        """Leading/trailing whitespace in IDs is stripped before API call."""
        ids = pd.Series(["  guid-1  "])
        self.client.dataframe.delete("account", ids)
        self.client._odata._delete.assert_called_once_with("account", "guid-1")


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

    def test_get_with_expand_includes_nested_data(self):
        """get() with expand returns DataFrame including expanded navigation property data."""
        page = [
            {
                "accountid": "guid-1",
                "name": "Contoso",
                "primarycontactid": {"contactid": "c-1", "fullname": "John"},
            }
        ]
        self.client._odata._get_multiple.return_value = iter([page])
        df = self.client.dataframe.get("account", expand=["primarycontactid"])
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["name"], "Contoso")
        self.assertIsInstance(df.iloc[0]["primarycontactid"], dict)
        self.assertEqual(df.iloc[0]["primarycontactid"]["fullname"], "John")

    def test_get_single_record_no_odata_keys(self):
        """Single-record get strips @odata.* keys from the returned DataFrame."""
        self.client._odata._get.return_value = {
            "@odata.context": "https://example.crm.dynamics.com/$metadata#accounts/$entity",
            "@odata.etag": 'W/"123"',
            "accountid": "guid-1",
            "name": "Contoso",
        }
        df = self.client.dataframe.get("account", record_id="guid-1")
        self.assertNotIn("@odata.context", df.columns)
        self.assertNotIn("@odata.etag", df.columns)
        self.assertIn("name", df.columns)
        self.assertEqual(df.iloc[0]["name"], "Contoso")

    def test_delete_whitespace_only_ids_rejected(self):
        """Series containing whitespace-only strings raises ValueError."""
        ids = pd.Series(["guid-1", "   ", "guid-3"])
        with self.assertRaises(ValueError) as ctx:
            self.client.dataframe.delete("account", ids)
        self.assertIn("invalid values", str(ctx.exception))
        self.assertIn("[1]", str(ctx.exception))

    def test_update_with_timezone_aware_timestamps(self):
        """Update correctly normalizes timezone-aware Timestamps."""
        ts = pd.Timestamp("2024-06-15 10:30:00", tz="UTC")
        df = pd.DataFrame([{"accountid": "guid-1", "lastonholdtime": ts}])
        self.client.dataframe.update("account", df, id_column="accountid")
        call_args = self.client._odata._update.call_args[0]
        changes = call_args[2]
        self.assertIsInstance(changes["lastonholdtime"], str)
        self.assertIn("2024-06-15T10:30:00", changes["lastonholdtime"])


if __name__ == "__main__":
    unittest.main()
