# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock

import pandas as pd
from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient


class TestDataFrameGet(unittest.TestCase):
    """Tests for get_dataframe."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"
        self.client = DataverseClient(self.base_url, self.mock_credential)
        self.client._odata = MagicMock()

    def test_get_single_record(self):
        """Single record_id returns a one-row DataFrame."""
        expected = {"accountid": "guid-1", "name": "Contoso"}
        self.client._odata._get.return_value = expected

        df = self.client.get_dataframe("account", record_id="guid-1")

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        self.assertEqual(df.iloc[0]["name"], "Contoso")
        self.client._odata._get.assert_called_once_with("account", "guid-1", select=None)

    def test_get_single_record_with_select(self):
        """Single record with select columns."""
        expected = {"accountid": "guid-1", "name": "Contoso"}
        self.client._odata._get.return_value = expected

        df = self.client.get_dataframe("account", record_id="guid-1", select=["name"])

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
        self.client._odata._get.assert_called_once_with("account", "guid-1", select=["name"])

    def test_get_multiple_records_single_page(self):
        """Single page yields one DataFrame."""
        batch = [
            {"accountid": "guid-1", "name": "A"},
            {"accountid": "guid-2", "name": "B"},
        ]
        self.client._odata._get_multiple.return_value = iter([batch])

        pages = list(self.client.get_dataframe("account", filter="statecode eq 0"))

        self.assertEqual(len(pages), 1)
        self.assertIsInstance(pages[0], pd.DataFrame)
        self.assertEqual(len(pages[0]), 2)
        self.assertListEqual(pages[0]["name"].tolist(), ["A", "B"])

    def test_get_multiple_records_multi_page(self):
        """Multiple pages yield one DataFrame per page."""
        page1 = [{"accountid": "guid-1", "name": "A"}]
        page2 = [{"accountid": "guid-2", "name": "B"}]
        self.client._odata._get_multiple.return_value = iter([page1, page2])

        pages = list(self.client.get_dataframe("account", top=100))

        self.assertEqual(len(pages), 2)
        self.assertIsInstance(pages[0], pd.DataFrame)
        self.assertIsInstance(pages[1], pd.DataFrame)
        self.assertEqual(pages[0].iloc[0]["name"], "A")
        self.assertEqual(pages[1].iloc[0]["name"], "B")

    def test_get_concat_all_pages(self):
        """pd.concat collects all pages into one DataFrame."""
        page1 = [{"accountid": "guid-1", "name": "A"}]
        page2 = [{"accountid": "guid-2", "name": "B"}]
        self.client._odata._get_multiple.return_value = iter([page1, page2])

        df = pd.concat(self.client.get_dataframe("account", top=100), ignore_index=True)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)
        self.assertListEqual(df["name"].tolist(), ["A", "B"])

    def test_get_empty_result(self):
        """Empty result set yields no DataFrames."""
        self.client._odata._get_multiple.return_value = iter([])

        pages = list(self.client.get_dataframe("account"))

        self.assertEqual(len(pages), 0)

    def test_get_passes_all_parameters(self):
        """All query parameters are forwarded to the underlying get method."""
        self.client._odata._get_multiple.return_value = iter([])

        # Consume the generator to trigger the underlying call
        list(self.client.get_dataframe(
            "account",
            select=["name"],
            filter="statecode eq 0",
            orderby=["name asc"],
            top=50,
            expand=["primarycontactid"],
            page_size=25,
        ))

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
    """Tests for create_dataframe."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"
        self.client = DataverseClient(self.base_url, self.mock_credential)
        self.client._odata = MagicMock()

    def test_create_dataframe(self):
        """DataFrame rows are converted to dicts and returned IDs are a Series."""
        df = pd.DataFrame([
            {"name": "Contoso", "telephone1": "555-0100"},
            {"name": "Fabrikam", "telephone1": "555-0200"},
        ])
        self.client._odata._create_multiple.return_value = ["guid-1", "guid-2"]
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

        ids = self.client.create_dataframe("account", df)

        self.assertIsInstance(ids, pd.Series)
        self.assertListEqual(ids.tolist(), ["guid-1", "guid-2"])
        call_args = self.client._odata._create_multiple.call_args
        records_arg = call_args[0][2]
        self.assertEqual(len(records_arg), 2)
        self.assertEqual(records_arg[0]["name"], "Contoso")
        self.assertEqual(records_arg[1]["name"], "Fabrikam")

    def test_create_assigns_to_column(self):
        """Returned Series can be assigned directly as a DataFrame column."""
        df = pd.DataFrame([{"name": "Contoso"}, {"name": "Fabrikam"}])
        self.client._odata._create_multiple.return_value = ["guid-1", "guid-2"]
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

        df["accountid"] = self.client.create_dataframe("account", df)

        self.assertListEqual(df["accountid"].tolist(), ["guid-1", "guid-2"])

    def test_create_single_row_dataframe(self):
        """Single-row DataFrame returns a single-element Series."""
        df = pd.DataFrame([{"name": "Contoso"}])
        self.client._odata._create_multiple.return_value = ["guid-1"]
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

        ids = self.client.create_dataframe("account", df)

        self.assertIsInstance(ids, pd.Series)
        self.assertEqual(ids.iloc[0], "guid-1")

    def test_create_rejects_non_dataframe(self):
        """Non-DataFrame input raises TypeError."""
        with self.assertRaises(TypeError) as ctx:
            self.client.create_dataframe("account", [{"name": "Contoso"}])
        self.assertIn("pandas DataFrame", str(ctx.exception))

    def test_create_empty_dataframe(self):
        """Empty DataFrame returns empty Series."""
        df = pd.DataFrame(columns=["name", "telephone1"])
        self.client._odata._create_multiple.return_value = []
        self.client._odata._entity_set_from_schema_name.return_value = "accounts"

        ids = self.client.create_dataframe("account", df)

        self.assertIsInstance(ids, pd.Series)
        self.assertEqual(len(ids), 0)


class TestDataFrameUpdate(unittest.TestCase):
    """Tests for update_dataframe."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"
        self.client = DataverseClient(self.base_url, self.mock_credential)
        self.client._odata = MagicMock()

    def test_update_dataframe(self):
        """DataFrame rows are split into IDs and changes, then passed to update."""
        df = pd.DataFrame([
            {"accountid": "guid-1", "telephone1": "555-0100"},
            {"accountid": "guid-2", "telephone1": "555-0200"},
        ])

        self.client.update_dataframe("account", df, id_column="accountid")

        self.client._odata._update_by_ids.assert_called_once()
        call_args = self.client._odata._update_by_ids.call_args[0]
        self.assertEqual(call_args[0], "account")
        self.assertEqual(call_args[1], ["guid-1", "guid-2"])
        self.assertEqual(call_args[2], [{"telephone1": "555-0100"}, {"telephone1": "555-0200"}])

    def test_update_rejects_non_dataframe(self):
        """Non-DataFrame input raises TypeError."""
        with self.assertRaises(TypeError) as ctx:
            self.client.update_dataframe("account", {"id": "guid-1"}, id_column="id")
        self.assertIn("pandas DataFrame", str(ctx.exception))

    def test_update_rejects_missing_id_column(self):
        """Missing id_column raises ValueError."""
        df = pd.DataFrame([{"name": "Contoso"}])
        with self.assertRaises(ValueError) as ctx:
            self.client.update_dataframe("account", df, id_column="accountid")
        self.assertIn("accountid", str(ctx.exception))

    def test_update_multiple_change_columns(self):
        """Multiple change columns are all included in the update payload (single row uses _update)."""
        df = pd.DataFrame([
            {"accountid": "guid-1", "name": "New Name", "telephone1": "555-0100"},
        ])

        self.client.update_dataframe("account", df, id_column="accountid")

        self.client._odata._update.assert_called_once()
        call_args = self.client._odata._update.call_args[0]
        self.assertEqual(call_args[0], "account")
        self.assertEqual(call_args[1], "guid-1")
        changes = call_args[2]
        self.assertIn("name", changes)
        self.assertIn("telephone1", changes)
        self.assertNotIn("accountid", changes)


class TestDataFrameDelete(unittest.TestCase):
    """Tests for delete_dataframe."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"
        self.client = DataverseClient(self.base_url, self.mock_credential)
        self.client._odata = MagicMock()

    def test_delete_dataframe_bulk(self):
        """Series of GUIDs passed to bulk delete."""
        ids = pd.Series(["guid-1", "guid-2", "guid-3"])
        self.client._odata._delete_multiple.return_value = "job-123"

        job_id = self.client.delete_dataframe("account", ids)

        self.assertEqual(job_id, "job-123")
        self.client._odata._delete_multiple.assert_called_once_with(
            "account", ["guid-1", "guid-2", "guid-3"]
        )

    def test_delete_from_dataframe_column(self):
        """Series extracted from a DataFrame column works directly."""
        df = pd.DataFrame({"accountid": ["guid-1", "guid-2"], "name": ["A", "B"]})
        self.client._odata._delete_multiple.return_value = "job-123"

        self.client.delete_dataframe("account", df["accountid"])

        self.client._odata._delete_multiple.assert_called_once_with(
            "account", ["guid-1", "guid-2"]
        )

    def test_delete_dataframe_sequential(self):
        """use_bulk_delete=False deletes records sequentially."""
        ids = pd.Series(["guid-1", "guid-2"])

        result = self.client.delete_dataframe("account", ids, use_bulk_delete=False)

        self.assertIsNone(result)
        self.assertEqual(self.client._odata._delete.call_count, 2)

    def test_delete_rejects_non_series(self):
        """Non-Series input raises TypeError."""
        with self.assertRaises(TypeError) as ctx:
            self.client.delete_dataframe("account", ["guid-1"])
        self.assertIn("pandas Series", str(ctx.exception))

    def test_delete_empty_series(self):
        """Empty Series returns None without calling delete."""
        ids = pd.Series([], dtype="str")

        result = self.client.delete_dataframe("account", ids)

        self.assertIsNone(result)


