# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for BatchDataFrameOperations."""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from PowerPlatform.Dataverse.operations.batch import (
    BatchDataFrameOperations,
    BatchRecordOperations,
    BatchRequest,
)
from PowerPlatform.Dataverse.data._batch import _RecordCreate, _RecordUpdate, _RecordDelete


def _make_batch():
    """Return a BatchRequest with a mocked client."""
    client = MagicMock()
    batch = BatchRequest(client)
    return batch


class TestBatchDataFrameNamespace(unittest.TestCase):
    """BatchRequest should have a .dataframe namespace."""

    def test_batch_has_dataframe_attribute(self):
        batch = _make_batch()
        self.assertIsInstance(batch.dataframe, BatchDataFrameOperations)


class TestBatchDataFrameCreate(unittest.TestCase):
    """batch.dataframe.create() converts DataFrame to records and delegates to batch.records.create."""

    def test_create_from_dataframe(self):
        batch = _make_batch()
        df = pd.DataFrame(
            [
                {"name": "Contoso", "telephone1": "555-0100"},
                {"name": "Fabrikam", "telephone1": "555-0200"},
            ]
        )
        batch.dataframe.create("account", df)
        # Should have enqueued a _RecordCreate with a list of dicts
        self.assertEqual(len(batch._items), 1)
        item = batch._items[0]
        self.assertIsInstance(item, _RecordCreate)
        self.assertEqual(item.table, "account")
        self.assertIsInstance(item.data, list)
        self.assertEqual(len(item.data), 2)
        self.assertEqual(item.data[0]["name"], "Contoso")
        self.assertEqual(item.data[1]["name"], "Fabrikam")

    def test_create_single_row(self):
        batch = _make_batch()
        df = pd.DataFrame([{"name": "SingleCo"}])
        batch.dataframe.create("account", df)
        self.assertEqual(len(batch._items), 1)
        self.assertIsInstance(batch._items[0].data, list)
        self.assertEqual(len(batch._items[0].data), 1)

    def test_create_rejects_non_dataframe(self):
        batch = _make_batch()
        with self.assertRaises(TypeError) as ctx:
            batch.dataframe.create("account", [{"name": "x"}])
        self.assertIn("DataFrame", str(ctx.exception))

    def test_create_rejects_empty_dataframe(self):
        batch = _make_batch()
        df = pd.DataFrame()
        with self.assertRaises(ValueError) as ctx:
            batch.dataframe.create("account", df)
        self.assertIn("non-empty", str(ctx.exception))

    def test_create_rejects_rows_with_all_nan(self):
        batch = _make_batch()
        df = pd.DataFrame([{"name": None}])
        with self.assertRaises(ValueError) as ctx:
            batch.dataframe.create("account", df)
        self.assertIn("no non-null", str(ctx.exception))

    def test_create_handles_nan_values(self):
        """NaN values are dropped from individual records by default."""
        batch = _make_batch()
        df = pd.DataFrame(
            [
                {"name": "Contoso", "telephone1": "555-0100"},
                {"name": "Fabrikam", "telephone1": None},
            ]
        )
        batch.dataframe.create("account", df)
        item = batch._items[0]
        # Second record should not have telephone1
        self.assertNotIn("telephone1", item.data[1])
        self.assertIn("name", item.data[1])


class TestBatchDataFrameUpdate(unittest.TestCase):
    """batch.dataframe.update() converts DataFrame to update operations."""

    def test_update_enqueues_record_update(self):
        batch = _make_batch()
        df = pd.DataFrame(
            [
                {"accountid": "guid-1", "telephone1": "555-0100"},
                {"accountid": "guid-2", "telephone1": "555-0200"},
            ]
        )
        batch.dataframe.update("account", df, id_column="accountid")
        self.assertEqual(len(batch._items), 1)
        item = batch._items[0]
        self.assertIsInstance(item, _RecordUpdate)
        self.assertEqual(item.table, "account")
        # ids should be a list, changes should be a list
        self.assertIsInstance(item.ids, list)
        self.assertEqual(len(item.ids), 2)
        self.assertEqual(item.ids[0], "guid-1")

    def test_update_rejects_non_dataframe(self):
        batch = _make_batch()
        with self.assertRaises(TypeError):
            batch.dataframe.update("account", [{}], id_column="id")

    def test_update_rejects_empty_dataframe(self):
        batch = _make_batch()
        with self.assertRaises(ValueError):
            batch.dataframe.update("account", pd.DataFrame(), id_column="id")

    def test_update_rejects_missing_id_column(self):
        batch = _make_batch()
        df = pd.DataFrame([{"name": "x"}])
        with self.assertRaises(ValueError) as ctx:
            batch.dataframe.update("account", df, id_column="accountid")
        self.assertIn("not found", str(ctx.exception))

    def test_update_rejects_invalid_ids(self):
        batch = _make_batch()
        df = pd.DataFrame([{"accountid": 123, "name": "x"}])
        with self.assertRaises(ValueError) as ctx:
            batch.dataframe.update("account", df, id_column="accountid")
        self.assertIn("invalid", str(ctx.exception))

    def test_update_skips_all_nan_rows(self):
        """Rows where all change values are NaN are silently skipped."""
        batch = _make_batch()
        df = pd.DataFrame(
            [
                {"accountid": "guid-1", "name": None},
            ]
        )
        batch.dataframe.update("account", df, id_column="accountid")
        # Nothing enqueued because all change values were NaN
        self.assertEqual(len(batch._items), 0)


class TestBatchDataFrameDelete(unittest.TestCase):
    """batch.dataframe.delete() converts Series to delete operation."""

    def test_delete_from_series(self):
        batch = _make_batch()
        ids = pd.Series(["guid-1", "guid-2", "guid-3"])
        batch.dataframe.delete("account", ids)
        self.assertEqual(len(batch._items), 1)
        item = batch._items[0]
        self.assertIsInstance(item, _RecordDelete)
        self.assertIsInstance(item.ids, list)
        self.assertEqual(len(item.ids), 3)

    def test_delete_rejects_non_series(self):
        batch = _make_batch()
        with self.assertRaises(TypeError):
            batch.dataframe.delete("account", ["guid-1"])

    def test_delete_empty_series_no_op(self):
        batch = _make_batch()
        ids = pd.Series([], dtype=str)
        batch.dataframe.delete("account", ids)
        self.assertEqual(len(batch._items), 0)

    def test_delete_rejects_invalid_ids(self):
        batch = _make_batch()
        ids = pd.Series([123, 456])
        with self.assertRaises(ValueError):
            batch.dataframe.delete("account", ids)

    def test_delete_with_bulk_delete_false(self):
        batch = _make_batch()
        ids = pd.Series(["guid-1", "guid-2"])
        batch.dataframe.delete("account", ids, use_bulk_delete=False)
        item = batch._items[0]
        self.assertIsInstance(item, _RecordDelete)
        self.assertFalse(item.use_bulk_delete)


if __name__ == "__main__":
    unittest.main()
