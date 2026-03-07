# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest

from PowerPlatform.Dataverse.models.record import Record


class TestRecordDictLike(unittest.TestCase):
    """Dict-like access on Record delegates to self.data."""

    def setUp(self):
        self.record = Record(
            id="guid-1",
            table="account",
            data={"name": "Contoso", "telephone1": "555-0100"},
        )

    def test_getitem(self):
        self.assertEqual(self.record["name"], "Contoso")

    def test_getitem_missing_raises(self):
        with self.assertRaises(KeyError):
            _ = self.record["nonexistent"]

    def test_get_with_default(self):
        self.assertEqual(self.record.get("name"), "Contoso")
        self.assertEqual(self.record.get("missing", "fallback"), "fallback")

    def test_contains(self):
        self.assertIn("name", self.record)
        self.assertNotIn("missing", self.record)

    def test_iter(self):
        self.assertEqual(set(self.record), {"name", "telephone1"})

    def test_len(self):
        self.assertEqual(len(self.record), 2)

    def test_setitem(self):
        self.record["new_key"] = "value"
        self.assertEqual(self.record["new_key"], "value")

    def test_delitem(self):
        del self.record["telephone1"]
        self.assertNotIn("telephone1", self.record)

    def test_keys_values_items(self):
        self.assertEqual(set(self.record.keys()), {"name", "telephone1"})
        self.assertIn("Contoso", list(self.record.values()))
        self.assertIn(("name", "Contoso"), list(self.record.items()))


class TestRecordFromApiResponse(unittest.TestCase):
    """Tests for Record.from_api_response factory."""

    def test_strips_odata_keys(self):
        raw = {
            "@odata.context": "https://org.crm.dynamics.com/...",
            "@odata.etag": 'W/"12345"',
            "accountid": "guid-1",
            "name": "Contoso",
        }
        record = Record.from_api_response("account", raw, record_id="guid-1")
        self.assertNotIn("@odata.context", record)
        self.assertNotIn("@odata.etag", record)
        self.assertEqual(record["accountid"], "guid-1")
        self.assertEqual(record["name"], "Contoso")

    def test_extracts_etag(self):
        raw = {"@odata.etag": 'W/"12345"', "name": "Test"}
        record = Record.from_api_response("account", raw)
        self.assertEqual(record.etag, 'W/"12345"')

    def test_no_etag(self):
        raw = {"name": "Test"}
        record = Record.from_api_response("account", raw)
        self.assertIsNone(record.etag)

    def test_record_id_set(self):
        raw = {"name": "Test"}
        record = Record.from_api_response("account", raw, record_id="guid-1")
        self.assertEqual(record.id, "guid-1")
        self.assertEqual(record.table, "account")

    def test_record_id_default_empty(self):
        raw = {"name": "Test"}
        record = Record.from_api_response("account", raw)
        self.assertEqual(record.id, "")

    def test_to_dict(self):
        raw = {"@odata.etag": 'W/"1"', "name": "Test", "revenue": 1000}
        record = Record.from_api_response("account", raw)
        d = record.to_dict()
        self.assertIsInstance(d, dict)
        self.assertEqual(d, {"name": "Test", "revenue": 1000})


if __name__ == "__main__":
    unittest.main()
