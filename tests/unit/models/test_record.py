# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for Record data model."""

import unittest

from PowerPlatform.Dataverse.models.record import Record, RecordId, TableSchema


class TestRecord(unittest.TestCase):
    """Test cases for the Record dataclass."""

    def setUp(self):
        """Set up test fixtures."""
        self.sample_data = {
            "accountid": "12345678-1234-1234-1234-123456789abc",
            "name": "Contoso Ltd",
            "telephone1": "555-0100",
            "revenue": 1000000.00,
        }
        self.record = Record(
            id="12345678-1234-1234-1234-123456789abc",
            table="account",
            data=dict(self.sample_data),
            etag='"1234567"',
        )

    def test_record_creation(self):
        """Test Record can be created with all fields."""
        self.assertEqual(self.record.id, "12345678-1234-1234-1234-123456789abc")
        self.assertEqual(self.record.table, "account")
        self.assertEqual(self.record.etag, '"1234567"')

    def test_record_default_data(self):
        """Test Record with default empty data."""
        record = Record(id="guid", table="contact")
        self.assertEqual(record.data, {})
        self.assertIsNone(record.etag)

    def test_getitem(self):
        """Test dictionary-like access via __getitem__."""
        self.assertEqual(self.record["name"], "Contoso Ltd")
        self.assertEqual(self.record["telephone1"], "555-0100")
        self.assertEqual(self.record["revenue"], 1000000.00)

    def test_getitem_keyerror(self):
        """Test __getitem__ raises KeyError for missing keys."""
        with self.assertRaises(KeyError):
            _ = self.record["nonexistent"]

    def test_setitem(self):
        """Test dictionary-like mutation via __setitem__."""
        self.record["telephone1"] = "555-0199"
        self.assertEqual(self.record["telephone1"], "555-0199")

        self.record["new_field"] = "new value"
        self.assertEqual(self.record["new_field"], "new value")

    def test_delitem(self):
        """Test dictionary-like deletion via __delitem__."""
        self.record["to_delete"] = "temporary"
        self.assertIn("to_delete", self.record)

        del self.record["to_delete"]
        self.assertNotIn("to_delete", self.record)

    def test_delitem_keyerror(self):
        """Test __delitem__ raises KeyError for missing keys."""
        with self.assertRaises(KeyError):
            del self.record["nonexistent"]

    def test_contains(self):
        """Test membership testing via __contains__."""
        self.assertIn("name", self.record)
        self.assertIn("telephone1", self.record)
        self.assertNotIn("nonexistent", self.record)

    def test_iter(self):
        """Test iteration over field names."""
        keys = list(self.record)
        self.assertIn("name", keys)
        self.assertIn("telephone1", keys)
        self.assertEqual(len(keys), 4)

    def test_len(self):
        """Test len() returns number of fields."""
        self.assertEqual(len(self.record), 4)

    def test_get_with_default(self):
        """Test get() method with default value."""
        self.assertEqual(self.record.get("name"), "Contoso Ltd")
        self.assertEqual(self.record.get("nonexistent"), None)
        self.assertEqual(self.record.get("nonexistent", "default"), "default")

    def test_keys(self):
        """Test keys() returns field names."""
        keys = list(self.record.keys())
        self.assertIn("name", keys)
        self.assertIn("telephone1", keys)

    def test_values(self):
        """Test values() returns field values."""
        values = list(self.record.values())
        self.assertIn("Contoso Ltd", values)
        self.assertIn("555-0100", values)

    def test_items(self):
        """Test items() returns key-value pairs."""
        items = dict(self.record.items())
        self.assertEqual(items["name"], "Contoso Ltd")
        self.assertEqual(items["telephone1"], "555-0100")

    def test_to_dict(self):
        """Test to_dict() returns only data fields."""
        result = self.record.to_dict()
        self.assertIsInstance(result, dict)
        self.assertEqual(result["name"], "Contoso Ltd")
        self.assertNotIn("id", result)
        self.assertNotIn("table", result)
        self.assertNotIn("etag", result)

    def test_to_full_dict(self):
        """Test to_full_dict() includes metadata."""
        result = self.record.to_full_dict()
        self.assertIsInstance(result, dict)
        self.assertEqual(result["id"], "12345678-1234-1234-1234-123456789abc")
        self.assertEqual(result["table"], "account")
        self.assertEqual(result["etag"], '"1234567"')
        self.assertEqual(result["data"]["name"], "Contoso Ltd")

    def test_from_api_response_basic(self):
        """Test from_api_response() factory method."""
        api_response = {
            "accountid": "guid-123",
            "name": "Test Account",
            "revenue": 500000,
            "@odata.etag": '"etag-value"',
            "@odata.context": "https://example.com/$metadata",
        }

        record = Record.from_api_response("account", api_response)

        self.assertEqual(record.id, "guid-123")
        self.assertEqual(record.table, "account")
        self.assertEqual(record.etag, '"etag-value"')
        self.assertEqual(record["name"], "Test Account")
        self.assertEqual(record["revenue"], 500000)
        # OData annotations should be removed
        self.assertNotIn("@odata.etag", record)
        self.assertNotIn("@odata.context", record)

    def test_from_api_response_with_explicit_id_field(self):
        """Test from_api_response() with explicit id_field parameter."""
        api_response = {
            "contactid": "contact-guid",
            "fullname": "John Doe",
        }

        record = Record.from_api_response("contact", api_response, id_field="contactid")

        self.assertEqual(record.id, "contact-guid")
        self.assertEqual(record["fullname"], "John Doe")

    def test_from_api_response_without_etag(self):
        """Test from_api_response() when no ETag is present."""
        api_response = {
            "accountid": "guid-456",
            "name": "No ETag Account",
        }

        record = Record.from_api_response("account", api_response)

        self.assertIsNone(record.etag)

    def test_from_api_response_does_not_mutate_original(self):
        """Test from_api_response() doesn't modify the original dict."""
        api_response = {
            "accountid": "guid-789",
            "name": "Original",
            "@odata.etag": '"etag"',
        }
        original_copy = dict(api_response)

        Record.from_api_response("account", api_response)

        # Original should still have @odata.etag
        self.assertEqual(api_response, original_copy)

    def test_type_aliases_exist(self):
        """Test that type aliases are exported."""
        # These should be str type aliases
        self.assertEqual(RecordId, str)
        self.assertEqual(TableSchema, str)


class TestRecordBackwardCompatibility(unittest.TestCase):
    """Test backward compatibility patterns for Record."""

    def setUp(self):
        """Set up test fixtures."""
        self.record = Record(
            id="guid",
            table="account",
            data={"name": "Test", "value": 100},
        )

    def test_dict_pattern_iteration(self):
        """Test common dict iteration pattern."""
        # Pattern: for key in record: record[key]
        result = {key: self.record[key] for key in self.record}
        self.assertEqual(result, {"name": "Test", "value": 100})

    def test_dict_pattern_items(self):
        """Test common dict items pattern."""
        # Pattern: for key, value in record.items()
        result = {k: v for k, v in self.record.items()}
        self.assertEqual(result, {"name": "Test", "value": 100})

    def test_dict_pattern_get(self):
        """Test common dict get pattern."""
        # Pattern: record.get("key", default)
        self.assertEqual(self.record.get("name"), "Test")
        self.assertEqual(self.record.get("missing", "default"), "default")

    def test_dict_pattern_in(self):
        """Test common 'in' operator pattern."""
        # Pattern: if "key" in record
        self.assertTrue("name" in self.record)
        self.assertFalse("missing" in self.record)


if __name__ == "__main__":
    unittest.main()
