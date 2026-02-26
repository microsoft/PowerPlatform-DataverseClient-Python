# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest

from PowerPlatform.Dataverse.models.table_info import AlternateKeyInfo


class TestAlternateKeyInfoDefaults(unittest.TestCase):
    """Tests for AlternateKeyInfo default values."""

    def test_default_values(self):
        """All fields should default to empty string or empty list."""
        info = AlternateKeyInfo()
        self.assertEqual(info.metadata_id, "")
        self.assertEqual(info.schema_name, "")
        self.assertEqual(info.key_attributes, [])
        self.assertEqual(info.status, "")

    def test_independent_default_lists(self):
        """Each instance should have its own key_attributes list (no shared mutable default)."""
        a = AlternateKeyInfo()
        b = AlternateKeyInfo()
        a.key_attributes.append("col1")
        self.assertEqual(b.key_attributes, [])


class TestAlternateKeyInfoFromApiResponse(unittest.TestCase):
    """Tests for AlternateKeyInfo.from_api_response factory."""

    def test_full_response(self):
        """from_api_response should map all PascalCase API fields."""
        raw = {
            "MetadataId": "key-guid-1",
            "SchemaName": "new_product_code_key",
            "KeyAttributes": ["new_productcode"],
            "EntityKeyIndexStatus": "Active",
        }
        info = AlternateKeyInfo.from_api_response(raw)
        self.assertEqual(info.metadata_id, "key-guid-1")
        self.assertEqual(info.schema_name, "new_product_code_key")
        self.assertEqual(info.key_attributes, ["new_productcode"])
        self.assertEqual(info.status, "Active")

    def test_multi_column_key(self):
        """from_api_response should handle multi-column keys."""
        raw = {
            "MetadataId": "key-guid-2",
            "SchemaName": "new_composite_key",
            "KeyAttributes": ["new_col1", "new_col2", "new_col3"],
            "EntityKeyIndexStatus": "Pending",
        }
        info = AlternateKeyInfo.from_api_response(raw)
        self.assertEqual(info.key_attributes, ["new_col1", "new_col2", "new_col3"])
        self.assertEqual(info.status, "Pending")

    def test_minimal_response(self):
        """from_api_response should handle a response with missing optional fields."""
        raw = {}
        info = AlternateKeyInfo.from_api_response(raw)
        self.assertEqual(info.metadata_id, "")
        self.assertEqual(info.schema_name, "")
        self.assertEqual(info.key_attributes, [])
        self.assertEqual(info.status, "")

    def test_partial_response(self):
        """from_api_response should handle a response with only some fields."""
        raw = {
            "MetadataId": "key-guid-3",
            "SchemaName": "new_partial_key",
        }
        info = AlternateKeyInfo.from_api_response(raw)
        self.assertEqual(info.metadata_id, "key-guid-3")
        self.assertEqual(info.schema_name, "new_partial_key")
        self.assertEqual(info.key_attributes, [])
        self.assertEqual(info.status, "")


if __name__ == "__main__":
    unittest.main()
