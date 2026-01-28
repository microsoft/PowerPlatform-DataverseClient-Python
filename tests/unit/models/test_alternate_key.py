# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for AlternateKeyInfo model."""

import unittest

from PowerPlatform.Dataverse.models.alternate_key import AlternateKeyInfo


class TestAlternateKeyInfo(unittest.TestCase):
    """Test cases for the AlternateKeyInfo dataclass."""

    def test_from_api_response_basic(self):
        """Test creating AlternateKeyInfo from API response."""
        api_response = {
            "SchemaName": "AccountNumberKey",
            "LogicalName": "accountnumberkey",
            "KeyAttributes": ["accountnumber"],
            "MetadataId": "key-guid-123",
            "EntityKeyIndexStatus": "Active",
        }

        key_info = AlternateKeyInfo.from_api_response(api_response)

        self.assertEqual(key_info.schema_name, "AccountNumberKey")
        self.assertEqual(key_info.logical_name, "accountnumberkey")
        self.assertEqual(key_info.columns, ["accountnumber"])
        self.assertEqual(key_info.metadata_id, "key-guid-123")
        self.assertEqual(key_info.status, "Active")

    def test_from_api_response_with_display_name(self):
        """Test creating AlternateKeyInfo with nested DisplayName."""
        api_response = {
            "SchemaName": "RegionStoreKey",
            "LogicalName": "regionstorekey",
            "KeyAttributes": ["my_region", "my_storeid"],
            "MetadataId": "key-guid",
            "EntityKeyIndexStatus": "Pending",
            "DisplayName": {
                "UserLocalizedLabel": {"Label": "Region and Store ID"},
            },
        }

        key_info = AlternateKeyInfo.from_api_response(api_response)

        self.assertEqual(key_info.display_name, "Region and Store ID")
        self.assertEqual(key_info.columns, ["my_region", "my_storeid"])

    def test_from_dict(self):
        """Test creating AlternateKeyInfo from internal dict format."""
        data = {
            "schema_name": "TestKey",
            "logical_name": "testkey",
            "columns": ["col1", "col2"],
            "metadata_id": "meta-id",
            "status": "Active",
            "display_name": "Test Key",
        }

        key_info = AlternateKeyInfo.from_dict(data)

        self.assertEqual(key_info.schema_name, "TestKey")
        self.assertEqual(key_info.columns, ["col1", "col2"])
        self.assertEqual(key_info.display_name, "Test Key")

    def test_dict_like_access(self):
        """Test dict-like access on AlternateKeyInfo."""
        key_info = AlternateKeyInfo(
            schema_name="TestKey",
            logical_name="testkey",
            columns=["col1"],
            metadata_id="guid",
            status="Active",
        )

        # Dict-like access
        self.assertEqual(key_info["schema_name"], "TestKey")
        self.assertEqual(key_info["columns"], ["col1"])
        self.assertEqual(key_info["status"], "Active")
        # key_attributes alias
        self.assertEqual(key_info["key_attributes"], ["col1"])

    def test_contains(self):
        """Test 'in' operator for AlternateKeyInfo."""
        key_info = AlternateKeyInfo(
            schema_name="TestKey",
            logical_name="testkey",
            columns=["col1"],
            metadata_id="guid",
        )

        self.assertIn("schema_name", key_info)
        self.assertIn("columns", key_info)
        self.assertIn("key_attributes", key_info)
        self.assertNotIn("nonexistent", key_info)

    def test_iteration(self):
        """Test iteration over AlternateKeyInfo keys."""
        key_info = AlternateKeyInfo(
            schema_name="TestKey",
            logical_name="testkey",
            columns=["col1"],
            metadata_id="guid",
        )

        keys = list(key_info)
        self.assertIn("schema_name", keys)
        self.assertIn("columns", keys)

    def test_to_dict(self):
        """Test converting AlternateKeyInfo to dict."""
        key_info = AlternateKeyInfo(
            schema_name="TestKey",
            logical_name="testkey",
            columns=["col1", "col2"],
            metadata_id="guid",
            status="Active",
            display_name="Test Display",
        )

        result = key_info.to_dict()

        self.assertEqual(result["schema_name"], "TestKey")
        self.assertEqual(result["columns"], ["col1", "col2"])
        self.assertEqual(result["display_name"], "Test Display")

    def test_get_with_default(self):
        """Test get() method with default value."""
        key_info = AlternateKeyInfo(
            schema_name="TestKey",
            logical_name="testkey",
            columns=["col1"],
            metadata_id="guid",
        )

        self.assertEqual(key_info.get("schema_name"), "TestKey")
        self.assertEqual(key_info.get("nonexistent", "default"), "default")
        self.assertIsNone(key_info.get("nonexistent"))


if __name__ == "__main__":
    unittest.main()
