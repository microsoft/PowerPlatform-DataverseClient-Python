# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for TableInfo and ColumnInfo data models."""

import unittest

from PowerPlatform.Dataverse.models.table_info import (
    TableInfo,
    ColumnInfo,
    ColumnSchema,
)


class TestColumnInfo(unittest.TestCase):
    """Test cases for the ColumnInfo dataclass."""

    def setUp(self):
        """Set up test fixtures."""
        self.column = ColumnInfo(
            schema_name="new_CustomColumn",
            logical_name="new_customcolumn",
            type="String",
            is_primary=False,
            is_required=True,
            max_length=100,
            display_name="Custom Column",
            description="A custom column for testing",
        )

    def test_column_creation(self):
        """Test ColumnInfo can be created with all fields."""
        self.assertEqual(self.column.schema_name, "new_CustomColumn")
        self.assertEqual(self.column.logical_name, "new_customcolumn")
        self.assertEqual(self.column.type, "String")
        self.assertFalse(self.column.is_primary)
        self.assertTrue(self.column.is_required)
        self.assertEqual(self.column.max_length, 100)
        self.assertEqual(self.column.display_name, "Custom Column")
        self.assertEqual(self.column.description, "A custom column for testing")

    def test_column_defaults(self):
        """Test ColumnInfo defaults."""
        column = ColumnInfo(
            schema_name="name",
            logical_name="name",
            type="String",
        )
        self.assertFalse(column.is_primary)
        self.assertFalse(column.is_required)
        self.assertIsNone(column.max_length)
        self.assertIsNone(column.display_name)
        self.assertIsNone(column.description)

    def test_to_dict(self):
        """Test to_dict() method."""
        result = self.column.to_dict()
        self.assertIsInstance(result, dict)
        self.assertEqual(result["schema_name"], "new_CustomColumn")
        self.assertEqual(result["logical_name"], "new_customcolumn")
        self.assertEqual(result["type"], "String")
        self.assertFalse(result["is_primary"])
        self.assertTrue(result["is_required"])
        self.assertEqual(result["max_length"], 100)

    def test_from_api_response_basic(self):
        """Test from_api_response() factory method."""
        api_response = {
            "SchemaName": "AccountId",
            "LogicalName": "accountid",
            "AttributeType": "Uniqueidentifier",
            "IsPrimaryId": True,
            "IsPrimaryName": False,
            "MaxLength": None,
        }

        column = ColumnInfo.from_api_response(api_response)

        self.assertEqual(column.schema_name, "AccountId")
        self.assertEqual(column.logical_name, "accountid")
        self.assertEqual(column.type, "Uniqueidentifier")
        self.assertTrue(column.is_primary)

    def test_from_api_response_with_nested_required_level(self):
        """Test from_api_response() with nested RequiredLevel."""
        api_response = {
            "SchemaName": "Name",
            "LogicalName": "name",
            "AttributeType": "String",
            "RequiredLevel": {"Value": "ApplicationRequired"},
        }

        column = ColumnInfo.from_api_response(api_response)

        self.assertTrue(column.is_required)

    def test_from_api_response_with_nested_display_name(self):
        """Test from_api_response() with nested DisplayName."""
        api_response = {
            "SchemaName": "Name",
            "LogicalName": "name",
            "AttributeType": "String",
            "DisplayName": {
                "UserLocalizedLabel": {"Label": "Account Name"},
            },
        }

        column = ColumnInfo.from_api_response(api_response)

        self.assertEqual(column.display_name, "Account Name")

    def test_from_api_response_with_odata_type(self):
        """Test from_api_response() falls back to @odata.type."""
        api_response = {
            "SchemaName": "Special",
            "LogicalName": "special",
            "@odata.type": "#Microsoft.Dynamics.CRM.StringAttributeMetadata",
        }

        column = ColumnInfo.from_api_response(api_response)

        self.assertEqual(column.type, "#Microsoft.Dynamics.CRM.StringAttributeMetadata")


class TestTableInfo(unittest.TestCase):
    """Test cases for the TableInfo dataclass."""

    def setUp(self):
        """Set up test fixtures."""
        self.table_info = TableInfo(
            schema_name="Account",
            logical_name="account",
            entity_set_name="accounts",
            metadata_id="abc123-def456",
            display_name="Account",
            description="Business account entity",
        )

    def test_table_creation(self):
        """Test TableInfo can be created with all fields."""
        self.assertEqual(self.table_info.schema_name, "Account")
        self.assertEqual(self.table_info.logical_name, "account")
        self.assertEqual(self.table_info.entity_set_name, "accounts")
        self.assertEqual(self.table_info.metadata_id, "abc123-def456")
        self.assertEqual(self.table_info.display_name, "Account")
        self.assertEqual(self.table_info.description, "Business account entity")
        self.assertIsNone(self.table_info.columns)

    def test_table_defaults(self):
        """Test TableInfo defaults."""
        table = TableInfo(
            schema_name="Test",
            logical_name="test",
            entity_set_name="tests",
            metadata_id="guid",
        )
        self.assertIsNone(table.display_name)
        self.assertIsNone(table.description)
        self.assertIsNone(table.columns)

    def test_getitem_legacy_keys(self):
        """Test dictionary-like access with legacy key names."""
        self.assertEqual(self.table_info["table_schema_name"], "Account")
        self.assertEqual(self.table_info["table_logical_name"], "account")
        self.assertEqual(self.table_info["entity_set_name"], "accounts")
        self.assertEqual(self.table_info["metadata_id"], "abc123-def456")

    def test_getitem_direct_attributes(self):
        """Test dictionary-like access with direct attribute names."""
        self.assertEqual(self.table_info["schema_name"], "Account")
        self.assertEqual(self.table_info["logical_name"], "account")

    def test_getitem_keyerror(self):
        """Test __getitem__ raises KeyError for invalid keys."""
        with self.assertRaises(KeyError):
            _ = self.table_info["nonexistent"]

    def test_contains_legacy_keys(self):
        """Test 'in' operator with legacy key names."""
        self.assertIn("table_schema_name", self.table_info)
        self.assertIn("table_logical_name", self.table_info)
        self.assertIn("entity_set_name", self.table_info)

    def test_contains_direct_attributes(self):
        """Test 'in' operator with direct attribute names."""
        self.assertIn("schema_name", self.table_info)
        self.assertIn("logical_name", self.table_info)

    def test_contains_false_for_missing(self):
        """Test 'in' operator returns False for missing keys."""
        self.assertNotIn("nonexistent", self.table_info)
        self.assertNotIn("_private", self.table_info)

    def test_iter(self):
        """Test iteration over legacy key names."""
        keys = list(self.table_info)
        self.assertIn("table_schema_name", keys)
        self.assertIn("table_logical_name", keys)
        self.assertIn("entity_set_name", keys)

    def test_len(self):
        """Test len() returns number of legacy keys."""
        self.assertEqual(len(self.table_info), 7)  # 7 keys in _LEGACY_KEY_MAP

    def test_get_with_default(self):
        """Test get() method with default value."""
        self.assertEqual(self.table_info.get("table_schema_name"), "Account")
        self.assertEqual(self.table_info.get("nonexistent"), None)
        self.assertEqual(self.table_info.get("nonexistent", "default"), "default")

    def test_keys(self):
        """Test keys() method."""
        keys = list(self.table_info.keys())
        self.assertIn("table_schema_name", keys)
        self.assertIn("entity_set_name", keys)

    def test_values(self):
        """Test values() method."""
        values = list(self.table_info.values())
        self.assertIn("Account", values)
        self.assertIn("accounts", values)

    def test_items(self):
        """Test items() method."""
        items = dict(self.table_info.items())
        self.assertEqual(items["table_schema_name"], "Account")
        self.assertEqual(items["entity_set_name"], "accounts")

    def test_to_dict(self):
        """Test to_dict() returns dictionary with legacy key names."""
        result = self.table_info.to_dict()
        self.assertIsInstance(result, dict)
        self.assertEqual(result["table_schema_name"], "Account")
        self.assertEqual(result["table_logical_name"], "account")
        self.assertEqual(result["entity_set_name"], "accounts")
        self.assertEqual(result["metadata_id"], "abc123-def456")

    def test_to_dict_with_columns(self):
        """Test to_dict() includes columns when present."""
        column = ColumnInfo(
            schema_name="Name",
            logical_name="name",
            type="String",
        )
        table = TableInfo(
            schema_name="Test",
            logical_name="test",
            entity_set_name="tests",
            metadata_id="guid",
            columns=[column],
        )

        result = table.to_dict()

        self.assertIn("columns", result)
        self.assertEqual(len(result["columns"]), 1)
        self.assertEqual(result["columns"][0]["schema_name"], "Name")

    def test_from_api_response(self):
        """Test from_api_response() factory method."""
        api_response = {
            "SchemaName": "Contact",
            "LogicalName": "contact",
            "EntitySetName": "contacts",
            "MetadataId": "contact-meta-guid",
            "DisplayName": {
                "UserLocalizedLabel": {"Label": "Contact"},
            },
        }

        table = TableInfo.from_api_response(api_response)

        self.assertEqual(table.schema_name, "Contact")
        self.assertEqual(table.logical_name, "contact")
        self.assertEqual(table.entity_set_name, "contacts")
        self.assertEqual(table.metadata_id, "contact-meta-guid")
        self.assertEqual(table.display_name, "Contact")

    def test_from_api_response_with_attributes(self):
        """Test from_api_response() parses Attributes to columns."""
        api_response = {
            "SchemaName": "Test",
            "LogicalName": "test",
            "EntitySetName": "tests",
            "MetadataId": "guid",
            "Attributes": [
                {
                    "SchemaName": "Col1",
                    "LogicalName": "col1",
                    "AttributeType": "String",
                },
                {
                    "SchemaName": "Col2",
                    "LogicalName": "col2",
                    "AttributeType": "Integer",
                },
            ],
        }

        table = TableInfo.from_api_response(api_response)

        self.assertIsNotNone(table.columns)
        self.assertEqual(len(table.columns), 2)
        self.assertEqual(table.columns[0].schema_name, "Col1")
        self.assertEqual(table.columns[1].type, "Integer")

    def test_from_dict(self):
        """Test from_dict() factory method with internal format."""
        data = {
            "table_schema_name": "Lead",
            "table_logical_name": "lead",
            "entity_set_name": "leads",
            "metadata_id": "lead-guid",
            "columns_created": ["new_col1", "new_col2"],
        }

        table = TableInfo.from_dict(data)

        self.assertEqual(table.schema_name, "Lead")
        self.assertEqual(table.logical_name, "lead")
        self.assertEqual(table.entity_set_name, "leads")
        self.assertEqual(table.columns_created, ["new_col1", "new_col2"])

    def test_type_alias_exists(self):
        """Test that ColumnSchema type alias is exported."""
        self.assertEqual(ColumnSchema, str)


class TestTableInfoBackwardCompatibility(unittest.TestCase):
    """Test backward compatibility patterns for TableInfo."""

    def setUp(self):
        """Set up test fixtures."""
        self.table_info = TableInfo(
            schema_name="Account",
            logical_name="account",
            entity_set_name="accounts",
            metadata_id="guid",
        )

    def test_dict_pattern_iteration(self):
        """Test common dict iteration pattern."""
        # Pattern: for key in info: info[key]
        result = {key: self.table_info[key] for key in self.table_info}
        self.assertIn("table_schema_name", result)
        self.assertEqual(result["table_schema_name"], "Account")

    def test_dict_pattern_get(self):
        """Test common dict get pattern."""
        # Pattern: info.get("key", default)
        self.assertEqual(self.table_info.get("table_schema_name"), "Account")
        self.assertEqual(self.table_info.get("missing", "default"), "default")

    def test_dict_pattern_in(self):
        """Test common 'in' operator pattern."""
        # Pattern: if "key" in info
        self.assertTrue("table_schema_name" in self.table_info)
        self.assertTrue("entity_set_name" in self.table_info)
        self.assertFalse("missing" in self.table_info)

    def test_existing_code_pattern(self):
        """Test pattern from existing code: info['table_schema_name']."""
        # This is the pattern used in existing tests and code
        self.assertEqual(self.table_info["table_schema_name"], "Account")
        self.assertEqual(self.table_info["entity_set_name"], "accounts")


if __name__ == "__main__":
    unittest.main()
