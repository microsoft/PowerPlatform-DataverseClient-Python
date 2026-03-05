# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest

from PowerPlatform.Dataverse.models.table_info import ColumnInfo, TableInfo


class TestTableInfoLegacyAccess(unittest.TestCase):
    """TableInfo should support both legacy dict keys and attribute access."""

    def setUp(self):
        self.info = TableInfo(
            schema_name="new_Product",
            logical_name="new_product",
            entity_set_name="new_products",
            metadata_id="meta-guid-1",
            columns_created=["new_Price", "new_InStock"],
        )

    def test_legacy_key_getitem(self):
        self.assertEqual(self.info["table_schema_name"], "new_Product")
        self.assertEqual(self.info["table_logical_name"], "new_product")
        self.assertEqual(self.info["entity_set_name"], "new_products")
        self.assertEqual(self.info["metadata_id"], "meta-guid-1")
        self.assertEqual(self.info["columns_created"], ["new_Price", "new_InStock"])

    def test_attribute_access(self):
        self.assertEqual(self.info.schema_name, "new_Product")
        self.assertEqual(self.info.logical_name, "new_product")

    def test_new_key_also_works(self):
        """Direct attribute names also work as dict keys."""
        self.assertEqual(self.info["schema_name"], "new_Product")

    def test_legacy_key_contains(self):
        self.assertIn("table_schema_name", self.info)
        self.assertIn("entity_set_name", self.info)

    def test_missing_key_raises(self):
        with self.assertRaises(KeyError):
            _ = self.info["nonexistent_key_xyz"]

    def test_get_with_default(self):
        self.assertEqual(self.info.get("table_schema_name"), "new_Product")
        self.assertEqual(self.info.get("nonexistent", "fallback"), "fallback")

    def test_legacy_key_iteration(self):
        keys = list(self.info)
        self.assertEqual(
            keys,
            ["table_schema_name", "table_logical_name", "entity_set_name", "metadata_id", "columns_created"],
        )

    def test_len(self):
        self.assertEqual(len(self.info), 5)

    def test_keys_values_items(self):
        self.assertEqual(list(self.info.keys()), list(self.info._LEGACY_KEY_MAP.keys()))
        items = dict(self.info.items())
        self.assertEqual(items["table_schema_name"], "new_Product")

    def test_to_dict(self):
        d = self.info.to_dict()
        self.assertIsInstance(d, dict)
        self.assertEqual(d["table_schema_name"], "new_Product")
        self.assertEqual(d["columns_created"], ["new_Price", "new_InStock"])


class TestTableInfoFromDict(unittest.TestCase):
    """Tests for TableInfo.from_dict factory (SDK internal dict format)."""

    def test_from_dict(self):
        data = {
            "table_schema_name": "new_Product",
            "table_logical_name": "new_product",
            "entity_set_name": "new_products",
            "metadata_id": "meta-guid-1",
            "columns_created": ["new_Price"],
        }
        info = TableInfo.from_dict(data)
        self.assertEqual(info.schema_name, "new_Product")
        self.assertEqual(info.logical_name, "new_product")
        self.assertEqual(info.entity_set_name, "new_products")
        self.assertEqual(info.metadata_id, "meta-guid-1")
        self.assertEqual(info.columns_created, ["new_Price"])

    def test_from_dict_missing_keys(self):
        info = TableInfo.from_dict({})
        self.assertEqual(info.schema_name, "")
        self.assertIsNone(info.columns_created)


class TestTableInfoFromApiResponse(unittest.TestCase):
    """Tests for TableInfo.from_api_response factory (PascalCase keys)."""

    def test_from_api_response(self):
        raw = {
            "SchemaName": "Account",
            "LogicalName": "account",
            "EntitySetName": "accounts",
            "MetadataId": "meta-guid-2",
            "DisplayName": {"UserLocalizedLabel": {"Label": "Account", "LanguageCode": 1033}},
            "Description": {"UserLocalizedLabel": {"Label": "Business account", "LanguageCode": 1033}},
        }
        info = TableInfo.from_api_response(raw)
        self.assertEqual(info.schema_name, "Account")
        self.assertEqual(info.logical_name, "account")
        self.assertEqual(info.entity_set_name, "accounts")
        self.assertEqual(info.metadata_id, "meta-guid-2")
        self.assertEqual(info.display_name, "Account")
        self.assertEqual(info.description, "Business account")

    def test_from_api_response_no_labels(self):
        raw = {"SchemaName": "contact", "LogicalName": "contact", "EntitySetName": "contacts", "MetadataId": "guid"}
        info = TableInfo.from_api_response(raw)
        self.assertIsNone(info.display_name)
        self.assertIsNone(info.description)


class TestColumnInfoFromApiResponse(unittest.TestCase):
    """Tests for ColumnInfo.from_api_response factory."""

    def test_from_api_response(self):
        raw = {
            "SchemaName": "new_Price",
            "LogicalName": "new_price",
            "AttributeTypeName": {"Value": "DecimalType"},
            "IsPrimaryName": False,
            "RequiredLevel": {"Value": "None"},
            "MaxLength": None,
            "DisplayName": {"UserLocalizedLabel": {"Label": "Price"}},
            "Description": {"UserLocalizedLabel": {"Label": "Product price"}},
        }
        col = ColumnInfo.from_api_response(raw)
        self.assertEqual(col.schema_name, "new_Price")
        self.assertEqual(col.logical_name, "new_price")
        self.assertEqual(col.type, "DecimalType")
        self.assertFalse(col.is_primary)
        self.assertFalse(col.is_required)
        self.assertEqual(col.display_name, "Price")
        self.assertEqual(col.description, "Product price")

    def test_required_level_not_none(self):
        raw = {
            "SchemaName": "name",
            "LogicalName": "name",
            "AttributeTypeName": {"Value": "StringType"},
            "RequiredLevel": {"Value": "ApplicationRequired"},
        }
        col = ColumnInfo.from_api_response(raw)
        self.assertTrue(col.is_required)

    def test_missing_nested_labels(self):
        raw = {"SchemaName": "x", "LogicalName": "x"}
        col = ColumnInfo.from_api_response(raw)
        self.assertIsNone(col.display_name)
        self.assertIsNone(col.description)


if __name__ == "__main__":
    unittest.main()
