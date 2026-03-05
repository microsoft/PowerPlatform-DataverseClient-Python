# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.models.relationship import RelationshipInfo
from PowerPlatform.Dataverse.models.table_info import TableInfo
from PowerPlatform.Dataverse.operations.tables import TableOperations


class TestTableOperations(unittest.TestCase):
    """Unit tests for the client.tables namespace (TableOperations)."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.client = DataverseClient("https://example.crm.dynamics.com", self.mock_credential)
        self.client._odata = MagicMock()

    # ---------------------------------------------------------------- namespace

    def test_namespace_exists(self):
        """The client.tables attribute should be a TableOperations instance."""
        self.assertIsInstance(self.client.tables, TableOperations)

    # ------------------------------------------------------------------ create

    def test_create(self):
        """create() should return TableInfo with dict-like backward compat."""
        raw = {
            "table_schema_name": "new_Product",
            "entity_set_name": "new_products",
            "table_logical_name": "new_product",
            "metadata_id": "meta-guid-1",
            "columns_created": ["new_Price", "new_InStock"],
        }
        self.client._odata._create_table.return_value = raw

        columns = {"new_Price": "decimal", "new_InStock": "bool"}
        result = self.client.tables.create(
            "new_Product",
            columns,
            solution="MySolution",
            primary_column="new_ProductName",
        )

        self.client._odata._create_table.assert_called_once_with(
            "new_Product",
            columns,
            "MySolution",
            "new_ProductName",
        )
        self.assertIsInstance(result, TableInfo)
        self.assertEqual(result.schema_name, "new_Product")
        self.assertEqual(result["table_schema_name"], "new_Product")
        self.assertEqual(result["entity_set_name"], "new_products")

    # ------------------------------------------------------------------ delete

    def test_delete(self):
        """delete() should call _delete_table with the table schema name."""
        self.client.tables.delete("new_Product")

        self.client._odata._delete_table.assert_called_once_with("new_Product")

    # --------------------------------------------------------------------- get

    def test_get(self):
        """get() should return TableInfo with dict-like backward compat."""
        raw = {
            "table_schema_name": "new_Product",
            "table_logical_name": "new_product",
            "entity_set_name": "new_products",
            "metadata_id": "meta-guid-1",
        }
        self.client._odata._get_table_info.return_value = raw

        result = self.client.tables.get("new_Product")

        self.client._odata._get_table_info.assert_called_once_with("new_Product")
        self.assertIsInstance(result, TableInfo)
        self.assertEqual(result.schema_name, "new_Product")
        self.assertEqual(result["table_schema_name"], "new_Product")

    def test_get_returns_none(self):
        """get() should return None when _get_table_info returns None (table not found)."""
        self.client._odata._get_table_info.return_value = None

        result = self.client.tables.get("nonexistent_Table")

        self.client._odata._get_table_info.assert_called_once_with("nonexistent_Table")
        self.assertIsNone(result)

    # ------------------------------------------------------------------- list

    def test_list(self):
        """list() should call _list_tables and return the list of metadata dicts."""
        expected_tables = [
            {"LogicalName": "account", "SchemaName": "Account"},
            {"LogicalName": "contact", "SchemaName": "Contact"},
        ]
        self.client._odata._list_tables.return_value = expected_tables

        result = self.client.tables.list()

        self.client._odata._list_tables.assert_called_once_with(filter=None, select=None)
        self.assertIsInstance(result, list)
        self.assertEqual(result, expected_tables)

    def test_list_with_filter(self):
        """list(filter=...) should pass the filter expression to _list_tables."""
        expected_tables = [
            {"LogicalName": "account", "SchemaName": "Account"},
        ]
        self.client._odata._list_tables.return_value = expected_tables

        result = self.client.tables.list(filter="SchemaName eq 'Account'")

        self.client._odata._list_tables.assert_called_once_with(filter="SchemaName eq 'Account'", select=None)
        self.assertIsInstance(result, list)
        self.assertEqual(result, expected_tables)

    def test_list_with_filter_none_explicit(self):
        """list(filter=None) should behave identically to list() with no args."""
        expected_tables = [
            {"LogicalName": "account", "SchemaName": "Account"},
        ]
        self.client._odata._list_tables.return_value = expected_tables

        result = self.client.tables.list(filter=None)

        self.client._odata._list_tables.assert_called_once_with(filter=None, select=None)
        self.assertEqual(result, expected_tables)

    def test_list_with_select(self):
        """list(select=...) should pass the select list to _list_tables."""
        expected_tables = [
            {"LogicalName": "account", "SchemaName": "Account"},
        ]
        self.client._odata._list_tables.return_value = expected_tables

        result = self.client.tables.list(select=["LogicalName", "SchemaName", "EntitySetName"])

        self.client._odata._list_tables.assert_called_once_with(
            filter=None,
            select=["LogicalName", "SchemaName", "EntitySetName"],
        )
        self.assertEqual(result, expected_tables)

    def test_list_with_select_none_explicit(self):
        """list(select=None) should behave identically to list() with no args."""
        expected_tables = [
            {"LogicalName": "account", "SchemaName": "Account"},
        ]
        self.client._odata._list_tables.return_value = expected_tables

        result = self.client.tables.list(select=None)

        self.client._odata._list_tables.assert_called_once_with(filter=None, select=None)
        self.assertEqual(result, expected_tables)

    def test_list_with_filter_and_select(self):
        """list(filter=..., select=...) should pass both params to _list_tables."""
        expected_tables = [
            {"LogicalName": "account", "SchemaName": "Account"},
        ]
        self.client._odata._list_tables.return_value = expected_tables

        result = self.client.tables.list(
            filter="SchemaName eq 'Account'",
            select=["LogicalName", "SchemaName"],
        )

        self.client._odata._list_tables.assert_called_once_with(
            filter="SchemaName eq 'Account'",
            select=["LogicalName", "SchemaName"],
        )
        self.assertEqual(result, expected_tables)

    # ------------------------------------------------------------ add_columns

    def test_add_columns(self):
        """add_columns() should call _create_columns with correct args."""
        self.client._odata._create_columns.return_value = ["new_Notes", "new_Active"]

        columns = {"new_Notes": "string", "new_Active": "bool"}
        result = self.client.tables.add_columns("new_Product", columns)

        self.client._odata._create_columns.assert_called_once_with("new_Product", columns)
        self.assertEqual(result, ["new_Notes", "new_Active"])

    # --------------------------------------------------------- remove_columns

    def test_remove_columns_single(self):
        """remove_columns() with a single string should pass it through to _delete_columns."""
        self.client._odata._delete_columns.return_value = ["new_Notes"]

        result = self.client.tables.remove_columns("new_Product", "new_Notes")

        self.client._odata._delete_columns.assert_called_once_with("new_Product", "new_Notes")
        self.assertEqual(result, ["new_Notes"])

    def test_remove_columns_list(self):
        """remove_columns() with a list of strings should pass it through to _delete_columns."""
        self.client._odata._delete_columns.return_value = ["new_Notes", "new_Active"]

        result = self.client.tables.remove_columns("new_Product", ["new_Notes", "new_Active"])

        self.client._odata._delete_columns.assert_called_once_with("new_Product", ["new_Notes", "new_Active"])
        self.assertEqual(result, ["new_Notes", "new_Active"])

    # ---------------------------------------------------- create_one_to_many

    def test_create_one_to_many(self):
        """create_one_to_many() should return RelationshipInfo."""
        raw = {
            "relationship_id": "rel-guid-1",
            "relationship_schema_name": "new_Dept_Emp",
            "lookup_schema_name": "new_DeptId",
            "referenced_entity": "new_department",
            "referencing_entity": "new_employee",
        }
        self.client._odata._create_one_to_many_relationship.return_value = raw

        lookup = MagicMock()
        relationship = MagicMock()
        result = self.client.tables.create_one_to_many_relationship(lookup, relationship, solution="MySolution")

        self.client._odata._create_one_to_many_relationship.assert_called_once_with(lookup, relationship, "MySolution")
        self.assertIsInstance(result, RelationshipInfo)
        self.assertEqual(result.relationship_id, "rel-guid-1")
        self.assertEqual(result.relationship_schema_name, "new_Dept_Emp")
        self.assertEqual(result.lookup_schema_name, "new_DeptId")
        self.assertEqual(result.referenced_entity, "new_department")
        self.assertEqual(result.referencing_entity, "new_employee")
        self.assertEqual(result.relationship_type, "one_to_many")

    # --------------------------------------------------- create_many_to_many

    def test_create_many_to_many(self):
        """create_many_to_many() should return RelationshipInfo."""
        raw = {
            "relationship_id": "rel-guid-2",
            "relationship_schema_name": "new_emp_proj",
            "entity1_logical_name": "new_employee",
            "entity2_logical_name": "new_project",
        }
        self.client._odata._create_many_to_many_relationship.return_value = raw

        relationship = MagicMock()
        result = self.client.tables.create_many_to_many_relationship(relationship, solution="MySolution")

        self.client._odata._create_many_to_many_relationship.assert_called_once_with(relationship, "MySolution")
        self.assertIsInstance(result, RelationshipInfo)
        self.assertEqual(result.relationship_id, "rel-guid-2")
        self.assertEqual(result.relationship_schema_name, "new_emp_proj")
        self.assertEqual(result.entity1_logical_name, "new_employee")
        self.assertEqual(result.entity2_logical_name, "new_project")
        self.assertEqual(result.relationship_type, "many_to_many")

    # ----------------------------------------------------- delete_relationship

    def test_delete_relationship(self):
        """delete_relationship() should call _delete_relationship."""
        self.client.tables.delete_relationship("rel-guid-1")

        self.client._odata._delete_relationship.assert_called_once_with("rel-guid-1")

    # ------------------------------------------------------- get_relationship

    def test_get_relationship(self):
        """get_relationship() should return RelationshipInfo from API response."""
        raw = {
            "@odata.type": "#Microsoft.Dynamics.CRM.OneToManyRelationshipMetadata",
            "SchemaName": "new_Dept_Emp",
            "MetadataId": "rel-guid-1",
            "ReferencedEntity": "new_department",
            "ReferencingEntity": "new_employee",
            "ReferencingEntityNavigationPropertyName": "new_DeptId",
        }
        self.client._odata._get_relationship.return_value = raw

        result = self.client.tables.get_relationship("new_Dept_Emp")

        self.client._odata._get_relationship.assert_called_once_with("new_Dept_Emp")
        self.assertIsInstance(result, RelationshipInfo)
        self.assertEqual(result.relationship_schema_name, "new_Dept_Emp")
        self.assertEqual(result.relationship_id, "rel-guid-1")
        self.assertEqual(result.relationship_type, "one_to_many")

    def test_get_relationship_returns_none(self):
        """get_relationship() should return None when not found."""
        self.client._odata._get_relationship.return_value = None

        result = self.client.tables.get_relationship("nonexistent")

        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
