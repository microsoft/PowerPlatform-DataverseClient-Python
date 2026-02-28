# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.models.metadata import ColumnMetadata, OptionSetInfo
from PowerPlatform.Dataverse.models.relationship import RelationshipInfo
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
        """create() should call _create_table with correct positional args including renamed kwargs."""
        expected_result = {
            "table_schema_name": "new_Product",
            "entity_set_name": "new_products",
            "table_logical_name": "new_product",
            "metadata_id": "meta-guid-1",
            "columns_created": ["new_Price", "new_InStock"],
        }
        self.client._odata._create_table.return_value = expected_result

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
        self.assertEqual(result, expected_result)

    # ------------------------------------------------------------------ delete

    def test_delete(self):
        """delete() should call _delete_table with the table schema name."""
        self.client.tables.delete("new_Product")

        self.client._odata._delete_table.assert_called_once_with("new_Product")

    # --------------------------------------------------------------------- get

    def test_get(self):
        """get() should call _get_table_info and return the metadata dict."""
        expected_info = {
            "table_schema_name": "new_Product",
            "table_logical_name": "new_product",
            "entity_set_name": "new_products",
            "metadata_id": "meta-guid-1",
        }
        self.client._odata._get_table_info.return_value = expected_info

        result = self.client.tables.get("new_Product")

        self.client._odata._get_table_info.assert_called_once_with("new_Product")
        self.assertEqual(result, expected_info)

    def test_get_returns_none(self):
        """get() should return None when _get_table_info returns None (table not found)."""
        self.client._odata._get_table_info.return_value = None

        result = self.client.tables.get("nonexistent_Table")

        self.client._odata._get_table_info.assert_called_once_with("nonexistent_Table")
        self.assertIsNone(result)

    def test_get_basic_unchanged(self):
        """get() with no extra args should use _get_table_info (backward compatibility)."""
        expected_info = {
            "table_schema_name": "account",
            "table_logical_name": "account",
            "entity_set_name": "accounts",
            "metadata_id": "meta-guid-1",
        }
        self.client._odata._get_table_info.return_value = expected_info

        result = self.client.tables.get("account")

        self.client._odata._get_table_info.assert_called_once_with("account")
        self.client._odata._get_table_metadata.assert_not_called()
        self.assertEqual(result, expected_info)

    def test_get_with_include_columns(self):
        """get(include_columns=True) should call _get_table_metadata and return columns."""
        raw = {
            "SchemaName": "Account",
            "LogicalName": "account",
            "EntitySetName": "accounts",
            "MetadataId": "meta-guid",
            "Attributes": [
                {"LogicalName": "name", "SchemaName": "Name", "AttributeType": "String"},
            ],
        }
        self.client._odata._get_table_metadata.return_value = raw

        result = self.client.tables.get("account", include_columns=True)

        self.client._odata._get_table_metadata.assert_called_once_with(
            "account",
            select=None,
            include_attributes=True,
            include_one_to_many=False,
            include_many_to_one=False,
            include_many_to_many=False,
        )
        self.assertIn("columns", result)
        self.assertEqual(len(result["columns"]), 1)
        self.assertIsInstance(result["columns"][0], ColumnMetadata)
        self.assertEqual(result["columns"][0].logical_name, "name")
        self.assertEqual(result["columns"][0].attribute_type, "String")

    def test_get_with_include_relationships(self):
        """get(include_relationships=True) should return relationship arrays."""
        raw = {
            "SchemaName": "Account",
            "LogicalName": "account",
            "EntitySetName": "accounts",
            "MetadataId": "meta-guid",
            "OneToManyRelationships": [{"SchemaName": "account_tasks", "ReferencingEntity": "task"}],
            "ManyToOneRelationships": [],
            "ManyToManyRelationships": [],
        }
        self.client._odata._get_table_metadata.return_value = raw

        result = self.client.tables.get("account", include_relationships=True)

        self.client._odata._get_table_metadata.assert_called_once_with(
            "account",
            select=None,
            include_attributes=False,
            include_one_to_many=True,
            include_many_to_one=True,
            include_many_to_many=True,
        )
        self.assertIn("one_to_many_relationships", result)
        self.assertEqual(len(result["one_to_many_relationships"]), 1)
        self.assertEqual(result["one_to_many_relationships"][0]["SchemaName"], "account_tasks")
        self.assertIn("many_to_one_relationships", result)
        self.assertIn("many_to_many_relationships", result)

    def test_get_with_select(self):
        """get(select=[...]) should pass select and include extra properties in result."""
        raw = {
            "SchemaName": "Account",
            "LogicalName": "account",
            "EntitySetName": "accounts",
            "MetadataId": "meta-guid",
            "DisplayName": {"UserLocalizedLabel": {"Label": "Account"}},
            "Description": {"UserLocalizedLabel": {"Label": "Business account"}},
        }
        self.client._odata._get_table_metadata.return_value = raw

        result = self.client.tables.get("account", select=["DisplayName", "Description"])

        self.client._odata._get_table_metadata.assert_called_once_with(
            "account",
            select=["DisplayName", "Description"],
            include_attributes=False,
            include_one_to_many=False,
            include_many_to_one=False,
            include_many_to_many=False,
        )
        self.assertIn("DisplayName", result)
        self.assertIn("Description", result)

    def test_get_extended_returns_none(self):
        """get(include_columns=True) should return None when table not found."""
        self.client._odata._get_table_metadata.return_value = None

        result = self.client.tables.get("nonexistent", include_columns=True)

        self.assertIsNone(result)

    def test_get_columns(self):
        """get_columns() should return list of ColumnMetadata."""
        raw_list = [
            {"LogicalName": "name", "SchemaName": "Name", "AttributeType": "String"},
            {"LogicalName": "emailaddress1", "SchemaName": "EMailAddress1", "AttributeType": "String"},
        ]
        self.client._odata._get_table_columns.return_value = raw_list

        result = self.client.tables.get_columns("account")

        self.client._odata._get_table_columns.assert_called_once_with(
            "account",
            select=None,
            filter=None,
        )
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], ColumnMetadata)
        self.assertIsInstance(result[1], ColumnMetadata)
        self.assertEqual(result[0].logical_name, "name")
        self.assertEqual(result[1].logical_name, "emailaddress1")

    def test_get_columns_with_filter(self):
        """get_columns(filter=...) should pass filter to _get_table_columns."""
        filter_expr = "AttributeType eq Microsoft.Dynamics.CRM.AttributeTypeCode'Picklist'"
        self.client._odata._get_table_columns.return_value = []

        self.client.tables.get_columns("account", filter=filter_expr)

        self.client._odata._get_table_columns.assert_called_once_with(
            "account",
            select=None,
            filter=filter_expr,
        )

    def test_get_column_found(self):
        """get_column() should return ColumnMetadata when column exists."""
        raw = {"LogicalName": "emailaddress1", "SchemaName": "EMailAddress1", "AttributeType": "String"}
        self.client._odata._get_table_column.return_value = raw

        result = self.client.tables.get_column("account", "emailaddress1")

        self.client._odata._get_table_column.assert_called_once_with(
            "account",
            "emailaddress1",
            select=None,
        )
        self.assertIsInstance(result, ColumnMetadata)
        self.assertEqual(result.logical_name, "emailaddress1")

    def test_get_column_not_found(self):
        """get_column() should return None when column not found."""
        self.client._odata._get_table_column.return_value = None

        result = self.client.tables.get_column("account", "nonexistent_col")

        self.assertIsNone(result)

    def test_get_column_options_picklist(self):
        """get_column_options() should return OptionSetInfo for picklist column."""
        raw_optionset = {
            "Name": "account_accountcategorycode",
            "OptionSetType": "Picklist",
            "Options": [
                {"Value": 1, "Label": {"UserLocalizedLabel": {"Label": "Preferred Customer"}}},
                {"Value": 2, "Label": {"UserLocalizedLabel": {"Label": "Standard"}}},
            ],
        }
        self.client._odata._get_column_optionset.return_value = raw_optionset

        result = self.client.tables.get_column_options("account", "accountcategorycode")

        self.client._odata._get_column_optionset.assert_called_once_with("account", "accountcategorycode")
        self.assertIsInstance(result, OptionSetInfo)
        self.assertEqual(len(result.options), 2)
        self.assertEqual(result.options[0].value, 1)
        self.assertEqual(result.options[0].label, "Preferred Customer")

    def test_get_column_options_not_picklist(self):
        """get_column_options() should return None for non-choice column."""
        self.client._odata._get_column_optionset.return_value = None

        result = self.client.tables.get_column_options("account", "name")

        self.assertIsNone(result)

    def test_list_relationships_all(self):
        """list_relationships() with no type should return all relationship types."""
        expected = [
            {"SchemaName": "account_tasks", "_relationship_type": "OneToMany"},
            {"SchemaName": "account_primarycontact", "_relationship_type": "ManyToOne"},
        ]
        self.client._odata._list_table_relationships.return_value = expected

        result = self.client.tables.list_relationships("account")

        self.client._odata._list_table_relationships.assert_called_once_with(
            "account",
            relationship_type=None,
            select=None,
        )
        self.assertEqual(result, expected)

    def test_list_relationships_filtered(self):
        """list_relationships(relationship_type=...) should pass type filter."""
        expected = [{"SchemaName": "account_tasks", "_relationship_type": "OneToMany"}]
        self.client._odata._list_table_relationships.return_value = expected

        result = self.client.tables.list_relationships("account", relationship_type="one_to_many")

        self.client._odata._list_table_relationships.assert_called_once_with(
            "account",
            relationship_type="one_to_many",
            select=None,
        )
        self.assertEqual(result, expected)

    def test_get_select_bare_string_raises(self):
        """get() with select as bare string should raise TypeError."""
        self.client._odata._get_table_metadata.side_effect = TypeError(
            "select must be a list of property names, not a bare string"
        )
        with self.assertRaises(TypeError):
            self.client.tables.get("account", select="DisplayName")

    def test_get_columns_select_bare_string_raises(self):
        """get_columns() with select as bare string should raise TypeError."""
        self.client._odata._get_table_columns.side_effect = TypeError(
            "select must be a list of property names, not a bare string"
        )
        with self.assertRaises(TypeError):
            self.client.tables.get_columns("account", select="LogicalName")

    def test_get_column_select_bare_string_raises(self):
        """get_column() with select as bare string should raise TypeError."""
        self.client._odata._get_table_column.side_effect = TypeError(
            "select must be a list of property names, not a bare string"
        )
        with self.assertRaises(TypeError):
            self.client.tables.get_column("account", "name", select="LogicalName")

    def test_list_relationships_select_bare_string_raises(self):
        """list_relationships() should raise TypeError on bare string select."""
        self.client._odata._list_table_relationships.side_effect = TypeError(
            "select must be a list of property names, not a bare string"
        )
        with self.assertRaises(TypeError):
            self.client.tables.list_relationships("account", select="SchemaName")

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
