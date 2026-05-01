# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.models.table_info import AlternateKeyInfo, ColumnInfo, OptionSetInfo, TableInfo
from PowerPlatform.Dataverse.models.relationship import RelationshipInfo
from PowerPlatform.Dataverse.operations.tables import TableOperations
from tests.fixtures.test_data import (
    ACCOUNT_CHATS_RELATIONSHIP,
    ACCOUNT_NAME_COLUMN,
    ACCOUNT_TABLE_ENTRY,
    ACCOUNT_TABLE_FULL,
    BOOLEAN_OPTIONSET,
    CONTACT_TABLE_ENTRY,
    EMAILADDRESS1_COLUMN,
    PICKLIST_OPTIONSET,
    STATE_OPTIONSET,
    STATUS_OPTIONSET,
)


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
            None,
        )
        self.assertIsInstance(result, TableInfo)
        self.assertEqual(result.schema_name, "new_Product")
        self.assertEqual(result["table_schema_name"], "new_Product")
        self.assertEqual(result["entity_set_name"], "new_products")

    def test_create_with_display_name(self):
        """create() should forward display_name to _create_table."""
        raw = {
            "table_schema_name": "new_Product",
            "entity_set_name": "new_products",
            "table_logical_name": "new_product",
            "metadata_id": "meta-guid-1",
            "columns_created": [],
        }
        self.client._odata._create_table.return_value = raw

        self.client.tables.create("new_Product", {}, display_name="Product")

        self.client._odata._create_table.assert_called_once_with(
            "new_Product",
            {},
            None,
            None,
            "Product",
        )

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
        self.assertIsInstance(result, TableInfo)
        self.assertEqual(result.schema_name, "account")
        self.assertEqual(result["table_schema_name"], "account")
        self.assertEqual(result["entity_set_name"], "accounts")

    def test_get_with_include_columns(self):
        """get(include_columns=True) should call _get_table_metadata and return columns."""
        raw = {
            "SchemaName": "Account",
            "LogicalName": "account",
            "EntitySetName": "accounts",
            "MetadataId": "70816501-edb9-4740-a16c-6a5efbc05d84",
            "Attributes": [ACCOUNT_NAME_COLUMN],
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
        self.assertNotIn("columns_created", result)
        self.assertEqual(len(result["columns"]), 1)
        col = result["columns"][0]
        self.assertIsInstance(col, ColumnInfo)
        self.assertEqual(col.logical_name, "name")
        self.assertEqual(col.schema_name, "Name")
        self.assertEqual(col.attribute_type, "String")
        self.assertEqual(col.attribute_type_name, "StringType")
        self.assertTrue(col.is_primary_name)
        self.assertFalse(col.is_primary_id)
        self.assertEqual(col.display_name, "Account Name")
        self.assertEqual(col.required_level, "ApplicationRequired")

    def test_get_with_include_relationships(self):
        """get(include_relationships=True) should return relationship arrays."""
        raw = {
            "SchemaName": "Account",
            "LogicalName": "account",
            "EntitySetName": "accounts",
            "MetadataId": "70816501-edb9-4740-a16c-6a5efbc05d84",
            "OneToManyRelationships": [ACCOUNT_CHATS_RELATIONSHIP],
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
        rel0 = result["one_to_many_relationships"][0]
        self.assertEqual(rel0["SchemaName"], "account_chats")
        self.assertEqual(rel0["ReferencedEntity"], "account")
        self.assertEqual(rel0["ReferencingEntity"], "chat")
        self.assertEqual(rel0["RelationshipType"], "OneToManyRelationship")
        self.assertIn("many_to_one_relationships", result)
        self.assertIn("many_to_many_relationships", result)

    def test_get_with_select(self):
        """get(select=[...]) should pass select and include extra properties in result."""
        self.client._odata._get_table_metadata.return_value = ACCOUNT_TABLE_FULL

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
        """get_columns() should return list of ColumnInfo."""
        raw_list = [ACCOUNT_NAME_COLUMN, EMAILADDRESS1_COLUMN]
        self.client._odata._get_table_columns.return_value = raw_list

        result = self.client.tables.get_columns("account")

        self.client._odata._get_table_columns.assert_called_once_with(
            "account",
            select=None,
            filter=None,
        )
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], ColumnInfo)
        self.assertIsInstance(result[1], ColumnInfo)
        self.assertEqual(result[0].logical_name, "name")
        self.assertEqual(result[0].display_name, "Account Name")
        self.assertEqual(result[0].required_level, "ApplicationRequired")
        self.assertTrue(result[0].is_primary_name)
        self.assertEqual(result[1].logical_name, "emailaddress1")
        self.assertEqual(result[1].display_name, "Email")
        self.assertEqual(result[1].required_level, "None")

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
        """get_column() should return ColumnInfo when column exists."""
        self.client._odata._get_table_column.return_value = EMAILADDRESS1_COLUMN

        result = self.client.tables.get_column("account", "emailaddress1")

        self.client._odata._get_table_column.assert_called_once_with(
            "account",
            "emailaddress1",
            select=None,
        )
        self.assertIsInstance(result, ColumnInfo)
        self.assertEqual(result.logical_name, "emailaddress1")
        self.assertEqual(result.schema_name, "EMailAddress1")
        self.assertEqual(result.display_name, "Email")
        self.assertEqual(result.attribute_type, "String")
        self.assertEqual(result.attribute_type_name, "StringType")
        self.assertEqual(result.required_level, "None")
        self.assertFalse(result.is_primary_name)
        self.assertFalse(result.is_primary_id)
        self.assertEqual(result.metadata_id, "024a2ee3-b983-4fd8-8991-f8d548a227e0")

    def test_get_column_not_found(self):
        """get_column() should return None when column not found."""
        self.client._odata._get_table_column.return_value = None

        result = self.client.tables.get_column("account", "nonexistent_col")

        self.assertIsNone(result)

    def test_get_column_options_picklist(self):
        """get_column_options() should return OptionSetInfo for picklist column."""
        self.client._odata._get_column_optionset.return_value = PICKLIST_OPTIONSET

        result = self.client.tables.get_column_options("account", "accountcategorycode")

        self.client._odata._get_column_optionset.assert_called_once_with("account", "accountcategorycode")
        self.assertIsInstance(result, OptionSetInfo)
        self.assertEqual(result.name, "account_accountcategorycode")
        self.assertEqual(result.display_name, "Category")
        self.assertEqual(result.option_set_type, "Picklist")
        self.assertFalse(result.is_global)
        self.assertEqual(result.metadata_id, "b994cdd8-5ce9-4ab9-bdd3-8888ebdb0407")
        self.assertEqual(len(result.options), 2)
        self.assertEqual(result.options[0].value, 1)
        self.assertEqual(result.options[0].label, "Preferred Customer")
        self.assertEqual(result.options[1].value, 2)
        self.assertEqual(result.options[1].label, "Standard")

    def test_get_column_options_status(self):
        """get_column_options() should return OptionSetInfo for Status column."""
        self.client._odata._get_column_optionset.return_value = STATUS_OPTIONSET

        result = self.client.tables.get_column_options("account", "statuscode")

        self.client._odata._get_column_optionset.assert_called_once_with("account", "statuscode")
        self.assertIsInstance(result, OptionSetInfo)
        self.assertEqual(result.name, "account_statuscode")
        self.assertEqual(result.display_name, "Status Reason")
        self.assertEqual(result.option_set_type, "Status")
        self.assertEqual(len(result.options), 2)
        self.assertEqual(result.options[0].value, 1)
        self.assertEqual(result.options[0].label, "Active")
        self.assertEqual(result.options[1].value, 2)
        self.assertEqual(result.options[1].label, "Inactive")

    def test_get_column_options_state(self):
        """get_column_options() should return OptionSetInfo for State column."""
        self.client._odata._get_column_optionset.return_value = STATE_OPTIONSET

        result = self.client.tables.get_column_options("contact", "statecode")

        self.client._odata._get_column_optionset.assert_called_once_with("contact", "statecode")
        self.assertIsInstance(result, OptionSetInfo)
        self.assertEqual(result.name, "contact_statecode")
        self.assertEqual(result.display_name, "Status")
        self.assertEqual(result.option_set_type, "State")
        self.assertEqual(len(result.options), 2)
        values = [o.value for o in result.options]
        self.assertIn(0, values)
        self.assertIn(1, values)
        labels = {o.value: o.label for o in result.options}
        self.assertEqual(labels[0], "Active")
        self.assertEqual(labels[1], "Inactive")

    def test_get_column_options_boolean(self):
        """get_column_options() should return OptionSetInfo for Boolean column."""
        self.client._odata._get_column_optionset.return_value = BOOLEAN_OPTIONSET

        result = self.client.tables.get_column_options("contact", "donotphone")

        self.client._odata._get_column_optionset.assert_called_once_with("contact", "donotphone")
        self.assertIsInstance(result, OptionSetInfo)
        self.assertEqual(result.name, "contact_donotphone")
        self.assertEqual(result.display_name, "Do not allow Phone Calls")
        self.assertEqual(result.option_set_type, "Boolean")
        self.assertEqual(len(result.options), 2)
        values = {o.value: o.label for o in result.options}
        self.assertEqual(values[0], "Allow")
        self.assertEqual(values[1], "Do Not Allow")

    def test_get_column_options_not_picklist(self):
        """get_column_options() should return None for non-choice column."""
        self.client._odata._get_column_optionset.return_value = None

        result = self.client.tables.get_column_options("account", "name")

        self.assertIsNone(result)

    def test_list_relationships_all(self):
        """list_relationships() with no type should return all relationship types."""
        expected = [
            {**ACCOUNT_CHATS_RELATIONSHIP, "_relationship_type": "OneToMany"},
            {
                "MetadataId": "2074fc1d-84a2-48ac-a47c-fcf1d249a052",
                "SchemaName": "lk_accountbase_modifiedonbehalfby",
                "ReferencedAttribute": "systemuserid",
                "ReferencedEntity": "systemuser",
                "ReferencingAttribute": "modifiedonbehalfby",
                "ReferencingEntity": "account",
                "RelationshipType": "OneToManyRelationship",
                "IsCustomRelationship": False,
                "IsManaged": True,
                "CascadeConfiguration": {
                    "Assign": "NoCascade",
                    "Delete": "NoCascade",
                    "Merge": "NoCascade",
                    "Reparent": "NoCascade",
                    "Share": "NoCascade",
                    "Unshare": "NoCascade",
                },
                "_relationship_type": "ManyToOne",
            },
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
        expected = [{**ACCOUNT_CHATS_RELATIONSHIP, "_relationship_type": "OneToMany"}]
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
            {
                **ACCOUNT_TABLE_ENTRY,
                "DisplayName": {
                    "LocalizedLabels": [{"Label": "Account", "LanguageCode": 1033, "IsManaged": True}],
                    "UserLocalizedLabel": {"Label": "Account", "LanguageCode": 1033, "IsManaged": True},
                },
            },
            {
                **CONTACT_TABLE_ENTRY,
                "DisplayName": {
                    "LocalizedLabels": [{"Label": "Contact", "LanguageCode": 1033, "IsManaged": True}],
                    "UserLocalizedLabel": {"Label": "Contact", "LanguageCode": 1033, "IsManaged": True},
                },
            },
        ]
        self.client._odata._list_tables.return_value = expected_tables

        result = self.client.tables.list()

        self.client._odata._list_tables.assert_called_once_with(filter=None, select=None)
        self.assertIsInstance(result, list)
        self.assertEqual(result, expected_tables)

    def test_list_with_filter(self):
        """list(filter=...) should pass the filter expression to _list_tables."""
        expected_tables = [ACCOUNT_TABLE_ENTRY]
        self.client._odata._list_tables.return_value = expected_tables

        result = self.client.tables.list(filter="SchemaName eq 'Account'")

        self.client._odata._list_tables.assert_called_once_with(filter="SchemaName eq 'Account'", select=None)
        self.assertIsInstance(result, list)
        self.assertEqual(result, expected_tables)

    def test_list_with_filter_none_explicit(self):
        """list(filter=None) should behave identically to list() with no args."""
        expected_tables = [ACCOUNT_TABLE_ENTRY]
        self.client._odata._list_tables.return_value = expected_tables

        result = self.client.tables.list(filter=None)

        self.client._odata._list_tables.assert_called_once_with(filter=None, select=None)
        self.assertEqual(result, expected_tables)

    def test_list_with_select(self):
        """list(select=...) should pass the select list to _list_tables."""
        expected_tables = [ACCOUNT_TABLE_ENTRY]
        self.client._odata._list_tables.return_value = expected_tables

        result = self.client.tables.list(select=["LogicalName", "SchemaName", "EntitySetName"])

        self.client._odata._list_tables.assert_called_once_with(
            filter=None,
            select=["LogicalName", "SchemaName", "EntitySetName"],
        )
        self.assertEqual(result, expected_tables)

    def test_list_with_select_none_explicit(self):
        """list(select=None) should behave identically to list() with no args."""
        expected_tables = [ACCOUNT_TABLE_ENTRY]
        self.client._odata._list_tables.return_value = expected_tables

        result = self.client.tables.list(select=None)

        self.client._odata._list_tables.assert_called_once_with(filter=None, select=None)
        self.assertEqual(result, expected_tables)

    def test_list_with_filter_and_select(self):
        """list(filter=..., select=...) should pass both params to _list_tables."""
        expected_tables = [
            {
                "MetadataId": "70816501-edb9-4740-a16c-6a5efbc05d84",
                "LogicalName": "account",
                "SchemaName": "Account",
            },
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

    def test_add_columns_memo(self):
        """add_columns() with memo type should pass through correctly."""
        self.client._odata._create_columns.return_value = ["new_Description"]

        columns = {"new_Description": "memo"}
        result = self.client.tables.add_columns("new_Product", columns)

        self.client._odata._create_columns.assert_called_once_with("new_Product", columns)
        self.assertEqual(result, ["new_Description"])

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

    # ------------------------------------------------ create_alternate_key

    def test_create_alternate_key(self):
        """create_alternate_key() should call OData layer and return AlternateKeyInfo."""
        raw = {
            "metadata_id": "key-guid-1",
            "schema_name": "new_product_code_key",
            "key_attributes": ["new_productcode"],
        }
        self.client._odata._create_alternate_key.return_value = raw

        result = self.client.tables.create_alternate_key(
            "new_Product",
            "new_product_code_key",
            ["new_productcode"],
        )

        self.client._odata._create_alternate_key.assert_called_once()
        call_args = self.client._odata._create_alternate_key.call_args
        self.assertEqual(call_args[0][0], "new_Product")
        self.assertEqual(call_args[0][1], "new_product_code_key")
        self.assertEqual(call_args[0][2], ["new_productcode"])
        # 4th arg is a Label object for the display name
        self.assertIsNotNone(call_args[0][3])
        self.assertIsInstance(result, AlternateKeyInfo)
        self.assertEqual(result.metadata_id, "key-guid-1")
        self.assertEqual(result.schema_name, "new_product_code_key")
        self.assertEqual(result.key_attributes, ["new_productcode"])
        self.assertEqual(result.status, "Pending")

    def test_create_alternate_key_multi_column(self):
        """create_alternate_key() should handle multi-column keys."""
        raw = {
            "metadata_id": "key-guid-2",
            "schema_name": "new_composite_key",
            "key_attributes": ["new_col1", "new_col2"],
        }
        self.client._odata._create_alternate_key.return_value = raw

        result = self.client.tables.create_alternate_key(
            "new_Product",
            "new_composite_key",
            ["new_col1", "new_col2"],
        )

        self.assertIsInstance(result, AlternateKeyInfo)
        self.assertEqual(result.key_attributes, ["new_col1", "new_col2"])

    # -------------------------------------------------- get_alternate_keys

    def test_get_alternate_keys(self):
        """get_alternate_keys() should return list of AlternateKeyInfo."""
        raw_list = [
            {
                "MetadataId": "key-guid-1",
                "SchemaName": "new_product_code_key",
                "KeyAttributes": ["new_productcode"],
                "EntityKeyIndexStatus": "Active",
            },
            {
                "MetadataId": "key-guid-2",
                "SchemaName": "new_composite_key",
                "KeyAttributes": ["new_col1", "new_col2"],
                "EntityKeyIndexStatus": "Pending",
            },
        ]
        self.client._odata._get_alternate_keys.return_value = raw_list

        result = self.client.tables.get_alternate_keys("new_Product")

        self.client._odata._get_alternate_keys.assert_called_once_with("new_Product")
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 2)
        self.assertIsInstance(result[0], AlternateKeyInfo)
        self.assertEqual(result[0].metadata_id, "key-guid-1")
        self.assertEqual(result[0].schema_name, "new_product_code_key")
        self.assertEqual(result[0].key_attributes, ["new_productcode"])
        self.assertEqual(result[0].status, "Active")
        self.assertIsInstance(result[1], AlternateKeyInfo)
        self.assertEqual(result[1].metadata_id, "key-guid-2")
        self.assertEqual(result[1].status, "Pending")

    def test_get_alternate_keys_empty(self):
        """get_alternate_keys() should return empty list when no keys exist."""
        self.client._odata._get_alternate_keys.return_value = []

        result = self.client.tables.get_alternate_keys("new_Product")

        self.assertEqual(result, [])

    # ------------------------------------------------- delete_alternate_key

    def test_delete_alternate_key(self):
        """delete_alternate_key() should call OData layer with correct args."""
        self.client.tables.delete_alternate_key("new_Product", "key-guid-1")

        self.client._odata._delete_alternate_key.assert_called_once_with("new_Product", "key-guid-1")

    # -------------------------------------------------------- list_columns

    def test_list_columns(self):
        """list_columns() should delegate to _list_columns and return the list."""
        expected = [
            {"LogicalName": "name", "AttributeType": "String"},
            {"LogicalName": "accountid", "AttributeType": "Uniqueidentifier"},
        ]
        self.client._odata._list_columns.return_value = expected

        result = self.client.tables.list_columns("account")

        self.client._odata._list_columns.assert_called_once_with("account", select=None, filter=None)
        self.assertEqual(result, expected)

    def test_list_columns_with_select_and_filter(self):
        """list_columns() should forward select and filter to _list_columns."""
        self.client._odata._list_columns.return_value = []

        self.client.tables.list_columns(
            "account",
            select=["LogicalName", "AttributeType"],
            filter="AttributeType eq 'String'",
        )

        self.client._odata._list_columns.assert_called_once_with(
            "account",
            select=["LogicalName", "AttributeType"],
            filter="AttributeType eq 'String'",
        )

    # ------------------------------------------------- list_relationships

    def test_list_relationships(self):
        """list_relationships() should delegate to _list_relationships and return the list."""
        expected = [
            {"SchemaName": "new_account_orders", "MetadataId": "rel-1"},
        ]
        self.client._odata._list_relationships.return_value = expected

        result = self.client.tables.list_relationships()

        self.client._odata._list_relationships.assert_called_once_with(filter=None, select=None)
        self.assertEqual(result, expected)

    def test_list_relationships_with_filter_and_select(self):
        """list_relationships() should forward filter and select to _list_relationships."""
        self.client._odata._list_relationships.return_value = []

        self.client.tables.list_relationships(
            filter="RelationshipType eq 'OneToManyRelationship'",
            select=["SchemaName", "ReferencedEntity"],
        )

        self.client._odata._list_relationships.assert_called_once_with(
            filter="RelationshipType eq 'OneToManyRelationship'",
            select=["SchemaName", "ReferencedEntity"],
        )

    # --------------------------------------------- list_table_relationships

    def test_list_table_relationships(self):
        """list_table_relationships() should delegate to _list_table_relationships."""
        expected = [
            {"SchemaName": "rel_1tm", "MetadataId": "r1"},
            {"SchemaName": "rel_mtm", "MetadataId": "r2"},
        ]
        self.client._odata._list_table_relationships.return_value = expected

        result = self.client.tables.list_table_relationships("account")

        self.client._odata._list_table_relationships.assert_called_once_with("account", filter=None, select=None)
        self.assertEqual(result, expected)

    def test_list_table_relationships_with_filter_and_select(self):
        """list_table_relationships() should forward filter and select."""
        self.client._odata._list_table_relationships.return_value = []

        self.client.tables.list_table_relationships(
            "account",
            filter="IsManaged eq false",
            select=["SchemaName"],
        )

        self.client._odata._list_table_relationships.assert_called_once_with(
            "account",
            filter="IsManaged eq false",
            select=["SchemaName"],
        )


if __name__ == "__main__":
    unittest.main()
