# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import pytest
from unittest.mock import AsyncMock

from PowerPlatform.Dataverse.aio.operations.async_tables import AsyncTableOperations
from PowerPlatform.Dataverse.models.relationship import RelationshipInfo
from PowerPlatform.Dataverse.models.table_info import AlternateKeyInfo, TableInfo
from PowerPlatform.Dataverse.models.relationship import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
)
from PowerPlatform.Dataverse.models.labels import Label, LocalizedLabel


def _label(text: str = "Test") -> Label:
    return Label(localized_labels=[LocalizedLabel(label=text, language_code=1033)])


def _table_raw(schema_name: str = "new_Product") -> dict:
    return {
        "table_schema_name": schema_name,
        "entity_set_name": "new_products",
        "table_logical_name": "new_product",
        "metadata_id": "meta-guid-1",
        "columns_created": ["new_Price"],
    }


def _rel_one_to_many_raw() -> dict:
    return {
        "relationship_id": "rel-guid-1",
        "relationship_schema_name": "new_Dept_Emp",
        "lookup_schema_name": "new_DeptId",
        "referenced_entity": "new_dept",
        "referencing_entity": "new_employee",
    }


def _rel_many_to_many_raw() -> dict:
    return {
        "relationship_id": "rel-guid-2",
        "relationship_schema_name": "new_emp_proj",
        "entity1_logical_name": "new_employee",
        "entity2_logical_name": "new_project",
    }


class TestAsyncTableOperationsNamespace:
    def test_namespace_type(self, async_client):
        assert isinstance(async_client.tables, AsyncTableOperations)


class TestAsyncTableCreate:
    async def test_create_returns_table_info(self, async_client, mock_od):
        """create() returns a TableInfo built from the raw dict."""
        mock_od._create_table.return_value = _table_raw()
        columns = {"new_Price": "decimal"}

        result = await async_client.tables.create(
            "new_Product",
            columns,
            solution="MySol",
            primary_column="new_ProductName",
            display_name="Product",
        )

        mock_od._create_table.assert_called_once_with("new_Product", columns, "MySol", "new_ProductName", "Product")
        assert isinstance(result, TableInfo)
        assert result.schema_name == "new_Product"

    async def test_create_with_minimal_args(self, async_client, mock_od):
        """create() works with only table and columns."""
        mock_od._create_table.return_value = _table_raw()
        await async_client.tables.create("new_Product", {})
        mock_od._create_table.assert_called_once_with("new_Product", {}, None, None, None)


class TestAsyncTableDelete:
    async def test_delete_calls_delete_table(self, async_client, mock_od):
        """delete() calls _delete_table with the table schema name."""
        await async_client.tables.delete("new_Product")
        mock_od._delete_table.assert_called_once_with("new_Product")


class TestAsyncTableGet:
    async def test_get_returns_table_info(self, async_client, mock_od):
        """get() returns TableInfo when table exists."""
        mock_od._get_table_info.return_value = _table_raw()
        result = await async_client.tables.get("new_Product")
        assert isinstance(result, TableInfo)
        assert result.schema_name == "new_Product"

    async def test_get_returns_none_when_not_found(self, async_client, mock_od):
        """get() returns None when _get_table_info returns None."""
        mock_od._get_table_info.return_value = None
        result = await async_client.tables.get("new_Product")
        assert result is None


class TestAsyncTableList:
    async def test_list_calls_list_tables(self, async_client, mock_od):
        """list() calls _list_tables and returns its result."""
        mock_od._list_tables.return_value = [{"LogicalName": "account"}]
        result = await async_client.tables.list()
        mock_od._list_tables.assert_called_once_with(filter=None, select=None)
        assert result == [{"LogicalName": "account"}]

    async def test_list_with_params(self, async_client, mock_od):
        """list() passes filter and select to _list_tables."""
        mock_od._list_tables.return_value = []
        await async_client.tables.list(filter="IsPrivate eq false", select=["LogicalName"])
        mock_od._list_tables.assert_called_once_with(filter="IsPrivate eq false", select=["LogicalName"])


class TestAsyncTableAddColumns:
    async def test_add_columns_calls_create_columns(self, async_client, mock_od):
        """add_columns() calls _create_columns and returns the result."""
        mock_od._create_columns.return_value = ["new_Notes"]
        result = await async_client.tables.add_columns("new_Product", {"new_Notes": "string"})
        mock_od._create_columns.assert_called_once_with("new_Product", {"new_Notes": "string"})
        assert result == ["new_Notes"]


class TestAsyncTableRemoveColumns:
    async def test_remove_columns_calls_delete_columns(self, async_client, mock_od):
        """remove_columns() calls _delete_columns and returns the result."""
        mock_od._delete_columns.return_value = ["new_Notes"]
        result = await async_client.tables.remove_columns("new_Product", "new_Notes")
        mock_od._delete_columns.assert_called_once_with("new_Product", "new_Notes")
        assert result == ["new_Notes"]


class TestAsyncTableOneToManyRelationship:
    async def test_create_one_to_many(self, async_client, mock_od):
        """create_one_to_many_relationship() calls _create_one_to_many_relationship and returns RelationshipInfo."""
        mock_od._create_one_to_many_relationship.return_value = _rel_one_to_many_raw()

        lookup = LookupAttributeMetadata(schema_name="new_DeptId", display_name=_label("Department"))
        relationship = OneToManyRelationshipMetadata(
            schema_name="new_Dept_Emp",
            referenced_entity="new_dept",
            referencing_entity="new_employee",
            referenced_attribute="new_deptid",
        )

        result = await async_client.tables.create_one_to_many_relationship(lookup, relationship)

        mock_od._create_one_to_many_relationship.assert_called_once_with(lookup, relationship, None)
        assert isinstance(result, RelationshipInfo)
        assert result.relationship_schema_name == "new_Dept_Emp"


class TestAsyncTableManyToManyRelationship:
    async def test_create_many_to_many(self, async_client, mock_od):
        """create_many_to_many_relationship() calls _create_many_to_many_relationship and returns RelationshipInfo."""
        mock_od._create_many_to_many_relationship.return_value = _rel_many_to_many_raw()

        relationship = ManyToManyRelationshipMetadata(
            schema_name="new_emp_proj",
            entity1_logical_name="new_employee",
            entity2_logical_name="new_project",
        )

        result = await async_client.tables.create_many_to_many_relationship(relationship)

        mock_od._create_many_to_many_relationship.assert_called_once_with(relationship, None)
        assert isinstance(result, RelationshipInfo)
        assert result.relationship_schema_name == "new_emp_proj"


class TestAsyncTableDeleteRelationship:
    async def test_delete_relationship(self, async_client, mock_od):
        """delete_relationship() calls _delete_relationship with the relationship_id."""
        await async_client.tables.delete_relationship("rel-guid-1")
        mock_od._delete_relationship.assert_called_once_with("rel-guid-1")


class TestAsyncTableGetRelationship:
    async def test_get_relationship_found(self, async_client, mock_od):
        """get_relationship() returns RelationshipInfo when found."""
        raw = {
            "@odata.type": "#Microsoft.Dynamics.CRM.OneToManyRelationshipMetadata",
            "RelationshipId": "rel-guid-1",
            "SchemaName": "new_Dept_Emp",
            "RelationshipType": "OneToManyRelationship",
            "ReferencedEntity": "new_dept",
            "ReferencingEntity": "new_employee",
            "ReferencingAttribute": "new_deptid",
        }
        mock_od._get_relationship.return_value = raw
        result = await async_client.tables.get_relationship("new_Dept_Emp")
        assert isinstance(result, RelationshipInfo)

    async def test_get_relationship_not_found(self, async_client, mock_od):
        """get_relationship() returns None when _get_relationship returns None."""
        mock_od._get_relationship.return_value = None
        result = await async_client.tables.get_relationship("nonexistent")
        assert result is None


class TestAsyncTableCreateLookupField:
    async def test_create_lookup_field_builds_models_and_delegates(self, async_client, mock_od):
        """create_lookup_field() builds lookup/relationship models and calls create_one_to_many_relationship."""
        from unittest.mock import MagicMock

        mock_lookup = LookupAttributeMetadata(schema_name="new_AccountId", display_name=_label("Account"))
        mock_rel = OneToManyRelationshipMetadata(
            schema_name="new_account_order",
            referenced_entity="account",
            referencing_entity="new_order",
            referenced_attribute="accountid",
        )
        # _build_lookup_field_models is a sync method on _ODataBase; use MagicMock so
        # od._build_lookup_field_models(...) returns the tuple directly (not a coroutine).
        mock_od._build_lookup_field_models = MagicMock(return_value=(mock_lookup, mock_rel))
        mock_od._create_one_to_many_relationship.return_value = {
            "relationship_id": "r-guid",
            "relationship_schema_name": "new_account_order",
            "lookup_schema_name": "new_AccountId",
            "referenced_entity": "account",
            "referencing_entity": "new_order",
        }

        result = await async_client.tables.create_lookup_field(
            referencing_table="new_order",
            lookup_field_name="new_AccountId",
            referenced_table="account",
        )

        mock_od._build_lookup_field_models.assert_called_once()
        mock_od._create_one_to_many_relationship.assert_called_once_with(mock_lookup, mock_rel, None)
        assert isinstance(result, RelationshipInfo)


class TestAsyncTableAlternateKeys:
    async def test_create_alternate_key(self, async_client, mock_od):
        """create_alternate_key() calls _create_alternate_key and returns AlternateKeyInfo."""
        mock_od._create_alternate_key.return_value = {
            "metadata_id": "key-guid",
            "schema_name": "new_prod_key",
            "key_attributes": ["new_productcode"],
        }

        result = await async_client.tables.create_alternate_key(
            "new_Product",
            "new_prod_key",
            ["new_productcode"],
            display_name="Product Code",
        )

        mock_od._create_alternate_key.assert_called_once()
        assert isinstance(result, AlternateKeyInfo)
        assert result.schema_name == "new_prod_key"
        assert result.status == "Pending"

    async def test_get_alternate_keys(self, async_client, mock_od):
        """get_alternate_keys() calls _get_alternate_keys and returns list of AlternateKeyInfo."""
        mock_od._get_alternate_keys.return_value = [
            {
                "MetadataId": "key-guid-1",
                "SchemaName": "new_prod_key",
                "KeyAttributes": ["new_productcode"],
                "EntityKeyIndexStatus": "Active",
            }
        ]

        result = await async_client.tables.get_alternate_keys("new_Product")

        mock_od._get_alternate_keys.assert_called_once_with("new_Product")
        assert len(result) == 1
        assert isinstance(result[0], AlternateKeyInfo)

    async def test_delete_alternate_key(self, async_client, mock_od):
        """delete_alternate_key() calls _delete_alternate_key with table and key_id."""
        await async_client.tables.delete_alternate_key("new_Product", "key-guid")
        mock_od._delete_alternate_key.assert_called_once_with("new_Product", "key-guid")


class TestAsyncTableListColumns:
    async def test_list_columns(self, async_client, mock_od):
        """list_columns() calls _list_columns and returns its result."""
        mock_od._list_columns.return_value = [{"LogicalName": "name"}]
        result = await async_client.tables.list_columns("account")
        mock_od._list_columns.assert_called_once_with("account", select=None, filter=None)
        assert result == [{"LogicalName": "name"}]

    async def test_list_columns_with_params(self, async_client, mock_od):
        """list_columns() passes select and filter to _list_columns."""
        mock_od._list_columns.return_value = []
        await async_client.tables.list_columns(
            "account",
            select=["LogicalName"],
            filter="AttributeType eq 'String'",
        )
        mock_od._list_columns.assert_called_once_with(
            "account", select=["LogicalName"], filter="AttributeType eq 'String'"
        )


class TestAsyncTableListRelationships:
    async def test_list_relationships(self, async_client, mock_od):
        """list_relationships() calls _list_relationships and returns its result."""
        mock_od._list_relationships.return_value = [{"SchemaName": "new_Dept_Emp"}]
        result = await async_client.tables.list_relationships()
        mock_od._list_relationships.assert_called_once_with(filter=None, select=None)
        assert result == [{"SchemaName": "new_Dept_Emp"}]

    async def test_list_table_relationships(self, async_client, mock_od):
        """list_table_relationships() calls _list_table_relationships and returns its result."""
        mock_od._list_table_relationships.return_value = [{"SchemaName": "new_Dept_Emp"}]
        result = await async_client.tables.list_table_relationships("account")
        mock_od._list_table_relationships.assert_called_once_with("account", filter=None, select=None)
        assert result == [{"SchemaName": "new_Dept_Emp"}]
