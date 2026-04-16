# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for AsyncTableOperations (client.tables namespace)."""

import pytest
from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

from azure.core.credentials_async import AsyncTokenCredential

from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient
from PowerPlatform.Dataverse.aio.operations.async_tables import AsyncTableOperations
from PowerPlatform.Dataverse.models.table_info import AlternateKeyInfo, TableInfo
from PowerPlatform.Dataverse.models.relationship import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
    RelationshipInfo,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client_with_mock_odata():
    """
    Return (client, mock_od).

    client._scoped_odata() is patched to yield mock_od without making any
    real HTTP or OData calls.
    """
    credential = AsyncMock(spec=AsyncTokenCredential)
    client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
    od = AsyncMock()

    @asynccontextmanager
    async def _fake_scoped_odata():
        yield od

    client._scoped_odata = _fake_scoped_odata
    return client, od


# Raw test data fixtures
_RAW_TABLE = {
    "table_schema_name": "new_Product",
    "table_logical_name": "new_product",
    "entity_set_name": "new_products",
    "metadata_id": "meta-1",
    "primary_id_attribute": "new_productid",
    "primary_name_attribute": None,
    "columns_created": [],
}

_RAW_REL_ONE_TO_MANY = {
    "@odata.type": "Microsoft.Dynamics.CRM.OneToManyRelationshipMetadata",
    "MetadataId": "rel-1",
    "SchemaName": "new_account_contact",
    "ReferencedEntity": "account",
    "ReferencingEntity": "contact",
    "ReferencingEntityNavigationPropertyName": "new_accountid",
}

_RAW_REL_MANY_TO_MANY = {
    "@odata.type": "Microsoft.Dynamics.CRM.ManyToManyRelationshipMetadata",
    "MetadataId": "rel-2",
    "SchemaName": "new_account_tag",
    "Entity1LogicalName": "account",
    "Entity2LogicalName": "new_tag",
}

_RAW_KEY = {
    "MetadataId": "key-1",
    "SchemaName": "new_mykey",
    "KeyAttributes": ["new_field1"],
    "EntityKeyIndexStatus": "Active",
}


# ---------------------------------------------------------------------------
# Namespace
# ---------------------------------------------------------------------------


class TestAsyncTableOperationsNamespace:
    """Tests that the tables namespace is correctly exposed on the client."""

    def test_namespace_exists(self):
        """client.tables exposes an AsyncTableOperations instance."""
        credential = AsyncMock(spec=AsyncTokenCredential)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", credential)
        assert isinstance(client.tables, AsyncTableOperations)


# ---------------------------------------------------------------------------
# create
# ---------------------------------------------------------------------------


class TestAsyncTableCreate:
    """Tests for tables.create() — table creation and TableInfo hydration."""

    async def test_create_calls_create_table_and_returns_table_info(self):
        """create() calls _create_table and returns a fully-hydrated TableInfo."""
        client, od = _make_client_with_mock_odata()
        od._create_table.return_value = _RAW_TABLE

        result = await client.tables.create("new_Product", {"new_Title": "string"})

        od._create_table.assert_awaited_once_with("new_Product", {"new_Title": "string"}, None, None)
        assert isinstance(result, TableInfo)
        assert result.schema_name == "new_Product"
        assert result.logical_name == "new_product"
        assert result.entity_set_name == "new_products"
        assert result.metadata_id == "meta-1"

    async def test_create_with_solution_passes_through(self):
        """create() forwards the solution name to _create_table."""
        client, od = _make_client_with_mock_odata()
        od._create_table.return_value = _RAW_TABLE

        await client.tables.create("new_Product", {"new_Title": "string"}, solution="MySolution")

        od._create_table.assert_awaited_once_with("new_Product", {"new_Title": "string"}, "MySolution", None)

    async def test_create_with_primary_column_passes_through(self):
        """create() forwards the primary_column name to _create_table."""
        client, od = _make_client_with_mock_odata()
        od._create_table.return_value = _RAW_TABLE

        await client.tables.create(
            "new_Product",
            {"new_Title": "string"},
            primary_column="new_Title",
        )

        od._create_table.assert_awaited_once_with("new_Product", {"new_Title": "string"}, None, "new_Title")


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


class TestAsyncTableDelete:
    """Tests for tables.delete() — table deletion delegation."""

    async def test_delete_calls_delete_table(self):
        """delete() delegates to _delete_table with the given table name."""
        client, od = _make_client_with_mock_odata()

        await client.tables.delete("new_Product")

        od._delete_table.assert_awaited_once_with("new_Product")

    async def test_delete_returns_none(self):
        """delete() returns None on success."""
        client, od = _make_client_with_mock_odata()

        result = await client.tables.delete("new_Product")

        assert result is None


# ---------------------------------------------------------------------------
# get
# ---------------------------------------------------------------------------


class TestAsyncTableGet:
    """Tests for tables.get() — table metadata retrieval and TableInfo hydration."""

    async def test_get_returns_table_info(self):
        """get() calls _get_table_info and returns a hydrated TableInfo on success."""
        client, od = _make_client_with_mock_odata()
        od._get_table_info.return_value = _RAW_TABLE

        result = await client.tables.get("new_product")

        od._get_table_info.assert_awaited_once_with("new_product")
        assert isinstance(result, TableInfo)
        assert result.logical_name == "new_product"

    async def test_get_returns_none_when_not_found(self):
        """get() returns None when the table does not exist."""
        client, od = _make_client_with_mock_odata()
        od._get_table_info.return_value = None

        result = await client.tables.get("nonexistent")

        assert result is None


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


class TestAsyncTableList:
    """Tests for tables.list() — table enumeration with optional filter and select."""

    async def test_list_returns_list_of_dicts(self):
        """list() calls _list_tables and returns the raw list of table dicts."""
        client, od = _make_client_with_mock_odata()
        od._list_tables.return_value = [{"LogicalName": "account"}, {"LogicalName": "contact"}]

        result = await client.tables.list()

        od._list_tables.assert_awaited_once_with(filter=None, select=None)
        assert isinstance(result, list)
        assert len(result) == 2

    async def test_list_passes_filter_kwarg(self):
        """list() forwards the filter argument to _list_tables."""
        client, od = _make_client_with_mock_odata()
        od._list_tables.return_value = []

        await client.tables.list(filter="IsCustomEntity eq true")

        od._list_tables.assert_awaited_once_with(filter="IsCustomEntity eq true", select=None)

    async def test_list_passes_select_kwarg(self):
        """list() forwards the select list to _list_tables."""
        client, od = _make_client_with_mock_odata()
        od._list_tables.return_value = []

        await client.tables.list(select=["LogicalName", "SchemaName"])

        od._list_tables.assert_awaited_once_with(filter=None, select=["LogicalName", "SchemaName"])


# ---------------------------------------------------------------------------
# add_columns / remove_columns
# ---------------------------------------------------------------------------


class TestAsyncTableColumns:
    """Tests for tables.add_columns() and tables.remove_columns() delegation."""

    async def test_add_columns_calls_create_columns(self):
        """add_columns() calls _create_columns and returns the list of created column names."""
        client, od = _make_client_with_mock_odata()
        od._create_columns.return_value = ["new_Notes"]

        result = await client.tables.add_columns("new_Product", {"new_Notes": "string"})

        od._create_columns.assert_awaited_once_with("new_Product", {"new_Notes": "string"})
        assert result == ["new_Notes"]

    async def test_remove_columns_calls_delete_columns(self):
        """remove_columns() with a string calls _delete_columns and returns the deleted column names."""
        client, od = _make_client_with_mock_odata()
        od._delete_columns.return_value = ["new_Notes"]

        result = await client.tables.remove_columns("new_Product", "new_Notes")

        od._delete_columns.assert_awaited_once_with("new_Product", "new_Notes")
        assert result == ["new_Notes"]

    async def test_remove_columns_with_list(self):
        """remove_columns() with a list calls _delete_columns and returns all deleted column names."""
        client, od = _make_client_with_mock_odata()
        od._delete_columns.return_value = ["new_A", "new_B"]

        result = await client.tables.remove_columns("new_Product", ["new_A", "new_B"])

        od._delete_columns.assert_awaited_once_with("new_Product", ["new_A", "new_B"])
        assert result == ["new_A", "new_B"]


# ---------------------------------------------------------------------------
# delete_relationship / get_relationship
# ---------------------------------------------------------------------------


class TestAsyncTableRelationship:
    """Tests for tables relationship operations: delete, get, and create one-to-many / many-to-many."""

    async def test_delete_relationship_calls_delete_relationship(self):
        """delete_relationship() delegates to _delete_relationship with the given GUID."""
        client, od = _make_client_with_mock_odata()

        await client.tables.delete_relationship("rel-guid-1")

        od._delete_relationship.assert_awaited_once_with("rel-guid-1")

    async def test_delete_relationship_returns_none(self):
        """delete_relationship() returns None on success."""
        client, od = _make_client_with_mock_odata()

        result = await client.tables.delete_relationship("rel-guid-1")

        assert result is None

    async def test_get_relationship_returns_relationship_info(self):
        """get_relationship() returns a RelationshipInfo hydrated from the OData response."""
        client, od = _make_client_with_mock_odata()
        od._get_relationship.return_value = _RAW_REL_ONE_TO_MANY

        result = await client.tables.get_relationship("new_account_contact")

        od._get_relationship.assert_awaited_once_with("new_account_contact")
        assert isinstance(result, RelationshipInfo)
        assert result.relationship_schema_name == "new_account_contact"
        assert result.relationship_type == "one_to_many"
        assert result.referenced_entity == "account"
        assert result.referencing_entity == "contact"

    async def test_get_relationship_returns_none_when_not_found(self):
        """get_relationship() returns None when the relationship does not exist."""
        client, od = _make_client_with_mock_odata()
        od._get_relationship.return_value = None

        result = await client.tables.get_relationship("nonexistent")

        assert result is None

    async def test_create_one_to_many_relationship_returns_relationship_info(self):
        """create_one_to_many_relationship() returns a RelationshipInfo with the correct fields."""
        client, od = _make_client_with_mock_odata()
        od._create_one_to_many_relationship.return_value = {
            "relationship_id": "rel-1",
            "relationship_schema_name": "new_account_contact",
            "lookup_schema_name": "new_accountid",
            "referenced_entity": "account",
            "referencing_entity": "contact",
        }
        lookup = MagicMock(spec=LookupAttributeMetadata)
        relationship = MagicMock(spec=OneToManyRelationshipMetadata)

        result = await client.tables.create_one_to_many_relationship(lookup, relationship)

        od._create_one_to_many_relationship.assert_awaited_once_with(lookup, relationship, None)
        assert isinstance(result, RelationshipInfo)
        assert result.relationship_type == "one_to_many"
        assert result.relationship_id == "rel-1"
        assert result.relationship_schema_name == "new_account_contact"

    async def test_create_many_to_many_relationship_returns_relationship_info(self):
        """create_many_to_many_relationship() returns a RelationshipInfo with the correct entity names."""
        client, od = _make_client_with_mock_odata()
        od._create_many_to_many_relationship.return_value = {
            "relationship_id": "rel-2",
            "relationship_schema_name": "new_account_tag",
            "entity1_logical_name": "account",
            "entity2_logical_name": "new_tag",
        }
        relationship = MagicMock(spec=ManyToManyRelationshipMetadata)

        result = await client.tables.create_many_to_many_relationship(relationship)

        od._create_many_to_many_relationship.assert_awaited_once_with(relationship, None)
        assert isinstance(result, RelationshipInfo)
        assert result.relationship_type == "many_to_many"
        assert result.entity1_logical_name == "account"
        assert result.entity2_logical_name == "new_tag"

    async def test_create_one_to_many_with_solution_passes_through(self):
        """create_one_to_many_relationship() forwards the solution name to the OData layer."""
        client, od = _make_client_with_mock_odata()
        od._create_one_to_many_relationship.return_value = {
            "relationship_id": "rel-1",
            "relationship_schema_name": "new_account_contact",
            "lookup_schema_name": "new_accountid",
            "referenced_entity": "account",
            "referencing_entity": "contact",
        }
        lookup = MagicMock(spec=LookupAttributeMetadata)
        relationship = MagicMock(spec=OneToManyRelationshipMetadata)

        await client.tables.create_one_to_many_relationship(lookup, relationship, solution="MySolution")

        od._create_one_to_many_relationship.assert_awaited_once_with(lookup, relationship, "MySolution")


# ---------------------------------------------------------------------------
# create_lookup_field
# ---------------------------------------------------------------------------


class TestAsyncTableCreateLookupField:
    """Tests for tables.create_lookup_field() — model building and relationship creation."""

    async def test_create_lookup_field_builds_models_and_creates_relationship(self):
        """create_lookup_field() builds lookup models then calls _create_one_to_many_relationship."""
        client, od = _make_client_with_mock_odata()
        mock_lookup = MagicMock(spec=LookupAttributeMetadata)
        mock_relationship = MagicMock(spec=OneToManyRelationshipMetadata)
        od._build_lookup_field_models = MagicMock(return_value=(mock_lookup, mock_relationship))
        od._create_one_to_many_relationship.return_value = {
            "relationship_id": "rel-1",
            "relationship_schema_name": "new_account_contact",
            "lookup_schema_name": "new_accountid",
            "referenced_entity": "account",
            "referencing_entity": "contact",
        }

        result = await client.tables.create_lookup_field(
            referencing_table="contact",
            lookup_field_name="new_accountid",
            referenced_table="account",
        )

        od._build_lookup_field_models.assert_called_once()
        od._create_one_to_many_relationship.assert_awaited_once_with(mock_lookup, mock_relationship, None)
        assert isinstance(result, RelationshipInfo)
        assert result.relationship_type == "one_to_many"

    async def test_create_lookup_field_with_display_name_and_solution(self):
        """create_lookup_field() forwards the solution name to _create_one_to_many_relationship."""
        client, od = _make_client_with_mock_odata()
        mock_lookup = MagicMock(spec=LookupAttributeMetadata)
        mock_relationship = MagicMock(spec=OneToManyRelationshipMetadata)
        od._build_lookup_field_models = MagicMock(return_value=(mock_lookup, mock_relationship))
        od._create_one_to_many_relationship.return_value = {
            "relationship_id": "rel-1",
            "relationship_schema_name": "new_account_contact",
            "lookup_schema_name": "new_accountid",
            "referenced_entity": "account",
            "referencing_entity": "contact",
        }

        await client.tables.create_lookup_field(
            referencing_table="contact",
            lookup_field_name="new_accountid",
            referenced_table="account",
            display_name="Account Lookup",
            solution="MySolution",
        )

        od._create_one_to_many_relationship.assert_awaited_once_with(mock_lookup, mock_relationship, "MySolution")


# ---------------------------------------------------------------------------
# alternate keys
# ---------------------------------------------------------------------------


class TestAsyncTableAlternateKeys:
    """Tests for tables alternate-key operations: create, get, and delete."""

    async def test_create_alternate_key_returns_alternate_key_info(self):
        """create_alternate_key() calls _create_alternate_key and returns a hydrated AlternateKeyInfo."""
        client, od = _make_client_with_mock_odata()
        od._create_alternate_key.return_value = {
            "metadata_id": "key-1",
            "schema_name": "new_mykey",
            "key_attributes": ["new_field1"],
        }

        result = await client.tables.create_alternate_key("new_Product", "new_mykey", ["new_field1"])

        od._create_alternate_key.assert_awaited_once()
        call_args = od._create_alternate_key.call_args[0]
        assert call_args[0] == "new_Product"
        assert call_args[1] == "new_mykey"
        assert call_args[2] == ["new_field1"]
        assert isinstance(result, AlternateKeyInfo)
        assert result.metadata_id == "key-1"
        assert result.schema_name == "new_mykey"
        assert result.key_attributes == ["new_field1"]
        assert result.status == "Pending"

    async def test_get_alternate_keys_returns_list(self):
        """get_alternate_keys() returns a list of AlternateKeyInfo objects for the table."""
        client, od = _make_client_with_mock_odata()
        od._get_alternate_keys.return_value = [_RAW_KEY]

        result = await client.tables.get_alternate_keys("new_Product")

        od._get_alternate_keys.assert_awaited_once_with("new_Product")
        assert isinstance(result, list)
        assert len(result) == 1
        assert isinstance(result[0], AlternateKeyInfo)
        assert result[0].metadata_id == "key-1"
        assert result[0].schema_name == "new_mykey"
        assert result[0].key_attributes == ["new_field1"]
        assert result[0].status == "Active"

    async def test_get_alternate_keys_empty_returns_empty_list(self):
        """get_alternate_keys() returns an empty list when no keys exist for the table."""
        client, od = _make_client_with_mock_odata()
        od._get_alternate_keys.return_value = []

        result = await client.tables.get_alternate_keys("new_Product")

        assert result == []

    async def test_delete_alternate_key_calls_delete(self):
        """delete_alternate_key() delegates to _delete_alternate_key with the table and key GUID."""
        client, od = _make_client_with_mock_odata()

        await client.tables.delete_alternate_key("new_Product", "key-guid-1")

        od._delete_alternate_key.assert_awaited_once_with("new_Product", "key-guid-1")

    async def test_delete_alternate_key_returns_none(self):
        """delete_alternate_key() returns None on success."""
        client, od = _make_client_with_mock_odata()

        result = await client.tables.delete_alternate_key("new_Product", "key-guid-1")

        assert result is None
