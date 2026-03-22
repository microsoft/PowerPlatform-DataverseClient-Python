# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Async table metadata operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

from ..models.relationship import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
    CascadeConfiguration,
    RelationshipInfo,
)
from ..models.table_info import AlternateKeyInfo
from ..models.labels import Label, LocalizedLabel
from ..models.table_info import TableInfo
from ..common.constants import CASCADE_BEHAVIOR_REMOVE_LINK

if TYPE_CHECKING:
    from ..async_client import AsyncDataverseClient


__all__ = ["AsyncTableOperations"]


class AsyncTableOperations:
    """Async namespace for table-level metadata operations.

    Accessed via ``client.tables`` on
    :class:`~PowerPlatform.Dataverse.async_client.AsyncDataverseClient`.

    :param client: The parent async client instance.
    :type client: ~PowerPlatform.Dataverse.async_client.AsyncDataverseClient
    """

    def __init__(self, client: AsyncDataverseClient) -> None:
        self._client = client

    # ----------------------------------------------------------------- create

    async def create(
        self,
        table: str,
        columns: Dict[str, Any],
        *,
        solution: Optional[str] = None,
        primary_column: Optional[str] = None,
    ) -> TableInfo:
        """Create a custom table with the specified columns.

        :param table: Schema name of the table (e.g. ``"new_MyTestTable"``).
        :type table: :class:`str`
        :param columns: Mapping of column schema names to their types.
        :type columns: :class:`dict`
        :param solution: Optional solution unique name.
        :type solution: :class:`str` or None
        :param primary_column: Optional primary name column schema name.
        :type primary_column: :class:`str` or None

        :return: Table metadata.
        :rtype: ~PowerPlatform.Dataverse.models.table_info.TableInfo

        Example::

            result = await client.tables.create(
                "new_Product",
                {"new_Title": "string", "new_Price": "decimal"},
                solution="MySolution",
            )
        """
        async with self._client._scoped_odata() as od:
            raw = await od._create_table(table, columns, solution, primary_column)
            return TableInfo.from_dict(raw)

    # ----------------------------------------------------------------- delete

    async def delete(self, table: str) -> None:
        """Delete a custom table by schema name.

        :param table: Schema name of the table (e.g. ``"new_MyTestTable"``).
        :type table: :class:`str`

        Example::

            await client.tables.delete("new_MyTestTable")
        """
        async with self._client._scoped_odata() as od:
            await od._delete_table(table)

    # -------------------------------------------------------------------- get

    async def get(self, table: str) -> Optional[TableInfo]:
        """Get basic metadata for a table if it exists.

        :param table: Schema name of the table.
        :type table: :class:`str`

        :return: Table metadata, or ``None`` if the table is not found.
        :rtype: ~PowerPlatform.Dataverse.models.table_info.TableInfo or None

        Example::

            info = await client.tables.get("new_MyTestTable")
            if info:
                print(info["table_logical_name"])
        """
        async with self._client._scoped_odata() as od:
            raw = await od._get_table_info(table)
            if raw is None:
                return None
            return TableInfo.from_dict(raw)

    # ------------------------------------------------------------------- list

    async def list(
        self,
        *,
        filter: Optional[str] = None,
        select: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """List all non-private tables in the Dataverse environment.

        :param filter: Optional OData ``$filter`` expression.
        :type filter: :class:`str` or None
        :param select: Optional list of property names to project.
        :type select: :class:`list` of :class:`str` or None

        :return: List of EntityDefinition metadata dictionaries.
        :rtype: :class:`list` of :class:`dict`

        Example::

            tables = await client.tables.list()
            for table in tables:
                print(table["LogicalName"])
        """
        async with self._client._scoped_odata() as od:
            return await od._list_tables(filter=filter, select=select)

    # ------------------------------------------------------------- add_columns

    async def add_columns(self, table: str, columns: Dict[str, Any]) -> List[str]:
        """Add one or more columns to an existing table.

        :param table: Schema name of the table.
        :type table: :class:`str`
        :param columns: Mapping of column schema names to their types.
        :type columns: :class:`dict`

        :return: Schema names of the columns that were created.
        :rtype: :class:`list` of :class:`str`

        Example::

            created = await client.tables.add_columns(
                "new_MyTestTable",
                {"new_Notes": "string", "new_Active": "bool"},
            )
        """
        async with self._client._scoped_odata() as od:
            return await od._create_columns(table, columns)

    # ---------------------------------------------------------- remove_columns

    async def remove_columns(self, table: str, columns: Union[str, List[str]]) -> List[str]:
        """Remove one or more columns from a table.

        :param table: Schema name of the table.
        :type table: :class:`str`
        :param columns: Column schema name or list of column schema names to remove.
        :type columns: :class:`str` or :class:`list` of :class:`str`

        :return: Schema names of the columns that were removed.
        :rtype: :class:`list` of :class:`str`

        Example::

            removed = await client.tables.remove_columns("new_MyTestTable", ["new_Notes"])
        """
        async with self._client._scoped_odata() as od:
            return await od._delete_columns(table, columns)

    # ------------------------------------------------------ create_one_to_many

    async def create_one_to_many_relationship(
        self,
        lookup: LookupAttributeMetadata,
        relationship: OneToManyRelationshipMetadata,
        *,
        solution: Optional[str] = None,
    ) -> RelationshipInfo:
        """Create a one-to-many relationship between tables.

        :param lookup: Metadata defining the lookup attribute.
        :param relationship: Metadata defining the relationship.
        :param solution: Optional solution unique name.

        :return: Relationship metadata.
        :rtype: ~PowerPlatform.Dataverse.models.relationship.RelationshipInfo
        """
        async with self._client._scoped_odata() as od:
            raw = await od._create_one_to_many_relationship(lookup, relationship, solution)
            return RelationshipInfo.from_one_to_many(
                relationship_id=raw["relationship_id"],
                relationship_schema_name=raw["relationship_schema_name"],
                lookup_schema_name=raw["lookup_schema_name"],
                referenced_entity=raw["referenced_entity"],
                referencing_entity=raw["referencing_entity"],
            )

    # ----------------------------------------------------- create_many_to_many

    async def create_many_to_many_relationship(
        self,
        relationship: ManyToManyRelationshipMetadata,
        *,
        solution: Optional[str] = None,
    ) -> RelationshipInfo:
        """Create a many-to-many relationship between tables.

        :param relationship: Metadata defining the relationship.
        :param solution: Optional solution unique name.

        :return: Relationship metadata.
        :rtype: ~PowerPlatform.Dataverse.models.relationship.RelationshipInfo
        """
        async with self._client._scoped_odata() as od:
            raw = await od._create_many_to_many_relationship(relationship, solution)
            return RelationshipInfo.from_many_to_many(
                relationship_id=raw["relationship_id"],
                relationship_schema_name=raw["relationship_schema_name"],
                entity1_logical_name=raw["entity1_logical_name"],
                entity2_logical_name=raw["entity2_logical_name"],
            )

    # ------------------------------------------------------- delete_relationship

    async def delete_relationship(self, relationship_id: str) -> None:
        """Delete a relationship by its metadata ID.

        :param relationship_id: The GUID of the relationship metadata.
        :type relationship_id: :class:`str`
        """
        async with self._client._scoped_odata() as od:
            await od._delete_relationship(relationship_id)

    # -------------------------------------------------------- get_relationship

    async def get_relationship(self, schema_name: str) -> Optional[RelationshipInfo]:
        """Retrieve relationship metadata by schema name.

        :param schema_name: The schema name of the relationship.
        :type schema_name: :class:`str`

        :return: Relationship metadata, or ``None`` if not found.
        :rtype: ~PowerPlatform.Dataverse.models.relationship.RelationshipInfo or None
        """
        async with self._client._scoped_odata() as od:
            raw = await od._get_relationship(schema_name)
            if raw is None:
                return None
            return RelationshipInfo.from_api_response(raw)

    # ------------------------------------------------------- create_lookup_field

    async def create_lookup_field(
        self,
        referencing_table: str,
        lookup_field_name: str,
        referenced_table: str,
        *,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        required: bool = False,
        cascade_delete: str = CASCADE_BEHAVIOR_REMOVE_LINK,
        solution: Optional[str] = None,
        language_code: int = 1033,
    ) -> RelationshipInfo:
        """Create a simple lookup field relationship (convenience wrapper).

        :param referencing_table: Logical name of the table with the lookup field.
        :param lookup_field_name: Schema name for the lookup field.
        :param referenced_table: Logical name of the referenced (parent) table.
        :param display_name: Display name for the lookup field.
        :param description: Optional description.
        :param required: Whether the lookup is required.
        :param cascade_delete: Delete behavior.
        :param solution: Optional solution unique name.
        :param language_code: Language code for labels.

        :return: Relationship metadata.
        :rtype: ~PowerPlatform.Dataverse.models.relationship.RelationshipInfo
        """
        localized_labels = [LocalizedLabel(label=display_name or referenced_table, language_code=language_code)]
        lookup = LookupAttributeMetadata(
            schema_name=lookup_field_name,
            display_name=Label(localized_labels=localized_labels),
            required_level="ApplicationRequired" if required else "None",
        )
        if description:
            lookup.description = Label(
                localized_labels=[LocalizedLabel(label=description, language_code=language_code)]
            )
        relationship_name = f"{referenced_table}_{referencing_table}_{lookup_field_name}"
        relationship = OneToManyRelationshipMetadata(
            schema_name=relationship_name,
            referenced_entity=referenced_table,
            referencing_entity=referencing_table,
            referenced_attribute=f"{referenced_table}id",
            cascade_configuration=CascadeConfiguration(delete=cascade_delete),
        )
        return await self.create_one_to_many_relationship(lookup, relationship, solution=solution)

    # ------------------------------------------------- create_alternate_key

    async def create_alternate_key(
        self,
        table: str,
        key_name: str,
        columns: List[str],
        *,
        display_name: Optional[str] = None,
        language_code: int = 1033,
    ) -> AlternateKeyInfo:
        """Create an alternate key on a table.

        :param table: Schema name of the table.
        :param key_name: Schema name for the new alternate key.
        :param columns: List of column logical names that compose the key.
        :param display_name: Display name for the key.
        :param language_code: Language code for labels.

        :return: Alternate key metadata.
        :rtype: ~PowerPlatform.Dataverse.models.table_info.AlternateKeyInfo
        """
        label = Label(localized_labels=[LocalizedLabel(label=display_name or key_name, language_code=language_code)])
        async with self._client._scoped_odata() as od:
            raw = await od._create_alternate_key(table, key_name, columns, label)
            return AlternateKeyInfo(
                metadata_id=raw["metadata_id"],
                schema_name=raw["schema_name"],
                key_attributes=raw["key_attributes"],
                status="Pending",
            )

    # --------------------------------------------------- get_alternate_keys

    async def get_alternate_keys(self, table: str) -> List[AlternateKeyInfo]:
        """List all alternate keys defined on a table.

        :param table: Schema name of the table.
        :type table: :class:`str`

        :return: List of alternate key metadata objects.
        :rtype: :class:`list` of ~PowerPlatform.Dataverse.models.table_info.AlternateKeyInfo
        """
        async with self._client._scoped_odata() as od:
            raw_list = await od._get_alternate_keys(table)
            return [AlternateKeyInfo.from_api_response(item) for item in raw_list]

    # ------------------------------------------------ delete_alternate_key

    async def delete_alternate_key(self, table: str, key_id: str) -> None:
        """Delete an alternate key by its metadata ID.

        :param table: Schema name of the table.
        :type table: :class:`str`
        :param key_id: Metadata GUID of the alternate key to delete.
        :type key_id: :class:`str`
        """
        async with self._client._scoped_odata() as od:
            await od._delete_alternate_key(table, key_id)
