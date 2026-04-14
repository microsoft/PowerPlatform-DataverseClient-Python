# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Async table metadata operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from ...models.relationship import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
    RelationshipInfo,
)
from ...models.table_info import AlternateKeyInfo, TableInfo
from ...models.labels import Label, LocalizedLabel
from ...common.constants import CASCADE_BEHAVIOR_REMOVE_LINK

__all__ = ["AsyncTableOperations"]


class AsyncTableOperations:
    """Async namespace for table-level metadata operations.

    Accessed via ``client.tables``.  Async counterpart of
    :class:`~PowerPlatform.Dataverse.operations.tables.TableOperations`.

    :param client: The parent
        :class:`~PowerPlatform.Dataverse.aio.AsyncDataverseClient` instance.
    """

    def __init__(self, client: Any) -> None:
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

        :param table: Schema name of the table with customization prefix
            (e.g. ``"new_MyTestTable"``).
        :type table: :class:`str`
        :param columns: Mapping of column schema names to types (same as
            sync :meth:`~PowerPlatform.Dataverse.operations.tables.TableOperations.create`).
        :type columns: dict
        :param solution: Optional solution unique name.
        :type solution: :class:`str` or None
        :param primary_column: Optional primary column schema name.
        :type primary_column: :class:`str` or None

        :return: Table metadata.
        :rtype: ~PowerPlatform.Dataverse.models.table_info.TableInfo

        Example::

            result = await client.tables.create(
                "new_Product",
                {"new_Title": "string", "new_Price": "decimal"},
            )
        """
        async with self._client._scoped_odata() as od:
            raw = await od._create_table(table, columns, solution, primary_column)
            return TableInfo.from_dict(raw)

    # ----------------------------------------------------------------- delete

    async def delete(self, table: str) -> None:
        """Delete a custom table by schema name.

        :param table: Schema name of the table.
        :type table: :class:`str`

        Example::

            await client.tables.delete("new_MyTestTable")
        """
        async with self._client._scoped_odata() as od:
            await od._delete_table(table)

    # -------------------------------------------------------------------- get

    async def get(self, table: str) -> Optional[TableInfo]:
        """Get metadata for a table if it exists.

        :param table: Schema name of the table.
        :type table: :class:`str`

        :return: Table metadata, or ``None`` if not found.
        :rtype: ~PowerPlatform.Dataverse.models.table_info.TableInfo or None

        Example::

            info = await client.tables.get("new_MyTestTable")
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
        :param select: Optional list of property names for ``$select``.
        :type select: list[str] or None

        :return: List of EntityDefinition metadata dictionaries.
        :rtype: list[dict]

        Example::

            tables = await client.tables.list()
            for table in tables:
                print(table["LogicalName"])
        """
        async with self._client._scoped_odata() as od:
            return await od._list_tables(filter=filter, select=select)

    # ------------------------------------------------------------- add_columns

    async def add_columns(
        self,
        table: str,
        columns: Dict[str, Any],
    ) -> List[str]:
        """Add one or more columns to an existing table.

        :param table: Schema name of the table.
        :type table: :class:`str`
        :param columns: Mapping of column schema names to types.
        :type columns: dict

        :return: Schema names of created columns.
        :rtype: list[str]

        Example::

            created = await client.tables.add_columns(
                "new_MyTestTable", {"new_Notes": "string"}
            )
        """
        async with self._client._scoped_odata() as od:
            return await od._create_columns(table, columns)

    # ---------------------------------------------------------- remove_columns

    async def remove_columns(
        self,
        table: str,
        columns: Union[str, List[str]],
    ) -> List[str]:
        """Remove one or more columns from a table.

        :param table: Schema name of the table.
        :type table: :class:`str`
        :param columns: Column schema name or list of names.
        :type columns: str or list[str]

        :return: Schema names of removed columns.
        :rtype: list[str]

        Example::

            removed = await client.tables.remove_columns("new_MyTestTable", "new_Notes")
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

        :param lookup: Lookup attribute metadata.
        :type lookup: ~PowerPlatform.Dataverse.models.relationship.LookupAttributeMetadata
        :param relationship: Relationship metadata.
        :type relationship: ~PowerPlatform.Dataverse.models.relationship.OneToManyRelationshipMetadata
        :param solution: Optional solution unique name.
        :type solution: :class:`str` or None

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

        :param relationship: Relationship metadata.
        :type relationship: ~PowerPlatform.Dataverse.models.relationship.ManyToManyRelationshipMetadata
        :param solution: Optional solution unique name.
        :type solution: :class:`str` or None

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

        :param relationship_id: GUID of the relationship metadata.
        :type relationship_id: :class:`str`

        Example::

            await client.tables.delete_relationship("12345678-...")
        """
        async with self._client._scoped_odata() as od:
            await od._delete_relationship(relationship_id)

    # -------------------------------------------------------- get_relationship

    async def get_relationship(self, schema_name: str) -> Optional[RelationshipInfo]:
        """Retrieve relationship metadata by schema name.

        :param schema_name: Schema name of the relationship.
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
        """Create a simple lookup field relationship.

        Convenience wrapper for :meth:`create_one_to_many_relationship`.

        :param referencing_table: Logical name of the child table.
        :type referencing_table: :class:`str`
        :param lookup_field_name: Schema name for the lookup field.
        :type lookup_field_name: :class:`str`
        :param referenced_table: Logical name of the parent table.
        :type referenced_table: :class:`str`
        :param display_name: Display name for the lookup field.
        :type display_name: :class:`str` or None
        :param description: Optional description.
        :type description: :class:`str` or None
        :param required: Whether the lookup is required.
        :type required: :class:`bool`
        :param cascade_delete: Delete cascade behaviour.
        :type cascade_delete: :class:`str`
        :param solution: Optional solution unique name.
        :type solution: :class:`str` or None
        :param language_code: Language code for labels (default 1033).
        :type language_code: :class:`int`

        :return: Relationship metadata.
        :rtype: ~PowerPlatform.Dataverse.models.relationship.RelationshipInfo
        """
        async with self._client._scoped_odata() as od:
            lookup, relationship = od._build_lookup_field_models(
                referencing_table=referencing_table,
                lookup_field_name=lookup_field_name,
                referenced_table=referenced_table,
                display_name=display_name,
                description=description,
                required=required,
                cascade_delete=cascade_delete,
                language_code=language_code,
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
        :type table: :class:`str`
        :param key_name: Schema name for the alternate key.
        :type key_name: :class:`str`
        :param columns: List of column logical names.
        :type columns: list[str]
        :param display_name: Display name for the key.
        :type display_name: :class:`str` or None
        :param language_code: Language code for labels (default 1033).
        :type language_code: :class:`int`

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
        :rtype: list[~PowerPlatform.Dataverse.models.table_info.AlternateKeyInfo]
        """
        async with self._client._scoped_odata() as od:
            raw_list = await od._get_alternate_keys(table)
            return [AlternateKeyInfo.from_api_response(item) for item in raw_list]

    # ------------------------------------------------ delete_alternate_key

    async def delete_alternate_key(self, table: str, key_id: str) -> None:
        """Delete an alternate key by its metadata ID.

        :param table: Schema name of the table.
        :type table: :class:`str`
        :param key_id: Metadata GUID of the alternate key.
        :type key_id: :class:`str`
        """
        async with self._client._scoped_odata() as od:
            await od._delete_alternate_key(table, key_id)
