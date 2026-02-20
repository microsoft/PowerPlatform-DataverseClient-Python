# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Table metadata operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

from ..models.metadata import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
    Label,
    LocalizedLabel,
    CascadeConfiguration,
)
from ..common.constants import CASCADE_BEHAVIOR_REMOVE_LINK

if TYPE_CHECKING:
    from ..client import DataverseClient


__all__ = ["TableOperations"]


class TableOperations:
    """Namespace for table-level metadata operations.

    Accessed via ``client.tables``. Provides operations to create, delete,
    inspect, and list Dataverse tables, as well as add and remove columns.

    :param client: The parent :class:`~PowerPlatform.Dataverse.client.DataverseClient` instance.
    :type client: ~PowerPlatform.Dataverse.client.DataverseClient

    Example::

        client = DataverseClient(base_url, credential)

        # Create a table
        info = client.tables.create(
            "new_Product",
            {"new_Price": "decimal", "new_InStock": "bool"},
            solution="MySolution",
        )

        # List tables
        tables = client.tables.list()

        # Get table info
        info = client.tables.get("new_Product")

        # Add columns
        client.tables.add_columns("new_Product", {"new_Rating": "int"})

        # Remove columns
        client.tables.remove_columns("new_Product", "new_Rating")

        # Delete a table
        client.tables.delete("new_Product")
    """

    def __init__(self, client: DataverseClient) -> None:
        self._client = client

    # ----------------------------------------------------------------- create

    def create(
        self,
        table: str,
        columns: Dict[str, Any],
        *,
        solution: Optional[str] = None,
        primary_column: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a custom table with the specified columns.

        :param table: Schema name of the table with customization prefix
            (e.g. ``"new_MyTestTable"``).
        :type table: :class:`str`
        :param columns: Mapping of column schema names (with customization
            prefix) to their types. Supported types include ``"string"``
            (or ``"text"``), ``"int"`` (or ``"integer"``), ``"decimal"``
            (or ``"money"``), ``"float"`` (or ``"double"``), ``"datetime"``
            (or ``"date"``), ``"bool"`` (or ``"boolean"``), ``"file"``, and
            ``Enum`` subclasses
            (for local option sets).
        :type columns: :class:`dict`
        :param solution: Optional solution unique name that should own the new
            table. When omitted the table is created in the default solution.
        :type solution: :class:`str` or None
        :param primary_column: Optional primary name column schema name with
            customization prefix (e.g. ``"new_ProductName"``). If not provided,
            defaults to ``"{prefix}_Name"``.
        :type primary_column: :class:`str` or None

        :return: Dictionary containing table metadata including
            ``table_schema_name``, ``entity_set_name``, ``table_logical_name``,
            ``metadata_id``, and ``columns_created``.
        :rtype: :class:`dict`

        :raises ~PowerPlatform.Dataverse.core.errors.MetadataError:
            If table creation fails or the table already exists.

        Example:
            Create a table with simple columns::

                from enum import IntEnum

                class ItemStatus(IntEnum):
                    ACTIVE = 1
                    INACTIVE = 2

                result = client.tables.create(
                    "new_Product",
                    {
                        "new_Title": "string",
                        "new_Price": "decimal",
                        "new_Status": ItemStatus,
                    },
                    solution="MySolution",
                    primary_column="new_ProductName",
                )
                print(f"Created: {result['table_schema_name']}")
        """
        with self._client._scoped_odata() as od:
            return od._create_table(
                table,
                columns,
                solution,
                primary_column,
            )

    # ----------------------------------------------------------------- delete

    def delete(self, table: str) -> None:
        """Delete a custom table by schema name.

        :param table: Schema name of the table (e.g. ``"new_MyTestTable"``).
        :type table: :class:`str`

        :raises ~PowerPlatform.Dataverse.core.errors.MetadataError:
            If the table does not exist or deletion fails.

        .. warning::
            This operation is irreversible and will delete all records in the
            table along with the table definition.

        Example::

            client.tables.delete("new_MyTestTable")
        """
        with self._client._scoped_odata() as od:
            od._delete_table(table)

    # -------------------------------------------------------------------- get

    def get(self, table: str) -> Optional[Dict[str, Any]]:
        """Get basic metadata for a table if it exists.

        :param table: Schema name of the table (e.g. ``"new_MyTestTable"``
            or ``"account"``).
        :type table: :class:`str`

        :return: Dictionary containing ``table_schema_name``,
            ``table_logical_name``, ``entity_set_name``, and ``metadata_id``.
            Returns None if the table is not found.
        :rtype: :class:`dict` or None

        Example::

            info = client.tables.get("new_MyTestTable")
            if info:
                print(f"Logical name: {info['table_logical_name']}")
                print(f"Entity set: {info['entity_set_name']}")
        """
        with self._client._scoped_odata() as od:
            return od._get_table_info(table)

    # ------------------------------------------------------------------- list

    def list(self) -> List[Dict[str, Any]]:
        """List all non-private tables in the Dataverse environment.

        :return: List of EntityDefinition metadata dictionaries.
        :rtype: :class:`list` of :class:`dict`

        Example::

            tables = client.tables.list()
            for table in tables:
                print(table["LogicalName"])
        """
        with self._client._scoped_odata() as od:
            return od._list_tables()

    # ------------------------------------------------------------- add_columns

    def add_columns(
        self,
        table: str,
        columns: Dict[str, Any],
    ) -> List[str]:
        """Add one or more columns to an existing table.

        :param table: Schema name of the table (e.g. ``"new_MyTestTable"``).
        :type table: :class:`str`
        :param columns: Mapping of column schema names (with customization
            prefix) to their types. Supported types are the same as for
            :meth:`create`.
        :type columns: :class:`dict`

        :return: Schema names of the columns that were created.
        :rtype: :class:`list` of :class:`str`

        :raises ~PowerPlatform.Dataverse.core.errors.MetadataError:
            If the table does not exist.

        Example::

            created = client.tables.add_columns(
                "new_MyTestTable",
                {"new_Notes": "string", "new_Active": "bool"},
            )
            print(created)  # ['new_Notes', 'new_Active']
        """
        with self._client._scoped_odata() as od:
            return od._create_columns(table, columns)

    # ---------------------------------------------------------- remove_columns

    def remove_columns(
        self,
        table: str,
        columns: Union[str, List[str]],
    ) -> List[str]:
        """Remove one or more columns from a table.

        :param table: Schema name of the table (e.g. ``"new_MyTestTable"``).
        :type table: :class:`str`
        :param columns: Column schema name or list of column schema names to
            remove. Must include the customization prefix (e.g.
            ``"new_TestColumn"``).
        :type columns: :class:`str` or :class:`list` of :class:`str`

        :return: Schema names of the columns that were removed.
        :rtype: :class:`list` of :class:`str`

        :raises ~PowerPlatform.Dataverse.core.errors.MetadataError:
            If the table or a specified column does not exist.

        Example::

            removed = client.tables.remove_columns(
                "new_MyTestTable",
                ["new_Notes", "new_Active"],
            )
            print(removed)  # ['new_Notes', 'new_Active']
        """
        with self._client._scoped_odata() as od:
            return od._delete_columns(table, columns)

    # ------------------------------------------------------ create_one_to_many

    def create_one_to_many_relationship(
        self,
        lookup: LookupAttributeMetadata,
        relationship: OneToManyRelationshipMetadata,
        *,
        solution: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a one-to-many relationship between tables.

        This operation creates both the relationship and the lookup attribute
        on the referencing table.

        :param lookup: Metadata defining the lookup attribute.
        :type lookup: ~PowerPlatform.Dataverse.models.metadata.LookupAttributeMetadata
        :param relationship: Metadata defining the relationship.
        :type relationship: ~PowerPlatform.Dataverse.models.metadata.OneToManyRelationshipMetadata
        :param solution: Optional solution unique name to add relationship to.
        :type solution: :class:`str` or None

        :return: Dictionary with ``relationship_id``, ``lookup_schema_name``,
            and related metadata.
        :rtype: :class:`dict`

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError:
            If the Web API request fails.

        Example:
            Create a one-to-many relationship: Department (1) -> Employee (N)::

                from PowerPlatform.Dataverse.models.metadata import (
                    LookupAttributeMetadata,
                    OneToManyRelationshipMetadata,
                    Label,
                    LocalizedLabel,
                    CascadeConfiguration,
                )
                from PowerPlatform.Dataverse.common.constants import (
                    CASCADE_BEHAVIOR_REMOVE_LINK,
                )

                lookup = LookupAttributeMetadata(
                    schema_name="new_DepartmentId",
                    display_name=Label(
                        localized_labels=[
                            LocalizedLabel(label="Department", language_code=1033)
                        ]
                    ),
                )

                relationship = OneToManyRelationshipMetadata(
                    schema_name="new_Department_Employee",
                    referenced_entity="new_department",
                    referencing_entity="new_employee",
                    referenced_attribute="new_departmentid",
                    cascade_configuration=CascadeConfiguration(
                        delete=CASCADE_BEHAVIOR_REMOVE_LINK,
                    ),
                )

                result = client.tables.create_one_to_many_relationship(lookup, relationship)
                print(f"Created lookup field: {result['lookup_schema_name']}")
        """
        with self._client._scoped_odata() as od:
            return od._create_one_to_many_relationship(
                lookup,
                relationship,
                solution,
            )

    # ----------------------------------------------------- create_many_to_many

    def create_many_to_many_relationship(
        self,
        relationship: ManyToManyRelationshipMetadata,
        *,
        solution: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a many-to-many relationship between tables.

        This operation creates a many-to-many relationship and an intersect
        table to manage the relationship.

        :param relationship: Metadata defining the many-to-many relationship.
        :type relationship: ~PowerPlatform.Dataverse.models.metadata.ManyToManyRelationshipMetadata
        :param solution: Optional solution unique name to add relationship to.
        :type solution: :class:`str` or None

        :return: Dictionary with ``relationship_id``,
            ``relationship_schema_name``, and entity names.
        :rtype: :class:`dict`

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError:
            If the Web API request fails.

        Example:
            Create a many-to-many relationship: Employee <-> Project::

                from PowerPlatform.Dataverse.models.metadata import (
                    ManyToManyRelationshipMetadata,
                )

                relationship = ManyToManyRelationshipMetadata(
                    schema_name="new_employee_project",
                    entity1_logical_name="new_employee",
                    entity2_logical_name="new_project",
                )

                result = client.tables.create_many_to_many_relationship(relationship)
                print(f"Created: {result['relationship_schema_name']}")
        """
        with self._client._scoped_odata() as od:
            return od._create_many_to_many_relationship(
                relationship,
                solution,
            )

    # ------------------------------------------------------- delete_relationship

    def delete_relationship(self, relationship_id: str) -> None:
        """Delete a relationship by its metadata ID.

        :param relationship_id: The GUID of the relationship metadata.
        :type relationship_id: :class:`str`

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError:
            If the Web API request fails.

        .. warning::
            Deleting a relationship also removes the associated lookup attribute
            for one-to-many relationships. This operation is irreversible.

        Example::

            client.tables.delete_relationship(
                "12345678-1234-1234-1234-123456789abc"
            )
        """
        with self._client._scoped_odata() as od:
            od._delete_relationship(relationship_id)

    # -------------------------------------------------------- get_relationship

    def get_relationship(self, schema_name: str) -> Optional[Dict[str, Any]]:
        """Retrieve relationship metadata by schema name.

        :param schema_name: The schema name of the relationship.
        :type schema_name: :class:`str`

        :return: Relationship metadata dictionary, or None if not found.
        :rtype: :class:`dict` or None

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError:
            If the Web API request fails.

        Example::

            rel = client.tables.get_relationship("new_Department_Employee")
            if rel:
                print(f"Found: {rel['SchemaName']}")
        """
        with self._client._scoped_odata() as od:
            return od._get_relationship(schema_name)

    # ------------------------------------------------------- create_lookup_field

    def create_lookup_field(
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
    ) -> Dict[str, Any]:
        """Create a simple lookup field relationship.

        This is a convenience method that wraps :meth:`create_one_to_many_relationship`
        for the common case of adding a lookup field to an existing table.

        :param referencing_table: Logical name of the table that will have
            the lookup field (child table).
        :type referencing_table: :class:`str`
        :param lookup_field_name: Schema name for the lookup field
            (e.g., ``"new_AccountId"``).
        :type lookup_field_name: :class:`str`
        :param referenced_table: Logical name of the table being referenced
            (parent table).
        :type referenced_table: :class:`str`
        :param display_name: Display name for the lookup field. Defaults to
            the referenced table name.
        :type display_name: :class:`str` or None
        :param description: Optional description for the lookup field.
        :type description: :class:`str` or None
        :param required: Whether the lookup is required. Defaults to ``False``.
        :type required: :class:`bool`
        :param cascade_delete: Delete behavior (``"RemoveLink"``,
            ``"Cascade"``, ``"Restrict"``). Defaults to ``"RemoveLink"``.
        :type cascade_delete: :class:`str`
        :param solution: Optional solution unique name to add the relationship
            to.
        :type solution: :class:`str` or None
        :param language_code: Language code for labels. Defaults to 1033
            (English).
        :type language_code: :class:`int`

        :return: Dictionary with ``relationship_id``, ``lookup_schema_name``,
            and related metadata.
        :rtype: :class:`dict`

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError:
            If the Web API request fails.

        Example:
            Create a simple lookup field::

                result = client.tables.create_lookup_field(
                    referencing_table="new_order",
                    lookup_field_name="new_AccountId",
                    referenced_table="account",
                    display_name="Account",
                    required=True,
                    cascade_delete=CASCADE_BEHAVIOR_REMOVE_LINK,
                )
                print(f"Created lookup: {result['lookup_schema_name']}")
        """
        localized_labels = [
            LocalizedLabel(
                label=display_name or referenced_table,
                language_code=language_code,
            )
        ]

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

        return self.create_one_to_many_relationship(lookup, relationship, solution=solution)
