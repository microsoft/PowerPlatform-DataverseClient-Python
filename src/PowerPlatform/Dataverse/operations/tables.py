# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Table metadata operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

from ..models.relationship import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
    CascadeConfiguration,
    RelationshipInfo,
)
from ..models.labels import Label, LocalizedLabel
from ..models.metadata import ColumnMetadata, OptionSetInfo
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

    def get(
        self,
        table: str,
        *,
        select: Optional[List[str]] = None,
        include_columns: bool = False,
        include_relationships: bool = False,
    ) -> Optional[Dict[str, Any]]:
        """Get basic or extended metadata for a table if it exists.

        When no extra parameters are passed, returns the same lightweight
        result as before (backward compatible). Use optional parameters to
        request richer metadata including columns and relationships.

        :param table: Schema name of the table (e.g. ``"new_MyTestTable"``
            or ``"account"``).
        :type table: :class:`str`
        :param select: Optional list of PascalCase EntityDefinition property
            names to include (e.g. ``["DisplayName", "Description"]``).
        :type select: :class:`list` of :class:`str` or None
        :param include_columns: If ``True``, expands and returns all column
            metadata as :class:`~PowerPlatform.Dataverse.models.metadata.ColumnMetadata`
            instances in the ``columns`` key.
        :type include_columns: :class:`bool`
        :param include_relationships: If ``True``, expands and returns
            ``one_to_many_relationships``, ``many_to_one_relationships``, and
            ``many_to_many_relationships``.
        :type include_relationships: :class:`bool`

        :return: Dictionary containing ``table_schema_name``,
            ``table_logical_name``, ``entity_set_name``, and ``metadata_id``.
            When extended params are used, may also include ``columns``,
            ``one_to_many_relationships``, ``many_to_one_relationships``,
            ``many_to_many_relationships``, and any extra selected properties.
            Returns None if the table is not found.
        :rtype: :class:`dict` or None

        Example::

            # Basic usage (unchanged)
            info = client.tables.get("new_MyTestTable")
            if info:
                print(f"Logical name: {info['table_logical_name']}")

            # Extended with columns
            info = client.tables.get("account", include_columns=True)
            for col in info.get("columns", []):
                print(f"{col.logical_name} ({col.attribute_type})")

            # Extended with relationships
            info = client.tables.get("account", include_relationships=True)
        """
        # When no extra parameters are passed, use the original lightweight lookup.
        # This ensures backward compatibility -- existing callers get identical behavior.
        if not include_columns and not include_relationships and select is None:
            with self._client._scoped_odata() as od:
                return od._get_table_info(table)

        # Extended metadata retrieval when any extra parameter is used
        with self._client._scoped_odata() as od:
            raw = od._get_table_metadata(
                table,
                select=select,
                include_attributes=include_columns,
                include_one_to_many=include_relationships,
                include_many_to_one=include_relationships,
                include_many_to_many=include_relationships,
            )
        if raw is None:
            return None

        # Build result dict starting with the standard 4 fields
        result: Dict[str, Any] = {
            "table_schema_name": raw.get("SchemaName", table),
            "table_logical_name": raw.get("LogicalName"),
            "entity_set_name": raw.get("EntitySetName"),
            "metadata_id": raw.get("MetadataId"),
        }

        # Include any extra selected entity properties
        if select:
            for prop in select:
                if prop not in ("SchemaName", "LogicalName", "EntitySetName", "MetadataId"):
                    result[prop] = raw.get(prop)

        # Convert expanded Attributes into ColumnMetadata instances
        if include_columns and "Attributes" in raw:
            result["columns"] = [ColumnMetadata.from_api_response(a) for a in raw["Attributes"]]

        # Include expanded relationship collections as raw dicts
        if include_relationships:
            for raw_key, result_key in (
                ("OneToManyRelationships", "one_to_many_relationships"),
                ("ManyToOneRelationships", "many_to_one_relationships"),
                ("ManyToManyRelationships", "many_to_many_relationships"),
            ):
                if raw_key in raw:
                    result[result_key] = raw[raw_key]

        return result

    # -------------------------------------------------------------- get_columns

    def get_columns(
        self,
        table: str,
        *,
        select: Optional[List[str]] = None,
        filter: Optional[str] = None,
    ) -> List[ColumnMetadata]:
        """Get column (attribute) metadata for a table.

        Returns a list of :class:`~PowerPlatform.Dataverse.models.metadata.ColumnMetadata`
        instances representing each column in the table.

        :param table: Schema name of the table (e.g. ``"account"``
            or ``"new_MyTestTable"``).
        :type table: :class:`str`
        :param select: Optional list of attribute metadata property names to
            include (PascalCase, e.g. ``["LogicalName", "AttributeType"]``).
            When ``None``, returns a default set of useful properties.
        :type select: :class:`list` of :class:`str` or None
        :param filter: Optional OData ``$filter`` expression. Column names in
            filter expressions must use PascalCase metadata property names.

            .. note::
                Enum values in filters must use fully-qualified type names,
                e.g. ``"AttributeType eq Microsoft.Dynamics.CRM.AttributeTypeCode'Picklist'"``.

            .. note::
                The ``filter`` expression is passed directly to the Web API.
                If constructing filters from external input, ensure values are
                properly escaped (single quotes doubled) to avoid malformed queries.

        :type filter: :class:`str` or None

        :return: List of column metadata.
        :rtype: :class:`list` of :class:`~PowerPlatform.Dataverse.models.metadata.ColumnMetadata`

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError:
            If the Web API request fails.

        Example::

            # Get all columns
            columns = client.tables.get_columns("account")
            for col in columns:
                print(f"{col.logical_name}: {col.attribute_type}")

            # Filter to only picklist columns
            picklists = client.tables.get_columns(
                "account",
                filter="AttributeType eq Microsoft.Dynamics.CRM.AttributeTypeCode'Picklist'",
            )
        """
        with self._client._scoped_odata() as od:
            raw_list = od._get_table_columns(table, select=select, filter=filter)
        return [ColumnMetadata.from_api_response(item) for item in raw_list]

    # --------------------------------------------------------------- get_column

    def get_column(
        self,
        table: str,
        column: str,
        *,
        select: Optional[List[str]] = None,
    ) -> Optional[ColumnMetadata]:
        """Get metadata for a single column by logical name.

        :param table: Schema name of the table (e.g. ``"account"``).
        :type table: :class:`str`
        :param column: Logical name of the column (e.g. ``"emailaddress1"``).
        :type column: :class:`str`
        :param select: Optional list of attribute metadata property names to
            include (PascalCase). When ``None``, returns all properties.
        :type select: :class:`list` of :class:`str` or None

        :return: Column metadata, or ``None`` if the column is not found.
        :rtype: :class:`~PowerPlatform.Dataverse.models.metadata.ColumnMetadata`
            or None

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError:
            If the Web API request fails (other than 404).

        Example::

            col = client.tables.get_column("account", "emailaddress1")
            if col:
                print(f"Type: {col.attribute_type}, Required: {col.required_level}")
        """
        with self._client._scoped_odata() as od:
            raw = od._get_table_column(table, column, select=select)
        if raw is None:
            return None
        return ColumnMetadata.from_api_response(raw)

    # -------------------------------------------------------- get_column_options

    def get_column_options(
        self,
        table: str,
        column: str,
    ) -> Optional[OptionSetInfo]:
        """Get option set values for a Picklist, MultiSelect, or Boolean column.

        This method retrieves the available choices for a column that uses an
        option set. For Picklist and MultiSelect columns, the options are the
        defined choice values. For Boolean columns, the result contains the
        True and False option labels.

        :param table: Schema name of the table (e.g. ``"account"``).
        :type table: :class:`str`
        :param column: Logical name of the column (e.g.
            ``"accountcategorycode"``).
        :type column: :class:`str`

        :return: Option set information with available choices, or ``None`` if
            the column is not a choice/boolean type.
        :rtype: :class:`~PowerPlatform.Dataverse.models.metadata.OptionSetInfo`
            or None

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError:
            If the Web API request fails (other than expected type mismatches).

        Example::

            options = client.tables.get_column_options("account", "accountcategorycode")
            if options:
                for opt in options.options:
                    print(f"  {opt.value}: {opt.label}")
        """
        with self._client._scoped_odata() as od:
            raw = od._get_column_optionset(table, column)
        if raw is None:
            return None
        return OptionSetInfo.from_api_response(raw)

    # -------------------------------------------------------- list_relationships

    def list_relationships(
        self,
        table: str,
        *,
        relationship_type: Optional[str] = None,
        select: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """List relationship metadata for a table.

        Returns relationship definitions from the Web API. Each dict in the
        result has a ``_relationship_type`` key added by the SDK with value
        ``"OneToMany"``, ``"ManyToOne"``, or ``"ManyToMany"`` to identify the
        relationship category.

        :param table: Schema name of the table (e.g. ``"account"``).
        :type table: :class:`str`
        :param relationship_type: Filter by relationship type. Valid values:
            ``"one_to_many"`` (or ``"1:N"``), ``"many_to_one"``
            (or ``"N:1"``), ``"many_to_many"`` (or ``"N:N"``), or
            ``None`` (default) to return all types.
        :type relationship_type: :class:`str` or None
        :param select: Optional list of relationship property names to include
            (PascalCase, e.g. ``["SchemaName", "ReferencedEntity"]``).
        :type select: :class:`list` of :class:`str` or None

        :return: List of relationship metadata dictionaries from the Web API.
        :rtype: :class:`list` of :class:`dict`

        :raises ValueError: If ``relationship_type`` is not a valid value.
        :raises ~PowerPlatform.Dataverse.core.errors.HttpError:
            If the Web API request fails.

        Example::

            # All relationships
            rels = client.tables.list_relationships("account")

            # Only one-to-many
            rels = client.tables.list_relationships(
                "account",
                relationship_type="one_to_many",
            )
            for rel in rels:
                print(f"{rel['SchemaName']}: {rel.get('ReferencingEntity')}")
        """
        with self._client._scoped_odata() as od:
            return od._list_table_relationships(
                table,
                relationship_type=relationship_type,
                select=select,
            )

    # ------------------------------------------------------------------- list

    def list(
        self,
        *,
        filter: Optional[str] = None,
        select: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """List all non-private tables in the Dataverse environment.

        By default returns every table where ``IsPrivate eq false``.  Supply
        an optional OData ``$filter`` expression to further narrow the results.
        The expression is combined with the default ``IsPrivate eq false``
        clause using ``and``.

        :param filter: Optional OData ``$filter`` expression to further narrow
            the list of returned tables (e.g.
            ``"SchemaName eq 'Account'"``).  Column names in filter
            expressions must use the exact property names from the
            ``EntityDefinitions`` metadata (typically PascalCase).
        :type filter: :class:`str` or None
        :param select: Optional list of property names to include in the
            response (projected via the OData ``$select`` query option).
            Property names must use the exact PascalCase names from the
            ``EntityDefinitions`` metadata (e.g.
            ``["LogicalName", "SchemaName", "DisplayName"]``).
            When ``None`` (the default) or an empty list, all properties are
            returned.
        :type select: :class:`list` of :class:`str` or None

        :return: List of EntityDefinition metadata dictionaries.
        :rtype: :class:`list` of :class:`dict`

        Example::

            # List all non-private tables
            tables = client.tables.list()
            for table in tables:
                print(table["LogicalName"])

            # List only tables whose schema name starts with "new_"
            custom_tables = client.tables.list(
                filter="startswith(SchemaName, 'new_')"
            )

            # List tables with only specific properties
            tables = client.tables.list(
                select=["LogicalName", "SchemaName", "EntitySetName"]
            )
        """
        with self._client._scoped_odata() as od:
            return od._list_tables(filter=filter, select=select)

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
    ) -> RelationshipInfo:
        """Create a one-to-many relationship between tables.

        This operation creates both the relationship and the lookup attribute
        on the referencing table.

        :param lookup: Metadata defining the lookup attribute.
        :type lookup: ~PowerPlatform.Dataverse.models.relationship.LookupAttributeMetadata
        :param relationship: Metadata defining the relationship.
        :type relationship: ~PowerPlatform.Dataverse.models.relationship.OneToManyRelationshipMetadata
        :param solution: Optional solution unique name to add relationship to.
        :type solution: :class:`str` or None

        :return: Relationship metadata with ``relationship_id``,
            ``relationship_schema_name``, ``relationship_type``,
            ``lookup_schema_name``, ``referenced_entity``, and
            ``referencing_entity``.
        :rtype: :class:`~PowerPlatform.Dataverse.models.relationship.RelationshipInfo`

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError:
            If the Web API request fails.

        Example:
            Create a one-to-many relationship: Department (1) -> Employee (N)::

                from PowerPlatform.Dataverse.models.relationship import (
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
                print(f"Created lookup field: {result.lookup_schema_name}")
        """
        with self._client._scoped_odata() as od:
            raw = od._create_one_to_many_relationship(
                lookup,
                relationship,
                solution,
            )
            return RelationshipInfo.from_one_to_many(
                relationship_id=raw["relationship_id"],
                relationship_schema_name=raw["relationship_schema_name"],
                lookup_schema_name=raw["lookup_schema_name"],
                referenced_entity=raw["referenced_entity"],
                referencing_entity=raw["referencing_entity"],
            )

    # ----------------------------------------------------- create_many_to_many

    def create_many_to_many_relationship(
        self,
        relationship: ManyToManyRelationshipMetadata,
        *,
        solution: Optional[str] = None,
    ) -> RelationshipInfo:
        """Create a many-to-many relationship between tables.

        This operation creates a many-to-many relationship and an intersect
        table to manage the relationship.

        :param relationship: Metadata defining the many-to-many relationship.
        :type relationship: ~PowerPlatform.Dataverse.models.relationship.ManyToManyRelationshipMetadata
        :param solution: Optional solution unique name to add relationship to.
        :type solution: :class:`str` or None

        :return: Relationship metadata with ``relationship_id``,
            ``relationship_schema_name``, ``relationship_type``,
            ``entity1_logical_name``, and ``entity2_logical_name``.
        :rtype: :class:`~PowerPlatform.Dataverse.models.relationship.RelationshipInfo`

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError:
            If the Web API request fails.

        Example:
            Create a many-to-many relationship: Employee <-> Project::

                from PowerPlatform.Dataverse.models.relationship import (
                    ManyToManyRelationshipMetadata,
                )

                relationship = ManyToManyRelationshipMetadata(
                    schema_name="new_employee_project",
                    entity1_logical_name="new_employee",
                    entity2_logical_name="new_project",
                )

                result = client.tables.create_many_to_many_relationship(relationship)
                print(f"Created: {result.relationship_schema_name}")
        """
        with self._client._scoped_odata() as od:
            raw = od._create_many_to_many_relationship(
                relationship,
                solution,
            )
            return RelationshipInfo.from_many_to_many(
                relationship_id=raw["relationship_id"],
                relationship_schema_name=raw["relationship_schema_name"],
                entity1_logical_name=raw["entity1_logical_name"],
                entity2_logical_name=raw["entity2_logical_name"],
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

    def get_relationship(self, schema_name: str) -> Optional[RelationshipInfo]:
        """Retrieve relationship metadata by schema name.

        :param schema_name: The schema name of the relationship.
        :type schema_name: :class:`str`

        :return: Relationship metadata, or ``None`` if not found.
        :rtype: :class:`~PowerPlatform.Dataverse.models.relationship.RelationshipInfo`
            or None

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError:
            If the Web API request fails.

        Example::

            rel = client.tables.get_relationship("new_Department_Employee")
            if rel:
                print(f"Found: {rel.relationship_schema_name}")
        """
        with self._client._scoped_odata() as od:
            raw = od._get_relationship(schema_name)
            if raw is None:
                return None
            return RelationshipInfo.from_api_response(raw)

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
    ) -> RelationshipInfo:
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

        :return: Relationship metadata with ``relationship_id``,
            ``relationship_schema_name``, ``relationship_type``,
            ``lookup_schema_name``, ``referenced_entity``, and
            ``referencing_entity``.
        :rtype: :class:`~PowerPlatform.Dataverse.models.relationship.RelationshipInfo`

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
