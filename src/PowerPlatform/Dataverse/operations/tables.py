# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Table metadata operations namespace for the Dataverse SDK."""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union, TYPE_CHECKING

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
        :type table: str
        :param columns: Mapping of column schema names (with customization
            prefix) to their types. Supported types include ``"string"``
            (or ``"text"``), ``"int"`` (or ``"integer"``), ``"decimal"``
            (or ``"money"``), ``"float"`` (or ``"double"``), ``"datetime"``
            (or ``"date"``), ``"bool"`` (or ``"boolean"``), ``"file"``, and
            ``Enum`` subclasses
            (for local option sets).
        :type columns: dict[str, Any]
        :param solution: Optional solution unique name that should own the new
            table. When omitted the table is created in the default solution.
        :type solution: str | None
        :param primary_column: Optional primary name column schema name with
            customization prefix (e.g. ``"new_ProductName"``). If not provided,
            defaults to ``"{prefix}_Name"``.
        :type primary_column: str | None

        :return: Dictionary containing table metadata including
            ``table_schema_name``, ``entity_set_name``, ``table_logical_name``,
            ``metadata_id``, and ``columns_created``.
        :rtype: dict[str, Any]

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
        :type table: str

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
        :type table: str

        :return: Dictionary containing ``table_schema_name``,
            ``table_logical_name``, ``entity_set_name``, and ``metadata_id``.
            Returns None if the table is not found.
        :rtype: dict[str, Any] | None

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
        :rtype: list[dict[str, Any]]

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
        :type table: str
        :param columns: Mapping of column schema names (with customization
            prefix) to their types. Supported types are the same as for
            :meth:`create`.
        :type columns: dict[str, Any]

        :return: Schema names of the columns that were created.
        :rtype: list[str]

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
        :type table: str
        :param columns: Column schema name or list of column schema names to
            remove. Must include the customization prefix (e.g.
            ``"new_TestColumn"``).
        :type columns: str | list[str]

        :return: Schema names of the columns that were removed.
        :rtype: list[str]

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
