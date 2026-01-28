# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Table metadata operations namespace."""

from __future__ import annotations

from typing import Any, Dict, Optional, List, Union, TYPE_CHECKING

from ..core.results import OperationResult
from ..models.table_info import TableInfo
from ..models.alternate_key import AlternateKeyInfo

if TYPE_CHECKING:
    from ..client import DataverseClient


class TableOperations:
    """
    Table metadata and schema operations.

    Accessed via ``client.tables``. Provides methods for creating, deleting,
    and inspecting tables and columns.

    Example:
        Create a custom table::

            result = client.tables.create(
                "new_Product",
                columns={
                    "new_Name": "string",
                    "new_Price": "decimal",
                    "new_Quantity": "int"
                }
            )
            print(f"Created: {result['table_schema_name']}")

        Inspect table metadata::

            info = client.tables.info("account")
            if info:
                print(f"Entity set: {info['entity_set_name']}")

        List custom tables::

            for table in client.tables.list():
                print(table["table_schema_name"])
    """

    def __init__(self, client: "DataverseClient") -> None:
        """
        Initialize TableOperations.

        :param client: Parent DataverseClient instance.
        :type client: DataverseClient
        """
        self._client = client

    def create(
        self,
        table: str,
        columns: Dict[str, Any],
        *,
        solution: Optional[str] = None,
        primary_column: Optional[str] = None,
    ) -> OperationResult[TableInfo]:
        """
        Create a custom table with columns.

        :param table: Table schema name with prefix (e.g., "new_Product").
        :type table: str
        :param columns: Column definitions {name: type}.
            Types: "string", "int", "decimal", "float", "datetime", "bool",
            or IntEnum subclass for option sets.
        :type columns: dict
        :param solution: Optional solution unique name.
        :type solution: str or None
        :param primary_column: Optional primary column schema name.
        :type primary_column: str or None
        :return: OperationResult with TableInfo object.
        :rtype: OperationResult[TableInfo]

        :raises ~PowerPlatform.Dataverse.core.errors.MetadataError: If table creation fails
            or the schema is invalid.

        Example:
            Create a table with simple columns::

                from enum import IntEnum

                class Status(IntEnum):
                    ACTIVE = 1
                    INACTIVE = 2

                result = client.tables.create(
                    "new_Product",
                    columns={
                        "new_Name": "string",
                        "new_Status": Status
                    },
                    solution="MySolution"
                )
                print(f"Created: {result['table_schema_name']}")  # Dict-like access
                print(f"Schema: {result.schema_name}")            # Structured access

            Access telemetry data::

                response = client.tables.create("new_Test", {"new_Col": "string"}).with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._client._scoped_odata() as od:
            result, metadata = od._create_table(
                table,
                columns,
                solution,
                primary_column,
            )
            table_info = TableInfo.from_dict(result)
            return OperationResult(table_info, metadata)

    def delete(self, table: str) -> OperationResult[None]:
        """
        Delete a custom table.

        .. warning::
            This is irreversible and deletes all records.

        :param table: Table schema name.
        :type table: str
        :return: OperationResult containing None.
        :rtype: OperationResult[None]

        :raises ~PowerPlatform.Dataverse.core.errors.MetadataError: If the table does not exist
            or deletion fails.

        Example:
            Delete a custom table::

                client.tables.delete("new_MyTestTable")

            Access telemetry data::

                response = client.tables.delete("new_MyTestTable").with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._client._scoped_odata() as od:
            _, metadata = od._delete_table(table)
            return OperationResult(None, metadata)

    def info(self, table: str) -> OperationResult[Optional[TableInfo]]:
        """
        Get table metadata.

        :param table: Table schema name.
        :type table: str
        :return: OperationResult with TableInfo object or None if not found.
        :rtype: OperationResult[Optional[TableInfo]]

        Example:
            Retrieve table metadata::

                info = client.tables.info("account")
                if info:
                    print(f"Logical name: {info['table_logical_name']}")  # Dict-like access
                    print(f"Entity set: {info['entity_set_name']}")
                    print(f"Schema: {info.schema_name}")                  # Structured access

            Access telemetry data::

                response = client.tables.info("account").with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._client._scoped_odata() as od:
            result, metadata = od._get_table_info(table)
            if result is None:
                return OperationResult(None, metadata)
            table_info = TableInfo.from_dict(result)
            return OperationResult(table_info, metadata)

    def list(self) -> OperationResult[List[TableInfo]]:
        """
        List all custom tables.

        :return: OperationResult with list of TableInfo objects.
        :rtype: OperationResult[List[TableInfo]]

        Example:
            List all custom tables::

                tables = client.tables.list()
                for table in tables:
                    print(table["table_schema_name"])  # Dict-like access
                    print(table.schema_name)          # Structured access

            Access telemetry data::

                response = client.tables.list().with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._client._scoped_odata() as od:
            result, metadata = od._list_tables()
            table_infos = [TableInfo.from_dict(table_dict) for table_dict in result]
            return OperationResult(table_infos, metadata)

    def add_columns(
        self,
        table: str,
        columns: Dict[str, Any],
    ) -> OperationResult[List[str]]:
        """
        Add columns to an existing table.

        :param table: Table schema name.
        :type table: str
        :param columns: Column definitions {name: type}.
        :type columns: dict
        :return: OperationResult with list of created column names.
        :rtype: OperationResult[List[str]]

        Example:
            Add columns to an existing table::

                created = client.tables.add_columns(
                    "new_Product",
                    {"new_Description": "string", "new_InStock": "bool"}
                )
                print(created)  # ['new_Description', 'new_InStock']

            Access telemetry data::

                response = client.tables.add_columns("new_Test", {"new_Col": "string"}).with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._client._scoped_odata() as od:
            result, metadata = od._create_columns(
                table,
                columns,
            )
            return OperationResult(result, metadata)

    def remove_columns(
        self,
        table: str,
        columns: Union[str, List[str]],
    ) -> OperationResult[List[str]]:
        """
        Remove columns from a table.

        :param table: Table schema name.
        :type table: str
        :param columns: Column name or list of column names.
        :type columns: str or list[str]
        :return: OperationResult with list of removed column names.
        :rtype: OperationResult[List[str]]

        Example:
            Remove columns from a table::

                removed = client.tables.remove_columns(
                    "new_Product",
                    ["new_Scratch", "new_Flags"]
                )
                print(removed)  # ['new_Scratch', 'new_Flags']

            Access telemetry data::

                response = client.tables.remove_columns("new_Test", ["new_Col"]).with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._client._scoped_odata() as od:
            result, metadata = od._delete_columns(
                table,
                columns,
            )
            return OperationResult(result, metadata)

    # ---------------------- Alternate Key Operations -------------------------

    def create_key(
        self,
        table: str,
        key_name: str,
        columns: List[str],
        *,
        display_name: Optional[str] = None,
    ) -> OperationResult[AlternateKeyInfo]:
        """
        Create an alternate key on a table.

        Alternate keys allow records to be uniquely identified using business keys
        instead of the primary GUID. They are required for upsert operations.

        .. note::
            Alternate key index creation runs asynchronously. The key may have
            status "Pending" or "InProgress" immediately after creation.
            Use ``get_key()`` to check when status becomes "Active".

        :param table: Table schema name.
        :type table: str
        :param key_name: Schema name for the key (e.g., "AccountNumberKey").
        :type key_name: str
        :param columns: Column logical names that make up the key.
        :type columns: list[str]
        :param display_name: Optional display name for the key.
        :type display_name: str or None
        :return: OperationResult with AlternateKeyInfo object.
        :rtype: OperationResult[AlternateKeyInfo]

        :raises ~PowerPlatform.Dataverse.core.errors.MetadataError: If table doesn't exist,
            key already exists, columns are invalid, or max keys exceeded (10 per table).

        Example:
            Create a simple alternate key::

                key = client.tables.create_key(
                    "account",
                    key_name="AccountNumberKey",
                    columns=["accountnumber"]
                )
                print(f"Created key: {key.schema_name}")
                print(f"Status: {key.status}")

            Create a composite alternate key::

                key = client.tables.create_key(
                    "store",
                    key_name="RegionStoreKey",
                    columns=["my_region", "my_storeid"],
                    display_name="Region and Store ID"
                )
        """
        with self._client._scoped_odata() as od:
            result, metadata = od._create_key(
                table,
                key_name,
                columns,
                display_name,
            )
            key_info = AlternateKeyInfo.from_api_response(result)
            return OperationResult(key_info, metadata)

    def list_keys(self, table: str) -> OperationResult[List[AlternateKeyInfo]]:
        """
        List all alternate keys for a table.

        :param table: Table schema name.
        :type table: str
        :return: OperationResult with list of AlternateKeyInfo objects.
        :rtype: OperationResult[List[AlternateKeyInfo]]

        :raises ~PowerPlatform.Dataverse.core.errors.MetadataError: If table doesn't exist.

        Example:
            List alternate keys::

                keys = client.tables.list_keys("account")
                for key in keys:
                    print(f"Key: {key.schema_name}")
                    print(f"  Columns: {key.columns}")
                    print(f"  Status: {key.status}")
        """
        with self._client._scoped_odata() as od:
            result, metadata = od._list_keys(table)
            key_infos = [AlternateKeyInfo.from_api_response(key) for key in result]
            return OperationResult(key_infos, metadata)

    def get_key(
        self,
        table: str,
        key_name: str,
    ) -> OperationResult[Optional[AlternateKeyInfo]]:
        """
        Get a specific alternate key by name.

        :param table: Table schema name.
        :type table: str
        :param key_name: Schema name of the key.
        :type key_name: str
        :return: OperationResult with AlternateKeyInfo or None if not found.
        :rtype: OperationResult[Optional[AlternateKeyInfo]]

        :raises ~PowerPlatform.Dataverse.core.errors.MetadataError: If table doesn't exist.

        Example:
            Get a specific alternate key::

                key = client.tables.get_key("account", "AccountNumberKey")
                if key:
                    print(f"Status: {key.status}")
                    if key.status == "Active":
                        print("Key is ready for use")
                else:
                    print("Key not found")
        """
        with self._client._scoped_odata() as od:
            result, metadata = od._get_key(table, key_name)
            if result is None:
                return OperationResult(None, metadata)
            key_info = AlternateKeyInfo.from_api_response(result)
            return OperationResult(key_info, metadata)

    def delete_key(
        self,
        table: str,
        key_name: str,
    ) -> OperationResult[None]:
        """
        Delete an alternate key from a table.

        :param table: Table schema name.
        :type table: str
        :param key_name: Schema name of the key to delete.
        :type key_name: str
        :return: OperationResult containing None.
        :rtype: OperationResult[None]

        :raises ~PowerPlatform.Dataverse.core.errors.MetadataError: If table or key doesn't exist.

        Example:
            Delete an alternate key::

                client.tables.delete_key("account", "AccountNumberKey")
        """
        with self._client._scoped_odata() as od:
            _, metadata = od._delete_key(table, key_name)
            return OperationResult(None, metadata)


__all__ = ["TableOperations"]
