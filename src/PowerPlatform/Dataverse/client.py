# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from __future__ import annotations

import warnings
from contextlib import contextmanager
from typing import Any, Dict, Iterable, Iterator, List, Optional, Union

from azure.core.credentials import TokenCredential

from .core._auth import _AuthManager
from .core.config import DataverseConfig
from .data._odata import _ODataClient
from .models.metadata import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
    Label,
    LocalizedLabel,
    CascadeConfiguration,
)
from .common.constants import CASCADE_BEHAVIOR_REMOVE_LINK
from .operations.records import RecordOperations
from .operations.query import QueryOperations
from .operations.tables import TableOperations


class DataverseClient:
    """
    High-level client for Microsoft Dataverse operations.

    This client provides a simple, stable interface for interacting with Dataverse environments
    through the Web API. It handles authentication via Azure Identity and delegates HTTP operations
    to an internal :class:`~PowerPlatform.Dataverse.data._odata._ODataClient`.

    Key capabilities:
        - OData CRUD operations: create, read, update, delete records
        - SQL queries: execute read-only SQL via Web API ``?sql`` parameter
        - Table metadata: create, inspect, and delete custom tables; create and delete columns
        - File uploads: upload files to file columns with chunking support

    :param base_url: Your Dataverse environment URL, for example
        ``"https://org.crm.dynamics.com"``. Trailing slash is automatically removed.
    :type base_url: :class:`str`
    :param credential: Azure Identity credential for authentication.
    :type credential: ~azure.core.credentials.TokenCredential
    :param config: Optional configuration for language, timeouts, and retries.
        If not provided, defaults are loaded from :meth:`~PowerPlatform.Dataverse.core.config.DataverseConfig.from_env`.
    :type config: ~PowerPlatform.Dataverse.core.config.DataverseConfig or None

    :raises ValueError: If ``base_url`` is missing or empty after trimming.

    .. note::
        The client lazily initializes its internal OData client on first use, allowing lightweight construction without immediate network calls.

    .. note::
        All methods that communicate with the Dataverse Web API may raise
        :class:`~PowerPlatform.Dataverse.core.errors.HttpError` on non-successful
        HTTP responses (e.g. 401, 403, 404, 429, 500). Individual method
        docstrings document only domain-specific exceptions.

    Operations are organized into namespaces:

    - ``client.records`` -- create, update, delete, get individual records
    - ``client.query`` -- paginated OData queries and read-only SQL queries (via Web API ``?sql=`` parameter)
    - ``client.tables`` -- table and column metadata management

    Example:
        Create a client and perform basic operations::

            from azure.identity import InteractiveBrowserCredential
            from PowerPlatform.Dataverse.client import DataverseClient

            credential = InteractiveBrowserCredential()
            client = DataverseClient(
                "https://org.crm.dynamics.com",
                credential
            )

            # Create a record
            record_id = client.records.create("account", {"name": "Contoso Ltd"})

            # Update a record
            client.records.update("account", record_id, {"telephone1": "555-0100"})

            # Query records
            for page in client.query.get("account", filter="name eq 'Contoso Ltd'"):
                for account in page:
                    print(account["name"])

            # Delete a record
            client.records.delete("account", record_id)
    """

    def __init__(
        self,
        base_url: str,
        credential: TokenCredential,
        config: Optional[DataverseConfig] = None,
    ) -> None:
        self.auth = _AuthManager(credential)
        self._base_url = (base_url or "").rstrip("/")
        if not self._base_url:
            raise ValueError("base_url is required.")
        self._config = config or DataverseConfig.from_env()
        self._odata: Optional[_ODataClient] = None

        # Operation namespaces
        self.records = RecordOperations(self)
        self.query = QueryOperations(self)
        self.tables = TableOperations(self)

    def _get_odata(self) -> _ODataClient:
        """
        Get or create the internal OData client instance.

        This method implements lazy initialization of the low-level OData client,
        deferring construction until the first API call.

        :return: The lazily-initialized low-level client used to perform HTTP requests.
        :rtype: ~PowerPlatform.Dataverse.data._odata._ODataClient
        """
        if self._odata is None:
            self._odata = _ODataClient(
                self.auth,
                self._base_url,
                self._config,
            )
        return self._odata

    @contextmanager
    def _scoped_odata(self) -> Iterator[_ODataClient]:
        """Yield the low-level client while ensuring a correlation scope is active."""
        od = self._get_odata()
        with od._call_scope():
            yield od

    # ---------------- Unified CRUD: create/update/delete ----------------
    def create(self, table_schema_name: str, records: Union[Dict[str, Any], List[Dict[str, Any]]]) -> List[str]:
        """
        .. note::
            Deprecated. Use :meth:`~PowerPlatform.Dataverse.operations.records.RecordOperations.create` instead.

        Create one or more records by table name.

        :param table_schema_name: Schema name of the table (e.g. ``"account"``, ``"contact"``, or ``"new_MyTestTable"``).
        :type table_schema_name: :class:`str`
        :param records: A single record dictionary or a list of record dictionaries.
            Each dictionary should contain column schema names as keys.
        :type records: :class:`dict` or :class:`list` of :class:`dict`

        :return: List of created record GUIDs. Returns a single-element list for a single input.
        :rtype: :class:`list` of :class:`str`

        :raises TypeError: If ``records`` is not a dict or list[dict], or if the internal
            client returns an unexpected type.

        Example:
            Create a single record::

                client = DataverseClient(base_url, credential)
                ids = client.create("account", {"name": "Contoso"})
                print(f"Created: {ids[0]}")

            Create multiple records::

                records = [
                    {"name": "Contoso"},
                    {"name": "Fabrikam"}
                ]
                ids = client.create("account", records)
                print(f"Created {len(ids)} accounts")
        """
        warnings.warn(
            "client.create() is deprecated. Use client.records.create() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        # Old API always returned list[str]; new returns str for single
        if isinstance(records, dict):
            return [self.records.create(table_schema_name, records)]
        return self.records.create(table_schema_name, records)

    def update(
        self, table_schema_name: str, ids: Union[str, List[str]], changes: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> None:
        """
        .. note::
            Deprecated. Use :meth:`~PowerPlatform.Dataverse.operations.records.RecordOperations.update` instead.

        Update one or more records.

        This method supports three usage patterns:

        1. Single record update: ``update("account", "guid", {"name": "New Name"})``
        2. Broadcast update: ``update("account", [id1, id2], {"status": 1})`` - applies same changes to all IDs
        3. Paired updates: ``update("account", [id1, id2], [changes1, changes2])`` - one-to-one mapping

        :param table_schema_name:  Schema name of the table (e.g. ``"account"`` or ``"new_MyTestTable"``).
        :type table_schema_name: :class:`str`
        :param ids: Single GUID string or list of GUID strings to update.
        :type ids: :class:`str` or :class:`list` of :class:`str`
        :param changes: Dictionary of changes for single/broadcast mode, or list of dictionaries
            for paired mode. When ``ids`` is a list and ``changes`` is a single dict,
            the same changes are broadcast to all records. When both are lists, they must
            have equal length for one-to-one mapping.
        :type changes: :class:`dict` or :class:`list` of :class:`dict`

        :raises TypeError: If ``ids`` is not str or list[str], or if ``changes`` type doesn't match usage pattern.

        .. note::
            Single updates discard the response representation for better performance. For broadcast or paired updates, the method delegates to the internal client's batch update logic.

        Example:
            Single record update::

                client.update("account", account_id, {"telephone1": "555-0100"})

            Broadcast same changes to multiple records::

                client.update("account", [id1, id2, id3], {"statecode": 1})

            Update multiple records with different values::

                ids = [id1, id2]
                changes = [
                    {"name": "Updated Name 1"},
                    {"name": "Updated Name 2"}
                ]
                client.update("account", ids, changes)
        """
        warnings.warn(
            "client.update() is deprecated. Use client.records.update() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.records.update(table_schema_name, ids, changes)

    def delete(
        self,
        table_schema_name: str,
        ids: Union[str, List[str]],
        use_bulk_delete: bool = True,
    ) -> Optional[str]:
        """
        .. note::
            Deprecated. Use :meth:`~PowerPlatform.Dataverse.operations.records.RecordOperations.delete` instead.

        Delete one or more records by GUID.

        :param table_schema_name: Schema name of the table (e.g. ``"account"`` or ``"new_MyTestTable"``).
        :type table_schema_name: :class:`str`
        :param ids: Single GUID string or list of GUID strings to delete.
        :type ids: :class:`str` or :class:`list` of :class:`str`
        :param use_bulk_delete: When ``True`` (default) and ``ids`` is a list, execute the BulkDelete action and
            return its async job identifier. When ``False`` each record is deleted sequentially.
        :type use_bulk_delete: :class:`bool`

        :raises TypeError: If ``ids`` is not str or list[str].
        :raises HttpError: If the underlying Web API delete request fails.

        :return: BulkDelete job ID when deleting multiple records via BulkDelete; otherwise ``None``.
        :rtype: :class:`str` or None

        Example:
            Delete a single record::

                client.delete("account", account_id)

            Delete multiple records::

                job_id = client.delete("account", [id1, id2, id3])
        """
        warnings.warn(
            "client.delete() is deprecated. Use client.records.delete() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.records.delete(table_schema_name, ids, use_bulk_delete=use_bulk_delete)

    def get(
        self,
        table_schema_name: str,
        record_id: Optional[str] = None,
        select: Optional[List[str]] = None,
        filter: Optional[str] = None,
        orderby: Optional[List[str]] = None,
        top: Optional[int] = None,
        expand: Optional[List[str]] = None,
        page_size: Optional[int] = None,
    ) -> Union[Dict[str, Any], Iterable[List[Dict[str, Any]]]]:
        """
        .. note::
            Deprecated. This method has been split into two:

            - **Single record by ID** -- use ``client.records.get(table, record_id)``
            - **Query / filter multiple records** -- use ``client.query.get(table, filter=..., select=...)``

        Fetch a single record by ID or query multiple records.

        When ``record_id`` is provided, returns a single record dictionary.
        When ``record_id`` is None, returns a generator yielding batches of records.

        :param table_schema_name: Schema name of the table (e.g. ``"account"`` or ``"new_MyTestTable"``).
        :type table_schema_name: :class:`str`
        :param record_id: Optional GUID to fetch a specific record. If None, queries multiple records.
        :type record_id: :class:`str` or None
        :param select: Optional list of attribute logical names to retrieve. Column names are case-insensitive and automatically lowercased (e.g. ``["new_Title", "new_Amount"]`` becomes ``"new_title,new_amount"``).
        :type select: :class:`list` of :class:`str` or None
        :param filter: Optional OData filter string, e.g. ``"name eq 'Contoso'"`` or ``"new_quantity gt 5"``. Column names in filter expressions must use exact lowercase logical names (e.g. ``"new_quantity"``, not ``"new_Quantity"``). The filter string is passed directly to the Dataverse Web API without transformation.
        :type filter: :class:`str` or None
        :param orderby: Optional list of attributes to sort by, e.g. ``["name asc", "createdon desc"]``. Column names are automatically lowercased.
        :type orderby: :class:`list` of :class:`str` or None
        :param top: Optional maximum number of records to return.
        :type top: :class:`int` or None
        :param expand: Optional list of navigation properties to expand, e.g. ``["primarycontactid"]``. Navigation property names are case-sensitive and must match the server-defined  names exactly. These are NOT automatically transformed. Consult entity metadata for correct casing.
        :type expand: :class:`list` of :class:`str` or None
        :param page_size: Optional number of records per page for pagination.
        :type page_size: :class:`int` or None

        :return: Single record dict if ``record_id`` is provided, otherwise a generator
            yielding lists of record dictionaries (one list per page).
        :rtype: :class:`dict` or :class:`collections.abc.Iterable` of :class:`list` of :class:`dict`

        :raises TypeError: If ``record_id`` is provided but not a string.

        Example:
            Fetch a single record::

                record = client.get("account", record_id=account_id, select=["name", "telephone1"])
                print(record["name"])

            Query multiple records with filtering (note: exact logical names in filter)::

                for batch in client.get(
                    "account",
                    filter="statecode eq 0 and name eq 'Contoso'",  # Must use exact logical names (lower-case)
                    select=["name", "telephone1"]
                ):
                    for account in batch:
                        print(account["name"])

            Query with navigation property expansion (note: case-sensitive property name)::

                for batch in client.get(
                    "account",
                    select=["name"],
                    expand=["primarycontactid"],  # Case-sensitive! Check metadata for exact name
                    filter="statecode eq 0"
                ):
                    for account in batch:
                        print(f"{account['name']} - Contact: {account.get('primarycontactid', {}).get('fullname')}")

            Query with sorting and pagination::

                for batch in client.get(
                    "account",
                    orderby=["createdon desc"],
                    top=100,
                    page_size=50
                ):
                    print(f"Batch size: {len(batch)}")
        """
        warnings.warn(
            "client.get() is deprecated. Use client.records.get() or client.query.get() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        if record_id is not None:
            return self.records.get(table_schema_name, record_id, select=select)
        else:
            return self.query.get(
                table_schema_name,
                select=select,
                filter=filter,
                orderby=orderby,
                top=top,
                expand=expand,
                page_size=page_size,
            )

    # SQL via Web API sql parameter
    def query_sql(self, sql: str) -> List[Dict[str, Any]]:
        """
        .. note::
            Deprecated. Use :meth:`~PowerPlatform.Dataverse.operations.query.QueryOperations.sql` instead.

        Execute a read-only SQL query using the Dataverse Web API ``?sql`` capability.

        The SQL query must follow the supported subset: a single SELECT statement with
        optional WHERE, TOP (integer literal), ORDER BY (column names only), and a simple
        table alias after FROM.

        :param sql: Supported SQL SELECT statement.
        :type sql: :class:`str`

        :return: List of result row dictionaries. Returns an empty list if no rows match.
        :rtype: :class:`list` of :class:`dict`

        :raises ~PowerPlatform.Dataverse.core.errors.SQLParseError: If the SQL query uses unsupported syntax.
        :raises ~PowerPlatform.Dataverse.core.errors.HttpError: If the Web API returns an error.

        .. note::
            The SQL support is limited to read-only queries. Complex joins, subqueries, and certain SQL functions may not be supported. Consult the Dataverse documentation for the current feature set.

        Example:
            Basic SQL query::

                sql = "SELECT TOP 10 accountid, name FROM account WHERE name LIKE 'C%' ORDER BY name"
                results = client.query_sql(sql)
                for row in results:
                    print(row["name"])

            Query with alias::

                sql = "SELECT a.name, a.telephone1 FROM account AS a WHERE a.statecode = 0"
                results = client.query_sql(sql)
        """
        warnings.warn(
            "client.query_sql() is deprecated. Use client.query.sql() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.query.sql(sql)

    # Table metadata helpers
    def get_table_info(self, table_schema_name: str) -> Optional[Dict[str, Any]]:
        """
        .. note::
            Deprecated. Use :meth:`~PowerPlatform.Dataverse.operations.tables.TableOperations.get` instead.

        Get basic metadata for a table if it exists.

        :param table_schema_name: Schema name of the table (e.g. ``"new_MyTestTable"`` or ``"account"``).
        :type table_schema_name: :class:`str`

        :return: Dictionary containing table metadata with keys ``table_schema_name``,
            ``table_logical_name``, ``entity_set_name``, and ``metadata_id``.
            Returns None if the table is not found.
        :rtype: :class:`dict` or None

        Example:
            Retrieve table metadata::

                info = client.get_table_info("new_MyTestTable")
                if info:
                    print(f"Logical name: {info['table_logical_name']}")
                    print(f"Entity set: {info['entity_set_name']}")
        """
        warnings.warn(
            "client.get_table_info() is deprecated. Use client.tables.get() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.tables.get(table_schema_name)

    def create_table(
        self,
        table_schema_name: str,
        columns: Dict[str, Any],
        solution_unique_name: Optional[str] = None,
        primary_column_schema_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        .. note::
            Deprecated. Use :meth:`~PowerPlatform.Dataverse.operations.tables.TableOperations.create` instead.

        Create a simple custom table with specified columns.

        :param table_schema_name: Schema name of the table with customization prefix value (e.g. ``"new_MyTestTable"``).
        :type table_schema_name: :class:`str`
        :param columns: Dictionary mapping column names (with customization prefix value) to their types. All custom column names must include the customization prefix value (e.g. ``"new_Title"``).
            Supported types:

            - Primitive types: ``"string"`` (alias: ``"text"``), ``"int"`` (alias: ``"integer"``), ``"decimal"`` (alias: ``"money"``), ``"float"`` (alias: ``"double"``), ``"datetime"`` (alias: ``"date"``), ``"bool"`` (alias: ``"boolean"``), and ``"file"``
            - Enum subclass (IntEnum preferred): Creates a local option set. Optional multilingual
              labels can be provided via ``__labels__`` class attribute, defined inside the Enum subclass::

                  class ItemStatus(IntEnum):
                      ACTIVE = 1
                      INACTIVE = 2
                      __labels__ = {
                          1033: {"Active": "Active", "Inactive": "Inactive"},
                          1036: {"Active": "Actif", "Inactive": "Inactif"}
                      }

        :type columns: :class:`dict` mapping :class:`str` to :class:`typing.Any`
        :param solution_unique_name: Optional solution unique name that should own the new table. When omitted the table is created in the default solution.
        :type solution_unique_name: :class:`str` or None
        :param primary_column_schema_name: Optional primary name column schema name with customization prefix value (e.g. ``"new_MyTestTable"``). If not provided, defaults to ``"{customization prefix value}_Name"``.
        :type primary_column_schema_name: :class:`str` or None

        :return: Dictionary containing table metadata including ``table_schema_name``,
            ``entity_set_name``, ``table_logical_name``, ``metadata_id``, and ``columns_created``.
        :rtype: :class:`dict`

        :raises ~PowerPlatform.Dataverse.core.errors.MetadataError: If table creation fails or the schema is invalid.

        Example:
            Create a table with simple columns::

                from enum import IntEnum

                class ItemStatus(IntEnum):
                    ACTIVE = 1
                    INACTIVE = 2

                columns = {
                    "new_Title": "string",      # Note: includes 'new_' customization prefix value
                    "new_Quantity": "int",
                    "new_Price": "decimal",
                    "new_Available": "bool",
                    "new_Status": ItemStatus
                }

                result = client.create_table("new_MyTestTable", columns)
                print(f"Created table: {result['table_schema_name']}")
                print(f"Columns: {result['columns_created']}")

            Create a table with a custom primary column name::

                result = client.create_table(
                    "new_Product",
                    {"new_Price": "decimal"},
                    primary_column_schema_name="new_ProductName"
                )
        """
        warnings.warn(
            "client.create_table() is deprecated. Use client.tables.create() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.tables.create(
            table_schema_name,
            columns,
            solution=solution_unique_name,
            primary_column=primary_column_schema_name,
        )

    def delete_table(self, table_schema_name: str) -> None:
        """
        .. note::
            Deprecated. Use :meth:`~PowerPlatform.Dataverse.operations.tables.TableOperations.delete` instead.

        Delete a custom table by name.

        :param table_schema_name: Schema name of the table (e.g. ``"new_MyTestTable"`` or ``"account"``).
        :type table_schema_name: :class:`str`

        :raises ~PowerPlatform.Dataverse.core.errors.MetadataError: If the table does not exist or deletion fails.

        .. warning::
            This operation is irreversible and will delete all records in the table along
            with the table definition. Use with caution.

        Example:
            Delete a custom table::

                client.delete_table("new_MyTestTable")
        """
        warnings.warn(
            "client.delete_table() is deprecated. Use client.tables.delete() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.tables.delete(table_schema_name)

    def list_tables(self) -> list[dict[str, Any]]:
        """
        .. note::
            Deprecated. Use :meth:`~PowerPlatform.Dataverse.operations.tables.TableOperations.list` instead.

        List all non-private tables in the Dataverse environment.

        :return: List of EntityDefinition metadata dictionaries.
        :rtype: :class:`list` of :class:`dict`

        Example:
            List all non-private tables and print their logical names::

                tables = client.list_tables()
                for table in tables:
                    print(table["LogicalName"])
        """
        warnings.warn(
            "client.list_tables() is deprecated. Use client.tables.list() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.tables.list()

    def create_columns(
        self,
        table_schema_name: str,
        columns: Dict[str, Any],
    ) -> List[str]:
        """
        .. note::
            Deprecated. Use :meth:`~PowerPlatform.Dataverse.operations.tables.TableOperations.add_columns` instead.

        Create one or more columns on an existing table using a schema-style mapping.

        :param table_schema_name: Schema name of the table (e.g. ``"new_MyTestTable"``).
        :type table_schema_name: :class:`str`
        :param columns: Mapping of column schema names (with customization prefix value) to supported types. All custom column names must include the customization prefix value** (e.g. ``"new_Notes"``). Primitive types include
            ``"string"`` (alias: ``"text"``), ``"int"`` (alias: ``"integer"``), ``"decimal"`` (alias: ``"money"``), ``"float"`` (alias: ``"double"``), ``"datetime"`` (alias: ``"date"``), ``"bool"`` (alias: ``"boolean"``), and ``"file"``. Enum subclasses (IntEnum preferred)
            generate a local option set and can specify localized labels via ``__labels__``.
        :type columns: :class:`dict` mapping :class:`str` to :class:`typing.Any`
        :returns: Schema names for the columns that were created.
        :rtype: :class:`list` of :class:`str`
        Example:
            Create multiple columns on the custom table::

                created = client.create_columns(
                    "new_MyTestTable",
                    {
                        "new_Scratch": "string",
                        "new_Flags": "bool",
                        "new_Document": "file",
                    },
                )
                print(created)  # ['new_Scratch', 'new_Flags', 'new_Document']
        """
        warnings.warn(
            "client.create_columns() is deprecated. Use client.tables.add_columns() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.tables.add_columns(table_schema_name, columns)

    def delete_columns(
        self,
        table_schema_name: str,
        columns: Union[str, List[str]],
    ) -> List[str]:
        """
        .. note::
            Deprecated. Use :meth:`~PowerPlatform.Dataverse.operations.tables.TableOperations.remove_columns` instead.

        Delete one or more columns from a table.

        :param table_schema_name: Schema name of the table (e.g. ``"new_MyTestTable"``).
        :type table_schema_name: :class:`str`
        :param columns: Column name or list of column names to remove. Must include customization prefix value (e.g. ``"new_TestColumn"``).
        :type columns: :class:`str` or :class:`list` of :class:`str`
        :returns: Schema names for the columns that were removed.
        :rtype: :class:`list` of :class:`str`
        Example:
            Remove two custom columns by schema name:

                removed = client.delete_columns(
                    "new_MyTestTable",
                    ["new_Scratch", "new_Flags"],
                )
                print(removed)  # ['new_Scratch', 'new_Flags']
        """
        warnings.warn(
            "client.delete_columns() is deprecated. Use client.tables.remove_columns() instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.tables.remove_columns(table_schema_name, columns)

    # File upload
    def upload_file(
        self,
        table_schema_name: str,
        record_id: str,
        file_name_attribute: str,
        path: str,
        mode: Optional[str] = None,
        mime_type: Optional[str] = None,
        if_none_match: bool = True,
    ) -> None:
        """
        Upload a file to a Dataverse file column.

        :param table_schema_name: Schema name of the table, e.g. ``"account"`` or ``"new_MyTestTable"``.
        :type table_schema_name: :class:`str`
        :param record_id: GUID of the target record.
        :type record_id: :class:`str`
        :param file_name_attribute: Schema name of the file column attribute (e.g., ``"new_Document"``). If the column doesn't exist, it will be created automatically.
        :type file_name_attribute: :class:`str`
        :param path: Local filesystem path to the file. The stored filename will be
            the basename of this path.
        :type path: :class:`str`
        :param mode: Upload strategy: ``"auto"`` (default), ``"small"``, or ``"chunk"``.
            Auto mode selects small or chunked upload based on file size.
        :type mode: :class:`str` or None
        :param mime_type: Explicit MIME type to store with the file (e.g. ``"application/pdf"``).
            If not provided, the MIME type may be inferred from the file extension.
        :type mime_type: :class:`str` or None
        :param if_none_match: When True (default), sends ``If-None-Match: null`` header to only
            succeed if the column is currently empty. Set False to always overwrite using
            ``If-Match: *``. Used for small and chunk modes only.
        :type if_none_match: :class:`bool`

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError: If the upload fails or the file column is not empty
            when ``if_none_match=True``.
        :raises FileNotFoundError: If the specified file path does not exist.

        .. note::
            Large files are automatically chunked to avoid request size limits. The chunk mode performs multiple requests with resumable upload support.

        Example:
            Upload a PDF file::

                client.upload_file(
                    table_schema_name="account",
                    record_id=account_id,
                    file_name_attribute="new_Contract",
                    path="/path/to/contract.pdf",
                    mime_type="application/pdf"
                )

            Upload with auto mode selection::

                client.upload_file(
                    table_schema_name="email",
                    record_id=email_id,
                    file_name_attribute="new_Attachment",
                    path="/path/to/large_file.zip",
                    mode="auto"
                )
        """
        with self._scoped_odata() as od:
            od._upload_file(
                table_schema_name,
                record_id,
                file_name_attribute,
                path,
                mode=mode,
                mime_type=mime_type,
                if_none_match=if_none_match,
            )
            return None

    # Cache utilities
    def flush_cache(self, kind) -> int:
        """
        Flush cached client metadata or state.

        :param kind: Cache kind to flush. Currently supported values:

            - ``"picklist"``: Clears picklist label cache used for label-to-integer conversion

            Future kinds (e.g. ``"entityset"``, ``"primaryid"``) may be added without
            breaking this signature.
        :type kind: :class:`str`

        :return: Number of cache entries removed.
        :rtype: :class:`int`

        Example:
            Clear the picklist cache::

                removed = client.flush_cache("picklist")
                print(f"Cleared {removed} cached picklist entries")
        """
        with self._scoped_odata() as od:
            return od._flush_cache(kind)

    # Relationship operations
    def create_one_to_many_relationship(
        self,
        lookup: LookupAttributeMetadata,
        relationship: OneToManyRelationshipMetadata,
        *,
        solution: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a one-to-many relationship between tables.

        This operation creates both the relationship and the lookup attribute
        on the referencing table.

        :param lookup: Metadata defining the lookup attribute.
        :type lookup: ~PowerPlatform.Dataverse.models.metadata.LookupAttributeMetadata
        :param relationship: Metadata defining the relationship.
        :type relationship: ~PowerPlatform.Dataverse.models.metadata.OneToManyRelationshipMetadata
        :param solution: Optional solution unique name to add relationship to.
        :type solution: :class:`str` or None

        :return: Dictionary with relationship_id, lookup_schema_name, and related metadata.
        :rtype: :class:`dict`

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError: If the Web API request fails.

        Example:
            Create a one-to-many relationship: Department (1) -> Employee (N)::

                from PowerPlatform.Dataverse.models.metadata import (
                    LookupAttributeMetadata,
                    OneToManyRelationshipMetadata,
                    Label,
                    LocalizedLabel,
                    CascadeConfiguration,
                )

                # Define the lookup attribute (added to Employee table)
                lookup = LookupAttributeMetadata(
                    schema_name="new_DepartmentId",
                    display_name=Label(
                        localized_labels=[
                            LocalizedLabel(label="Department", language_code=1033)
                        ]
                    ),
                )

                # Define the relationship
                relationship = OneToManyRelationshipMetadata(
                    schema_name="new_Department_Employee",
                    referenced_entity="new_department",   # Parent table (the "one" side)
                    referencing_entity="new_employee",    # Child table (the "many" side)
                    referenced_attribute="new_departmentid",
                    cascade_configuration=CascadeConfiguration(
                        delete=CASCADE_BEHAVIOR_REMOVE_LINK,
                    ),
                )

                result = client.create_one_to_many_relationship(lookup, relationship)
                print(f"Created lookup field: {result['lookup_schema_name']}")
        """
        with self._scoped_odata() as od:
            return od._create_one_to_many_relationship(
                lookup,
                relationship,
                solution,
            )

    def create_many_to_many_relationship(
        self,
        relationship: ManyToManyRelationshipMetadata,
        *,
        solution: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a many-to-many relationship between tables.

        This operation creates a many-to-many relationship and an intersect table
        to manage the relationship.

        :param relationship: Metadata defining the many-to-many relationship.
        :type relationship: ~PowerPlatform.Dataverse.models.metadata.ManyToManyRelationshipMetadata
        :param solution: Optional solution unique name to add relationship to.
        :type solution: :class:`str` or None

        :return: Dictionary with relationship_id, relationship_schema_name, and entity names.
        :rtype: :class:`dict`

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError: If the Web API request fails.

        Example:
            Create a many-to-many relationship: Employee (N) <-> Project (N)::

                from PowerPlatform.Dataverse.models.metadata import (
                    ManyToManyRelationshipMetadata,
                )

                # Employees work on multiple projects; projects have multiple team members
                relationship = ManyToManyRelationshipMetadata(
                    schema_name="new_employee_project",
                    entity1_logical_name="new_employee",
                    entity2_logical_name="new_project",
                )

                result = client.create_many_to_many_relationship(relationship)
                print(f"Created M:N relationship: {result['relationship_schema_name']}")
        """
        with self._scoped_odata() as od:
            return od._create_many_to_many_relationship(
                relationship,
                solution,
            )

    def delete_relationship(self, relationship_id: str) -> None:
        """
        Delete a relationship by its metadata ID.

        :param relationship_id: The GUID of the relationship metadata.
        :type relationship_id: :class:`str`

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError: If the Web API request fails.

        .. warning::
            Deleting a relationship also removes the associated lookup attribute
            for one-to-many relationships. This operation is irreversible.

        Example:
            Delete a relationship::

                client.delete_relationship("12345678-1234-1234-1234-123456789abc")
        """
        with self._scoped_odata() as od:
            od._delete_relationship(relationship_id)

    def get_relationship(self, schema_name: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve relationship metadata by schema name.

        :param schema_name: The schema name of the relationship.
        :type schema_name: :class:`str`

        :return: Relationship metadata dictionary, or None if not found.
        :rtype: :class:`dict` or None

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError: If the Web API request fails.

        Example:
            Get relationship metadata::

                rel = client.get_relationship("new_Department_Employee")
                if rel:
                    print(f"Found relationship: {rel['SchemaName']}")
        """
        with self._scoped_odata() as od:
            return od._get_relationship(schema_name)

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
        """
        Create a simple lookup field relationship.

        This is a convenience method that wraps :meth:`create_one_to_many_relationship`
        for the common case of adding a lookup field to an existing table.

        :param referencing_table: Logical name of the table that will have the lookup field (child table).
        :type referencing_table: :class:`str`
        :param lookup_field_name: Schema name for the lookup field (e.g., ``"new_AccountId"``).
        :type lookup_field_name: :class:`str`
        :param referenced_table: Logical name of the table being referenced (parent table).
        :type referenced_table: :class:`str`
        :param display_name: Display name for the lookup field. Defaults to the referenced table name.
        :type display_name: :class:`str` or None
        :param description: Optional description for the lookup field.
        :type description: :class:`str` or None
        :param required: Whether the lookup is required. Defaults to ``False``.
        :type required: :class:`bool`
        :param cascade_delete: Delete behavior (``"RemoveLink"``, ``"Cascade"``, ``"Restrict"``).
            Defaults to ``"RemoveLink"``.
        :type cascade_delete: :class:`str`
        :param solution: Optional solution unique name to add the relationship to.
        :type solution: :class:`str` or None
        :param language_code: Language code for labels. Defaults to 1033 (English).
        :type language_code: :class:`int`

        :return: Dictionary with ``relationship_id``, ``lookup_schema_name``, and related metadata.
        :rtype: :class:`dict`

        :raises ~PowerPlatform.Dataverse.core.errors.HttpError: If the Web API request fails.

        Example:
            Create a simple lookup field::

                result = client.create_lookup_field(
                    referencing_table="new_order",
                    lookup_field_name="new_AccountId",
                    referenced_table="account",
                    display_name="Account",
                    required=True,
                    cascade_delete=CASCADE_BEHAVIOR_REMOVE_LINK
                )

                print(f"Created lookup: {result['lookup_schema_name']}")
        """
        # Build the label
        localized_labels = [LocalizedLabel(label=display_name or referenced_table, language_code=language_code)]

        # Build the lookup attribute
        lookup = LookupAttributeMetadata(
            schema_name=lookup_field_name,
            display_name=Label(localized_labels=localized_labels),
            required_level="ApplicationRequired" if required else "None",
        )

        # Add description if provided
        if description:
            lookup.description = Label(
                localized_labels=[LocalizedLabel(label=description, language_code=language_code)]
            )

        # Generate a relationship name
        relationship_name = f"{referenced_table}_{referencing_table}_{lookup_field_name}"

        # Build the relationship metadata
        relationship = OneToManyRelationshipMetadata(
            schema_name=relationship_name,
            referenced_entity=referenced_table,
            referencing_entity=referencing_table,
            referenced_attribute=f"{referenced_table}id",
            cascade_configuration=CascadeConfiguration(delete=cascade_delete),
        )

        return self.create_one_to_many_relationship(lookup, relationship, solution=solution)


__all__ = ["DataverseClient"]
