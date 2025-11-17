# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from __future__ import annotations

from typing import Any, Dict, Optional, Union, List, Iterable

from azure.core.credentials import TokenCredential

from .core.auth import AuthManager
from .core.config import DataverseConfig
from .data.odata import ODataClient


class DataverseClient:
    """
    High-level client for Microsoft Dataverse operations.

    This client provides a simple, stable interface for interacting with Dataverse environments
    through the Web API. It handles authentication via Azure Identity and delegates HTTP operations
    to an internal :class:`~PowerPlatform.Dataverse.data.odata.ODataClient`.

    Key capabilities:
        - OData CRUD operations: create, read, update, delete records
        - SQL queries: execute read-only SQL via Web API ``?sql`` parameter
        - Table metadata: create, inspect, and delete custom tables; create and delete columns
        - File uploads: upload files to file columns with chunking support

    :param base_url: Your Dataverse environment URL, for example
        ``"https://org.crm.dynamics.com"``. Trailing slash is automatically removed.
    :type base_url: ``str``
    :param credential: Azure Identity credential for authentication.
    :type credential: ~azure.core.credentials.TokenCredential
    :param config: Optional configuration for language, timeouts, and retries.
        If not provided, defaults are loaded from :meth:`~PowerPlatform.Dataverse.core.config.DataverseConfig.from_env`.
    :type config: ~PowerPlatform.Dataverse.core.config.DataverseConfig or None

    :raises ValueError: If ``base_url`` is missing or empty after trimming.

    .. note::
        The client lazily initializes its internal OData client on first use, allowing lightweight construction without immediate network calls.

    Example:
        Create a client and perform basic operations::

            from azure.identity import InteractiveBrowserCredential
            from PowerPlatform.Dataverse import DataverseClient

            credential = InteractiveBrowserCredential()
            client = DataverseClient(
                "https://org.crm.dynamics.com",
                credential
            )

            # Create a record
            record_ids = client.create("account", {"name": "Contoso Ltd"})
            print(f"Created account: {record_ids[0]}")

            # Update a record
            client.update("account", record_ids[0], {"telephone1": "555-0100"})

            # Query records
            for batch in client.get("account", filter="name eq 'Contoso Ltd'"):
                for account in batch:
                    print(account["name"])

            # Delete a record
            client.delete("account", record_ids[0])
    """

    def __init__(
        self,
        base_url: str,
        credential: TokenCredential,
        config: Optional[DataverseConfig] = None,
    ) -> None:
        self.auth = AuthManager(credential)
        self._base_url = (base_url or "").rstrip("/")
        if not self._base_url:
            raise ValueError("base_url is required.")
        self._config = config or DataverseConfig.from_env()
        self._odata: Optional[ODataClient] = None

    def _get_odata(self) -> ODataClient:
        """
        Get or create the internal OData client instance.

        This method implements lazy initialization of the low-level OData client,
        deferring construction until the first API call.

        :return: The lazily-initialized low-level client used to perform HTTP requests.
        :rtype: ~PowerPlatform.Dataverse.data.odata.ODataClient
        """
        if self._odata is None:
            self._odata = ODataClient(
                self.auth,
                self._base_url,
                self._config,
            )
        return self._odata

    # ---------------- Unified CRUD: create/update/delete ----------------
    def create(self, table_schema_name: str, records: Union[Dict[str, Any], List[Dict[str, Any]]]) -> List[str]:
        """
        Create one or more records by table name.

        :param table_schema_name: Schema name of the table (e.g. ``"account"``, ``"contact"``, or ``"new_MyTestTable"``).
        :type table_schema_name: ``str``
        :param records: A single record dictionary or a list of record dictionaries.
            Each dictionary should contain column schema names as keys.
        :type records: ``dict`` or ``list[dict]``

        :return: List of created record GUIDs. Returns a single-element list for a single input.
        :rtype: ``list[str]``

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
        od = self._get_odata()
        entity_set = od._entity_set_from_schema_name(table_schema_name)
        if isinstance(records, dict):
            rid = od._create(entity_set, table_schema_name, records)
            # _create returns str on single input
            if not isinstance(rid, str):
                raise TypeError("_create (single) did not return GUID string")
            return [rid]
        if isinstance(records, list):
            ids = od._create_multiple(entity_set, table_schema_name, records)
            if not isinstance(ids, list) or not all(isinstance(x, str) for x in ids):
                raise TypeError("_create (multi) did not return list[str]")
            return ids
        raise TypeError("records must be dict or list[dict]")

    def update(
        self, table_schema_name: str, ids: Union[str, List[str]], changes: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> None:
        """
        Update one or more records.

        This method supports three usage patterns:

        1. Single record update: ``update("account", "guid", {"name": "New Name"})``
        2. Broadcast update: ``update("account", [id1, id2], {"status": 1})`` - applies same changes to all IDs
        3. Paired updates: ``update("account", [id1, id2], [changes1, changes2])`` - one-to-one mapping

        :param table_schema_name:  Schema name of the table (e.g. ``"account"`` or ``"new_MyTestTable"``).
        :type table_schema_name: ``str``
        :param ids: Single GUID string or list of GUID strings to update.
        :type ids: ``str`` or ``list[str]``
        :param changes: Dictionary of changes for single/broadcast mode, or list of dictionaries
            for paired mode. When ``ids`` is a list and ``changes`` is a single dict,
            the same changes are broadcast to all records. When both are lists, they must
            have equal length for one-to-one mapping.
        :type changes: ``dict`` or ``list[dict]``

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
        od = self._get_odata()
        if isinstance(ids, str):
            if not isinstance(changes, dict):
                raise TypeError("For single id, changes must be a dict")
            od._update(table_schema_name, ids, changes)  # discard representation
            return None
        if not isinstance(ids, list):
            raise TypeError("ids must be str or list[str]")
        od._update_by_ids(table_schema_name, ids, changes)
        return None

    def delete(
        self,
        table_schema_name: str,
        ids: Union[str, List[str]],
        use_bulk_delete: bool = True,
    ) -> Optional[str]:
        """
        Delete one or more records by GUID.

        :param table_schema_name: Schema name of the table (e.g. ``"account"`` or ``"new_MyTestTable"``).
        :type table_schema_name: ``str``
        :param ids: Single GUID string or list of GUID strings to delete.
        :type ids: ``str`` or ``list[str]``
        :param use_bulk_delete: When ``True`` (default) and ``ids`` is a list, execute the BulkDelete action and
            return its async job identifier. When ``False`` each record is deleted sequentially.
        :type use_bulk_delete: ``bool``

        :raises TypeError: If ``ids`` is not str or list[str].
        :raises HttpError: If the underlying Web API delete request fails.

        :return: BulkDelete job ID when deleting multiple records via BulkDelete; otherwise ``None``.
        :rtype: ``str`` or ``None``

        Example:
            Delete a single record::

                client.delete("account", account_id)

            Delete multiple records::

                job_id = client.delete("account", [id1, id2, id3])
        """
        od = self._get_odata()
        if isinstance(ids, str):
            od._delete(table_schema_name, ids)
            return None
        if not isinstance(ids, list):
            raise TypeError("ids must be str or list[str]")
        if not ids:
            return None
        if not all(isinstance(rid, str) for rid in ids):
            raise TypeError("ids must contain string GUIDs")
        if use_bulk_delete:
            return od._delete_multiple(table_schema_name, ids)
        for rid in ids:
            od._delete(table_schema_name, rid)
        return None

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
        Fetch a single record by ID or query multiple records.

        When ``record_id`` is provided, returns a single record dictionary.
        When ``record_id`` is None, returns a generator yielding batches of records.

        :param table_schema_name: Schema name of the table (e.g. ``"account"`` or ``"new_MyTestTable"``).
        :type table_schema_name: ``str``
        :param record_id: Optional GUID to fetch a specific record. If None, queries multiple records.
        :type record_id: ``str`` or ``None``
        :param select: Optional list of attribute logical names to retrieve. Column names are case-insensitive and automatically lowercased (e.g. ``["new_Title", "new_Amount"]`` becomes ``"new_title,new_amount"``).
        :type select: ``list[str]`` or ``None``
        :param filter: Optional OData filter string, e.g. ``"name eq 'Contoso'"`` or ``"new_quantity gt 5"``. Column names in filter expressions must use exact lowercase logical names (e.g. ``"new_quantity"``, not ``"new_Quantity"``). The filter string is passed directly to the Dataverse Web API without transformation.
        :type filter: ``str`` or ``None``
        :param orderby: Optional list of attributes to sort by, e.g. ``["name asc", "createdon desc"]``. Column names are automatically lowercased.
        :type orderby: ``list[str]`` or ``None``
        :param top: Optional maximum number of records to return.
        :type top: ``int`` or ``None``
        :param expand: Optional list of navigation properties to expand, e.g. ``["primarycontactid"]``. Navigation property names are case-sensitive and must match the server-defined  names exactly. These are NOT automatically transformed. Consult entity metadata for correct casing.
        :type expand: ``list[str]`` or ``None``
        :param page_size: Optional number of records per page for pagination.
        :type page_size: ``int`` or ``None``

        :return: Single record dict if ``record_id`` is provided, otherwise a generator
            yielding lists of record dictionaries (one list per page).
        :rtype: ``dict`` or ``Iterable[list[dict]]``

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
        od = self._get_odata()
        if record_id is not None:
            if not isinstance(record_id, str):
                raise TypeError("record_id must be str")
            return od._get(
                table_schema_name,
                record_id,
                select=select,
            )
        return od._get_multiple(
            table_schema_name,
            select=select,
            filter=filter,
            orderby=orderby,
            top=top,
            expand=expand,
            page_size=page_size,
        )

    # SQL via Web API sql parameter
    def query_sql(self, sql: str):
        """
        Execute a read-only SQL query using the Dataverse Web API ``?sql`` capability.

        The SQL query must follow the supported subset: a single SELECT statement with
        optional WHERE, TOP (integer literal), ORDER BY (column names only), and a simple
        table alias after FROM.

        :param sql: Supported SQL SELECT statement.
        :type sql: ``str``

        :return: List of result row dictionaries. Returns an empty list if no rows match.
        :rtype: ``list[dict]``

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
        return self._get_odata()._query_sql(sql)

    # Table metadata helpers
    def get_table_info(self, table_schema_name: str) -> Optional[Dict[str, Any]]:
        """
        Get basic metadata for a table if it exists.

        :param table_schema_name: Schema name of the table (e.g. ``"new_MyTestTable"`` or ``"account"``).
        :type table_schema_name: ``str``

        :return: Dictionary containing table metadata with keys ``table_schema_name``,
            ``table_logical_name``, ``entity_set_name``, and ``metadata_id``.
            Returns None if the table is not found.
        :rtype: ``dict`` or ``None``

        Example:
            Retrieve table metadata::

                info = client.get_table_info("new_MyTestTable")
                if info:
                    print(f"Logical name: {info['table_logical_name']}")
                    print(f"Entity set: {info['entity_set_name']}")
        """
        return self._get_odata()._get_table_info(table_schema_name)

    def create_table(
        self,
        table_schema_name: str,
        columns: Dict[str, Any],
        solution_unique_name: Optional[str] = None,
        primary_column_schema_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a simple custom table with specified columns.

        :param table_schema_name: Schema name of the table with customization prefix value (e.g. ``"new_MyTestTable"``).
        :type table_schema_name: ``str``
        :param columns: Dictionary mapping column names (with customization prefix value) to their types. All custom column names must include the customization prefix value (e.g. ``"new_Title"``).
            Supported types:

            - Primitive types: ``"string"``, ``"int"``, ``"decimal"``, ``"float"``, ``"datetime"``, ``"bool"``
            - Enum subclass (IntEnum preferred): Creates a local option set. Optional multilingual
              labels can be provided via ``__labels__`` class attribute, defined inside the Enum subclass::

                  class ItemStatus(IntEnum):
                      ACTIVE = 1
                      INACTIVE = 2
                      __labels__ = {
                          1033: {"Active": "Active", "Inactive": "Inactive"},
                          1036: {"Active": "Actif", "Inactive": "Inactif"}
                      }

        :type columns: dict[str, Any]
        :param solution_unique_name: Optional solution unique name that should own the new table. When omitted the table is created in the default solution.
        :type solution_unique_name: ``str`` or ``None``
        :param primary_column_schema_name: Optional primary name column schema name with customization prefix value (e.g. ``"new_MyTestTable"``). If not provided, defaults to ``"{customization prefix value}_Name"``.
        :type primary_column_schema_name: ``str`` or ``None``

        :return: Dictionary containing table metadata including ``table_schema_name``,
            ``entity_set_name``, ``table_logical_name``, ``metadata_id``, and ``columns_created``.
        :rtype: ``dict``

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
        return self._get_odata()._create_table(
            table_schema_name,
            columns,
            solution_unique_name,
            primary_column_schema_name,
        )

    def delete_table(self, table_schema_name: str) -> None:
        """
        Delete a custom table by name.

        :param table_schema_name: Schema name of the table (e.g. ``"new_MyTestTable"`` or ``"account"``).
        :type table_schema_name: ``str``

        :raises ~PowerPlatform.Dataverse.core.errors.MetadataError: If the table does not exist or deletion fails.

        .. warning::
            This operation is irreversible and will delete all records in the table along
            with the table definition. Use with caution.

        Example:
            Delete a custom table::

                client.delete_table("new_MyTestTable")
        """
        self._get_odata()._delete_table(table_schema_name)

    def list_tables(self) -> list[str]:
        """
        List all custom tables in the Dataverse environment.

        :return: List of custom table names.
        :rtype: ``list[str]``

        Example:
            List all custom tables::

                tables = client.list_tables()
                for table in tables:
                    print(table)
        """
        return self._get_odata()._list_tables()

    def create_columns(
        self,
        table_schema_name: str,
        columns: Dict[str, Any],
    ) -> List[str]:
        """
        Create one or more columns on an existing table using a schema-style mapping.

        :param table_schema_name: Schema name of the table (e.g. ``"new_MyTestTable"``).
        :type table_schema_name: ``str``
        :param columns: Mapping of column schema names (with customization prefix value) to supported types. All custom column names must include the customization prefix value** (e.g. ``"new_Notes"``). Primitive types include
            ``string``, ``int``, ``decimal``, ``float``, ``datetime``, and ``bool``. Enum subclasses (IntEnum preferred)
            generate a local option set and can specify localized labels via ``__labels__``.
        :type columns: ``Dict[str, Any]``
        :returns: Schema names for the columns that were created.
        :rtype: ``list[str]``
        Example:
            Create two columns on the custom table::

                created = client.create_columns(
                    "new_MyTestTable",
                    {
                        "new_Scratch": "string",
                        "new_Flags": "bool",
                    },
                )
                print(created)  # ['new_Scratch', 'new_Flags']
        """
        return self._get_odata()._create_columns(
            table_schema_name,
            columns,
        )

    def delete_columns(
        self,
        table_schema_name: str,
        columns: Union[str, List[str]],
    ) -> List[str]:
        """
        Delete one or more columns from a table.

        :param table_schema_name: Schema name of the table (e.g. ``"new_MyTestTable"``).
        :type table_schema_name: ``str``
        :param columns: Column name or list of column names to remove. Must include customization prefix value (e.g. ``"new_TestColumn"``).
        :type columns: ``str`` | ``list[str]``
        :returns: Schema names for the columns that were removed.
        :rtype: ``list[str]``
        Example:
            Remove two custom columns by schema name:

                removed = client.delete_columns(
                    "new_MyTestTable",
                    ["new_Scratch", "new_Flags"],
                )
                print(removed)  # ['new_Scratch', 'new_Flags']
        """
        return self._get_odata()._delete_columns(
            table_schema_name,
            columns,
        )

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
        :type table_schema_name: ``str``
        :param record_id: GUID of the target record.
        :type record_id: ``str``
        :param file_name_attribute: Logical name of the file column attribute.
        :type file_name_attribute: ``str``
        :param path: Local filesystem path to the file. The stored filename will be
            the basename of this path.
        :type path: ``str``
        :param mode: Upload strategy: ``"auto"`` (default), ``"small"``, or ``"chunk"``.
            Auto mode selects small or chunked upload based on file size.
        :type mode: ``str`` or ``None``
        :param mime_type: Explicit MIME type to store with the file (e.g. ``"application/pdf"``).
            If not provided, the MIME type may be inferred from the file extension.
        :type mime_type: ``str`` or ``None``
        :param if_none_match: When True (default), sends ``If-None-Match: null`` header to only
            succeed if the column is currently empty. Set False to always overwrite using
            ``If-Match: *``. Used for small and chunk modes only.
        :type if_none_match: ``bool``

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
                    file_name_attribute="new_contract",
                    path="/path/to/contract.pdf",
                    mime_type="application/pdf"
                )

            Upload with auto mode selection::

                client.upload_file(
                    table_schema_name="email",
                    record_id=email_id,
                    file_name_attribute="new_attachment",
                    path="/path/to/large_file.zip",
                    mode="auto"
                )
        """
        od = self._get_odata()
        entity_set = od._entity_set_from_schema_name(table_schema_name)
        od.upload_file(
            entity_set,
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
        :type kind: ``str``

        :return: Number of cache entries removed.
        :rtype: ``int``

        Example:
            Clear the picklist cache::

                removed = client.flush_cache("picklist")
                print(f"Cleared {removed} cached picklist entries")
        """
        return self._get_odata()._flush_cache(kind)


__all__ = ["DataverseClient"]
