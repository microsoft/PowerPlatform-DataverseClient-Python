# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from __future__ import annotations

from typing import Any, Dict, Optional, Union, List, Iterable, Iterator
from contextlib import contextmanager

from azure.core.credentials import TokenCredential

from .core._auth import _AuthManager
from .core.config import DataverseConfig
from .core.results import OperationResult, RequestTelemetryData
from .data._odata import _ODataClient


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
        self.auth = _AuthManager(credential)
        self._base_url = (base_url or "").rstrip("/")
        if not self._base_url:
            raise ValueError("base_url is required.")
        self._config = config or DataverseConfig.from_env()
        self._odata: Optional[_ODataClient] = None

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
    def create(
        self, table_schema_name: str, records: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> OperationResult[List[str]]:
        """
        Create one or more records by table name.

        :param table_schema_name: Schema name of the table (e.g. ``"account"``, ``"contact"``, or ``"new_MyTestTable"``).
        :type table_schema_name: :class:`str`
        :param records: A single record dictionary or a list of record dictionaries.
            Each dictionary should contain column schema names as keys.
        :type records: :class:`dict` or :class:`list` of :class:`dict`

        :return: OperationResult containing the list of created record GUIDs. The result
            can be used directly (supports iteration, indexing, length) or call
            ``.with_response_details()`` to access telemetry data.
        :rtype: :class:`OperationResult` [:class:`list` of :class:`str`]

        :raises TypeError: If ``records`` is not a dict or list[dict], or if the internal
            client returns an unexpected type.

        Example:
            Create a single record::

                client = DataverseClient(base_url, credential)
                ids = client.create("account", {"name": "Contoso"})
                print(f"Created: {ids[0]}")  # Works via __getitem__

            Create multiple records and iterate::

                records = [{"name": "Contoso"}, {"name": "Fabrikam"}]
                ids = client.create("account", records)
                for id in ids:  # Works via __iter__
                    print(id)

            Access telemetry data::

                response = client.create("account", {"name": "Test"}).with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._scoped_odata() as od:
            entity_set = od._entity_set_from_schema_name(table_schema_name)
            if isinstance(records, dict):
                rid, metadata = od._create(entity_set, table_schema_name, records)
                if not isinstance(rid, str):
                    raise TypeError("_create (single) did not return GUID string")
                return OperationResult([rid], metadata)
            if isinstance(records, list):
                ids, metadata = od._create_multiple(entity_set, table_schema_name, records)
                if not isinstance(ids, list) or not all(isinstance(x, str) for x in ids):
                    raise TypeError("_create (multi) did not return list[str]")
                return OperationResult(ids, metadata)
        raise TypeError("records must be dict or list[dict]")

    def update(
        self, table_schema_name: str, ids: Union[str, List[str]], changes: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> OperationResult[None]:
        """
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

        :return: OperationResult containing None. Call ``.with_response_details()`` to access
            telemetry data from the update request.
        :rtype: :class:`OperationResult` [None]

        :raises TypeError: If ``ids`` is not str or list[str], or if ``changes`` type doesn't match usage pattern.

        .. note::
            Single updates discard the response representation for better performance. For broadcast or paired updates, the method delegates to the internal client's batch update logic.

        Example:
            Single record update::

                client.update("account", account_id, {"telephone1": "555-0100"})

            Broadcast same changes to multiple records::

                client.update("account", [id1, id2, id3], {"statecode": 1})

            Access telemetry data::

                response = client.update("account", id, {"name": "New"}).with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._scoped_odata() as od:
            # Unwrap OperationResult if passed directly from create()
            if isinstance(ids, OperationResult):
                ids = ids.value
            if isinstance(ids, str):
                if not isinstance(changes, dict):
                    raise TypeError("For single id, changes must be a dict")
                _, metadata = od._update(table_schema_name, ids, changes)
                return OperationResult(None, metadata)
            if not isinstance(ids, list):
                raise TypeError("ids must be str or list[str]")
            _, metadata = od._update_by_ids(table_schema_name, ids, changes)
            return OperationResult(None, metadata)

    def delete(
        self,
        table_schema_name: str,
        ids: Union[str, List[str]],
        use_bulk_delete: bool = True,
    ) -> OperationResult[Optional[str]]:
        """
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

        :return: OperationResult containing the BulkDelete job ID when deleting multiple
            records via BulkDelete; otherwise contains ``None``. Call ``.with_response_details()``
            to access telemetry data.
        :rtype: :class:`OperationResult` [:class:`str` or None]

        Example:
            Delete a single record::

                client.delete("account", account_id)

            Delete multiple records and get job ID::

                result = client.delete("account", [id1, id2, id3])
                job_id = result.value  # Access the job ID directly

            Access telemetry data::

                response = client.delete("account", id).with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._scoped_odata() as od:
            # Unwrap OperationResult if passed directly from create()
            if isinstance(ids, OperationResult):
                ids = ids.value
            if isinstance(ids, str):
                _, metadata = od._delete(table_schema_name, ids)
                return OperationResult(None, metadata)
            if not isinstance(ids, list):
                raise TypeError("ids must be str or list[str]")
            if not ids:
                return OperationResult(None, RequestTelemetryData())
            if not all(isinstance(rid, str) for rid in ids):
                raise TypeError("ids must contain string GUIDs")
            if use_bulk_delete:
                job_id, metadata = od._delete_multiple(table_schema_name, ids)
                return OperationResult(job_id, metadata)
            # Sequential deletes - capture metadata from the last delete
            metadata = RequestTelemetryData()
            for rid in ids:
                _, metadata = od._delete(table_schema_name, rid)
            return OperationResult(None, metadata)

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
    ) -> Union[OperationResult[Dict[str, Any]], Iterable[OperationResult[List[Dict[str, Any]]]]]:
        """
        Fetch a single record by ID or query multiple records.

        When ``record_id`` is provided, returns an OperationResult containing a single record dictionary.
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

        :return: When ``record_id`` is provided, returns an OperationResult containing the record dict.
            The result supports dict-like access (e.g., ``result["name"]``) or call
            ``.with_response_details()`` to access telemetry data.
            When querying multiple records, returns a generator yielding OperationResult objects,
            each containing a list of record dictionaries (one list per page). Each batch supports
            iteration and indexing directly, or call ``.with_response_details()`` to access
            that page's telemetry data.
        :rtype: :class:`OperationResult` [:class:`dict`] or :class:`collections.abc.Iterable` of :class:`OperationResult` [:class:`list` of :class:`dict`]

        :raises TypeError: If ``record_id`` is provided but not a string.

        Example:
            Fetch a single record::

                record = client.get("account", record_id=account_id, select=["name", "telephone1"])
                print(record["name"])  # Works via __getitem__

            Fetch single record with telemetry::

                response = client.get("account", record_id=account_id).with_response_details()
                print(f"Record: {response.result['name']}")
                print(f"Request ID: {response.telemetry['client_request_id']}")

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

            Query with per-page telemetry access::

                for batch in client.get("account", filter="statecode eq 0"):
                    response = batch.with_response_details()
                    print(f"Page request ID: {response.telemetry['service_request_id']}")
                    for account in response.result:
                        print(account["name"])
        """
        if record_id is not None:
            if not isinstance(record_id, str):
                raise TypeError("record_id must be str")
            with self._scoped_odata() as od:
                record, metadata = od._get(
                    table_schema_name,
                    record_id,
                    select=select,
                )
                return OperationResult(record, metadata)

        def _paged() -> Iterable[OperationResult[List[Dict[str, Any]]]]:
            with self._scoped_odata() as od:
                for batch, metadata in od._get_multiple(
                    table_schema_name,
                    select=select,
                    filter=filter,
                    orderby=orderby,
                    top=top,
                    expand=expand,
                    page_size=page_size,
                ):
                    yield OperationResult(batch, metadata)

        return _paged()

    # SQL via Web API sql parameter
    def query_sql(self, sql: str) -> OperationResult[List[Dict[str, Any]]]:
        """
        Execute a read-only SQL query using the Dataverse Web API ``?sql`` capability.

        The SQL query must follow the supported subset: a single SELECT statement with
        optional WHERE, TOP (integer literal), ORDER BY (column names only), and a simple
        table alias after FROM.

        :param sql: Supported SQL SELECT statement.
        :type sql: :class:`str`

        :return: OperationResult containing list of result row dictionaries. Returns an empty list if no rows match.
            Call ``.with_response_details()`` to access telemetry data.
        :rtype: :class:`OperationResult` [:class:`list` of :class:`dict`]

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

            Access telemetry data::

                response = client.query_sql(sql).with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._scoped_odata() as od:
            result, metadata = od._query_sql(sql)
            return OperationResult(result, metadata)

    # Table metadata helpers
    def get_table_info(self, table_schema_name: str) -> OperationResult[Optional[Dict[str, Any]]]:
        """
        Get basic metadata for a table if it exists.

        :param table_schema_name: Schema name of the table (e.g. ``"new_MyTestTable"`` or ``"account"``).
        :type table_schema_name: :class:`str`

        :return: OperationResult containing dictionary with table metadata (keys: ``table_schema_name``,
            ``table_logical_name``, ``entity_set_name``, ``metadata_id``) or None if not found.
            Call ``.with_response_details()`` to access telemetry data.
        :rtype: :class:`OperationResult` [:class:`dict` or None]

        Example:
            Retrieve table metadata::

                info = client.get_table_info("new_MyTestTable")
                if info:
                    print(f"Logical name: {info['table_logical_name']}")
                    print(f"Entity set: {info['entity_set_name']}")

            Access telemetry data::

                response = client.get_table_info("account").with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._scoped_odata() as od:
            result, metadata = od._get_table_info(table_schema_name)
            return OperationResult(result, metadata)

    def create_table(
        self,
        table_schema_name: str,
        columns: Dict[str, Any],
        solution_unique_name: Optional[str] = None,
        primary_column_schema_name: Optional[str] = None,
    ) -> OperationResult[Dict[str, Any]]:
        """
        Create a simple custom table with specified columns.

        :param table_schema_name: Schema name of the table with customization prefix value (e.g. ``"new_MyTestTable"``).
        :type table_schema_name: :class:`str`
        :param columns: Dictionary mapping column names (with customization prefix value) to their types. All custom column names must include the customization prefix value (e.g. ``"new_Title"``).
            Supported types:

            - Primitive types: ``"string"`` (alias: ``"text"``), ``"int"`` (alias: ``"integer"``), ``"decimal"`` (alias: ``"money"``), ``"float"`` (alias: ``"double"``), ``"datetime"`` (alias: ``"date"``), ``"bool"`` (alias: ``"boolean"``)
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

        :return: OperationResult containing dictionary with table metadata (keys: ``table_schema_name``,
            ``entity_set_name``, ``table_logical_name``, ``metadata_id``, ``columns_created``).
            Call ``.with_response_details()`` to access telemetry data.
        :rtype: :class:`OperationResult` [:class:`dict`]

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

            Access telemetry data::

                response = client.create_table("new_Test", {"new_Col": "string"}).with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._scoped_odata() as od:
            result, metadata = od._create_table(
                table_schema_name,
                columns,
                solution_unique_name,
                primary_column_schema_name,
            )
            return OperationResult(result, metadata)

    def delete_table(self, table_schema_name: str) -> OperationResult[None]:
        """
        Delete a custom table by name.

        :param table_schema_name: Schema name of the table (e.g. ``"new_MyTestTable"`` or ``"account"``).
        :type table_schema_name: :class:`str`

        :return: OperationResult containing None. Call ``.with_response_details()`` to access telemetry data.
        :rtype: :class:`OperationResult` [None]

        :raises ~PowerPlatform.Dataverse.core.errors.MetadataError: If the table does not exist or deletion fails.

        .. warning::
            This operation is irreversible and will delete all records in the table along
            with the table definition. Use with caution.

        Example:
            Delete a custom table::

                client.delete_table("new_MyTestTable")

            Access telemetry data::

                response = client.delete_table("new_MyTestTable").with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._scoped_odata() as od:
            _, metadata = od._delete_table(table_schema_name)
            return OperationResult(None, metadata)

    def list_tables(self) -> OperationResult[List[Dict[str, Any]]]:
        """
        List all custom tables in the Dataverse environment.

        :return: OperationResult containing list of table metadata. Call ``.with_response_details()``
            to access telemetry data.
        :rtype: :class:`OperationResult` [:class:`list` of :class:`dict`]

        Example:
            List all custom tables::

                tables = client.list_tables()
                for table in tables:
                    print(table)

            Access telemetry data::

                response = client.list_tables().with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._scoped_odata() as od:
            result, metadata = od._list_tables()
            return OperationResult(result, metadata)

    def create_columns(
        self,
        table_schema_name: str,
        columns: Dict[str, Any],
    ) -> OperationResult[List[str]]:
        """
        Create one or more columns on an existing table using a schema-style mapping.

        :param table_schema_name: Schema name of the table (e.g. ``"new_MyTestTable"``).
        :type table_schema_name: :class:`str`
        :param columns: Mapping of column schema names (with customization prefix value) to supported types. All custom column names must include the customization prefix value** (e.g. ``"new_Notes"``). Primitive types include
            ``"string"`` (alias: ``"text"``), ``"int"`` (alias: ``"integer"``), ``"decimal"`` (alias: ``"money"``), ``"float"`` (alias: ``"double"``), ``"datetime"`` (alias: ``"date"``), and ``"bool"`` (alias: ``"boolean"``). Enum subclasses (IntEnum preferred)
            generate a local option set and can specify localized labels via ``__labels__``.
        :type columns: :class:`dict` mapping :class:`str` to :class:`typing.Any`
        :returns: OperationResult containing schema names for the columns that were created.
            Call ``.with_response_details()`` to access telemetry data.
        :rtype: :class:`OperationResult` [:class:`list` of :class:`str`]
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

            Access telemetry data::

                response = client.create_columns("new_Test", {"new_Col": "string"}).with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._scoped_odata() as od:
            result, metadata = od._create_columns(
                table_schema_name,
                columns,
            )
            return OperationResult(result, metadata)

    def delete_columns(
        self,
        table_schema_name: str,
        columns: Union[str, List[str]],
    ) -> OperationResult[List[str]]:
        """
        Delete one or more columns from a table.

        :param table_schema_name: Schema name of the table (e.g. ``"new_MyTestTable"``).
        :type table_schema_name: :class:`str`
        :param columns: Column name or list of column names to remove. Must include customization prefix value (e.g. ``"new_TestColumn"``).
        :type columns: :class:`str` or :class:`list` of :class:`str`
        :returns: OperationResult containing schema names for the columns that were removed.
            Call ``.with_response_details()`` to access telemetry data.
        :rtype: :class:`OperationResult` [:class:`list` of :class:`str`]
        Example:
            Remove two custom columns by schema name:

                removed = client.delete_columns(
                    "new_MyTestTable",
                    ["new_Scratch", "new_Flags"],
                )
                print(removed)  # ['new_Scratch', 'new_Flags']

            Access telemetry data::

                response = client.delete_columns("new_Test", ["new_Col"]).with_response_details()
                print(f"Request ID: {response.telemetry['client_request_id']}")
        """
        with self._scoped_odata() as od:
            result, metadata = od._delete_columns(
                table_schema_name,
                columns,
            )
            return OperationResult(result, metadata)

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
        :param file_name_attribute: Logical name of the file column attribute.
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
        with self._scoped_odata() as od:
            entity_set = od._entity_set_from_schema_name(table_schema_name)
            od._upload_file(
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


__all__ = ["DataverseClient"]
