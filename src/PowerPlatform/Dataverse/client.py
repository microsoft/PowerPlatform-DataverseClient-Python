# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator, Optional

import requests

from azure.core.credentials import TokenCredential

from .core._auth import _AuthManager
from .core.config import DataverseConfig, OperationContext
from .data._odata import _ODataClient
from .operations.dataframe import DataFrameOperations
from .operations.records import RecordOperations
from .operations.query import QueryOperations
from .operations.files import FileOperations
from .operations.tables import TableOperations
from .operations.batch import BatchOperations


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
    :param context: Optional caller-defined context object appended to the
        outbound ``User-Agent`` header for plugin/tool attribution. Cannot be used
        together with ``config`` -- pass the context via
        :class:`~PowerPlatform.Dataverse.core.config.DataverseConfig` instead.
    :type context: ~PowerPlatform.Dataverse.core.config.OperationContext or None

    :raises ValueError: If ``base_url`` is missing or empty after trimming.
    :raises ValueError: If ``base_url`` does not use HTTPS. Dataverse only
        serves over HTTPS; constructing with ``http://`` would send the OAuth
        bearer token unencrypted, so the check fails fast at construction.
    :raises ValueError: If both ``config`` and ``context`` are provided.
    :raises AttributeError: If a v0 beta shortcut (``create``, ``update``,
        ``delete``, ``get``, ``query_sql``, ``create_table``, ``delete_table``,
        ``list_tables``, ``get_table_info``, ``create_columns``,
        ``delete_columns``, ``upload_file``) is accessed. The error message
        names the GA replacement and the codemod command. See
        :attr:`_REMOVED_BETA_METHODS`.

    .. note::
        The client lazily initializes its internal OData client on first use, allowing lightweight construction without immediate network calls.

    .. note::
        All methods that communicate with the Dataverse Web API may raise
        :class:`~PowerPlatform.Dataverse.core.errors.HttpError` on non-successful
        HTTP responses (e.g. 401, 403, 404, 429, 500). Individual method
        docstrings document only domain-specific exceptions.

    Operations are organized into namespaces:

    - ``client.records`` -- create, update, delete, and get records (single or paginated queries)
    - ``client.query`` -- query and search operations
    - ``client.tables`` -- table and column metadata management
    - ``client.files`` -- file upload operations
    - ``client.dataframe`` -- pandas DataFrame wrappers for record CRUD
    - ``client.batch`` -- batch multiple operations into a single HTTP request

    v0 beta methods (``client.create``, ``client.query_sql``, etc.) were removed
    in 1.0 GA. Calling one now raises :class:`AttributeError` with a message
    naming the GA replacement and the codemod command -- previously these calls
    raised a bare ``AttributeError`` with no migration hint, so debugging
    half-migrated code was painful. See :attr:`_REMOVED_BETA_METHODS`.

    The client supports Python's context manager protocol for automatic resource
    cleanup and HTTP connection pooling:

    Example:
        **Recommended -- context manager** (enables HTTP connection pooling)::

            from azure.identity import InteractiveBrowserCredential
            from PowerPlatform.Dataverse.client import DataverseClient

            credential = InteractiveBrowserCredential()

            with DataverseClient("https://org.crm.dynamics.com", credential) as client:
                record_id = client.records.create("account", {"name": "Contoso Ltd"})
                client.records.update("account", record_id, {"telephone1": "555-0100"})
            # Session closed, caches cleared automatically

        **Manual lifecycle**::

            client = DataverseClient("https://org.crm.dynamics.com", credential)
            try:
                record_id = client.records.create("account", {"name": "Contoso Ltd"})
            finally:
                client.close()
    """

    # v0 beta methods removed in 1.0 GA -> human-readable GA replacement.
    # Kept in sync with ``migrate_v0_to_v1._CLIENT_SHORTCUTS``: any addition
    # there should land here too so the runtime error and the codemod's
    # rewrite always agree. ``__getattr__`` reads this to turn what used to
    # be a bare ``AttributeError`` into one with an actionable hint.
    _REMOVED_BETA_METHODS = {
        "create": "client.records.create(table, data)",
        "update": "client.records.update(table, record_id, data)",
        "delete": "client.records.delete(table, record_id)",
        "get": "client.records.get(table, ...)",
        "query_sql": "client.query.sql(sql)",
        "get_table_info": "client.tables.get(table)",
        "create_table": "client.tables.create(...)",
        "delete_table": "client.tables.delete(table)",
        "list_tables": "client.tables.list()",
        "create_columns": "client.tables.add_columns(table, ...)",
        "delete_columns": "client.tables.remove_columns(table, ...)",
        "upload_file": "client.files.upload(...)",
    }

    def __init__(
        self,
        base_url: str,
        credential: TokenCredential,
        config: Optional[DataverseConfig] = None,
        *,
        context: Optional[OperationContext] = None,
    ) -> None:
        if config is not None and context is not None:
            raise ValueError(
                "Cannot specify both 'config' and 'context'. Pass operation_context via DataverseConfig instead."
            )
        self.auth = _AuthManager(credential)
        self._base_url = (base_url or "").rstrip("/")
        if not self._base_url:
            raise ValueError("base_url is required.")
        # Dataverse never serves over plaintext. A typo or copy-paste that leaves
        # the scheme as ``http://`` would send the bearer token unencrypted on the
        # very first request -- by then the credential is already on the wire.
        # Fail-fast at construction so the leak can't happen.
        if not self._base_url.lower().startswith("https://"):
            raise ValueError(
                f"base_url must use HTTPS (got {base_url!r}). Dataverse only serves "
                f"over HTTPS; plaintext would send OAuth bearer tokens unencrypted."
            )
        if config is not None:
            self._config = config
        elif context is not None:
            self._config = DataverseConfig(operation_context=context)
        else:
            self._config = DataverseConfig.from_env()
        self._odata: Optional[_ODataClient] = None
        self._session: Optional[requests.Session] = None
        self._closed: bool = False

        # Operation namespaces
        self.records = RecordOperations(self)
        self.query = QueryOperations(self)
        self.tables = TableOperations(self)
        self.files = FileOperations(self)
        self.dataframe = DataFrameOperations(self)
        self.batch = BatchOperations(self)

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
                session=self._session,
            )
        return self._odata

    @contextmanager
    def _scoped_odata(self) -> Iterator[_ODataClient]:
        """Yield the low-level client while ensuring a correlation scope is active."""
        self._check_closed()
        od = self._get_odata()
        with od._call_scope():
            yield od

    # ---------------- Context manager / lifecycle ----------------

    def __enter__(self) -> DataverseClient:
        """Enter the context manager.

        Creates a :class:`requests.Session` for HTTP connection pooling.
        All operations within the ``with`` block reuse this session for
        better performance (TCP and TLS reuse).

        :return: The client instance.
        :rtype: DataverseClient

        :raises RuntimeError: If the client has been closed.
        """
        self._check_closed()
        if self._session is None:
            self._session = requests.Session()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the context manager with cleanup.

        Calls :meth:`close` to release resources. Exceptions are not
        suppressed.
        """
        self.close()

    def close(self) -> None:
        """Close the client and release resources.

        Closes the HTTP session (if any), clears internal caches, and
        marks the client as closed. Safe to call multiple times. After
        closing, any operation will raise :class:`RuntimeError`.

        Called automatically when using the client as a context manager.

        Example::

            client = DataverseClient(base_url, credential)
            try:
                client.records.create("account", {"name": "Contoso"})
            finally:
                client.close()
        """
        if self._closed:
            return
        if self._odata is not None:
            self._odata.close()
            self._odata = None
        if self._session is not None:
            self._session.close()
            self._session = None
        self._closed = True

    def _check_closed(self) -> None:
        """Raise :class:`RuntimeError` if the client has been closed."""
        if self._closed:
            raise RuntimeError("DataverseClient is closed")

    def __getattr__(self, name: str):
        """Surface a migration hint for v0 beta shortcuts removed at 1.0 GA.

        Python only calls this when normal attribute lookup has failed, so the
        GA namespaces (``records``, ``query``, ``tables``, ``files``,
        ``dataframe``, ``batch``) -- which are set on the instance in
        ``__init__`` -- never reach here. Names starting with ``_`` fall
        through to a plain :class:`AttributeError` so pickling, ``copy.deepcopy``,
        IDE introspection, and similar protocol lookups behave normally.
        """
        if name.startswith("_"):
            raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")
        new_call = self._REMOVED_BETA_METHODS.get(name)
        if new_call is None:
            raise AttributeError(f"{type(self).__name__!r} object has no attribute {name!r}")
        raise AttributeError(
            f"'DataverseClient' has no attribute {name!r}. This was a v0 beta "
            f"method removed in 1.0 GA. Use {new_call} instead. To migrate a "
            f"codebase automatically, run: "
            f"python -m PowerPlatform.Dataverse.migration.migrate_v0_to_v1 <path>"
        )

    # ---------------- Cache utilities ----------------

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
