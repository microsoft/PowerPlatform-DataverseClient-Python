# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

import aiohttp
from azure.core.credentials_async import AsyncTokenCredential

from .core._async_auth import _AsyncAuthManager
from ..core.config import DataverseConfig
from .data._async_odata import _AsyncODataClient
from .operations.async_dataframe import AsyncDataFrameOperations
from .operations.async_records import AsyncRecordOperations
from .operations.async_query import AsyncQueryOperations
from .operations.async_files import AsyncFileOperations
from .operations.async_tables import AsyncTableOperations
from .operations.async_batch import AsyncBatchOperations


class AsyncDataverseClient:
    """
    Async high-level client for Microsoft Dataverse operations.

    This client provides a simple, stable async interface for interacting with
    Dataverse environments through the Web API. It handles authentication via
    Azure Identity and delegates HTTP operations to an internal
    :class:`~PowerPlatform.Dataverse.aio.data._async_odata._AsyncODataClient`.

    Key capabilities:
        - OData CRUD operations: create, read, update, delete records
        - SQL queries: execute read-only SQL via Web API ``?sql`` parameter
        - Table metadata: create, inspect, and delete custom tables; create and delete columns
        - File uploads: upload files to file columns with chunking support

    :param base_url: Your Dataverse environment URL, for example
        ``"https://org.crm.dynamics.com"``. Trailing slash is automatically removed.
    :type base_url: :class:`str`
    :param credential: Azure async Identity credential for authentication.
    :type credential: ~azure.core.credentials_async.AsyncTokenCredential
    :param config: Optional configuration for language, timeouts, and retries.
        If not provided, defaults are loaded from :meth:`~PowerPlatform.Dataverse.core.config.DataverseConfig.from_env`.
    :type config: ~PowerPlatform.Dataverse.core.config.DataverseConfig or None

    :raises ValueError: If ``base_url`` is missing or empty after trimming.

    .. note::
        The client lazily initializes its internal OData client on first use,
        allowing lightweight construction without immediate network calls.

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

    The client supports Python's async context manager protocol for automatic
    resource cleanup and HTTP connection pooling:

    Example:
        **Recommended -- async context manager** (enables HTTP connection pooling)::

            from azure.identity.aio import DefaultAzureCredential
            from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient

            credential = DefaultAzureCredential()

            async with AsyncDataverseClient("https://org.crm.dynamics.com", credential) as client:
                record_id = await client.records.create("account", {"name": "Contoso Ltd"})
                await client.records.update("account", record_id, {"telephone1": "555-0100"})
            # Session closed, caches cleared automatically

        **Manual lifecycle**::

            client = AsyncDataverseClient("https://org.crm.dynamics.com", credential)
            try:
                record_id = await client.records.create("account", {"name": "Contoso Ltd"})
            finally:
                await client.aclose()
    """

    def __init__(
        self,
        base_url: str,
        credential: AsyncTokenCredential,
        config: Optional[DataverseConfig] = None,
    ) -> None:
        self.auth = _AsyncAuthManager(credential)
        self._base_url = (base_url or "").rstrip("/")
        if not self._base_url:
            raise ValueError("base_url is required.")
        self._config = config or DataverseConfig.from_env()
        self._odata: Optional[_AsyncODataClient] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._closed: bool = False

        # Operation namespaces
        self.records = AsyncRecordOperations(self)
        self.query = AsyncQueryOperations(self)
        self.tables = AsyncTableOperations(self)
        self.files = AsyncFileOperations(self)
        self.dataframe = AsyncDataFrameOperations(self)
        self.batch = AsyncBatchOperations(self)

    def _get_odata(self) -> _AsyncODataClient:
        """
        Get or create the internal async OData client instance.

        This method implements lazy initialization of the low-level async OData
        client, deferring construction until the first API call.

        :return: The lazily-initialized low-level async client.
        :rtype: ~PowerPlatform.Dataverse.aio.data._async_odata._AsyncODataClient
        """
        if self._odata is None:
            self._odata = _AsyncODataClient(
                self.auth,
                self._base_url,
                self._config,
                session=self._session,
            )
        return self._odata

    @asynccontextmanager
    async def _scoped_odata(self) -> AsyncIterator[_AsyncODataClient]:
        """Async context manager yielding the low-level client with a correlation scope."""
        self._check_closed()
        od = self._get_odata()
        # _call_scope() is a sync context manager (just sets a context var — no I/O).
        with od._call_scope():
            yield od

    # ---------------- Context manager / lifecycle ----------------

    async def __aenter__(self) -> "AsyncDataverseClient":
        """Enter the async context manager.

        Creates an :class:`aiohttp.ClientSession` for HTTP connection pooling.
        All operations within the ``async with`` block reuse this session for
        better performance (TCP and TLS reuse).

        :return: The client instance.
        :rtype: AsyncDataverseClient

        :raises RuntimeError: If the client has been closed.
        """
        self._check_closed()
        if self._session is None:
            self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit the async context manager with cleanup.

        Calls :meth:`aclose` to release resources. Exceptions are not
        suppressed.
        """
        await self.aclose()

    async def aclose(self) -> None:
        """Close the async client and release resources.

        Closes the HTTP session (if any), clears internal caches, and
        marks the client as closed. Safe to call multiple times. After
        closing, any operation will raise :class:`RuntimeError`.

        Called automatically when using the client as an async context manager.

        Example::

            client = AsyncDataverseClient(base_url, credential)
            try:
                await client.records.create("account", {"name": "Contoso"})
            finally:
                await client.aclose()
        """
        if self._closed:
            return
        if self._odata is not None:
            await self._odata.close()
            self._odata = None
        if self._session is not None:
            await self._session.close()
            self._session = None
        self._closed = True

    def _check_closed(self) -> None:
        """Raise :class:`RuntimeError` if the client has been closed."""
        if self._closed:
            raise RuntimeError("AsyncDataverseClient is closed")
