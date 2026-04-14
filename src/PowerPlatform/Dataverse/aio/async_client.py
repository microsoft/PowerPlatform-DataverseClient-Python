# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Async Dataverse client implementation."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Optional

from ..core.config import DataverseConfig
from .core._async_auth import _AsyncAuthManager
from .data._async_odata import _AsyncODataClient
from .operations.async_records import AsyncRecordOperations
from .operations.async_query import AsyncQueryOperations
from .operations.async_tables import AsyncTableOperations
from .operations.async_files import AsyncFileOperations
from .operations.async_dataframe import AsyncDataFrameOperations
from .operations.async_batch import AsyncBatchOperations


class AsyncDataverseClient:
    """Async high-level client for Microsoft Dataverse operations.

    Drop-in async counterpart of
    :class:`~PowerPlatform.Dataverse.client.DataverseClient`.  All public
    methods that communicate with Dataverse are ``async def`` coroutines.
    The client uses ``aiohttp`` for transport and accepts
    ``azure.identity.aio`` async credentials.

    :param base_url: Dataverse environment URL, e.g.
        ``"https://org.crm.dynamics.com"``.  A trailing slash is removed
        automatically.
    :type base_url: :class:`str`
    :param credential: An async Azure Identity credential that exposes an
        async ``get_token(scope)`` coroutine (e.g.
        ``azure.identity.aio.ClientSecretCredential``).
    :param config: Optional SDK configuration for language, timeouts, and
        retries.  Defaults to
        :meth:`~PowerPlatform.Dataverse.core.config.DataverseConfig.from_env`.
    :type config: ~PowerPlatform.Dataverse.core.config.DataverseConfig or None

    :raises ValueError: If ``base_url`` is missing or empty after trimming.

    .. note::
        The client lazily initialises its internal OData client on first use.

    .. note::
        All methods that communicate with the Dataverse Web API may raise
        :class:`~PowerPlatform.Dataverse.core.errors.HttpError` on
        non-successful HTTP responses.

    Operations are organised into namespaces:

    - ``client.records`` — create, update, delete, and get records
    - ``client.query`` — SQL query and fluent query builder
    - ``client.tables`` — table and column metadata management
    - ``client.files`` — file upload operations
    - ``client.dataframe`` — pandas DataFrame wrappers for record CRUD
    - ``client.batch`` — batch multiple operations into a single HTTP request

    The client supports Python's **async** context manager protocol for
    automatic resource cleanup and ``aiohttp`` connection pooling:

    Example:
        **Recommended — async context manager** (connection pooling)::

            from azure.identity.aio import InteractiveBrowserCredential
            from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient

            credential = InteractiveBrowserCredential()

            async with AsyncDataverseClient(
                "https://org.crm.dynamics.com", credential
            ) as client:
                guid = await client.records.create("account", {"name": "Contoso"})
                await client.records.delete("account", guid)
            # Session closed automatically

        **Manual lifecycle**::

            client = AsyncDataverseClient("https://org.crm.dynamics.com", credential)
            try:
                guid = await client.records.create("account", {"name": "Contoso"})
            finally:
                await client.close()
    """

    def __init__(
        self,
        base_url: str,
        credential: Any,  # azure.identity.aio async credential
        config: Optional[DataverseConfig] = None,
    ) -> None:
        self.auth = _AsyncAuthManager(credential)
        self._base_url = (base_url or "").rstrip("/")
        if not self._base_url:
            raise ValueError("base_url is required.")
        self._config = config or DataverseConfig.from_env()
        self._odata: Optional[_AsyncODataClient] = None
        self._session: Any = None  # aiohttp.ClientSession | None
        self._closed: bool = False

        # Operation namespaces
        self.records = AsyncRecordOperations(self)
        self.query = AsyncQueryOperations(self)
        self.tables = AsyncTableOperations(self)
        self.files = AsyncFileOperations(self)
        self.dataframe = AsyncDataFrameOperations(self)
        self.batch = AsyncBatchOperations(self)

    def _get_odata(self) -> _AsyncODataClient:
        """Get or lazily create the internal async OData client."""
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
        """Async context manager that yields the OData client with a correlation scope."""
        self._check_closed()
        od = self._get_odata()
        async with od._call_scope():
            yield od

    # ---------------- Async context manager / lifecycle ----------------

    async def __aenter__(self) -> "AsyncDataverseClient":
        """Enter the async context manager.

        Creates an ``aiohttp.ClientSession`` for HTTP connection pooling.
        All operations within the ``async with`` block reuse this session.

        :return: The client instance.
        :rtype: AsyncDataverseClient

        :raises RuntimeError: If the client has been closed.
        """
        self._check_closed()
        if self._session is None:
            import aiohttp

            self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the async context manager with cleanup.

        Calls :meth:`close` to release resources.  Exceptions are not
        suppressed.
        """
        await self.close()

    async def close(self) -> None:
        """Close the client and release resources.

        Closes the internal ``aiohttp`` session (if any), clears caches, and
        marks the client as closed.  Safe to call multiple times.

        Called automatically when using the client as an async context manager.
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

    # ---------------- Cache utilities ----------------

    async def flush_cache(self, kind: str) -> int:
        """Flush cached client metadata or state.

        :param kind: Cache kind to flush. Currently supported:

            - ``"picklist"``: Clears the picklist label cache used for
              label-to-integer conversion.

        :type kind: :class:`str`

        :return: Number of cache entries removed.
        :rtype: :class:`int`

        Example::

            removed = await client.flush_cache("picklist")
            print(f"Cleared {removed} cached picklist entries")
        """
        async with self._scoped_odata() as od:
            return od._flush_cache(kind)


__all__ = ["AsyncDataverseClient"]
