# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Async Dataverse client.

:class:`~PowerPlatform.Dataverse.async_client.AsyncDataverseClient` mirrors the public API of
:class:`~PowerPlatform.Dataverse.client.DataverseClient` with full ``async``/``await`` support.
Existing sync code is completely unaffected; async support is opt-in via a separate import.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Dict, List, Optional, Union

from azure.core.credentials_async import AsyncTokenCredential

from .core._async_auth import _AsyncAuthManager
from .core.config import DataverseConfig
from .data._async_odata import _AsyncODataClient
from .operations.async_records import AsyncRecordOperations
from .operations.async_query import AsyncQueryOperations
from .operations.async_tables import AsyncTableOperations
from .operations.async_files import AsyncFileOperations

__all__ = ["AsyncDataverseClient"]


class AsyncDataverseClient:
    """
    Async high-level client for Microsoft Dataverse operations.

    Mirrors :class:`~PowerPlatform.Dataverse.client.DataverseClient` with ``async``/``await``
    support. All methods are ``async def`` and must be awaited.

    :param base_url: Your Dataverse environment URL, for example
        ``"https://org.crm.dynamics.com"``. Trailing slash is automatically removed.
    :type base_url: :class:`str`
    :param credential: Azure Identity async credential for authentication.
    :type credential: ~azure.core.credentials_async.AsyncTokenCredential
    :param config: Optional configuration for language, timeouts, and retries.
        If not provided, defaults are loaded from
        :meth:`~PowerPlatform.Dataverse.core.config.DataverseConfig.from_env`.
    :type config: ~PowerPlatform.Dataverse.core.config.DataverseConfig or None

    :raises ValueError: If ``base_url`` is missing or empty after trimming.

    Operations are organized into namespaces:

    - ``client.records`` -- create, update, delete, and get records (single or paginated)
    - ``client.query``   -- query and search operations
    - ``client.tables``  -- table and column metadata management
    - ``client.files``   -- file upload operations

    The client supports Python's async context manager protocol::

        from azure.identity.aio import ClientSecretCredential
        from PowerPlatform.Dataverse.async_client import AsyncDataverseClient

        credential = ClientSecretCredential(tenant_id, client_id, client_secret)

        async with AsyncDataverseClient("https://org.crm.dynamics.com", credential) as client:
            guid = await client.records.create("account", {"name": "Contoso Ltd"})
            record = await client.records.get("account", guid, select=["name"])
            print(record["name"])
            await client.records.delete("account", guid)
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
        self._closed: bool = False

        # Operation namespaces
        self.records = AsyncRecordOperations(self)
        self.query = AsyncQueryOperations(self)
        self.tables = AsyncTableOperations(self)
        self.files = AsyncFileOperations(self)

    def _get_odata(self) -> _AsyncODataClient:
        """Get or lazily create the internal async OData client."""
        if self._odata is None:
            self._odata = _AsyncODataClient(
                self.auth,
                self._base_url,
                self._config,
            )
        return self._odata

    @asynccontextmanager
    async def _scoped_odata(self) -> AsyncIterator[_AsyncODataClient]:
        """Yield the async OData client with an active correlation scope."""
        self._check_closed()
        od = self._get_odata()
        with od._call_scope():
            yield od

    # ---------------- Context manager / lifecycle ----------------

    async def __aenter__(self) -> AsyncDataverseClient:
        """Enter the async context manager.

        :return: The client instance.
        :rtype: AsyncDataverseClient
        :raises RuntimeError: If the client has been closed.
        """
        self._check_closed()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the async context manager and close the client."""
        await self.close()

    async def close(self) -> None:
        """Close the client and release resources.

        Closes the underlying aiohttp session, clears caches, and marks the
        client as closed. Safe to call multiple times.

        Called automatically when using the client as an async context manager.
        """
        if self._closed:
            return
        if self._odata is not None:
            await self._odata.close()
            self._odata = None
        self._closed = True

    def _check_closed(self) -> None:
        """Raise :class:`RuntimeError` if the client has been closed."""
        if self._closed:
            raise RuntimeError("AsyncDataverseClient is closed")
