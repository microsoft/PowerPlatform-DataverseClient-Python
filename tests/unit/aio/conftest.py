# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Shared fixtures for async unit tests."""

from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock

import pytest
from azure.core.credentials_async import AsyncTokenCredential

from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient


@pytest.fixture
def mock_od() -> AsyncMock:
    """AsyncMock representing the low-level _AsyncODataClient."""
    od = AsyncMock()
    # _call_scope() is a sync context manager; MagicMock supports __enter__/__exit__
    od._call_scope.return_value = MagicMock()
    return od


@pytest.fixture
def async_client(mock_od: AsyncMock) -> AsyncDataverseClient:
    """AsyncDataverseClient with _scoped_odata patched to yield mock_od."""
    cred = MagicMock(spec=AsyncTokenCredential)
    client = AsyncDataverseClient("https://example.crm.dynamics.com", cred)

    @asynccontextmanager
    async def _fake_scoped_odata():
        yield mock_od

    client._scoped_odata = _fake_scoped_odata
    return client
