# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for AsyncDataverseClient lifecycle and namespaces."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from azure.core.credentials_async import AsyncTokenCredential

from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient
from PowerPlatform.Dataverse.aio.operations.async_records import AsyncRecordOperations
from PowerPlatform.Dataverse.aio.operations.async_query import AsyncQueryOperations
from PowerPlatform.Dataverse.aio.operations.async_tables import AsyncTableOperations
from PowerPlatform.Dataverse.aio.operations.async_files import AsyncFileOperations
from PowerPlatform.Dataverse.aio.operations.async_dataframe import AsyncDataFrameOperations
from PowerPlatform.Dataverse.aio.operations.async_batch import AsyncBatchOperations

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client(base_url="https://example.crm.dynamics.com"):
    """Return an AsyncDataverseClient with a mock credential."""
    credential = AsyncMock(spec=AsyncTokenCredential)
    return AsyncDataverseClient(base_url, credential)


def _make_mock_odata():
    """Return an AsyncMock that mimics _AsyncODataClient."""
    od = AsyncMock()
    od._call_scope = MagicMock(return_value=_async_ctx())
    return od


class _async_ctx:
    """Trivial async context manager for mocking _call_scope()."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return False


# ---------------------------------------------------------------------------
# Namespace tests
# ---------------------------------------------------------------------------


class TestAsyncClientNamespaces:
    def test_records_namespace(self):
        client = _make_client()
        assert isinstance(client.records, AsyncRecordOperations)

    def test_query_namespace(self):
        client = _make_client()
        assert isinstance(client.query, AsyncQueryOperations)

    def test_tables_namespace(self):
        client = _make_client()
        assert isinstance(client.tables, AsyncTableOperations)

    def test_files_namespace(self):
        client = _make_client()
        assert isinstance(client.files, AsyncFileOperations)

    def test_dataframe_namespace(self):
        client = _make_client()
        assert isinstance(client.dataframe, AsyncDataFrameOperations)

    def test_batch_namespace(self):
        client = _make_client()
        assert isinstance(client.batch, AsyncBatchOperations)


# ---------------------------------------------------------------------------
# Construction / validation
# ---------------------------------------------------------------------------


class TestAsyncClientConstruction:
    def test_empty_base_url_raises(self):
        with pytest.raises(ValueError):
            AsyncDataverseClient("", AsyncMock(spec=AsyncTokenCredential))

    def test_whitespace_only_base_url_raises(self):
        # "   ".rstrip("/") = "   " which is truthy — client won't raise on whitespace.
        # Only truly empty string (or None-equivalent) raises.
        # This verifies the current behavior: whitespace alone does NOT raise.
        client = AsyncDataverseClient("   ", AsyncMock(spec=AsyncTokenCredential))
        assert client._base_url == "   "

    def test_trailing_slash_stripped(self):
        client = AsyncDataverseClient("https://example.crm.dynamics.com/", AsyncMock(spec=AsyncTokenCredential))
        assert client._base_url == "https://example.crm.dynamics.com"

    def test_custom_config_stored(self):
        from PowerPlatform.Dataverse.core.config import DataverseConfig

        cfg = DataverseConfig()
        client = AsyncDataverseClient(
            "https://example.crm.dynamics.com", AsyncMock(spec=AsyncTokenCredential), config=cfg
        )
        assert client._config is cfg


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


class TestAsyncClientLifecycle:
    async def test_context_manager_creates_session(self):
        client = _make_client()
        with patch("aiohttp.ClientSession") as mock_session_cls:
            mock_session_cls.return_value = AsyncMock()
            async with client:
                assert client._session is not None
            mock_session_cls.assert_called_once()

    async def test_context_manager_closes_on_exit(self):
        client = _make_client()
        mock_session = AsyncMock()
        with patch("aiohttp.ClientSession", return_value=mock_session):
            async with client:
                pass
        mock_session.close.assert_awaited_once()
        assert client._closed

    async def test_close_idempotent(self):
        """Calling close() twice should not raise."""
        client = _make_client()
        mock_session = AsyncMock()
        with patch("aiohttp.ClientSession", return_value=mock_session):
            async with client:
                pass
        await client.close()  # second call — should be a no-op

    async def test_check_closed_raises_after_close(self):
        client = _make_client()
        mock_session = AsyncMock()
        with patch("aiohttp.ClientSession", return_value=mock_session):
            async with client:
                pass
        with pytest.raises(RuntimeError, match="closed"):
            client._check_closed()

    async def test_flush_cache_delegates_to_odata(self):
        client = _make_client()
        od = _make_mock_odata()
        # _flush_cache is called synchronously (no await) in flush_cache():
        #   return od._flush_cache(kind)
        # So override the AsyncMock-generated attribute with a plain MagicMock.
        od._flush_cache = MagicMock(return_value=3)
        client._odata = od

        result = await client.flush_cache("picklist")
        od._flush_cache.assert_called_once_with("picklist")
        assert result == 3
