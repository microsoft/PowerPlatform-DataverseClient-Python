# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from azure.core.credentials_async import AsyncTokenCredential

from PowerPlatform.Dataverse.aio.async_client import AsyncDataverseClient
from PowerPlatform.Dataverse.aio.operations.async_records import AsyncRecordOperations
from PowerPlatform.Dataverse.aio.operations.async_tables import AsyncTableOperations
from PowerPlatform.Dataverse.aio.operations.async_query import AsyncQueryOperations
from PowerPlatform.Dataverse.aio.operations.async_files import AsyncFileOperations
from PowerPlatform.Dataverse.aio.operations.async_dataframe import AsyncDataFrameOperations
from PowerPlatform.Dataverse.aio.operations.async_batch import AsyncBatchOperations


def _make_credential() -> MagicMock:
    return MagicMock(spec=AsyncTokenCredential)


class TestAsyncDataverseClientInit:
    """Tests for AsyncDataverseClient initialization and validation."""

    def test_valid_init(self):
        """AsyncDataverseClient initializes with valid url and credential."""
        client = AsyncDataverseClient("https://org.crm.dynamics.com", _make_credential())
        assert client._base_url == "https://org.crm.dynamics.com"
        assert not client._closed

    def test_trailing_slash_stripped(self):
        """Trailing slash is stripped from base_url."""
        client = AsyncDataverseClient("https://org.crm.dynamics.com/", _make_credential())
        assert client._base_url == "https://org.crm.dynamics.com"

    def test_empty_base_url_raises(self):
        """Empty base_url raises ValueError."""
        with pytest.raises(ValueError, match="base_url is required"):
            AsyncDataverseClient("", _make_credential())

    def test_namespace_attributes_created(self):
        """All operation namespace attributes are created."""
        client = AsyncDataverseClient("https://org.crm.dynamics.com", _make_credential())
        assert isinstance(client.records, AsyncRecordOperations)
        assert isinstance(client.tables, AsyncTableOperations)
        assert isinstance(client.query, AsyncQueryOperations)
        assert isinstance(client.files, AsyncFileOperations)
        assert isinstance(client.dataframe, AsyncDataFrameOperations)
        assert isinstance(client.batch, AsyncBatchOperations)

    def test_odata_and_session_initially_none(self):
        """_odata and _session are None until first use."""
        client = AsyncDataverseClient("https://org.crm.dynamics.com", _make_credential())
        assert client._odata is None
        assert client._session is None


class TestAsyncDataverseClientContextManager:
    """Tests for async context manager protocol."""

    async def test_aenter_returns_self(self):
        """__aenter__ returns the client instance."""
        client = AsyncDataverseClient("https://org.crm.dynamics.com", _make_credential())
        with patch("aiohttp.ClientSession") as mock_session_cls:
            mock_session_cls.return_value = MagicMock()
            result = await client.__aenter__()
        assert result is client

    async def test_aenter_creates_session(self):
        """__aenter__ creates an aiohttp.ClientSession."""
        client = AsyncDataverseClient("https://org.crm.dynamics.com", _make_credential())
        with patch("aiohttp.ClientSession") as mock_session_cls:
            mock_session_cls.return_value = MagicMock()
            await client.__aenter__()
        mock_session_cls.assert_called_once()
        assert client._session is not None

    async def test_aenter_does_not_recreate_existing_session(self):
        """__aenter__ does not replace an existing session."""
        client = AsyncDataverseClient("https://org.crm.dynamics.com", _make_credential())
        existing_session = MagicMock()
        client._session = existing_session
        with patch("aiohttp.ClientSession") as mock_session_cls:
            await client.__aenter__()
        mock_session_cls.assert_not_called()
        assert client._session is existing_session

    async def test_aexit_calls_aclose(self):
        """__aexit__ calls aclose() to release resources."""
        client = AsyncDataverseClient("https://org.crm.dynamics.com", _make_credential())
        client.aclose = AsyncMock()
        await client.__aexit__(None, None, None)
        client.aclose.assert_called_once()

    async def test_aenter_raises_after_close(self):
        """__aenter__ raises RuntimeError after the client has been closed."""
        client = AsyncDataverseClient("https://org.crm.dynamics.com", _make_credential())
        client._closed = True
        with pytest.raises(RuntimeError, match="closed"):
            await client.__aenter__()


class TestAsyncDataverseClientAclose:
    """Tests for aclose() lifecycle."""

    async def test_aclose_sets_closed_flag(self):
        """aclose() marks the client as closed."""
        client = AsyncDataverseClient("https://org.crm.dynamics.com", _make_credential())
        await client.aclose()
        assert client._closed

    async def test_aclose_closes_session(self):
        """aclose() closes the aiohttp.ClientSession."""
        client = AsyncDataverseClient("https://org.crm.dynamics.com", _make_credential())
        mock_session = MagicMock()
        mock_session.close = AsyncMock()
        client._session = mock_session
        await client.aclose()
        mock_session.close.assert_called_once()
        assert client._session is None

    async def test_aclose_closes_odata(self):
        """aclose() closes the internal _odata client."""
        client = AsyncDataverseClient("https://org.crm.dynamics.com", _make_credential())
        mock_odata = AsyncMock()
        client._odata = mock_odata
        await client.aclose()
        mock_odata.close.assert_called_once()
        assert client._odata is None

    async def test_aclose_idempotent(self):
        """aclose() is safe to call multiple times."""
        client = AsyncDataverseClient("https://org.crm.dynamics.com", _make_credential())
        await client.aclose()
        await client.aclose()  # should not raise
        assert client._closed

    async def test_context_manager_closes_on_exit(self):
        """Using async with calls aclose() on exit."""
        client = AsyncDataverseClient("https://org.crm.dynamics.com", _make_credential())
        with patch("aiohttp.ClientSession") as mock_session_cls:
            mock_session = MagicMock()
            mock_session.close = AsyncMock()
            mock_session_cls.return_value = mock_session
            async with client:
                pass
        assert client._closed


class TestAsyncDataverseClientCheckClosed:
    """Tests for _check_closed guard."""

    def test_check_closed_raises_when_closed(self):
        """_check_closed() raises RuntimeError when the client is closed."""
        client = AsyncDataverseClient("https://org.crm.dynamics.com", _make_credential())
        client._closed = True
        with pytest.raises(RuntimeError, match="closed"):
            client._check_closed()

    def test_check_closed_does_not_raise_when_open(self):
        """_check_closed() does not raise when the client is open."""
        client = AsyncDataverseClient("https://org.crm.dynamics.com", _make_credential())
        client._check_closed()  # should not raise


class TestAsyncDataverseClientGetOdata:
    """Tests for _get_odata() lazy initialisation of the internal OData client."""

    def test_get_odata_creates_client_on_first_call(self):
        """_get_odata() instantiates _AsyncODataClient and stores it in _odata on first call."""
        from PowerPlatform.Dataverse.aio.data._async_odata import _AsyncODataClient

        client = AsyncDataverseClient("https://org.crm.dynamics.com", _make_credential())
        assert client._odata is None
        od = client._get_odata()
        assert isinstance(od, _AsyncODataClient)
        assert client._odata is od

    def test_get_odata_returns_same_instance(self):
        """Subsequent calls to _get_odata() return the same cached instance."""
        client = AsyncDataverseClient("https://org.crm.dynamics.com", _make_credential())
        od1 = client._get_odata()
        od2 = client._get_odata()
        assert od1 is od2


class TestAsyncDataverseClientScopedOdata:
    """Tests for _scoped_odata(), an async context manager that guards OData client access."""

    async def test_scoped_odata_yields_odata_client(self):
        """_scoped_odata() yields the low-level _AsyncODataClient instance."""
        from PowerPlatform.Dataverse.aio.data._async_odata import _AsyncODataClient

        client = AsyncDataverseClient("https://org.crm.dynamics.com", _make_credential())
        async with client._scoped_odata() as od:
            assert isinstance(od, _AsyncODataClient)

    async def test_scoped_odata_raises_when_closed(self):
        """RuntimeError is raised when _scoped_odata() is entered after the client is closed."""
        client = AsyncDataverseClient("https://org.crm.dynamics.com", _make_credential())
        client._closed = True
        with pytest.raises(RuntimeError, match="closed"):
            async with client._scoped_odata():
                pass
