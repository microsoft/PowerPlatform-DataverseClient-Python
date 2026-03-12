# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for AsyncDataverseClient and async operation namespaces."""

from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from PowerPlatform.Dataverse.async_client import AsyncDataverseClient
from PowerPlatform.Dataverse.operations.async_records import AsyncRecordOperations
from PowerPlatform.Dataverse.operations.async_query import AsyncQueryOperations
from PowerPlatform.Dataverse.operations.async_tables import AsyncTableOperations
from PowerPlatform.Dataverse.operations.async_files import AsyncFileOperations
from PowerPlatform.Dataverse.core._async_auth import _AsyncAuthManager
from PowerPlatform.Dataverse.core._async_http import _AsyncHttpClient, _AsyncResponse
from PowerPlatform.Dataverse.core.config import DataverseConfig

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_credential():
    """Return a mock AsyncTokenCredential that returns a token."""
    cred = AsyncMock()
    token = MagicMock()
    token.token = "mock-access-token"
    cred.get_token = AsyncMock(return_value=token)
    return cred


def _make_client(base_url="https://example.crm.dynamics.com"):
    """Return an AsyncDataverseClient with a mock credential."""
    return AsyncDataverseClient(base_url, _make_mock_credential())


# ---------------------------------------------------------------------------
# AsyncDataverseClient instantiation
# ---------------------------------------------------------------------------


class TestAsyncDataverseClientInstantiation(unittest.IsolatedAsyncioTestCase):
    def test_can_import(self):
        """AsyncDataverseClient can be imported."""
        from PowerPlatform.Dataverse.async_client import AsyncDataverseClient as C

        assert C is AsyncDataverseClient

    def test_instantiate(self):
        """AsyncDataverseClient can be constructed with base_url and credential."""
        client = _make_client()
        assert client is not None

    def test_base_url_stripped(self):
        """Trailing slash is stripped from base_url."""
        client = _make_client("https://example.crm.dynamics.com/")
        assert client._base_url == "https://example.crm.dynamics.com"

    def test_empty_base_url_raises(self):
        """Empty base_url raises ValueError."""
        with self.assertRaises(ValueError):
            AsyncDataverseClient("", _make_mock_credential())

    def test_has_operation_namespaces(self):
        """Client exposes expected operation namespaces."""
        client = _make_client()
        assert isinstance(client.records, AsyncRecordOperations)
        assert isinstance(client.query, AsyncQueryOperations)
        assert isinstance(client.tables, AsyncTableOperations)
        assert isinstance(client.files, AsyncFileOperations)

    def test_auth_manager_created(self):
        """Client creates an _AsyncAuthManager."""
        client = _make_client()
        assert isinstance(client.auth, _AsyncAuthManager)

    def test_custom_config(self):
        """Custom DataverseConfig is stored."""
        config = DataverseConfig(language_code=1036)
        client = AsyncDataverseClient("https://example.crm.dynamics.com", _make_mock_credential(), config)
        assert client._config.language_code == 1036


# ---------------------------------------------------------------------------
# Context manager lifecycle
# ---------------------------------------------------------------------------


class TestAsyncContextManager(unittest.IsolatedAsyncioTestCase):
    async def test_aenter_returns_self(self):
        """__aenter__ returns the client itself."""
        client = _make_client()
        result = await client.__aenter__()
        assert result is client
        await client.close()

    async def test_aexit_closes_client(self):
        """__aexit__ marks the client as closed."""
        client = _make_client()
        await client.__aenter__()
        await client.__aexit__(None, None, None)
        assert client._closed is True

    async def test_async_with(self):
        """async with block works correctly."""
        async with _make_client() as client:
            assert not client._closed
        assert client._closed

    async def test_double_close_safe(self):
        """Calling close() multiple times is safe."""
        client = _make_client()
        await client.close()
        await client.close()  # Should not raise

    async def test_check_closed_raises(self):
        """Operations on a closed client raise RuntimeError."""
        client = _make_client()
        await client.close()
        with self.assertRaises(RuntimeError):
            client._check_closed()

    async def test_aenter_on_closed_raises(self):
        """Entering a closed client raises RuntimeError."""
        client = _make_client()
        await client.close()
        with self.assertRaises(RuntimeError):
            await client.__aenter__()


# ---------------------------------------------------------------------------
# _AsyncAuthManager
# ---------------------------------------------------------------------------


class TestAsyncAuthManager(unittest.IsolatedAsyncioTestCase):
    async def test_acquire_token(self):
        """_acquire_token returns _TokenPair with correct fields."""
        from PowerPlatform.Dataverse.core._auth import _TokenPair

        cred = _make_mock_credential()
        auth = _AsyncAuthManager(cred)
        pair = await auth._acquire_token("https://example.crm.dynamics.com/.default")
        assert isinstance(pair, _TokenPair)
        assert pair.access_token == "mock-access-token"
        assert pair.resource == "https://example.crm.dynamics.com/.default"


# ---------------------------------------------------------------------------
# _AsyncResponse
# ---------------------------------------------------------------------------


class TestAsyncResponse(unittest.TestCase):
    def test_status_code(self):
        """status_code is stored correctly."""
        r = _AsyncResponse(200, {}, b'{"key": "value"}')
        assert r.status_code == 200

    def test_text(self):
        """text property decodes body bytes."""
        r = _AsyncResponse(200, {}, b"hello")
        assert r.text == "hello"

    def test_json(self):
        """json() parses body bytes as JSON."""
        r = _AsyncResponse(200, {}, b'{"key": "value"}')
        assert r.json() == {"key": "value"}

    def test_empty_text(self):
        """Empty body gives empty text."""
        r = _AsyncResponse(204, {}, b"")
        assert r.text == ""


# ---------------------------------------------------------------------------
# _AsyncODataClient — core method mocking
# ---------------------------------------------------------------------------


class TestAsyncODataClient(unittest.IsolatedAsyncioTestCase):
    def _make_odata(self):
        from PowerPlatform.Dataverse.data._async_odata import _AsyncODataClient

        auth = _AsyncAuthManager(_make_mock_credential())
        return _AsyncODataClient(auth, "https://example.crm.dynamics.com")

    def test_init_sets_api(self):
        od = self._make_odata()
        assert od.api == "https://example.crm.dynamics.com/api/data/v9.2"

    def test_init_empty_url_raises(self):
        from PowerPlatform.Dataverse.data._async_odata import _AsyncODataClient

        with self.assertRaises(ValueError):
            _AsyncODataClient(_AsyncAuthManager(_make_mock_credential()), "")

    async def test_headers_contains_bearer(self):
        od = self._make_odata()
        headers = await od._headers()
        assert headers["Authorization"] == "Bearer mock-access-token"
        assert "OData-Version" in headers

    async def test_close_is_safe(self):
        od = self._make_odata()
        await od.close()  # No session open — should not raise


# ---------------------------------------------------------------------------
# AsyncRecordOperations — mock-based flow
# ---------------------------------------------------------------------------


class TestAsyncRecordOperations(unittest.IsolatedAsyncioTestCase):
    def _make_client_with_mock_odata(self):
        """Return a client whose _get_odata returns a fully mocked _AsyncODataClient."""
        client = _make_client()
        mock_odata = AsyncMock()
        mock_odata._call_scope = MagicMock(return_value=_NullContextManager())
        client._odata = mock_odata
        return client, mock_odata

    async def test_create_single(self):
        client, mock_od = self._make_client_with_mock_odata()
        mock_od._entity_set_from_schema_name = AsyncMock(return_value="accounts")
        mock_od._create = AsyncMock(return_value="00000000-0000-0000-0000-000000000001")

        result = await client.records.create("account", {"name": "Contoso"})

        mock_od._create.assert_awaited_once_with("accounts", "account", {"name": "Contoso"})
        assert result == "00000000-0000-0000-0000-000000000001"

    async def test_create_multiple(self):
        client, mock_od = self._make_client_with_mock_odata()
        ids = ["id1", "id2"]
        mock_od._entity_set_from_schema_name = AsyncMock(return_value="accounts")
        mock_od._create_multiple = AsyncMock(return_value=ids)

        result = await client.records.create("account", [{"name": "A"}, {"name": "B"}])

        mock_od._create_multiple.assert_awaited_once()
        assert result == ids

    async def test_update_single(self):
        client, mock_od = self._make_client_with_mock_odata()
        mock_od._update = AsyncMock()

        await client.records.update("account", "guid-1", {"telephone1": "555"})

        mock_od._update.assert_awaited_once_with("account", "guid-1", {"telephone1": "555"})

    async def test_update_multiple(self):
        client, mock_od = self._make_client_with_mock_odata()
        mock_od._update_by_ids = AsyncMock()

        await client.records.update("account", ["id1", "id2"], {"statecode": 1})

        mock_od._update_by_ids.assert_awaited_once_with("account", ["id1", "id2"], {"statecode": 1})

    async def test_delete_single(self):
        client, mock_od = self._make_client_with_mock_odata()
        mock_od._delete = AsyncMock()

        await client.records.delete("account", "guid-1")

        mock_od._delete.assert_awaited_once_with("account", "guid-1")

    async def test_delete_multiple_bulk(self):
        client, mock_od = self._make_client_with_mock_odata()
        mock_od._delete_multiple = AsyncMock(return_value="job-1")

        result = await client.records.delete("account", ["id1", "id2"])

        mock_od._delete_multiple.assert_awaited_once_with("account", ["id1", "id2"])
        assert result == "job-1"

    async def test_get_single(self):
        client, mock_od = self._make_client_with_mock_odata()
        mock_od._get = AsyncMock(return_value={"name": "Contoso", "accountid": "guid-1"})

        result = await client.records.get("account", "guid-1", select=["name"])

        mock_od._get.assert_awaited_once_with("account", "guid-1", select=["name"])
        assert result["name"] == "Contoso"

    async def test_get_multiple_returns_async_iterable(self):
        """get() without record_id returns an async iterable."""
        client, mock_od = self._make_client_with_mock_odata()

        async def _fake_get_multiple(*args, **kwargs):
            yield [{"name": "Contoso", "accountid": "guid-1"}]

        mock_od._get_multiple = _fake_get_multiple

        pages = await client.records.get("account", filter="statecode eq 0")
        collected = []
        async for page in pages:
            collected.extend(page)

        assert len(collected) == 1
        assert collected[0]["name"] == "Contoso"


# ---------------------------------------------------------------------------
# AsyncQueryOperations
# ---------------------------------------------------------------------------


class TestAsyncQueryOperations(unittest.IsolatedAsyncioTestCase):
    def _make_client_with_mock_odata(self):
        client = _make_client()
        mock_odata = AsyncMock()
        mock_odata._call_scope = MagicMock(return_value=_NullContextManager())
        client._odata = mock_odata
        return client, mock_odata

    async def test_sql_returns_records(self):
        client, mock_od = self._make_client_with_mock_odata()
        mock_od._query_sql = AsyncMock(return_value=[{"name": "Row1"}, {"name": "Row2"}])

        result = await client.query.sql("SELECT name FROM account")

        mock_od._query_sql.assert_awaited_once_with("SELECT name FROM account")
        assert len(result) == 2
        assert result[0]["name"] == "Row1"


# ---------------------------------------------------------------------------
# Helper: null context manager (sync, for mocking _call_scope)
# ---------------------------------------------------------------------------


class _NullContextManager:
    """Sync context manager that does nothing — used to mock _call_scope."""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass


if __name__ == "__main__":
    unittest.main()
