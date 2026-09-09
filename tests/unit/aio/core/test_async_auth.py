# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import pytest
from unittest.mock import AsyncMock, MagicMock

from azure.core.credentials_async import AsyncTokenCredential

from PowerPlatform.Dataverse.aio.core._async_auth import _AsyncAuthManager
from PowerPlatform.Dataverse.core._auth import _TokenPair


class TestAsyncAuthManager:
    """Tests for _AsyncAuthManager credential validation and token acquisition."""

    def test_non_async_token_credential_raises(self):
        """_AsyncAuthManager raises TypeError when credential does not implement AsyncTokenCredential."""
        with pytest.raises(TypeError) as exc_info:
            _AsyncAuthManager("not-a-credential")
        assert "AsyncTokenCredential" in str(exc_info.value)

    def test_valid_credential_accepted(self):
        """_AsyncAuthManager accepts a valid AsyncTokenCredential."""
        mock_cred = MagicMock(spec=AsyncTokenCredential)
        manager = _AsyncAuthManager(mock_cred)
        assert manager.credential is mock_cred

    async def test_acquire_token_returns_token_pair(self):
        """_acquire_token calls get_token and returns a _TokenPair with scope and token."""
        mock_cred = MagicMock(spec=AsyncTokenCredential)
        mock_cred.get_token = AsyncMock(return_value=MagicMock(token="my-access-token"))

        manager = _AsyncAuthManager(mock_cred)
        result = await manager._acquire_token("https://org.crm.dynamics.com/.default")

        mock_cred.get_token.assert_called_once_with("https://org.crm.dynamics.com/.default")
        assert isinstance(result, _TokenPair)
        assert result.resource == "https://org.crm.dynamics.com/.default"
        assert result.access_token == "my-access-token"

    async def test_acquire_token_different_scope(self):
        """_acquire_token passes the scope string through to get_token."""
        mock_cred = MagicMock(spec=AsyncTokenCredential)
        mock_cred.get_token = AsyncMock(return_value=MagicMock(token="tok"))

        manager = _AsyncAuthManager(mock_cred)
        await manager._acquire_token("https://example.crm10.dynamics.com/.default")

        mock_cred.get_token.assert_called_once_with("https://example.crm10.dynamics.com/.default")


class TestAsyncAuthManagerAcquireToken:
    """Tests for the public, resource-agnostic ``_AsyncAuthManager.acquire_token``.

    Mirrors ``tests/unit/core/test_auth.py::TestAuthManagerAcquireToken`` so the async
    client keeps parity with the sync client for cross-resource token acquisition.
    """

    async def test_appends_default_scope_and_returns_token_string(self):
        """acquire_token appends /.default to the resource URL and returns the access token string."""
        mock_cred = MagicMock(spec=AsyncTokenCredential)
        mock_cred.get_token = AsyncMock(return_value=MagicMock(token="dv-token"))

        manager = _AsyncAuthManager(mock_cred)
        result = await manager.acquire_token("https://org.crm.dynamics.com")

        mock_cred.get_token.assert_called_once_with("https://org.crm.dynamics.com/.default")
        assert result == "dv-token"

    async def test_strips_trailing_slash(self):
        """acquire_token strips trailing slashes before constructing the scope."""
        mock_cred = MagicMock(spec=AsyncTokenCredential)
        mock_cred.get_token = AsyncMock(return_value=MagicMock(token="t"))

        manager = _AsyncAuthManager(mock_cred)
        await manager.acquire_token("https://myenv.operations.dynamics.com/")

        mock_cred.get_token.assert_called_once_with("https://myenv.operations.dynamics.com/.default")

    async def test_strips_surrounding_whitespace(self):
        """acquire_token trims whitespace so a padded URL still yields a well-formed scope."""
        mock_cred = MagicMock(spec=AsyncTokenCredential)
        mock_cred.get_token = AsyncMock(return_value=MagicMock(token="t"))

        manager = _AsyncAuthManager(mock_cred)
        await manager.acquire_token("  https://myenv.operations.dynamics.com/  ")

        mock_cred.get_token.assert_called_once_with("https://myenv.operations.dynamics.com/.default")

    async def test_supports_alternate_resource(self):
        """acquire_token works for any resource URL (for example a linked Finance & Operations env)."""
        mock_cred = MagicMock(spec=AsyncTokenCredential)
        mock_cred.get_token = AsyncMock(return_value=MagicMock(token="fno-token"))

        manager = _AsyncAuthManager(mock_cred)
        result = await manager.acquire_token("https://myenv.operations.dynamics.com")

        mock_cred.get_token.assert_called_once_with("https://myenv.operations.dynamics.com/.default")
        assert result == "fno-token"

    @pytest.mark.parametrize("bad", ["", "   ", "/", "  //  ", None])
    async def test_blank_url_raises_without_calling_credential(self, bad):
        """Blank input fails locally with ValueError instead of requesting a malformed scope."""
        mock_cred = MagicMock(spec=AsyncTokenCredential)
        mock_cred.get_token = AsyncMock()
        manager = _AsyncAuthManager(mock_cred)

        with pytest.raises(ValueError):
            await manager.acquire_token(bad)
        mock_cred.get_token.assert_not_called()
