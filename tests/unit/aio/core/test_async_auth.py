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
