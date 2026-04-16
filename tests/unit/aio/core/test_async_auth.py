# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for _AsyncAuthManager."""

import pytest
from unittest.mock import AsyncMock, MagicMock

from azure.core.credentials_async import AsyncTokenCredential

from PowerPlatform.Dataverse.aio.core._async_auth import _AsyncAuthManager
from PowerPlatform.Dataverse.core._auth import _TokenPair


class TestAsyncAuthManager:
    """Tests for _AsyncAuthManager credential validation and token acquisition."""

    async def test_acquire_token_calls_credential_get_token(self):
        """_acquire_token calls get_token on the credential with the given scope."""
        mock_credential = AsyncMock(spec=AsyncTokenCredential)
        mock_token = MagicMock()
        mock_token.token = "bearer-xyz"
        mock_credential.get_token.return_value = mock_token

        auth = _AsyncAuthManager(mock_credential)
        pair = await auth._acquire_token("https://org.crm.dynamics.com/.default")

        mock_credential.get_token.assert_awaited_once_with("https://org.crm.dynamics.com/.default")
        assert isinstance(pair, _TokenPair)
        assert pair.access_token == "bearer-xyz"
        assert pair.resource == "https://org.crm.dynamics.com/.default"

    async def test_acquire_token_different_scopes(self):
        """_acquire_token correctly forwards different scope strings to the credential."""
        mock_credential = AsyncMock(spec=AsyncTokenCredential)
        mock_credential.get_token.return_value = MagicMock(token="tok2")

        auth = _AsyncAuthManager(mock_credential)
        pair = await auth._acquire_token("https://other.crm/.default")
        assert pair.resource == "https://other.crm/.default"
        assert pair.access_token == "tok2"

    def test_credential_stored(self):
        """_AsyncAuthManager stores the credential reference on the instance."""
        cred = AsyncMock(spec=AsyncTokenCredential)
        auth = _AsyncAuthManager(cred)
        assert auth.credential is cred

    def test_non_async_credential_raises_type_error(self):
        """_AsyncAuthManager raises TypeError when credential does not implement AsyncTokenCredential."""
        with pytest.raises(TypeError, match="AsyncTokenCredential"):
            _AsyncAuthManager(MagicMock())
