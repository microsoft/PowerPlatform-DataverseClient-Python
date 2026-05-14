# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.core._auth import _AuthManager, _TokenPair


class TestAuthManager(unittest.TestCase):
    """Tests for _AuthManager credential validation and token acquisition."""

    def test_non_token_credential_raises(self):
        """_AuthManager raises TypeError when credential does not implement TokenCredential."""
        with self.assertRaises(TypeError) as ctx:
            _AuthManager("not-a-credential")
        self.assertEqual(
            str(ctx.exception),
            "credential must implement azure.core.credentials.TokenCredential.",
        )

    def test_acquire_token_returns_token_pair(self):
        """_acquire_token calls get_token and returns a _TokenPair with scope and token."""
        mock_credential = MagicMock(spec=TokenCredential)
        mock_credential.get_token.return_value = MagicMock(token="my-access-token")

        manager = _AuthManager(mock_credential)
        result = manager._acquire_token("https://org.crm.dynamics.com/.default")

        mock_credential.get_token.assert_called_once_with("https://org.crm.dynamics.com/.default")
        self.assertIsInstance(result, _TokenPair)
        self.assertEqual(result.resource, "https://org.crm.dynamics.com/.default")
        self.assertEqual(result.access_token, "my-access-token")

    def test_acquire_token_public_appends_default_scope(self):
        """acquire_token appends /.default to the resource URL and returns the access_token string."""
        mock_credential = MagicMock(spec=TokenCredential)
        mock_credential.get_token.return_value = MagicMock(token="dv-token")

        manager = _AuthManager(mock_credential)
        result = manager.acquire_token("https://org.crm.dynamics.com")

        mock_credential.get_token.assert_called_once_with("https://org.crm.dynamics.com/.default")
        self.assertEqual(result, "dv-token")

    def test_acquire_token_public_strips_trailing_slash(self):
        """acquire_token strips a trailing slash before constructing the scope."""
        mock_credential = MagicMock(spec=TokenCredential)
        mock_credential.get_token.return_value = MagicMock(token="t")

        manager = _AuthManager(mock_credential)
        manager.acquire_token("https://myenv.operations.dynamics.com/")

        mock_credential.get_token.assert_called_once_with("https://myenv.operations.dynamics.com/.default")

    def test_acquire_token_public_supports_alternate_resource(self):
        """acquire_token works for any resource URL (e.g. linked Finance & Operations env)."""
        mock_credential = MagicMock(spec=TokenCredential)
        mock_credential.get_token.return_value = MagicMock(token="fno-token")

        manager = _AuthManager(mock_credential)
        result = manager.acquire_token("https://myenv.operations.dynamics.com")

        mock_credential.get_token.assert_called_once_with("https://myenv.operations.dynamics.com/.default")
        self.assertEqual(result, "fno-token")

    def test_acquire_token_public_empty_url_raises(self):
        """acquire_token raises ValueError when resource_url is empty after trim and does not call get_token."""
        mock_credential = MagicMock(spec=TokenCredential)
        manager = _AuthManager(mock_credential)

        with self.assertRaises(ValueError):
            manager.acquire_token("")
        with self.assertRaises(ValueError):
            manager.acquire_token("/")
        mock_credential.get_token.assert_not_called()
