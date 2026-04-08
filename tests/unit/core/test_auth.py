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
