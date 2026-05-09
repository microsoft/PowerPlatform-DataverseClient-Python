# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Tests for operation_context support on DataverseClient and User-Agent header."""

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.config import DataverseConfig
from PowerPlatform.Dataverse.data._odata import _ODataClient, _USER_AGENT


class TestOperationContextConfig(unittest.TestCase):
    """Tests for operation_context on DataverseConfig."""

    def test_default_is_none(self):
        config = DataverseConfig.from_env()
        self.assertIsNone(config.operation_context)

    def test_explicit_value(self):
        config = DataverseConfig(operation_context="app=test/1.0;agent=claude-code")
        self.assertEqual(config.operation_context, "app=test/1.0;agent=claude-code")

    def test_default_constructor_is_none(self):
        config = DataverseConfig()
        self.assertIsNone(config.operation_context)


class TestOperationContextClient(unittest.TestCase):
    """Tests for operation_context kwarg on DataverseClient."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"

    def test_kwarg_sets_config(self):
        client = DataverseClient(
            self.base_url,
            self.mock_credential,
            operation_context="app=test/1.0;skill=dv-data;agent=claude-code",
        )
        self.assertEqual(client._config.operation_context, "app=test/1.0;skill=dv-data;agent=claude-code")

    def test_no_kwarg_leaves_config_default(self):
        client = DataverseClient(self.base_url, self.mock_credential)
        self.assertIsNone(client._config.operation_context)

    def test_config_and_kwarg_raises(self):
        config = DataverseConfig(operation_context="app=test/1.0")
        with self.assertRaises(ValueError):
            DataverseClient(
                self.base_url,
                self.mock_credential,
                config=config,
                operation_context="app=other/2.0",
            )

    def test_config_alone_works(self):
        config = DataverseConfig(operation_context="app=test/1.0;agent=copilot")
        client = DataverseClient(self.base_url, self.mock_credential, config=config)
        self.assertEqual(client._config.operation_context, "app=test/1.0;agent=copilot")


class TestOperationContextUserAgent(unittest.TestCase):
    """Tests for User-Agent header with operation_context."""

    def setUp(self):
        self.dummy_auth = MagicMock()
        token_result = MagicMock()
        token_result.access_token = "test-token"
        self.dummy_auth._acquire_token.return_value = token_result
        self.base_url = "https://org.example.com"

    def test_default_user_agent_unchanged(self):
        odata = _ODataClient(self.dummy_auth, self.base_url)
        headers = odata._headers()
        self.assertEqual(headers["User-Agent"], _USER_AGENT)

    def test_operation_context_appended(self):
        ctx = "app=dataverse-skills/1.2.1;skill=dv-data;agent=claude-code"
        config = DataverseConfig(operation_context=ctx)
        odata = _ODataClient(self.dummy_auth, self.base_url, config=config)
        headers = odata._headers()
        self.assertEqual(headers["User-Agent"], f"{_USER_AGENT} ({ctx})")

    def test_none_context_no_parentheses(self):
        config = DataverseConfig(operation_context=None)
        odata = _ODataClient(self.dummy_auth, self.base_url, config=config)
        headers = odata._headers()
        self.assertNotIn("(", headers["User-Agent"])

    def test_empty_string_context_no_parentheses(self):
        config = DataverseConfig(operation_context="")
        odata = _ODataClient(self.dummy_auth, self.base_url, config=config)
        headers = odata._headers()
        self.assertNotIn("(", headers["User-Agent"])

    def test_control_chars_rejected(self):
        for bad in ["has\rnewline", "has\nnewline", "has\x00null"]:
            config = DataverseConfig(operation_context=bad)
            with self.assertRaises(ValueError):
                _ODataClient(self.dummy_auth, self.base_url, config=config)
