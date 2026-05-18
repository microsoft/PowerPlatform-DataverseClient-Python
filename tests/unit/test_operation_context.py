# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Tests for operation_context support on DataverseClient and User-Agent header."""

import unittest
from unittest.mock import MagicMock

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.config import DataverseConfig, OperationContext
from PowerPlatform.Dataverse.data._odata import _ODataClient, _USER_AGENT


class TestOperationContextValidation(unittest.TestCase):
    """Tests for OperationContext format validation and PII rejection."""

    def test_valid_single_pair(self):
        ctx = OperationContext(user_agent_context="app=test/1.0")
        self.assertEqual(ctx.user_agent_context, "app=test/1.0")

    def test_valid_multiple_pairs(self):
        ctx = OperationContext(user_agent_context="app=test/1.0;skill=dv-data;agent=claude-code")
        self.assertEqual(ctx.user_agent_context, "app=test/1.0;skill=dv-data;agent=claude-code")

    def test_valid_with_dots_slashes_hyphens(self):
        ctx = OperationContext(user_agent_context="app=dataverse-skills/1.2.1")
        self.assertEqual(ctx.user_agent_context, "app=dataverse-skills/1.2.1")

    def test_reject_empty(self):
        with self.assertRaises(ValueError):
            OperationContext(user_agent_context="")

    def test_reject_email(self):
        with self.assertRaises(ValueError):
            OperationContext(user_agent_context="myname@email.com")

    def test_reject_freeform_text(self):
        with self.assertRaises(ValueError):
            OperationContext(user_agent_context="my bank password is 1234")

    def test_reject_control_chars(self):
        for bad in ["has\rnewline", "has\nnewline", "has\x00null"]:
            with self.assertRaises(ValueError):
                OperationContext(user_agent_context=bad)

    def test_reject_spaces(self):
        with self.assertRaises(ValueError):
            OperationContext(user_agent_context="app=my app")

    def test_reject_no_equals(self):
        with self.assertRaises(ValueError):
            OperationContext(user_agent_context="justaplainstring")


class TestOperationContextConfig(unittest.TestCase):
    """Tests for operation_context on DataverseConfig."""

    def test_default_is_none(self):
        config = DataverseConfig.from_env()
        self.assertIsNone(config.operation_context)

    def test_explicit_value(self):
        ctx = OperationContext(user_agent_context="app=test/1.0;agent=claude-code")
        config = DataverseConfig(operation_context=ctx)
        self.assertEqual(config.operation_context.user_agent_context, "app=test/1.0;agent=claude-code")

    def test_default_constructor_is_none(self):
        config = DataverseConfig()
        self.assertIsNone(config.operation_context)


class TestOperationContextClient(unittest.TestCase):
    """Tests for context kwarg on DataverseClient."""

    def setUp(self):
        self.mock_credential = MagicMock(spec=TokenCredential)
        self.base_url = "https://example.crm.dynamics.com"

    def test_kwarg_sets_config(self):
        ctx = OperationContext(user_agent_context="app=test/1.0;skill=dv-data;agent=claude-code")
        client = DataverseClient(
            self.base_url,
            self.mock_credential,
            context=ctx,
        )
        self.assertEqual(
            client._config.operation_context.user_agent_context,
            "app=test/1.0;skill=dv-data;agent=claude-code",
        )

    def test_no_kwarg_leaves_config_default(self):
        client = DataverseClient(self.base_url, self.mock_credential)
        self.assertIsNone(client._config.operation_context)

    def test_config_and_context_raises(self):
        ctx = OperationContext(user_agent_context="app=test/1.0")
        config = DataverseConfig(operation_context=ctx)
        with self.assertRaises(ValueError):
            DataverseClient(
                self.base_url,
                self.mock_credential,
                config=config,
                context=OperationContext(user_agent_context="app=other/2.0"),
            )

    def test_config_alone_works(self):
        ctx = OperationContext(user_agent_context="app=test/1.0;agent=copilot")
        config = DataverseConfig(operation_context=ctx)
        client = DataverseClient(self.base_url, self.mock_credential, config=config)
        self.assertEqual(
            client._config.operation_context.user_agent_context,
            "app=test/1.0;agent=copilot",
        )


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
        ctx_str = "app=dataverse-skills/1.2.1;skill=dv-data;agent=claude-code"
        ctx = OperationContext(user_agent_context=ctx_str)
        config = DataverseConfig(operation_context=ctx)
        odata = _ODataClient(self.dummy_auth, self.base_url, config=config)
        headers = odata._headers()
        self.assertEqual(headers["User-Agent"], f"{_USER_AGENT} ({ctx_str})")

    def test_none_context_no_parentheses(self):
        config = DataverseConfig(operation_context=None)
        odata = _ODataClient(self.dummy_auth, self.base_url, config=config)
        headers = odata._headers()
        self.assertNotIn("(", headers["User-Agent"])

    def test_empty_string_rejected_at_creation(self):
        with self.assertRaises(ValueError):
            OperationContext(user_agent_context="")
