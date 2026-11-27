# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Tests for filling ``agent=unknown`` from the coding agent that started the process."""

import os
import unittest
from unittest.mock import MagicMock, patch

from PowerPlatform.Dataverse.core._agent_detection import _detect_agent, _fill_unknown_agent
from PowerPlatform.Dataverse.core.config import DataverseConfig, OperationContext
from PowerPlatform.Dataverse.data._odata import _ODataClient, _USER_AGENT

UNKNOWN_CTX = "app=dataverse-skills/1.17.0;skill=dv-data;agent=unknown"


class TestDetectAgent(unittest.TestCase):
    def test_no_markers(self):
        self.assertIsNone(_detect_agent({}))

    def test_vendor_markers(self):
        cases = {
            "CLAUDECODE": "claude-code",
            "CLAUDE_CODE": "claude-code",
            "CODEX_THREAD_ID": "codex",
            "CODEX_SANDBOX": "codex",
            "CODEX_CI": "codex",
            "GEMINI_CLI": "gemini-cli",
            "COPILOT_CLI": "copilot",
            "ANTIGRAVITY_AGENT": "antigravity-cli",
            "CURSOR_AGENT": "cursor",
        }
        for var, expected in cases.items():
            with self.subTest(var=var):
                self.assertEqual(_detect_agent({var: "1"}), expected)

    def test_empty_marker_is_ignored(self):
        self.assertIsNone(_detect_agent({"CLAUDECODE": ""}))

    def test_cursor_agent_exec_role(self):
        self.assertEqual(_detect_agent({"CURSOR_EXTENSION_HOST_ROLE": "agent-exec"}), "cursor")

    def test_cursor_terminal_alone_is_not_an_agent(self):
        self.assertIsNone(_detect_agent({"CURSOR_TRACE_ID": "abc"}))

    def test_ai_agent_takes_precedence(self):
        self.assertEqual(_detect_agent({"AI_AGENT": "codex", "CLAUDECODE": "1"}), "codex")

    def test_ai_agent_aliases(self):
        cases = {
            "github_copilot_vscode_agent": "copilot",
            "github-copilot-cli": "copilot",
            "Claude-Code": "claude-code",
            "gemini": "gemini-cli",
            "cursor-cli": "cursor",
        }
        for value, expected in cases.items():
            with self.subTest(value=value):
                self.assertEqual(_detect_agent({"AI_AGENT": value}), expected)

    def test_ai_agent_unlisted_safe_value_passes_through(self):
        self.assertEqual(_detect_agent({"AI_AGENT": "opencode"}), "opencode")

    def test_ai_agent_unsafe_value_falls_back_to_markers(self):
        self.assertEqual(_detect_agent({"AI_AGENT": "my agent@2", "GEMINI_CLI": "1"}), "gemini-cli")
        self.assertIsNone(_detect_agent({"AI_AGENT": "x" * 41}))

    def test_amp_wins_over_claudecode(self):
        self.assertEqual(_detect_agent({"AGENT": "amp", "CLAUDECODE": "1"}), "amp")


class TestFillUnknownAgent(unittest.TestCase):
    def test_unknown_is_replaced(self):
        self.assertEqual(
            _fill_unknown_agent(UNKNOWN_CTX, {"CLAUDECODE": "1"}),
            "app=dataverse-skills/1.17.0;skill=dv-data;agent=claude-code",
        )

    def test_surface_suffix_is_kept(self):
        self.assertEqual(
            _fill_unknown_agent("app=x/1.0;agent=unknown/jetbrains", {"CODEX_THREAD_ID": "t"}),
            "app=x/1.0;agent=codex/jetbrains",
        )

    def test_unknown_kept_without_markers(self):
        self.assertEqual(_fill_unknown_agent(UNKNOWN_CTX, {}), UNKNOWN_CTX)

    def test_explicit_agent_is_never_overridden(self):
        ctx = "app=x/1.0;agent=copilot"
        self.assertEqual(_fill_unknown_agent(ctx, {"CLAUDECODE": "1"}), ctx)

    def test_agent_key_is_never_added(self):
        ctx = "app=x/1.0;skill=dv-data"
        self.assertEqual(_fill_unknown_agent(ctx, {"CLAUDECODE": "1"}), ctx)

    def test_similar_values_are_not_touched(self):
        for ctx in ("app=x/1.0;agent=unknown-tool", "app=x/1.0;skill=unknown"):
            with self.subTest(ctx=ctx):
                self.assertEqual(_fill_unknown_agent(ctx, {"CLAUDECODE": "1"}), ctx)

    def test_result_is_a_valid_operation_context(self):
        filled = _fill_unknown_agent(UNKNOWN_CTX, {"AI_AGENT": "github_copilot_vscode_agent"})
        self.assertEqual(OperationContext(user_agent_context=filled).user_agent_context, filled)


class TestUserAgentHeader(unittest.TestCase):
    def setUp(self):
        self.auth = MagicMock()
        self.auth._acquire_token.return_value = MagicMock(access_token="test-token")
        self.base_url = "https://org.example.com"

    def _user_agent(self, config=None):
        return _ODataClient(self.auth, self.base_url, config=config)._headers()["User-Agent"]

    @patch.dict(os.environ, {"CLAUDECODE": "1", "AI_AGENT": "codex"})
    def test_client_without_context_is_unchanged_inside_an_agent(self):
        self.assertEqual(self._user_agent(), _USER_AGENT)
        self.assertEqual(self._user_agent(DataverseConfig()), _USER_AGENT)

    @patch.dict(os.environ, {"CODEX_THREAD_ID": "t"}, clear=True)
    def test_unknown_agent_filled_in_header(self):
        config = DataverseConfig(operation_context=OperationContext(user_agent_context=UNKNOWN_CTX))
        self.assertEqual(
            self._user_agent(config),
            f"{_USER_AGENT} (app=dataverse-skills/1.17.0;skill=dv-data;agent=codex)",
        )

    @patch.dict(os.environ, {"CODEX_THREAD_ID": "t"}, clear=True)
    def test_caller_context_object_is_not_modified(self):
        ctx = OperationContext(user_agent_context=UNKNOWN_CTX)
        self._user_agent(DataverseConfig(operation_context=ctx))
        self.assertEqual(ctx.user_agent_context, UNKNOWN_CTX)

    @patch.dict(os.environ, {}, clear=True)
    def test_unknown_agent_kept_outside_an_agent(self):
        config = DataverseConfig(operation_context=OperationContext(user_agent_context=UNKNOWN_CTX))
        self.assertEqual(self._user_agent(config), f"{_USER_AGENT} ({UNKNOWN_CTX})")


if __name__ == "__main__":
    unittest.main()
