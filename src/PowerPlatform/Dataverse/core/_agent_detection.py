# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Detect the AI coding agent that started this process, for User-Agent attribution.

Coding agents mark the processes they launch with environment variables
(``CLAUDECODE``, ``CODEX_THREAD_ID``, ``GEMINI_CLI``, ...), and the cross-vendor
``AI_AGENT`` convention names the agent directly. The marker list and precedence
follow the GitHub CLI (``internal/agents/detect.go``) and ``@vercel/detect-agent``.

Detection is used only to replace an explicit ``agent=unknown`` in a caller-supplied
:class:`~PowerPlatform.Dataverse.core.config.OperationContext`. A client created
without an operation context never reads these variables, so non-agent use of the
SDK is unaffected.
"""

from __future__ import annotations

import os
import re
from typing import Mapping, Optional

__all__ = []

_AI_AGENT_RE = re.compile(r"^[a-z0-9_.-]{1,40}$")

# AI_AGENT values that name an agent the Dataverse plugin already reports under another name.
_AI_AGENT_ALIASES = {
    "claude": "claude-code",
    "claude-code": "claude-code",
    "codex": "codex",
    "cursor": "cursor",
    "cursor-cli": "cursor",
    "gemini": "gemini-cli",
    "gemini-cli": "gemini-cli",
    "antigravity": "antigravity-cli",
    "antigravity-cli": "antigravity-cli",
    "copilot": "copilot",
    "copilot-cli": "copilot",
    "github-copilot": "copilot",
    "github-copilot-cli": "copilot",
    "github_copilot_vscode_agent": "copilot",
    "github_copilot_cloud_agent": "copilot",
}

_UNKNOWN_AGENT_RE = re.compile(r"(^|;)agent=unknown(?=/|;|$)")


def _detect_agent(environ: Optional[Mapping[str, str]] = None) -> Optional[str]:
    """Return the agent that started this process, or ``None`` when no marker is present."""
    env = os.environ if environ is None else environ

    def is_set(name: str) -> bool:
        return bool(env.get(name))

    ai_agent = (env.get("AI_AGENT") or "").strip().lower()
    if ai_agent and _AI_AGENT_RE.match(ai_agent):
        return _AI_AGENT_ALIASES.get(ai_agent, ai_agent)

    # Amp also sets CLAUDECODE, so it is checked first.
    if env.get("AGENT") == "amp":
        return "amp"
    if is_set("CODEX_THREAD_ID") or is_set("CODEX_SANDBOX") or is_set("CODEX_CI"):
        return "codex"
    if is_set("GEMINI_CLI"):
        return "gemini-cli"
    if is_set("COPILOT_CLI"):
        return "copilot"
    if is_set("ANTIGRAVITY_AGENT"):
        return "antigravity-cli"
    if is_set("CLAUDECODE") or is_set("CLAUDE_CODE"):
        return "claude-code"
    # CURSOR_TRACE_ID is deliberately ignored: it is present in every Cursor
    # terminal, including commands a person runs by hand.
    if is_set("CURSOR_AGENT") or env.get("CURSOR_EXTENSION_HOST_ROLE") == "agent-exec":
        return "cursor"
    return None


def _fill_unknown_agent(context: str, environ: Optional[Mapping[str, str]] = None) -> str:
    """Replace ``agent=unknown`` in an operation context with the detected agent.

    Any ``/<surface>`` suffix is kept (``agent=unknown/jetbrains`` becomes
    ``agent=codex/jetbrains``). A context that names an agent, or has no ``agent``
    key, is returned unchanged.
    """
    if not _UNKNOWN_AGENT_RE.search(context):
        return context
    agent = _detect_agent(environ)
    if agent is None:
        return context
    return _UNKNOWN_AGENT_RE.sub(lambda m: f"{m.group(1)}agent={agent}", context, count=1)
