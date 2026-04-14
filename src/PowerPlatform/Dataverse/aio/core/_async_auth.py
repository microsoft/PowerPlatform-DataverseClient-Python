# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Async authentication helpers for Dataverse.

Provides :class:`_AsyncAuthManager` for acquiring OAuth 2.0 access tokens
from an ``azure.identity.aio`` async credential.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class _AsyncTokenPair:
    """Container for an OAuth2 access token and its associated resource scope."""

    resource: str
    access_token: str


class _AsyncAuthManager:
    """Async Azure Identity-based authentication manager for Dataverse.

    Accepts any ``azure.identity.aio`` credential that exposes an async
    ``get_token(scope)`` coroutine (e.g. ``ClientSecretCredential``,
    ``DefaultAzureCredential``, ``InteractiveBrowserCredential``).

    :param credential: An async Azure Identity credential.
    """

    def __init__(self, credential) -> None:
        self.credential = credential

    async def _acquire_token(self, scope: str) -> _AsyncTokenPair:
        """Acquire an access token for *scope*.

        :param scope: OAuth2 scope string, e.g.
            ``"https://<org>.crm.dynamics.com/.default"``.
        :type scope: :class:`str`
        :return: Token pair containing the scope and access token.
        :rtype: ~PowerPlatform.Dataverse.aio.core._async_auth._AsyncTokenPair
        """
        token = await self.credential.get_token(scope)
        return _AsyncTokenPair(resource=scope, access_token=token.token)
