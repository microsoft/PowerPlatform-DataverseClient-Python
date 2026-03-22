# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Async authentication helpers for Dataverse.

This module provides :class:`~PowerPlatform.Dataverse.core._async_auth._AsyncAuthManager`, a thin
async wrapper over any Azure Identity ``AsyncTokenCredential`` for acquiring OAuth2 access tokens.
"""

from __future__ import annotations

from azure.core.credentials_async import AsyncTokenCredential

from ._auth import _TokenPair


class _AsyncAuthManager:
    """
    Azure Identity-based async authentication manager for Dataverse.

    :param credential: Azure Identity async credential implementation.
    :type credential: ~azure.core.credentials_async.AsyncTokenCredential
    """

    def __init__(self, credential: AsyncTokenCredential) -> None:
        self.credential: AsyncTokenCredential = credential

    async def _acquire_token(self, scope: str) -> _TokenPair:
        """
        Acquire an access token for the specified OAuth2 scope (async).

        :param scope: OAuth2 scope string, typically ``"https://<org>.crm.dynamics.com/.default"``.
        :type scope: :class:`str`
        :return: Token pair containing the scope and access token.
        :rtype: ~PowerPlatform.Dataverse.core._auth._TokenPair
        :raises ~azure.core.exceptions.ClientAuthenticationError: If token acquisition fails.
        """
        token = await self.credential.get_token(scope)
        return _TokenPair(resource=scope, access_token=token.token)
