# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Async authentication helpers for Dataverse.

Provides :class:`_AsyncAuthManager` for acquiring OAuth 2.0 access tokens
from an ``azure.identity.aio`` async credential.
"""

from __future__ import annotations

from azure.core.credentials_async import AsyncTokenCredential

from ...core._auth import _TokenPair


class _AsyncAuthManager:
    """Async Azure Identity-based authentication manager for Dataverse.

    Accepts any ``azure.identity.aio`` credential that exposes an async
    ``get_token(scope)`` coroutine (e.g. ``ClientSecretCredential``,
    ``DefaultAzureCredential``, ``InteractiveBrowserCredential``).

    :param credential: An async Azure Identity credential.
    :type credential: ~azure.core.credentials_async.AsyncTokenCredential
    :raises TypeError: If ``credential`` does not implement
        :class:`~azure.core.credentials_async.AsyncTokenCredential`.
    """

    def __init__(self, credential: AsyncTokenCredential) -> None:
        if not isinstance(credential, AsyncTokenCredential):
            raise TypeError("credential must implement AsyncTokenCredential.")
        self.credential: AsyncTokenCredential = credential

    async def _acquire_token(self, scope: str) -> _TokenPair:
        """Acquire an access token for *scope*.

        :param scope: OAuth2 scope string, e.g.
            ``"https://<org>.crm.dynamics.com/.default"``.
        :type scope: :class:`str`
        :return: Token pair containing the scope and access token.
        :rtype: ~PowerPlatform.Dataverse.core._auth._TokenPair
        """
        token = await self.credential.get_token(scope)
        return _TokenPair(resource=scope, access_token=token.token)
