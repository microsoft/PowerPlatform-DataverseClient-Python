# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Async authentication helpers for Dataverse.

Provides :class:`_AsyncAuthManager` for acquiring OAuth 2.0 access tokens
from an ``azure.identity.aio`` async credential.
"""

from __future__ import annotations

import inspect
from dataclasses import dataclass

from azure.core.credentials_async import AsyncTokenCredential


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
    :type credential: ~azure.core.credentials_async.AsyncTokenCredential
    :raises TypeError: If ``credential`` does not implement
        :class:`~azure.core.credentials_async.AsyncTokenCredential`.
    """

    def __init__(self, credential: AsyncTokenCredential) -> None:
        if not inspect.iscoroutinefunction(getattr(credential, "get_token", None)):
            raise TypeError(
                "credential must implement AsyncTokenCredential with an async get_token() method. "
                "For async usage, pass a credential from azure.identity.aio "
                "(e.g. azure.identity.aio.DefaultAzureCredential)."
            )
        self.credential: AsyncTokenCredential = credential

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
