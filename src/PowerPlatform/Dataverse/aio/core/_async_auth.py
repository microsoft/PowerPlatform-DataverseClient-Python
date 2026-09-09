# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Async authentication helpers for Dataverse.

This module provides :class:`~PowerPlatform.Dataverse.aio.core._async_auth._AsyncAuthManager`,
a thin wrapper over any Azure Identity ``AsyncTokenCredential`` for acquiring OAuth2 access
tokens asynchronously for Microsoft Entra ID protected resources -- Dataverse by default, and
any other resource (for example a linked Dynamics 365 Finance & Operations environment) when a
different resource URL is supplied -- and reuses
:class:`~PowerPlatform.Dataverse.core._auth._TokenPair` for storing the acquired token alongside
its scope.
"""

from __future__ import annotations

from azure.core.credentials_async import AsyncTokenCredential

from ...core._auth import _TokenPair, _build_default_scope


class _AsyncAuthManager:
    """
    Azure Identity-based async authentication manager.

    Async counterpart to :class:`~PowerPlatform.Dataverse.core._auth._AuthManager` with the same
    resource-agnostic surface: the resource URL passed to :meth:`acquire_token` selects the target
    resource. The async Dataverse client supplies its own organization URL on every internal
    request, and the same method can be awaited by application code (through
    ``await client.auth.acquire_token(...)``) to obtain tokens for other Microsoft Entra ID
    protected resources -- for example a linked Dynamics 365 Finance & Operations environment.

    :param credential: Azure Identity async credential implementation.
    :type credential: ~azure.core.credentials_async.AsyncTokenCredential
    :raises TypeError: If ``credential`` does not implement :class:`~azure.core.credentials_async.AsyncTokenCredential`.
    """

    def __init__(self, credential: AsyncTokenCredential) -> None:
        if not isinstance(credential, AsyncTokenCredential):
            raise TypeError("credential must implement azure.core.credentials_async.AsyncTokenCredential.")
        self.credential: AsyncTokenCredential = credential

    async def _acquire_token(self, scope: str) -> _TokenPair:
        """
        Acquire an access token asynchronously for the specified OAuth2 scope.

        :param scope: OAuth2 scope string, typically ``"https://<org>.crm.dynamics.com/.default"``.
        :type scope: :class:`str`
        :return: Token pair containing the scope and access token.
        :rtype: ~PowerPlatform.Dataverse.core._auth._TokenPair
        :raises ~azure.core.exceptions.ClientAuthenticationError: If token acquisition fails.
        """
        token = await self.credential.get_token(scope)
        return _TokenPair(resource=scope, access_token=token.token)

    async def acquire_token(self, resource_url: str) -> str:
        """
        Acquire an OAuth2 access token asynchronously for a Microsoft Entra ID protected resource.

        Async counterpart of :meth:`~PowerPlatform.Dataverse.core._auth._AuthManager.acquire_token`.
        Resource-agnostic helper: pass the resource URL (the Dataverse environment URL for
        Dataverse, the Finance & Operations environment URL for ERP, and so on) and the
        ``/.default`` scope suffix is appended automatically before delegating to the underlying
        credential. Token caching, refresh, and silent reauthentication remain the credential's
        responsibility; Azure Identity credentials cache in memory by default, so repeated calls
        are cheap.

        :param resource_url: Resource URL for the target Microsoft service (for example
            ``"https://myenv.operations.dynamics.com"``). Surrounding whitespace and trailing
            slashes are removed before scope construction.
        :type resource_url: :class:`str`
        :return: OAuth2 access token string suitable for an ``Authorization: Bearer <token>`` header.
        :rtype: :class:`str`
        :raises ValueError: If ``resource_url`` is empty after trimming whitespace and trailing slashes.
        :raises ~azure.core.exceptions.ClientAuthenticationError: If token acquisition fails.

        Example:
            Acquire a token for a linked Finance & Operations environment using the same credential
            the async Dataverse client was built with::

                async with AsyncDataverseClient(dataverse_url, credential) as client:
                    fno_token = await client.auth.acquire_token("https://myenv.operations.dynamics.com")
        """
        pair = await self._acquire_token(_build_default_scope(resource_url))
        return pair.access_token
