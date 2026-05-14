# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Authentication helpers.

This module provides :class:`~PowerPlatform.Dataverse.core._auth._AuthManager`, a thin wrapper over any Azure Identity
``TokenCredential`` for acquiring OAuth2 access tokens for Microsoft AAD-protected resources -- Dataverse by default,
and any other resource (e.g. a linked Finance & Operations environment) when an explicit scope is supplied --
and :class:`~PowerPlatform.Dataverse.core._auth._TokenPair` for storing the acquired token alongside its scope.
"""

from __future__ import annotations

from dataclasses import dataclass

from azure.core.credentials import TokenCredential


@dataclass
class _TokenPair:
    """
    Container for an OAuth2 access token and its associated resource scope.

    :param resource: The OAuth2 scope/resource for which the token was acquired.
    :type resource: :class:`str`
    :param access_token: The access token string.
    :type access_token: :class:`str`
    """

    resource: str
    access_token: str


class _AuthManager:
    """
    Azure Identity-based authentication manager.

    Resource-agnostic: the scope passed to :meth:`_acquire_token` selects
    the target resource. The Dataverse client supplies its own
    ``<base_url>/.default`` scope on every internal request via
    :meth:`acquire_token`, and the same method can be called externally
    (through ``client.auth.acquire_token(...)``) to obtain tokens for
    other Microsoft AAD-protected resources -- for example a linked
    Finance & Operations environment.

    :param credential: Azure Identity credential implementation.
    :type credential: ~azure.core.credentials.TokenCredential
    :raises TypeError: If ``credential`` does not implement :class:`~azure.core.credentials.TokenCredential`.
    """

    def __init__(self, credential: TokenCredential) -> None:
        if not isinstance(credential, TokenCredential):
            raise TypeError("credential must implement azure.core.credentials.TokenCredential.")
        self.credential: TokenCredential = credential

    def _acquire_token(self, scope: str) -> _TokenPair:
        """
        Acquire an access token for the specified OAuth2 scope.

        :param scope: OAuth2 scope string, typically ``"https://<org>.crm.dynamics.com/.default"``.
        :type scope: :class:`str`
        :return: Token pair containing the scope and access token.
        :rtype: ~PowerPlatform.Dataverse.core._auth._TokenPair
        :raises ~azure.core.exceptions.ClientAuthenticationError: If token acquisition fails.
        """
        token = self.credential.get_token(scope)
        return _TokenPair(resource=scope, access_token=token.token)

    def acquire_token(self, resource_url: str) -> str:
        """
        Acquire an OAuth2 access token for a Microsoft AAD-protected resource.

        Resource-agnostic helper: pass the resource URL (Dataverse env URL
        for Dataverse, Finance & Operations env URL for F&O, etc.) and the
        ``/.default`` scope suffix is appended automatically before
        delegating to the underlying credential. Token caching, refresh,
        and silent reauthentication are the credential's responsibility;
        Azure Identity credentials cache in-memory by default so repeated
        calls are cheap.

        :param resource_url: Resource URL for the target Microsoft service
            (for example ``"https://myenv.operations.dynamics.com"``).
            Trailing slash is removed before scope construction.
        :type resource_url: :class:`str`

        :return: OAuth2 access token string suitable for placing in an
            ``Authorization: Bearer ...`` header.
        :rtype: :class:`str`

        :raises ValueError: If ``resource_url`` is empty after trimming.
        :raises ~azure.core.exceptions.ClientAuthenticationError: If token
            acquisition fails.

        Example:
            Acquire a token for a linked Finance & Operations environment
            using the same credential the Dataverse client was built with::

                client = DataverseClient(dv_url, credential)
                fno_token = client.auth.acquire_token(
                    "https://myenv.operations.dynamics.com"
                )
        """
        target = (resource_url or "").rstrip("/")
        if not target:
            raise ValueError("resource_url must not be empty.")
        scope = f"{target}/.default"
        return self._acquire_token(scope).access_token
