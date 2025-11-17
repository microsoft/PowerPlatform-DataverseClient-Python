# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Authentication helpers for Dataverse.

This module provides :class:`~PowerPlatform.Dataverse.core._auth._AuthManager`, a thin wrapper over any Azure Identity
``TokenCredential`` for acquiring OAuth2 access tokens, and :class:`~PowerPlatform.Dataverse.core._auth._TokenPair` for
storing the acquired token alongside its scope.
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
    Azure Identity-based authentication manager for Dataverse.

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
