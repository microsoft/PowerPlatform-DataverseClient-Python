from __future__ import annotations

from dataclasses import dataclass

from azure.core.credentials import TokenCredential


@dataclass
class TokenPair:
    resource: str
    access_token: str


class AuthManager:
    """Azure Identity-based authentication helper for Dataverse."""

    def __init__(self, credential: TokenCredential) -> None:
        if not isinstance(credential, TokenCredential):
            raise TypeError(
                "credential must implement azure.core.credentials.TokenCredential."
            )
        self.credential: TokenCredential = credential

    def acquire_token(self, scope: str) -> TokenPair:
        """Acquire an access token for the given scope using Azure Identity."""
        token = self.credential.get_token(scope)
        return TokenPair(resource=scope, access_token=token.token)
