from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from azure.identity import DefaultAzureCredential
from azure.core.credentials import TokenCredential


@dataclass
class TokenPair:
    resource: str
    access_token: str


class AuthManager:
    """Azure Identity-based authentication helper for Dataverse.

    Uses DefaultAzureCredential by default, or a provided TokenCredential.
    """

    def __init__(self, credential: Optional[TokenCredential] = None) -> None:
        # Let callers inject any azure.identity credential; default to DAC
        self.credential: TokenCredential = credential or DefaultAzureCredential()

    def acquire_token(self, scope: str) -> TokenPair:
        """Acquire an access token for the given scope using Azure Identity."""
        token = self.credential.get_token(scope)
        return TokenPair(resource=scope, access_token=token.token)
