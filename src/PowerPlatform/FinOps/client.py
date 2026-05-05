"""Public client entry point for the FinOps SDK."""
from __future__ import annotations

from types import TracebackType
from typing import TYPE_CHECKING, Optional, Type

from ._auth import TokenProvider
from ._http import HttpClient
from .operations import RecordOperations

if TYPE_CHECKING:  # pragma: no cover
    from azure.core.credentials import TokenCredential


class FinOpsClient:
    """High-level client for a single Dynamics 365 Finance & Operations environment.

    Parameters
    ----------
    environment_url:
        Base URL of the FinOps environment, e.g.
        ``"https://my-finops-env.cloudax.dynamics.com"``. Trailing slashes are
        tolerated.
    credential:
        Any object satisfying ``azure.core.credentials.TokenCredential`` —
        ``azure.identity.AzureCliCredential``, ``DefaultAzureCredential``,
        ``ClientSecretCredential``, etc.
    scope:
        Optional override for the OAuth scope. Defaults to
        ``"<environment_url>/.default"`` (the standard Entra resource-scope form).

    Example
    -------
    >>> from azure.identity import AzureCliCredential
    >>> from PowerPlatform.FinOps import FinOpsClient
    >>> with FinOpsClient(env_url, AzureCliCredential()) as client:
    ...     loc = client.records.create("CustomersV3", {"CustomerAccount": "TEST", ...})
    ...     row = client.records.get("CustomersV3",
    ...                              {"dataAreaId": "usmf", "CustomerAccount": "TEST"})
    ...     client.records.update("CustomersV3",
    ...                           {"dataAreaId": "usmf", "CustomerAccount": "TEST"},
    ...                           {"OrganizationName": "Updated"})
    ...     client.records.delete("CustomersV3",
    ...                           {"dataAreaId": "usmf", "CustomerAccount": "TEST"})
    """

    def __init__(
        self,
        environment_url: str,
        credential: "TokenCredential",
        *,
        scope: Optional[str] = None,
        max_retries: int = 5,
        timeout: float = 60.0,
    ) -> None:
        if not environment_url:
            raise ValueError("environment_url is required")
        self._env_url = environment_url.rstrip("/")
        self._data_url = f"{self._env_url}/data"
        self._scope = scope or f"{self._env_url}/.default"
        self._token_provider = TokenProvider(credential, self._scope)
        self._http = HttpClient(
            self._token_provider, max_retries=max_retries, timeout=timeout
        )

        # Operation namespaces.
        self.records = RecordOperations(self)

    # -- accessors ------------------------------------------------------

    @property
    def environment_url(self) -> str:
        return self._env_url

    @property
    def data_url(self) -> str:
        return self._data_url

    @property
    def scope(self) -> str:
        return self._scope

    # -- lifecycle ------------------------------------------------------

    def close(self) -> None:
        self._http.close()

    def __enter__(self) -> "FinOpsClient":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        self.close()
