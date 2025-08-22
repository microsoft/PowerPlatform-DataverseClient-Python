from __future__ import annotations

from typing import Any, Dict, Optional

from azure.core.credentials import TokenCredential

from .auth import AuthManager
from .config import DataverseConfig
from .odata import ODataClient


class DataverseClient:
    """High-level client for Dataverse operations.

    This client exposes a simple, stable surface for:
    - OData CRUD: create, get, update, delete records
    - SQL (read-only): execute T-SQL via Dataverse Custom API (no ODBC/TDS driver)
    - Table metadata: create, inspect, and delete simple custom tables

    The client owns authentication (Azure Identity) and configuration, and delegates
    requests to an internal OData client responsible for HTTP calls and URL shaping.

    Parameters
    ----------
    base_url : str
        Your Dataverse environment URL, for example:
        ``"https://<org>.crm.dynamics.com"``. A trailing slash is ignored.
    credential : azure.core.credentials.TokenCredential | None, optional
        Any Azure Identity credential. If omitted, the SDK uses
        ``DefaultAzureCredential`` internally.
    config : DataverseConfig | None, optional
        Optional configuration (language code, SQL API name, HTTP timeouts/retries).

    Raises
    ------
    ValueError
        If ``base_url`` is missing or empty after trimming.
    """

    def __init__(
        self,
        base_url: str,
        credential: Optional[TokenCredential] = None,
        config: Optional[DataverseConfig] = None,
    ) -> None:
        self.auth = AuthManager(credential)
        self._base_url = (base_url or "").rstrip("/")
        if not self._base_url:
            raise ValueError("base_url is required.")
        self._config = config or DataverseConfig.from_env()
        self._odata: Optional[ODataClient] = None

    def _get_odata(self) -> ODataClient:
        """Get or create the internal OData client instance.

        Returns
        -------
        ODataClient
            The lazily-initialized low-level client used to perform requests.
        """
        if self._odata is None:
            self._odata = ODataClient(self.auth, self._base_url, self._config)
        return self._odata

    # CRUD
    def create(self, entity: str, record_data: dict) -> dict:
        """Create a record and return its full representation.

        Parameters
        ----------
        entity : str
            Entity set name (plural logical name), e.g., ``"accounts"``.
        record_data : dict
            Field-value pairs to set on the new record.

        Returns
        -------
        dict
            The created record as returned by the Web API (``Prefer: return=representation``).

        Raises
        ------
        requests.exceptions.HTTPError
            If the request fails (via ``raise_for_status`` in the underlying client).
        """
        return self._get_odata().create(entity, record_data)

    def update(self, entity: str, record_id: str, record_data: dict) -> dict:
        """Update a record and return its full representation.

        Parameters
        ----------
        entity : str
            Entity set name (plural logical name).
        record_id : str
            The record GUID (with or without parentheses).
        record_data : dict
            Field-value pairs to update.

        Returns
        -------
        dict
            The updated record payload.
        """
        return self._get_odata().update(entity, record_id, record_data)

    def delete(self, entity: str, record_id: str) -> None:
        """Delete a record by ID.

        Parameters
        ----------
        entity : str
            Entity set name (plural logical name).
        record_id : str
            The record GUID (with or without parentheses).
        """
        self._get_odata().delete(entity, record_id)

    def get(self, entity: str, record_id: str) -> dict:
        """Fetch a record by ID.

        Parameters
        ----------
        entity : str
            Entity set name (plural logical name).
        record_id : str
            The record GUID (with or without parentheses).

        Returns
        -------
        dict
            The record JSON payload.
        """
        return self._get_odata().get(entity, record_id)

    # SQL via Custom API
    def query_sql(self, tsql: str):
        """Execute a read-only SQL query via the configured Custom API.

        Parameters
        ----------
        tsql : str
            A SELECT-only T-SQL statement (e.g., ``"SELECT TOP 3 * FROM account"``).

        Returns
        -------
        list[dict]
            Rows as a list of dictionaries.
        """
        return self._get_odata().query_sql(tsql)

    # Table metadata helpers
    def get_table_info(self, tablename: str) -> Optional[Dict[str, Any]]:
        """Get basic metadata for a custom table if it exists.

        Parameters
        ----------
        tablename : str
            Friendly name (e.g., ``"SampleItem"``) or full schema name
            (e.g., ``"new_SampleItem"``).

        Returns
        -------
        dict | None
            Dict with keys like ``entity_schema``, ``entity_logical_name``,
            ``entity_set_name``, and ``metadata_id``; ``None`` if not found.
        """
        return self._get_odata().get_table_info(tablename)

    def create_table(self, tablename: str, schema: Dict[str, str]) -> Dict[str, Any]:
        """Create a simple custom table.

        Parameters
        ----------
        tablename : str
            Friendly name (``"SampleItem"``) or a full schema name (``"new_SampleItem"``).
        schema : dict[str, str]
            Column definitions mapping logical names (without prefix) to types.
            Supported: ``string``, ``int``, ``decimal``, ``float``, ``datetime``, ``bool``.

        Returns
        -------
        dict
            Metadata summary including ``entity_schema``, ``entity_set_name``,
            ``entity_logical_name``, ``metadata_id``, and ``columns_created``.
        """
        return self._get_odata().create_table(tablename, schema)

    def delete_table(self, tablename: str) -> None:
        """Delete a custom table by name.

        Parameters
        ----------
        tablename : str
            Friendly name (``"SampleItem"``) or a full schema name (``"new_SampleItem"``).
        """
        self._get_odata().delete_table(tablename)


__all__ = ["DataverseClient"]
        
