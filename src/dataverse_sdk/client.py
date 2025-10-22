from __future__ import annotations

from typing import Any, Dict, Optional, Union, List, Iterable

from azure.core.credentials import TokenCredential

from .auth import AuthManager
from .config import DataverseConfig
from .odata import ODataClient


class DataverseClient:
    """High-level client for Dataverse operations.

    This client exposes a simple, stable surface for:
    - OData CRUD: create, get, update, delete records
    - SQL (read-only): query SQL via ?sql parameter in Web API
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
            self._odata = ODataClient(
                self.auth,
                self._base_url,
                self._config,
            )
        return self._odata

    # ---------------- Unified CRUD: create/update/delete ----------------
    def create(self, logical_name: str, records: Union[Dict[str, Any], List[Dict[str, Any]]]) -> List[str]:
        """Create one or many records by logical (singular) name; returns list[str] of created IDs.

        Parameters
        ----------
        logical_name : str
            Logical (singular) entity name, e.g. "account".
        records : dict | list[dict]
            A single record dict or a list of record dicts.

        Returns
        -------
        list[str]
            List of created GUIDs (length 1 for single input).
        """
        od = self._get_odata()
        entity_set = od._entity_set_from_logical(logical_name)
        if isinstance(records, dict):
            rid = od._create(entity_set, logical_name, records)
            # _create returns str on single input
            if not isinstance(rid, str):
                raise TypeError("_create (single) did not return GUID string")
            return [rid]
        if isinstance(records, list):
            ids = od._create_multiple(entity_set, logical_name, records)
            if not isinstance(ids, list) or not all(isinstance(x, str) for x in ids):
                raise TypeError("_create (multi) did not return list[str]")
            return ids
        raise TypeError("records must be dict or list[dict]")

    def update(self, logical_name: str, ids: Union[str, List[str]], changes: Union[Dict[str, Any], List[Dict[str, Any]]]) -> None:
        """Update one or many records. Returns None.

        Usage patterns:
            update("accounts", some_id, {"telephone1": "555"})
            update("accounts", [id1, id2], {"statecode": 1})            # broadcast
            update("accounts", [id1, id2], [{"name": "A"}, {"name": "B"}])  # 1:1

        Rules:
        - If ids is a list and changes is a single dict -> broadcast.
        - If both are lists they must have equal length.
        - Single update discards representation (performance-focused).
        """
        od = self._get_odata()
        if isinstance(ids, str):
            if not isinstance(changes, dict):
                raise TypeError("For single id, changes must be a dict")
            od._update(logical_name, ids, changes)  # discard representation
            return None
        if not isinstance(ids, list):
            raise TypeError("ids must be str or list[str]")
        od._update_by_ids(logical_name, ids, changes)
        return None

    def delete(self, logical_name: str, ids: Union[str, List[str]]) -> None:
        """Delete one or many records (GUIDs). Returns None."""
        od = self._get_odata()
        if isinstance(ids, str):
            od._delete(logical_name, ids)
            return None
        if not isinstance(ids, list):
            raise TypeError("ids must be str or list[str]")
        od._delete_multiple(logical_name, ids)
        return None

    def get(
        self,
        logical_name: str,
        record_id: Optional[str] = None,
        select: Optional[List[str]] = None,
        filter: Optional[str] = None,
        orderby: Optional[List[str]] = None,
        top: Optional[int] = None,
        expand: Optional[List[str]] = None,
        page_size: Optional[int] = None,
    ) -> Union[Dict[str, Any], Iterable[List[Dict[str, Any]]]]:
        """Fetch single record by ID or multiple records as a generator."""
        od = self._get_odata()
        if record_id is not None:
            if not isinstance(record_id, str):
                raise TypeError("record_id must be str")
            return od._get(
                logical_name,
                record_id, 
                select=select,
            )
        return od._get_multiple(
            logical_name,
            select=select,
            filter=filter,
            orderby=orderby,
            top=top,
            expand=expand,
            page_size=page_size,
        )

    # SQL via Web API sql parameter
    def query_sql(self, sql: str):
        """Execute a read-only SQL query using the Dataverse Web API `?sql=` capability.

        The query must follow the currently supported subset: single SELECT with optional WHERE,
        TOP (integer), ORDER BY (columns only), and simple alias after FROM. Example:
            ``SELECT TOP 3 accountid, name FROM account ORDER BY name DESC``

        Parameters
        ----------
        sql : str
            Supported single SELECT statement.

        Returns
        -------
        list[dict]
            Result rows (empty list if none).
        """
        return self._get_odata()._query_sql(sql)

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
        return self._get_odata()._get_table_info(tablename)

    def create_table(self, tablename: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Create a simple custom table.

        Parameters
        ----------
        tablename : str
            Friendly name (``"SampleItem"``) or a full schema name (``"new_SampleItem"``).
        schema : dict[str, Any]
            Column definitions mapping logical names (without prefix) to types.
            Supported:
              - Primitive types: ``string``, ``int``, ``decimal``, ``float``, ``datetime``, ``bool``
              - Enum subclass (IntEnum preferred): generates a local option set.
                Optional multilingual labels via ``__labels__ = {1033: {"Active": "Active"}, 1036: {"Active": "Actif"}}``

        Returns
        -------
        dict
            Metadata summary including ``entity_schema``, ``entity_set_name``,
            ``entity_logical_name``, ``metadata_id``, and ``columns_created``.
        """
        return self._get_odata()._create_table(tablename, schema)

    def delete_table(self, tablename: str) -> None:
        """Delete a custom table by name.

        Parameters
        ----------
        tablename : str
            Friendly name (``"SampleItem"``) or a full schema name (``"new_SampleItem"``).
        """
        self._get_odata()._delete_table(tablename)

    def list_tables(self) -> list[str]:
        """List all custom tables in the Dataverse environment.

        Returns
        -------
        list[str]
            A list of table names.
        """
        return self._get_odata()._list_tables()

    # File upload
    def upload_file(
        self,
        logical_name: str,
        record_id: str,
        file_name_attribute: str,
        path: str,
        mode: Optional[str] = None,
        mime_type: Optional[str] = None,
        if_none_match: bool = True,
    ) -> None:
        """Upload a file to a Dataverse file column using a logical (singular) name.

        Parameters
        ----------
        logical_name : str
            Singular logical table name, e.g. "account".
        record_id : str
            GUID of the target record.
        file_name_attribute : str
            Logical name of the file column attribute.
        path : str
            Local filesystem path to the file. Stored filename will be the basename of this path.
        mode : str | None, keyword-only, optional
            Upload strategy: "auto" (default), "small", or "chunk".
        mime_type : str | None, keyword-only, optional
            Explicit MIME type to persist with the file (e.g. "application/pdf").
        if_none_match : bool, keyword-only, optional
            When True (default), sends ``If-None-Match: null`` to only succeed if the column is 
            currently empty. Set False to always overwrite (uses ``If-Match: *``).
            Used for "small" and "chunk" modes only.

        Returns
        -------
        None
            Returns nothing on success. Raises on failure.
        """
        od = self._get_odata()
        entity_set = od._entity_set_from_logical(logical_name)
        od.upload_file(
            entity_set,
            record_id,
            file_name_attribute,
            path,
            mode=mode,
            mime_type=mime_type,
            if_none_match=if_none_match,
        )
        return None

    # Cache utilities
    def flush_cache(self, kind) -> int:
        """Flush cached client metadata/state.

        Currently supported kinds:
          - 'picklist': clears entries from the picklist label cache used by label -> int conversion.

        Parameters
        ----------
        kind : str
            Cache kind to flush. Only 'picklist' is implemented today. Future kinds
            (e.g. 'entityset', 'primaryid') can be added without breaking the signature.

        Returns
        -------
        int
            Number of cache entries removed.

        """
        return self._get_odata()._flush_cache(kind)

__all__ = ["DataverseClient"]

