# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Dataverse Web API client with CRUD, SQL query, and table/column metadata management."""

from __future__ import annotations

from typing import Any, Dict, Optional, List, Union, Iterable
from enum import Enum
import unicodedata
import time
import re
import json
from datetime import datetime, timezone
import importlib.resources as ir

from ..core.http import HttpClient
from .upload import ODataFileUpload
from ..core.errors import *
from ..core.error_codes import (
    http_subcode,
    is_transient_status,
    VALIDATION_SQL_NOT_STRING,
    VALIDATION_SQL_EMPTY,
    METADATA_ENTITYSET_NOT_FOUND,
    METADATA_ENTITYSET_NAME_MISSING,
    METADATA_TABLE_NOT_FOUND,
    METADATA_TABLE_ALREADY_EXISTS,
    METADATA_COLUMN_NOT_FOUND,
    VALIDATION_UNSUPPORTED_CACHE_KIND,
)

from ..__version__ import __version__ as _SDK_VERSION


_USER_AGENT = f"DataverseSvcPythonClient:{_SDK_VERSION}"
_GUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")


class ODataClient(ODataFileUpload):
    """Dataverse Web API client: CRUD, SQL-over-API, and table metadata helpers."""

    @staticmethod
    def _escape_odata_quotes(value: str) -> str:
        """Escape single quotes for OData queries (by doubling them)."""
        return value.replace("'", "''")

    @staticmethod
    def _normalize_cache_key(table_schema_name: str) -> str:
        """Normalize table_schema_name to lowercase for case-insensitive cache keys."""
        return table_schema_name.lower() if isinstance(table_schema_name, str) else ""

    @staticmethod
    def _lowercase_keys(record: Dict[str, Any]) -> Dict[str, Any]:
        """Convert all dictionary keys to lowercase for case-insensitive column names.

        Dataverse LogicalNames for attributes are stored lowercase, but users may
        provide PascalCase names (matching SchemaName). This normalizes the input.
        """
        if not isinstance(record, dict):
            return record
        return {k.lower() if isinstance(k, str) else k: v for k, v in record.items()}

    @staticmethod
    def _lowercase_list(items: Optional[List[str]]) -> Optional[List[str]]:
        """Convert all strings in a list to lowercase for case-insensitive column names.

        Used for $select, $orderby, $expand parameters where column names must be lowercase.
        """
        if not items:
            return items
        return [item.lower() if isinstance(item, str) else item for item in items]

    def __init__(
        self,
        auth,
        base_url: str,
        config=None,
    ) -> None:
        """Initialize the OData client.

        Sets up authentication, base URL, configuration, and internal caches.

        :param auth: Authentication manager providing ``acquire_token(scope)`` that returns an object with ``access_token``.
        :type auth: ~PowerPlatform.Dataverse.core.auth.AuthManager
        :param base_url: Organization base URL (e.g. ``"https://<org>.crm.dynamics.com"``).
        :type base_url: ``str``
        :param config: Optional Dataverse configuration (HTTP retry, backoff, timeout, language code). If omitted ``DataverseConfig.from_env()`` is used.
        :type config: ~PowerPlatform.Dataverse.core.config.DataverseConfig | ``None``
        :raises ValueError: If ``base_url`` is empty after stripping.
        """
        self.auth = auth
        self.base_url = (base_url or "").rstrip("/")
        if not self.base_url:
            raise ValueError("base_url is required.")
        self.api = f"{self.base_url}/api/data/v9.2"
        self.config = (
            config
            or __import__(
                "PowerPlatform.Dataverse.core.config", fromlist=["DataverseConfig"]
            ).DataverseConfig.from_env()
        )
        self._http = HttpClient(
            retries=self.config.http_retries,
            backoff=self.config.http_backoff,
            timeout=self.config.http_timeout,
        )
        # Cache: normalized table_schema_name (lowercase) -> entity set name (plural) resolved from metadata
        self._logical_to_entityset_cache: dict[str, str] = {}
        # Cache: normalized table_schema_name (lowercase) -> primary id attribute (e.g. accountid)
        self._logical_primaryid_cache: dict[str, str] = {}
        # Picklist label cache: (normalized_table_schema_name, normalized_attribute) -> {'map': {...}, 'ts': epoch_seconds}
        self._picklist_label_cache = {}
        self._picklist_cache_ttl_seconds = 3600  # 1 hour TTL

    def _headers(self) -> Dict[str, str]:
        """Build standard OData headers with bearer auth."""
        scope = f"{self.base_url}/.default"
        token = self.auth.acquire_token(scope).access_token
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "User-Agent": _USER_AGENT,
        }

    def _merge_headers(self, headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:
        base = self._headers()
        if not headers:
            return base
        merged = base.copy()
        merged.update(headers)
        return merged

    def _raw_request(self, method: str, url: str, **kwargs):
        return self._http.request(method, url, **kwargs)

    def _request(self, method: str, url: str, *, expected: tuple[int, ...] = (200, 201, 202, 204), **kwargs):
        headers_in = kwargs.pop("headers", None)
        kwargs["headers"] = self._merge_headers(headers_in)
        r = self._raw_request(method, url, **kwargs)
        if r.status_code in expected:
            return r
        headers = getattr(r, "headers", {}) or {}
        body_excerpt = (getattr(r, "text", "") or "")[:200]
        svc_code = None
        msg = f"HTTP {r.status_code}"
        try:
            data = r.json() if getattr(r, "text", None) else {}
            if isinstance(data, dict):
                inner = data.get("error")
                if isinstance(inner, dict):
                    svc_code = inner.get("code")
                    imsg = inner.get("message")
                    if isinstance(imsg, str) and imsg.strip():
                        msg = imsg.strip()
                else:
                    imsg2 = data.get("message")
                    if isinstance(imsg2, str) and imsg2.strip():
                        msg = imsg2.strip()
        except Exception:
            pass
        sc = r.status_code
        subcode = http_subcode(sc)
        correlation_id = headers.get("x-ms-correlation-request-id") or headers.get("x-ms-correlation-id")
        request_id = (
            headers.get("x-ms-client-request-id") or headers.get("request-id") or headers.get("x-ms-request-id")
        )
        traceparent = headers.get("traceparent")
        ra = headers.get("Retry-After")
        retry_after = None
        if ra:
            try:
                retry_after = int(ra)
            except Exception:
                retry_after = None
        is_transient = is_transient_status(sc)
        raise HttpError(
            msg,
            status_code=sc,
            subcode=subcode,
            service_error_code=svc_code,
            correlation_id=correlation_id,
            request_id=request_id,
            traceparent=traceparent,
            body_excerpt=body_excerpt,
            retry_after=retry_after,
            is_transient=is_transient,
        )

    # --- CRUD Internal functions ---
    def _create(self, entity_set: str, table_schema_name: str, record: Dict[str, Any]) -> str:
        """Create a single record and return its GUID.

        :param entity_set: Resolved entity set (plural) name.
        :type entity_set: ``str``
        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``
        :param record: Attribute payload mapped by logical column names.
        :type record: ``dict[str, Any]``

        :return: Created record GUID.
        :rtype: ``str``

        .. note::
           Relies on ``OData-EntityId`` (canonical) or ``Location`` response header. No response body parsing is performed. Raises ``RuntimeError`` if neither header contains a GUID.
        """
        # Lowercase all keys to match Dataverse LogicalName expectations
        record = self._lowercase_keys(record)
        record = self._convert_labels_to_ints(table_schema_name, record)
        url = f"{self.api}/{entity_set}"
        r = self._request("post", url, json=record)

        ent_loc = r.headers.get("OData-EntityId") or r.headers.get("OData-EntityID")
        if ent_loc:
            m = _GUID_RE.search(ent_loc)
            if m:
                return m.group(0)
        loc = r.headers.get("Location")
        if loc:
            m = _GUID_RE.search(loc)
            if m:
                return m.group(0)
        header_keys = ", ".join(sorted(r.headers.keys()))
        raise RuntimeError(
            f"Create response missing GUID in OData-EntityId/Location headers (status={getattr(r,'status_code', '?')}). Headers: {header_keys}"
        )

    def _create_multiple(self, entity_set: str, table_schema_name: str, records: List[Dict[str, Any]]) -> List[str]:
        """Create multiple records using the collection-bound ``CreateMultiple`` action.

        :param entity_set: Resolved entity set (plural) name.
        :type entity_set: ``str``
        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``
        :param records: Payload dictionaries mapped by column schema names.
        :type records: ``list[dict[str, Any]]``

        :return: List of created record GUIDs (may be empty if response lacks IDs).
        :rtype: ``list[str]``

        .. note::
           Logical type stamping: if any payload omits ``@odata.type`` the client injects ``Microsoft.Dynamics.CRM.<table_logical_name>``. If all payloads already include ``@odata.type`` no modification occurs.
        """
        if not all(isinstance(r, dict) for r in records):
            raise TypeError("All items for multi-create must be dicts")
        need_logical = any("@odata.type" not in r for r in records)
        # @odata.type uses LogicalName (lowercase)
        logical_name = table_schema_name.lower()
        enriched: List[Dict[str, Any]] = []
        for r in records:
            # Lowercase all keys to match Dataverse LogicalName expectations
            r = self._lowercase_keys(r)
            r = self._convert_labels_to_ints(table_schema_name, r)
            if "@odata.type" in r or not need_logical:
                enriched.append(r)
            else:
                nr = r.copy()
                nr["@odata.type"] = f"Microsoft.Dynamics.CRM.{logical_name}"
                enriched.append(nr)
        payload = {"Targets": enriched}
        # Bound action form: POST {entity_set}/Microsoft.Dynamics.CRM.CreateMultiple
        url = f"{self.api}/{entity_set}/Microsoft.Dynamics.CRM.CreateMultiple"
        # The action currently returns only Ids; no need to request representation.
        r = self._request("post", url, json=payload)
        try:
            body = r.json() if r.text else {}
        except ValueError:
            body = {}
        if not isinstance(body, dict):
            return []
        # Expected: { "Ids": [guid, ...] }
        ids = body.get("Ids")
        if isinstance(ids, list):
            return [i for i in ids if isinstance(i, str)]

        value = body.get("value")
        if isinstance(value, list):
            # Extract IDs if possible
            out: List[str] = []
            for item in value:
                if isinstance(item, dict):
                    # Heuristic: look for a property ending with 'id'
                    for k, v in item.items():
                        if isinstance(k, str) and k.lower().endswith("id") and isinstance(v, str) and len(v) >= 32:
                            out.append(v)
                            break
            return out
        return []

    # --- Derived helpers for high-level client ergonomics ---
    def _primary_id_attr(self, table_schema_name: str) -> str:
        """Return primary key attribute using metadata; error if unavailable."""
        cache_key = self._normalize_cache_key(table_schema_name)
        pid = self._logical_primaryid_cache.get(cache_key)
        if pid:
            return pid
        # Resolve metadata (populates _logical_primaryid_cache or raises if table_schema_name unknown)
        self._entity_set_from_schema_name(table_schema_name)
        pid2 = self._logical_primaryid_cache.get(cache_key)
        if pid2:
            return pid2
        raise RuntimeError(
            f"PrimaryIdAttribute not resolved for table_schema_name '{table_schema_name}'. Metadata did not include PrimaryIdAttribute."
        )

    def _update_by_ids(
        self, table_schema_name: str, ids: List[str], changes: Union[Dict[str, Any], List[Dict[str, Any]]]
    ) -> None:
        """Update many records by GUID list using the collection-bound ``UpdateMultiple`` action.

        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``
        :param ids: GUIDs of target records.
        :type ids: ``list[str]``
        :param changes: Broadcast patch (``dict``) applied to all IDs, or list of per-record patches (1:1 with ``ids``).
        :type changes: ``dict`` | ``list[dict]``

        :return: ``None``
        :rtype: ``None``
        """
        if not isinstance(ids, list):
            raise TypeError("ids must be list[str]")
        if not ids:
            return None
        pk_attr = self._primary_id_attr(table_schema_name)
        entity_set = self._entity_set_from_schema_name(table_schema_name)
        if isinstance(changes, dict):
            batch = [{pk_attr: rid, **changes} for rid in ids]
            self._update_multiple(entity_set, table_schema_name, batch)
            return None
        if not isinstance(changes, list):
            raise TypeError("changes must be dict or list[dict]")
        if len(changes) != len(ids):
            raise ValueError("Length of changes list must match length of ids list")
        batch: List[Dict[str, Any]] = []
        for rid, patch in zip(ids, changes):
            if not isinstance(patch, dict):
                raise TypeError("Each patch must be a dict")
            batch.append({pk_attr: rid, **patch})
        self._update_multiple(entity_set, table_schema_name, batch)
        return None

    def _delete_multiple(
        self,
        table_schema_name: str,
        ids: List[str],
    ) -> Optional[str]:
        """Delete many records by GUID list via the ``BulkDelete`` action.

        :param logical_name: Logical (singular) entity name.
        :type logical_name: ``str``
        :param ids: GUIDs of records to delete.
        :type ids: ``list[str]``

        :return: BulkDelete asynchronous job identifier when executed in bulk; ``None`` if no IDs provided or single deletes performed.
        :rtype: ``str`` | ``None``
        """
        targets = [rid for rid in ids if rid]
        if not targets:
            return None
        value_objects = [{"Value": rid, "Type": "System.Guid"} for rid in targets]

        pk_attr = self._primary_id_attr(table_schema_name)
        timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        job_label = f"Bulk delete {table_schema_name} records @ {timestamp}"

        # EntityName must use lowercase LogicalName
        logical_name = table_schema_name.lower()

        query = {
            "@odata.type": "Microsoft.Dynamics.CRM.QueryExpression",
            "EntityName": logical_name,
            "ColumnSet": {
                "@odata.type": "Microsoft.Dynamics.CRM.ColumnSet",
                "AllColumns": False,
                "Columns": [],
            },
            "Criteria": {
                "@odata.type": "Microsoft.Dynamics.CRM.FilterExpression",
                "FilterOperator": "And",
                "Conditions": [
                    {
                        "@odata.type": "Microsoft.Dynamics.CRM.ConditionExpression",
                        "AttributeName": pk_attr,
                        "Operator": "In",
                        "Values": value_objects,
                    }
                ],
            },
        }

        payload = {
            "JobName": job_label,
            "SendEmailNotification": False,
            "ToRecipients": [],
            "CCRecipients": [],
            "RecurrencePattern": "",
            "StartDateTime": timestamp,
            "QuerySet": [query],
        }

        url = f"{self.api}/BulkDelete"
        response = self._request("post", url, json=payload, expected=(200, 202, 204))

        job_id = None
        try:
            body = response.json() if response.text else {}
        except ValueError:
            body = {}
        if isinstance(body, dict):
            job_id = body.get("JobId")

        return job_id

    def _format_key(self, key: str) -> str:
        k = key.strip()
        if k.startswith("(") and k.endswith(")"):
            return k
        # Escape single quotes in alternate key values
        if "=" in k and "'" in k:

            def esc(match):
                # match.group(1) is the key, match.group(2) is the value
                return f"{match.group(1)}='{self._escape_odata_quotes(match.group(2))}'"

            k = re.sub(r"(\w+)=\'([^\']*)\'", esc, k)
            return f"({k})"
        if len(k) == 36 and "-" in k:
            return f"({k})"
        return f"({k})"

    def _update(self, table_schema_name: str, key: str, data: Dict[str, Any]) -> None:
        """Update an existing record by GUID.

        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``
        :param key: Record GUID (with or without parentheses).
        :type key: ``str``
        :param data: Partial entity payload (attributes to patch).
        :type data: ``dict[str, Any]``
        :return: ``None``
        :rtype: ``None``
        """
        # Lowercase all keys to match Dataverse LogicalName expectations
        data = self._lowercase_keys(data)
        data = self._convert_labels_to_ints(table_schema_name, data)
        entity_set = self._entity_set_from_schema_name(table_schema_name)
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        r = self._request("patch", url, headers={"If-Match": "*"}, json=data)

    def _update_multiple(self, entity_set: str, table_schema_name: str, records: List[Dict[str, Any]]) -> None:
        """Bulk update existing records via the collection-bound ``UpdateMultiple`` action.

        :param entity_set: Resolved entity set (plural) name.
        :type entity_set: ``str``
        :param table_schema_name: Schema name of the table, e.g. "new_MyTestTable".
        :type table_schema_name: ``str``
        :param records: List of patch dictionaries. Each must include the true primary key attribute (e.g. ``accountid``) and one or more fields to update.
        :type records: ``list[dict[str, Any]]``
        :return: ``None``
        :rtype: ``None``

        .. note::
           - Endpoint: ``POST /{entity_set}/Microsoft.Dynamics.CRM.UpdateMultiple`` with body ``{"Targets": [...]}``.
           - Transactional semantics: if any individual update fails, the entire request rolls back.
           - Response content is ignored; no stable contract for returned IDs/representations.
           - Caller must supply the correct primary key attribute (e.g. ``accountid``) in every record.
        """
        if not isinstance(records, list) or not records or not all(isinstance(r, dict) for r in records):
            raise TypeError("records must be a non-empty list[dict]")

        # Determine whether we need logical name resolution (@odata.type missing in any payload)
        need_logical = any("@odata.type" not in r for r in records)
        # @odata.type uses LogicalName (lowercase)
        logical_name = table_schema_name.lower()
        enriched: List[Dict[str, Any]] = []
        for r in records:
            # Lowercase all keys to match Dataverse LogicalName expectations
            r = self._lowercase_keys(r)
            r = self._convert_labels_to_ints(table_schema_name, r)
            if "@odata.type" in r or not need_logical:
                enriched.append(r)
            else:
                nr = r.copy()
                nr["@odata.type"] = f"Microsoft.Dynamics.CRM.{logical_name}"
                enriched.append(nr)

        payload = {"Targets": enriched}
        url = f"{self.api}/{entity_set}/Microsoft.Dynamics.CRM.UpdateMultiple"
        r = self._request("post", url, json=payload)
        # Intentionally ignore response content: no stable contract for IDs across environments.
        return None

    def _delete(self, table_schema_name: str, key: str) -> None:
        """Delete a record by GUID.

        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``
        :param key: Record GUID (with or without parentheses)
        :type key: ``str``

        :return: ``None``
        :rtype: ``None``
        """
        entity_set = self._entity_set_from_schema_name(table_schema_name)
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        self._request("delete", url, headers={"If-Match": "*"})

    def _get(self, table_schema_name: str, key: str, select: Optional[List[str]] = None) -> Dict[str, Any]:
        """Retrieve a single record.

        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``
        :param key: Record GUID (with or without parentheses).
        :type key: ``str``
        :param select: Columns to select; joined with commas into $select.
        :type select: ``list[str]`` | ``None``

        :return: Retrieved record dictionary (may be empty if no selected attributes).
        :rtype: ``dict[str, Any]``
        """
        params = {}
        if select:
            # Lowercase column names for case-insensitive matching
            params["$select"] = ",".join(select)
        entity_set = self._entity_set_from_schema_name(table_schema_name)
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        r = self._request("get", url, params=params)
        return r.json()

    def _get_multiple(
        self,
        table_schema_name: str,
        select: Optional[List[str]] = None,
        filter: Optional[str] = None,
        orderby: Optional[List[str]] = None,
        top: Optional[int] = None,
        expand: Optional[List[str]] = None,
        page_size: Optional[int] = None,
    ) -> Iterable[List[Dict[str, Any]]]:
        """Iterate records from an entity set, yielding one page (list of dicts) at a time.

        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``
        :param select: Columns to include (``$select``) or ``None``. Column names are automatically lowercased.
        :type select: ``list[str]`` | ``None``
        :param filter: OData ``$filter`` expression or ``None``. This is passed as-is without transformation. Users must provide lowercase logical column names (e.g., "statecode eq 0").
        :type filter: ``str`` | ``None``
        :param orderby: Order expressions (``$orderby``) or ``None``. Column names are automatically lowercased.
        :type orderby: ``list[str]`` | ``None``
        :param top: Max total records (applied on first request as ``$top``) or ``None``.
        :type top: ``int`` | ``None``
        :param expand: Navigation properties to expand (``$expand``) or ``None``. These are case-sensitive and passed as-is. Users must provide exact navigation property names from entity metadata.
        :type expand: ``list[str]`` | ``None``
        :param page_size: Per-page size hint via ``Prefer: odata.maxpagesize``.
        :type page_size: ``int`` | ``None``

        :return: Iterator yielding pages (each page is a ``list`` of record dicts).
        :rtype: ``Iterable[list[dict[str, Any]]]``
        """

        extra_headers: Dict[str, str] = {}
        if page_size is not None:
            ps = int(page_size)
            if ps > 0:
                extra_headers["Prefer"] = f"odata.maxpagesize={ps}"

        def _do_request(url: str, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
            headers = extra_headers if extra_headers else None
            r = self._request("get", url, headers=headers, params=params)
            try:
                return r.json()
            except ValueError:
                return {}

        entity_set = self._entity_set_from_schema_name(table_schema_name)
        base_url = f"{self.api}/{entity_set}"
        params: Dict[str, Any] = {}
        if select:
            # Lowercase column names for case-insensitive matching
            params["$select"] = ",".join(self._lowercase_list(select))
        if filter:
            # Filter is passed as-is; users must use lowercase column names in filter expressions
            params["$filter"] = filter
        if orderby:
            # Lowercase column names for case-insensitive matching
            params["$orderby"] = ",".join(self._lowercase_list(orderby))
        if expand:
            # Lowercase navigation property names for case-insensitive matching
            params["$expand"] = ",".join(expand)
        if top is not None:
            params["$top"] = int(top)

        data = _do_request(base_url, params=params)
        items = data.get("value") if isinstance(data, dict) else None
        if isinstance(items, list) and items:
            yield [x for x in items if isinstance(x, dict)]

        next_link = None
        if isinstance(data, dict):
            next_link = data.get("@odata.nextLink") or data.get("odata.nextLink")

        while next_link:
            data = _do_request(next_link)
            items = data.get("value") if isinstance(data, dict) else None
            if isinstance(items, list) and items:
                yield [x for x in items if isinstance(x, dict)]
            next_link = data.get("@odata.nextLink") or data.get("odata.nextLink") if isinstance(data, dict) else None

    # --------------------------- SQL Custom API -------------------------
    def _query_sql(self, sql: str) -> list[dict[str, Any]]:
        """Execute a read-only SQL SELECT using the Dataverse Web API ``?sql=`` capability.

        :param sql: Single SELECT statement within the supported subset.
        :type sql: ``str``

        :return: Result rows (empty list if none).
        :rtype: ``list[dict[str, Any]]``

        :raises ValidationError: If ``sql`` is not a ``str`` or is empty.
        :raises MetadataError: If logical table name resolution fails.

        .. note::
           Endpoint form: ``GET /{entity_set}?sql=<encoded select>``. The client extracts the logical table name, resolves the entity set (metadata cached), then issues the request. Only a constrained SELECT subset is supported by the platform.
        """
        if not isinstance(sql, str):
            raise ValidationError("sql must be a string", subcode=VALIDATION_SQL_NOT_STRING)
        if not sql.strip():
            raise ValidationError("sql must be a non-empty string", subcode=VALIDATION_SQL_EMPTY)
        sql = sql.strip()

        # Extract logical table name via helper (robust to identifiers ending with 'from')
        logical = self._extract_logical_table(sql)

        entity_set = self._entity_set_from_schema_name(logical)
        # Issue GET /{entity_set}?sql=<query>
        url = f"{self.api}/{entity_set}"
        params = {"sql": sql}
        r = self._request("get", url, params=params)
        try:
            body = r.json()
        except ValueError:
            return []
        if isinstance(body, dict):
            value = body.get("value")
            if isinstance(value, list):
                # Ensure dict rows only
                return [row for row in value if isinstance(row, dict)]
        # Fallbacks: if body itself is a list
        if isinstance(body, list):
            return [row for row in body if isinstance(row, dict)]
        return []

    @staticmethod
    def _extract_logical_table(sql: str) -> str:
        """Extract the logical table name after the first standalone FROM.

        Examples:
            SELECT * FROM account
            SELECT col1, startfrom FROM new_sampleitem WHERE col1 = 1

        """
        if not isinstance(sql, str):
            raise ValueError("sql must be a string")
        # Mask out single-quoted string literals to avoid matching FROM inside them.
        masked = re.sub(r"'([^']|'')*'", "'x'", sql)
        pattern = r"\bfrom\b\s+([A-Za-z0-9_]+)"  # minimal, single-line regex
        m = re.search(pattern, masked, flags=re.IGNORECASE)
        if not m:
            raise ValueError("Unable to determine table logical name from SQL (expected 'FROM <name>').")
        return m.group(1).lower()

    # ---------------------- Entity set resolution -----------------------
    def _entity_set_from_schema_name(self, table_schema_name: str) -> str:
        """Resolve entity set name (plural) from a schema name (singular) name using metadata.

        Caches results for subsequent queries. Case-insensitive.
        """
        if not table_schema_name:
            raise ValueError("table schema name required")

        # Use normalized (lowercase) key for cache lookup
        cache_key = self._normalize_cache_key(table_schema_name)
        cached = self._logical_to_entityset_cache.get(cache_key)
        if cached:
            return cached
        url = f"{self.api}/EntityDefinitions"
        # LogicalName in Dataverse is stored in lowercase, so we need to lowercase for the filter
        logical_lower = table_schema_name.lower()
        logical_escaped = self._escape_odata_quotes(logical_lower)
        params = {
            "$select": "LogicalName,EntitySetName,PrimaryIdAttribute",
            "$filter": f"LogicalName eq '{logical_escaped}'",
        }
        r = self._request("get", url, params=params)
        try:
            body = r.json()
            items = body.get("value", []) if isinstance(body, dict) else []
        except ValueError:
            items = []
        if not items:
            plural_hint = (
                " (did you pass a plural entity set name instead of the singular table schema name?)"
                if table_schema_name.endswith("s") and not table_schema_name.endswith("ss")
                else ""
            )
            raise MetadataError(
                f"Unable to resolve entity set for table schema name '{table_schema_name}'. Provide the singular table schema name.{plural_hint}",
                subcode=METADATA_ENTITYSET_NOT_FOUND,
            )
        md = items[0]
        es = md.get("EntitySetName")
        if not es:
            raise MetadataError(
                f"Metadata response missing EntitySetName for table schema name '{table_schema_name}'.",
                subcode=METADATA_ENTITYSET_NAME_MISSING,
            )
        self._logical_to_entityset_cache[cache_key] = es
        primary_id_attr = md.get("PrimaryIdAttribute")
        if isinstance(primary_id_attr, str) and primary_id_attr:
            self._logical_primaryid_cache[cache_key] = primary_id_attr
        return es

    # ---------------------- Table metadata helpers ----------------------
    def _label(self, text: str) -> Dict[str, Any]:
        lang = int(self.config.language_code)
        return {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [
                {
                    "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                    "Label": text,
                    "LanguageCode": lang,
                }
            ],
        }

    def _to_pascal(self, name: str) -> str:
        parts = re.split(r"[^A-Za-z0-9]+", name)
        return "".join(p[:1].upper() + p[1:] for p in parts if p)

    def _get_entity_by_table_schema_name(
        self,
        table_schema_name: str,
        headers: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Get entity metadata by table schema name. Case-insensitive.

        Note: LogicalName is stored lowercase in Dataverse, so we lowercase the input
        for case-insensitive matching. The response includes SchemaName, LogicalName,
        EntitySetName, and MetadataId.
        """
        url = f"{self.api}/EntityDefinitions"
        # LogicalName is stored lowercase, so we lowercase the input for lookup
        logical_lower = table_schema_name.lower()
        logical_escaped = self._escape_odata_quotes(logical_lower)
        params = {
            "$select": "MetadataId,LogicalName,SchemaName,EntitySetName",
            "$filter": f"LogicalName eq '{logical_escaped}'",
        }
        r = self._request("get", url, params=params, headers=headers)
        items = r.json().get("value", [])
        return items[0] if items else None

    def _create_entity(
        self,
        table_schema_name: str,
        display_name: str,
        attributes: List[Dict[str, Any]],
        solution_unique_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        url = f"{self.api}/EntityDefinitions"
        payload = {
            "@odata.type": "Microsoft.Dynamics.CRM.EntityMetadata",
            "SchemaName": table_schema_name,
            "DisplayName": self._label(display_name),
            "DisplayCollectionName": self._label(display_name + "s"),
            "Description": self._label(f"Custom entity for {display_name}"),
            "OwnershipType": "UserOwned",
            "HasActivities": False,
            "HasNotes": True,
            "IsActivity": False,
            "Attributes": attributes,
        }
        params = None
        if solution_unique_name:
            params = {"SolutionUniqueName": solution_unique_name}
        self._request("post", url, json=payload, params=params)
        ent = self._get_entity_by_table_schema_name(
            table_schema_name,
            headers={"Consistency": "Strong"},
        )
        if not ent or not ent.get("EntitySetName"):
            raise RuntimeError(
                f"Failed to create or retrieve entity '{table_schema_name}' (EntitySetName not available)."
            )
        if not ent.get("MetadataId"):
            raise RuntimeError(f"MetadataId missing after creating entity '{table_schema_name}'.")
        return ent

    def _get_attribute_metadata(
        self,
        entity_metadata_id: str,
        column_schema_name: str,
        extra_select: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        attr_escaped = self._escape_odata_quotes(column_schema_name)
        url = f"{self.api}/EntityDefinitions({entity_metadata_id})/Attributes"
        select_fields = ["MetadataId", "LogicalName", "SchemaName"]
        if extra_select:
            for piece in extra_select.split(","):
                piece = piece.strip()
                if not piece or piece in select_fields:
                    continue
                if piece.startswith("@"):
                    continue
                if piece not in select_fields:
                    select_fields.append(piece)
        params = {
            "$select": ",".join(select_fields),
            "$filter": f"SchemaName eq '{attr_escaped}'",
        }
        r = self._request("get", url, params=params)
        try:
            body = r.json() if r.text else {}
        except ValueError:
            return None
        items = body.get("value") if isinstance(body, dict) else None
        if isinstance(items, list) and items:
            item = items[0]
            if isinstance(item, dict):
                return item
        return None

    # ---------------------- Enum / Option Set helpers ------------------
    def _build_localizedlabels_payload(self, translations: Dict[int, str]) -> Dict[str, Any]:
        """Build a Dataverse Label object from {<language_code>: <text>} entries.

        Ensures at least one localized label. Does not deduplicate language codes; last wins.
        """
        locs: List[Dict[str, Any]] = []
        for lang, text in translations.items():
            if not isinstance(lang, int):
                raise ValueError(f"Language code '{lang}' must be int")
            if not isinstance(text, str) or not text.strip():
                raise ValueError(f"Label for lang {lang} must be non-empty string")
            locs.append(
                {
                    "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                    "Label": text,
                    "LanguageCode": lang,
                }
            )
        if not locs:
            raise ValueError("At least one translation required")
        return {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": locs,
        }

    def _enum_optionset_payload(
        self, column_schema_name: str, enum_cls: type[Enum], is_primary_name: bool = False
    ) -> Dict[str, Any]:
        """Create local (IsGlobal=False) PicklistAttributeMetadata from an Enum subclass.

        Supports translation mapping via optional class attribute `__labels__`:
            __labels__ = { 1033: { "Active": "Active", "Inactive": "Inactive" },
                           1036: { "Active": "Actif",  "Inactive": "Inactif" } }

        Keys inside per-language dict may be either enum member objects or their names.
        If a language lacks a label for a member, member.name is used as fallback.
        The client's configured language code is always ensured to exist.
        """
        all_member_items = list(enum_cls.__members__.items())
        if not all_member_items:
            raise ValueError(f"Enum {enum_cls.__name__} has no members")

        # Duplicate detection
        value_to_first_name: Dict[int, str] = {}
        for name, member in all_member_items:
            val = getattr(member, "value", None)
            # Defer non-int validation to later loop for consistency
            if val in value_to_first_name and value_to_first_name[val] != name:
                raise ValueError(
                    f"Duplicate enum value {val} in {enum_cls.__name__} (names: {value_to_first_name[val]}, {name})"
                )
            value_to_first_name[val] = name

        members = list(enum_cls)
        # Validate integer values
        for m in members:
            if not isinstance(m.value, int):
                raise ValueError(f"Enum member '{m.name}' has non-int value '{m.value}' (only int values supported)")

        raw_labels = getattr(enum_cls, "__labels__", None)
        labels_by_lang: Dict[int, Dict[str, str]] = {}
        if raw_labels is not None:
            if not isinstance(raw_labels, dict):
                raise ValueError("__labels__ must be a dict {lang:int -> {member: label}}")
            # Build a helper map for value -> member name to resolve raw int keys
            value_to_name = {m.value: m.name for m in members}
            for lang, mapping in raw_labels.items():
                if not isinstance(lang, int):
                    raise ValueError("Language codes in __labels__ must be ints")
                if not isinstance(mapping, dict):
                    raise ValueError(f"__labels__[{lang}] must be a dict of member names to strings")
                labels_by_lang.setdefault(lang, {})
                for k, v in mapping.items():
                    # Accept enum member object, its name, or raw int value (from class body reference)
                    if isinstance(k, enum_cls):
                        member_name = k.name
                    elif isinstance(k, int):
                        member_name = value_to_name.get(k)
                        if member_name is None:
                            raise ValueError(f"__labels__[{lang}] has int key {k} not matching any enum value")
                    else:
                        member_name = str(k)
                    if not isinstance(v, str) or not v.strip():
                        raise ValueError(f"Label for {member_name} lang {lang} must be non-empty string")
                    labels_by_lang[lang][member_name] = v

        config_lang = int(self.config.language_code)
        # Ensure config language appears (fallback to names)
        all_langs = set(labels_by_lang.keys()) | {config_lang}

        options: List[Dict[str, Any]] = []
        for m in sorted(members, key=lambda x: x.value):
            per_lang: Dict[int, str] = {}
            for lang in all_langs:
                label_text = labels_by_lang.get(lang, {}).get(m.name, m.name)
                per_lang[lang] = label_text
            options.append(
                {
                    "@odata.type": "Microsoft.Dynamics.CRM.OptionMetadata",
                    "Value": m.value,
                    "Label": self._build_localizedlabels_payload(per_lang),
                }
            )

        attr_label = column_schema_name.split("_")[-1]
        return {
            "@odata.type": "Microsoft.Dynamics.CRM.PicklistAttributeMetadata",
            "SchemaName": column_schema_name,
            "DisplayName": self._label(attr_label),
            "RequiredLevel": {"Value": "None"},
            "IsPrimaryName": bool(is_primary_name),
            "OptionSet": {
                "@odata.type": "Microsoft.Dynamics.CRM.OptionSetMetadata",
                "IsGlobal": False,
                "Options": options,
            },
        }

    def _normalize_picklist_label(self, label: str) -> str:
        """Normalize a label for case / diacritic insensitive comparison."""
        if not isinstance(label, str):
            return ""
        # Strip accents
        norm = unicodedata.normalize("NFD", label)
        norm = "".join(c for c in norm if unicodedata.category(c) != "Mn")
        # Collapse whitespace, lowercase
        norm = re.sub(r"\s+", " ", norm).strip().lower()
        return norm

    def _optionset_map(self, table_schema_name: str, attr_logical: str) -> Optional[Dict[str, int]]:
        """Build or return cached mapping of normalized label -> value for a picklist attribute.

        Returns empty dict if attribute is not a picklist or has no options. Returns None only
        for invalid inputs or unexpected metadata parse failures.

        Notes
        -----
        - This method calls the Web API twice per attribute so it could have perf impact when there are lots of columns on the entity.
        """
        if not table_schema_name or not attr_logical:
            return None
        # Normalize cache key for case-insensitive lookups
        cache_key = (self._normalize_cache_key(table_schema_name), self._normalize_cache_key(attr_logical))
        now = time.time()
        entry = self._picklist_label_cache.get(cache_key)
        if isinstance(entry, dict) and "map" in entry and (now - entry.get("ts", 0)) < self._picklist_cache_ttl_seconds:
            return entry["map"]

        # LogicalNames in Dataverse are stored in lowercase, so we need to lowercase for filters
        attr_esc = self._escape_odata_quotes(attr_logical.lower())
        table_schema_name_esc = self._escape_odata_quotes(table_schema_name.lower())

        # Step 1: lightweight fetch (no expand) to determine attribute type
        url_type = (
            f"{self.api}/EntityDefinitions(LogicalName='{table_schema_name_esc}')/Attributes"
            f"?$filter=LogicalName eq '{attr_esc}'&$select=LogicalName,AttributeType"
        )
        # Retry up to 3 times on 404 (new or not-yet-published attribute metadata). If still 404, raise.
        r_type = None
        for attempt in range(3):
            try:
                r_type = self._request("get", url_type)
                break
            except HttpError as err:
                if getattr(err, "status_code", None) == 404:
                    if attempt < 2:
                        # Exponential-ish backoff: 0.4s, 0.8s
                        time.sleep(0.4 * (2**attempt))
                        continue
                    raise RuntimeError(
                        f"Picklist attribute metadata not found after retries: entity='{table_schema_name}' attribute='{attr_logical}' (404)"
                    ) from err
                raise
        if r_type is None:
            raise RuntimeError("Failed to retrieve attribute metadata due to repeated request failures.")

        body_type = r_type.json()
        items = body_type.get("value", []) if isinstance(body_type, dict) else []
        if not items:
            return None
        attr_md = items[0]
        if attr_md.get("AttributeType") not in ("Picklist", "PickList"):
            self._picklist_label_cache[cache_key] = {"map": {}, "ts": now}
            return {}

        # Step 2: fetch with expand only now that we know it's a picklist
        # Need to cast to the derived PicklistAttributeMetadata type; OptionSet is not a nav on base AttributeMetadata.
        cast_url = (
            f"{self.api}/EntityDefinitions(LogicalName='{table_schema_name_esc}')/Attributes(LogicalName='{attr_esc}')/"
            "Microsoft.Dynamics.CRM.PicklistAttributeMetadata?$select=LogicalName&$expand=OptionSet($select=Options)"
        )
        # Step 2 fetch with retries: expanded OptionSet (cast form first)
        r_opts = None
        for attempt in range(3):
            try:
                r_opts = self._request("get", cast_url)
                break
            except HttpError as err:
                if getattr(err, "status_code", None) == 404:
                    if attempt < 2:
                        time.sleep(0.4 * (2**attempt))  # 0.4s, 0.8s
                        continue
                    raise RuntimeError(
                        f"Picklist OptionSet metadata not found after retries: entity='{table_schema_name}' attribute='{attr_logical}' (404)"
                    ) from err
                raise
        if r_opts is None:
            raise RuntimeError("Failed to retrieve picklist OptionSet metadata due to repeated request failures.")

        attr_full = {}
        try:
            attr_full = r_opts.json() if r_opts.text else {}
        except ValueError:
            return None
        option_set = attr_full.get("OptionSet") or {}
        options = option_set.get("Options") if isinstance(option_set, dict) else None
        if not isinstance(options, list):
            return None
        mapping: Dict[str, int] = {}
        for opt in options:
            if not isinstance(opt, dict):
                continue
            val = opt.get("Value")
            if not isinstance(val, int):
                continue
            label_def = opt.get("Label") or {}
            locs = label_def.get("LocalizedLabels")
            if isinstance(locs, list):
                for loc in locs:
                    if isinstance(loc, dict):
                        lab = loc.get("Label")
                        if isinstance(lab, str) and lab.strip():
                            normalized = self._normalize_picklist_label(lab)
                            mapping.setdefault(normalized, val)
        if mapping:
            self._picklist_label_cache[cache_key] = {"map": mapping, "ts": now}
            return mapping
        # No options available
        self._picklist_label_cache[cache_key] = {"map": {}, "ts": now}
        return {}

    def _convert_labels_to_ints(self, table_schema_name: str, record: Dict[str, Any]) -> Dict[str, Any]:
        """Return a copy of record with any labels converted to option ints.

        Heuristic: For each string value, attempt to resolve against picklist metadata.
        If attribute isn't a picklist or label not found, value left unchanged.
        """
        out = record.copy()
        for k, v in list(out.items()):
            if not isinstance(v, str) or not v.strip():
                continue
            mapping = self._optionset_map(table_schema_name, k)
            if not mapping:
                continue
            norm = self._normalize_picklist_label(v)
            val = mapping.get(norm)
            if val is not None:
                out[k] = val
        return out

    def _attribute_payload(
        self, column_schema_name: str, dtype: Any, *, is_primary_name: bool = False
    ) -> Optional[Dict[str, Any]]:
        # Enum-based local option set support
        if isinstance(dtype, type) and issubclass(dtype, Enum):
            return self._enum_optionset_payload(column_schema_name, dtype, is_primary_name=is_primary_name)
        if not isinstance(dtype, str):
            raise ValueError(
                f"Unsupported column spec type for '{column_schema_name}': {type(dtype)} (expected str or Enum subclass)"
            )
        dtype_l = dtype.lower().strip()
        label = column_schema_name.split("_")[-1]
        if dtype_l in ("string", "text"):
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
                "SchemaName": column_schema_name,
                "DisplayName": self._label(label),
                "RequiredLevel": {"Value": "None"},
                "MaxLength": 200,
                "FormatName": {"Value": "Text"},
                "IsPrimaryName": bool(is_primary_name),
            }
        if dtype_l in ("int", "integer"):
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.IntegerAttributeMetadata",
                "SchemaName": column_schema_name,
                "DisplayName": self._label(label),
                "RequiredLevel": {"Value": "None"},
                "Format": "None",
                "MinValue": -2147483648,
                "MaxValue": 2147483647,
            }
        if dtype_l in ("decimal", "money"):
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.DecimalAttributeMetadata",
                "SchemaName": column_schema_name,
                "DisplayName": self._label(label),
                "RequiredLevel": {"Value": "None"},
                "MinValue": -100000000000.0,
                "MaxValue": 100000000000.0,
                "Precision": 2,
            }
        if dtype_l in ("float", "double"):
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.DoubleAttributeMetadata",
                "SchemaName": column_schema_name,
                "DisplayName": self._label(label),
                "RequiredLevel": {"Value": "None"},
                "MinValue": -100000000000.0,
                "MaxValue": 100000000000.0,
                "Precision": 2,
            }
        if dtype_l in ("datetime", "date"):
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata",
                "SchemaName": column_schema_name,
                "DisplayName": self._label(label),
                "RequiredLevel": {"Value": "None"},
                "Format": "DateOnly",
                "ImeMode": "Inactive",
            }
        if dtype_l in ("bool", "boolean"):
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.BooleanAttributeMetadata",
                "SchemaName": column_schema_name,
                "DisplayName": self._label(label),
                "RequiredLevel": {"Value": "None"},
                "OptionSet": {
                    "@odata.type": "Microsoft.Dynamics.CRM.BooleanOptionSetMetadata",
                    "TrueOption": {
                        "Value": 1,
                        "Label": self._label("True"),
                    },
                    "FalseOption": {
                        "Value": 0,
                        "Label": self._label("False"),
                    },
                    "IsGlobal": False,
                },
            }
        return None

    def _get_table_info(self, table_schema_name: str) -> Optional[Dict[str, Any]]:
        """Return basic metadata for a custom table if it exists.

        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``

        :return: Metadata summary or ``None`` if not found.
        :rtype: ``dict[str, Any]`` | ``None``
        """
        ent = self._get_entity_by_table_schema_name(table_schema_name)
        if not ent:
            return None
        return {
            "table_schema_name": ent.get("SchemaName") or table_schema_name,
            "table_logical_name": ent.get("LogicalName"),
            "entity_set_name": ent.get("EntitySetName"),
            "metadata_id": ent.get("MetadataId"),
            "columns_created": [],
        }

    def _list_tables(self) -> List[Dict[str, Any]]:
        """List all non-private tables (``IsPrivate eq false``).

        :return: Metadata entries for non-private tables (may be empty).
        :rtype: ``list[dict[str, Any]]``

        :raises HttpError: If the metadata request fails.
        """
        url = f"{self.api}/EntityDefinitions"
        params = {"$filter": "IsPrivate eq false"}
        r = self._request("get", url, params=params)
        return r.json().get("value", [])

    def _delete_table(self, table_schema_name: str) -> None:
        """Delete a table by schema name.

        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``

        :return: ``None``
        :rtype: ``None``

        :raises MetadataError: If the table does not exist.
        :raises HttpError: If the delete request fails.
        """
        ent = self._get_entity_by_table_schema_name(table_schema_name)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{table_schema_name}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )
        metadata_id = ent["MetadataId"]
        url = f"{self.api}/EntityDefinitions({metadata_id})"
        r = self._request("delete", url)

    def _create_table(
        self,
        table_schema_name: str,
        schema: Dict[str, Any],
        solution_unique_name: Optional[str] = None,
        primary_column_schema_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a custom table with specified columns.

        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``
        :param schema: Mapping of column name -> type spec (``str`` or ``Enum`` subclass).
        :type schema: ``dict[str, Any]``
        :param solution_unique_name: Optional solution container for the new table; if provided must be non-empty.
        :type solution_unique_name: ``str`` | ``None``
        :param primary_column_schema_name: Optional primary column schema name.
        :type primary_column_schema_name: ``str`` | ``None``

        :return: Metadata summary for the created table including created column schema names.
        :rtype: ``dict[str, Any]``

        :raises MetadataError: If the table already exists.
        :raises ValueError: If a column type is unsupported or ``solution_unique_name`` is empty.
        :raises TypeError: If ``solution_unique_name`` is not a ``str`` when provided.
        :raises HttpError: If underlying HTTP requests fail.
        """
        # Check if table already exists (case-insensitive)
        ent = self._get_entity_by_table_schema_name(table_schema_name)
        if ent:
            raise MetadataError(
                f"Table '{table_schema_name}' already exists.",
                subcode=METADATA_TABLE_ALREADY_EXISTS,
            )

        created_cols: List[str] = []

        # Use provided primary column name, or derive from table_schema_name prefix (e.g., "new_Product" -> "new_Name").
        # If no prefix detected, default to "new_Name"; server will validate overall table schema.
        if primary_column_schema_name:
            primary_attr_schema = primary_column_schema_name
        else:
            primary_attr_schema = (
                f"{table_schema_name.split('_',1)[0]}_Name" if "_" in table_schema_name else "new_Name"
            )

        attributes: List[Dict[str, Any]] = []
        attributes.append(self._attribute_payload(primary_attr_schema, "string", is_primary_name=True))
        for col_name, dtype in schema.items():
            payload = self._attribute_payload(col_name, dtype)
            if not payload:
                raise ValueError(f"Unsupported column type '{dtype}' for '{col_name}'.")
            attributes.append(payload)
            created_cols.append(col_name)

        if solution_unique_name is not None:
            if not isinstance(solution_unique_name, str):
                raise TypeError("solution_unique_name must be a string when provided")
            if not solution_unique_name:
                raise ValueError("solution_unique_name cannot be empty")

        metadata = self._create_entity(
            table_schema_name=table_schema_name,
            display_name=table_schema_name,
            attributes=attributes,
            solution_unique_name=solution_unique_name,
        )

        return {
            "table_schema_name": table_schema_name,
            "table_logical_name": metadata.get("LogicalName"),
            "entity_set_name": metadata.get("EntitySetName"),
            "metadata_id": metadata.get("MetadataId"),
            "columns_created": created_cols,
        }

    def _create_columns(
        self,
        table_schema_name: str,
        columns: Dict[str, Any],
    ) -> List[str]:
        """Create new columns on an existing table.

        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``
        :param columns: Mapping of column schema name -> type spec (``str`` or ``Enum`` subclass).
        :type columns: ``dict[str, Any]``

        :return: List of created column schema names.
        :rtype: ``list[str]``

        :raises TypeError: If ``columns`` is not a non-empty dict.
        :raises MetadataError: If the target table does not exist.
        :raises ValueError: If a column type is unsupported.
        :raises HttpError: If an underlying HTTP request fails.
        """
        if not isinstance(columns, dict) or not columns:
            raise TypeError("columns must be a non-empty dict[name -> type]")

        ent = self._get_entity_by_table_schema_name(table_schema_name)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{table_schema_name}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )

        metadata_id = ent.get("MetadataId")
        created: List[str] = []
        needs_picklist_flush = False

        for column_name, column_type in columns.items():
            payload = self._attribute_payload(column_name, column_type)
            if not payload:
                raise ValueError(f"Unsupported column type '{column_type}' for '{column_name}'.")

            url = f"{self.api}/EntityDefinitions({metadata_id})/Attributes"
            self._request("post", url, json=payload)

            created.append(column_name)

            if "OptionSet" in payload:
                needs_picklist_flush = True

        if needs_picklist_flush:
            self._flush_cache("picklist")

        return created

    def _delete_columns(
        self,
        table_schema_name: str,
        columns: Union[str, List[str]],
    ) -> List[str]:
        """Delete one or more columns from a table.

        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``
        :param columns: Single column name or list of column names
        :type columns: ``str`` | ``list[str]``

        :return: List of deleted column schema names (empty if none removed).
        :rtype: ``list[str]``

        :raises TypeError: If ``columns`` is neither a ``str`` nor ``list[str]``.
        :raises ValueError: If any provided column name is empty.
        :raises MetadataError: If the table or a specified column does not exist.
        :raises RuntimeError: If column metadata lacks a required ``MetadataId``.
        :raises HttpError: If an underlying delete request fails.
        """
        if isinstance(columns, str):
            names = [columns]
        elif isinstance(columns, list):
            names = columns
        else:
            raise TypeError("columns must be str or list[str]")

        for name in names:
            if not isinstance(name, str) or not name.strip():
                raise ValueError("column names must be non-empty strings")

        ent = self._get_entity_by_table_schema_name(table_schema_name)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{table_schema_name}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )

        # Use the actual SchemaName from the entity metadata
        entity_schema = ent.get("SchemaName") or table_schema_name
        metadata_id = ent.get("MetadataId")
        deleted: List[str] = []
        needs_picklist_flush = False

        for column_name in names:
            attr_meta = self._get_attribute_metadata(metadata_id, column_name, extra_select="@odata.type,AttributeType")
            if not attr_meta:
                raise MetadataError(
                    f"Column '{column_name}' not found on table '{entity_schema}'.",
                    subcode=METADATA_COLUMN_NOT_FOUND,
                )

            attr_metadata_id = attr_meta.get("MetadataId")
            if not attr_metadata_id:
                raise RuntimeError(f"Metadata incomplete for column '{column_name}' (missing MetadataId).")

            attr_url = f"{self.api}/EntityDefinitions({metadata_id})/Attributes({attr_metadata_id})"
            self._request("delete", attr_url, headers={"If-Match": "*"})

            attr_type = attr_meta.get("@odata.type") or attr_meta.get("AttributeType")
            if isinstance(attr_type, str):
                attr_type_l = attr_type.lower()
                if "picklist" in attr_type_l or "optionset" in attr_type_l:
                    needs_picklist_flush = True

            deleted.append(column_name)

        if needs_picklist_flush:
            self._flush_cache("picklist")

        return deleted

    # ---------------------- Cache maintenance -------------------------
    def _flush_cache(
        self,
        kind,
    ) -> int:
        """Flush cached client metadata/state.

        :param kind: Cache kind to flush (only ``"picklist"`` supported).
        :type kind: ``str``
        :return: Number of cache entries removed.
        :rtype: ``int``
        :raises ValidationError: If ``kind`` is unsupported.
        """
        k = (kind or "").strip().lower()
        if k != "picklist":
            raise ValidationError(
                f"Unsupported cache kind '{kind}' (only 'picklist' is implemented)",
                subcode=VALIDATION_UNSUPPORTED_CACHE_KIND,
            )

        removed = len(self._picklist_label_cache)
        self._picklist_label_cache.clear()
        return removed
