# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Async Dataverse Web API client.

:class:`_AsyncODataClient` extends :class:`~PowerPlatform.Dataverse.data._odata._ODataClient`
and overrides every method that performs HTTP I/O as an ``async def`` coroutine.
Pure helper methods (URL builders, body serialisers, cache utilities) are
inherited unchanged from the sync parent class.
"""

from __future__ import annotations

import asyncio
import json
import re
import time
import uuid
import warnings
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Any, AsyncGenerator, AsyncIterator, Dict, List, Optional, Union
from urllib.parse import quote as _url_quote

from ...core._error_codes import (
    METADATA_COLUMN_NOT_FOUND,
    METADATA_ENTITYSET_NAME_MISSING,
    METADATA_ENTITYSET_NOT_FOUND,
    METADATA_TABLE_ALREADY_EXISTS,
    METADATA_TABLE_NOT_FOUND,
    VALIDATION_SQL_EMPTY,
    VALIDATION_SQL_NOT_STRING,
    VALIDATION_UNSUPPORTED_COLUMN_TYPE,
    _is_transient_status,
    _http_subcode,
)
from ...core.errors import HttpError, MetadataError, ValidationError
from ...data._odata import (
    _CALL_SCOPE_CORRELATION_ID,
    _DEFAULT_EXPECTED_STATUSES,
    _GUID_RE,
    _ODataClient,
    _USER_AGENT,
    _extract_pagingcookie,
)
from ...data._raw_request import _RawRequest
from ..core._async_auth import _AsyncAuthManager
from ..core._async_http import _AsyncHttpClient, _AsyncResponse
from ._async_relationships import _AsyncRelationshipOperationsMixin
from ._async_upload import _AsyncFileUploadMixin

if TYPE_CHECKING:
    import aiohttp

__all__: list[str] = []


class _AsyncODataClient(_AsyncFileUploadMixin, _AsyncRelationshipOperationsMixin, _ODataClient):
    """Async Dataverse Web API client.

    Inherits all pure helper methods (URL/body builders, cache utilities,
    column-type mappings) from :class:`~PowerPlatform.Dataverse.data._odata._ODataClient`.
    Every method that performs HTTP I/O is overridden as an ``async def``
    coroutine using :class:`~PowerPlatform.Dataverse.aio.core._async_http._AsyncHttpClient`
    (aiohttp) for transport and
    :class:`~PowerPlatform.Dataverse.aio.core._async_auth._AsyncAuthManager` for
    token acquisition.

    :param auth: Async authentication manager.
    :type auth: ~PowerPlatform.Dataverse.aio.core._async_auth._AsyncAuthManager
    :param base_url: Dataverse environment URL (e.g. ``"https://org.crm.dynamics.com"``).
    :type base_url: :class:`str`
    :param config: Optional SDK configuration. Defaults to
        :meth:`~PowerPlatform.Dataverse.core.config.DataverseConfig.from_env`.
    :param session: Optional ``aiohttp.ClientSession`` for connection pooling.
    """

    def __init__(
        self,
        auth: _AsyncAuthManager,
        base_url: str,
        config: Any = None,
        session: Optional["aiohttp.ClientSession"] = None,
    ) -> None:
        # Bypass _ODataClient.__init__ — it creates a sync _HttpClient and threading.Lock.
        # We initialise equivalent attributes manually.
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
        self._http_logger = None
        if self.config.log_config is not None:
            from ...core._http_logger import _HttpLogger

            self._http_logger = _HttpLogger(self.config.log_config)
        self._http = _AsyncHttpClient(
            retries=self.config.http_retries,
            backoff=self.config.http_backoff,
            timeout=self.config.http_timeout,
            session=session,
            logger=self._http_logger,
        )
        # Caches (same structure as sync client)
        self._logical_to_entityset_cache: dict[str, str] = {}
        # Cache: normalized table_schema_name (lowercase) -> primary id attribute (e.g. accountid)
        self._logical_primaryid_cache: dict[str, str] = {}
        self._picklist_label_cache: dict[str, dict] = {}
        self._picklist_cache_ttl_seconds: int = 3600  # 1 hour TTL
        # asyncio.Lock instead of threading.Lock for concurrent coroutine safety
        self._picklist_cache_lock: asyncio.Lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Context manager / lifecycle
    # ------------------------------------------------------------------

    @asynccontextmanager
    async def _call_scope(self) -> AsyncIterator[str]:  # type: ignore[override]
        """Async context manager that sets a per-call correlation ID."""
        shared_id = str(uuid.uuid4())
        token = _CALL_SCOPE_CORRELATION_ID.set(shared_id)
        try:
            yield shared_id
        finally:
            _CALL_SCOPE_CORRELATION_ID.reset(token)

    async def close(self) -> None:  # type: ignore[override]
        """Close the async HTTP client and clear caches."""
        self._logical_to_entityset_cache.clear()
        self._logical_primaryid_cache.clear()
        self._picklist_label_cache.clear()
        await self._http.close()
        if self._http_logger is not None:
            self._http_logger.close()
            self._http_logger = None

    # ------------------------------------------------------------------
    # HTTP pipeline (async overrides)
    # ------------------------------------------------------------------

    async def _headers(self) -> Dict[str, str]:  # type: ignore[override]
        """Build standard OData headers with a fresh bearer token."""
        scope = f"{self.base_url}/.default"
        token = (await self.auth._acquire_token(scope)).access_token
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "User-Agent": _USER_AGENT,
        }

    async def _merge_headers(self, headers: Optional[Dict[str, str]] = None) -> Dict[str, str]:  # type: ignore[override]
        """Merge caller-supplied headers on top of the standard OData headers."""
        base = await self._headers()
        if not headers:
            return base
        merged = base.copy()
        merged.update(headers)
        return merged

    async def _raw_request(self, method: str, url: str, **kwargs: Any) -> _AsyncResponse:  # type: ignore[override]
        """Execute an HTTP request via aiohttp (no error parsing)."""
        return await self._http._request(method, url, **kwargs)

    async def _request(  # type: ignore[override]
        self,
        method: str,
        url: str,
        *,
        expected: tuple[int, ...] = _DEFAULT_EXPECTED_STATUSES,
        **kwargs: Any,
    ) -> _AsyncResponse:
        """Execute an HTTP request, merging auth headers and raising on unexpected status."""
        # Merge auth headers with any caller-supplied headers
        caller_headers: Optional[Dict[str, str]] = kwargs.pop("headers", None)
        merged = await self._merge_headers(caller_headers)
        merged.setdefault("x-ms-client-request-id", str(uuid.uuid4()))
        merged.setdefault("x-ms-correlation-id", _CALL_SCOPE_CORRELATION_ID.get())
        kwargs["headers"] = merged

        response = await self._raw_request(method, url, **kwargs)
        if response.status_code in expected:
            return response

        # Parse error body for a useful message
        response_headers = response.headers or {}
        body_excerpt = (response.text or "")[:200]
        svc_code = None
        msg = f"HTTP {response.status_code}"
        try:
            data = response.json() if response.text else {}
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

        sc = response.status_code
        subcode = _http_subcode(sc)
        request_id = (
            response_headers.get("x-ms-service-request-id")
            or response_headers.get("req_id")
            or response_headers.get("x-ms-request-id")
        )
        traceparent = response_headers.get("traceparent")
        ra = response_headers.get("Retry-After")
        retry_after = None
        if ra:
            try:
                retry_after = int(ra)
            except Exception:
                retry_after = None
        is_transient = _is_transient_status(sc)
        raise HttpError(
            msg,
            status_code=sc,
            subcode=subcode,
            service_error_code=svc_code,
            correlation_id=merged.get("x-ms-correlation-id"),
            client_request_id=merged.get("x-ms-client-request-id"),
            service_request_id=request_id,
            traceparent=traceparent,
            body_excerpt=body_excerpt,
            retry_after=retry_after,
            is_transient=is_transient,
        )

    async def _execute_raw(  # type: ignore[override]
        self,
        req: _RawRequest,
        *,
        expected: tuple[int, ...] = _DEFAULT_EXPECTED_STATUSES,
    ) -> _AsyncResponse:
        """Execute a pre-built :class:`_RawRequest` and return the response."""
        kwargs: Dict[str, Any] = {}
        if req.body is not None:
            kwargs["data"] = req.body.encode("utf-8")
        if req.headers:
            kwargs["headers"] = req.headers
        return await self._request(req.method.lower(), req.url, expected=expected, **kwargs)

    # ------------------------------------------------------------------
    # Entity set / primary key resolution
    # ------------------------------------------------------------------

    async def _entity_set_from_schema_name(self, table_schema_name: str) -> str:  # type: ignore[override]
        """Resolve the entity set name for *table_schema_name* (cached)."""
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
        response = await self._request("get", url, params=params)
        try:
            body = response.json()
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
                f"Unable to resolve entity set for table schema name '{table_schema_name}'. "
                f"Provide the singular table schema name.{plural_hint}",
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

    async def _primary_id_attr(self, table_schema_name: str) -> str:  # type: ignore[override]
        """Return the primary key attribute name for *table_schema_name* (cached)."""
        cache_key = self._normalize_cache_key(table_schema_name)
        pid = self._logical_primaryid_cache.get(cache_key)
        if pid:
            return pid
        # Resolve metadata (populates _logical_primaryid_cache or raises if table_schema_name unknown)
        await self._entity_set_from_schema_name(table_schema_name)
        pid2 = self._logical_primaryid_cache.get(cache_key)
        if pid2:
            return pid2
        raise RuntimeError(
            f"PrimaryIdAttribute not resolved for table_schema_name '{table_schema_name}'. "
            "Metadata did not include PrimaryIdAttribute."
        )

    # ------------------------------------------------------------------
    # Picklist / label resolution
    # ------------------------------------------------------------------

    async def _request_metadata_with_retry(self, method: str, url: str, **kwargs: Any) -> _AsyncResponse:  # type: ignore[override]
        """Fetch metadata with up to 5 retries on 404 (newly provisioned tables)."""
        max_attempts = 5
        backoff_seconds = 0.4
        for attempt in range(1, max_attempts + 1):
            try:
                return await self._request(method, url, **kwargs)
            except HttpError as err:
                if getattr(err, "status_code", None) == 404:
                    if attempt < max_attempts:
                        await asyncio.sleep(backoff_seconds * (2 ** (attempt - 1)))
                        continue
                    raise RuntimeError(f"Metadata request failed after {max_attempts} retries (404): {url}") from err
                raise
        raise RuntimeError("_request_metadata_with_retry: retry loop exhausted")  # pragma: no cover

    async def _bulk_fetch_picklists(self, table_schema_name: str) -> None:  # type: ignore[override]
        """Fetch all picklist attributes and cache their label→int mappings."""
        table_key = self._normalize_cache_key(table_schema_name)
        now = time.time()

        # Fast path: check without lock
        table_entry = self._picklist_label_cache.get(table_key)
        if isinstance(table_entry, dict) and (now - table_entry.get("ts", 0)) < self._picklist_cache_ttl_seconds:
            return

        async with self._picklist_cache_lock:
            # Double-checked: another coroutine may have populated the cache while we waited
            table_entry = self._picklist_label_cache.get(table_key)
            if isinstance(table_entry, dict) and (now - table_entry.get("ts", 0)) < self._picklist_cache_ttl_seconds:
                return

            table_esc = self._escape_odata_quotes(table_schema_name.lower())
            url = (
                f"{self.api}/EntityDefinitions(LogicalName='{table_esc}')"
                f"/Attributes/Microsoft.Dynamics.CRM.PicklistAttributeMetadata"
                f"?$select=LogicalName&$expand=OptionSet($select=Options)"
            )
            response = await self._request_metadata_with_retry("get", url)
            body = response.json()
            items = body.get("value", []) if isinstance(body, dict) else []

            picklists: Dict[str, Dict[str, int]] = {}
            for item in items:
                if not isinstance(item, dict):
                    continue
                ln = item.get("LogicalName", "").lower()
                if not ln:
                    continue
                option_set = item.get("OptionSet") or {}
                options = option_set.get("Options") if isinstance(option_set, dict) else None
                mapping: Dict[str, int] = {}
                if isinstance(options, list):
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
                picklists[ln] = mapping

            self._picklist_label_cache[table_key] = {"ts": now, "picklists": picklists}

    async def _convert_labels_to_ints(  # type: ignore[override]
        self, table_schema_name: str, record: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Return a copy of *record* with picklist label strings converted to ints."""
        resolved_record = record.copy()
        # Check if there are any string-valued candidates worth resolving
        has_candidates = any(
            isinstance(v, str) and v.strip() and isinstance(k, str) and "@odata." not in k
            for k, v in resolved_record.items()
        )
        if not has_candidates:
            return resolved_record

        # Bulk-fetch all picklists for this table (1 API call, cached for TTL)
        await self._bulk_fetch_picklists(table_schema_name)

        # Resolve labels from the nested cache
        table_key = self._normalize_cache_key(table_schema_name)
        table_entry = self._picklist_label_cache.get(table_key)
        if not isinstance(table_entry, dict):
            return resolved_record
        picklists = table_entry.get("picklists", {})

        for k, v in resolved_record.items():
            if not isinstance(v, str) or not v.strip():
                continue
            if isinstance(k, str) and "@odata." in k:
                continue
            attr_key = self._normalize_cache_key(k)
            mapping = picklists.get(attr_key)
            if not isinstance(mapping, dict) or not mapping:
                continue
            norm = self._normalize_picklist_label(v)
            val = mapping.get(norm)
            if val is not None:
                resolved_record[k] = val
        return resolved_record

    # ------------------------------------------------------------------
    # CRUD — single record
    # ------------------------------------------------------------------

    async def _create(self, entity_set: str, table_schema_name: str, record: Dict[str, Any]) -> str:  # type: ignore[override]
        """Create a single record and return its GUID."""
        req = await self._build_create(entity_set, table_schema_name, record)
        response = await self._execute_raw(req)
        ent_loc = response.headers.get("OData-EntityId") or response.headers.get("OData-EntityID")
        if ent_loc:
            guid_match = _GUID_RE.search(ent_loc)
            if guid_match:
                return guid_match.group(0)
        loc = response.headers.get("Location")
        if loc:
            guid_match = _GUID_RE.search(loc)
            if guid_match:
                return guid_match.group(0)
        header_keys = ", ".join(sorted(response.headers.keys()))
        raise RuntimeError(
            f"Create response missing GUID in OData-EntityId/Location headers "
            f"(status={response.status_code}). Headers: {header_keys}"
        )

    async def _create_multiple(  # type: ignore[override]
        self,
        entity_set: str,
        table_schema_name: str,
        records: List[Dict[str, Any]],
    ) -> List[str]:
        """Create multiple records via ``CreateMultiple`` and return GUIDs."""
        req = await self._build_create_multiple(entity_set, table_schema_name, records)
        response = await self._execute_raw(req)
        try:
            body = response.json() if response.text else {}
        except ValueError:
            body = {}
        if not isinstance(body, dict):
            return []
        ids = body.get("Ids")
        if isinstance(ids, list):
            return [i for i in ids if isinstance(i, str)]
        value = body.get("value")
        if isinstance(value, list):
            out: List[str] = []
            for item in value:
                if isinstance(item, dict):
                    for k, v in item.items():
                        if isinstance(k, str) and k.lower().endswith("id") and isinstance(v, str) and len(v) >= 32:
                            out.append(v)
                            break
            return out
        return []

    async def _get(  # type: ignore[override]
        self, table_schema_name: str, key: str, select: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Retrieve a single record by GUID."""
        return (await self._execute_raw(await self._build_get(table_schema_name, key, select=select))).json()

    async def _get_multiple(  # type: ignore[override]
        self,
        table_schema_name: str,
        select: Optional[List[str]] = None,
        filter: Optional[str] = None,
        orderby: Optional[List[str]] = None,
        top: Optional[int] = None,
        expand: Optional[List[str]] = None,
        page_size: Optional[int] = None,
        count: bool = False,
        include_annotations: Optional[str] = None,
    ) -> AsyncGenerator[List[Dict[str, Any]], None]:
        """Async generator yielding pages of records."""
        extra_headers: Dict[str, str] = {}
        prefer_parts: List[str] = []
        if page_size is not None:
            ps = int(page_size)
            if ps > 0:
                prefer_parts.append(f"odata.maxpagesize={ps}")
        if include_annotations:
            prefer_parts.append(f'odata.include-annotations="{include_annotations}"')
        if prefer_parts:
            extra_headers["Prefer"] = ",".join(prefer_parts)

        async def _do_request(url: str, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
            response = await self._request("get", url, headers=extra_headers or None, params=params)
            try:
                return response.json()
            except ValueError:
                return {}

        entity_set = await self._entity_set_from_schema_name(table_schema_name)
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
        if count:
            params["$count"] = "true"

        data = await _do_request(base_url, params=params or None)
        items = data.get("value") if isinstance(data, dict) else None
        if isinstance(items, list) and items:
            yield [x for x in items if isinstance(x, dict)]

        next_link = None
        if isinstance(data, dict):
            next_link = data.get("@odata.nextLink") or data.get("odata.nextLink")

        while next_link:
            data = await _do_request(next_link)
            items = data.get("value") if isinstance(data, dict) else None
            if isinstance(items, list) and items:
                yield [x for x in items if isinstance(x, dict)]
            next_link = (data.get("@odata.nextLink") or data.get("odata.nextLink")) if isinstance(data, dict) else None

    async def _update(self, table_schema_name: str, key: str, data: Dict[str, Any]) -> None:  # type: ignore[override]
        """Update a single record by GUID."""
        await self._execute_raw(await self._build_update(table_schema_name, key, data))

    async def _update_multiple(  # type: ignore[override]
        self,
        entity_set: str,
        table_schema_name: str,
        records: List[Dict[str, Any]],
    ) -> None:
        """Bulk update via ``UpdateMultiple``."""
        if not isinstance(records, list) or not records or not all(isinstance(r, dict) for r in records):
            raise TypeError("records must be a non-empty list[dict]")
        await self._execute_raw(await self._build_update_multiple_from_records(entity_set, table_schema_name, records))

    async def _update_by_ids(  # type: ignore[override]
        self,
        table_schema_name: str,
        ids: List[str],
        changes: Union[Dict[str, Any], List[Dict[str, Any]]],
    ) -> None:
        """Update many records by GUID list via ``UpdateMultiple``."""
        if not isinstance(ids, list):
            raise TypeError("ids must be list[str]")
        if not ids:
            return
        entity_set = await self._entity_set_from_schema_name(table_schema_name)
        await self._execute_raw(await self._build_update_multiple(entity_set, table_schema_name, ids, changes))

    async def _delete(self, table_schema_name: str, key: str) -> None:  # type: ignore[override]
        """Delete a single record by GUID."""
        await self._execute_raw(await self._build_delete(table_schema_name, key))

    async def _delete_multiple(  # type: ignore[override]
        self,
        table_schema_name: str,
        ids: List[str],
    ) -> Optional[str]:
        """Delete many records via ``BulkDelete`` and return the async job ID."""
        targets = [rid for rid in ids if rid]
        if not targets:
            return None
        req = await self._build_delete_multiple(table_schema_name, targets)
        response = await self._execute_raw(req, expected=(200, 202, 204))
        job_id = None
        try:
            body = response.json() if response.text else {}
        except ValueError:
            body = {}
        if isinstance(body, dict):
            job_id = body.get("JobId")
        return job_id

    async def _upsert(  # type: ignore[override]
        self,
        entity_set: str,
        table_schema_name: str,
        alternate_key: Dict[str, Any],
        record: Dict[str, Any],
    ) -> None:
        """Upsert a single record using an alternate key."""
        await self._execute_raw(
            await self._build_upsert(entity_set, table_schema_name, alternate_key, record),
            expected=(200, 201, 204),
        )

    async def _upsert_multiple(  # type: ignore[override]
        self,
        entity_set: str,
        table_schema_name: str,
        alternate_keys: List[Dict[str, Any]],
        records: List[Dict[str, Any]],
    ) -> None:
        """Upsert multiple records via ``UpsertMultiple``."""
        await self._execute_raw(
            await self._build_upsert_multiple(entity_set, table_schema_name, alternate_keys, records),
            expected=(200, 201, 204),
        )

    # ------------------------------------------------------------------
    # SQL query
    # ------------------------------------------------------------------

    async def _query_sql(self, sql: str) -> List[Dict[str, Any]]:  # type: ignore[override]
        """Execute a read-only SQL SELECT via the Dataverse ``?sql=`` API."""
        if not isinstance(sql, str):
            raise ValidationError("sql must be a string", subcode=VALIDATION_SQL_NOT_STRING)
        if not sql.strip():
            raise ValidationError("sql must be a non-empty string", subcode=VALIDATION_SQL_EMPTY)
        sql = sql.strip()
        response = await self._execute_raw(await self._build_sql(sql))
        try:
            body = response.json()
        except ValueError:
            return []

        # Collect first page
        results: List[Dict[str, Any]] = []
        if isinstance(body, list):
            return [row for row in body if isinstance(row, dict)]
        if not isinstance(body, dict):
            return results

        value = body.get("value")
        if isinstance(value, list):
            results = [row for row in value if isinstance(row, dict)]

        # Follow pagination links until exhausted
        raw_link = body.get("@odata.nextLink") or body.get("odata.nextLink")
        next_link: Optional[str] = raw_link if isinstance(raw_link, str) else None
        visited: set[str] = set()
        seen_cookies: set[str] = set()
        while next_link:
            # Guard 1: exact URL cycle (same next_link returned twice)
            if next_link in visited:
                warnings.warn(
                    f"SQL pagination stopped after {len(results)} rows — "
                    "the Dataverse server returned the same nextLink URL twice, "
                    "indicating an infinite pagination cycle. "
                    "Returning the rows collected so far. "
                    "To avoid pagination entirely, add a TOP clause to your query.",
                    RuntimeWarning,
                    stacklevel=4,
                )
                break
            visited.add(next_link)
            # Guard 2: server-side bug where pagingcookie does not advance between
            # pages (pagenumber increments but cookie GUIDs stay the same), which
            # causes an infinite loop even though URLs differ.
            cookie = _extract_pagingcookie(next_link)
            if cookie is not None:
                if cookie in seen_cookies:
                    warnings.warn(
                        f"SQL pagination stopped after {len(results)} rows — "
                        "the Dataverse server returned the same pagingcookie twice "
                        "(pagenumber incremented but the paging position did not advance). "
                        "This is a server-side bug. Returning the rows collected so far. "
                        "To avoid pagination entirely, add a TOP clause to your query.",
                        RuntimeWarning,
                        stacklevel=4,
                    )
                    break
                seen_cookies.add(cookie)
            try:
                page_resp = await self._request("get", next_link)
            except Exception as exc:
                warnings.warn(
                    f"SQL pagination stopped after {len(results)} rows — "
                    f"the next-page request failed: {exc}. "
                    "Add a TOP clause to your query to limit results to a single page.",
                    RuntimeWarning,
                    stacklevel=5,
                )
                break
            try:
                page_body = page_resp.json()
            except ValueError as exc:
                warnings.warn(
                    f"SQL pagination stopped after {len(results)} rows — "
                    f"the next-page response was not valid JSON: {exc}. "
                    "Add a TOP clause to your query to limit results to a single page.",
                    RuntimeWarning,
                    stacklevel=5,
                )
                break
            if not isinstance(page_body, dict):
                break
            page_value = page_body.get("value")
            if not isinstance(page_value, list) or not page_value:
                break
            results.extend(row for row in page_value if isinstance(row, dict))
            raw_link = page_body.get("@odata.nextLink") or page_body.get("odata.nextLink")
            next_link = raw_link if isinstance(raw_link, str) else None

        return results

    # ------------------------------------------------------------------
    # Table / entity metadata
    # ------------------------------------------------------------------

    async def _get_entity_by_table_schema_name(  # type: ignore[override]
        self,
        table_schema_name: str,
        headers: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Fetch entity metadata by table schema name."""
        url = f"{self.api}/EntityDefinitions"
        # LogicalName is stored lowercase, so we lowercase the input for lookup
        logical_lower = table_schema_name.lower()
        logical_escaped = self._escape_odata_quotes(logical_lower)
        params = {
            "$select": "MetadataId,LogicalName,SchemaName,EntitySetName,PrimaryNameAttribute,PrimaryIdAttribute",
            "$filter": f"LogicalName eq '{logical_escaped}'",
        }
        response = await self._request("get", url, headers=headers, params=params)
        items = response.json().get("value", [])
        return items[0] if items else None

    async def _create_entity(  # type: ignore[override]
        self,
        table_schema_name: str,
        display_name: str,
        attributes: List[Dict[str, Any]],
        solution_unique_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create an entity definition and return its metadata."""
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
        await self._request("post", url, json=payload, params=params)
        ent = await self._get_entity_by_table_schema_name(
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

    async def _get_attribute_metadata(  # type: ignore[override]
        self,
        entity_metadata_id: str,
        column_name: str,
        extra_select: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Fetch attribute metadata for a column."""
        # Convert to lowercase logical name for lookup
        logical_name = column_name.lower()
        attr_escaped = self._escape_odata_quotes(logical_name)
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
            "$filter": f"LogicalName eq '{attr_escaped}'",
        }
        response = await self._request("get", url, params=params)
        try:
            body = response.json() if response.text else {}
        except ValueError:
            return None
        items = body.get("value") if isinstance(body, dict) else None
        if isinstance(items, list) and items:
            item = items[0]
            if isinstance(item, dict):
                return item
        return None

    async def _wait_for_attribute_visibility(  # type: ignore[override]
        self,
        entity_set: str,
        attribute_name: str,
        delays: tuple = (0, 3, 10, 20),
    ) -> None:
        """Poll until a newly created attribute becomes visible in the data API."""
        # Convert to lowercase logical name for URL
        logical_name = attribute_name.lower()
        probe_url = f"{self.api}/{entity_set}?$top=1&$select={logical_name}"
        last_error = None
        total_wait = sum(delays)

        for delay in delays:
            if delay:
                await asyncio.sleep(delay)
            try:
                await self._request("get", probe_url)
                return
            except Exception as ex:
                last_error = ex
                continue

        # All retries exhausted - raise with context
        raise RuntimeError(
            f"Attribute '{logical_name}' did not become visible in the data API "
            f"after {total_wait} seconds (exhausted all retries)."
        ) from last_error

    async def _get_table_info(self, table_schema_name: str) -> Optional[Dict[str, Any]]:  # type: ignore[override]
        """Return basic metadata for a table, or ``None`` if not found."""
        ent = await self._get_entity_by_table_schema_name(table_schema_name)
        if not ent:
            return None
        return {
            "table_schema_name": ent.get("SchemaName") or table_schema_name,
            "table_logical_name": ent.get("LogicalName"),
            "entity_set_name": ent.get("EntitySetName"),
            "metadata_id": ent.get("MetadataId"),
            "primary_name_attribute": ent.get("PrimaryNameAttribute"),
            "primary_id_attribute": ent.get("PrimaryIdAttribute"),
            "columns_created": [],
        }

    async def _list_tables(  # type: ignore[override]
        self,
        filter: Optional[str] = None,
        select: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """List all non-private tables."""
        response = await self._execute_raw(self._build_list_entities(filter=filter, select=select))
        return response.json().get("value", [])

    async def _delete_table(self, table_schema_name: str) -> None:  # type: ignore[override]
        """Delete a table by schema name."""
        ent = await self._get_entity_by_table_schema_name(table_schema_name)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{table_schema_name}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )
        await self._execute_raw(self._build_delete_entity(ent["MetadataId"]))

    async def _create_table(  # type: ignore[override]
        self,
        table_schema_name: str,
        schema: Dict[str, Any],
        solution_unique_name: Optional[str] = None,
        primary_column_schema_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a custom table with specified columns."""
        ent = await self._get_entity_by_table_schema_name(table_schema_name)
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
                f"{table_schema_name.split('_', 1)[0]}_Name" if "_" in table_schema_name else "new_Name"
            )

        attributes: List[Dict[str, Any]] = [
            self._attribute_payload(primary_attr_schema, "string", is_primary_name=True)
        ]
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

        metadata = await self._create_entity(
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
            "primary_name_attribute": metadata.get("PrimaryNameAttribute"),
            "primary_id_attribute": metadata.get("PrimaryIdAttribute"),
            "columns_created": created_cols,
        }

    async def _create_columns(  # type: ignore[override]
        self,
        table_schema_name: str,
        columns: Dict[str, Any],
    ) -> List[str]:
        """Add columns to an existing table."""
        if not isinstance(columns, dict) or not columns:
            raise TypeError("columns must be a non-empty dict[name -> type]")
        ent = await self._get_entity_by_table_schema_name(table_schema_name)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{table_schema_name}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )
        metadata_id = ent.get("MetadataId")
        created: List[str] = []
        needs_picklist_flush = False
        for column_name, column_type in columns.items():
            attr = self._attribute_payload(column_name, column_type)
            if not attr:
                raise ValidationError(
                    f"Unsupported column type '{column_type}' for column '{column_name}'.",
                    subcode=VALIDATION_UNSUPPORTED_COLUMN_TYPE,
                )
            if "OptionSet" in attr:
                needs_picklist_flush = True
            req = _RawRequest(
                method="POST",
                url=f"{self.api}/EntityDefinitions({metadata_id})/Attributes",
                body=json.dumps(attr, ensure_ascii=False),
            )
            await self._execute_raw(req)
            created.append(column_name)
        if needs_picklist_flush:
            self._flush_cache("picklist")
        return created

    async def _delete_columns(  # type: ignore[override]
        self,
        table_schema_name: str,
        columns: Union[str, List[str]],
    ) -> List[str]:
        """Remove columns from a table."""
        if isinstance(columns, str):
            names = [columns]
        elif isinstance(columns, list):
            names = columns
        else:
            raise TypeError("columns must be str or list[str]")
        for name in names:
            if not isinstance(name, str) or not name.strip():
                raise ValueError("column names must be non-empty strings")
        ent = await self._get_entity_by_table_schema_name(table_schema_name)
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
            attr_meta = await self._get_attribute_metadata(
                metadata_id, column_name, extra_select="@odata.type,AttributeType"
            )
            if not attr_meta:
                raise MetadataError(
                    f"Column '{column_name}' not found on table '{entity_schema}'.",
                    subcode=METADATA_COLUMN_NOT_FOUND,
                )
            attr_metadata_id = attr_meta.get("MetadataId")
            if not attr_metadata_id:
                raise RuntimeError(f"Metadata incomplete for column '{column_name}' (missing MetadataId).")
            await self._execute_raw(self._build_delete_column(metadata_id, attr_metadata_id))
            attr_type = attr_meta.get("@odata.type") or attr_meta.get("AttributeType")
            if isinstance(attr_type, str):
                attr_type_l = attr_type.lower()
                if "picklist" in attr_type_l or "optionset" in attr_type_l:
                    needs_picklist_flush = True
            deleted.append(column_name)
        if needs_picklist_flush:
            self._flush_cache("picklist")
        return deleted

    async def _create_alternate_key(  # type: ignore[override]
        self,
        table_schema_name: str,
        key_name: str,
        columns: List[str],
        display_name_label: Any = None,
    ) -> Dict[str, Any]:
        """Create an alternate key on a table."""
        ent = await self._get_entity_by_table_schema_name(table_schema_name)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{table_schema_name}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )
        logical_name = ent.get("LogicalName", table_schema_name.lower())
        url = f"{self.api}/EntityDefinitions(LogicalName='{logical_name}')/Keys"
        payload: Dict[str, Any] = {
            "SchemaName": key_name,
            "KeyAttributes": columns,
        }
        if display_name_label is not None:
            payload["DisplayName"] = display_name_label.to_dict()
        response = await self._request("post", url, json=payload)
        metadata_id = self._extract_id_from_header(response.headers.get("OData-EntityId"))
        return {
            "metadata_id": metadata_id,
            "schema_name": key_name,
            "key_attributes": columns,
        }

    async def _get_alternate_keys(self, table_schema_name: str) -> List[Dict[str, Any]]:  # type: ignore[override]
        """List all alternate keys on a table."""
        ent = await self._get_entity_by_table_schema_name(table_schema_name)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{table_schema_name}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )
        logical_name = ent.get("LogicalName", table_schema_name.lower())
        url = f"{self.api}/EntityDefinitions(LogicalName='{logical_name}')/Keys"
        response = await self._request("get", url)
        data = response.json()
        return data.get("value", []) if isinstance(data, dict) else []

    async def _delete_alternate_key(self, table_schema_name: str, key_id: str) -> None:  # type: ignore[override]
        """Delete an alternate key by metadata ID."""
        ent = await self._get_entity_by_table_schema_name(table_schema_name)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{table_schema_name}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )
        logical_name = ent.get("LogicalName", table_schema_name.lower())
        url = f"{self.api}/EntityDefinitions(LogicalName='{logical_name}')/Keys({key_id})"
        await self._request("delete", url)

    # ------------------------------------------------------------------
    # _build_* request builders (async — await IO-touching helpers)
    # ------------------------------------------------------------------

    async def _build_create(
        self,
        entity_set: str,
        table: str,
        data: Dict[str, Any],
        *,
        content_id: Optional[int] = None,
    ) -> _RawRequest:
        """Build a single-record POST request without sending it."""
        body = self._lowercase_keys(data)
        body = await self._convert_labels_to_ints(table, body)
        return _RawRequest(
            method="POST",
            url=f"{self.api}/{entity_set}",
            body=json.dumps(body, ensure_ascii=False),
            content_id=content_id,
        )

    async def _build_create_multiple(
        self,
        entity_set: str,
        table: str,
        records: List[Dict[str, Any]],
    ) -> _RawRequest:
        """Build a CreateMultiple POST request without sending it."""
        if not all(isinstance(r, dict) for r in records):
            raise TypeError("All items for multi-create must be dicts")
        logical_name = table.lower()
        enriched = []
        for r in records:
            r = self._lowercase_keys(r)
            r = await self._convert_labels_to_ints(table, r)
            if "@odata.type" not in r:
                r = {**r, "@odata.type": f"Microsoft.Dynamics.CRM.{logical_name}"}
            enriched.append(r)
        return _RawRequest(
            method="POST",
            url=f"{self.api}/{entity_set}/Microsoft.Dynamics.CRM.CreateMultiple",
            body=json.dumps({"Targets": enriched}, ensure_ascii=False),
        )

    async def _build_update(
        self,
        table: str,
        record_id: str,
        changes: Dict[str, Any],
        *,
        content_id: Optional[int] = None,
    ) -> _RawRequest:
        """Build a single-record PATCH request without sending it.

        ``record_id`` may be a ``"$n"`` content-ID reference; in that case the
        URL is the reference itself (resolved server-side within a changeset).
        """
        body = self._lowercase_keys(changes)
        body = await self._convert_labels_to_ints(table, body)
        if record_id.startswith("$"):
            url = record_id
        else:
            entity_set = await self._entity_set_from_schema_name(table)
            url = f"{self.api}/{entity_set}{self._format_key(record_id)}"
        return _RawRequest(
            method="PATCH",
            url=url,
            body=json.dumps(body, ensure_ascii=False),
            headers={"If-Match": "*"},
            content_id=content_id,
        )

    async def _build_update_multiple_from_records(
        self,
        entity_set: str,
        table: str,
        records: List[Dict[str, Any]],
    ) -> _RawRequest:
        """Build an UpdateMultiple POST request from pre-assembled records.

        Each record must already contain the primary key attribute. This helper
        is shared by :meth:`_update_multiple` (which pre-assembles records) and
        :meth:`_build_update_multiple` (which assembles from ids + changes).
        """
        logical_name = table.lower()
        enriched = []
        for r in records:
            r = self._lowercase_keys(r)
            r = await self._convert_labels_to_ints(table, r)
            if "@odata.type" not in r:
                r = {**r, "@odata.type": f"Microsoft.Dynamics.CRM.{logical_name}"}
            enriched.append(r)
        return _RawRequest(
            method="POST",
            url=f"{self.api}/{entity_set}/Microsoft.Dynamics.CRM.UpdateMultiple",
            body=json.dumps({"Targets": enriched}, ensure_ascii=False),
        )

    async def _build_update_multiple(
        self,
        entity_set: str,
        table: str,
        ids: List[str],
        changes: Union[Dict[str, Any], List[Dict[str, Any]]],
    ) -> _RawRequest:
        """Build an UpdateMultiple POST request without sending it."""
        pk_attr = await self._primary_id_attr(table)
        if isinstance(changes, dict):
            records = [{pk_attr: rid, **changes} for rid in ids]
        elif isinstance(changes, list):
            if len(changes) != len(ids):
                raise ValidationError(
                    "ids and changes lists must have equal length for paired update.",
                    subcode="ids_changes_length_mismatch",
                )
            for i, ch in enumerate(changes):
                if not isinstance(ch, dict):
                    raise ValidationError(
                        f"Each patch in changes must be a dict; got {type(ch).__name__} at index {i}.",
                        subcode="invalid_patch_type",
                    )
            records = [{pk_attr: rid, **ch} for rid, ch in zip(ids, changes)]
        else:
            raise ValidationError("changes must be a dict or list[dict].", subcode="invalid_changes_type")
        return await self._build_update_multiple_from_records(entity_set, table, records)

    async def _build_upsert(
        self,
        entity_set: str,
        table: str,
        alternate_key: Dict[str, Any],
        record: Dict[str, Any],
    ) -> _RawRequest:
        """Build a single-record PATCH upsert request without sending it.

        Unlike :meth:`_build_update`, no ``If-Match: *`` header is added so the
        server creates the record when it does not yet exist.
        """
        body = self._lowercase_keys(record)
        body = await self._convert_labels_to_ints(table, body)
        key_str = self._build_alternate_key_str(alternate_key)
        url = f"{self.api}/{entity_set}({key_str})"
        return _RawRequest(
            method="PATCH",
            url=url,
            body=json.dumps(body, ensure_ascii=False),
        )

    async def _build_upsert_multiple(
        self,
        entity_set: str,
        table: str,
        alternate_keys: List[Dict[str, Any]],
        records: List[Dict[str, Any]],
    ) -> _RawRequest:
        """Build an UpsertMultiple POST request without sending it."""
        if len(alternate_keys) != len(records):
            raise ValidationError(
                f"alternate_keys and records must have the same length " f"({len(alternate_keys)} != {len(records)})",
                subcode="upsert_length_mismatch",
            )
        logical_name = table.lower()
        targets: List[Dict[str, Any]] = []
        for alt_key, record in zip(alternate_keys, records):
            alt_key_lower = self._lowercase_keys(alt_key)
            record_processed = self._lowercase_keys(record)
            record_processed = await self._convert_labels_to_ints(table, record_processed)
            conflicting = {
                k for k in set(alt_key_lower) & set(record_processed) if alt_key_lower[k] != record_processed[k]
            }
            if conflicting:
                raise ValidationError(
                    f"record payload conflicts with alternate_key on fields: {sorted(conflicting)!r}",
                    subcode="upsert_key_conflict",
                )
            if "@odata.type" not in record_processed:
                record_processed["@odata.type"] = f"Microsoft.Dynamics.CRM.{logical_name}"
            key_str = self._build_alternate_key_str(alt_key)
            record_processed["@odata.id"] = f"{entity_set}({key_str})"
            targets.append(record_processed)
        return _RawRequest(
            method="POST",
            url=f"{self.api}/{entity_set}/Microsoft.Dynamics.CRM.UpsertMultiple",
            body=json.dumps({"Targets": targets}, ensure_ascii=False),
        )

    async def _build_delete(
        self,
        table: str,
        record_id: str,
        *,
        content_id: Optional[int] = None,
    ) -> _RawRequest:
        """Build a single-record DELETE request without sending it.

        ``record_id`` may be a ``"$n"`` content-ID reference.
        """
        if record_id.startswith("$"):
            url = record_id
        else:
            entity_set = await self._entity_set_from_schema_name(table)
            url = f"{self.api}/{entity_set}{self._format_key(record_id)}"
        return _RawRequest(
            method="DELETE",
            url=url,
            headers={"If-Match": "*"},
            content_id=content_id,
        )

    async def _build_delete_multiple(self, table: str, ids: List[str]) -> _RawRequest:
        """Build a BulkDelete POST request without sending it."""
        pk_attr = await self._primary_id_attr(table)
        logical_name = table.lower()
        timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        payload = {
            "JobName": f"Bulk delete {table} records @ {timestamp}",
            "SendEmailNotification": False,
            "ToRecipients": [],
            "CCRecipients": [],
            "RecurrencePattern": "",
            "StartDateTime": timestamp,
            "QuerySet": [
                {
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
                                "Values": [{"Value": rid, "Type": "System.Guid"} for rid in ids],
                            }
                        ],
                    },
                }
            ],
        }
        return _RawRequest(
            method="POST",
            url=f"{self.api}/BulkDelete",
            body=json.dumps(payload, ensure_ascii=False),
        )

    async def _build_get(
        self,
        table: str,
        record_id: str,
        *,
        select: Optional[List[str]] = None,
    ) -> _RawRequest:
        """Build a single-record GET request without sending it."""
        entity_set = await self._entity_set_from_schema_name(table)
        url = f"{self.api}/{entity_set}{self._format_key(record_id)}"
        if select:
            url += "?$select=" + ",".join(self._lowercase_list(select))
        return _RawRequest(method="GET", url=url)

    async def _build_sql(self, sql: str) -> _RawRequest:
        """Build a SQL query GET request without sending it.

        Resolves the entity set from the table name in the SQL statement via
        :meth:`_extract_logical_table`, then embeds the SQL as a URL-encoded
        ``?sql=`` query parameter.
        """
        logical = self._extract_logical_table(sql)
        entity_set = await self._entity_set_from_schema_name(logical)
        return _RawRequest(
            method="GET",
            url=f"{self.api}/{entity_set}?sql={_url_quote(sql, safe='')}",
        )
