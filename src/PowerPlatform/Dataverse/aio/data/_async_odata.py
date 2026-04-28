# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Async Dataverse Web API client with CRUD, SQL query, and table/column metadata management."""

from __future__ import annotations

__all__ = []

import asyncio
import json
import time
import warnings
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Dict, List, Optional, Union

from urllib.parse import quote as _url_quote

import aiohttp

from ..core._async_http import _AsyncHttpClient
from ._async_upload import _AsyncFileUploadMixin
from ._async_relationships import _AsyncRelationshipOperationsMixin
from ...core.errors import *
from ...data._raw_request import _RawRequest
from ...core._error_codes import (
    _http_subcode,
    _is_transient_status,
    VALIDATION_SQL_NOT_STRING,
    VALIDATION_SQL_EMPTY,
    VALIDATION_UNSUPPORTED_COLUMN_TYPE,
    METADATA_ENTITYSET_NOT_FOUND,
    METADATA_ENTITYSET_NAME_MISSING,
    METADATA_TABLE_NOT_FOUND,
    METADATA_TABLE_ALREADY_EXISTS,
    METADATA_COLUMN_NOT_FOUND,
)

from ...data._odata_base import (
    _ODataBase,
    _GUID_RE,
    _extract_pagingcookie,
    _USER_AGENT,
    _DEFAULT_EXPECTED_STATUSES,
    _RequestContext,
)


class _AsyncODataClient(_AsyncFileUploadMixin, _AsyncRelationshipOperationsMixin, _ODataBase):
    """Async Dataverse Web API client: CRUD, SQL-over-API, and table metadata helpers."""

    def __init__(
        self,
        auth,
        base_url: str,
        config=None,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        """Initialize the async OData client.

        Sets up authentication, base URL, configuration, and internal caches.

        :param auth: Async authentication manager providing ``_acquire_token(scope)`` that returns an object with ``access_token``.
        :type auth: ~PowerPlatform.Dataverse.aio.core._async_auth._AsyncAuthManager
        :param base_url: Organization base URL (e.g. ``"https://<org>.crm.dynamics.com"``).
        :type base_url: ``str``
        :param config: Optional Dataverse configuration (HTTP retry, backoff, timeout, language code). If omitted ``DataverseConfig.from_env()`` is used.
        :type config: ~PowerPlatform.Dataverse.core.config.DataverseConfig | ``None``
        :param session: ``aiohttp.ClientSession`` for HTTP connection pooling. Must remain open for the lifetime of this client.
        :type session: :class:`aiohttp.ClientSession` | ``None``
        :raises ValueError: If ``base_url`` is empty after stripping.
        """
        super().__init__(base_url, config)
        self.auth = auth
        self._http = _AsyncHttpClient(
            retries=self.config.http_retries,
            backoff=self.config.http_backoff,
            timeout=self.config.http_timeout,
            session=session,
            logger=self._http_logger,
        )
        # Prevents concurrent coroutines from racing through the picklist TTL check
        # and issuing redundant metadata fetches.
        self._picklist_cache_lock: asyncio.Lock = asyncio.Lock()

    async def close(self) -> None:
        """Close the async OData client and release resources.

        Clears all internal caches and closes the underlying HTTP client.
        Safe to call multiple times.
        """
        super().close()  # sync: clears caches, closes logger
        if self._http is not None:
            await self._http.close()

    async def _headers(self) -> Dict[str, str]:
        """Build standard OData headers with bearer auth."""
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

    async def _raw_request(self, method: str, url: str, **kwargs) -> aiohttp.ClientResponse:
        return await self._http._request(method, url, **kwargs)

    async def _request(
        self,
        method: str,
        url: str,
        *,
        expected: tuple[int, ...] = _DEFAULT_EXPECTED_STATUSES,
        **kwargs,
    ) -> aiohttp.ClientResponse:
        # Acquire base headers once (async), then use a sync closure for _RequestContext.build.
        # _RequestContext.build is a sync classmethod defined in _ODataBase and shared by both
        # sync and async clients — keeping it sync avoids duplicating the header-injection logic.
        base_headers = await self._headers()

        def _merge(h: Optional[Dict[str, str]]) -> Dict[str, str]:
            if not h:
                return base_headers.copy()
            merged = base_headers.copy()
            merged.update(h)
            return merged

        request_context = _RequestContext.build(
            method,
            url,
            expected=expected,
            merge_headers=_merge,
            **kwargs,
        )

        r = await self._raw_request(request_context.method, request_context.url, **request_context.kwargs)
        if r.status in request_context.expected:
            return r

        response_headers = dict(r.headers) if r.headers else {}
        raw_text = ""
        try:
            raw_text = await r.text()
        except Exception:
            pass
        body_excerpt = raw_text[:200]
        svc_code = None
        msg = f"HTTP {r.status}"
        try:
            data = json.loads(raw_text) if raw_text else {}
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
        sc = r.status
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
            correlation_id=request_context.headers.get(
                "x-ms-correlation-id"
            ),  # this is a value set on client side, although it's logged on server side too
            client_request_id=request_context.headers.get(
                "x-ms-client-request-id"
            ),  # this is a value set on client side, although it's logged on server side too
            service_request_id=request_id,
            traceparent=traceparent,
            body_excerpt=body_excerpt,
            retry_after=retry_after,
            is_transient=is_transient,
        )

    async def _execute_raw(
        self,
        req: _RawRequest,
        *,
        expected: tuple[int, ...] = _DEFAULT_EXPECTED_STATUSES,
    ) -> aiohttp.ClientResponse:
        """Execute a ``_RawRequest`` and return the HTTP response.

        Encodes the pre-serialised body (if present) as UTF-8 and merges any
        per-request headers into the standard OData header set.
        """
        kwargs: Dict[str, Any] = {}
        if req.body is not None:
            kwargs["data"] = req.body.encode("utf-8")
        if req.headers:
            kwargs["headers"] = req.headers
        return await self._request(req.method.lower(), req.url, expected=expected, **kwargs)

    # --- CRUD Internal functions ---
    async def _create(self, entity_set: str, table_schema_name: str, record: Dict[str, Any]) -> str:
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
        r = await self._execute_raw(await self._build_create(entity_set, table_schema_name, record))
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
            f"Create response missing GUID in OData-EntityId/Location headers (status={r.status}). Headers: {header_keys}"
        )

    async def _create_multiple(
        self,
        entity_set: str,
        table_schema_name: str,
        records: List[Dict[str, Any]],
    ) -> List[str]:
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
        r = await self._execute_raw(await self._build_create_multiple(entity_set, table_schema_name, records))
        try:
            body = await r.json(content_type=None)
        except (ValueError, aiohttp.ContentTypeError):
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

    async def _upsert(
        self,
        entity_set: str,
        table_schema_name: str,
        alternate_key: Dict[str, Any],
        record: Dict[str, Any],
    ) -> None:
        """Upsert a single record using an alternate key.

        Issues a PATCH request to ``{entity_set}({key_pairs})`` where ``key_pairs``
        is the OData alternate key segment built from ``alternate_key``. Creates the
        record if it does not exist; updates it if it does.

        :param entity_set: Resolved entity set (plural) name.
        :type entity_set: ``str``
        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``
        :param alternate_key: Mapping of alternate key attribute names to their values
            used to identify the target record in the URL.
        :type alternate_key: ``dict[str, Any]``
        :param record: Attribute payload to set on the record.
        :type record: ``dict[str, Any]``

        :return: ``None``
        :rtype: ``None``
        """
        record = self._lowercase_keys(record)
        record = await self._convert_labels_to_ints(table_schema_name, record)
        key_str = self._build_alternate_key_str(alternate_key)
        url = f"{self.api}/{entity_set}({key_str})"
        await self._request("patch", url, json=record, expected=(200, 201, 204))

    async def _upsert_multiple(
        self,
        entity_set: str,
        table_schema_name: str,
        alternate_keys: List[Dict[str, Any]],
        records: List[Dict[str, Any]],
    ) -> None:
        """Upsert multiple records using the collection-bound ``UpsertMultiple`` action.

        Each target is formed by merging the corresponding alternate key fields and record
        fields. The ``@odata.type`` annotation is injected automatically if absent.

        :param entity_set: Resolved entity set (plural) name.
        :type entity_set: ``str``
        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``
        :param alternate_keys: List of alternate key dictionaries, one per record.
            Order is significant: ``alternate_keys[i]`` must correspond to ``records[i]``.
            Python ``list`` preserves insertion order, so the correspondence is guaranteed
            as long as both lists are built from the same source in the same order.
        :type alternate_keys: ``list[dict[str, Any]]``
        :param records: List of record payload dictionaries, one per record.
            Must be the same length as ``alternate_keys``.
        :type records: ``list[dict[str, Any]]``

        :return: ``None``
        :rtype: ``None``

        :raises ValueError: If ``alternate_keys`` and ``records`` differ in length, or if
            any record payload contains an alternate key field with a conflicting value.
        """
        if len(alternate_keys) != len(records):
            raise ValueError(
                f"alternate_keys and records must have the same length " f"({len(alternate_keys)} != {len(records)})"
            )
        logical_name = table_schema_name.lower()
        targets: List[Dict[str, Any]] = []
        for alt_key, record in zip(alternate_keys, records):
            alt_key_lower = self._lowercase_keys(alt_key)
            record_processed = self._lowercase_keys(record)
            record_processed = await self._convert_labels_to_ints(table_schema_name, record_processed)
            conflicting = {
                k for k in set(alt_key_lower) & set(record_processed) if alt_key_lower[k] != record_processed[k]
            }
            if conflicting:
                raise ValueError(f"record payload conflicts with alternate_key on fields: {sorted(conflicting)!r}")
            if "@odata.type" not in record_processed:
                record_processed["@odata.type"] = f"Microsoft.Dynamics.CRM.{logical_name}"
            key_str = self._build_alternate_key_str(alt_key)
            record_processed["@odata.id"] = f"{entity_set}({key_str})"
            targets.append(record_processed)
        payload = {"Targets": targets}
        url = f"{self.api}/{entity_set}/Microsoft.Dynamics.CRM.UpsertMultiple"
        await self._request("post", url, json=payload, expected=(200, 201, 204))

    # --- Derived helpers for high-level client ergonomics ---
    async def _primary_id_attr(self, table_schema_name: str) -> str:
        """Return primary key attribute using metadata; error if unavailable."""
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
            f"PrimaryIdAttribute not resolved for table_schema_name '{table_schema_name}'. Metadata did not include PrimaryIdAttribute."
        )

    async def _update_by_ids(
        self,
        table_schema_name: str,
        ids: List[str],
        changes: Union[Dict[str, Any], List[Dict[str, Any]]],
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
        pk_attr = await self._primary_id_attr(table_schema_name)
        entity_set = await self._entity_set_from_schema_name(table_schema_name)
        if isinstance(changes, dict):
            batch = [{pk_attr: rid, **changes} for rid in ids]
            await self._update_multiple(entity_set, table_schema_name, batch)
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
        await self._update_multiple(entity_set, table_schema_name, batch)
        return None

    async def _delete_multiple(
        self,
        table_schema_name: str,
        ids: List[str],
    ) -> Optional[str]:
        """Delete many records by GUID list via the ``BulkDelete`` action.

        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``
        :param ids: GUIDs of records to delete.
        :type ids: ``list[str]``

        :return: BulkDelete asynchronous job identifier when executed in bulk; ``None`` if no IDs provided or single deletes performed.
        :rtype: ``str`` | ``None``
        """
        targets = [rid for rid in ids if rid]
        if not targets:
            return None
        response = await self._execute_raw(
            await self._build_delete_multiple(table_schema_name, targets),
            expected=(200, 202, 204),
        )
        job_id = None
        try:
            body = await response.json(content_type=None)
        except (ValueError, aiohttp.ContentTypeError):
            body = {}
        if isinstance(body, dict):
            job_id = body.get("JobId")
        return job_id

    async def _update(self, table_schema_name: str, key: str, data: Dict[str, Any]) -> None:
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
        await self._execute_raw(await self._build_update(table_schema_name, key, data))

    async def _update_multiple(
        self,
        entity_set: str,
        table_schema_name: str,
        records: List[Dict[str, Any]],
    ) -> None:
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
        await self._execute_raw(await self._build_update_multiple_from_records(entity_set, table_schema_name, records))
        return None

    async def _delete(self, table_schema_name: str, key: str) -> None:
        """Delete a record by GUID.

        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``
        :param key: Record GUID (with or without parentheses)
        :type key: ``str``

        :return: ``None``
        :rtype: ``None``
        """
        await self._execute_raw(await self._build_delete(table_schema_name, key))

    async def _get(
        self,
        table_schema_name: str,
        key: str,
        select: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
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
        r = await self._execute_raw(await self._build_get(table_schema_name, key, select=select))
        return await r.json(content_type=None)

    async def _get_multiple(
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
    ) -> AsyncIterator[List[Dict[str, Any]]]:
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
        :param count: If ``True``, adds ``$count=true`` to include a total record count in the response.
        :type count: ``bool``
        :param include_annotations: OData annotation pattern for the ``Prefer: odata.include-annotations`` header (e.g. ``"*"`` or ``"OData.Community.Display.V1.FormattedValue"``), or ``None``.
        :type include_annotations: ``str`` | ``None``

        :return: Async iterator yielding pages (each page is a ``list`` of record dicts).
        :rtype: ``AsyncIterator[list[dict[str, Any]]]``
        """
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
            headers = extra_headers if extra_headers else None
            r = await self._request("get", url, headers=headers, params=params)
            try:
                return await r.json(content_type=None)
            except (ValueError, aiohttp.ContentTypeError):
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

        data = await _do_request(base_url, params=params)
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
            next_link = data.get("@odata.nextLink") or data.get("odata.nextLink") if isinstance(data, dict) else None

    # --------------------------- SQL Custom API -------------------------
    async def _query_sql(self, sql: str) -> list[dict[str, Any]]:
        """Execute a read-only SQL SELECT using the Dataverse Web API ``?sql=`` capability.

        :param sql: Single SELECT statement within the supported subset.
        :type sql: ``str``

        :return: Result rows (empty list if none).
        :rtype: ``list[dict[str, Any]]``

        :raises ValidationError: If ``sql`` is not a ``str`` or is empty.
        :raises MetadataError: If logical table name resolution fails.

        .. note::
           Endpoint form: ``GET /{entity_set}?sql=<encoded select>``. The client
           extracts the logical table name, resolves the entity set (metadata
           cached), then issues the request.  ``SELECT *`` raises
           :class:`~PowerPlatform.Dataverse.core.errors.ValidationError` --
           it is deliberately rejected, not silently rewritten.
        """
        if not isinstance(sql, str):
            raise ValidationError("sql must be a string", subcode=VALIDATION_SQL_NOT_STRING)
        if not sql.strip():
            raise ValidationError("sql must be a non-empty string", subcode=VALIDATION_SQL_EMPTY)
        sql = sql.strip()

        # Apply safety guardrails (block unsupported syntax including writes,
        # warn on risky patterns). SELECT * raises ValidationError here before
        # any table resolution.
        sql = self._sql_guardrails(sql)

        r = await self._execute_raw(await self._build_sql(sql))
        try:
            body = await r.json(content_type=None)
        except (ValueError, aiohttp.ContentTypeError):
            return []

        # Collect first page
        results: list[dict[str, Any]] = []
        if isinstance(body, list):
            return [row for row in body if isinstance(row, dict)]
        if not isinstance(body, dict):
            return results

        value = body.get("value")
        if isinstance(value, list):
            results = [row for row in value if isinstance(row, dict)]

        # Follow pagination links until exhausted
        raw_link = body.get("@odata.nextLink") or body.get("odata.nextLink")
        next_link: str | None = raw_link if isinstance(raw_link, str) else None
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
                page_body = await page_resp.json(content_type=None)
            except (ValueError, aiohttp.ContentTypeError) as exc:
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

    # ---------------------- Entity set resolution -----------------------
    async def _entity_set_from_schema_name(self, table_schema_name: str) -> str:
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
        r = await self._request("get", url, params=params)
        try:
            body = await r.json(content_type=None)
            items = body.get("value", []) if isinstance(body, dict) else []
        except (ValueError, aiohttp.ContentTypeError):
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
    async def _get_entity_by_table_schema_name(
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
            "$select": "MetadataId,LogicalName,SchemaName,EntitySetName,PrimaryNameAttribute,PrimaryIdAttribute",
            "$filter": f"LogicalName eq '{logical_escaped}'",
        }
        r = await self._request("get", url, params=params, headers=headers)
        items = (await r.json(content_type=None)).get("value", [])
        return items[0] if items else None

    async def _create_entity(
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

    async def _get_attribute_metadata(
        self,
        entity_metadata_id: str,
        column_name: str,
        extra_select: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
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
        r = await self._request("get", url, params=params)
        try:
            body = await r.json(content_type=None)
        except (ValueError, aiohttp.ContentTypeError):
            return None
        items = body.get("value") if isinstance(body, dict) else None
        if isinstance(items, list) and items:
            item = items[0]
            if isinstance(item, dict):
                return item
        return None

    async def _list_columns(
        self,
        table_schema_name: str,
        *,
        select: Optional[List[str]] = None,
        filter: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """List all attribute (column) definitions for a table.

        Issues ``GET EntityDefinitions({MetadataId})/Attributes`` with optional
        ``$select`` and ``$filter`` query parameters.

        :param table_schema_name: Schema name of the table
            (e.g. ``"account"`` or ``"new_Product"``).
        :type table_schema_name: ``str``
        :param select: Optional list of property names to project via
            ``$select``.  Values are passed as-is (PascalCase).
        :type select: ``list[str]`` or ``None``
        :param filter: Optional OData ``$filter`` expression.  For example,
            ``"AttributeType eq 'String'"`` returns only string columns.
        :type filter: ``str`` or ``None``

        :return: List of raw attribute metadata dictionaries (may be empty).
        :rtype: ``list[dict[str, Any]]``

        :raises MetadataError: If the table is not found.
        :raises HttpError: If the Web API request fails.
        """
        ent = await self._get_entity_by_table_schema_name(table_schema_name)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{table_schema_name}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )
        metadata_id = ent["MetadataId"]
        url = f"{self.api}/EntityDefinitions({metadata_id})/Attributes"
        params: Dict[str, str] = {}
        if select:
            params["$select"] = ",".join(select)
        if filter:
            params["$filter"] = filter
        r = await self._request("get", url, params=params)
        return (await r.json(content_type=None)).get("value", [])

    async def _wait_for_attribute_visibility(
        self,
        entity_set: str,
        attribute_name: str,
        delays: tuple = (0, 3, 10, 20),
    ) -> None:
        """Wait for a newly created attribute to become visible in the data API.

        After creating an attribute via the metadata API, there can be a delay before
        it becomes queryable in the data API. This method polls the entity set with
        the attribute in the $select clause until it succeeds or all delays are exhausted.
        """
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

    async def _request_metadata_with_retry(self, method: str, url: str, **kwargs) -> aiohttp.ClientResponse:
        """Fetch metadata with retries on transient errors."""
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

    async def _bulk_fetch_picklists(self, table_schema_name: str) -> None:
        """Fetch all picklist attributes and their options for a table in one API call.

        Uses collection-level PicklistAttributeMetadata cast to retrieve every picklist
        attribute on the table, including its OptionSet options. Populates the nested
        cache so that ``_convert_labels_to_ints`` resolves labels without further API calls.
        The Dataverse metadata API does not page results.
        """
        table_key = self._normalize_cache_key(table_schema_name)

        # Fast path: skip the lock when the cache is already warm.
        now = time.time()
        table_entry = self._picklist_label_cache.get(table_key)
        if isinstance(table_entry, dict) and (now - table_entry.get("ts", 0)) < self._picklist_cache_ttl_seconds:
            return

        # Slow path: acquire the lock so that only one coroutine issues the metadata
        # fetch.  The second TTL check inside the lock handles the case where another
        # coroutine populated the cache while we were waiting.
        async with self._picklist_cache_lock:
            now = time.time()
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
            body = await response.json(content_type=None)
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

    async def _convert_labels_to_ints(self, table_schema_name: str, record: Dict[str, Any]) -> Dict[str, Any]:
        """Return a copy of record with any labels converted to option ints.

        Heuristic: For each string value, attempt to resolve against picklist metadata.
        If attribute isn't a picklist or label not found, value left unchanged.

        On first encounter of a table, bulk-fetches all picklist attributes and
        their options in a single API call, then resolves labels from the warm cache.
        """
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

    async def _get_table_info(self, table_schema_name: str) -> Optional[Dict[str, Any]]:
        """Return basic metadata for a custom table if it exists.

        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``

        :return: Metadata summary or ``None`` if not found.
        :rtype: ``dict[str, Any]`` | ``None``
        """
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

    async def _list_tables(
        self,
        filter: Optional[str] = None,
        select: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """List all non-private tables (``IsPrivate eq false``).

        :param filter: Optional additional OData ``$filter`` expression that is
            combined with the default ``IsPrivate eq false`` clause using
            ``and``.  For example, ``"SchemaName eq 'Account'"`` becomes
            ``"IsPrivate eq false and (SchemaName eq 'Account')"``.
            When ``None`` (the default), only the ``IsPrivate eq false`` filter
            is applied.
        :type filter: ``str`` or ``None``
        :param select: Optional list of property names to project via
            ``$select``.  Values are passed as-is (PascalCase) because
            ``EntityDefinitions`` uses PascalCase property names.
            When ``None`` (the default) or an empty list, no ``$select`` is
            applied and all properties are returned.  Passing a bare string
            raises ``TypeError``.
        :type select: ``list[str]`` or ``None``

        :return: Metadata entries for non-private tables (may be empty).
        :rtype: ``list[dict[str, Any]]``

        :raises HttpError: If the metadata request fails.
        """
        r = await self._execute_raw(self._build_list_entities(filter=filter, select=select))
        return (await r.json(content_type=None)).get("value", [])

    async def _delete_table(self, table_schema_name: str) -> None:
        """Delete a table by schema name.

        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``

        :return: ``None``
        :rtype: ``None``

        :raises MetadataError: If the table does not exist.
        :raises HttpError: If the delete request fails.
        """
        ent = await self._get_entity_by_table_schema_name(table_schema_name)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{table_schema_name}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )
        await self._execute_raw(self._build_delete_entity(ent["MetadataId"]))

    # ------------------- Alternate key metadata helpers -------------------

    async def _create_alternate_key(
        self,
        table_schema_name: str,
        key_name: str,
        columns: List[str],
        display_name_label=None,
    ) -> Dict[str, Any]:
        """Create an alternate key on a table.

        Issues ``POST EntityDefinitions(LogicalName='{logical_name}')/Keys``
        with ``EntityKeyMetadata`` payload.

        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``
        :param key_name: Schema name for the new alternate key.
        :type key_name: ``str``
        :param columns: List of column logical names that compose the key.
        :type columns: ``list[str]``
        :param display_name_label: Label for the key display name.
        :type display_name_label: ``Label`` or ``None``

        :return: Dictionary with ``metadata_id``, ``schema_name``, and ``key_attributes``.
        :rtype: ``dict[str, Any]``

        :raises MetadataError: If the table does not exist.
        :raises HttpError: If the Web API request fails.
        """
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
        r = await self._request("post", url, json=payload)
        metadata_id = self._extract_id_from_header(r.headers.get("OData-EntityId"))

        return {
            "metadata_id": metadata_id,
            "schema_name": key_name,
            "key_attributes": columns,
        }

    async def _get_alternate_keys(self, table_schema_name: str) -> List[Dict[str, Any]]:
        """List all alternate keys on a table.

        Issues ``GET EntityDefinitions(LogicalName='{logical_name}')/Keys``.

        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``

        :return: List of raw ``EntityKeyMetadata`` dictionaries.
        :rtype: ``list[dict[str, Any]]``

        :raises MetadataError: If the table does not exist.
        :raises HttpError: If the Web API request fails.
        """
        ent = await self._get_entity_by_table_schema_name(table_schema_name)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{table_schema_name}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )

        logical_name = ent.get("LogicalName", table_schema_name.lower())
        url = f"{self.api}/EntityDefinitions(LogicalName='{logical_name}')/Keys"
        r = await self._request("get", url)
        return (await r.json(content_type=None)).get("value", [])

    async def _delete_alternate_key(self, table_schema_name: str, key_id: str) -> None:
        """Delete an alternate key by metadata ID.

        Issues ``DELETE EntityDefinitions(LogicalName='{logical_name}')/Keys({key_id})``.

        :param table_schema_name: Schema name of the table.
        :type table_schema_name: ``str``
        :param key_id: Metadata GUID of the alternate key.
        :type key_id: ``str``

        :return: ``None``
        :rtype: ``None``

        :raises MetadataError: If the table does not exist.
        :raises HttpError: If the Web API request fails.
        """
        ent = await self._get_entity_by_table_schema_name(table_schema_name)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{table_schema_name}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )

        logical_name = ent.get("LogicalName", table_schema_name.lower())
        url = f"{self.api}/EntityDefinitions(LogicalName='{logical_name}')/Keys({key_id})"
        await self._request("delete", url)

    async def _create_table(
        self,
        table_schema_name: str,
        schema: Dict[str, Any],
        solution_unique_name: Optional[str] = None,
        primary_column_schema_name: Optional[str] = None,
        display_name: Optional[str] = None,
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
        :param display_name: Human-readable display name for the table. Defaults to ``table_schema_name``.
        :type display_name: ``str`` | ``None``

        :return: Metadata summary for the created table including created column schema names.
        :rtype: ``dict[str, Any]``

        :raises MetadataError: If the table already exists.
        :raises ValueError: If a column type is unsupported or ``solution_unique_name`` is empty.
        :raises TypeError: If ``solution_unique_name`` is not a ``str`` when provided.
        :raises HttpError: If underlying HTTP requests fail.
        """
        # Check if table already exists (case-insensitive)
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

        if display_name is not None:
            if not isinstance(display_name, str) or not display_name.strip():
                raise TypeError("display_name must be a non-empty string when provided")

        metadata = await self._create_entity(
            table_schema_name=table_schema_name,
            display_name=display_name if display_name is not None else table_schema_name,
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

    async def _create_columns(
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

    async def _delete_columns(
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

    # ---------------------- _build_* methods (no HTTP, but may call async helpers) ---------------

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

        Uses ``urllib.parse.quote`` (``%20`` for spaces) rather than
        ``urllib.parse.urlencode`` (``+`` for spaces).  Both are accepted by
        Dataverse and ``%20`` is the canonical RFC 3986 encoding for query-
        string values.

        :param sql: SELECT statement (non-empty string; caller is responsible
            for validation).
        """
        logical = self._extract_logical_table(sql)
        entity_set = await self._entity_set_from_schema_name(logical)
        return _RawRequest(
            method="GET",
            url=f"{self.api}/{entity_set}?sql={_url_quote(sql, safe='')}",
        )
