# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Async Dataverse Web API client with CRUD, SQL query, and table/column metadata management.

:class:`~PowerPlatform.Dataverse.data._async_odata._AsyncODataClient` inherits all pure-logic
methods from :class:`~PowerPlatform.Dataverse.data._odata._ODataClient` and overrides only the
methods that perform I/O (HTTP requests, ``time.sleep``, token acquisition) as ``async def``.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any, AsyncIterator, Dict, List, Optional, Union

from ..core._async_auth import _AsyncAuthManager
from ..core._async_http import _AsyncHttpClient
from ..core.config import DataverseConfig
from ..core.errors import HttpError
from ..core._error_codes import _http_subcode, _is_transient_status
from ._odata import _ODataClient, _CALL_SCOPE_CORRELATION_ID, _DEFAULT_EXPECTED_STATUSES, _RequestContext


class _AsyncODataClient(_ODataClient):
    """
    Async Dataverse Web API client.

    Inherits all pure-logic methods from :class:`~PowerPlatform.Dataverse.data._odata._ODataClient`
    (URL building, payload construction, cache lookups, data normalisation) and overrides only the
    three blocking operations:

    * ``_acquire_token`` — delegated to :class:`~PowerPlatform.Dataverse.core._async_auth._AsyncAuthManager`
    * HTTP I/O — delegated to :class:`~PowerPlatform.Dataverse.core._async_http._AsyncHttpClient`
    * ``time.sleep`` — replaced with ``await asyncio.sleep()``

    :param auth: Async authentication manager.
    :type auth: ~PowerPlatform.Dataverse.core._async_auth._AsyncAuthManager
    :param base_url: Organization base URL (e.g. ``"https://<org>.crm.dynamics.com"``).
    :type base_url: :class:`str`
    :param config: Optional Dataverse configuration.
    :type config: ~PowerPlatform.Dataverse.core.config.DataverseConfig | None
    """

    def __init__(
        self,
        auth: _AsyncAuthManager,
        base_url: str,
        config: Optional[DataverseConfig] = None,
    ) -> None:
        # Do NOT call super().__init__() — that would create a sync _HttpClient.
        # Replicate only the attribute setup from _ODataClient.__init__().
        self.auth = auth
        self.base_url = (base_url or "").rstrip("/")
        if not self.base_url:
            raise ValueError("base_url is required.")
        self.api = f"{self.base_url}/api/data/v9.2"
        self.config = config or DataverseConfig.from_env()
        self._async_http = _AsyncHttpClient(
            retries=self.config.http_retries,
            backoff=self.config.http_backoff,
            timeout=self.config.http_timeout,
        )
        # Set _http to None so that _ODataClient.close() is safe if called directly.
        self._http = None  # type: ignore[assignment]
        # Shared caches (inherited logic still reads/writes these)
        self._logical_to_entityset_cache: dict[str, str] = {}
        self._logical_primaryid_cache: dict[str, str] = {}
        self._picklist_label_cache: dict[Any, Any] = {}
        self._picklist_cache_ttl_seconds = 3600

    # ------------------------------------------------------------------ lifecycle

    async def close(self) -> None:  # type: ignore[override]
        """Close the async OData client and release resources.

        Clears all internal caches and closes the underlying async HTTP client.
        Safe to call multiple times.
        """
        self._logical_to_entityset_cache.clear()
        self._logical_primaryid_cache.clear()
        self._picklist_label_cache.clear()
        await self._async_http.close()

    # ------------------------------------------------------------------ auth / headers

    async def _headers(self) -> Dict[str, str]:  # type: ignore[override]
        """Build standard OData headers with bearer auth (async)."""
        scope = f"{self.base_url}/.default"
        token = (await self.auth._acquire_token(scope)).access_token
        from .. import __version__ as _SDK_VERSION

        user_agent = f"DataverseSvcPythonClient:{_SDK_VERSION}"
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "User-Agent": user_agent,
        }

    async def _merge_headers(  # type: ignore[override]
        self, headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, str]:
        base = await self._headers()
        if not headers:
            return base
        merged = base.copy()
        merged.update(headers)
        return merged

    # ------------------------------------------------------------------ raw / request

    async def _raw_request(self, method: str, url: str, **kwargs: Any) -> Any:  # type: ignore[override]
        """Execute a raw async HTTP request via _AsyncHttpClient."""
        return await self._async_http._request(method, url, **kwargs)

    async def _request(  # type: ignore[override]
        self,
        method: str,
        url: str,
        *,
        expected: tuple[int, ...] = _DEFAULT_EXPECTED_STATUSES,
        **kwargs: Any,
    ) -> Any:
        """Execute an authenticated async HTTP request with error handling.

        Merges auth headers, stamps correlation IDs, delegates to
        :meth:`_raw_request`, and raises :class:`~PowerPlatform.Dataverse.core.errors.HttpError`
        on unexpected HTTP status codes.
        """
        # Pre-await headers so we can build RequestContext without a sync callback.
        existing_headers = kwargs.pop("headers", None)
        merged = await self._merge_headers(existing_headers)
        merged.setdefault("x-ms-client-request-id", str(uuid.uuid4()))
        merged.setdefault("x-ms-correlation-id", _CALL_SCOPE_CORRELATION_ID.get())
        kwargs["headers"] = merged

        request_context = _RequestContext(
            method=method,
            url=url,
            expected=expected,
            headers=merged,
            kwargs=kwargs,
        )

        r = await self._raw_request(request_context.method, request_context.url, **request_context.kwargs)

        if r.status_code in request_context.expected:
            return r

        response_headers = getattr(r, "headers", {}) or {}
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
            correlation_id=request_context.headers.get("x-ms-correlation-id"),
            client_request_id=request_context.headers.get("x-ms-client-request-id"),
            service_request_id=request_id,
            traceparent=traceparent,
            body_excerpt=body_excerpt,
            retry_after=retry_after,
            is_transient=is_transient,
        )

    # ------------------------------------------------------------------ CRUD

    async def _create(  # type: ignore[override]
        self, entity_set: str, table_schema_name: str, record: Dict[str, Any]
    ) -> str:
        import re

        _GUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
        record = self._lowercase_keys(record)
        record = await self._convert_labels_to_ints(table_schema_name, record)
        url = f"{self.api}/{entity_set}"
        r = await self._request("post", url, json=record)

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
            f"Create response missing GUID in OData-EntityId/Location headers "
            f"(status={getattr(r, 'status_code', '?')}). Headers: {header_keys}"
        )

    async def _create_multiple(  # type: ignore[override]
        self, entity_set: str, table_schema_name: str, records: List[Dict[str, Any]]
    ) -> List[str]:
        if not all(isinstance(r, dict) for r in records):
            raise TypeError("All items for multi-create must be dicts")
        need_logical = any("@odata.type" not in r for r in records)
        logical_name = table_schema_name.lower()
        enriched: List[Dict[str, Any]] = []
        for r in records:
            r = self._lowercase_keys(r)
            r = await self._convert_labels_to_ints(table_schema_name, r)
            if "@odata.type" in r or not need_logical:
                enriched.append(r)
            else:
                nr = r.copy()
                nr["@odata.type"] = f"Microsoft.Dynamics.CRM.{logical_name}"
                enriched.append(nr)
        payload = {"Targets": enriched}
        url = f"{self.api}/{entity_set}/Microsoft.Dynamics.CRM.CreateMultiple"
        r = await self._request("post", url, json=payload)
        try:
            body = r.json() if r.text else {}
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

    async def _update(self, table_schema_name: str, key: str, data: Dict[str, Any]) -> None:  # type: ignore[override]
        data = self._lowercase_keys(data)
        data = await self._convert_labels_to_ints(table_schema_name, data)
        entity_set = await self._entity_set_from_schema_name(table_schema_name)
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        await self._request("patch", url, headers={"If-Match": "*"}, json=data)

    async def _update_multiple(  # type: ignore[override]
        self, entity_set: str, table_schema_name: str, records: List[Dict[str, Any]]
    ) -> None:
        if not isinstance(records, list) or not records or not all(isinstance(r, dict) for r in records):
            raise TypeError("records must be a non-empty list[dict]")
        need_logical = any("@odata.type" not in r for r in records)
        logical_name = table_schema_name.lower()
        enriched: List[Dict[str, Any]] = []
        for r in records:
            r = self._lowercase_keys(r)
            r = await self._convert_labels_to_ints(table_schema_name, r)
            if "@odata.type" in r or not need_logical:
                enriched.append(r)
            else:
                nr = r.copy()
                nr["@odata.type"] = f"Microsoft.Dynamics.CRM.{logical_name}"
                enriched.append(nr)
        payload = {"Targets": enriched}
        url = f"{self.api}/{entity_set}/Microsoft.Dynamics.CRM.UpdateMultiple"
        await self._request("post", url, json=payload)

    async def _delete(self, table_schema_name: str, key: str) -> None:  # type: ignore[override]
        entity_set = await self._entity_set_from_schema_name(table_schema_name)
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        await self._request("delete", url, headers={"If-Match": "*"})

    async def _get(  # type: ignore[override]
        self,
        table_schema_name: str,
        key: str,
        select: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        params = {}
        if select:
            params["$select"] = ",".join(select)
        entity_set = await self._entity_set_from_schema_name(table_schema_name)
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        r = await self._request("get", url, params=params)
        return r.json()

    async def _get_multiple(  # type: ignore[override]
        self,
        table_schema_name: str,
        select: Optional[List[str]] = None,
        filter: Optional[str] = None,
        orderby: Optional[List[str]] = None,
        top: Optional[int] = None,
        expand: Optional[List[str]] = None,
        page_size: Optional[int] = None,
    ) -> AsyncIterator[List[Dict[str, Any]]]:
        """Async generator yielding pages (list of dicts) from an entity set."""
        extra_headers: Dict[str, str] = {}
        if page_size is not None:
            ps = int(page_size)
            if ps > 0:
                extra_headers["Prefer"] = f"odata.maxpagesize={ps}"

        async def _do_request(url: str, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
            headers = extra_headers if extra_headers else None
            r = await self._request("get", url, headers=headers, params=params)
            try:
                return r.json()
            except ValueError:
                return {}

        entity_set = await self._entity_set_from_schema_name(table_schema_name)
        base_url = f"{self.api}/{entity_set}"
        params: Dict[str, Any] = {}
        if select:
            params["$select"] = ",".join(self._lowercase_list(select))
        if filter:
            params["$filter"] = filter
        if orderby:
            params["$orderby"] = ",".join(self._lowercase_list(orderby))
        if expand:
            params["$expand"] = ",".join(expand)
        if top is not None:
            params["$top"] = int(top)

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

    async def _query_sql(self, sql: str) -> list[dict[str, Any]]:  # type: ignore[override]
        from ..core.errors import ValidationError
        from ..core._error_codes import VALIDATION_SQL_NOT_STRING, VALIDATION_SQL_EMPTY

        if not isinstance(sql, str):
            raise ValidationError("sql must be a string", subcode=VALIDATION_SQL_NOT_STRING)
        if not sql.strip():
            raise ValidationError("sql must be a non-empty string", subcode=VALIDATION_SQL_EMPTY)
        sql = sql.strip()
        logical = self._extract_logical_table(sql)
        entity_set = await self._entity_set_from_schema_name(logical)
        url = f"{self.api}/{entity_set}"
        params = {"sql": sql}
        r = await self._request("get", url, params=params)
        try:
            body = r.json()
        except ValueError:
            return []
        if isinstance(body, dict):
            value = body.get("value")
            if isinstance(value, list):
                return [row for row in value if isinstance(row, dict)]
        if isinstance(body, list):
            return [row for row in body if isinstance(row, dict)]
        return []

    # ------------------------------------------------------------------ entity set resolution

    async def _entity_set_from_schema_name(self, table_schema_name: str) -> str:  # type: ignore[override]
        from ..core.errors import MetadataError
        from ..core._error_codes import (
            METADATA_ENTITYSET_NOT_FOUND,
            METADATA_ENTITYSET_NAME_MISSING,
        )

        if not table_schema_name:
            raise ValueError("table schema name required")
        cache_key = self._normalize_cache_key(table_schema_name)
        cached = self._logical_to_entityset_cache.get(cache_key)
        if cached:
            return cached
        url = f"{self.api}/EntityDefinitions"
        logical_lower = table_schema_name.lower()
        logical_escaped = self._escape_odata_quotes(logical_lower)
        params = {
            "$select": "LogicalName,EntitySetName,PrimaryIdAttribute",
            "$filter": f"LogicalName eq '{logical_escaped}'",
        }
        r = await self._request("get", url, params=params)
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
        cache_key = self._normalize_cache_key(table_schema_name)
        pid = self._logical_primaryid_cache.get(cache_key)
        if pid:
            return pid
        await self._entity_set_from_schema_name(table_schema_name)
        pid2 = self._logical_primaryid_cache.get(cache_key)
        if pid2:
            return pid2
        raise RuntimeError(
            f"PrimaryIdAttribute not resolved for table_schema_name '{table_schema_name}'. "
            "Metadata did not include PrimaryIdAttribute."
        )

    async def _update_by_ids(  # type: ignore[override]
        self,
        table_schema_name: str,
        ids: List[str],
        changes: Union[Dict[str, Any], List[Dict[str, Any]]],
    ) -> None:
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
        batch_list: List[Dict[str, Any]] = []
        for rid, patch in zip(ids, changes):
            if not isinstance(patch, dict):
                raise TypeError("Each patch must be a dict")
            batch_list.append({pk_attr: rid, **patch})
        await self._update_multiple(entity_set, table_schema_name, batch_list)
        return None

    async def _delete_multiple(self, table_schema_name: str, ids: List[str]) -> Optional[str]:  # type: ignore[override]
        from datetime import datetime, timezone

        targets = [rid for rid in ids if rid]
        if not targets:
            return None
        value_objects = [{"Value": rid, "Type": "System.Guid"} for rid in targets]
        pk_attr = await self._primary_id_attr(table_schema_name)
        timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        job_label = f"Bulk delete {table_schema_name} records @ {timestamp}"
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
        response = await self._request("post", url, json=payload, expected=(200, 202, 204))
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
        record = self._lowercase_keys(record)
        record = await self._convert_labels_to_ints(table_schema_name, record)
        key_str = self._build_alternate_key_str(alternate_key)
        url = f"{self.api}/{entity_set}({key_str})"
        await self._request("patch", url, json=record, expected=(200, 201, 204))

    async def _upsert_multiple(  # type: ignore[override]
        self,
        entity_set: str,
        table_schema_name: str,
        alternate_keys: List[Dict[str, Any]],
        records: List[Dict[str, Any]],
    ) -> None:
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

    # ------------------------------------------------------------------ table metadata helpers

    async def _get_entity_by_table_schema_name(  # type: ignore[override]
        self,
        table_schema_name: str,
        headers: Optional[Dict[str, str]] = None,
    ) -> Optional[Dict[str, Any]]:
        url = f"{self.api}/EntityDefinitions"
        logical_lower = table_schema_name.lower()
        logical_escaped = self._escape_odata_quotes(logical_lower)
        params = {
            "$select": "MetadataId,LogicalName,SchemaName,EntitySetName",
            "$filter": f"LogicalName eq '{logical_escaped}'",
        }
        r = await self._request("get", url, params=params, headers=headers)
        items = r.json().get("value", [])
        return items[0] if items else None

    async def _create_entity(  # type: ignore[override]
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

    async def _get_attribute_metadata(  # type: ignore[override]
        self,
        entity_metadata_id: str,
        column_name: str,
        extra_select: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
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
            body = r.json() if r.text else {}
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

        raise RuntimeError(
            f"Attribute '{logical_name}' did not become visible in the data API "
            f"after {total_wait} seconds (exhausted all retries)."
        ) from last_error

    async def _optionset_map(  # type: ignore[override]
        self, table_schema_name: str, attr_logical: str
    ) -> Optional[Dict[str, int]]:
        import time as _time

        if not table_schema_name or not attr_logical:
            return None
        cache_key = (self._normalize_cache_key(table_schema_name), self._normalize_cache_key(attr_logical))
        now = _time.time()
        entry = self._picklist_label_cache.get(cache_key)
        if isinstance(entry, dict) and "map" in entry and (now - entry.get("ts", 0)) < self._picklist_cache_ttl_seconds:
            return entry["map"]

        attr_esc = self._escape_odata_quotes(attr_logical.lower())
        table_schema_name_esc = self._escape_odata_quotes(table_schema_name.lower())

        url_type = (
            f"{self.api}/EntityDefinitions(LogicalName='{table_schema_name_esc}')/Attributes"
            f"?$filter=LogicalName eq '{attr_esc}'&$select=LogicalName,AttributeType"
        )
        r_type = None
        max_attempts = 5
        backoff_seconds = 0.4
        for attempt in range(1, max_attempts + 1):
            try:
                r_type = await self._request("get", url_type)
                break
            except HttpError as err:
                if getattr(err, "status_code", None) == 404:
                    if attempt < max_attempts:
                        await asyncio.sleep(backoff_seconds * (2 ** (attempt - 1)))
                        continue
                    raise RuntimeError(
                        f"Picklist attribute metadata not found after retries: "
                        f"entity='{table_schema_name}' attribute='{attr_logical}' (404)"
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

        cast_url = (
            f"{self.api}/EntityDefinitions(LogicalName='{table_schema_name_esc}')"
            f"/Attributes(LogicalName='{attr_esc}')/"
            "Microsoft.Dynamics.CRM.PicklistAttributeMetadata?$select=LogicalName&$expand=OptionSet($select=Options)"
        )
        r_opts = None
        for attempt in range(1, max_attempts + 1):
            try:
                r_opts = await self._request("get", cast_url)
                break
            except HttpError as err:
                if getattr(err, "status_code", None) == 404:
                    if attempt < max_attempts:
                        await asyncio.sleep(backoff_seconds * (2 ** (attempt - 1)))
                        continue
                    raise RuntimeError(
                        f"Picklist OptionSet metadata not found after retries: "
                        f"entity='{table_schema_name}' attribute='{attr_logical}' (404)"
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
        self._picklist_label_cache[cache_key] = {"map": {}, "ts": now}
        return {}

    async def _convert_labels_to_ints(  # type: ignore[override]
        self, table_schema_name: str, record: Dict[str, Any]
    ) -> Dict[str, Any]:
        out = record.copy()
        for k, v in list(out.items()):
            if not isinstance(v, str) or not v.strip():
                continue
            mapping = await self._optionset_map(table_schema_name, k)
            if not mapping:
                continue
            norm = self._normalize_picklist_label(v)
            val = mapping.get(norm)
            if val is not None:
                out[k] = val
        return out

    # ------------------------------------------------------------------ table CRUD

    async def _get_table_info(self, table_schema_name: str) -> Optional[Dict[str, Any]]:  # type: ignore[override]
        ent = await self._get_entity_by_table_schema_name(table_schema_name)
        if not ent:
            return None
        return {
            "table_schema_name": ent.get("SchemaName") or table_schema_name,
            "table_logical_name": ent.get("LogicalName"),
            "entity_set_name": ent.get("EntitySetName"),
            "metadata_id": ent.get("MetadataId"),
            "columns_created": [],
        }

    async def _list_tables(  # type: ignore[override]
        self,
        filter: Optional[str] = None,
        select: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        url = f"{self.api}/EntityDefinitions"
        base_filter = "IsPrivate eq false"
        combined_filter = f"{base_filter} and ({filter})" if filter else base_filter
        params: Dict[str, str] = {"$filter": combined_filter}
        if select is not None and isinstance(select, str):
            raise TypeError("select must be a list of property names, not a bare string")
        if select:
            params["$select"] = ",".join(select)
        r = await self._request("get", url, params=params)
        return r.json().get("value", [])

    async def _delete_table(self, table_schema_name: str) -> None:  # type: ignore[override]
        from ..core.errors import MetadataError
        from ..core._error_codes import METADATA_TABLE_NOT_FOUND

        ent = await self._get_entity_by_table_schema_name(table_schema_name)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{table_schema_name}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )
        metadata_id = ent["MetadataId"]
        url = f"{self.api}/EntityDefinitions({metadata_id})"
        await self._request("delete", url)

    async def _create_table(  # type: ignore[override]
        self,
        table_schema_name: str,
        schema: Dict[str, Any],
        solution_unique_name: Optional[str] = None,
        primary_column_schema_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        from ..core.errors import MetadataError
        from ..core._error_codes import METADATA_TABLE_ALREADY_EXISTS

        ent = await self._get_entity_by_table_schema_name(table_schema_name)
        if ent:
            raise MetadataError(
                f"Table '{table_schema_name}' already exists.",
                subcode=METADATA_TABLE_ALREADY_EXISTS,
            )
        created_cols: List[str] = []
        if primary_column_schema_name:
            primary_attr_schema = primary_column_schema_name
        else:
            primary_attr_schema = (
                f"{table_schema_name.split('_', 1)[0]}_Name" if "_" in table_schema_name else "new_Name"
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
            "columns_created": created_cols,
        }

    async def _create_columns(  # type: ignore[override]
        self, table_schema_name: str, columns: Dict[str, Any]
    ) -> List[str]:
        from ..core.errors import MetadataError
        from ..core._error_codes import METADATA_TABLE_NOT_FOUND

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
            payload = self._attribute_payload(column_name, column_type)
            if not payload:
                raise ValueError(f"Unsupported column type '{column_type}' for '{column_name}'.")
            url = f"{self.api}/EntityDefinitions({metadata_id})/Attributes"
            await self._request("post", url, json=payload)
            created.append(column_name)
            if "OptionSet" in payload:
                needs_picklist_flush = True
        if needs_picklist_flush:
            self._flush_cache("picklist")
        return created

    async def _delete_columns(  # type: ignore[override]
        self, table_schema_name: str, columns: Union[str, List[str]]
    ) -> List[str]:
        from ..core.errors import MetadataError
        from ..core._error_codes import METADATA_TABLE_NOT_FOUND, METADATA_COLUMN_NOT_FOUND

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
            attr_url = f"{self.api}/EntityDefinitions({metadata_id})/Attributes({attr_metadata_id})"
            await self._request("delete", attr_url, headers={"If-Match": "*"})
            attr_type = attr_meta.get("@odata.type") or attr_meta.get("AttributeType")
            if isinstance(attr_type, str):
                attr_type_l = attr_type.lower()
                if "picklist" in attr_type_l or "optionset" in attr_type_l:
                    needs_picklist_flush = True
            deleted.append(column_name)
        if needs_picklist_flush:
            self._flush_cache("picklist")
        return deleted

    # ------------------------------------------------------------------ alternate keys

    async def _create_alternate_key(  # type: ignore[override]
        self,
        table_schema_name: str,
        key_name: str,
        columns: List[str],
        display_name_label: Any = None,
    ) -> Dict[str, Any]:
        from ..core.errors import MetadataError
        from ..core._error_codes import METADATA_TABLE_NOT_FOUND

        ent = await self._get_entity_by_table_schema_name(table_schema_name)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{table_schema_name}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )
        logical_name = ent.get("LogicalName", table_schema_name.lower())
        url = f"{self.api}/EntityDefinitions(LogicalName='{logical_name}')/Keys"
        payload: Dict[str, Any] = {"SchemaName": key_name, "KeyAttributes": columns}
        if display_name_label is not None:
            payload["DisplayName"] = display_name_label.to_dict()
        r = await self._request("post", url, json=payload)
        metadata_id = self._extract_id_from_header(r.headers.get("OData-EntityId"))
        return {"metadata_id": metadata_id, "schema_name": key_name, "key_attributes": columns}

    async def _get_alternate_keys(self, table_schema_name: str) -> List[Dict[str, Any]]:  # type: ignore[override]
        from ..core.errors import MetadataError
        from ..core._error_codes import METADATA_TABLE_NOT_FOUND

        ent = await self._get_entity_by_table_schema_name(table_schema_name)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{table_schema_name}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )
        logical_name = ent.get("LogicalName", table_schema_name.lower())
        url = f"{self.api}/EntityDefinitions(LogicalName='{logical_name}')/Keys"
        r = await self._request("get", url)
        return r.json().get("value", [])

    async def _delete_alternate_key(self, table_schema_name: str, key_id: str) -> None:  # type: ignore[override]
        from ..core.errors import MetadataError
        from ..core._error_codes import METADATA_TABLE_NOT_FOUND

        ent = await self._get_entity_by_table_schema_name(table_schema_name)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{table_schema_name}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )
        logical_name = ent.get("LogicalName", table_schema_name.lower())
        url = f"{self.api}/EntityDefinitions(LogicalName='{logical_name}')/Keys({key_id})"
        await self._request("delete", url)

    # ------------------------------------------------------------------ relationship mixins (async overrides)

    async def _create_one_to_many_relationship(  # type: ignore[override]
        self, lookup: Any, relationship: Any, solution: Optional[str] = None
    ) -> Dict[str, Any]:
        url = f"{self.api}/RelationshipDefinitions"
        payload = relationship.to_dict()
        payload["Lookup"] = lookup.to_dict()
        headers = (await self._headers()).copy()
        if solution:
            headers["MSCRM.SolutionUniqueName"] = solution
        r = await self._request("post", url, headers=headers, json=payload)
        relationship_id = self._extract_id_from_header(r.headers.get("OData-EntityId"))
        return {
            "relationship_id": relationship_id,
            "relationship_schema_name": relationship.schema_name,
            "lookup_schema_name": lookup.schema_name,
            "referenced_entity": relationship.referenced_entity,
            "referencing_entity": relationship.referencing_entity,
        }

    async def _create_many_to_many_relationship(  # type: ignore[override]
        self, relationship: Any, solution: Optional[str] = None
    ) -> Dict[str, Any]:
        url = f"{self.api}/RelationshipDefinitions"
        payload = relationship.to_dict()
        headers = (await self._headers()).copy()
        if solution:
            headers["MSCRM.SolutionUniqueName"] = solution
        r = await self._request("post", url, headers=headers, json=payload)
        relationship_id = self._extract_id_from_header(r.headers.get("OData-EntityId"))
        return {
            "relationship_id": relationship_id,
            "relationship_schema_name": relationship.schema_name,
            "entity1_logical_name": relationship.entity1_logical_name,
            "entity2_logical_name": relationship.entity2_logical_name,
        }

    async def _delete_relationship(self, relationship_id: str) -> None:  # type: ignore[override]
        url = f"{self.api}/RelationshipDefinitions({relationship_id})"
        headers = (await self._headers()).copy()
        headers["If-Match"] = "*"
        await self._request("delete", url, headers=headers)

    async def _get_relationship(self, schema_name: str) -> Optional[Dict[str, Any]]:  # type: ignore[override]
        url = f"{self.api}/RelationshipDefinitions"
        params = {"$filter": f"SchemaName eq '{self._escape_odata_quotes(schema_name)}'"}
        r = await self._request("get", url, headers=await self._headers(), params=params)
        data = r.json()
        results = data.get("value", [])
        return results[0] if results else None

    # ------------------------------------------------------------------ file upload mixins (async overrides)

    async def _upload_file(  # type: ignore[override]
        self,
        table_schema_name: str,
        record_id: str,
        file_name_attribute: str,
        path: str,
        mode: Optional[str] = None,
        mime_type: Optional[str] = None,
        if_none_match: bool = True,
    ) -> None:
        import os

        entity_set = await self._entity_set_from_schema_name(table_schema_name)
        entity_metadata = await self._get_entity_by_table_schema_name(table_schema_name)
        if entity_metadata:
            metadata_id = entity_metadata.get("MetadataId")
            if metadata_id:
                attr_metadata = await self._get_attribute_metadata(metadata_id, file_name_attribute)
                if not attr_metadata:
                    await self._create_columns(table_schema_name, {file_name_attribute: "file"})
                    await self._wait_for_attribute_visibility(entity_set, file_name_attribute)

        mode = (mode or "auto").lower()
        if mode == "auto":
            if not os.path.isfile(path):
                raise FileNotFoundError(f"File not found: {path}")
            size = os.path.getsize(path)
            mode = "small" if size < 128 * 1024 * 1024 else "chunk"

        logical_name = file_name_attribute.lower()
        if mode == "small":
            return await self._upload_file_small(
                entity_set, record_id, logical_name, path, content_type=mime_type, if_none_match=if_none_match
            )
        if mode == "chunk":
            return await self._upload_file_chunk(entity_set, record_id, logical_name, path, if_none_match=if_none_match)
        raise ValueError(f"Invalid mode '{mode}'. Use 'auto', 'small', or 'chunk'.")

    async def _upload_file_small(  # type: ignore[override]
        self,
        entity_set: str,
        record_id: str,
        file_name_attribute: str,
        path: str,
        content_type: Optional[str] = None,
        if_none_match: bool = True,
    ) -> None:
        import os

        if not record_id:
            raise ValueError("record_id required")
        if not os.path.isfile(path):
            raise FileNotFoundError(f"File not found: {path}")
        size = os.path.getsize(path)
        limit = 128 * 1024 * 1024
        if size > limit:
            raise ValueError(f"File size {size} exceeds single-upload limit {limit}; use chunk mode.")
        with open(path, "rb") as fh:
            data = fh.read()
        fname = os.path.basename(path)
        key = self._format_key(record_id)
        url = f"{self.api}/{entity_set}{key}/{file_name_attribute}"
        headers = {
            "Content-Type": content_type or "application/octet-stream",
            "x-ms-file-name": fname,
        }
        if if_none_match:
            headers["If-None-Match"] = "null"
        else:
            headers["If-Match"] = "*"
        await self._request("patch", url, headers=headers, data=data)
        return None

    async def _upload_file_chunk(  # type: ignore[override]
        self,
        entity_set: str,
        record_id: str,
        file_name_attribute: str,
        path: str,
        if_none_match: bool = True,
    ) -> None:
        import math
        import os
        from urllib.parse import quote

        if not record_id:
            raise ValueError("record_id required")
        if not os.path.isfile(path):
            raise FileNotFoundError(f"File not found: {path}")
        total_size = os.path.getsize(path)
        fname = os.path.basename(path)
        key = self._format_key(record_id)
        init_url = f"{self.api}/{entity_set}{key}/{file_name_attribute}?x-ms-file-name={quote(fname)}"
        headers: Dict[str, str] = {"x-ms-transfer-mode": "chunked"}
        if if_none_match:
            headers["If-None-Match"] = "null"
        else:
            headers["If-Match"] = "*"
        r_init = await self._request("patch", init_url, headers=headers, data=b"")
        location = r_init.headers.get("Location") or r_init.headers.get("location")
        if not location:
            raise RuntimeError("Missing Location header with sessiontoken for chunked upload")
        rec_hdr = r_init.headers.get("x-ms-chunk-size") or r_init.headers.get("X-MS-CHUNK-SIZE")
        try:
            recommended_size = int(rec_hdr) if rec_hdr else None
        except Exception:
            recommended_size = None
        effective_size = recommended_size or (4 * 1024 * 1024)
        if effective_size <= 0:
            raise ValueError("effective chunk size must be positive")
        total_chunks = int(math.ceil(total_size / effective_size)) if total_size else 1
        uploaded_bytes = 0
        with open(path, "rb") as fh:
            for _idx in range(total_chunks):
                chunk = fh.read(effective_size)
                if not chunk:
                    break
                start = uploaded_bytes
                end = start + len(chunk) - 1
                c_headers = {
                    "x-ms-file-name": fname,
                    "Content-Type": "application/octet-stream",
                    "Content-Range": f"bytes {start}-{end}/{total_size}",
                    "Content-Length": str(len(chunk)),
                }
                await self._request("patch", location, headers=c_headers, data=chunk, expected=(206, 204))
                uploaded_bytes += len(chunk)
        return None
