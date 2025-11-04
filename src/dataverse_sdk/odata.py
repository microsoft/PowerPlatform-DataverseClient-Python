from __future__ import annotations

from typing import Any, Dict, Optional, List, Union, Iterable
from enum import Enum
import unicodedata
import time
import re
import json
import importlib.resources as ir

from .http import HttpClient
from .odata_upload_files import ODataFileUpload
from .errors import *
from . import error_codes as ec


_GUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")


class ODataClient(ODataFileUpload):
    """Dataverse Web API client: CRUD, SQL-over-API, and table metadata helpers."""

    @staticmethod
    def _escape_odata_quotes(value: str) -> str:
        """Escape single quotes for OData queries (by doubling them)."""
        return value.replace("'", "''")

    def __init__(
        self,
        auth,
        base_url: str,
        config=None,
    ) -> None:
        self.auth = auth
        self.base_url = (base_url or "").rstrip("/")
        if not self.base_url:
            raise ValueError("base_url is required.")
        self.api = f"{self.base_url}/api/data/v9.2"
        self.config = config or __import__("dataverse_sdk.config", fromlist=["DataverseConfig"]).DataverseConfig.from_env()
        self._http = HttpClient(
            retries=self.config.http_retries,
            backoff=self.config.http_backoff,
            timeout=self.config.http_timeout,
        )
        # Cache: logical name -> entity set name (plural) resolved from metadata
        self._logical_to_entityset_cache: dict[str, str] = {}
        # Cache: logical name -> primary id attribute (e.g. accountid)
        self._logical_primaryid_cache: dict[str, str] = {}
        # Picklist label cache: (logical_name, attribute_logical) -> {'map': {...}, 'ts': epoch_seconds}
        self._picklist_label_cache = {}
        self._picklist_cache_ttl_seconds = 3600  # 1 hour TTL

    def _headers(self) -> Dict[str, str]:
        """Build standard OData headers with bearer auth."""
        scope = f"{self.base_url}/.default"
        token = self.auth.acquire_token(scope).access_token
        # TODO: add version to User-Agent
        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "User-Agent": "DataversePythonSDK",
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
        subcode = ec.http_subcode(sc)
        correlation_id = headers.get("x-ms-correlation-request-id") or headers.get("x-ms-correlation-id")
        request_id = headers.get("x-ms-client-request-id") or headers.get("request-id") or headers.get("x-ms-request-id")
        traceparent = headers.get("traceparent")
        ra = headers.get("Retry-After")
        retry_after = None
        if ra:
            try:
                retry_after = int(ra)
            except Exception:
                retry_after = None
        is_transient = ec.is_transient_status(sc)
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
    def _create(self, entity_set: str, logical_name: str, record: Dict[str, Any]) -> str:
        """Create a single record and return its GUID.

        Parameters
        -------
        entity_set : str
            Resolved entity set (plural) name.
        logical_name : str
            Singular logical entity name.
        record : dict[str, Any]
            Attribute payload mapped by logical column names.

        Returns
        -------
        str
            Created record GUID.

        Notes
        -------
        Relies on OData-EntityId (canonical) or Location header. No response body parsing is performed.
        Raises RuntimeError if neither header contains a GUID.
        """
        record = self._convert_labels_to_ints(logical_name, record)
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

    def _create_multiple(self, entity_set: str, logical_name: str, records: List[Dict[str, Any]]) -> List[str]:
        """Create multiple records using the collection-bound CreateMultiple action.

        Parameters
        ----------
        entity_set : str
            Resolved entity set (plural) name.
        logical_name : str
            Singular logical entity name.
        records : list[dict[str, Any]]
            Payloads mapped by logical attribute names.

        Multi-create logical name resolution
        ------------------------------------
        - If any payload omits ``@odata.type`` the client stamps ``Microsoft.Dynamics.CRM.<logical_name>``.
        - If all payloads already include ``@odata.type`` no modification occurs.
        
        Returns
        -------
        list[str]
            List of created IDs.
        """
        if not all(isinstance(r, dict) for r in records):
            raise TypeError("All items for multi-create must be dicts")
        need_logical = any("@odata.type" not in r for r in records)
        enriched: List[Dict[str, Any]] = []
        for r in records:
            r = self._convert_labels_to_ints(logical_name, r)
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
    def _primary_id_attr(self, logical_name: str) -> str:
        """Return primary key attribute using metadata; error if unavailable."""
        pid = self._logical_primaryid_cache.get(logical_name)
        if pid:
            return pid
        # Resolve metadata (populates _logical_primaryid_cache or raises if logical unknown)
        self._entity_set_from_logical(logical_name)
        pid2 = self._logical_primaryid_cache.get(logical_name)
        if pid2:
            return pid2
        raise RuntimeError(
            f"PrimaryIdAttribute not resolved for logical name '{logical_name}'. Metadata did not include PrimaryIdAttribute."
        )

    def _update_by_ids(self, logical_name: str, ids: List[str], changes: Union[Dict[str, Any], List[Dict[str, Any]]]) -> None:
        """Update many records by GUID list using UpdateMultiple under the hood.

        Parameters
        ----------
        logical_name : str
            Logical name (singular).
        ids : list[str]
            GUIDs of target records.
        changes : dict | list[dict]
            Broadcast patch (dict) applied to all IDs, or list of per-record patches (1:1 with ids).
        """
        if not isinstance(ids, list):
            raise TypeError("ids must be list[str]")
        if not ids:
            return None
        pk_attr = self._primary_id_attr(logical_name)
        entity_set = self._entity_set_from_logical(logical_name)
        if isinstance(changes, dict):
            batch = [{pk_attr: rid, **changes} for rid in ids]
            self._update_multiple(entity_set, logical_name, batch)
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
        self._update_multiple(entity_set, logical_name, batch)
        return None

    def _delete_multiple(self, logical_name: str, ids: List[str]) -> None:
        """Delete many records by GUID list (simple loop; potential future optimization point)."""
        if not isinstance(ids, list):
            raise TypeError("ids must be list[str]")
        for rid in ids:
            self._delete(logical_name, rid)
        return None

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

    def _update(self, logical_name: str, key: str, data: Dict[str, Any]) -> None:
        """Update an existing record.

        Parameters
        ----------
        logical_name : str
            Logical (singular) entity name.
        key : str
            Record GUID (with or without parentheses) or alternate key.
        data : dict
            Partial entity payload.

        Returns
        -------
        None
        """
        data = self._convert_labels_to_ints(logical_name, data)
        entity_set = self._entity_set_from_logical(logical_name)
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        r = self._request("patch", url, headers={"If-Match": "*"}, json=data)

    def _update_multiple(self, entity_set: str, logical_name: str, records: List[Dict[str, Any]]) -> None:
        """Bulk update existing records via the collection-bound UpdateMultiple action.

        Parameters
        ----------
        entity_set : str
            Resolved entity set name.
        logical_name : str
            Logical (singular) name, e.g. "account".
        records : list[dict]
            Each dict must include the real primary key attribute for the entity (e.g. ``accountid``) and one or more
            fields to update. If ``@odata.type`` is omitted in any payload, the logical name is resolved once and
            stamped into those payloads as ``Microsoft.Dynamics.CRM.<logical>`` (same behaviour as bulk create).

        Behaviour
        ---------
        - POST ``/{entity_set}/Microsoft.Dynamics.CRM.UpdateMultiple`` with body ``{"Targets": [...]}``.
        - Expects Dataverse transactional semantics: if any individual update fails the entire request is rolled back.
        - Response content is ignored; no stable contract for returned IDs or representations.

        Returns
        -------
        None
            No representation is returned (symmetry with single update).

        Notes
        -----
        - Caller must include the correct primary key attribute (e.g. ``accountid``) in every record.
        - Both single and multiple updates return None.
        """
        if not isinstance(records, list) or not records or not all(isinstance(r, dict) for r in records):
            raise TypeError("records must be a non-empty list[dict]")

        # Determine whether we need logical name resolution (@odata.type missing in any payload)
        need_logical = any("@odata.type" not in r for r in records)
        enriched: List[Dict[str, Any]] = []
        for r in records:
            r = self._convert_labels_to_ints(logical_name, r)
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

    def _delete(self, logical_name: str, key: str) -> None:
        """Delete a record by GUID or alternate key."""
        entity_set = self._entity_set_from_logical(logical_name)
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        self._request("delete", url, headers={"If-Match": "*"})

    def _get(self, logical_name: str, key: str, select: Optional[str] = None) -> Dict[str, Any]:
        """Retrieve a single record.

        Parameters
        ----------
        logical_name : str
            Logical (singular) name.
        key : str
            Record GUID (with or without parentheses) or alternate key syntax.
        select : str | None
            Comma separated columns for $select.
        """
        params = {}
        if select:
            params["$select"] = select
        entity_set = self._entity_set_from_logical(logical_name)
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        r = self._request("get", url, params=params)
        return r.json()

    def _get_multiple(
        self,
        logical_name: str,
        select: Optional[List[str]] = None,
        filter: Optional[str] = None,
        orderby: Optional[List[str]] = None,
        top: Optional[int] = None,
        expand: Optional[List[str]] = None,
        page_size: Optional[int] = None,
    ) -> Iterable[List[Dict[str, Any]]]:
        """Iterate records from an entity set, yielding one page (list of dicts) at a time.

        Parameters
        ----------
        logical_name : str
            Logical (singular) entity name.
        select : list[str] | None
            Columns to select; joined with commas into $select.
        filter : str | None
            OData $filter expression as a string.
        orderby : list[str] | None
            Order expressions; joined with commas into $orderby.
        top : int | None
            Max number of records across all pages. Passed as $top on the first request; the server will paginate via nextLink as needed.
        expand : list[str] | None
            Navigation properties to expand; joined with commas into $expand.
        page_size : int | None
            Hint for per-page size using Prefer: ``odata.maxpagesize``.

        Yields
        ------
        list[dict]
            A page of records from the Web API (the "value" array for each page).
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

        entity_set = self._entity_set_from_logical(logical_name)
        base_url = f"{self.api}/{entity_set}"
        params: Dict[str, Any] = {}
        if select:
            params["$select"] = ",".join(select)
        if filter:
            params["$filter"] = filter
        if orderby:
            params["$orderby"] = ",".join(orderby)
        if expand:
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
        """Execute a read-only SQL query using the Dataverse Web API `?sql=` capability.

        The platform supports a constrained subset of SQL SELECT statements directly on entity set endpoints:
            GET /{entity_set}?sql=<encoded select statement>

        This client extracts the logical table name from the query, resolves the corresponding
        entity set name (cached) and invokes the Web API using the `sql` query parameter.

        Parameters
        ----------
        sql : str
            Single SELECT statement within supported subset.

        Returns
        -------
        list[dict]
            Result rows (empty list if none).

        Raises
        ------
        ValueError
            If the SQL is empty or malformed, or if the table logical name cannot be determined.
        RuntimeError
            If metadata lookup for the logical name fails.
        """
        if not isinstance(sql, str):
            raise ValidationError("sql must be a string", subcode=ec.VALIDATION_SQL_NOT_STRING)
        if not sql.strip():
            raise ValidationError("sql must be a non-empty string", subcode=ec.VALIDATION_SQL_EMPTY)
        sql = sql.strip()

        # Extract logical table name via helper (robust to identifiers ending with 'from')
        logical = self._extract_logical_table(sql)

        entity_set = self._entity_set_from_logical(logical)
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
    def _entity_set_from_logical(self, logical: str) -> str:
        """Resolve entity set name (plural) from a logical (singular) name using metadata.

        Caches results for subsequent SQL queries.
        """
        if not logical:
            raise ValueError("logical name required")
        cached = self._logical_to_entityset_cache.get(logical)
        if cached:
            return cached
        url = f"{self.api}/EntityDefinitions"
        logical_escaped = self._escape_odata_quotes(logical)
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
            plural_hint = " (did you pass a plural entity set name instead of the singular logical name?)" if logical.endswith("s") and not logical.endswith("ss") else ""
            raise MetadataError(
                f"Unable to resolve entity set for logical name '{logical}'. Provide the singular logical name.{plural_hint}",
                subcode=ec.METADATA_ENTITYSET_NOT_FOUND,
            )
        md = items[0]
        es = md.get("EntitySetName")
        if not es:
            raise MetadataError(
                f"Metadata response missing EntitySetName for logical '{logical}'.",
                subcode=ec.METADATA_ENTITYSET_NAME_MISSING,
            )
        self._logical_to_entityset_cache[logical] = es
        primary_id_attr = md.get("PrimaryIdAttribute")
        if isinstance(primary_id_attr, str) and primary_id_attr:
            self._logical_primaryid_cache[logical] = primary_id_attr
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

    def _normalize_entity_schema(self, tablename: str) -> str:
        if "_" in tablename:
            return tablename
        return f"new_{self._to_pascal(tablename)}"

    def _get_entity_by_schema(self, schema_name: str) -> Optional[Dict[str, Any]]:
        url = f"{self.api}/EntityDefinitions"
        # Escape single quotes in schema name
        schema_escaped = self._escape_odata_quotes(schema_name)
        params = {
            "$select": "MetadataId,LogicalName,SchemaName,EntitySetName",
            "$filter": f"SchemaName eq '{schema_escaped}'",
        }
        r = self._request("get", url, params=params)
        items = r.json().get("value", [])
        return items[0] if items else None

    def _create_entity(self, schema_name: str, display_name: str, attributes: List[Dict[str, Any]]) -> str:
        url = f"{self.api}/EntityDefinitions"
        payload = {
            "@odata.type": "Microsoft.Dynamics.CRM.EntityMetadata",
            "SchemaName": schema_name,
            "DisplayName": self._label(display_name),
            "DisplayCollectionName": self._label(display_name + "s"),
            "Description": self._label(f"Custom entity for {display_name}"),
            "OwnershipType": "UserOwned",
            "HasActivities": False,
            "HasNotes": True,
            "IsActivity": False,
            "Attributes": attributes,
        }
        r = self._request("post", url, json=payload)
        ent = self._wait_for_entity_ready(schema_name)
        if not ent or not ent.get("EntitySetName"):
            raise RuntimeError(
                f"Failed to create or retrieve entity '{schema_name}' (EntitySetName not available)."
            )
        return ent["MetadataId"]

    def _wait_for_entity_ready(self, schema_name: str, delays: Optional[List[int]] = None) -> Optional[Dict[str, Any]]:
        import time
        delays = delays or [0, 2, 5, 10, 20, 30]
        ent: Optional[Dict[str, Any]] = None
        for idx, delay in enumerate(delays):
            if idx > 0 and delay > 0:
                time.sleep(delay)
            ent = self._get_entity_by_schema(schema_name)
            if ent and ent.get("EntitySetName"):
                return ent
        return ent

    def _normalize_attribute_schema(self, entity_schema: str, column_name: str) -> str:
        # Use same publisher prefix segment as entity_schema if present; else default to 'new_'.
        if not isinstance(column_name, str) or not column_name.strip():
            raise ValueError("column_name must be a non-empty string")
        publisher = entity_schema.split("_", 1)[0] if "_" in entity_schema else "new"
        expected_prefix = f"{publisher}_"
        if column_name.lower().startswith(expected_prefix.lower()):
            return column_name
        return f"{publisher}_{self._to_pascal(column_name)}"

    def _get_attribute_metadata(
        self,
        entity_metadata_id: str,
        schema_name: str,
        extra_select: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        attr_escaped = self._escape_odata_quotes(schema_name)
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
            locs.append({
                "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                "Label": text,
                "LanguageCode": lang,
            })
        if not locs:
            raise ValueError("At least one translation required")
        return {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": locs,
        }

    def _enum_optionset_payload(self, schema_name: str, enum_cls: type[Enum], is_primary_name: bool = False) -> Dict[str, Any]:
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
            options.append({
                "@odata.type": "Microsoft.Dynamics.CRM.OptionMetadata",
                "Value": m.value,
                "Label": self._build_localizedlabels_payload(per_lang),
            })

        attr_label = schema_name.split("_")[-1]
        return {
            "@odata.type": "Microsoft.Dynamics.CRM.PicklistAttributeMetadata",
            "SchemaName": schema_name,
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

    def _optionset_map(self, logical_name: str, attr_logical: str) -> Optional[Dict[str, int]]:
        """Build or return cached mapping of normalized label -> value for a picklist attribute.

        Returns empty dict if attribute is not a picklist or has no options. Returns None only
        for invalid inputs or unexpected metadata parse failures.

        Notes
        -----
        - This method calls the Web API twice per attribute so it could have perf impact when there are lots of columns on the entity.
        """
        if not logical_name or not attr_logical:
            return None
        cache_key = (logical_name, attr_logical.lower())
        now = time.time()
        entry = self._picklist_label_cache.get(cache_key)
        if isinstance(entry, dict) and 'map' in entry and (now - entry.get('ts', 0)) < self._picklist_cache_ttl_seconds:
            return entry['map']

        attr_esc = self._escape_odata_quotes(attr_logical)
        logical_esc = self._escape_odata_quotes(logical_name)

        # Step 1: lightweight fetch (no expand) to determine attribute type
        url_type = (
            f"{self.api}/EntityDefinitions(LogicalName='{logical_esc}')/Attributes"
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
                        time.sleep(0.4 * (2 ** attempt))
                        continue
                    raise RuntimeError(
                        f"Picklist attribute metadata not found after retries: entity='{logical_name}' attribute='{attr_logical}' (404)"
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
            self._picklist_label_cache[cache_key] = {'map': {}, 'ts': now}
            return {}

        # Step 2: fetch with expand only now that we know it's a picklist
        # Need to cast to the derived PicklistAttributeMetadata type; OptionSet is not a nav on base AttributeMetadata.
        cast_url = (
            f"{self.api}/EntityDefinitions(LogicalName='{logical_esc}')/Attributes(LogicalName='{attr_esc}')/"
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
                        time.sleep(0.4 * (2 ** attempt))  # 0.4s, 0.8s
                        continue
                    raise RuntimeError(
                        f"Picklist OptionSet metadata not found after retries: entity='{logical_name}' attribute='{attr_logical}' (404)"
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
            self._picklist_label_cache[cache_key] = {'map': mapping, 'ts': now}
            return mapping
        # No options available
        self._picklist_label_cache[cache_key] = {'map': {}, 'ts': now}
        return {}

    def _convert_labels_to_ints(self, logical_name: str, record: Dict[str, Any]) -> Dict[str, Any]:
        """Return a copy of record with any labels converted to option ints.

        Heuristic: For each string value, attempt to resolve against picklist metadata.
        If attribute isn't a picklist or label not found, value left unchanged.
        """
        out = record.copy()
        for k, v in list(out.items()):
            if not isinstance(v, str) or not v.strip():
                continue
            mapping = self._optionset_map(logical_name, k)
            if not mapping:
                continue
            norm = self._normalize_picklist_label(v)
            val = mapping.get(norm)
            if val is not None:
                out[k] = val
        return out

    def _attribute_payload(self, schema_name: str, dtype: Any, *, is_primary_name: bool = False) -> Optional[Dict[str, Any]]:
        # Enum-based local option set support
        if isinstance(dtype, type) and issubclass(dtype, Enum):
            return self._enum_optionset_payload(schema_name, dtype, is_primary_name=is_primary_name)
        if not isinstance(dtype, str):
            raise ValueError(f"Unsupported column spec type for '{schema_name}': {type(dtype)} (expected str or Enum subclass)")
        dtype_l = dtype.lower().strip()
        label = schema_name.split("_")[-1]
        if dtype_l in ("string", "text"):
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata",
                "SchemaName": schema_name,
                "DisplayName": self._label(label),
                "RequiredLevel": {"Value": "None"},
                "MaxLength": 200,
                "FormatName": {"Value": "Text"},
                "IsPrimaryName": bool(is_primary_name),
            }
        if dtype_l in ("int", "integer"):
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.IntegerAttributeMetadata",
                "SchemaName": schema_name,
                "DisplayName": self._label(label),
                "RequiredLevel": {"Value": "None"},
                "Format": "None",
                "MinValue": -2147483648,
                "MaxValue": 2147483647,
            }
        if dtype_l in ("decimal", "money"):
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.DecimalAttributeMetadata",
                "SchemaName": schema_name,
                "DisplayName": self._label(label),
                "RequiredLevel": {"Value": "None"},
                "MinValue": -100000000000.0,
                "MaxValue": 100000000000.0,
                "Precision": 2,
            }
        if dtype_l in ("float", "double"):
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.DoubleAttributeMetadata",
                "SchemaName": schema_name,
                "DisplayName": self._label(label),
                "RequiredLevel": {"Value": "None"},
                "MinValue": -100000000000.0,
                "MaxValue": 100000000000.0,
                "Precision": 2,
            }
        if dtype_l in ("datetime", "date"):
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata",
                "SchemaName": schema_name,
                "DisplayName": self._label(label),
                "RequiredLevel": {"Value": "None"},
                "Format": "DateOnly",
                "ImeMode": "Inactive",
            }
        if dtype_l in ("bool", "boolean"):
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.BooleanAttributeMetadata",
                "SchemaName": schema_name,
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

    def _get_table_info(self, tablename: str) -> Optional[Dict[str, Any]]:
        """Return basic metadata for a custom table if it exists.

        Parameters
        ----------
        tablename : str
            Friendly name or full schema name (with publisher prefix and underscore).

        Returns
        -------
        dict | None
            Metadata summary or ``None`` if not found.
        """
        ent = self._get_entity_by_schema(tablename)
        if not ent:
            return None
        return {
            "entity_schema": ent.get("SchemaName") or tablename,
            "entity_logical_name": ent.get("LogicalName"),
            "entity_set_name": ent.get("EntitySetName"),
            "metadata_id": ent.get("MetadataId"),
            "columns_created": [],
        }
    
    def _list_tables(self) -> List[Dict[str, Any]]:
        """List all tables in the Dataverse, excluding private tables (IsPrivate=true)."""
        url = f"{self.api}/EntityDefinitions"
        params = {
            "$filter": "IsPrivate eq false"
        }
        r = self._request("get", url, params=params)
        return r.json().get("value", [])

    def _delete_table(self, tablename: str) -> None:
        entity_schema = self._normalize_entity_schema(tablename)
        ent = self._get_entity_by_schema(entity_schema)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{entity_schema}' not found.",
                subcode=ec.METADATA_TABLE_NOT_FOUND,
            )
        metadata_id = ent["MetadataId"]
        url = f"{self.api}/EntityDefinitions({metadata_id})"
        r = self._request("delete", url)

    def _create_table(self, tablename: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        # Accept a friendly name and construct a default schema under 'new_'.
        # If a full SchemaName is passed (contains '_'), use as-is.
        entity_schema = self._normalize_entity_schema(tablename)

        ent = self._get_entity_by_schema(entity_schema)
        if ent:
            raise MetadataError(
                f"Table '{entity_schema}' already exists.",
                subcode=ec.METADATA_TABLE_ALREADY_EXISTS,
            )

        created_cols: List[str] = []
        primary_attr_schema = "new_Name" if "_" not in entity_schema else f"{entity_schema.split('_',1)[0]}_Name"
        attributes: List[Dict[str, Any]] = []
        attributes.append(self._attribute_payload(primary_attr_schema, "string", is_primary_name=True))
        for col_name, dtype in schema.items():
            attr_schema = self._normalize_attribute_schema(entity_schema, col_name)
            payload = self._attribute_payload(attr_schema, dtype)
            if not payload:
                raise ValueError(f"Unsupported column type '{dtype}' for '{col_name}'.")
            attributes.append(payload)
            created_cols.append(attr_schema)

        metadata_id = self._create_entity(entity_schema, tablename, attributes)
        ent2: Dict[str, Any] = self._wait_for_entity_ready(entity_schema) or {}
        logical_name = ent2.get("LogicalName")

        return {
            "entity_schema": entity_schema,
            "entity_logical_name": logical_name,
            "entity_set_name": ent2.get("EntitySetName") if ent2 else None,
            "metadata_id": metadata_id,
            "columns_created": created_cols,
        }

    def _create_columns(
        self,
        tablename: str,
        columns: Dict[str, Any],
    ) -> List[str]:
        if not isinstance(columns, dict) or not columns:
            raise TypeError("columns must be a non-empty dict[name -> type]")
        entity_schema = self._normalize_entity_schema(tablename)
        ent = self._get_entity_by_schema(entity_schema)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{entity_schema}' not found.",
                subcode=ec.METADATA_TABLE_NOT_FOUND,
            )

        metadata_id = ent.get("MetadataId")
        created: List[str] = []
        needs_picklist_flush = False

        for column_name, column_type in columns.items():
            schema_name = self._normalize_attribute_schema(entity_schema, column_name)
            payload = self._attribute_payload(schema_name, column_type)
            if not payload:
                raise ValueError(f"Unsupported column type '{column_type}' for '{schema_name}'.")

            url = f"{self.api}/EntityDefinitions({metadata_id})/Attributes"
            self._request("post", url, json=payload)

            created.append(schema_name)

            if "OptionSet" in payload:
                needs_picklist_flush = True

        if needs_picklist_flush:
            self._flush_cache("picklist")

        return created

    def _delete_columns(
        self,
        tablename: str,
        columns: Union[str, List[str]],
    ) -> List[str]:
        if isinstance(columns, str):
            names = [columns]
        elif isinstance(columns, list):
            names = columns
        else:
            raise TypeError("columns must be str or list[str]")

        for name in names:
            if not isinstance(name, str) or not name.strip():
                raise ValueError("column names must be non-empty strings")

        entity_schema = self._normalize_entity_schema(tablename)
        ent = self._get_entity_by_schema(entity_schema)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{entity_schema}' not found.",
                subcode=ec.METADATA_TABLE_NOT_FOUND,
            )

        metadata_id = ent.get("MetadataId")
        deleted: List[str] = []
        needs_picklist_flush = False

        for column_name in names:
            schema_name = self._normalize_attribute_schema(entity_schema, column_name)
            attr_meta = self._get_attribute_metadata(metadata_id, schema_name, extra_select="@odata.type,AttributeType")
            if not attr_meta:
                raise MetadataError(
                    f"Column '{schema_name}' not found on table '{entity_schema}'.",
                    subcode=ec.METADATA_COLUMN_NOT_FOUND,
                )

            attr_metadata_id = attr_meta.get("MetadataId")
            if not attr_metadata_id:
                raise RuntimeError(
                    f"Metadata incomplete for column '{schema_name}' (missing MetadataId)."
                )

            attr_url = f"{self.api}/EntityDefinitions({metadata_id})/Attributes({attr_metadata_id})"
            self._request("delete", attr_url, headers={"If-Match": "*"})

            attr_type = attr_meta.get("@odata.type") or attr_meta.get("AttributeType")
            if isinstance(attr_type, str):
                attr_type_l = attr_type.lower()
                if "picklist" in attr_type_l or "optionset" in attr_type_l:
                    needs_picklist_flush = True

            deleted.append(schema_name)

        if needs_picklist_flush:
            self._flush_cache("picklist")

        return deleted
    
    # ---------------------- Cache maintenance -------------------------
    def _flush_cache(
        self,
        kind,
    ) -> int:
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
        k = (kind or "").strip().lower()
        if k != "picklist":
            raise ValidationError(
                f"Unsupported cache kind '{kind}' (only 'picklist' is implemented)",
                subcode=ec.VALIDATION_UNSUPPORTED_CACHE_KIND,
            )

        removed = len(self._picklist_label_cache)
        self._picklist_label_cache.clear()
        return removed