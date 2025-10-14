from __future__ import annotations

from typing import Any, Dict, Optional, List, Union, Iterable
from enum import Enum
import unicodedata
import re
import json

from .http import HttpClient


_GUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")


class ODataClient:
    """Dataverse Web API client: CRUD, SQL-over-API, and table metadata helpers."""

    @staticmethod
    def _escape_odata_quotes(value: str) -> str:
        """Escape single quotes for OData queries (by doubling them)."""
        return value.replace("'", "''")

    def __init__(self, auth, base_url: str, config=None) -> None:
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
        # Cache: entity set name -> logical name (resolved via metadata lookup)
        self._entityset_logical_cache = {}
        # Cache: logical name -> entity set name (reverse lookup for SQL endpoint)
        self._logical_to_entityset_cache: dict[str, str] = {}
        # Cache: entity set name -> primary id attribute (metadata PrimaryIdAttribute)
        self._entityset_primaryid_cache: dict[str, str] = {}
        # Cache: logical name -> primary id attribute
        self._logical_primaryid_cache: dict[str, str] = {}

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
        }

    def _request(self, method: str, url: str, **kwargs):
        return self._http.request(method, url, **kwargs)

    # ----------------------------- CRUD ---------------------------------
    def _create(self, entity_set: str, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Union[str, List[str]]:
        """Create one or many records.

        Parameters
        ----------
        entity_set : str
            Entity set (plural logical name), e.g. "accounts".
        data : dict | list[dict]
            Single entity payload or list of payloads for batch create.

        Behaviour
        ---------
        - Single (dict): POST /{entity_set}. Returns GUID string (no representation fetched).
        - Multiple (list[dict]): POST /{entity_set}/Microsoft.Dynamics.CRM.CreateMultiple. Returns list[str] of created GUIDs.

        Multi-create logical name resolution
        ------------------------------------
        - If any payload omits ``@odata.type`` the client performs a metadata lookup (once per entity set, cached)
          to resolve the logical name and stamps ``Microsoft.Dynamics.CRM.<logical>`` into missing payloads.
        - If all payloads already include ``@odata.type`` no lookup or modification occurs.

        Returns
        -------
        str | list[str]
            Created record GUID (single) or list of created IDs (multi).
        """
        if isinstance(data, dict):
            return self._create_single(entity_set, data)
        if isinstance(data, list):
            return self._create_multiple(entity_set, data)
        raise TypeError("data must be dict or list[dict]")

    # --- Internal helpers ---
    def _create_single(self, entity_set: str, record: Dict[str, Any]) -> str:
        """Create a single record and return its GUID.

        Relies on OData-EntityId (canonical) or Location header. No response body parsing is performed.
        Raises RuntimeError if neither header contains a GUID.
        """
        record = self._convert_labels_to_ints(entity_set, record)
        url = f"{self.api}/{entity_set}"
        headers = self._headers().copy()
        r = self._request("post", url, headers=headers, json=record)
        r.raise_for_status()

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

    def _logical_from_entity_set(self, entity_set: str) -> str:
        """Resolve logical name from an entity set using metadata (cached)."""
        es = (entity_set or "").strip()
        if not es:
            raise ValueError("entity_set is required")
        cached = self._entityset_logical_cache.get(es)
        if cached:
            return cached
        url = f"{self.api}/EntityDefinitions"
        # Escape single quotes in entity set name
        es_escaped = self._escape_odata_quotes(es)
        params = {
            "$select": "LogicalName,EntitySetName,PrimaryIdAttribute",
            "$filter": f"EntitySetName eq '{es_escaped}'",
        }
        r = self._request("get", url, headers=self._headers(), params=params)
        r.raise_for_status()
        try:
            body = r.json()
            items = body.get("value", []) if isinstance(body, dict) else []
        except ValueError:
            items = []
        if not items:
            raise RuntimeError(f"Unable to resolve logical name for entity set '{es}'. Provide @odata.type explicitly.")
        md = items[0]
        logical = md.get("LogicalName")
        if not logical:
            raise RuntimeError(f"Metadata response missing LogicalName for entity set '{es}'.")
        primary_id_attr = md.get("PrimaryIdAttribute")
        self._entityset_logical_cache[es] = logical
        if isinstance(primary_id_attr, str) and primary_id_attr:
            self._entityset_primaryid_cache[es] = primary_id_attr
            self._logical_primaryid_cache[logical] = primary_id_attr
        return logical

    def _create_multiple(self, entity_set: str, records: List[Dict[str, Any]]) -> List[str]:
        if not all(isinstance(r, dict) for r in records):
            raise TypeError("All items for multi-create must be dicts")
        need_logical = any("@odata.type" not in r for r in records)
        logical: Optional[str] = None
        if need_logical:
            logical = self._logical_from_entity_set(entity_set)
        enriched: List[Dict[str, Any]] = []
        for r in records:
            r = self._convert_labels_to_ints(entity_set, r)
            if "@odata.type" in r or not logical:
                enriched.append(r)
            else:
                nr = r.copy()
                nr["@odata.type"] = f"Microsoft.Dynamics.CRM.{logical}"
                enriched.append(nr)
        payload = {"Targets": enriched}
        # Bound action form: POST {entity_set}/Microsoft.Dynamics.CRM.CreateMultiple
        url = f"{self.api}/{entity_set}/Microsoft.Dynamics.CRM.CreateMultiple"
        # The action currently returns only Ids; no need to request representation.
        headers = self._headers().copy()
        r = self._request("post", url, headers=headers, json=payload)
        r.raise_for_status()
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
    def _primary_id_attr(self, entity_set: str) -> str:
        """Return primary key attribute using metadata (fallback to <logical>id)."""
        pid = self._entityset_primaryid_cache.get(entity_set)
        if pid:
            return pid
        logical = self._logical_from_entity_set(entity_set)
        pid = self._entityset_primaryid_cache.get(entity_set) or self._logical_primaryid_cache.get(logical)
        if pid:
            return pid
        return f"{logical}id"

    def _update_by_ids(self, entity_set: str, ids: List[str], changes: Union[Dict[str, Any], List[Dict[str, Any]]]) -> None:
        """Update many records by GUID list using UpdateMultiple under the hood.

        Parameters
        ----------
        entity_set : str
            Entity set (plural logical name).
        ids : list[str]
            GUIDs of target records.
        changes : dict | list[dict]
            Broadcast patch (dict) applied to all IDs, or list of per-record patches (1:1 with ids).
        """
        if not isinstance(ids, list):
            raise TypeError("ids must be list[str]")
        if not ids:
            return None
        pk_attr = self._primary_id_attr(entity_set)
        if isinstance(changes, dict):
            batch = [{pk_attr: rid, **changes} for rid in ids]
            self._update_multiple(entity_set, batch)
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
        self._update_multiple(entity_set, batch)
        return None

    def _delete_multiple(self, entity_set: str, ids: List[str]) -> None:
        """Delete many records by GUID list (simple loop; potential future optimization point)."""
        if not isinstance(ids, list):
            raise TypeError("ids must be list[str]")
        for rid in ids:
            self.delete(entity_set, rid)
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

    def _update(self, entity_set: str, key: str, data: Dict[str, Any]) -> None:
        """Update an existing record.

        Parameters
        ----------
        entity_set : str
            Entity set name (plural logical name).
        key : str
            Record GUID (with or without parentheses) or alternate key.
        data : dict
            Partial entity payload.

        Returns
        -------
        None
        """
        data = self._convert_labels_to_ints(entity_set, data)
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        headers = self._headers().copy()
        headers["If-Match"] = "*"
        r = self._request("patch", url, headers=headers, json=data)
        r.raise_for_status()

    def _update_multiple(self, entity_set: str, records: List[Dict[str, Any]]) -> None:
        """Bulk update existing records via the collection-bound UpdateMultiple action.

        Parameters
        ----------
        entity_set : str
            Entity set (plural logical name), e.g. "accounts".
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
        logical: Optional[str] = None
        if need_logical:
            logical = self._logical_from_entity_set(entity_set)
        enriched: List[Dict[str, Any]] = []
        for r in records:
            r = self._convert_labels_to_ints(entity_set, r)
            if "@odata.type" in r or not logical:
                enriched.append(r)
            else:
                nr = r.copy()
                nr["@odata.type"] = f"Microsoft.Dynamics.CRM.{logical}"
                enriched.append(nr)

        payload = {"Targets": enriched}
        url = f"{self.api}/{entity_set}/Microsoft.Dynamics.CRM.UpdateMultiple"
        headers = self._headers().copy()
        r = self._request("post", url, headers=headers, json=payload)
        r.raise_for_status()
        # Intentionally ignore response content: no stable contract for IDs across environments.
        return None

    def _delete(self, entity_set: str, key: str) -> None:
        """Delete a record by GUID or alternate key."""
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        headers = self._headers().copy()
        headers["If-Match"] = "*"
        r = self._request("delete", url, headers=headers)
        r.raise_for_status()

    def _get(self, entity_set: str, key: str, select: Optional[str] = None) -> Dict[str, Any]:
        """Retrieve a single record.

        Parameters
        ----------
        entity_set : str
            Entity set name.
        key : str
            Record GUID (with or without parentheses) or alternate key syntax.
        select : str | None
            Comma separated columns for $select.
        """
        params = {}
        if select:
            params["$select"] = select
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        r = self._request("get", url, headers=self._headers(), params=params)
        r.raise_for_status()
        return r.json()

    def _get_multiple(
        self,
        entity_set: str,
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
        entity_set : str
            Entity set name (plural logical name).
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

        headers = self._headers().copy()
        if page_size is not None:
            ps = int(page_size)
            if ps > 0:
                headers["Prefer"] = f"odata.maxpagesize={ps}"

        def _do_request(url: str, *, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
            r = self._request("get", url, headers=headers, params=params)
            r.raise_for_status()
            try:
                return r.json()
            except ValueError:
                return {}

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
        if not isinstance(sql, str) or not sql.strip():
            raise ValueError("sql must be a non-empty string")
        sql = sql.strip()

        # Extract logical table name via helper (robust to identifiers ending with 'from')
        logical = self._extract_logical_table(sql)

        entity_set = self._entity_set_from_logical(logical)
        # Issue GET /{entity_set}?sql=<query>
        headers = self._headers().copy()
        url = f"{self.api}/{entity_set}"
        params = {"sql": sql}
        r = self._request("get", url, headers=headers, params=params)
        try:
            r.raise_for_status()
        except Exception as e:
            # Attach response snippet to aid debugging unsupported SQL patterns
            resp_text = None
            try:
                resp_text = r.text[:500] if getattr(r, 'text', None) else None
            except Exception:
                pass
            detail = f" SQL query failed (status={getattr(r, 'status_code', '?')}): {resp_text}" if resp_text else ""
            raise RuntimeError(str(e) + detail) from e
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
        r = self._request("get", url, headers=self._headers(), params=params)
        r.raise_for_status()
        try:
            body = r.json()
            items = body.get("value", []) if isinstance(body, dict) else []
        except ValueError:
            items = []
        if not items:
            raise RuntimeError(f"Unable to resolve entity set for logical name '{logical}'.")
        md = items[0]
        es = md.get("EntitySetName")
        if not es:
            raise RuntimeError(f"Metadata response missing EntitySetName for logical '{logical}'.")
        self._logical_to_entityset_cache[logical] = es
        primary_id_attr = md.get("PrimaryIdAttribute")
        if isinstance(primary_id_attr, str) and primary_id_attr:
            self._logical_primaryid_cache[logical] = primary_id_attr
            self._entityset_primaryid_cache[es] = primary_id_attr
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

    def _get_entity_by_schema(self, schema_name: str) -> Optional[Dict[str, Any]]:
        url = f"{self.api}/EntityDefinitions"
        # Escape single quotes in schema name
        schema_escaped = self._escape_odata_quotes(schema_name)
        params = {
            "$select": "MetadataId,LogicalName,SchemaName,EntitySetName",
            "$filter": f"SchemaName eq '{schema_escaped}'",
        }
        r = self._request("get", url, headers=self._headers(), params=params)
        r.raise_for_status()
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
        headers = self._headers()
        r = self._request("post", url, headers=headers, json=payload)
        r.raise_for_status()
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

    def _enum_optionset_payload(self, schema_name: str, enum_cls: type[Enum], *, is_primary_name: bool = False) -> Dict[str, Any]:
        """Create local (IsGlobal=False) PicklistAttributeMetadata from an Enum subclass.

        Supported translation mapping via optional class attribute `__labels__`:
            __labels__ = { 1033: { "Active": "Active", "Inactive": "Inactive" },
                           1036: { "Active": "Actif",  "Inactive": "Inactif" } }

        Keys inside per-language dict may be either enum member objects or their names.
        If a language lacks a label for a member, member.name is used as fallback.
        The client's configured language code is always ensured to exist.
        """
        members = list(enum_cls)
        if not members:
            raise ValueError(f"Enum {enum_cls.__name__} has no members")
        # Validate integer values & uniqueness
        seen_vals: set[int] = set()
        for m in members:
            if not isinstance(m.value, int):
                raise ValueError(f"Enum member '{m.name}' has non-int value '{m.value}' (only int values supported)")
            if m.value in seen_vals:
                raise ValueError(f"Duplicate enum value {m.value} in {enum_cls.__name__}")
            seen_vals.add(m.value)

        raw_labels = getattr(enum_cls, "__labels__", None)
        labels_by_lang: Dict[int, Dict[str, str]] = {}
        if raw_labels is not None:
            if not isinstance(raw_labels, dict):
                raise ValueError("__labels__ must be a dict {lang:int -> {member: label}}")
            for lang, mapping in raw_labels.items():
                if not isinstance(lang, int):
                    raise ValueError("Language codes in __labels__ must be ints")
                if not isinstance(mapping, dict):
                    raise ValueError(f"__labels__[{lang}] must be a dict of member names to strings")
                labels_by_lang.setdefault(lang, {})
                for k, v in mapping.items():
                    member_name = k.name if isinstance(k, enum_cls) else str(k)
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

    # ---------------------- Picklist label coercion ----------------------
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

    def _optionset_map(self, entity_set: str, attr_logical: str) -> Optional[Dict[str, int]]:
        """Build or return cached mapping of normalized label -> value for a picklist attribute.

        Returns None if attribute is not a picklist or no options available.
        """
        if not entity_set or not attr_logical:
            return None
        logical = self._logical_from_entity_set(entity_set)
        cache_key = (logical, attr_logical.lower())
        if not hasattr(self, "_picklist_label_cache"):
            self._picklist_label_cache = {}
        if cache_key in self._picklist_label_cache:
            return self._picklist_label_cache[cache_key]
        debug_attr = attr_logical.lower().endswith("_status")

        attr_esc = self._escape_odata_quotes(attr_logical)
        logical_esc = self._escape_odata_quotes(logical)

        # Step 1: lightweight fetch (no expand) to determine attribute type
        url_type = (
            f"{self.api}/EntityDefinitions(LogicalName='{logical_esc}')/Attributes"
            f"?$filter=LogicalName eq '{attr_esc}'&$select=LogicalName,AttributeType"
        )
        r_type = self._request("get", url_type, headers=self._headers())
        if debug_attr:
            try:
                print({"debug_picklist_probe_type": {"attr": attr_logical, "status": r_type.status_code}})
            except Exception:
                pass
        if r_type.status_code == 404:
            # Do not permanently cache negative result; metadata may appear later.
            return None
        r_type.raise_for_status()
        body_type = r_type.json()
        items = body_type.get("value", []) if isinstance(body_type, dict) else []
        if not items:
            return None
        attr_md = items[0]
        if debug_attr:
            try:
                print({"debug_picklist_attr_type": attr_md.get("AttributeType")})
            except Exception:
                pass
        if attr_md.get("AttributeType") not in ("Picklist", "PickList"):
            return None

        # Step 2: fetch with expand only now that we know it's a picklist
        # Need to cast to the derived PicklistAttributeMetadata type; OptionSet is not a nav on base AttributeMetadata.
        cast_url = (
            f"{self.api}/EntityDefinitions(LogicalName='{logical_esc}')/Attributes(LogicalName='{attr_esc}')/"
            "Microsoft.Dynamics.CRM.PicklistAttributeMetadata?$select=LogicalName&$expand=OptionSet($select=Options)"
        )
        r_opts = self._request("get", cast_url, headers=self._headers())
        if debug_attr:
            try:
                print({"debug_picklist_cast_fetch": {"status": r_opts.status_code}})
            except Exception:
                pass
        if r_opts.status_code == 404:
            # Fallback: try non-cast form (older behaviour) just in case environment differs
            alt_url = (
                f"{self.api}/EntityDefinitions(LogicalName='{logical_esc}')/Attributes(LogicalName='{attr_esc}')"
                f"?$select=LogicalName&$expand=OptionSet($select=Options)"
            )
            r_opts = self._request("get", alt_url, headers=self._headers())
            if r_opts.status_code == 404:
                return None
        try:
            r_opts.raise_for_status()
        except Exception:
            # If expansion still fails, skip caching negative to allow future retry.
            return None
        attr_full = {}
        try:
            attr_full = r_opts.json() if r_opts.text else {}
        except ValueError:
            return None
        option_set = attr_full.get("OptionSet") or {}
        options = option_set.get("Options") if isinstance(option_set, dict) else None
        if not isinstance(options, list):
            return None
        if debug_attr:
            try:
                print({"debug_picklist_options_count": len(options)})
            except Exception:
                pass
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
        if debug_attr:
            try:
                print({"debug_picklist_mapping_keys": sorted(mapping.keys())})
            except Exception:
                pass
        if mapping:
            self._picklist_label_cache[cache_key] = mapping
            return mapping
        return None

    def _convert_labels_to_ints(self, entity_set: str, record: Dict[str, Any]) -> Dict[str, Any]:
        """Return a copy of record with any labels converted to option ints.

        Heuristic: For each string value, attempt to resolve against picklist metadata.
        If attribute isn't a picklist or label not found, value left unchanged.
        """
        out = record.copy()
        for k, v in list(out.items()):
            if not isinstance(v, str) or not v.strip():
                continue
            mapping = self._optionset_map(entity_set, k)
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
        r = self._request("get", url, headers=self._headers(), params=params)
        r.raise_for_status()
        return r.json().get("value", [])

    def _delete_table(self, tablename: str) -> None:
        schema_name = tablename if "_" in tablename else f"new_{self._to_pascal(tablename)}"
        entity_schema = schema_name
        ent = self._get_entity_by_schema(entity_schema)
        if not ent or not ent.get("MetadataId"):
            raise RuntimeError(f"Table '{entity_schema}' not found.")
        metadata_id = ent["MetadataId"]
        url = f"{self.api}/EntityDefinitions({metadata_id})"
        headers = self._headers()
        r = self._request("delete", url, headers=headers)
        r.raise_for_status()

    def _create_table(self, tablename: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        # Accept a friendly name and construct a default schema under 'new_'.
        # If a full SchemaName is passed (contains '_'), use as-is.
        entity_schema = tablename if "_" in tablename else f"new_{self._to_pascal(tablename)}"

        ent = self._get_entity_by_schema(entity_schema)
        if ent:
            raise RuntimeError(f"Table '{entity_schema}' already exists. No update performed.")

        created_cols: List[str] = []
        primary_attr_schema = "new_Name" if "_" not in entity_schema else f"{entity_schema.split('_',1)[0]}_Name"
        attributes: List[Dict[str, Any]] = []
        attributes.append(self._attribute_payload(primary_attr_schema, "string", is_primary_name=True))
        for col_name, dtype in schema.items():
            # Use same publisher prefix segment as entity_schema if present; else default to 'new_'.
            publisher = entity_schema.split("_", 1)[0] if "_" in entity_schema else "new"
            if col_name.lower().startswith(f"{publisher}_"):
                attr_schema = col_name
            else:
                attr_schema = f"{publisher}_{self._to_pascal(col_name)}"
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
