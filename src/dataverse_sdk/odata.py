from __future__ import annotations

from typing import Any, Dict, Optional, List, Union, Iterable
import re
import json

from .http import HttpClient


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
    def create(self, entity_set: str, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> Union[Dict[str, Any], List[str]]:
        """Create one or many records.

        Parameters
        ----------
        entity_set : str
            Entity set (plural logical name), e.g. "accounts".
        data : dict | list[dict]
            Single entity payload or list of payloads for batch create.

        Behaviour
        ---------
        - Single (dict): POST /{entity_set} with Prefer: return=representation. Returns created record (dict).
        - Multiple (list[dict]): POST /{entity_set}/Microsoft.Dynamics.CRM.CreateMultiple. Returns list[str] of created GUIDs.

        Multi-create logical name resolution
        ------------------------------------
        - If any payload omits ``@odata.type`` the client performs a metadata lookup (once per entity set, cached)
          to resolve the logical name and stamps ``Microsoft.Dynamics.CRM.<logical>`` into missing payloads.
        - If all payloads already include ``@odata.type`` no lookup or modification occurs.

        Returns
        -------
        dict | list[str]
            Created entity (single) or list of created IDs (multi).
        """
        if isinstance(data, dict):
            return self._create_single(entity_set, data)
        if isinstance(data, list):
            return self._create_multiple(entity_set, data)
        raise TypeError("data must be dict or list[dict]")

    # --- Internal helpers ---
    def _create_single(self, entity_set: str, record: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.api}/{entity_set}"
        headers = self._headers().copy()
        # Always request the created representation; server may ignore but for single create
        # Dataverse typically returns the full body when asked.
        headers["Prefer"] = "return=representation"
        r = self._request("post", url, headers=headers, json=record)
        r.raise_for_status()
        # If empty body, return {} (server might not honour prefer)
        try:
            return r.json() if r.text else {}
        except ValueError:
            return {}

    def _logical_from_entity_set(self, entity_set: str) -> str:
        """Resolve logical name from an entity set using metadata (cached)."""
        es = (entity_set or "").strip()
        if not es:
            raise ValueError("entity_set is required")
        cached = self._entityset_logical_cache.get(es)
        if cached:
            return cached
        url = f"{self.api}/EntityDefinitions"
        params = {
            "$select": "LogicalName,EntitySetName",
            "$filter": f"EntitySetName eq '{es}'",
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
        logical = items[0].get("LogicalName")
        if not logical:
            raise RuntimeError(f"Metadata response missing LogicalName for entity set '{es}'.")
        self._entityset_logical_cache[es] = logical
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

    def update(self, entity_set: str, key: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """Update an existing record and return the updated representation.

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
        dict
            Updated record representation.
        """
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        headers = self._headers().copy()
        headers["If-Match"] = "*"
        headers["Prefer"] = "return=representation"
        r = self._request("patch", url, headers=headers, json=data)
        r.raise_for_status()
        return r.json()

    def update_multiple(self, entity_set: str, records: List[Dict[str, Any]]) -> None:
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
        - Expects Dataverse transactional semantics: if any individual update fails the entire request is rolled back
          and an error HTTP status is returned (no partial success handling in V1).
        - Response is expected to include an ``Ids`` list (mirrors CreateMultiple); if absent an empty list is
          returned.

        Returns
        -------
        None
            This method does not return IDs or record bodies. The Dataverse UpdateMultiple action does not
            consistently emit identifiers across environments; to keep semantics predictable the SDK returns
            nothing on success. Use follow-up queries (e.g. get / get_multiple) if you need refreshed data.

        Notes
        -----
        - Caller must include the correct primary key attribute (e.g. ``accountid``) in every record.
        - No representation of updated records is returned; for a single record representation use ``update``.
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

    def delete(self, entity_set: str, key: str) -> None:
        """Delete a record by GUID or alternate key."""
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        headers = self._headers().copy()
        headers["If-Match"] = "*"
        r = self._request("delete", url, headers=headers)
        r.raise_for_status()

    def get(self, entity_set: str, key: str, select: Optional[str] = None) -> Dict[str, Any]:
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

    def get_multiple(
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
    def query_sql(self, sql: str) -> list[dict[str, Any]]:
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
            "$select": "LogicalName,EntitySetName",
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
        es = items[0].get("EntitySetName")
        if not es:
            raise RuntimeError(f"Metadata response missing EntitySetName for logical '{logical}'.")
        self._logical_to_entityset_cache[logical] = es
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
        params = {
            "$select": "MetadataId,LogicalName,SchemaName,EntitySetName",
            "$filter": f"SchemaName eq '{schema_name}'",
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

    def _attribute_payload(self, schema_name: str, dtype: str, *, is_primary_name: bool = False) -> Optional[Dict[str, Any]]:
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

    def get_table_info(self, tablename: str) -> Optional[Dict[str, Any]]:
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
    
    def list_tables(self) -> List[Dict[str, Any]]:
        """List all tables in the Dataverse, excluding private tables (IsPrivate=true)."""
        url = f"{self.api}/EntityDefinitions"
        params = {
            "$filter": "IsPrivate eq false"
        }
        r = self._request("get", url, headers=self._headers(), params=params)
        r.raise_for_status()
        return r.json().get("value", [])

    def delete_table(self, tablename: str) -> None:
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

    def create_lookup_field(
        self,
        table_name: str, 
        field_name: str, 
        target_table: str,
        display_name: str = None,
        description: str = None,
        required_level: str = "None",
        relationship_name: str = None,
        relationship_behavior: str = "UseLabel",
        cascade_delete: str = "RemoveLink",
    ) -> Dict[str, Any]:
        """
        Create a lookup field (n:1 relationship) between two tables.
        
        Parameters
        ----------
        table_name : str
            The logical name of the table where the lookup field will be created (referencing entity).
        field_name : str
            The name of the lookup field to create (without _id suffix).
        target_table : str
            The logical name of the table the lookup will reference (referenced entity).
        display_name : str, optional
            The display name for the lookup field.
        description : str, optional
            The description for the lookup field.
        required_level : str, optional
            The requirement level: "None", "Recommended", or "ApplicationRequired".
        relationship_name : str, optional
            The name of the relationship. If not provided, one will be generated.
        relationship_behavior : str, optional
            The relationship menu behavior: "UseLabel", "UseCollectionName", "DoNotDisplay".
        cascade_delete : str, optional
            The cascade behavior on delete: "Cascade", "RemoveLink", "Restrict".
            
        Returns
        -------
        dict
            Details about the created relationship.
        """
        # Get information about both tables
        referencing_entity = self._get_entity_by_schema(table_name)
        referenced_entity = self._get_entity_by_schema(target_table)
        
        if not referencing_entity:
            raise ValueError(f"Table '{table_name}' not found.")
        if not referenced_entity:
            raise ValueError(f"Target table '{target_table}' not found.")
            
        referencing_logical_name = referencing_entity.get("LogicalName")
        referenced_logical_name = referenced_entity.get("LogicalName")
        
        if not referencing_logical_name or not referenced_logical_name:
            raise ValueError("Could not determine logical names for the tables.")
            
        # If no relationship name provided, generate one
        if not relationship_name:
            relationship_name = f"{referenced_logical_name}_{referencing_logical_name}"
            
        # If no display name provided, use the target table name
        if not display_name:
            display_name = self._to_pascal(referenced_logical_name)
            
        # Prepare relationship metadata
        one_to_many_relationship = {
            "@odata.type": "Microsoft.Dynamics.CRM.OneToManyRelationshipMetadata",
            "SchemaName": relationship_name,
            "ReferencedEntity": referenced_logical_name,
            "ReferencingEntity": referencing_logical_name,
            "ReferencedAttribute": f"{referenced_logical_name}id",  # Usually the primary key attribute
            "AssociatedMenuConfiguration": {
                "Behavior": relationship_behavior,
                "Group": "Details",
                "Label": {
                    "@odata.type": "Microsoft.Dynamics.CRM.Label",
                    "LocalizedLabels": [
                        {
                            "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                            "Label": display_name or referenced_logical_name,
                            "LanguageCode": int(self.config.language_code),
                        }
                    ],
                    "UserLocalizedLabel": {
                        "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                        "Label": display_name or referenced_logical_name,
                        "LanguageCode": int(self.config.language_code),
                    }
                },
                "Order": 10000
            },
            "CascadeConfiguration": {
                "Assign": "NoCascade",
                "Delete": cascade_delete,
                "Merge": "NoCascade",
                "Reparent": "NoCascade",
                "Share": "NoCascade",
                "Unshare": "NoCascade"
            }
        }
        
        # Prepare lookup attribute metadata
        lookup_field_schema_name = f"{field_name}"
        if not lookup_field_schema_name.lower().startswith(f"{referencing_logical_name.split('_')[0]}_"):
            lookup_field_schema_name = f"{referencing_logical_name.split('_')[0]}_{field_name}"
            
        lookup_attribute = {
            "@odata.type": "Microsoft.Dynamics.CRM.LookupAttributeMetadata",
            "SchemaName": lookup_field_schema_name,
            "DisplayName": {
                "@odata.type": "Microsoft.Dynamics.CRM.Label",
                "LocalizedLabels": [
                    {
                        "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                        "Label": display_name or field_name,
                        "LanguageCode": int(self.config.language_code),
                    }
                ]
            }
        }
        
        if description:
            lookup_attribute["Description"] = {
                "@odata.type": "Microsoft.Dynamics.CRM.Label",
                "LocalizedLabels": [
                    {
                        "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
                        "Label": description,
                        "LanguageCode": int(self.config.language_code),
                    }
                ]
            }
            
        lookup_attribute["RequiredLevel"] = {
            "Value": required_level,
            "CanBeChanged": True,
            "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings"
        }
        
        # Create the relationship
        url = f"{self.api}/RelationshipDefinitions"
        headers = self._headers().copy()
        
        # Add the lookup attribute to the relationship definition
        one_to_many_relationship["Lookup"] = lookup_attribute
        
        # POST the relationship definition
        r = self._request("post", url, headers=headers, json=one_to_many_relationship)
        r.raise_for_status()
        
        # Get the relationship ID from the OData-EntityId header
        relationship_id = None
        if "OData-EntityId" in r.headers:
            entity_id_url = r.headers["OData-EntityId"]
            # Extract GUID from the URL
            import re
            match = re.search(r'RelationshipDefinitions\((.*?)\)', entity_id_url)
            if match:
                relationship_id = match.group(1)
                
        # Return relationship info
        return {
            "relationship_id": relationship_id,
            "relationship_name": relationship_name,
            "lookup_field": lookup_field_schema_name,
            "referenced_entity": referenced_logical_name,
            "referencing_entity": referencing_logical_name
        }

    def create_table(self, tablename: str, schema: Dict[str, Union[str, Dict[str, Any]]]) -> Dict[str, Any]:
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
        
        # Track lookups to create after table creation
        lookup_fields = []
        
        for col_name, col_info in schema.items():
            # Use same publisher prefix segment as entity_schema if present; else default to 'new_'.
            publisher = entity_schema.split("_", 1)[0] if "_" in entity_schema else "new"
            
            # Handle lookup fields (dictionary values in schema)
            if isinstance(col_info, dict) and "lookup" in col_info:
                lookup_fields.append({
                    "field_name": col_name,
                    "target_table": col_info["lookup"],
                    "display_name": col_info.get("display_name"),
                    "description": col_info.get("description"),
                    "required_level": col_info.get("required_level", "None"),
                    "relationship_name": col_info.get("relationship_name"),
                    "relationship_behavior": col_info.get("relationship_behavior", "UseLabel"),
                    "cascade_delete": col_info.get("cascade_delete", "RemoveLink"),
                })
                continue
                
            # Handle regular fields (string type values)
            dtype = col_info if isinstance(col_info, str) else "string"
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
        
        # Create lookup fields after table is created
        for lookup in lookup_fields:
            try:
                lookup_result = self.create_lookup_field(
                    table_name=logical_name,
                    field_name=lookup["field_name"],
                    target_table=lookup["target_table"],
                    display_name=lookup["display_name"],
                    description=lookup["description"],
                    required_level=lookup["required_level"],
                    relationship_name=lookup["relationship_name"],
                    relationship_behavior=lookup["relationship_behavior"],
                    cascade_delete=lookup["cascade_delete"]
                )
                created_cols.append(lookup_result["lookup_field"])
            except Exception as e:
                # Continue creating other lookup fields even if one fails
                print(f"Warning: Could not create lookup field '{lookup['field_name']}': {str(e)}")

        return {
            "entity_schema": entity_schema,
            "entity_logical_name": logical_name,
            "entity_set_name": ent2.get("EntitySetName") if ent2 else None,
            "metadata_id": metadata_id,
            "columns_created": created_cols,
        }
