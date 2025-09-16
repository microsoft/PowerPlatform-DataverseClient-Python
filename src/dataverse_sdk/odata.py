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
        # Escape single quotes in entity set name
        es_escaped = self._escape_odata_quotes(es)
        params = {
            "$select": "LogicalName,EntitySetName",
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
    def query_sql(self, tsql: str) -> list[dict[str, Any]]:
        """Execute a read-only T-SQL query via the configured Custom API.

        Parameters
        ----------
        tsql : str
            SELECT-style Dataverse-supported T-SQL (read-only).

        Returns
        -------
        list[dict]
            Rows materialised as list of dictionaries (empty list if no rows).

        Raises
        ------
        RuntimeError
            If the Custom API response is missing the expected ``queryresult`` property or type is unexpected.
        """
        payload = {"querytext": tsql}
        headers = self._headers()
        api_name = self.config.sql_api_name
        url = f"{self.api}/{api_name}"
        r = self._request("post", url, headers=headers, json=payload)
        r.raise_for_status()
        data = r.json()
        if "queryresult" not in data:
            raise RuntimeError(f"{api_name} response missing 'queryresult'.")
        q = data["queryresult"]
        if q is None:
            parsed = []
        elif isinstance(q, str):
            s = q.strip()
            parsed = [] if not s else json.loads(s)
        else:
            raise RuntimeError(f"Unexpected queryresult type: {type(q)}")
        return parsed

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

    def create_table(self, tablename: str, schema: Dict[str, str]) -> Dict[str, Any]:
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
