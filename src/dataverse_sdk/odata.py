from __future__ import annotations

from typing import Any, Dict, Optional, List
import re
import json

from .http import HttpClient


class ODataClient:
    """Dataverse Web API client: CRUD, SQL-over-API, and table metadata helpers."""

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
    def create(self, entity_set: str, data: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.api}/{entity_set}"
        headers = self._headers().copy()
        headers["Prefer"] = "return=representation"
        r = self._request("post", url, headers=headers, json=data)
        r.raise_for_status()
        return r.json()

    def _format_key(self, key: str) -> str:
        k = key.strip()
        if k.startswith("(") and k.endswith(")"):
            return k
        if len(k) == 36 and "-" in k:
            return f"({k})"
        return f"({k})"

    def update(self, entity_set: str, key: str, data: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        headers = self._headers().copy()
        headers["If-Match"] = "*"
        headers["Prefer"] = "return=representation"
        r = self._request("patch", url, headers=headers, json=data)
        r.raise_for_status()
        return r.json()

    def delete(self, entity_set: str, key: str) -> None:
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        headers = self._headers().copy()
        headers["If-Match"] = "*"
        r = self._request("delete", url, headers=headers)
        r.raise_for_status()

    def get(self, entity_set: str, key: str, select: Optional[str] = None) -> Dict[str, Any]:
        params = {}
        if select:
            params["$select"] = select
        url = f"{self.api}/{entity_set}{self._format_key(key)}"
        r = self._request("get", url, headers=self._headers(), params=params)
        r.raise_for_status()
        return r.json()

    # --------------------------- SQL Custom API -------------------------
    def query_sql(self, tsql: str) -> list[dict[str, Any]]:
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
        # Accept tablename as a display/logical root; infer a default schema using 'new_' if not provided.
        # If caller passes a full SchemaName, use it as-is.
        schema_name = tablename if "_" in tablename else f"new_{self._to_pascal(tablename)}"
        entity_schema = schema_name
        ent = self._get_entity_by_schema(entity_schema)
        if not ent:
            return None
        return {
            "entity_schema": ent.get("SchemaName") or entity_schema,
            "entity_logical_name": ent.get("LogicalName"),
            "entity_set_name": ent.get("EntitySetName"),
            "metadata_id": ent.get("MetadataId"),
            "columns_created": [],
        }

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
