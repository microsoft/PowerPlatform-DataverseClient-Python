from __future__ import annotations

from typing import Any, Dict, Optional, List, Iterable
import re
import json
from urllib.parse import urlencode

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

    # ---------------------- Custom API (metadata) ----------------------
    def list_custom_apis(self, select: Optional[Iterable[str]] = None, filter_expr: Optional[str] = None) -> List[Dict[str, Any]]:
        """List Custom APIs (metadata records).

        Parameters
        ----------
        select : iterable of str, optional
            Specific columns to select (e.g. ["customapiid","uniquename","isfunction"]).
        filter_expr : str, optional
            OData $filter expression.
        """
        params: Dict[str, Any] = {"$select": ",".join(select) if select else "customapiid,uniquename,isfunction,bindingtype"}
        if filter_expr:
            params["$filter"] = filter_expr
        url = f"{self.api}/customapis"
        r = self._request("get", url, headers=self._headers(), params=params)
        r.raise_for_status()
        return r.json().get("value", [])

    def _get_custom_api(self, *, unique_name: Optional[str] = None, customapiid: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Internal helper to fetch a single Custom API by unique name or id."""
        if not unique_name and not customapiid:
            raise ValueError("Provide unique_name or customapiid")
        if customapiid:
            url = f"{self.api}/customapis({customapiid})"
            r = self._request("get", url, headers=self._headers())
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        # unique name path
        flt = f"uniquename eq '{unique_name}'"
        items = self.list_custom_apis(filter_expr=flt)
        if not items:
            return None
        return items[0]

    def get_custom_api(self, unique_name: Optional[str] = None, customapiid: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Public accessor for a Custom API metadata record."""
        return self._get_custom_api(unique_name=unique_name, customapiid=customapiid)

    def create_custom_api(
        self,
        *,
        unique_name: str,
        name: str,
        description: Optional[str] = None,
        is_function: bool = False,
        binding_type: str | int = "Global",
        bound_entity_logical_name: Optional[str] = None,
        allowed_custom_processing_step_type: int = 0,
        execute_privilege_name: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        request_parameters: Optional[List[Dict[str, Any]]] = None,
        response_properties: Optional[List[Dict[str, Any]]] = None,
        plugin_type_id: Optional[str] = None,
        is_private: bool = False,
        is_customizable: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Create a Dataverse Custom API metadata record.

        Parameters
        ----------
        unique_name : str
            Unique name (publisher prefix + name), e.g. ``new_Echo``.
        name : str
            Friendly display/primary name.
        description : str, optional
            Description text.
        is_function : bool, default False
            When True creates a function (GET); otherwise an action (POST).
        binding_type : str | int, default "Global"
            One of ``Global``, ``Entity``, ``EntityCollection`` (or 0/1/2).
        bound_entity_logical_name : str, optional
            Logical name required when binding_type is Entity or EntityCollection.
        allowed_custom_processing_step_type : int, default 0
            Allowed custom processing step type (0 = None, 1 = Plug-in, etc. per platform option set) – typically leave 0.
        execute_privilege_name : str, optional
            Privilege name required to execute (rare; use to gate execution by security role privilege).
        payload : dict, optional
            Raw body overrides/extra fields to merge; values here win only if not already set by convenience params.
        request_parameters : list[dict], optional
            Inline definitions for CustomAPIRequestParameters.
            (e.g. uniquename, name, displayname, description, type (option set int), isoptional, logicalentityname, iscustomizable={"Value": bool}).
        response_properties : list[dict], optional
            Inline definitions for CustomAPIResponseProperties (same shape as request parameters, minus isoptional).
        plugin_type_id : str, optional
            GUID of an existing plugintype to bind via ``PluginTypeId@odata.bind`` so the API executes that plug-in.
        is_private : bool, default False
            Marks the Custom API as private (hidden from some discovery scenarios).
        is_customizable : bool, optional
            When provided wraps into ``{"Value": bool}`` to set the metadata customizability flag.

        Notes
        -----
        This does not register any plug-in code; invocation will only succeed if server logic exists.
        """
        if self._get_custom_api(unique_name=unique_name):
            raise RuntimeError(f"Custom API '{unique_name}' already exists")
        body: Dict[str, Any] = payload.copy() if payload else {}
        body.setdefault("uniquename", unique_name)
        body.setdefault("name", name)
        body.setdefault("displayname", name)
        if description:
            body.setdefault("description", description)
        body.setdefault("isfunction", bool(is_function))
        body.setdefault("isprivate", bool(is_private))
        # bindingtype expects an int (0=Global,1=Entity,2=EntityCollection)
        if isinstance(binding_type, str):
            bt_map = {"global": 0, "entity": 1, "entitycollection": 2}
            bt_key = binding_type.lower().strip()
            binding_type_value = bt_map.get(bt_key)
            if binding_type_value is None:
                raise ValueError("binding_type must be one of Global, Entity, EntityCollection or an int 0/1/2")
            body.setdefault("bindingtype", binding_type_value)
        else:
            body.setdefault("bindingtype", binding_type)
        if bound_entity_logical_name:
            body.setdefault("boundentitylogicalname", bound_entity_logical_name)
        body.setdefault("allowedcustomprocessingsteptype", allowed_custom_processing_step_type)
        if execute_privilege_name:
            body.setdefault("executeprivilegename", execute_privilege_name)
        if is_customizable is not None:
            body.setdefault("iscustomizable", {"Value": bool(is_customizable)})
        if plugin_type_id:
            body.setdefault("PluginTypeId@odata.bind", f"/plugintypes({plugin_type_id})")

        if request_parameters:
            body["CustomAPIRequestParameters"] = request_parameters
        if response_properties:
            body["CustomAPIResponseProperties"] = response_properties

        url = f"{self.api}/customapis"
        headers = self._headers().copy()
        headers["Prefer"] = "return=representation"
        r = self._request("post", url, headers=headers, json=body)
        r.raise_for_status()
        created: Dict[str, Any]
        if r.status_code == 204 or not r.content:
            # Representation not returned; do a lookup by unique name
            created = self._get_custom_api(unique_name=unique_name) or {"uniquename": unique_name}
        else:
            try:
                created = r.json()
            except Exception:  # noqa: BLE001
                created = {"uniquename": unique_name}
        return created

    def update_custom_api(self, *, unique_name: Optional[str] = None, customapiid: Optional[str] = None, changes: Dict[str, Any]) -> Dict[str, Any]:
        """Patch an existing Custom API."""
        rec = self._get_custom_api(unique_name=unique_name, customapiid=customapiid)
        if not rec:
            raise RuntimeError("Custom API not found")
        cid = rec.get("customapiid")
        url = f"{self.api}/customapis({cid})"
        headers = self._headers().copy()
        headers["If-Match"] = "*"
        # Prefer return representation
        headers["Prefer"] = "return=representation"
        r = self._request("patch", url, headers=headers, json=changes)
        r.raise_for_status()
        return r.json()

    def delete_custom_api(self, *, unique_name: Optional[str] = None, customapiid: Optional[str] = None) -> None:
        rec = self._get_custom_api(unique_name=unique_name, customapiid=customapiid)
        if not rec:
            return
        cid = rec.get("customapiid")
        url = f"{self.api}/customapis({cid})"
        headers = self._headers().copy()
        headers["If-Match"] = "*"
        r = self._request("delete", url, headers=headers)
        if r.status_code not in (200, 204, 404):
            r.raise_for_status()

    # ---------------------- Custom API invocation ----------------------
    def call_custom_api(self, name: str, parameters: Optional[Dict[str, Any]] = None, *, is_function: Optional[bool] = None) -> Any:
        """Invoke a custom API by its unique name.

        Parameters
        ----------
        name : str
            Unique name of the custom API.
        parameters : dict, optional
            Key/value pairs of parameters. For functions these are sent as query string; for actions in the body.
        is_function : bool, optional
            If not provided, is_function lookup is performed.
        """
        params = parameters or {}
        # Determine if function or action
        fn_flag = is_function
        if fn_flag is None:
            meta = self._get_custom_api(unique_name=name)
            if not meta:
                raise RuntimeError(f"Custom API '{name}' not found")
            fn_flag = bool(meta.get("isfunction"))
        try:
            if fn_flag:
                # Function -> GET with query string parameters (primitive only)
                if params:
                    def format_val(v: Any) -> str:
                        if isinstance(v, str):
                            return f"'{v.replace("'", "''")}'"
                        if isinstance(v, bool):
                            return "true" if v else "false"
                        return str(v)
                    inner = ",".join(f"{k}={format_val(v)}" for k, v in params.items())
                    url = f"{self.api}/{name}({inner})"
                else:
                    url = f"{self.api}/{name}()"
                r = self._request("get", url, headers=self._headers())
            else:
                # Action -> POST with JSON body (parameters serialized directly)
                url = f"{self.api}/{name}"
                r = self._request("post", url, headers=self._headers(), json=params if params else None)
            r.raise_for_status()
        except Exception as ex:
            # Try to surface server diagnostic if available
            resp = getattr(ex, 'response', None)
            if resp is not None:
                try:
                    detail = resp.text[:1000]
                    raise RuntimeError(f"Custom API call failed ({resp.status_code}) body={detail}") from ex
                except Exception:
                    pass
            raise
        # Some custom APIs return no content (204)
        if r.status_code == 204 or not r.content:
            return None
        ct = r.headers.get("Content-Type", "")
        if "application/json" in ct:
            try:
                return r.json()
            except Exception:  # noqa: BLE001
                return r.text
        return r.text

    # ----------------- Custom API request parameters -------------------
    _DATA_TYPE_MAP = {
        "boolean": 0,
        "datetime": 1,
        "decimal": 2,
        "entity": 3,
        "entitycollection": 4,
        "float": 5,
        "int": 6,
        "integer": 6,
        "money": 7,
        "picklist": 8,
        "string": 9,
        "stringarray": 10,
        "guid": 11,
        "entityreference": 12,
        "entityreferencecollection": 13,
        "bigint": 14,
    }

    def _resolve_data_type(self, data_type: Any) -> Any:
        if isinstance(data_type, int):
            return data_type
        if isinstance(data_type, str):
            k = data_type.lower().strip()
            if k in self._DATA_TYPE_MAP:
                return self._DATA_TYPE_MAP[k]
        return data_type  # let server validate

    def list_custom_api_request_parameters(self, customapiid: str) -> List[Dict[str, Any]]:
        params = {
            "$select": "customapirequestparameterid,uniquename,name,type,isoptional",
            "$filter": f"_customapiid_value eq {customapiid}",
        }
        url = f"{self.api}/customapirequestparameters"
        r = self._request("get", url, headers=self._headers(), params=params)
        r.raise_for_status()
        return r.json().get("value", [])

    def create_custom_api_request_parameter(
        self,
        *,
        customapiid: Optional[str] = None,
        custom_api_unique_name: Optional[str] = None,
        unique_name: str,
        name: Optional[str] = None,
        data_type: Any = "string",
        description: Optional[str] = None,
        is_optional: bool = False,
        logical_entity_name: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create a Custom API Request Parameter.

        If customapiid is not supplied, custom_api_unique_name is resolved first.
        data_type can be an int (raw option set) or a friendly string (see _DATA_TYPE_MAP).
        """
        if not customapiid:
            if not custom_api_unique_name:
                raise ValueError("Provide customapiid or custom_api_unique_name")
            api_meta = self._get_custom_api(unique_name=custom_api_unique_name)
            if not api_meta:
                raise RuntimeError(f"Custom API '{custom_api_unique_name}' not found")
            customapiid = api_meta.get("customapiid")
        body: Dict[str, Any] = (extra or {}).copy()
        body.setdefault("uniquename", unique_name)
        body.setdefault("name", name or unique_name)
        body.setdefault("displayname", name or unique_name)
        body.setdefault("isoptional", bool(is_optional))
        body.setdefault("type", self._resolve_data_type(data_type))
        if description:
            body.setdefault("description", description)
        if logical_entity_name:
            body.setdefault("logicalentityname", logical_entity_name)
        # Associate to parent Custom API via navigation property
        body.setdefault("customapiid@odata.bind", f"/customapis({customapiid})")
        url = f"{self.api}/customapirequestparameters"
        r = self._request("post", url, headers=self._headers(), json=body)
        r.raise_for_status()
        return r.json()

    def delete_custom_api_request_parameter(self, request_parameter_id: str) -> None:
        url = f"{self.api}/customapirequestparameters({request_parameter_id})"
        headers = self._headers().copy()
        headers["If-Match"] = "*"
        r = self._request("delete", url, headers=headers)
        if r.status_code not in (200, 204, 404):
            r.raise_for_status()

    # --------------- Custom API response properties --------------------
    def list_custom_api_response_properties(self, customapiid: str) -> List[Dict[str, Any]]:
        params = {
            "$select": "customapiresponsepropertyid,uniquename,name,type",
            "$filter": f"_customapiid_value eq {customapiid}",
        }
        url = f"{self.api}/customapiresponseproperties"
        r = self._request("get", url, headers=self._headers(), params=params)
        r.raise_for_status()
        return r.json().get("value", [])

    def create_custom_api_response_property(
        self,
        *,
        customapiid: Optional[str] = None,
        custom_api_unique_name: Optional[str] = None,
        unique_name: str,
        name: Optional[str] = None,
        data_type: Any = "string",
        description: Optional[str] = None,
        logical_entity_name: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create a Custom API Response Property.

        If customapiid is not supplied, custom_api_unique_name is resolved first.
        data_type can be an int (raw option set) or a friendly string (see _DATA_TYPE_MAP).
        """
        if not customapiid:
            if not custom_api_unique_name:
                raise ValueError("Provide customapiid or custom_api_unique_name")
            api_meta = self._get_custom_api(unique_name=custom_api_unique_name)
            if not api_meta:
                raise RuntimeError(f"Custom API '{custom_api_unique_name}' not found")
            customapiid = api_meta.get("customapiid")
        body: Dict[str, Any] = (extra or {}).copy()
        body.setdefault("uniquename", unique_name)
        body.setdefault("name", name or unique_name)
        body.setdefault("displayname", name or unique_name)
        body.setdefault("type", self._resolve_data_type(data_type))
        if description:
            body.setdefault("description", description)
        if logical_entity_name:
            body.setdefault("logicalentityname", logical_entity_name)
        body.setdefault("customapiid@odata.bind", f"/customapis({customapiid})")
        url = f"{self.api}/customapiresponseproperties"
        r = self._request("post", url, headers=self._headers(), json=body)
        r.raise_for_status()
        return r.json()

    def delete_custom_api_response_property(self, response_property_id: str) -> None:
        url = f"{self.api}/customapiresponseproperties({response_property_id})"
        headers = self._headers().copy()
        headers["If-Match"] = "*"
        r = self._request("delete", url, headers=headers)
        if r.status_code not in (200, 204, 404):
            r.raise_for_status()
