# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Shared pure-logic base for the Dataverse OData client. Contains no I/O.

Subclasses add the HTTP transport layer (sync or async) while sharing all
URL construction, payload building, cache helpers, and other stateless logic.
"""

from __future__ import annotations

import json
import re
import unicodedata
import uuid
import warnings
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union
from urllib.parse import parse_qs, urlparse

from .. import __version__ as _SDK_VERSION

from ..core.errors import ValidationError
from ..core._error_codes import (
    VALIDATION_UNSUPPORTED_COLUMN_TYPE,
    VALIDATION_UNSUPPORTED_CACHE_KIND,
    VALIDATION_SQL_WRITE_BLOCKED,
    VALIDATION_SQL_UNSUPPORTED_SYNTAX,
)
from ..models.relationship import (
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    CascadeConfiguration,
)
from ..models.labels import Label, LocalizedLabel
from ..common.constants import CASCADE_BEHAVIOR_REMOVE_LINK
from ._raw_request import _RawRequest

__all__ = []

_GUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
_CALL_SCOPE_CORRELATION_ID: ContextVar[Optional[str]] = ContextVar("_CALL_SCOPE_CORRELATION_ID", default=None)
_USER_AGENT = f"DataverseSvcPythonClient:{_SDK_VERSION}"
_DEFAULT_EXPECTED_STATUSES: tuple[int, ...] = (200, 201, 202, 204)


def _extract_pagingcookie(next_link: str) -> Optional[str]:
    """Extract the raw pagingcookie value from a SQL ``@odata.nextLink`` URL.

    The Dataverse SQL endpoint has a server-side bug where the pagingcookie
    (containing first/last record GUIDs) does not advance between pages even
    though ``pagenumber`` increments. Detecting a repeated cookie lets the
    pagination loop break instead of looping indefinitely.

    Returns the pagingcookie string if present, or ``None`` if not found.
    """
    try:
        qs = parse_qs(urlparse(next_link).query)
        skiptoken = qs.get("$skiptoken", [None])[0]
        if not skiptoken:
            return None
        # parse_qs already URL-decodes the value once, giving the outer XML with
        # pagingcookie still percent-encoded (e.g. pagingcookie="%3ccookie...").
        # A second decode is intentionally omitted: decoding again would turn %22
        # into " inside the cookie XML, breaking the regex and causing every page
        # to extract the same truncated prefix regardless of the actual GUIDs.
        m = re.search(r'pagingcookie="([^"]+)"', skiptoken)
        if m:
            return m.group(1)
    except Exception:
        pass
    return None


@dataclass
class _RequestContext:
    """Structured request context used by ``_request`` to clarify payload and metadata."""

    method: str
    url: str
    expected: tuple[int, ...] = _DEFAULT_EXPECTED_STATUSES
    headers: Optional[Dict[str, str]] = None
    kwargs: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def build(
        cls,
        method: str,
        url: str,
        *,
        expected: tuple[int, ...] = _DEFAULT_EXPECTED_STATUSES,
        merge_headers: Optional[Callable[[Optional[Dict[str, str]]], Dict[str, str]]] = None,
        **kwargs: Any,
    ) -> "_RequestContext":
        headers = kwargs.get("headers")
        headers = merge_headers(headers) if merge_headers else (headers or {})
        headers.setdefault("x-ms-client-request-id", str(uuid.uuid4()))
        headers.setdefault("x-ms-correlation-id", _CALL_SCOPE_CORRELATION_ID.get())
        kwargs["headers"] = headers
        return cls(
            method=method,
            url=url,
            expected=expected,
            headers=headers,
            kwargs=kwargs or {},
        )


class _ODataBase:
    """Pure-logic base for the Dataverse OData client.

    Provides URL construction, cache management, payload builders, and other
    stateless or cache-only helpers.  No I/O is performed here; subclasses
    must supply ``_request`` and the rest of the HTTP transport layer.
    """

    def __init__(self, base_url: str, config=None) -> None:
        """Initialise shared state: URL, API root, config, in-memory caches, and HTTP logger.

        :param base_url: Organisation base URL (e.g. ``"https://<org>.crm.dynamics.com"``).
        :type base_url: :class:`str`
        :param config: Optional Dataverse configuration (HTTP retry, backoff, timeout,
            language code, HTTP diagnostic logging). If omitted, ``DataverseConfig.from_env()`` is used.
        :type config: ~PowerPlatform.Dataverse.core.config.DataverseConfig or None
        :raises ValueError: If ``base_url`` is empty after stripping.
        """
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
        self._logical_to_entityset_cache: dict[str, str] = {}
        # Cache: normalized table_schema_name (lowercase) -> primary id attribute (e.g. accountid)
        self._logical_primaryid_cache: dict[str, str] = {}
        self._picklist_label_cache: dict[str, dict] = {}
        self._picklist_cache_ttl_seconds = 3600  # 1 hour TTL
        self._http_logger = None
        if self.config.log_config is not None:
            from ..core._http_logger import _HttpLogger

            self._http_logger = _HttpLogger(self.config.log_config)

    # ------------------------------------------------------------------
    # Static helpers
    # ------------------------------------------------------------------

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

        Keys containing ``@odata.`` (e.g. ``new_CustomerId@odata.bind``) are
        preserved as-is because the navigation property portion before ``@``
        must retain its original casing (case-sensitive navigation property name).  The OData
        parser validates ``@odata.bind`` property names **case-sensitively**
        against the entity's declared navigation properties, so lowercasing
        these keys causes ``400 - undeclared property`` errors.
        """
        if not isinstance(record, dict):
            return record
        return {k.lower() if isinstance(k, str) and "@odata." not in k else k: v for k, v in record.items()}

    @staticmethod
    def _lowercase_list(items: Optional[List[str]]) -> Optional[List[str]]:
        """Convert all strings in a list to lowercase for case-insensitive column names.

        Used for $select, $orderby, $expand parameters where column names must be lowercase.
        """
        if not items:
            return items
        return [item.lower() if isinstance(item, str) else item for item in items]

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

    # ------------------------------------------------------------------
    # Instance helpers
    # ------------------------------------------------------------------

    @contextmanager
    def _call_scope(self):
        """Context manager to generate a new correlation id for each SDK call scope."""
        shared_id = str(uuid.uuid4())
        token = _CALL_SCOPE_CORRELATION_ID.set(shared_id)
        try:
            yield shared_id
        finally:
            _CALL_SCOPE_CORRELATION_ID.reset(token)

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

    def _build_alternate_key_str(self, alternate_key: Dict[str, Any]) -> str:
        """Build an OData alternate key segment from a mapping of key names to values.

        String values are single-quoted and escaped; all other values are rendered as-is.

        :param alternate_key: Mapping of alternate key attribute names to their values.
            Must be a non-empty dict with string keys.
        :type alternate_key: ``dict[str, Any]``

        :return: Comma-separated key=value pairs suitable for use in a URL segment.
        :rtype: ``str``

        :raises ValueError: If ``alternate_key`` is empty.
        :raises TypeError: If any key in ``alternate_key`` is not a string.
        """
        if not alternate_key:
            raise ValueError("alternate_key must be a non-empty dict")
        bad_keys = [k for k in alternate_key if not isinstance(k, str)]
        if bad_keys:
            raise TypeError(f"alternate_key keys must be strings; got: {bad_keys!r}")
        parts = []
        for k, v in alternate_key.items():
            k_lower = k.lower() if isinstance(k, str) else k
            if isinstance(v, str):
                v_escaped = self._escape_odata_quotes(v)
                parts.append(f"{k_lower}='{v_escaped}'")
            else:
                parts.append(f"{k_lower}={v}")
        return ",".join(parts)

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

    # ------------------------------------------------------------------
    # Payload builders (no I/O)
    # ------------------------------------------------------------------

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
        if dtype_l in ("memo", "multiline"):
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.MemoAttributeMetadata",
                "SchemaName": column_schema_name,
                "DisplayName": self._label(label),
                "RequiredLevel": {"Value": "None"},
                "MaxLength": 4000,
                "FormatName": {"Value": "Text"},
                "ImeMode": "Auto",
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
        if dtype_l == "file":
            return {
                "@odata.type": "Microsoft.Dynamics.CRM.FileAttributeMetadata",
                "SchemaName": column_schema_name,
                "DisplayName": self._label(label),
                "RequiredLevel": {"Value": "None"},
            }
        return None

    # ------------------------------------------------------------------
    # Entity / column / relationship _build_* methods (no I/O)
    # ------------------------------------------------------------------

    def _build_create_entity(
        self,
        table: str,
        columns: Dict[str, Any],
        solution: Optional[str] = None,
        primary_column: Optional[str] = None,
        display_name: Optional[str] = None,
    ) -> _RawRequest:
        """Build an EntityDefinitions POST request without sending it."""
        if primary_column:
            primary_attr = primary_column
        else:
            primary_attr = f"{table.split('_', 1)[0]}_Name" if "_" in table else "new_Name"
        attributes = [self._attribute_payload(primary_attr, "string", is_primary_name=True)]
        for col_name, dtype in columns.items():
            attr = self._attribute_payload(col_name, dtype)
            if not attr:
                raise ValidationError(
                    f"Unsupported column type '{dtype}' for column '{col_name}'.",
                    subcode=VALIDATION_UNSUPPORTED_COLUMN_TYPE,
                )
            attributes.append(attr)
        if display_name is not None:
            if not isinstance(display_name, str) or not display_name.strip():
                raise TypeError("display_name must be a non-empty string when provided")
        label = display_name if display_name is not None else table
        body = {
            "@odata.type": "Microsoft.Dynamics.CRM.EntityMetadata",
            "SchemaName": table,
            "DisplayName": self._label(label),
            "DisplayCollectionName": self._label(label + "s"),
            "Description": self._label(f"Custom entity for {label}"),
            "OwnershipType": "UserOwned",
            "HasActivities": False,
            "HasNotes": True,
            "IsActivity": False,
            "Attributes": attributes,
        }
        url = f"{self.api}/EntityDefinitions"
        if solution:
            url += f"?SolutionUniqueName={solution}"
        return _RawRequest(
            method="POST",
            url=url,
            body=json.dumps(body, ensure_ascii=False),
        )

    def _build_delete_entity(self, metadata_id: str) -> _RawRequest:
        """Build an EntityDefinitions DELETE request without sending it."""
        return _RawRequest(
            method="DELETE",
            url=f"{self.api}/EntityDefinitions({metadata_id})",
            headers={"If-Match": "*"},
        )

    def _build_get_entity(self, table: str) -> _RawRequest:
        """Build an EntityDefinitions GET request without sending it."""
        logical = self._escape_odata_quotes(table.lower())
        return _RawRequest(
            method="GET",
            url=(
                f"{self.api}/EntityDefinitions"
                f"?$select=MetadataId,LogicalName,SchemaName,EntitySetName,PrimaryNameAttribute,PrimaryIdAttribute"
                f"&$filter=LogicalName eq '{logical}'"
            ),
        )

    def _build_list_entities(
        self,
        *,
        filter: Optional[str] = None,
        select: Optional[List[str]] = None,
    ) -> _RawRequest:
        """Build an EntityDefinitions list GET request without sending it."""
        base_filter = "IsPrivate eq false"
        if filter:
            combined_filter = f"{base_filter} and ({filter})"
        else:
            combined_filter = base_filter
        url = f"{self.api}/EntityDefinitions?$filter={combined_filter}"
        if select is not None and isinstance(select, str):
            raise TypeError("select must be a list of property names, not a bare string")
        if select:
            url += "&$select=" + ",".join(select)
        return _RawRequest(method="GET", url=url)

    def _build_create_column(
        self,
        entity_metadata_id: str,
        col_name: str,
        dtype: Any,
    ) -> _RawRequest:
        """Build an Attributes POST request for one column without sending it."""
        attr = self._attribute_payload(col_name, dtype)
        if not attr:
            raise ValidationError(
                f"Unsupported column type '{dtype}' for column '{col_name}'.",
                subcode=VALIDATION_UNSUPPORTED_COLUMN_TYPE,
            )
        return _RawRequest(
            method="POST",
            url=f"{self.api}/EntityDefinitions({entity_metadata_id})/Attributes",
            body=json.dumps(attr, ensure_ascii=False),
        )

    def _build_delete_column(
        self,
        entity_metadata_id: str,
        col_metadata_id: str,
    ) -> _RawRequest:
        """Build an Attributes DELETE request for one column without sending it."""
        return _RawRequest(
            method="DELETE",
            url=f"{self.api}/EntityDefinitions({entity_metadata_id})/Attributes({col_metadata_id})",
            headers={"If-Match": "*"},
        )

    @staticmethod
    def _build_lookup_field_models(
        referencing_table: str,
        lookup_field_name: str,
        referenced_table: str,
        *,
        display_name: Optional[str] = None,
        description: Optional[str] = None,
        required: bool = False,
        cascade_delete: str = CASCADE_BEHAVIOR_REMOVE_LINK,
        language_code: int = 1033,
    ) -> tuple:
        """Build a (lookup, relationship) pair for a lookup field creation.

        Returns ``(LookupAttributeMetadata, OneToManyRelationshipMetadata)``.
        Used by both the batch resolver and ``TableOperations.create_lookup_field``
        to avoid duplicating the metadata assembly logic.

        Note: ``referencing_table`` and ``referenced_table`` are lowercased
        automatically because Dataverse stores entity logical names in
        lowercase.  ``lookup_field_name`` is kept as-is (it is a SchemaName).
        """
        # Dataverse logical names are always lowercase.  Callers may pass
        # SchemaName-cased values (e.g. "new_SQLTeam"); normalise here so
        # the relationship metadata uses valid logical names.
        referencing_lower = referencing_table.lower()
        referenced_lower = referenced_table.lower()

        lookup = LookupAttributeMetadata(
            schema_name=lookup_field_name,
            display_name=Label(
                localized_labels=[
                    LocalizedLabel(
                        label=display_name or referenced_table,
                        language_code=language_code,
                    )
                ]
            ),
            required_level="ApplicationRequired" if required else "None",
        )
        if description:
            lookup.description = Label(
                localized_labels=[LocalizedLabel(label=description, language_code=language_code)]
            )
        rel_name = f"{referenced_lower}_{referencing_lower}_{lookup_field_name}"
        relationship = OneToManyRelationshipMetadata(
            schema_name=rel_name,
            referenced_entity=referenced_lower,
            referencing_entity=referencing_lower,
            referenced_attribute=f"{referenced_lower}id",
            cascade_configuration=CascadeConfiguration(delete=cascade_delete),
        )
        return lookup, relationship

    def _build_create_relationship(
        self,
        body: Dict[str, Any],
        *,
        solution: Optional[str] = None,
    ) -> _RawRequest:
        """Build a RelationshipDefinitions POST request without sending it."""
        headers: Dict[str, str] = {}
        if solution:
            headers["MSCRM.SolutionUniqueName"] = solution
        return _RawRequest(
            method="POST",
            url=f"{self.api}/RelationshipDefinitions",
            body=json.dumps(body, ensure_ascii=False),
            headers=headers or None,
        )

    def _build_delete_relationship(self, relationship_id: str) -> _RawRequest:
        """Build a RelationshipDefinitions DELETE request without sending it."""
        return _RawRequest(
            method="DELETE",
            url=f"{self.api}/RelationshipDefinitions({relationship_id})",
            headers={"If-Match": "*"},
        )

    def _build_get_relationship(self, schema_name: str) -> _RawRequest:
        """Build a RelationshipDefinitions GET request without sending it."""
        escaped = self._escape_odata_quotes(schema_name)
        return _RawRequest(
            method="GET",
            url=f"{self.api}/RelationshipDefinitions?$filter=SchemaName eq '{escaped}'",
        )

    # ------------------------------------------------------------------
    # SQL guardrails
    # ------------------------------------------------------------------

    _SQL_WRITE_RE = re.compile(
        r"^\s*(?:INSERT|UPDATE|DELETE|DROP|TRUNCATE|ALTER|CREATE|EXEC|GRANT|REVOKE|BULK)\b",
        re.IGNORECASE,
    )
    _SQL_COMMENT_RE = re.compile(r"/\*[^*]*\*+(?:[^/*][^*]*\*+)*/|--[^\n]*", re.DOTALL)
    _SQL_LEADING_WILDCARD_RE = re.compile(r"\bLIKE\s+'%[^']", re.IGNORECASE)
    _SQL_IMPLICIT_CROSS_JOIN_RE = re.compile(
        r"\bFROM\s+[A-Za-z0-9_]+(?:\s+[A-Za-z0-9_]+)?\s*,\s*[A-Za-z0-9_]+",
        re.IGNORECASE,
    )
    _SQL_UNSUPPORTED_JOIN_RE = re.compile(
        r"\b(?:CROSS\s+JOIN|RIGHT\s+(?:OUTER\s+)?JOIN|FULL\s+(?:OUTER\s+)?JOIN)\b",
        re.IGNORECASE,
    )
    _SQL_UNION_RE = re.compile(r"\bUNION\b", re.IGNORECASE)
    _SQL_HAVING_RE = re.compile(r"\bHAVING\b", re.IGNORECASE)
    _SQL_CTE_RE = re.compile(r"^\s*WITH\b", re.IGNORECASE)
    _SQL_SUBQUERY_RE = re.compile(
        r"\bIN\s*\(\s*SELECT\b|\bEXISTS\s*\(\s*SELECT\b|\(\s*SELECT\b.*\bFROM\b",
        re.IGNORECASE,
    )
    # SELECT * is intentionally rejected -- not a technical limitation but a
    # deliberate design decision.  Wide entities (e.g. account has 307 columns)
    # make SELECT * extremely expensive on shared database infrastructure.
    # COUNT(*) is NOT matched because COUNT appears before the *.
    _SQL_SELECT_STAR_RE = re.compile(
        r"\bSELECT\b\s+(?:DISTINCT\s+)?(?:TOP\s+\d+(?:\s+PERCENT)?\s+)?\*\s",
        re.IGNORECASE,
    )

    def _sql_guardrails(self, sql: str) -> str:
        """Apply safety guardrails to a SQL query before sending to the server.

        Checks split into two categories:

        **Blocked** (``ValidationError`` -- saves a server round-trip):

        1. Write statements (INSERT/UPDATE/DELETE/DROP/etc.)
        2. CROSS JOIN, RIGHT JOIN, FULL OUTER JOIN (server rejects these)
        3. UNION / UNION ALL (server rejects)
        4. HAVING clause (server rejects)
        5. CTE / WITH clause (server rejects)
        6. Subqueries -- IN (SELECT ...), EXISTS (SELECT ...) (server rejects)
        7. SELECT * -- intentional design decision, not a technical limitation.
           Wide entities make wildcard selects extremely expensive on shared
           database infrastructure.  ``COUNT(*)`` is not affected.

        **Warned** (``UserWarning`` -- query still executes):

        8. Leading-wildcard LIKE (full table scan)
        9. Implicit cross join FROM a, b (cartesian product)

        All blocked patterns are also blocked by the server, but catching
        them here saves the network round-trip and provides clearer error
        messages.

        :param sql: The SQL string (already stripped).
        :return: The SQL string (unchanged).
        :raises ValidationError: If the SQL contains a blocked pattern.
        """
        sql_no_comments = self._SQL_COMMENT_RE.sub(" ", sql).strip()
        if self._SQL_WRITE_RE.search(sql_no_comments):
            raise ValidationError(
                "SQL endpoint is read-only. Use client.records or "
                "client.dataframe for write operations "
                "(INSERT/UPDATE/DELETE are not supported).",
                subcode=VALIDATION_SQL_WRITE_BLOCKED,
            )
        m = self._SQL_UNSUPPORTED_JOIN_RE.search(sql)
        if m:
            raise ValidationError(
                f"Unsupported JOIN type: '{m.group(0).strip()}'. "
                "Only INNER JOIN and LEFT JOIN are supported by the "
                "Dataverse SQL endpoint.",
                subcode=VALIDATION_SQL_UNSUPPORTED_SYNTAX,
            )
        if self._SQL_UNION_RE.search(sql):
            raise ValidationError(
                "UNION is not supported by the Dataverse SQL endpoint. "
                "Execute separate queries and combine results in Python "
                "(e.g. pd.concat([df1, df2])).",
                subcode=VALIDATION_SQL_UNSUPPORTED_SYNTAX,
            )
        if self._SQL_HAVING_RE.search(sql):
            raise ValidationError(
                "HAVING is not supported by the Dataverse SQL endpoint. "
                "Use WHERE to filter before GROUP BY instead.",
                subcode=VALIDATION_SQL_UNSUPPORTED_SYNTAX,
            )
        if self._SQL_CTE_RE.search(sql):
            raise ValidationError(
                "CTE (WITH ... AS) is not supported by the Dataverse SQL "
                "endpoint. Use separate queries and combine in Python.",
                subcode=VALIDATION_SQL_UNSUPPORTED_SYNTAX,
            )
        if self._SQL_SUBQUERY_RE.search(sql):
            raise ValidationError(
                "Subqueries are not supported by the Dataverse SQL "
                "endpoint. Use separate SQL calls and combine results "
                "in Python (e.g. step 1: get IDs, step 2: WHERE IN).",
                subcode=VALIDATION_SQL_UNSUPPORTED_SYNTAX,
            )
        if self._SQL_SELECT_STAR_RE.search(sql):
            raise ValidationError(
                "SELECT * is not supported. Specify column names explicitly "
                "(e.g. SELECT name, revenue FROM account). "
                "Use client.query.sql_columns('account') to discover available columns.",
                subcode=VALIDATION_SQL_UNSUPPORTED_SYNTAX,
            )
        if self._SQL_LEADING_WILDCARD_RE.search(sql):
            warnings.warn(
                "Query contains a leading-wildcard LIKE pattern "
                "(e.g. LIKE '%value'). This forces a full table scan "
                "and may degrade performance on large tables. "
                "Prefer trailing wildcards (LIKE 'value%') when possible.",
                UserWarning,
                stacklevel=4,
            )
        if self._SQL_IMPLICIT_CROSS_JOIN_RE.search(sql):
            warnings.warn(
                "Query uses an implicit cross join (FROM table1, table2). "
                "This produces a cartesian product that can generate "
                "millions of intermediate rows and degrade shared database "
                "performance. Use explicit JOIN...ON syntax instead: "
                "FROM table1 a JOIN table2 b ON a.column = b.column",
                UserWarning,
                stacklevel=4,
            )
        return sql

    # ------------------------------------------------------------------
    # Cache maintenance
    # ------------------------------------------------------------------

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
