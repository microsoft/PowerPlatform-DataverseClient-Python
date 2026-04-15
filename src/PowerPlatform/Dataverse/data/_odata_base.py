# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Pure (I/O-free) helpers shared between the sync and async OData clients.

:class:`_ODataBase` contains every method that neither makes HTTP requests nor
calls any method that does.  Both :class:`~PowerPlatform.Dataverse.data._odata._ODataClient`
(sync) and :class:`~PowerPlatform.Dataverse.aio.data._async_odata._AsyncODataClient`
(async) inherit from this class and pick up all helpers without duplication.

Subclasses are responsible for initialising the instance attributes that these
methods reference:

* ``self.api``  — full API root URL (``"{base_url}/api/data/v9.2"``)
* ``self.config`` — :class:`~PowerPlatform.Dataverse.core.config.DataverseConfig` instance
* ``self._picklist_label_cache`` — ``dict[str, dict]`` picklist label cache
* ``self._picklist_cache_ttl_seconds`` — ``int`` TTL in seconds (default 3600)
"""

from __future__ import annotations

import json
import re
import unicodedata
from enum import Enum
from typing import Any, Dict, List, Optional

from ..common.constants import CASCADE_BEHAVIOR_REMOVE_LINK
from ..core._error_codes import VALIDATION_UNSUPPORTED_CACHE_KIND, VALIDATION_UNSUPPORTED_COLUMN_TYPE
from ..core.errors import ValidationError
from ..models.labels import Label, LocalizedLabel
from ..models.relationship import CascadeConfiguration, LookupAttributeMetadata, OneToManyRelationshipMetadata
from ._raw_request import _RawRequest


class _ODataBase:
    """Pure (I/O-free) helper methods shared by sync and async OData clients.

    This class has no ``__init__``; subclasses set up all instance attributes.
    """

    # ------------------------------------------------------------------
    # Static string / key helpers
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
        """
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
        rel_name = f"{referenced_table}_{referencing_table}_{lookup_field_name}"
        relationship = OneToManyRelationshipMetadata(
            schema_name=rel_name,
            referenced_entity=referenced_table,
            referencing_entity=referencing_table,
            referenced_attribute=f"{referenced_table}id",
            cascade_configuration=CascadeConfiguration(delete=cascade_delete),
        )
        return lookup, relationship

    # ------------------------------------------------------------------
    # Instance key / URL helpers (use self.api or self._escape_odata_quotes)
    # ------------------------------------------------------------------

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

    # ------------------------------------------------------------------
    # Label / metadata payload builders (use self.config.language_code)
    # ------------------------------------------------------------------

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
    # Pure _build_* methods (assemble _RawRequest without I/O)
    # ------------------------------------------------------------------

    def _build_create_entity(
        self,
        table: str,
        columns: Dict[str, Any],
        solution: Optional[str] = None,
        primary_column: Optional[str] = None,
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
        body = {
            "@odata.type": "Microsoft.Dynamics.CRM.EntityMetadata",
            "SchemaName": table,
            "DisplayName": self._label(table),
            "DisplayCollectionName": self._label(table + "s"),
            "Description": self._label(f"Custom entity for {table}"),
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
    # Cache maintenance
    # ------------------------------------------------------------------

    def _flush_cache(self, kind: Any) -> int:
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
