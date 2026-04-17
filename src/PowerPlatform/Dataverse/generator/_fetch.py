# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Metadata fetching helpers for the entity class generator.

Thin wrappers around the Dataverse Web API ``EntityDefinitions`` and
``Attributes`` endpoints.  All network calls go through the existing
``_ODataClient`` so auth, retries, and logging are inherited for free.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ..data._odata import _ODataClient

# ---------------------------------------------------------------------------
# Attribute type → (python_type_str, dataverse_type_hint) mapping
# ---------------------------------------------------------------------------

#: Maps the ``@odata.type`` fragment (without the ``#`` prefix) to a tuple of
#: ``(python_type, dataverse_type_hint)``.
#:
#: ``python_type`` is the string used in generated ``Field(...)`` calls.
#: ``dataverse_type_hint`` is the ``dataverse_type`` kwarg value; ``None`` means
#: the kwarg is omitted (e.g. for the primary-key GUID column).
ATTR_TYPE_MAP: Dict[str, tuple[str, Optional[str]]] = {
    "Microsoft.Dynamics.CRM.StringAttributeMetadata":          ("str",   "string"),
    "Microsoft.Dynamics.CRM.MemoAttributeMetadata":            ("str",   "memo"),
    "Microsoft.Dynamics.CRM.IntegerAttributeMetadata":         ("int",   "int"),
    "Microsoft.Dynamics.CRM.BigIntAttributeMetadata":          ("int",   "int"),
    "Microsoft.Dynamics.CRM.DecimalAttributeMetadata":         ("float", "decimal"),
    "Microsoft.Dynamics.CRM.DoubleAttributeMetadata":          ("float", "float"),
    "Microsoft.Dynamics.CRM.MoneyAttributeMetadata":           ("float", "decimal"),
    "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata":        ("str",   "datetime"),
    "Microsoft.Dynamics.CRM.BooleanAttributeMetadata":         ("bool",  "bool"),
    "Microsoft.Dynamics.CRM.PicklistAttributeMetadata":        ("int",   "picklist"),
    "Microsoft.Dynamics.CRM.StateAttributeMetadata":           ("int",   "picklist"),
    "Microsoft.Dynamics.CRM.StatusAttributeMetadata":          ("int",   "picklist"),
    "Microsoft.Dynamics.CRM.LookupAttributeMetadata":          ("str",   "lookup"),
    "Microsoft.Dynamics.CRM.UniqueIdentifierAttributeMetadata":("str",   None),
    "Microsoft.Dynamics.CRM.EntityNameAttributeMetadata":      ("str",   None),
}

#: Attribute types that carry no useful OData-queryable value and are skipped.
ATTR_SKIP_TYPES = {
    "Microsoft.Dynamics.CRM.ImageAttributeMetadata",
    "Microsoft.Dynamics.CRM.FileAttributeMetadata",
    "Microsoft.Dynamics.CRM.VirtualAttributeMetadata",
    "Microsoft.Dynamics.CRM.MultiSelectPicklistAttributeMetadata",
}

# ---------------------------------------------------------------------------
# Fallback: map the plain ``AttributeType`` field value (always populated)
# to the same (python_type, dataverse_type_hint) tuples.
# Used when ``@odata.type`` is absent from the response.
# ---------------------------------------------------------------------------

_ATTR_TYPE_BY_FIELD: Dict[str, tuple[str, Optional[str]]] = {
    "String":            ("str",   "string"),
    "Memo":              ("str",   "memo"),
    "Integer":           ("int",   "int"),
    "BigInt":            ("int",   "int"),
    "Decimal":           ("float", "decimal"),
    "Double":            ("float", "float"),
    "Money":             ("float", "decimal"),
    "DateTime":          ("str",   "datetime"),
    "Boolean":           ("bool",  "bool"),
    "Picklist":          ("int",   "picklist"),
    "State":             ("int",   "picklist"),
    "Status":            ("int",   "picklist"),
    "Lookup":            ("str",   "lookup"),
    "Customer":          ("str",   "lookup"),
    "Owner":             ("str",   "lookup"),
    "UniqueIdentifier":  ("str",   None),
    "EntityName":        ("str",   None),
}

_ATTR_SKIP_BY_FIELD = {
    "Image", "File", "Virtual", "ManagedProperty", "CalendarRules", "PartyList",
}


# ---------------------------------------------------------------------------
# Entity listing
# ---------------------------------------------------------------------------

def list_entities(
    odata: "_ODataClient",
    logical_names: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Return entity metadata rows from ``EntityDefinitions``.

    :param odata: Internal OData client (``DataverseClient._odata``).
    :param logical_names: If given, only return entries whose ``LogicalName``
        is in this list (case-insensitive).  ``None`` returns every
        non-private entity.
    :returns: List of dicts with at least ``MetadataId``, ``LogicalName``,
        ``SchemaName``, ``EntitySetName``, and ``PrimaryIdAttribute``.
    """
    select = [
        "MetadataId",
        "LogicalName",
        "SchemaName",
        "EntitySetName",
        "PrimaryIdAttribute",
        "PrimaryNameAttribute",
    ]

    if logical_names:
        # Fetch individually to preserve caller ordering and avoid giant filters.
        rows = []
        seen = set()
        for name in logical_names:
            ent = odata._get_entity_by_table_schema_name(name)
            if ent and ent.get("MetadataId") not in seen:
                seen.add(ent["MetadataId"])
                rows.append(ent)
        return rows

    return odata._list_tables(select=select)


# ---------------------------------------------------------------------------
# Attribute listing
# ---------------------------------------------------------------------------

def list_attributes(
    odata: "_ODataClient",
    metadata_id: str,
) -> List[Dict[str, Any]]:
    """Return all attributes for an entity, including ``@odata.type``.

    Uses a single ``GET EntityDefinitions({id})/Attributes`` call.

    :param odata: Internal OData client.
    :param metadata_id: The entity's ``MetadataId`` GUID.
    :returns: List of attribute metadata dicts.  Each dict contains at least
        ``LogicalName``, ``SchemaName``, and ``@odata.type``.
    """
    url = (
        f"{odata.api}/EntityDefinitions({metadata_id})/Attributes"
        f"?$select=LogicalName,SchemaName,AttributeType,IsValidForRead"
    )
    r = odata._request("get", url)
    return r.json().get("value", [])


# ---------------------------------------------------------------------------
# Type resolution
# ---------------------------------------------------------------------------

def resolve_attr_type(attr: Dict[str, Any]) -> Optional[tuple[str, Optional[str]]]:
    """Map an attribute metadata dict to ``(python_type, dataverse_type_hint)``.

    Returns ``None`` for attribute types that should be skipped entirely
    (e.g. image, file, virtual columns).

    Resolution order:
    1. ``@odata.type`` annotation (present for most attributes)
    2. ``AttributeType`` field (always populated — used as fallback)
    """
    # --- primary: @odata.type ---
    raw = attr.get("@odata.type", "")
    type_name = raw.lstrip("#")  # strip leading '#'

    if type_name in ATTR_SKIP_TYPES:
        return None
    if type_name in ATTR_TYPE_MAP:
        return ATTR_TYPE_MAP[type_name]

    # --- fallback: AttributeType field ---
    field_type = attr.get("AttributeType", "")
    if field_type in _ATTR_SKIP_BY_FIELD:
        return None
    if field_type in _ATTR_TYPE_BY_FIELD:
        return _ATTR_TYPE_BY_FIELD[field_type]

    return ("Any", None)
