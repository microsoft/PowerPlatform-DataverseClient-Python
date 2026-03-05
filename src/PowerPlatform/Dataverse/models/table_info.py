# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Table and column metadata models for Dataverse."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, ClassVar, Dict, Iterator, KeysView, List, Optional

__all__ = ["TableInfo", "ColumnInfo"]


@dataclass
class ColumnInfo:
    """Column metadata from a Dataverse table definition.

    :param schema_name: Column schema name (e.g. ``"new_Price"``).
    :type schema_name: :class:`str`
    :param logical_name: Column logical name (lowercase).
    :type logical_name: :class:`str`
    :param type: Column type string (e.g. ``"String"``, ``"Integer"``).
    :type type: :class:`str`
    :param is_primary: Whether this is the primary name column.
    :type is_primary: :class:`bool`
    :param is_required: Whether the column is required.
    :type is_required: :class:`bool`
    :param max_length: Maximum length for string columns.
    :type max_length: :class:`int` or None
    :param display_name: Human-readable display name.
    :type display_name: :class:`str` or None
    :param description: Column description.
    :type description: :class:`str` or None
    """

    schema_name: str = ""
    logical_name: str = ""
    type: str = ""
    is_primary: bool = False
    is_required: bool = False
    max_length: Optional[int] = None
    display_name: Optional[str] = None
    description: Optional[str] = None

    @classmethod
    def from_api_response(cls, response_data: Dict[str, Any]) -> ColumnInfo:
        """Create from a raw Dataverse ``AttributeMetadata`` API response.

        :param response_data: Raw attribute metadata dict (PascalCase keys).
        :type response_data: :class:`dict`
        :rtype: :class:`ColumnInfo`
        """
        # Extract display name from nested structure
        display_name_obj = response_data.get("DisplayName", {})
        user_label = display_name_obj.get("UserLocalizedLabel") or {}
        display_name = user_label.get("Label")

        # Extract description from nested structure
        desc_obj = response_data.get("Description", {})
        desc_label = desc_obj.get("UserLocalizedLabel") or {}
        description = desc_label.get("Label")

        # Extract required level
        req_level = response_data.get("RequiredLevel", {})
        is_required = req_level.get("Value", "None") != "None" if isinstance(req_level, dict) else False

        return cls(
            schema_name=response_data.get("SchemaName", ""),
            logical_name=response_data.get("LogicalName", ""),
            type=response_data.get("AttributeTypeName", {}).get("Value", response_data.get("AttributeType", "")),
            is_primary=response_data.get("IsPrimaryName", False),
            is_required=is_required,
            max_length=response_data.get("MaxLength"),
            display_name=display_name,
            description=description,
        )


@dataclass
class TableInfo:
    """Table metadata with dict-like backward compatibility.

    Supports both new attribute access (``info.schema_name``) and legacy
    dict-key access (``info["table_schema_name"]``) for backward
    compatibility with code written against the raw dict API.

    :param schema_name: Table schema name (e.g. ``"Account"``).
    :type schema_name: :class:`str`
    :param logical_name: Table logical name (lowercase).
    :type logical_name: :class:`str`
    :param entity_set_name: OData entity set name.
    :type entity_set_name: :class:`str`
    :param metadata_id: Metadata GUID.
    :type metadata_id: :class:`str`
    :param display_name: Human-readable display name.
    :type display_name: :class:`str` or None
    :param description: Table description.
    :type description: :class:`str` or None
    :param columns: Column metadata (when retrieved).
    :type columns: :class:`list` of :class:`ColumnInfo` or None
    :param columns_created: Column schema names created with the table.
    :type columns_created: :class:`list` of :class:`str` or None

    Example::

        info = client.tables.create("new_Product", {"new_Price": "decimal"})
        print(info.schema_name)              # new attribute access
        print(info["table_schema_name"])     # legacy dict-key access
    """

    schema_name: str = ""
    logical_name: str = ""
    entity_set_name: str = ""
    metadata_id: str = ""
    display_name: Optional[str] = None
    description: Optional[str] = None
    columns: Optional[List[ColumnInfo]] = field(default=None, repr=False)
    columns_created: Optional[List[str]] = field(default=None, repr=False)

    # Maps legacy dict keys (used by existing code) to attribute names.
    _LEGACY_KEY_MAP: ClassVar[Dict[str, str]] = {
        "table_schema_name": "schema_name",
        "table_logical_name": "logical_name",
        "entity_set_name": "entity_set_name",
        "metadata_id": "metadata_id",
        "columns_created": "columns_created",
    }

    # --------------------------------------------------------- dict-like access

    def _resolve_key(self, key: str) -> str:
        """Resolve a legacy or direct key to an attribute name."""
        return self._LEGACY_KEY_MAP.get(key, key)

    def __getitem__(self, key: str) -> Any:
        attr = self._resolve_key(key)
        if hasattr(self, attr):
            return getattr(self, attr)
        raise KeyError(key)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        attr = self._resolve_key(key)
        return hasattr(self, attr)

    def __iter__(self) -> Iterator[str]:
        return iter(self._LEGACY_KEY_MAP)

    def __len__(self) -> int:
        return len(self._LEGACY_KEY_MAP)

    def get(self, key: str, default: Any = None) -> Any:
        """Return value for *key*, or *default* if not present."""
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self) -> KeysView[str]:
        """Return legacy dict keys."""
        return self._LEGACY_KEY_MAP.keys()

    def values(self) -> List[Any]:
        """Return values corresponding to legacy dict keys."""
        return [getattr(self, attr) for attr in self._LEGACY_KEY_MAP.values()]

    def items(self) -> List[tuple]:
        """Return (legacy_key, value) pairs."""
        return [(k, getattr(self, attr)) for k, attr in self._LEGACY_KEY_MAP.items()]

    # -------------------------------------------------------------- factories

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> TableInfo:
        """Create from an SDK internal dict (snake_case keys).

        This handles the dict format returned by ``_create_table`` and
        ``_get_table_info`` in the OData layer.

        :param data: Dictionary with SDK snake_case keys.
        :type data: :class:`dict`
        :rtype: :class:`TableInfo`
        """
        return cls(
            schema_name=data.get("table_schema_name", ""),
            logical_name=data.get("table_logical_name", ""),
            entity_set_name=data.get("entity_set_name", ""),
            metadata_id=data.get("metadata_id", ""),
            columns_created=data.get("columns_created"),
        )

    @classmethod
    def from_api_response(cls, response_data: Dict[str, Any]) -> TableInfo:
        """Create from a raw Dataverse ``EntityDefinition`` API response.

        :param response_data: Raw entity metadata dict (PascalCase keys).
        :type response_data: :class:`dict`
        :rtype: :class:`TableInfo`
        """
        # Extract display name from nested structure
        display_name_obj = response_data.get("DisplayName", {})
        user_label = display_name_obj.get("UserLocalizedLabel") or {}
        display_name = user_label.get("Label")

        # Extract description from nested structure
        desc_obj = response_data.get("Description", {})
        desc_label = desc_obj.get("UserLocalizedLabel") or {}
        description = desc_label.get("Label")

        return cls(
            schema_name=response_data.get("SchemaName", ""),
            logical_name=response_data.get("LogicalName", ""),
            entity_set_name=response_data.get("EntitySetName", ""),
            metadata_id=response_data.get("MetadataId", ""),
            display_name=display_name,
            description=description,
        )

    # -------------------------------------------------------------- conversion

    def to_dict(self) -> Dict[str, Any]:
        """Return a dict with legacy keys for backward compatibility."""
        return {k: getattr(self, attr) for k, attr in self._LEGACY_KEY_MAP.items()}
