# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Table and column metadata models for the Dataverse SDK.

Provides strongly-typed representations of Dataverse table and column metadata.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, List, Optional, ClassVar

# Type alias for semantic clarity
ColumnSchema = str  # e.g., "name", "new_CustomColumn"


@dataclass
class ColumnInfo:
    """
    Column metadata.

    :param schema_name: Column schema name (e.g., "new_CustomColumn").
    :type schema_name: str
    :param logical_name: Column logical name (lowercase).
    :type logical_name: str
    :param type: Column type (e.g., "String", "Integer", "Decimal", "DateTime").
    :type type: str
    :param is_primary: Whether this is the primary column.
    :type is_primary: bool
    :param is_required: Whether the column is required.
    :type is_required: bool
    :param max_length: Maximum length for string columns.
    :type max_length: int | None
    :param display_name: Human-readable display name.
    :type display_name: str | None
    :param description: Column description.
    :type description: str | None

    Example:
        Access column metadata::

            info = client.tables.info("account")
            if info and info.columns:
                for col in info.columns:
                    print(f"{col.schema_name}: {col.type}")
                    if col.is_primary:
                        print("  (primary column)")
    """

    schema_name: str
    logical_name: str
    type: str
    is_primary: bool = False
    is_required: bool = False
    max_length: Optional[int] = None
    display_name: Optional[str] = None
    description: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to a dictionary representation.

        :return: Dictionary with column metadata.
        :rtype: dict[str, Any]
        """
        return {
            "schema_name": self.schema_name,
            "logical_name": self.logical_name,
            "type": self.type,
            "is_primary": self.is_primary,
            "is_required": self.is_required,
            "max_length": self.max_length,
            "display_name": self.display_name,
            "description": self.description,
        }

    @classmethod
    def from_api_response(cls, response_data: Dict[str, Any]) -> "ColumnInfo":
        """
        Create a ColumnInfo from a Dataverse metadata API response.

        :param response_data: Raw API response dictionary.
        :type response_data: dict[str, Any]
        :return: ColumnInfo instance.
        :rtype: ColumnInfo
        """
        # Handle RequiredLevel which can be nested object or simple value
        required_level = response_data.get("RequiredLevel", {})
        if isinstance(required_level, dict):
            is_required = required_level.get("Value") == "ApplicationRequired"
        else:
            is_required = required_level == "ApplicationRequired"

        # Handle DisplayName which can be nested or simple
        display_name_obj = response_data.get("DisplayName", {})
        if isinstance(display_name_obj, dict):
            label_obj = display_name_obj.get("UserLocalizedLabel", {})
            display_name = label_obj.get("Label") if isinstance(label_obj, dict) else None
        else:
            display_name = display_name_obj if display_name_obj else None

        # Handle Description which can be nested or simple
        description_obj = response_data.get("Description", {})
        if isinstance(description_obj, dict):
            label_obj = description_obj.get("UserLocalizedLabel", {})
            description = label_obj.get("Label") if isinstance(label_obj, dict) else None
        else:
            description = description_obj if description_obj else None

        return cls(
            schema_name=response_data.get("SchemaName", ""),
            logical_name=response_data.get("LogicalName", ""),
            type=response_data.get("AttributeType", response_data.get("@odata.type", "Unknown")),
            is_primary=response_data.get("IsPrimaryId", False) or response_data.get("IsPrimaryName", False),
            is_required=is_required,
            max_length=response_data.get("MaxLength"),
            display_name=display_name,
            description=description,
        )


@dataclass
class TableInfo:
    """
    Table metadata.

    Provides dict-like access for backward compatibility with existing code
    that expects dictionary return types.

    :param schema_name: Table schema name (e.g., "Account", "new_MyTable").
    :type schema_name: str
    :param logical_name: Table logical name (lowercase).
    :type logical_name: str
    :param entity_set_name: OData entity set name for API calls.
    :type entity_set_name: str
    :param metadata_id: Unique metadata identifier (GUID).
    :type metadata_id: str
    :param display_name: Human-readable display name.
    :type display_name: str | None
    :param description: Table description.
    :type description: str | None
    :param columns: List of column metadata (populated on demand).
    :type columns: list[ColumnInfo] | None

    Example:
        Structured access::

            info = client.tables.info("account")
            if info:
                print(f"Schema: {info.schema_name}")
                print(f"Entity set: {info.entity_set_name}")

        Dict-like access (backward compatible)::

            # These work for backward compatibility
            print(info["table_schema_name"])
            print(info["entity_set_name"])
    """

    schema_name: str
    logical_name: str
    entity_set_name: str
    metadata_id: str
    display_name: Optional[str] = None
    description: Optional[str] = None
    columns: Optional[List[ColumnInfo]] = None
    columns_created: Optional[List[str]] = field(default=None, repr=False)

    # Class-level mapping from legacy dict keys to dataclass attributes
    _LEGACY_KEY_MAP: ClassVar[Dict[str, str]] = {
        "table_schema_name": "schema_name",
        "table_logical_name": "logical_name",
        "entity_set_name": "entity_set_name",
        "metadata_id": "metadata_id",
        "display_name": "display_name",
        "description": "description",
        "columns_created": "columns_created",
    }

    def __getitem__(self, key: str) -> Any:
        """
        Dictionary-like access for backward compatibility.

        Supports both legacy keys (e.g., "table_schema_name") and
        direct attribute names (e.g., "schema_name").

        :param key: Key to access.
        :type key: str
        :return: Value for the key.
        :raises KeyError: If key doesn't exist.
        """
        # Check legacy key mapping
        if key in self._LEGACY_KEY_MAP:
            return getattr(self, self._LEGACY_KEY_MAP[key])
        # Check direct attribute access
        if hasattr(self, key) and not key.startswith("_"):
            return getattr(self, key)
        raise KeyError(key)

    def __contains__(self, key: object) -> bool:
        """
        Check if a key exists.

        :param key: Key to check.
        :return: True if key exists.
        :rtype: bool
        """
        if isinstance(key, str):
            return key in self._LEGACY_KEY_MAP or (hasattr(self, key) and not key.startswith("_"))
        return False

    def __iter__(self) -> Iterator[str]:
        """
        Iterate over legacy key names for backward compatibility.

        :return: Iterator over key names.
        :rtype: Iterator[str]
        """
        return iter(self._LEGACY_KEY_MAP.keys())

    def __len__(self) -> int:
        """
        Return the number of accessible keys.

        :return: Number of keys.
        :rtype: int
        """
        return len(self._LEGACY_KEY_MAP)

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a value with optional default.

        :param key: Key to access.
        :type key: str
        :param default: Default value if key doesn't exist.
        :return: Value or default.
        """
        try:
            return self[key]
        except KeyError:
            return default

    def keys(self):
        """
        Return available key names for backward compatibility.

        :return: View of key names.
        """
        return self._LEGACY_KEY_MAP.keys()

    def values(self):
        """
        Return values for backward compatibility.

        :return: Generator of values.
        """
        return (self[k] for k in self._LEGACY_KEY_MAP.keys())

    def items(self):
        """
        Return key-value pairs for backward compatibility.

        :return: Generator of key-value pairs.
        """
        return ((k, self[k]) for k in self._LEGACY_KEY_MAP.keys())

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to a dictionary using legacy key names.

        This maintains backward compatibility with code expecting dict returns.

        :return: Dictionary with table metadata.
        :rtype: dict[str, Any]
        """
        result: Dict[str, Any] = {
            "table_schema_name": self.schema_name,
            "table_logical_name": self.logical_name,
            "entity_set_name": self.entity_set_name,
            "metadata_id": self.metadata_id,
        }
        if self.display_name is not None:
            result["display_name"] = self.display_name
        if self.description is not None:
            result["description"] = self.description
        if self.columns is not None:
            result["columns"] = [col.to_dict() for col in self.columns]
        if self.columns_created is not None:
            result["columns_created"] = self.columns_created
        return result

    @classmethod
    def from_api_response(cls, response_data: Dict[str, Any]) -> "TableInfo":
        """
        Create a TableInfo from a Dataverse metadata API response.

        :param response_data: Raw API response dictionary.
        :type response_data: dict[str, Any]
        :return: TableInfo instance.
        :rtype: TableInfo
        """
        # Handle columns/Attributes if present
        columns = None
        if "Attributes" in response_data:
            columns = [ColumnInfo.from_api_response(attr) for attr in response_data["Attributes"]]

        # Handle DisplayName which can be nested or simple
        display_name_obj = response_data.get("DisplayName", {})
        if isinstance(display_name_obj, dict):
            label_obj = display_name_obj.get("UserLocalizedLabel", {})
            display_name = label_obj.get("Label") if isinstance(label_obj, dict) else None
        else:
            display_name = display_name_obj if display_name_obj else None

        # Handle Description which can be nested or simple
        description_obj = response_data.get("Description", {})
        if isinstance(description_obj, dict):
            label_obj = description_obj.get("UserLocalizedLabel", {})
            description = label_obj.get("Label") if isinstance(label_obj, dict) else None
        else:
            description = description_obj if description_obj else None

        return cls(
            schema_name=response_data.get("SchemaName", response_data.get("table_schema_name", "")),
            logical_name=response_data.get("LogicalName", response_data.get("table_logical_name", "")),
            entity_set_name=response_data.get("EntitySetName", response_data.get("entity_set_name", "")),
            metadata_id=response_data.get("MetadataId", response_data.get("metadata_id", "")),
            display_name=display_name or response_data.get("display_name"),
            description=description or response_data.get("description"),
            columns=columns,
            columns_created=response_data.get("columns_created"),
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "TableInfo":
        """
        Create a TableInfo from a dictionary (internal format).

        Used for converting existing dict returns to TableInfo objects.

        :param data: Dictionary with table metadata.
        :type data: dict[str, Any]
        :return: TableInfo instance.
        :rtype: TableInfo
        """
        return cls(
            schema_name=data.get("table_schema_name", ""),
            logical_name=data.get("table_logical_name", ""),
            entity_set_name=data.get("entity_set_name", ""),
            metadata_id=data.get("metadata_id", ""),
            display_name=data.get("display_name"),
            description=data.get("description"),
            columns_created=data.get("columns_created"),
        )


__all__ = ["TableInfo", "ColumnInfo", "ColumnSchema"]
