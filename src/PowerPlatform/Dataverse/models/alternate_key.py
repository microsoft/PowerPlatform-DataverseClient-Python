# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Alternate key metadata models for the Dataverse SDK.

Provides strongly-typed representations of Dataverse alternate key definitions.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterator, List, Optional, ClassVar


@dataclass
class AlternateKeyInfo:
    """
    Alternate key metadata for a Dataverse table.

    Alternate keys allow records to be uniquely identified using business keys
    instead of the primary key GUID. They are essential for upsert operations
    and data integration scenarios.

    Provides dict-like access for consistency with other SDK models.

    :param schema_name: Key schema name (e.g., "AccountNumberKey").
    :type schema_name: str
    :param logical_name: Key logical name (lowercase).
    :type logical_name: str
    :param display_name: Human-readable display name.
    :type display_name: str | None
    :param columns: List of column logical names that make up the key.
    :type columns: list[str]
    :param metadata_id: Unique metadata identifier (GUID).
    :type metadata_id: str
    :param status: Index status ("Pending", "InProgress", "Active", "Failed").
    :type status: str

    Example:
        Access alternate key metadata::

            keys = client.tables.list_keys("account")
            for key in keys:
                print(f"Key: {key.schema_name}")
                print(f"Columns: {key.columns}")
                print(f"Status: {key.status}")

        Dict-like access::

            print(key["schema_name"])
            print(key["columns"])
    """

    schema_name: str
    logical_name: str
    columns: List[str]
    metadata_id: str
    status: str = "Active"
    display_name: Optional[str] = None

    # Class-level mapping from legacy dict keys to dataclass attributes
    _LEGACY_KEY_MAP: ClassVar[Dict[str, str]] = {
        "schema_name": "schema_name",
        "logical_name": "logical_name",
        "display_name": "display_name",
        "columns": "columns",
        "metadata_id": "metadata_id",
        "status": "status",
        "key_attributes": "columns",  # Alias for Dataverse API naming
    }

    def __getitem__(self, key: str) -> Any:
        """
        Dictionary-like access for consistency with other SDK models.

        :param key: Key to access.
        :type key: str
        :return: Value for the key.
        :raises KeyError: If key doesn't exist.
        """
        if key in self._LEGACY_KEY_MAP:
            return getattr(self, self._LEGACY_KEY_MAP[key])
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
        Iterate over key names for dict-like interface.

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
        """Return available key names."""
        return self._LEGACY_KEY_MAP.keys()

    def values(self):
        """Return values."""
        return (self[k] for k in self._LEGACY_KEY_MAP.keys())

    def items(self):
        """Return key-value pairs."""
        return ((k, self[k]) for k in self._LEGACY_KEY_MAP.keys())

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to a dictionary representation.

        :return: Dictionary with alternate key metadata.
        :rtype: dict[str, Any]
        """
        result: Dict[str, Any] = {
            "schema_name": self.schema_name,
            "logical_name": self.logical_name,
            "columns": self.columns,
            "metadata_id": self.metadata_id,
            "status": self.status,
        }
        if self.display_name is not None:
            result["display_name"] = self.display_name
        return result

    @classmethod
    def from_api_response(cls, response_data: Dict[str, Any]) -> "AlternateKeyInfo":
        """
        Create an AlternateKeyInfo from a Dataverse metadata API response.

        :param response_data: Raw API response dictionary.
        :type response_data: dict[str, Any]
        :return: AlternateKeyInfo instance.
        :rtype: AlternateKeyInfo
        """
        # Handle DisplayName which can be nested or simple
        display_name_obj = response_data.get("DisplayName", {})
        if isinstance(display_name_obj, dict):
            label_obj = display_name_obj.get("UserLocalizedLabel", {})
            display_name = label_obj.get("Label") if isinstance(label_obj, dict) else None
        else:
            display_name = display_name_obj if display_name_obj else None

        # Handle EntityKeyIndexStatus which is an enum string
        status_value = response_data.get("EntityKeyIndexStatus", "Active")
        if isinstance(status_value, str):
            status = status_value
        else:
            status = str(status_value) if status_value else "Active"

        return cls(
            schema_name=response_data.get("SchemaName", ""),
            logical_name=response_data.get("LogicalName", ""),
            columns=response_data.get("KeyAttributes", []),
            metadata_id=response_data.get("MetadataId", ""),
            status=status,
            display_name=display_name,
        )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AlternateKeyInfo":
        """
        Create an AlternateKeyInfo from a dictionary (internal format).

        :param data: Dictionary with alternate key metadata.
        :type data: dict[str, Any]
        :return: AlternateKeyInfo instance.
        :rtype: AlternateKeyInfo
        """
        return cls(
            schema_name=data.get("schema_name", ""),
            logical_name=data.get("logical_name", ""),
            columns=data.get("columns", data.get("key_attributes", [])),
            metadata_id=data.get("metadata_id", ""),
            status=data.get("status", "Active"),
            display_name=data.get("display_name"),
        )


__all__ = ["AlternateKeyInfo"]
