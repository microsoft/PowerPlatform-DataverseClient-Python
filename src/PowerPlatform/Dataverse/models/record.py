# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Record data model for Dataverse entities.

Provides a strongly-typed representation of Dataverse records with
backward-compatible dict-like access patterns.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, Optional

# Type aliases for semantic clarity
RecordId = str  # UUID string
TableSchema = str  # e.g., "account", "new_CustomTable"


@dataclass
class Record:
    """
    Strongly-typed record representation with metadata.

    Provides dict-like access for backward compatibility while offering
    structured access to record metadata.

    :param id: Record GUID (primary key).
    :type id: str
    :param table: Table schema name (e.g., "account", "new_MyTable").
    :type table: str
    :param data: Record field data as key-value pairs.
    :type data: dict[str, Any]
    :param etag: Optional ETag for optimistic concurrency control.
    :type etag: str | None

    Example:
        Structured access::

            record = client.records.get("account", account_id)
            print(record.id)      # GUID string
            print(record.table)   # "account"
            print(record.etag)    # ETag for concurrency

        Dict-like access (backward compatible)::

            print(record["name"])           # Field access
            record["telephone1"] = "555-0100"  # Field mutation
            for key in record:              # Iteration over keys
                print(key, record[key])

        Check field existence::

            if "revenue" in record:
                print(record["revenue"])
    """

    id: RecordId
    table: TableSchema
    data: Dict[str, Any] = field(default_factory=dict)
    etag: Optional[str] = None

    # Dict-like access for backward compatibility

    def __getitem__(self, key: str) -> Any:
        """
        Dictionary-like field access.

        :param key: Field name to access.
        :type key: str
        :return: Field value.
        :raises KeyError: If the field doesn't exist.
        """
        return self.data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        """
        Dictionary-like field mutation.

        :param key: Field name to set.
        :type key: str
        :param value: Value to assign.
        """
        self.data[key] = value

    def __delitem__(self, key: str) -> None:
        """
        Dictionary-like field deletion.

        :param key: Field name to delete.
        :type key: str
        :raises KeyError: If the field doesn't exist.
        """
        del self.data[key]

    def __contains__(self, key: object) -> bool:
        """
        Check if a field exists in the record.

        :param key: Field name to check.
        :return: True if field exists.
        :rtype: bool
        """
        return key in self.data

    def __iter__(self) -> Iterator[str]:
        """
        Iterate over field names.

        :return: Iterator over field names.
        :rtype: Iterator[str]
        """
        return iter(self.data)

    def __len__(self) -> int:
        """
        Return the number of fields in the record.

        :return: Number of fields.
        :rtype: int
        """
        return len(self.data)

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a field value with optional default.

        :param key: Field name to access.
        :type key: str
        :param default: Default value if field doesn't exist.
        :return: Field value or default.
        """
        return self.data.get(key, default)

    def keys(self):
        """
        Return field names.

        :return: View of field names.
        """
        return self.data.keys()

    def values(self):
        """
        Return field values.

        :return: View of field values.
        """
        return self.data.values()

    def items(self):
        """
        Return field name-value pairs.

        :return: View of field items.
        """
        return self.data.items()

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to a plain dictionary (for serialization).

        Returns only the data fields, not metadata like id/table/etag.

        :return: Dictionary of field data.
        :rtype: dict[str, Any]
        """
        return dict(self.data)

    def to_full_dict(self) -> Dict[str, Any]:
        """
        Convert to a dictionary including metadata.

        :return: Dictionary with id, table, data, and etag.
        :rtype: dict[str, Any]
        """
        return {
            "id": self.id,
            "table": self.table,
            "data": dict(self.data),
            "etag": self.etag,
        }

    @classmethod
    def from_api_response(
        cls,
        table: str,
        response_data: Dict[str, Any],
        *,
        id_field: Optional[str] = None,
    ) -> "Record":
        """
        Create a Record from a Dataverse API response.

        :param table: Table schema name.
        :type table: str
        :param response_data: Raw API response dictionary.
        :type response_data: dict[str, Any]
        :param id_field: Optional explicit ID field name. If not provided,
            attempts to find the ID field automatically.
        :type id_field: str | None
        :return: Record instance.
        :rtype: Record
        """
        # Make a copy to avoid mutating the original
        data = dict(response_data)

        # Extract ID from response
        record_id = ""
        if id_field and id_field in data:
            record_id = str(data[id_field])
        else:
            # Try common ID field patterns
            for key in data:
                if key.endswith("id") and not key.startswith("_"):
                    record_id = str(data[key])
                    break

        # Extract ETag if present
        etag = data.pop("@odata.etag", None)

        # Remove OData annotations from data
        clean_data = {k: v for k, v in data.items() if not k.startswith("@odata.")}

        return cls(id=record_id, table=table, data=clean_data, etag=etag)


__all__ = ["Record", "RecordId", "TableSchema"]
