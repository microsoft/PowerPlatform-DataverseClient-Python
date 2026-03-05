# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Record data model for Dataverse entities."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterator, KeysView, Optional, ValuesView, ItemsView

__all__ = ["Record"]

_ODATA_PREFIX = "@odata."


@dataclass
class Record:
    """Strongly-typed Dataverse record with dict-like backward compatibility.

    Wraps raw OData response data into a structured object while preserving
    ``result["key"]`` access patterns for existing code.

    :param id: Record GUID. Empty string if not extracted (e.g. paginated
        results, SQL queries).
    :type id: :class:`str`
    :param table: Table schema name used in the request.
    :type table: :class:`str`
    :param data: Record field data as key-value pairs.
    :type data: :class:`dict`
    :param etag: ETag for optimistic concurrency, extracted from
        ``@odata.etag`` in the API response.
    :type etag: :class:`str` or None

    Example::

        record = client.records.get("account", account_id, select=["name"])
        print(record.id)          # structured access
        print(record["name"])     # dict-like access (backward compat)
    """

    id: str = ""
    table: str = ""
    data: Dict[str, Any] = field(default_factory=dict)
    etag: Optional[str] = None

    # --------------------------------------------------------- dict-like access

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.data[key] = value

    def __delitem__(self, key: str) -> None:
        del self.data[key]

    def __contains__(self, key: object) -> bool:
        return key in self.data

    def __iter__(self) -> Iterator[str]:
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)

    def get(self, key: str, default: Any = None) -> Any:
        """Return value for *key*, or *default* if not present."""
        return self.data.get(key, default)

    def keys(self) -> KeysView[str]:
        """Return data keys."""
        return self.data.keys()

    def values(self) -> ValuesView[Any]:
        """Return data values."""
        return self.data.values()

    def items(self) -> ItemsView[str, Any]:
        """Return data items."""
        return self.data.items()

    # -------------------------------------------------------------- factories

    @classmethod
    def from_api_response(
        cls,
        table: str,
        response_data: Dict[str, Any],
        *,
        record_id: str = "",
    ) -> Record:
        """Create a :class:`Record` from a raw OData API response.

        Strips ``@odata.*`` annotation keys from the data and extracts the
        ``@odata.etag`` value if present.

        :param table: Table schema name.
        :type table: :class:`str`
        :param response_data: Raw JSON dict from the OData response.
        :type response_data: :class:`dict`
        :param record_id: Known record GUID. Pass explicitly when available
            (e.g. single-record get). Defaults to empty string.
        :type record_id: :class:`str`
        :rtype: :class:`Record`
        """
        etag = response_data.get("@odata.etag")
        data = {k: v for k, v in response_data.items() if not k.startswith(_ODATA_PREFIX)}
        return cls(id=record_id, table=table, data=data, etag=etag)

    # -------------------------------------------------------------- conversion

    def to_dict(self) -> Dict[str, Any]:
        """Return a plain dict copy of the record data (excludes metadata)."""
        return dict(self.data)
