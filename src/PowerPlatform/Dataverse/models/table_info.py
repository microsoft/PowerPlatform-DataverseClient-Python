# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Table metadata models for Dataverse."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class AlternateKeyInfo:
    """Alternate key metadata for a Dataverse table.

    :param metadata_id: Key metadata GUID.
    :type metadata_id: :class:`str`
    :param schema_name: Key schema name.
    :type schema_name: :class:`str`
    :param key_attributes: List of column logical names that compose the key.
    :type key_attributes: :class:`list` of :class:`str`
    :param status: Index creation status (``"Active"``, ``"Pending"``, ``"InProgress"``, ``"Failed"``).
    :type status: :class:`str`
    """

    metadata_id: str = ""
    schema_name: str = ""
    key_attributes: List[str] = field(default_factory=list)
    status: str = ""

    @classmethod
    def from_api_response(cls, response_data: Dict[str, Any]) -> AlternateKeyInfo:
        """Create from raw EntityKeyMetadata API response.

        :param response_data: Raw key metadata dictionary from the Web API.
        :type response_data: :class:`dict`
        :rtype: :class:`AlternateKeyInfo`
        """
        return cls(
            metadata_id=response_data.get("MetadataId", ""),
            schema_name=response_data.get("SchemaName", ""),
            key_attributes=response_data.get("KeyAttributes", []),
            status=response_data.get("EntityKeyIndexStatus", ""),
        )


__all__ = ["AlternateKeyInfo"]
