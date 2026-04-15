# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Async relationship metadata operations for Dataverse Web API.

Provides :class:`_AsyncRelationshipOperationsMixin`, the async counterpart of
:class:`~PowerPlatform.Dataverse.data._relationships._RelationshipOperationsMixin`.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional


class _AsyncRelationshipOperationsMixin:
    """Async mixin providing relationship metadata CRUD operations.

    Designed to be composed with :class:`~PowerPlatform.Dataverse.aio.data._async_odata._AsyncODataClient`.

    Depends on:

    - ``self.api`` — API base URL string.
    - ``self._request()`` — async HTTP coroutine.
    - ``self._escape_odata_quotes()`` — pure sync helper (from
      :class:`~PowerPlatform.Dataverse.data._odata._ODataClient` base).
    """

    def _extract_id_from_header(self, header_value: Optional[str]) -> Optional[str]:
        """Extract a GUID from an OData-EntityId header value.

        :param header_value: Header value containing a URL with a GUID in parentheses.
        :type header_value: ``str`` | ``None``

        :returns: Extracted GUID, or ``None`` if not found.
        """
        if not header_value:
            return None
        match = re.search(r"\(([0-9a-fA-F-]+)\)", header_value)
        return match.group(1) if match else None

    async def _create_one_to_many_relationship(  # type: ignore[override]
        self,
        lookup: Any,
        relationship: Any,
        solution: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a one-to-many relationship.

        Posts to ``/RelationshipDefinitions`` with ``OneToManyRelationshipMetadata``.

        :param lookup: Lookup attribute metadata
            (:class:`~PowerPlatform.Dataverse.models.relationship.LookupAttributeMetadata`).
        :param relationship: Relationship metadata
            (:class:`~PowerPlatform.Dataverse.models.relationship.OneToManyRelationshipMetadata`).
        :param solution: Optional solution unique name.
        :type solution: ``str`` | ``None``

        :returns: Dict with ``relationship_id``, ``relationship_schema_name``,
            ``lookup_schema_name``, ``referenced_entity``, ``referencing_entity``.

        :raises HttpError: If the Web API request fails.
        """
        url = f"{self.api}/RelationshipDefinitions"
        # Build the payload by combining relationship and lookup metadata
        payload = relationship.to_dict()
        payload["Lookup"] = lookup.to_dict()
        headers: Dict[str, str] = {}
        if solution:
            headers["MSCRM.SolutionUniqueName"] = solution
        response = await self._request("post", url, headers=headers or None, json=payload)
        # Extract IDs from response headers
        relationship_id = self._extract_id_from_header(response.headers.get("OData-EntityId"))
        return {
            "relationship_id": relationship_id,
            "relationship_schema_name": relationship.schema_name,
            "lookup_schema_name": lookup.schema_name,
            "referenced_entity": relationship.referenced_entity,
            "referencing_entity": relationship.referencing_entity,
        }

    async def _create_many_to_many_relationship(  # type: ignore[override]
        self,
        relationship: Any,
        solution: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Create a many-to-many relationship.

        Posts to ``/RelationshipDefinitions`` with ``ManyToManyRelationshipMetadata``.

        :param relationship: Relationship metadata
            (:class:`~PowerPlatform.Dataverse.models.relationship.ManyToManyRelationshipMetadata`).
        :param solution: Optional solution unique name.
        :type solution: ``str`` | ``None``

        :returns: Dict with ``relationship_id``, ``relationship_schema_name``,
            ``entity1_logical_name``, ``entity2_logical_name``.

        :raises HttpError: If the Web API request fails.
        """
        url = f"{self.api}/RelationshipDefinitions"
        payload = relationship.to_dict()
        headers: Dict[str, str] = {}
        if solution:
            headers["MSCRM.SolutionUniqueName"] = solution
        response = await self._request("post", url, headers=headers or None, json=payload)
        # Extract ID from response header
        relationship_id = self._extract_id_from_header(response.headers.get("OData-EntityId"))
        return {
            "relationship_id": relationship_id,
            "relationship_schema_name": relationship.schema_name,
            "entity1_logical_name": relationship.entity1_logical_name,
            "entity2_logical_name": relationship.entity2_logical_name,
        }

    async def _delete_relationship(self, relationship_id: str) -> None:  # type: ignore[override]
        """Delete a relationship by metadata ID.

        :param relationship_id: GUID of the relationship metadata record.
        :type relationship_id: ``str``

        :raises HttpError: If the Web API request fails.
        """
        url = f"{self.api}/RelationshipDefinitions({relationship_id})"
        headers = {"If-Match": "*"}
        await self._request("delete", url, headers=headers)

    async def _get_relationship(self, schema_name: str) -> Optional[Dict[str, Any]]:  # type: ignore[override]
        """Retrieve relationship metadata by schema name.

        :param schema_name: Schema name of the relationship.
        :type schema_name: ``str``

        :returns: Relationship metadata dict, or ``None`` if not found.

        :raises HttpError: If the Web API request fails.
        """
        url = f"{self.api}/RelationshipDefinitions"
        params = {"$filter": f"SchemaName eq '{self._escape_odata_quotes(schema_name)}'"}
        response = await self._request("get", url, params=params)
        data = response.json()
        results = data.get("value", [])
        return results[0] if results else None
