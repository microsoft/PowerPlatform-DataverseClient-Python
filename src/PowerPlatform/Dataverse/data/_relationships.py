# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Relationship metadata operations for Dataverse Web API.

This module provides mixin functionality for relationship CRUD operations.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Optional


class _RelationshipOperationsMixin:
    """
    Mixin providing relationship metadata operations.

    This mixin is designed to be used with _ODataClient and depends on:
    - self.api: The API base URL
    - self._headers(): Method to get auth headers
    - self._request(): Method to make HTTP requests
    """

    def _create_one_to_many_relationship(
        self,
        lookup,
        relationship,
        solution_unique_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a one-to-many relationship with lookup attribute.

        Posts to /RelationshipDefinitions with OneToManyRelationshipMetadata.

        :param lookup: Lookup attribute metadata (LookupAttributeMetadata instance).
        :type lookup: ~PowerPlatform.Dataverse.models.metadata.LookupAttributeMetadata
        :param relationship: Relationship metadata (OneToManyRelationshipMetadata instance).
        :type relationship: ~PowerPlatform.Dataverse.models.metadata.OneToManyRelationshipMetadata
        :param solution_unique_name: Optional solution to add the relationship to.
        :type solution_unique_name: ``str`` | ``None``

        :return: Dictionary with relationship_id, attribute_id, and schema names.
        :rtype: ``dict[str, Any]``

        :raises HttpError: If the Web API request fails.
        """
        url = f"{self.api}/RelationshipDefinitions"

        # Build the payload by combining relationship and lookup metadata
        payload = relationship.to_dict()
        payload["Lookup"] = lookup.to_dict()

        headers = self._headers().copy()
        if solution_unique_name:
            headers["MSCRM.SolutionUniqueName"] = solution_unique_name

        r = self._request("post", url, headers=headers, json=payload)

        # Extract IDs from response headers
        relationship_id = self._extract_id_from_header(r.headers.get("OData-EntityId"))

        return {
            "relationship_id": relationship_id,
            "relationship_schema_name": relationship.schema_name,
            "lookup_schema_name": lookup.schema_name,
            "referenced_entity": relationship.referenced_entity,
            "referencing_entity": relationship.referencing_entity,
        }

    def _create_many_to_many_relationship(
        self,
        relationship,
        solution_unique_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a many-to-many relationship.

        Posts to /RelationshipDefinitions with ManyToManyRelationshipMetadata.

        :param relationship: Relationship metadata (ManyToManyRelationshipMetadata instance).
        :type relationship: ~PowerPlatform.Dataverse.models.metadata.ManyToManyRelationshipMetadata
        :param solution_unique_name: Optional solution to add the relationship to.
        :type solution_unique_name: ``str`` | ``None``

        :return: Dictionary with relationship_id and schema name.
        :rtype: ``dict[str, Any]``

        :raises HttpError: If the Web API request fails.
        """
        url = f"{self.api}/RelationshipDefinitions"

        payload = relationship.to_dict()

        headers = self._headers().copy()
        if solution_unique_name:
            headers["MSCRM.SolutionUniqueName"] = solution_unique_name

        r = self._request("post", url, headers=headers, json=payload)

        # Extract ID from response header
        relationship_id = self._extract_id_from_header(r.headers.get("OData-EntityId"))

        return {
            "relationship_id": relationship_id,
            "relationship_schema_name": relationship.schema_name,
            "entity1_logical_name": relationship.entity1_logical_name,
            "entity2_logical_name": relationship.entity2_logical_name,
        }

    def _delete_relationship(self, relationship_id: str) -> None:
        """
        Delete a relationship by its metadata ID.

        :param relationship_id: The GUID of the relationship metadata.
        :type relationship_id: ``str``

        :raises HttpError: If the Web API request fails.
        """
        url = f"{self.api}/RelationshipDefinitions({relationship_id})"
        headers = self._headers().copy()
        headers["If-Match"] = "*"
        self._request("delete", url, headers=headers)

    def _get_relationship(self, schema_name: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve relationship metadata by schema name.

        :param schema_name: The schema name of the relationship.
        :type schema_name: ``str``

        :return: Relationship metadata dictionary, or None if not found.
        :rtype: ``dict[str, Any]`` | ``None``

        :raises HttpError: If the Web API request fails.
        """
        url = f"{self.api}/RelationshipDefinitions"
        params = {"$filter": f"SchemaName eq '{self._escape_odata_quotes(schema_name)}'"}
        r = self._request("get", url, headers=self._headers(), params=params)
        data = r.json()
        results = data.get("value", [])
        return results[0] if results else None

    def _extract_id_from_header(self, header_value: Optional[str]) -> Optional[str]:
        """
        Extract a GUID from an OData-EntityId header value.

        :param header_value: The header value containing a URL with GUID.
        :type header_value: ``str`` | ``None``

        :return: Extracted GUID or None if not found.
        :rtype: ``str`` | ``None``
        """
        if not header_value:
            return None
        match = re.search(r"\(([0-9a-fA-F-]+)\)", header_value)
        return match.group(1) if match else None
