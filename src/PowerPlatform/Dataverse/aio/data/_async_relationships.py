# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Async relationship metadata operations for Dataverse Web API.

This module provides mixin functionality for relationship CRUD operations.
"""

from __future__ import annotations

__all__ = []

import re
from typing import Any, Dict, List, Optional


class _AsyncRelationshipOperationsMixin:
    """
    Async mixin providing relationship metadata operations.

    This mixin is designed to be used with _AsyncODataClient and depends on:
    - self.api: The API base URL
    - self._headers(): Method to get auth headers
    - self._request(): Async method to make HTTP requests
    """

    async def _create_one_to_many_relationship(
        self,
        lookup,
        relationship,
        solution: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a one-to-many relationship with lookup attribute.

        Posts to /RelationshipDefinitions with OneToManyRelationshipMetadata.

        :param lookup: Lookup attribute metadata (LookupAttributeMetadata instance).
        :type lookup: ~PowerPlatform.Dataverse.models.relationship.LookupAttributeMetadata
        :param relationship: Relationship metadata (OneToManyRelationshipMetadata instance).
        :type relationship: ~PowerPlatform.Dataverse.models.relationship.OneToManyRelationshipMetadata
        :param solution: Optional solution unique name to add the relationship to.
        :type solution: ``str`` | ``None``

        :return: Dictionary with relationship_id, attribute_id, and schema names.
        :rtype: ``dict[str, Any]``

        :raises HttpError: If the Web API request fails.
        """
        url = f"{self.api}/RelationshipDefinitions"

        # Build the payload by combining relationship and lookup metadata
        payload = relationship.to_dict()
        payload["Lookup"] = lookup.to_dict()

        headers = (await self._headers()).copy()
        if solution:
            headers["MSCRM.SolutionUniqueName"] = solution

        r = await self._request("post", url, headers=headers, json=payload)

        # Extract IDs from response headers
        relationship_id = self._extract_id_from_header(r.headers.get("OData-EntityId"))

        return {
            "relationship_id": relationship_id,
            "relationship_schema_name": relationship.schema_name,
            "lookup_schema_name": lookup.schema_name,
            "referenced_entity": relationship.referenced_entity,
            "referencing_entity": relationship.referencing_entity,
        }

    async def _create_many_to_many_relationship(
        self,
        relationship,
        solution: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a many-to-many relationship.

        Posts to /RelationshipDefinitions with ManyToManyRelationshipMetadata.

        :param relationship: Relationship metadata (ManyToManyRelationshipMetadata instance).
        :type relationship: ~PowerPlatform.Dataverse.models.relationship.ManyToManyRelationshipMetadata
        :param solution: Optional solution unique name to add the relationship to.
        :type solution: ``str`` | ``None``

        :return: Dictionary with relationship_id and schema name.
        :rtype: ``dict[str, Any]``

        :raises HttpError: If the Web API request fails.
        """
        url = f"{self.api}/RelationshipDefinitions"

        payload = relationship.to_dict()

        headers = (await self._headers()).copy()
        if solution:
            headers["MSCRM.SolutionUniqueName"] = solution

        r = await self._request("post", url, headers=headers, json=payload)

        # Extract ID from response header
        relationship_id = self._extract_id_from_header(r.headers.get("OData-EntityId"))

        return {
            "relationship_id": relationship_id,
            "relationship_schema_name": relationship.schema_name,
            "entity1_logical_name": relationship.entity1_logical_name,
            "entity2_logical_name": relationship.entity2_logical_name,
        }

    async def _delete_relationship(self, relationship_id: str) -> None:
        """
        Delete a relationship by its metadata ID.

        :param relationship_id: The GUID of the relationship metadata.
        :type relationship_id: ``str``

        :raises HttpError: If the Web API request fails.
        """
        url = f"{self.api}/RelationshipDefinitions({relationship_id})"
        headers = (await self._headers()).copy()
        headers["If-Match"] = "*"
        await self._request("delete", url, headers=headers)

    async def _get_relationship(self, schema_name: str) -> Optional[Dict[str, Any]]:
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
        r = await self._request("get", url, headers=await self._headers(), params=params)
        data = await r.json()
        results = data.get("value", [])
        return results[0] if results else None

    async def _list_relationships(
        self,
        *,
        filter: Optional[str] = None,
        select: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """List all relationship definitions.

        Issues ``GET /RelationshipDefinitions`` with optional ``$filter`` and
        ``$select`` query parameters.

        :param filter: Optional OData ``$filter`` expression.  For example,
            ``"RelationshipType eq Microsoft.Dynamics.CRM.RelationshipType'OneToManyRelationship'"``
            returns only one-to-many relationships.
        :type filter: ``str`` or ``None``
        :param select: Optional list of property names to project via
            ``$select``.  Values are passed as-is (PascalCase).
        :type select: ``list[str]`` or ``None``

        :return: List of raw relationship metadata dictionaries (may be empty).
        :rtype: ``list[dict[str, Any]]``

        :raises HttpError: If the Web API request fails.
        """
        url = f"{self.api}/RelationshipDefinitions"
        params: Dict[str, str] = {}
        if filter:
            params["$filter"] = filter
        if select:
            params["$select"] = ",".join(select)
        r = await self._request("get", url, headers=await self._headers(), params=params)
        return (await r.json()).get("value", [])

    async def _list_table_relationships(
        self,
        table_schema_name: str,
        *,
        filter: Optional[str] = None,
        select: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """List all relationships for a specific table.

        Issues ``GET EntityDefinitions({MetadataId})/OneToManyRelationships``,
        ``GET EntityDefinitions({MetadataId})/ManyToOneRelationships``, and
        ``GET EntityDefinitions({MetadataId})/ManyToManyRelationships``,
        then combines the results.

        :param table_schema_name: Schema name of the table (e.g. ``"account"``).
        :type table_schema_name: ``str``
        :param filter: Optional OData ``$filter`` expression applied to each
            sub-request.
        :type filter: ``str`` or ``None``
        :param select: Optional list of property names to project via
            ``$select``.  Values are passed as-is (PascalCase).
        :type select: ``list[str]`` or ``None``

        :return: Combined list of one-to-many, many-to-one, and many-to-many
            relationship metadata dictionaries (may be empty).
        :rtype: ``list[dict[str, Any]]``

        :raises MetadataError: If the table is not found.
        :raises HttpError: If the Web API request fails.
        """
        from ...core.errors import MetadataError
        from ...core._error_codes import METADATA_TABLE_NOT_FOUND

        ent = await self._get_entity_by_table_schema_name(table_schema_name)
        if not ent or not ent.get("MetadataId"):
            raise MetadataError(
                f"Table '{table_schema_name}' not found.",
                subcode=METADATA_TABLE_NOT_FOUND,
            )

        metadata_id = ent["MetadataId"]
        # OneToMany/ManyToOne share the same property surface (ReferencedEntity,
        # ReferencingEntity, etc.).  ManyToManyRelationshipMetadata has a
        # different schema -- it only exposes SchemaName plus Entity1/Entity2
        # fields, not ReferencedEntity or ReferencingEntity.  Sending a $select
        # that includes those properties to the ManyToMany endpoint causes a
        # 400: "Could not find a property named 'ReferencedEntity' on type
        # 'ManyToManyRelationshipMetadata'".  Use separate param dicts.
        one_to_many_params: Dict[str, str] = {}
        many_to_many_params: Dict[str, str] = {}
        if filter:
            one_to_many_params["$filter"] = filter
            many_to_many_params["$filter"] = filter
        if select:
            one_to_many_params["$select"] = ",".join(select)

        one_to_many_url = f"{self.api}/EntityDefinitions({metadata_id})/OneToManyRelationships"
        many_to_one_url = f"{self.api}/EntityDefinitions({metadata_id})/ManyToOneRelationships"
        many_to_many_url = f"{self.api}/EntityDefinitions({metadata_id})/ManyToManyRelationships"

        r1 = await self._request("get", one_to_many_url, headers=await self._headers(), params=one_to_many_params)
        r2 = await self._request("get", many_to_one_url, headers=await self._headers(), params=one_to_many_params)
        r3 = await self._request("get", many_to_many_url, headers=await self._headers(), params=many_to_many_params)

        return (
            (await r1.json()).get("value", []) + (await r2.json()).get("value", []) + (await r3.json()).get("value", [])
        )

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
