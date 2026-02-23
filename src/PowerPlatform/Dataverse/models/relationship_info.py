# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Typed return model for relationship metadata."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from PowerPlatform.Dataverse.common.constants import (
    ODATA_TYPE_ONE_TO_MANY_RELATIONSHIP,
    ODATA_TYPE_MANY_TO_MANY_RELATIONSHIP,
)

__all__ = ["RelationshipInfo"]


@dataclass
class RelationshipInfo:
    """Typed return model for relationship metadata.

    Returned by :meth:`~PowerPlatform.Dataverse.operations.tables.TableOperations.create_one_to_many_relationship`,
    :meth:`~PowerPlatform.Dataverse.operations.tables.TableOperations.create_many_to_many_relationship`,
    :meth:`~PowerPlatform.Dataverse.operations.tables.TableOperations.get_relationship`, and
    :meth:`~PowerPlatform.Dataverse.operations.tables.TableOperations.create_lookup_field`.

    :param relationship_id: Relationship metadata GUID.
    :type relationship_id: :class:`str` or None
    :param relationship_schema_name: Relationship schema name.
    :type relationship_schema_name: :class:`str`
    :param relationship_type: Either ``"one_to_many"`` or ``"many_to_many"``.
    :type relationship_type: :class:`str`
    :param lookup_schema_name: Lookup field schema name (one-to-many only).
    :type lookup_schema_name: :class:`str` or None
    :param referenced_entity: Parent entity logical name (one-to-many only).
    :type referenced_entity: :class:`str` or None
    :param referencing_entity: Child entity logical name (one-to-many only).
    :type referencing_entity: :class:`str` or None
    :param entity1_logical_name: First entity logical name (many-to-many only).
    :type entity1_logical_name: :class:`str` or None
    :param entity2_logical_name: Second entity logical name (many-to-many only).
    :type entity2_logical_name: :class:`str` or None

    Example::

        result = client.tables.create_one_to_many_relationship(lookup, relationship)
        print(result.relationship_schema_name)
        print(result.lookup_schema_name)
    """

    relationship_id: Optional[str] = None
    relationship_schema_name: str = ""
    relationship_type: str = ""

    # One-to-many specific
    lookup_schema_name: Optional[str] = None
    referenced_entity: Optional[str] = None
    referencing_entity: Optional[str] = None

    # Many-to-many specific
    entity1_logical_name: Optional[str] = None
    entity2_logical_name: Optional[str] = None

    @classmethod
    def from_one_to_many(
        cls,
        *,
        relationship_id: Optional[str],
        relationship_schema_name: str,
        lookup_schema_name: str,
        referenced_entity: str,
        referencing_entity: str,
    ) -> RelationshipInfo:
        """Create from a one-to-many relationship result.

        :param relationship_id: Relationship metadata GUID.
        :type relationship_id: :class:`str` or None
        :param relationship_schema_name: Relationship schema name.
        :type relationship_schema_name: :class:`str`
        :param lookup_schema_name: Lookup field schema name.
        :type lookup_schema_name: :class:`str`
        :param referenced_entity: Parent entity logical name.
        :type referenced_entity: :class:`str`
        :param referencing_entity: Child entity logical name.
        :type referencing_entity: :class:`str`
        :rtype: :class:`RelationshipInfo`
        """
        return cls(
            relationship_id=relationship_id,
            relationship_schema_name=relationship_schema_name,
            relationship_type="one_to_many",
            lookup_schema_name=lookup_schema_name,
            referenced_entity=referenced_entity,
            referencing_entity=referencing_entity,
        )

    @classmethod
    def from_many_to_many(
        cls,
        *,
        relationship_id: Optional[str],
        relationship_schema_name: str,
        entity1_logical_name: str,
        entity2_logical_name: str,
    ) -> RelationshipInfo:
        """Create from a many-to-many relationship result.

        :param relationship_id: Relationship metadata GUID.
        :type relationship_id: :class:`str` or None
        :param relationship_schema_name: Relationship schema name.
        :type relationship_schema_name: :class:`str`
        :param entity1_logical_name: First entity logical name.
        :type entity1_logical_name: :class:`str`
        :param entity2_logical_name: Second entity logical name.
        :type entity2_logical_name: :class:`str`
        :rtype: :class:`RelationshipInfo`
        """
        return cls(
            relationship_id=relationship_id,
            relationship_schema_name=relationship_schema_name,
            relationship_type="many_to_many",
            entity1_logical_name=entity1_logical_name,
            entity2_logical_name=entity2_logical_name,
        )

    @classmethod
    def from_api_response(cls, response_data: Dict[str, Any]) -> RelationshipInfo:
        """Create from a raw Dataverse Web API response.

        Detects one-to-many vs many-to-many from the ``@odata.type`` field
        in the response and maps PascalCase keys to snake_case attributes.

        :param response_data: Raw relationship metadata from the Web API.
        :type response_data: :class:`dict`
        :rtype: :class:`RelationshipInfo`
        """
        odata_type = response_data.get("@odata.type", "")
        rel_id = response_data.get("MetadataId")
        schema_name = response_data.get("SchemaName", "")

        if ODATA_TYPE_ONE_TO_MANY_RELATIONSHIP in odata_type:
            return cls(
                relationship_id=rel_id,
                relationship_schema_name=schema_name,
                relationship_type="one_to_many",
                referenced_entity=response_data.get("ReferencedEntity"),
                referencing_entity=response_data.get("ReferencingEntity"),
                lookup_schema_name=response_data.get("ReferencingEntityNavigationPropertyName"),
            )

        if ODATA_TYPE_MANY_TO_MANY_RELATIONSHIP in odata_type:
            return cls(
                relationship_id=rel_id,
                relationship_schema_name=schema_name,
                relationship_type="many_to_many",
                entity1_logical_name=response_data.get("Entity1LogicalName"),
                entity2_logical_name=response_data.get("Entity2LogicalName"),
            )

        # Fallback: unknown type, populate what we can
        return cls(
            relationship_id=rel_id,
            relationship_schema_name=schema_name,
        )
