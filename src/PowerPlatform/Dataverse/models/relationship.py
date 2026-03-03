# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Relationship models for Dataverse (input and output)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from ..common.constants import (
    CASCADE_BEHAVIOR_CASCADE,
    CASCADE_BEHAVIOR_NO_CASCADE,
    CASCADE_BEHAVIOR_REMOVE_LINK,
    CASCADE_BEHAVIOR_RESTRICT,
    ODATA_TYPE_LOOKUP_ATTRIBUTE,
    ODATA_TYPE_ONE_TO_MANY_RELATIONSHIP,
    ODATA_TYPE_MANY_TO_MANY_RELATIONSHIP,
)
from .labels import Label


@dataclass
class CascadeConfiguration:
    """
    Defines cascade behavior for relationship operations.

    :param assign: Cascade behavior for assign operations.
    :type assign: str
    :param delete: Cascade behavior for delete operations.
    :type delete: str
    :param merge: Cascade behavior for merge operations.
    :type merge: str
    :param reparent: Cascade behavior for reparent operations.
    :type reparent: str
    :param share: Cascade behavior for share operations.
    :type share: str
    :param unshare: Cascade behavior for unshare operations.
    :type unshare: str
    :param additional_properties: Optional dict of additional properties to include
        in the Web API payload (e.g., "Archive", "RollupView"). These are merged
        last and can override default values.
    :type additional_properties: Optional[Dict[str, Any]]

    Valid values for each parameter:
        - "Cascade": Perform the operation on all related records
        - "NoCascade": Do not perform the operation on related records
        - "RemoveLink": Remove the relationship link but keep the records
        - "Restrict": Prevent the operation if related records exist
    """

    assign: str = CASCADE_BEHAVIOR_NO_CASCADE
    delete: str = CASCADE_BEHAVIOR_REMOVE_LINK
    merge: str = CASCADE_BEHAVIOR_NO_CASCADE
    reparent: str = CASCADE_BEHAVIOR_NO_CASCADE
    share: str = CASCADE_BEHAVIOR_NO_CASCADE
    unshare: str = CASCADE_BEHAVIOR_NO_CASCADE
    additional_properties: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to Web API JSON format.

        Example::

            >>> config = CascadeConfiguration(delete="Cascade", assign="NoCascade")
            >>> config.to_dict()
            {
                'Assign': 'NoCascade',
                'Delete': 'Cascade',
                'Merge': 'NoCascade',
                'Reparent': 'NoCascade',
                'Share': 'NoCascade',
                'Unshare': 'NoCascade'
            }
        """
        result = {
            "Assign": self.assign,
            "Delete": self.delete,
            "Merge": self.merge,
            "Reparent": self.reparent,
            "Share": self.share,
            "Unshare": self.unshare,
        }
        if self.additional_properties:
            result.update(self.additional_properties)
        return result


@dataclass
class LookupAttributeMetadata:
    """
    Metadata for a lookup attribute.

    :param schema_name: Schema name for the attribute (e.g., "new_AccountId").
    :type schema_name: str
    :param display_name: Display name for the attribute.
    :type display_name: Label
    :param description: Optional description of the attribute.
    :type description: Optional[Label]
    :param required_level: Requirement level for the attribute.
    :type required_level: str
    :param additional_properties: Optional dict of additional properties to include
        in the Web API payload. Useful for setting properties like "Targets" (to
        specify which entity types the lookup can reference), "LogicalName",
        "IsSecured", "IsValidForAdvancedFind", etc. These are merged last and
        can override default values.
    :type additional_properties: Optional[Dict[str, Any]]

    Valid required_level values:
        - "None": The attribute is optional
        - "Recommended": The attribute is recommended
        - "ApplicationRequired": The attribute is required
    """

    schema_name: str
    display_name: Label
    description: Optional[Label] = None
    required_level: str = "None"
    additional_properties: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to Web API JSON format.

        Example::

            >>> lookup = LookupAttributeMetadata(
            ...     schema_name="new_AccountId",
            ...     display_name=Label([LocalizedLabel("Account", 1033)])
            ... )
            >>> lookup.to_dict()
            {
                '@odata.type': 'Microsoft.Dynamics.CRM.LookupAttributeMetadata',
                'SchemaName': 'new_AccountId',
                'AttributeType': 'Lookup',
                'AttributeTypeName': {'Value': 'LookupType'},
                'DisplayName': {...},
                'RequiredLevel': {'Value': 'None', 'CanBeChanged': True, ...}
            }
        """
        result = {
            "@odata.type": ODATA_TYPE_LOOKUP_ATTRIBUTE,
            "SchemaName": self.schema_name,
            "AttributeType": "Lookup",
            "AttributeTypeName": {"Value": "LookupType"},
            "DisplayName": self.display_name.to_dict(),
            "RequiredLevel": {
                "Value": self.required_level,
                "CanBeChanged": True,
                "ManagedPropertyLogicalName": "canmodifyrequirementlevelsettings",
            },
        }
        if self.description:
            result["Description"] = self.description.to_dict()
        if self.additional_properties:
            result.update(self.additional_properties)
        return result


@dataclass
class OneToManyRelationshipMetadata:
    """
    Metadata for a one-to-many entity relationship.

    :param schema_name: Schema name for the relationship (e.g., "new_Account_Orders").
    :type schema_name: str
    :param referenced_entity: Logical name of the referenced (parent) entity.
    :type referenced_entity: str
    :param referencing_entity: Logical name of the referencing (child) entity.
    :type referencing_entity: str
    :param referenced_attribute: Attribute on the referenced entity (typically the primary key).
    :type referenced_attribute: str
    :param cascade_configuration: Cascade behavior configuration.
    :type cascade_configuration: CascadeConfiguration
    :param referencing_attribute: Optional name for the referencing attribute (usually auto-generated).
    :type referencing_attribute: Optional[str]
    :param additional_properties: Optional dict of additional properties to include
        in the Web API payload. Useful for setting inherited properties like
        "IsValidForAdvancedFind", "IsCustomizable", "SecurityTypes", etc.
        These are merged last and can override default values.
    :type additional_properties: Optional[Dict[str, Any]]
    """

    schema_name: str
    referenced_entity: str
    referencing_entity: str
    referenced_attribute: str
    cascade_configuration: CascadeConfiguration = field(default_factory=CascadeConfiguration)
    referencing_attribute: Optional[str] = None
    additional_properties: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to Web API JSON format.

        Example::

            >>> rel = OneToManyRelationshipMetadata(
            ...     schema_name="new_account_orders",
            ...     referenced_entity="account",
            ...     referencing_entity="new_order",
            ...     referenced_attribute="accountid"
            ... )
            >>> rel.to_dict()
            {
                '@odata.type': 'Microsoft.Dynamics.CRM.OneToManyRelationshipMetadata',
                'SchemaName': 'new_account_orders',
                'ReferencedEntity': 'account',
                'ReferencingEntity': 'new_order',
                'ReferencedAttribute': 'accountid',
                'CascadeConfiguration': {...}
            }
        """
        result = {
            "@odata.type": ODATA_TYPE_ONE_TO_MANY_RELATIONSHIP,
            "SchemaName": self.schema_name,
            "ReferencedEntity": self.referenced_entity,
            "ReferencingEntity": self.referencing_entity,
            "ReferencedAttribute": self.referenced_attribute,
            "CascadeConfiguration": self.cascade_configuration.to_dict(),
        }
        if self.referencing_attribute:
            result["ReferencingAttribute"] = self.referencing_attribute
        if self.additional_properties:
            result.update(self.additional_properties)
        return result


@dataclass
class ManyToManyRelationshipMetadata:
    """
    Metadata for a many-to-many entity relationship.

    :param schema_name: Schema name for the relationship.
    :type schema_name: str
    :param entity1_logical_name: Logical name of the first entity.
    :type entity1_logical_name: str
    :param entity2_logical_name: Logical name of the second entity.
    :type entity2_logical_name: str
    :param intersect_entity_name: Name for the intersect table (defaults to schema_name if not provided).
    :type intersect_entity_name: Optional[str]
    :param additional_properties: Optional dict of additional properties to include
        in the Web API payload. Useful for setting inherited properties like
        "IsValidForAdvancedFind", "IsCustomizable", "SecurityTypes", or direct
        properties like "Entity1NavigationPropertyName". These are merged last
        and can override default values.
    :type additional_properties: Optional[Dict[str, Any]]
    """

    schema_name: str
    entity1_logical_name: str
    entity2_logical_name: str
    intersect_entity_name: Optional[str] = None
    additional_properties: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to Web API JSON format.

        Example::

            >>> rel = ManyToManyRelationshipMetadata(
            ...     schema_name="new_account_contact",
            ...     entity1_logical_name="account",
            ...     entity2_logical_name="contact"
            ... )
            >>> rel.to_dict()
            {
                '@odata.type': 'Microsoft.Dynamics.CRM.ManyToManyRelationshipMetadata',
                'SchemaName': 'new_account_contact',
                'Entity1LogicalName': 'account',
                'Entity2LogicalName': 'contact',
                'IntersectEntityName': 'new_account_contact'
            }
        """
        # IntersectEntityName is required - use provided value or default to schema_name
        intersect_name = self.intersect_entity_name or self.schema_name
        result = {
            "@odata.type": ODATA_TYPE_MANY_TO_MANY_RELATIONSHIP,
            "SchemaName": self.schema_name,
            "Entity1LogicalName": self.entity1_logical_name,
            "Entity2LogicalName": self.entity2_logical_name,
            "IntersectEntityName": intersect_name,
        }
        if self.additional_properties:
            result.update(self.additional_properties)
        return result


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
        Dataverse only supports these two relationship types; an unrecognized
        ``@odata.type`` raises :class:`ValueError`.

        :param response_data: Raw relationship metadata from the Web API.
        :type response_data: :class:`dict`
        :rtype: :class:`RelationshipInfo`
        :raises ValueError: If the ``@odata.type`` is not a recognized
            relationship type.
        """
        odata_type = response_data.get("@odata.type", "")
        rel_id = response_data.get("MetadataId")
        schema_name = response_data.get("SchemaName", "")

        if ODATA_TYPE_ONE_TO_MANY_RELATIONSHIP in odata_type:
            return cls.from_one_to_many(
                relationship_id=rel_id,
                relationship_schema_name=schema_name,
                referenced_entity=response_data["ReferencedEntity"],
                referencing_entity=response_data["ReferencingEntity"],
                lookup_schema_name=response_data.get(
                    "ReferencingEntityNavigationPropertyName", ""
                ),  # nav property may be absent
            )

        if ODATA_TYPE_MANY_TO_MANY_RELATIONSHIP in odata_type:
            return cls.from_many_to_many(
                relationship_id=rel_id,
                relationship_schema_name=schema_name,
                entity1_logical_name=response_data["Entity1LogicalName"],
                entity2_logical_name=response_data["Entity2LogicalName"],
            )

        raise ValueError(f"Unrecognized relationship @odata.type: {odata_type!r}")


__all__ = [
    "RelationshipInfo",
    "CascadeConfiguration",
    "LookupAttributeMetadata",
    "OneToManyRelationshipMetadata",
    "ManyToManyRelationshipMetadata",
]
