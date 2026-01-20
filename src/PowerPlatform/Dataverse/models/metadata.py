# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Metadata entity types for Microsoft Dataverse.

These classes represent the metadata entity types used in the Dataverse Web API
for defining and managing table definitions, attributes, and relationships.

See: https://learn.microsoft.com/en-us/power-apps/developer/data-platform/webapi/reference/metadataentitytypes
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from dataclasses import dataclass, field


@dataclass
class LocalizedLabel:
    """
    Represents a localized label with a language code.

    :param label: The text of the label.
    :type label: str
    :param language_code: The language code (LCID), e.g., 1033 for English.
    :type language_code: int
    :param additional_properties: Optional dict of additional properties to include
        in the Web API payload. These are merged last and can override default values.
    :type additional_properties: Optional[Dict[str, Any]]
    """

    label: str
    language_code: int
    additional_properties: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to Web API JSON format."""
        result = {
            "@odata.type": "Microsoft.Dynamics.CRM.LocalizedLabel",
            "Label": self.label,
            "LanguageCode": self.language_code,
        }
        if self.additional_properties:
            result.update(self.additional_properties)
        return result


@dataclass
class Label:
    """
    Represents a label that can have multiple localized versions.

    :param localized_labels: List of LocalizedLabel instances.
    :type localized_labels: List[LocalizedLabel]
    :param user_localized_label: Optional user-specific localized label.
    :type user_localized_label: Optional[LocalizedLabel]
    :param additional_properties: Optional dict of additional properties to include
        in the Web API payload. These are merged last and can override default values.
    :type additional_properties: Optional[Dict[str, Any]]
    """

    localized_labels: List[LocalizedLabel]
    user_localized_label: Optional[LocalizedLabel] = None
    additional_properties: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to Web API JSON format."""
        result = {
            "@odata.type": "Microsoft.Dynamics.CRM.Label",
            "LocalizedLabels": [ll.to_dict() for ll in self.localized_labels],
        }
        # Use explicit user_localized_label, or default to first localized label
        if self.user_localized_label:
            result["UserLocalizedLabel"] = self.user_localized_label.to_dict()
        elif self.localized_labels:
            result["UserLocalizedLabel"] = self.localized_labels[0].to_dict()
        if self.additional_properties:
            result.update(self.additional_properties)
        return result


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

    assign: str = "NoCascade"
    delete: str = "RemoveLink"
    merge: str = "NoCascade"
    reparent: str = "NoCascade"
    share: str = "NoCascade"
    unshare: str = "NoCascade"
    additional_properties: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to Web API JSON format."""
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
class AssociatedMenuConfiguration:
    """
    Configuration for how the relationship appears in the associated menu.

    :param behavior: Display behavior in the menu.
    :type behavior: str
    :param group: The menu group where the item appears.
    :type group: str
    :param label: Display label for the menu item.
    :type label: Optional[Label]
    :param order: Display order within the group.
    :type order: int
    :param additional_properties: Optional dict of additional properties to include
        in the Web API payload (e.g., "Icon", "ViewId", "AvailableOffline").
        These are merged last and can override default values.
    :type additional_properties: Optional[Dict[str, Any]]

    Valid behavior values:
        - "UseCollectionName": Use the collection name
        - "UseLabel": Use the specified label
        - "DoNotDisplay": Do not display in the menu
    """

    behavior: str = "UseLabel"
    group: str = "Details"
    label: Optional[Label] = None
    order: int = 10000
    additional_properties: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to Web API JSON format."""
        result = {
            "Behavior": self.behavior,
            "Group": self.group,
            "Order": self.order,
        }
        if self.label:
            result["Label"] = self.label.to_dict()
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
        """Convert to Web API JSON format."""
        result = {
            "@odata.type": "Microsoft.Dynamics.CRM.LookupAttributeMetadata",
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
    :param associated_menu_configuration: Optional menu display configuration.
    :type associated_menu_configuration: Optional[AssociatedMenuConfiguration]
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
    associated_menu_configuration: Optional[AssociatedMenuConfiguration] = None
    referencing_attribute: Optional[str] = None
    additional_properties: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to Web API JSON format."""
        result = {
            "@odata.type": "Microsoft.Dynamics.CRM.OneToManyRelationshipMetadata",
            "SchemaName": self.schema_name,
            "ReferencedEntity": self.referenced_entity,
            "ReferencingEntity": self.referencing_entity,
            "ReferencedAttribute": self.referenced_attribute,
            "CascadeConfiguration": self.cascade_configuration.to_dict(),
        }
        if self.associated_menu_configuration:
            result["AssociatedMenuConfiguration"] = self.associated_menu_configuration.to_dict()
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
    :param entity1_associated_menu_configuration: Menu configuration for entity1.
    :type entity1_associated_menu_configuration: Optional[AssociatedMenuConfiguration]
    :param entity2_associated_menu_configuration: Menu configuration for entity2.
    :type entity2_associated_menu_configuration: Optional[AssociatedMenuConfiguration]
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
    entity1_associated_menu_configuration: Optional[AssociatedMenuConfiguration] = None
    entity2_associated_menu_configuration: Optional[AssociatedMenuConfiguration] = None
    additional_properties: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to Web API JSON format."""
        # IntersectEntityName is required - use provided value or default to schema_name
        intersect_name = self.intersect_entity_name or self.schema_name
        result = {
            "@odata.type": "Microsoft.Dynamics.CRM.ManyToManyRelationshipMetadata",
            "SchemaName": self.schema_name,
            "Entity1LogicalName": self.entity1_logical_name,
            "Entity2LogicalName": self.entity2_logical_name,
            "IntersectEntityName": intersect_name,
        }
        if self.entity1_associated_menu_configuration:
            result["Entity1AssociatedMenuConfiguration"] = self.entity1_associated_menu_configuration.to_dict()
        if self.entity2_associated_menu_configuration:
            result["Entity2AssociatedMenuConfiguration"] = self.entity2_associated_menu_configuration.to_dict()
        if self.additional_properties:
            result.update(self.additional_properties)
        return result


__all__ = [
    "LocalizedLabel",
    "Label",
    "CascadeConfiguration",
    "AssociatedMenuConfiguration",
    "LookupAttributeMetadata",
    "OneToManyRelationshipMetadata",
    "ManyToManyRelationshipMetadata",
]
