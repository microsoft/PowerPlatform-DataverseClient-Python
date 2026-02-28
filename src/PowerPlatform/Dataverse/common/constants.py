# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Constants for Dataverse Web API metadata types.

These constants define the OData type identifiers used in Web API payloads
for metadata operations.
"""

# OData type identifiers for metadata entities
ODATA_TYPE_LOCALIZED_LABEL = "Microsoft.Dynamics.CRM.LocalizedLabel"
ODATA_TYPE_LABEL = "Microsoft.Dynamics.CRM.Label"
ODATA_TYPE_LOOKUP_ATTRIBUTE = "Microsoft.Dynamics.CRM.LookupAttributeMetadata"
ODATA_TYPE_ONE_TO_MANY_RELATIONSHIP = "Microsoft.Dynamics.CRM.OneToManyRelationshipMetadata"
ODATA_TYPE_MANY_TO_MANY_RELATIONSHIP = "Microsoft.Dynamics.CRM.ManyToManyRelationshipMetadata"

# Cascade behavior values for relationship operations
# See: https://learn.microsoft.com/en-us/power-apps/developer/data-platform/configure-entity-relationship-cascading-behavior

CASCADE_BEHAVIOR_CASCADE = "Cascade"
"""Perform the action on all referencing table records associated with the referenced table record."""

CASCADE_BEHAVIOR_NO_CASCADE = "NoCascade"
"""Do not apply the action to any referencing table records associated with the referenced table record."""

CASCADE_BEHAVIOR_REMOVE_LINK = "RemoveLink"
"""Remove the value of the referencing column for all referencing table records when the referenced record is deleted."""

CASCADE_BEHAVIOR_RESTRICT = "Restrict"
"""Prevent the referenced table record from being deleted when referencing table records exist."""

# AttributeMetadata derived type OData identifiers
# Used when casting Attributes collection to a specific derived type in Web API URLs
ODATA_TYPE_PICKLIST_ATTRIBUTE = "Microsoft.Dynamics.CRM.PicklistAttributeMetadata"
ODATA_TYPE_BOOLEAN_ATTRIBUTE = "Microsoft.Dynamics.CRM.BooleanAttributeMetadata"
ODATA_TYPE_MULTISELECT_PICKLIST_ATTRIBUTE = "Microsoft.Dynamics.CRM.MultiSelectPicklistAttributeMetadata"
ODATA_TYPE_STRING_ATTRIBUTE = "Microsoft.Dynamics.CRM.StringAttributeMetadata"
ODATA_TYPE_INTEGER_ATTRIBUTE = "Microsoft.Dynamics.CRM.IntegerAttributeMetadata"
ODATA_TYPE_DECIMAL_ATTRIBUTE = "Microsoft.Dynamics.CRM.DecimalAttributeMetadata"
ODATA_TYPE_DOUBLE_ATTRIBUTE = "Microsoft.Dynamics.CRM.DoubleAttributeMetadata"
ODATA_TYPE_MONEY_ATTRIBUTE = "Microsoft.Dynamics.CRM.MoneyAttributeMetadata"
ODATA_TYPE_DATETIME_ATTRIBUTE = "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata"
ODATA_TYPE_MEMO_ATTRIBUTE = "Microsoft.Dynamics.CRM.MemoAttributeMetadata"
ODATA_TYPE_FILE_ATTRIBUTE = "Microsoft.Dynamics.CRM.FileAttributeMetadata"

# Attribute type code values returned in the AttributeType property of attribute metadata
ATTRIBUTE_TYPE_PICKLIST = "Picklist"
ATTRIBUTE_TYPE_BOOLEAN = "Boolean"
ATTRIBUTE_TYPE_STRING = "String"
ATTRIBUTE_TYPE_INTEGER = "Integer"
ATTRIBUTE_TYPE_DECIMAL = "Decimal"
ATTRIBUTE_TYPE_DOUBLE = "Double"
ATTRIBUTE_TYPE_MONEY = "Money"
ATTRIBUTE_TYPE_DATETIME = "DateTime"
ATTRIBUTE_TYPE_MEMO = "Memo"
ATTRIBUTE_TYPE_LOOKUP = "Lookup"
ATTRIBUTE_TYPE_UNIQUEIDENTIFIER = "Uniqueidentifier"
