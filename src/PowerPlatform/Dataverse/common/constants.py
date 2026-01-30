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
CASCADE_BEHAVIOR_CASCADE = "Cascade"
CASCADE_BEHAVIOR_NO_CASCADE = "NoCascade"
CASCADE_BEHAVIOR_REMOVE_LINK = "RemoveLink"
CASCADE_BEHAVIOR_RESTRICT = "Restrict"
