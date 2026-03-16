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


# OpenTelemetry semantic convention attribute names
# See: https://opentelemetry.io/docs/specs/semconv/

OTEL_ATTR_DB_SYSTEM = "db.system"
"""Database system identifier."""

OTEL_ATTR_DB_OPERATION = "db.operation"
"""Database operation name."""

OTEL_ATTR_HTTP_METHOD = "http.request.method"
"""HTTP request method."""

OTEL_ATTR_HTTP_URL = "url.full"
"""Full HTTP request URL."""

OTEL_ATTR_HTTP_STATUS_CODE = "http.response.status_code"
"""HTTP response status code."""

OTEL_ATTR_DATAVERSE_TABLE = "dataverse.table"
"""Dataverse table (entity) name."""

OTEL_ATTR_DATAVERSE_REQUEST_ID = "dataverse.client_request_id"
"""Client-generated request ID (x-ms-client-request-id header)."""

OTEL_ATTR_DATAVERSE_CORRELATION_ID = "dataverse.correlation_id"
"""Client-generated correlation ID (x-ms-correlation-id header)."""

OTEL_ATTR_DATAVERSE_SERVICE_REQUEST_ID = "dataverse.service_request_id"
"""Server-assigned request ID (x-ms-service-request-id header)."""
