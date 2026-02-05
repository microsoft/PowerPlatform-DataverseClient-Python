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

# =============================================================================
# OpenTelemetry Semantic Convention Attribute Names
# =============================================================================
# These follow the OpenTelemetry semantic conventions for database and HTTP spans.
# See: https://opentelemetry.io/docs/specs/semconv/

# Database semantic conventions
OTEL_ATTR_DB_SYSTEM = "db.system"
OTEL_ATTR_DB_OPERATION = "db.operation"

# HTTP semantic conventions
OTEL_ATTR_HTTP_METHOD = "http.request.method"
OTEL_ATTR_HTTP_URL = "url.full"
OTEL_ATTR_HTTP_STATUS_CODE = "http.response.status_code"

# Dataverse-specific attributes
OTEL_ATTR_DATAVERSE_TABLE = "dataverse.table"
OTEL_ATTR_DATAVERSE_REQUEST_ID = "dataverse.client_request_id"
OTEL_ATTR_DATAVERSE_CORRELATION_ID = "dataverse.correlation_id"
OTEL_ATTR_DATAVERSE_SERVICE_REQUEST_ID = "dataverse.service_request_id"
