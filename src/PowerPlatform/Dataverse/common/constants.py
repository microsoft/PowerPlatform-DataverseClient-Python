# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Constants for the Dataverse SDK.
"""

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
