# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

# HTTP subcode constants
HTTP_400 = "http_400"
HTTP_401 = "http_401"
HTTP_403 = "http_403"
HTTP_404 = "http_404"
HTTP_409 = "http_409"
HTTP_412 = "http_412"
HTTP_415 = "http_415"
HTTP_429 = "http_429"
HTTP_500 = "http_500"
HTTP_502 = "http_502"
HTTP_503 = "http_503"
HTTP_504 = "http_504"

ALL_HTTP_SUBCODES = {
    HTTP_400,
    HTTP_401,
    HTTP_403,
    HTTP_404,
    HTTP_409,
    HTTP_412,
    HTTP_415,
    HTTP_429,
    HTTP_500,
    HTTP_502,
    HTTP_503,
    HTTP_504,
}

# Validation subcodes
VALIDATION_SQL_NOT_STRING = "validation_sql_not_string"
VALIDATION_SQL_EMPTY = "validation_sql_empty"
VALIDATION_ENUM_NO_MEMBERS = "validation_enum_no_members"
VALIDATION_ENUM_NON_INT_VALUE = "validation_enum_non_int_value"
VALIDATION_UNSUPPORTED_COLUMN_TYPE = "validation_unsupported_column_type"
VALIDATION_UNSUPPORTED_CACHE_KIND = "validation_unsupported_cache_kind"

# SQL parse subcodes
SQL_PARSE_TABLE_NOT_FOUND = "sql_parse_table_not_found"

# Metadata subcodes
METADATA_ENTITYSET_NOT_FOUND = "metadata_entityset_not_found"
METADATA_ENTITYSET_NAME_MISSING = "metadata_entityset_name_missing"
METADATA_TABLE_NOT_FOUND = "metadata_table_not_found"
METADATA_TABLE_ALREADY_EXISTS = "metadata_table_already_exists"
METADATA_COLUMN_NOT_FOUND = "metadata_column_not_found"
METADATA_ATTRIBUTE_RETRY_EXHAUSTED = "metadata_attribute_retry_exhausted"
METADATA_PICKLIST_RETRY_EXHAUSTED = "metadata_picklist_retry_exhausted"

# Mapping from status code -> subcode
HTTP_STATUS_TO_SUBCODE: dict[int, str] = {
    400: HTTP_400,
    401: HTTP_401,
    403: HTTP_403,
    404: HTTP_404,
    409: HTTP_409,
    412: HTTP_412,
    415: HTTP_415,
    429: HTTP_429,
    500: HTTP_500,
    502: HTTP_502,
    503: HTTP_503,
    504: HTTP_504,
}

TRANSIENT_STATUS = {429, 502, 503, 504}

def http_subcode(status: int) -> str:
    return HTTP_STATUS_TO_SUBCODE.get(status, f"http_{status}")

def is_transient_status(status: int) -> bool:
    return status in TRANSIENT_STATUS
