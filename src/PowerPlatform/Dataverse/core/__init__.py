# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Core infrastructure components for the Dataverse SDK.

This module contains the foundational components including authentication,
configuration, HTTP client, and error handling.
"""

from .results import (
    # New fluent API types
    RequestMetadata,
    DataverseResponse,
    FluentResult,
    # Legacy types (backward compatible)
    OperationResult,
    CreateResult,
    UpdateResult,
    DeleteResult,
    GetResult,
    PagedResult,
)

__all__ = [
    # New fluent API types
    "RequestMetadata",
    "DataverseResponse",
    "FluentResult",
    # Legacy types (backward compatible)
    "OperationResult",
    "CreateResult",
    "UpdateResult",
    "DeleteResult",
    "GetResult",
    "PagedResult",
]
