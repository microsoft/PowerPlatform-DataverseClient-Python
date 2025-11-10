# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Core infrastructure components for the Dataverse SDK.

This module contains the foundational components including authentication,
configuration, HTTP client, and error handling.
"""

from .auth import AuthManager, TokenPair
from .config import DataverseConfig
from .errors import (
    DataverseError,
    HttpError,
    ValidationError,
    MetadataError,
    SQLParseError,
)
from .http import HttpClient

__all__ = [
    "AuthManager",
    "TokenPair",
    "DataverseConfig",
    "DataverseError",
    "HttpError",
    "ValidationError",
    "MetadataError",
    "SQLParseError",
    "HttpClient",
]