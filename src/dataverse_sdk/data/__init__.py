# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Data access layer for the Dataverse SDK.

This module contains OData protocol handling, CRUD operations, metadata management,
SQL query functionality, and file upload capabilities.
"""

from .odata import ODataClient
from .upload import ODataFileUpload

__all__ = ["ODataClient", "ODataFileUpload"]