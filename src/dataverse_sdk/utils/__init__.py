# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Utilities and adapters for the Dataverse SDK.

This module contains helper functions, adapters (like Pandas integration),
logging utilities, and validation helpers.
"""

from .pandas_adapter import PandasODataClient

__all__ = ["PandasODataClient"]