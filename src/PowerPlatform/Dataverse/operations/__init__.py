# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Operation namespaces for the Dataverse SDK.

This module contains the operation namespace classes that organize
SDK operations into logical groups: records, query, and tables.
"""

from .dataframe import DataFrameOperations

__all__ = ["DataFrameOperations"]
