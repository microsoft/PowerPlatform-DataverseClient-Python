# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Operation namespaces for the Dataverse SDK.

This module contains the operation namespace classes that organize
SDK operations into logical groups: records, query, and tables.
"""

from .batch import (
    BatchDataFrameOperations,
    BatchOperations,
    BatchQueryOperations,
    BatchRecordOperations,
    BatchRequest,
    BatchTableOperations,
    ChangeSet,
    ChangeSetRecordOperations,
)
from .dataframe import DataFrameOperations
from .files import FileOperations
from .query import QueryOperations
from .records import RecordOperations
from .tables import TableOperations

__all__ = [
    # batch
    "BatchDataFrameOperations",
    "BatchOperations",
    "BatchQueryOperations",
    "BatchRecordOperations",
    "BatchRequest",
    "BatchTableOperations",
    "ChangeSet",
    "ChangeSetRecordOperations",
    # other operations
    "DataFrameOperations",
    "FileOperations",
    "QueryOperations",
    "RecordOperations",
    "TableOperations",
]
