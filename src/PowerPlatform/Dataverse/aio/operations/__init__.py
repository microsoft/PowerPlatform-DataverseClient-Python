# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Async operation namespaces for the Dataverse SDK.

This module contains the async operation namespace classes that organize
SDK operations into logical groups: records, query, tables, files, and batch.
"""

from .async_batch import AsyncBatchOperations, AsyncBatchRequest, AsyncChangeSet
from .async_dataframe import AsyncDataFrameOperations
from .async_files import AsyncFileOperations
from .async_query import AsyncQueryOperations
from .async_records import AsyncRecordOperations
from .async_tables import AsyncTableOperations

__all__ = [
    # batch
    "AsyncBatchOperations",
    "AsyncBatchRequest",
    "AsyncChangeSet",
    # other operations
    "AsyncDataFrameOperations",
    "AsyncFileOperations",
    "AsyncQueryOperations",
    "AsyncRecordOperations",
    "AsyncTableOperations",
]
