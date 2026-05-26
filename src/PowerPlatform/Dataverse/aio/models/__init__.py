# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Async data models and type definitions for the Dataverse SDK.

Provides async-specific models for Dataverse entities:

- :class:`~PowerPlatform.Dataverse.aio.models.async_query_builder.AsyncQueryBuilder`: Async fluent query builder.
- :class:`~PowerPlatform.Dataverse.aio.models.async_fetchxml_query.AsyncFetchXmlQuery`: Async FetchXML query.
"""

from .async_fetchxml_query import AsyncFetchXmlQuery
from .async_query_builder import AsyncQueryBuilder

__all__ = [
    "AsyncFetchXmlQuery",
    "AsyncQueryBuilder",
]
