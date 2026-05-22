# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Data models and type definitions for the Dataverse SDK.

Provides dataclasses and helpers for Dataverse entities:

- :class:`~PowerPlatform.Dataverse.models.query_builder.QueryBuilder`: Fluent query builder.
- :mod:`~PowerPlatform.Dataverse.models.filters`: Composable OData filter expressions.
- :class:`~PowerPlatform.Dataverse.models.record.QueryResult`: Iterable result wrapper.
- :class:`~PowerPlatform.Dataverse.models.upsert.UpsertItem`: Upsert operation item.

Import directly from the specific module, e.g.::

    from PowerPlatform.Dataverse.models.query_builder import QueryBuilder
    from PowerPlatform.Dataverse.models.filters import col, raw
    from PowerPlatform.Dataverse.models.record import QueryResult
"""

from .filters import col, raw
from .protocol import DataverseModel
from .record import QueryResult

__all__ = ["col", "raw", "DataverseModel", "QueryResult"]
