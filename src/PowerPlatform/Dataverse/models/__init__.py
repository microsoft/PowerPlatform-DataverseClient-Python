# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Data models and type definitions for the Dataverse SDK.

Provides dataclasses and helpers for Dataverse entities:

- :class:`~PowerPlatform.Dataverse.models.query_builder.QueryBuilder`: Fluent query builder.
- :mod:`~PowerPlatform.Dataverse.models.filters`: Composable OData filter expressions
  via :func:`~PowerPlatform.Dataverse.models.filters.col` and
  :func:`~PowerPlatform.Dataverse.models.filters.raw`.
- :class:`~PowerPlatform.Dataverse.models.record.QueryResult`: Iterable result wrapper.
- :class:`~PowerPlatform.Dataverse.models.record.Record`: Dataverse entity record.
- :class:`~PowerPlatform.Dataverse.models.upsert.UpsertItem`: Upsert operation item.
- :class:`~PowerPlatform.Dataverse.models.fetchxml_query.FetchXmlQuery`: FetchXML query object.
- :class:`~PowerPlatform.Dataverse.models.protocol.DataverseModel`: Typed-model protocol.
"""

from .batch import BatchItemResponse, BatchResult
from .fetchxml_query import FetchXmlQuery
from .filters import ColumnProxy, FilterExpression, col, raw
from .labels import Label, LocalizedLabel
from .protocol import DataverseModel
from .query_builder import ExpandOption, QueryBuilder, QueryParams
from .record import QueryResult, Record
from .relationship import (
    CascadeConfiguration,
    LookupAttributeMetadata,
    ManyToManyRelationshipMetadata,
    OneToManyRelationshipMetadata,
    RelationshipInfo,
)
from .table_info import AlternateKeyInfo, ColumnInfo, TableInfo
from .upsert import UpsertItem

__all__ = [
    "BatchItemResponse",
    "BatchResult",
    "FetchXmlQuery",
    "ColumnProxy",
    "FilterExpression",
    "col",
    "raw",
    "Label",
    "LocalizedLabel",
    "DataverseModel",
    "ExpandOption",
    "QueryBuilder",
    "QueryParams",
    "QueryResult",
    "Record",
    "CascadeConfiguration",
    "LookupAttributeMetadata",
    "ManyToManyRelationshipMetadata",
    "OneToManyRelationshipMetadata",
    "RelationshipInfo",
    "AlternateKeyInfo",
    "ColumnInfo",
    "TableInfo",
    "UpsertItem",
]
