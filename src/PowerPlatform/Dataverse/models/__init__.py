# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Data models and type definitions for the Dataverse SDK.

Provides dataclasses and helpers for Dataverse entities:

- :class:`~PowerPlatform.Dataverse.models.query_builder.QueryBuilder`: Fluent query builder.
- :mod:`~PowerPlatform.Dataverse.models.filters`: Composable OData filter expressions.
- :class:`~PowerPlatform.Dataverse.models.upsert.UpsertItem`: Upsert operation item.
"""

from .batch import BatchItemResponse, BatchResult
from .filters import (
    FilterExpression,
    between,
    contains,
    endswith,
    eq,
    filter_in,
    ge,
    gt,
    is_not_null,
    is_null,
    le,
    lt,
    ne,
    not_between,
    not_in,
    raw,
    startswith,
)
from .labels import Label, LocalizedLabel
from .query_builder import ExpandOption, QueryBuilder, QueryParams
from .record import Record
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
    # batch
    "BatchItemResponse",
    "BatchResult",
    # filters
    "FilterExpression",
    "between",
    "contains",
    "endswith",
    "eq",
    "filter_in",
    "ge",
    "gt",
    "is_not_null",
    "is_null",
    "le",
    "lt",
    "ne",
    "not_between",
    "not_in",
    "raw",
    "startswith",
    # labels
    "Label",
    "LocalizedLabel",
    # query builder
    "ExpandOption",
    "QueryBuilder",
    "QueryParams",
    # record
    "Record",
    # relationship
    "CascadeConfiguration",
    "LookupAttributeMetadata",
    "ManyToManyRelationshipMetadata",
    "OneToManyRelationshipMetadata",
    "RelationshipInfo",
    # table info
    "AlternateKeyInfo",
    "ColumnInfo",
    "TableInfo",
    # upsert
    "UpsertItem",
]
