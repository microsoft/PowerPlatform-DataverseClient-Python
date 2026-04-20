# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Data models and type definitions for the Dataverse SDK.

Typed entity model::

    from PowerPlatform.Dataverse.models import Entity, Text, Integer, Guid, Lookup
    from PowerPlatform.Dataverse.models import PicklistBase, PicklistOption, Boolean

Untyped record (existing API)::

    from PowerPlatform.Dataverse.models import Record
    from PowerPlatform.Dataverse.models.query_builder import QueryBuilder
    from PowerPlatform.Dataverse.models.filters import eq, gt
"""

from .entity import Entity, _EntityT
from .datatypes import (
    _FieldBase,
    Text,
    Memo,
    Integer,
    BigInt,
    DecimalNumber,
    Double,
    Money,
    DateTime,
    Guid,
)
from .lookup import Lookup, CustomerLookup
from .picklist import PicklistBase, PicklistOption, MultiPicklist, Picklist, State, Status
from .boolean import BooleanBase, BooleanOption, Boolean
from .record import Record
from .filters import FilterExpression

__all__ = [
    # Entity base
    "Entity",
    "_EntityT",
    # Primitive field types
    "_FieldBase",
    "Text",
    "Memo",
    "Integer",
    "BigInt",
    "DecimalNumber",
    "Double",
    "Money",
    "DateTime",
    "Guid",
    # Relationship types
    "Lookup",
    "CustomerLookup",
    # Choice types
    "PicklistBase",
    "PicklistOption",
    "MultiPicklist",
    "Picklist",
    "State",
    "Status",
    # Boolean types
    "BooleanBase",
    "BooleanOption",
    "Boolean",
    # Untyped record (existing API)
    "Record",
    # Filter expressions
    "FilterExpression",
]
