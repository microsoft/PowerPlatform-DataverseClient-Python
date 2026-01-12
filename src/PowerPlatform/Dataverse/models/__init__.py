# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Data models and type definitions for the Dataverse SDK.
"""

from .metadata import (
    LocalizedLabel,
    Label,
    CascadeConfiguration,
    AssociatedMenuConfiguration,
    LookupAttributeMetadata,
    OneToManyRelationshipMetadata,
    ManyToManyRelationshipMetadata,
)

__all__ = [
    "LocalizedLabel",
    "Label",
    "CascadeConfiguration",
    "AssociatedMenuConfiguration",
    "LookupAttributeMetadata",
    "OneToManyRelationshipMetadata",
    "ManyToManyRelationshipMetadata",
]
