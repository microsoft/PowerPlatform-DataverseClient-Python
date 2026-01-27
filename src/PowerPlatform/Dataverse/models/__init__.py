# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Data models and type definitions for the Dataverse SDK.

This module provides strongly-typed dataclasses for Dataverse entities:

- :class:`~PowerPlatform.Dataverse.models.record.Record`: Record representation with dict-like access.
- :class:`~PowerPlatform.Dataverse.models.table_info.TableInfo`: Table metadata.
- :class:`~PowerPlatform.Dataverse.models.table_info.ColumnInfo`: Column metadata.
- :class:`~PowerPlatform.Dataverse.models.query_builder.QueryBuilder`: Fluent query builder.

Type aliases:

- ``RecordId``: Type alias for record GUIDs (str).
- ``TableSchema``: Type alias for table schema names (str).
- ``ColumnSchema``: Type alias for column schema names (str).

Note:
    Per project requirements, this ``__init__.py`` does NOT import/export models.
    Users should import directly from the specific module files to avoid
    duplicate entries in auto-generated documentation.
"""

# NOTE: Per project requirements, this __init__.py should NOT import/export models.
# Users should import directly from the specific module files.
# Auto-docstring generation creates duplicate entries if we export at both levels.

__all__ = []
