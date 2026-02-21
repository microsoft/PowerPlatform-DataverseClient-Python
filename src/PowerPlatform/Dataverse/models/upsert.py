# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Upsert data models for the Dataverse SDK."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

__all__ = ["UpsertItem"]


@dataclass
class UpsertItem:
    """Represents a single upsert operation targeting a record by its alternate key.

    Used with :meth:`~PowerPlatform.Dataverse.operations.records.RecordOperations.upsert`
    to upsert one or more records identified by alternate keys rather than primary GUIDs.

    :param alternate_key: Dictionary mapping alternate key attribute names to their values.
        String values are automatically quoted and escaped in the OData URL. Integer and
        other non-string values are included without quotes.
    :type alternate_key: dict[str, Any]
    :param record: Dictionary of attribute names to values for the record payload.
        Keys are automatically lowercased. Picklist labels are resolved to integer option
        values when a matching option set is found.
    :type record: dict[str, Any]

    Example::

        item = UpsertItem(
            alternate_key={"accountnumber": "ACC-001", "address1_postalcode": "98052"},
            record={"name": "Contoso Ltd", "telephone1": "555-0100"},
        )
    """

    alternate_key: Dict[str, Any]
    record: Dict[str, Any]
