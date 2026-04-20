# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Lookup field descriptor classes for strongly-typed Dataverse entities.

Lookup fields reference records in other tables.  Unlike primitive types,
lookups do not extend a native Python type; the stored value is the referenced
record's GUID string (or OData bind expression for creates/updates).

Example::

    from PowerPlatform.Dataverse.models.entity import Entity
    from PowerPlatform.Dataverse.models.lookup import Lookup

    class Contact(Entity, table="contact", primary_key="contactid"):
        accountid = Lookup(target="account", nullable=False)

    # Filter DSL — class-level access returns the descriptor
    expr = Contact.accountid == "some-guid"

    # Instance access returns the stored value
    c = Contact(accountid="some-guid")
    print(c.accountid)  # "some-guid"
"""

from __future__ import annotations

from typing import Any, Optional, Tuple

from .datatypes import _FieldBase

__all__ = ["Lookup", "CustomerLookup"]


class Lookup(_FieldBase[str]):
    """Single-target lookup field (Dataverse ``Lookup`` / ``Owner`` types).

    The value is the referenced record's GUID string.  For create/update
    payloads the SDK serializes this as an OData bind expression
    (``"fieldname@odata.bind": "/entitysets(guid)"``); for read responses the
    raw GUID is stored.

    :param target: Logical name of the referenced table (e.g. ``"account"``).
    :param nullable: Whether the field may be ``None`` / absent.
    :param schema_name: SchemaName of the lookup attribute (used in metadata).
    :param referenced_attribute: Primary key attribute of the target table.
    :param logical_name: Override the Python attribute name as logical name.
    :param label: Human-readable label.
    :param writable_on_create: Writable on create.
    :param writable_on_update: Writable on update.
    """

    def __init__(
        self,
        *,
        target: str = "",
        nullable: bool = True,
        schema_name: str = "",
        referenced_attribute: str = "",
        logical_name: str = "",
        label: str = "",
        writable_on_create: bool = True,
        writable_on_update: bool = True,
    ) -> None:
        self.target = target
        self.nullable = nullable
        self.schema_name = schema_name
        self.referenced_attribute = referenced_attribute
        self._logical_name = logical_name
        self.label = label
        self.writable_on_create = writable_on_create
        self.writable_on_update = writable_on_update

    def __repr__(self) -> str:
        return (
            f"Lookup(target={self.target!r}, "
            f"logical_name={self._logical_name!r}, "
            f"nullable={self.nullable!r})"
        )


class CustomerLookup(_FieldBase[str]):
    """Polymorphic customer lookup field (Dataverse ``Customer`` type).

    References either an ``account`` or a ``contact`` record (or other
    configured targets).  The stored value is the referenced record's GUID.

    :param targets: Tuple of target logical names (e.g. ``("account", "contact")``).
    :param nullable: Whether the field may be ``None`` / absent.
    :param schema_name: SchemaName of the attribute.
    :param logical_name: Override the Python attribute name as logical name.
    :param label: Human-readable label.
    :param writable_on_create: Writable on create.
    :param writable_on_update: Writable on update.
    """

    def __init__(
        self,
        *,
        targets: Optional[Tuple[str, ...]] = None,
        nullable: bool = True,
        schema_name: str = "",
        logical_name: str = "",
        label: str = "",
        writable_on_create: bool = True,
        writable_on_update: bool = True,
    ) -> None:
        self.targets: Tuple[str, ...] = tuple(targets) if targets else ()
        self.nullable = nullable
        self.schema_name = schema_name
        self._logical_name = logical_name
        self.label = label
        self.writable_on_create = writable_on_create
        self.writable_on_update = writable_on_update

    def __repr__(self) -> str:
        return (
            f"CustomerLookup(targets={self.targets!r}, "
            f"logical_name={self._logical_name!r}, "
            f"nullable={self.nullable!r})"
        )
