# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Typed field descriptor classes for strongly-typed Dataverse entities.

Each class corresponds to a Dataverse attribute type and implements the Python
descriptor protocol so that class-level access returns the descriptor itself
(enabling filter expressions) while instance-level access returns the stored
field value from the entity's ``_data`` dict.

All primitive descriptors extend both :class:`_FieldBase` and a native Python
immutable type (``str``, ``int``, ``float``, ``Decimal``, ``datetime``).  This
lets IDEs infer the value type and lets the descriptors participate naturally in
comparisons without importing external type stubs.

Comparison operators (``==``, ``!=``, ``<``, ``<=``, ``>``, ``>=``) on a
descriptor instance produce a
:class:`~PowerPlatform.Dataverse.models.filters.FilterExpression` object
suitable for :meth:`~PowerPlatform.Dataverse.models.query_builder.QueryBuilder.where`.

Example::

    from PowerPlatform.Dataverse.models.entity import Entity
    from PowerPlatform.Dataverse.models.datatypes import Text, Integer, Guid

    class Account(Entity, table="account", primary_key="accountid"):
        accountid = Guid(writable_on_create=False, writable_on_update=False)
        name      = Text(nullable=False, max_length=160)
        employees = Integer(min_value=0)

    # Class-level access → descriptor (filter DSL)
    expr = Account.name == "Contoso"   # FilterExpression: name eq 'Contoso'

    # Instance-level access → value
    a = Account(name="Contoso")
    print(a.name)   # "Contoso"
"""

from __future__ import annotations

import uuid as _uuid_mod
from datetime import datetime
from decimal import Decimal
from typing import Any, Generic, Optional, TypeVar

__all__ = [
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
]

_V = TypeVar("_V")


# ---------------------------------------------------------------------------
# Base mixin — descriptor protocol + filter-expression operators
# ---------------------------------------------------------------------------


class _FieldBase(Generic[_V]):
    """Mixin providing the descriptor protocol and OData filter operators.

    All concrete field descriptor classes inherit from this mixin.  It stores
    the field's logical name (populated by :meth:`__set_name__` at class
    definition time) and routes instance attribute access through the entity's
    ``_data`` dictionary.

    Comparison operators produce
    :class:`~PowerPlatform.Dataverse.models.filters.FilterExpression` objects
    so that ``Account.name == "x"`` can be passed directly to
    :meth:`~PowerPlatform.Dataverse.models.query_builder.QueryBuilder.where`.
    """

    _logical_name: str = ""
    _is_field_descriptor: bool = True

    # -------------------------------------------------------------- descriptor

    def __set_name__(self, owner: type, name: str) -> None:
        if not self._logical_name:
            self._logical_name = name

    def __get__(self, obj: Any, objtype: Any = None) -> Any:
        if obj is None:
            return self
        return obj._data.get(self._logical_name)

    def __set__(self, obj: Any, value: Any) -> None:
        obj._data[self._logical_name] = value

    # ------------------------------------------------------ filter operators

    def __eq__(self, other: Any) -> Any:  # type: ignore[override]
        from .filters import _ComparisonFilter
        return _ComparisonFilter(self._logical_name, "eq", other)

    def __ne__(self, other: Any) -> Any:  # type: ignore[override]
        from .filters import _ComparisonFilter
        return _ComparisonFilter(self._logical_name, "ne", other)

    def __gt__(self, other: Any) -> Any:
        from .filters import _ComparisonFilter
        return _ComparisonFilter(self._logical_name, "gt", other)

    def __ge__(self, other: Any) -> Any:
        from .filters import _ComparisonFilter
        return _ComparisonFilter(self._logical_name, "ge", other)

    def __lt__(self, other: Any) -> Any:
        from .filters import _ComparisonFilter
        return _ComparisonFilter(self._logical_name, "lt", other)

    def __le__(self, other: Any) -> Any:
        from .filters import _ComparisonFilter
        return _ComparisonFilter(self._logical_name, "le", other)

    def __hash__(self) -> int:
        return id(self)

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}(logical_name={self._logical_name!r}, "
            f"nullable={getattr(self, 'nullable', True)!r})"
        )


# ---------------------------------------------------------------------------
# String types
# ---------------------------------------------------------------------------


class Text(_FieldBase[str], str):
    """Single-line text field (Dataverse ``String`` type).

    :param nullable: Whether the field may be ``None`` / absent.
    :param default: Optional default value.
    :param max_length: Maximum string length defined in Dataverse metadata.
    :param logical_name: Override the logical name (defaults to the Python
        attribute name via ``__set_name__``).
    :param label: Human-readable label from Dataverse metadata.
    :param writable_on_create: Whether the field is writable in create payloads.
    :param writable_on_update: Whether the field is writable in update payloads.
    """

    def __new__(
        cls,
        *,
        nullable: bool = True,
        default: Optional[str] = None,
        max_length: Optional[int] = None,
        logical_name: str = "",
        label: str = "",
        writable_on_create: bool = True,
        writable_on_update: bool = True,
    ) -> "Text":
        instance = str.__new__(cls, "")
        instance.nullable = nullable
        instance.default = default
        instance.max_length = max_length
        instance._logical_name = logical_name
        instance.label = label
        instance.writable_on_create = writable_on_create
        instance.writable_on_update = writable_on_update
        return instance


class Memo(_FieldBase[str], str):
    """Multi-line text field (Dataverse ``Memo`` type).

    :param nullable: Whether the field may be ``None`` / absent.
    :param default: Optional default value.
    :param max_length: Maximum string length.
    :param logical_name: Override the logical name.
    :param label: Human-readable label.
    :param writable_on_create: Writable on create.
    :param writable_on_update: Writable on update.
    """

    def __new__(
        cls,
        *,
        nullable: bool = True,
        default: Optional[str] = None,
        max_length: Optional[int] = None,
        logical_name: str = "",
        label: str = "",
        writable_on_create: bool = True,
        writable_on_update: bool = True,
    ) -> "Memo":
        instance = str.__new__(cls, "")
        instance.nullable = nullable
        instance.default = default
        instance.max_length = max_length
        instance._logical_name = logical_name
        instance.label = label
        instance.writable_on_create = writable_on_create
        instance.writable_on_update = writable_on_update
        return instance


# ---------------------------------------------------------------------------
# Integer types
# ---------------------------------------------------------------------------


class Integer(_FieldBase[int], int):
    """Whole-number field (Dataverse ``Integer`` type).

    :param nullable: Whether the field may be ``None`` / absent.
    :param default: Optional default value.
    :param min_value: Minimum allowed value from metadata.
    :param max_value: Maximum allowed value from metadata.
    :param logical_name: Override the logical name.
    :param label: Human-readable label.
    :param writable_on_create: Writable on create.
    :param writable_on_update: Writable on update.
    """

    def __new__(
        cls,
        *,
        nullable: bool = True,
        default: Optional[int] = None,
        min_value: Optional[int] = None,
        max_value: Optional[int] = None,
        logical_name: str = "",
        label: str = "",
        writable_on_create: bool = True,
        writable_on_update: bool = True,
    ) -> "Integer":
        instance = int.__new__(cls, 0)
        instance.nullable = nullable
        instance.default = default
        instance.min_value = min_value
        instance.max_value = max_value
        instance._logical_name = logical_name
        instance.label = label
        instance.writable_on_create = writable_on_create
        instance.writable_on_update = writable_on_update
        return instance


class BigInt(_FieldBase[int], int):
    """Large integer field (Dataverse ``BigInt`` type).

    :param nullable: Whether the field may be ``None`` / absent.
    :param default: Optional default value.
    :param logical_name: Override the logical name.
    :param label: Human-readable label.
    :param writable_on_create: Writable on create.
    :param writable_on_update: Writable on update.
    """

    def __new__(
        cls,
        *,
        nullable: bool = True,
        default: Optional[int] = None,
        logical_name: str = "",
        label: str = "",
        writable_on_create: bool = True,
        writable_on_update: bool = True,
    ) -> "BigInt":
        instance = int.__new__(cls, 0)
        instance.nullable = nullable
        instance.default = default
        instance._logical_name = logical_name
        instance.label = label
        instance.writable_on_create = writable_on_create
        instance.writable_on_update = writable_on_update
        return instance


# ---------------------------------------------------------------------------
# Decimal / float types
# ---------------------------------------------------------------------------


class DecimalNumber(_FieldBase[Decimal], Decimal):
    """Fixed-precision decimal field (Dataverse ``Decimal`` type).

    :param nullable: Whether the field may be ``None`` / absent.
    :param default: Optional default value.
    :param min_value: Minimum allowed value.
    :param max_value: Maximum allowed value.
    :param precision: Decimal precision (digits after the decimal point).
    :param logical_name: Override the logical name.
    :param label: Human-readable label.
    :param writable_on_create: Writable on create.
    :param writable_on_update: Writable on update.
    """

    def __new__(
        cls,
        *,
        nullable: bool = True,
        default: Optional[Decimal] = None,
        min_value: Optional[Decimal] = None,
        max_value: Optional[Decimal] = None,
        precision: Optional[int] = None,
        logical_name: str = "",
        label: str = "",
        writable_on_create: bool = True,
        writable_on_update: bool = True,
    ) -> "DecimalNumber":
        instance = Decimal.__new__(cls, "0")
        instance.nullable = nullable
        instance.default = default
        instance.min_value = min_value
        instance.max_value = max_value
        instance.precision = precision
        instance._logical_name = logical_name
        instance.label = label
        instance.writable_on_create = writable_on_create
        instance.writable_on_update = writable_on_update
        return instance


class Double(_FieldBase[float], float):
    """Double-precision floating-point field (Dataverse ``Double`` type).

    :param nullable: Whether the field may be ``None`` / absent.
    :param default: Optional default value.
    :param min_value: Minimum allowed value.
    :param max_value: Maximum allowed value.
    :param precision: Display precision.
    :param logical_name: Override the logical name.
    :param label: Human-readable label.
    :param writable_on_create: Writable on create.
    :param writable_on_update: Writable on update.
    """

    def __new__(
        cls,
        *,
        nullable: bool = True,
        default: Optional[float] = None,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        precision: Optional[int] = None,
        logical_name: str = "",
        label: str = "",
        writable_on_create: bool = True,
        writable_on_update: bool = True,
    ) -> "Double":
        instance = float.__new__(cls, 0.0)
        instance.nullable = nullable
        instance.default = default
        instance.min_value = min_value
        instance.max_value = max_value
        instance.precision = precision
        instance._logical_name = logical_name
        instance.label = label
        instance.writable_on_create = writable_on_create
        instance.writable_on_update = writable_on_update
        return instance


class Money(_FieldBase[Decimal], Decimal):
    """Currency field (Dataverse ``Money`` type).

    Stored as ``Decimal`` for precision.  Behaves identically to
    :class:`DecimalNumber` but carries a distinct type name so generators and
    tools can signal currency semantics.

    :param nullable: Whether the field may be ``None`` / absent.
    :param default: Optional default value.
    :param min_value: Minimum allowed value.
    :param max_value: Maximum allowed value.
    :param precision: Currency precision.
    :param logical_name: Override the logical name.
    :param label: Human-readable label.
    :param writable_on_create: Writable on create.
    :param writable_on_update: Writable on update.
    """

    def __new__(
        cls,
        *,
        nullable: bool = True,
        default: Optional[Decimal] = None,
        min_value: Optional[Decimal] = None,
        max_value: Optional[Decimal] = None,
        precision: Optional[int] = None,
        logical_name: str = "",
        label: str = "",
        writable_on_create: bool = True,
        writable_on_update: bool = True,
    ) -> "Money":
        instance = Decimal.__new__(cls, "0")
        instance.nullable = nullable
        instance.default = default
        instance.min_value = min_value
        instance.max_value = max_value
        instance.precision = precision
        instance._logical_name = logical_name
        instance.label = label
        instance.writable_on_create = writable_on_create
        instance.writable_on_update = writable_on_update
        return instance


# ---------------------------------------------------------------------------
# Date / time type
# ---------------------------------------------------------------------------


class DateTime(_FieldBase[datetime], datetime):
    """Date-and-time field (Dataverse ``DateTime`` type).

    The descriptor instance itself is a ``datetime`` set to ``datetime.min``
    (a sentinel value used only when the descriptor is accessed at class level).
    Instance-level access returns the actual ``datetime`` stored in
    ``entity._data``.

    :param nullable: Whether the field may be ``None`` / absent.
    :param default: Optional default value.
    :param date_format: Dataverse ``Format`` metadata value (e.g. ``"DateOnly"``).
    :param datetime_behavior: Dataverse ``DateTimeBehavior`` (e.g.
        ``"UserLocal"``, ``"TimeZoneIndependent"``, ``"DateOnly"``).
    :param logical_name: Override the logical name.
    :param label: Human-readable label.
    :param writable_on_create: Writable on create.
    :param writable_on_update: Writable on update.
    """

    def __new__(
        cls,
        *,
        nullable: bool = True,
        default: Optional[datetime] = None,
        date_format: Optional[str] = None,
        datetime_behavior: Optional[str] = None,
        logical_name: str = "",
        label: str = "",
        writable_on_create: bool = True,
        writable_on_update: bool = True,
    ) -> "DateTime":
        _min = datetime.min
        instance = datetime.__new__(cls, _min.year, _min.month, _min.day)
        instance.nullable = nullable
        instance.default = default
        instance.date_format = date_format
        instance.datetime_behavior = datetime_behavior
        instance._logical_name = logical_name
        instance.label = label
        instance.writable_on_create = writable_on_create
        instance.writable_on_update = writable_on_update
        return instance


# ---------------------------------------------------------------------------
# GUID type
# ---------------------------------------------------------------------------


class Guid(_FieldBase[str], str):
    """Unique-identifier field (Dataverse ``Uniqueidentifier`` type).

    :param nullable: Whether the field may be ``None`` / absent.
    :param logical_name: Override the logical name.
    :param label: Human-readable label.
    :param writable_on_create: Writable on create (typically ``False`` for
        primary keys).
    :param writable_on_update: Writable on update (typically ``False`` for
        primary keys).

    Convenience static methods::

        Guid.new()    # → new random UUID string
        Guid.empty()  # → "00000000-0000-0000-0000-000000000000"
    """

    def __new__(
        cls,
        *,
        nullable: bool = True,
        logical_name: str = "",
        label: str = "",
        writable_on_create: bool = True,
        writable_on_update: bool = True,
    ) -> "Guid":
        instance = str.__new__(cls, "")
        instance.nullable = nullable
        instance._logical_name = logical_name
        instance.label = label
        instance.writable_on_create = writable_on_create
        instance.writable_on_update = writable_on_update
        return instance

    @staticmethod
    def new() -> str:
        """Return a new random UUID string."""
        return str(_uuid_mod.uuid4())

    @staticmethod
    def empty() -> str:
        """Return the all-zeros UUID string."""
        return "00000000-0000-0000-0000-000000000000"
