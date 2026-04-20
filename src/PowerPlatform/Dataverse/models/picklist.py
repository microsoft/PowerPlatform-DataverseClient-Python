# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Picklist (choice column) field descriptor classes for strongly-typed Dataverse entities.

Dataverse choice columns (``Picklist``, ``State``, ``Status``,
``Multiselectpicklist``) map to integer option codes.  This module provides:

- :class:`PicklistOption` — an ``int`` subclass that carries a ``label`` string,
  used as class-level constants on generated picklist classes.
- :class:`PicklistBase` — the base descriptor class for all single-choice
  columns.  Subclasses define their named options as class-level
  :class:`PicklistOption` instances.
- :class:`MultiPicklist` — descriptor for multi-select choice columns.
- Type aliases ``Picklist``, ``State``, ``Status`` → ``PicklistBase``.

Example (code-first)::

    from PowerPlatform.Dataverse.models.picklist import PicklistBase, PicklistOption

    class AccountIndustryCode(PicklistBase):
        Technology  = PicklistOption(7,  "Technology")
        Consulting  = PicklistOption(8,  "Consulting")
        Finance     = PicklistOption(6,  "Finance")

    class Account(Entity, table="account", primary_key="accountid"):
        industrycode = AccountIndustryCode()

    # Option introspection
    AccountIndustryCode.from_value(7).label   # "Technology"
    AccountIndustryCode.from_label("Finance") # PicklistOption(6, "Finance")

    # Filter DSL
    expr = Account.industrycode == AccountIndustryCode.Technology

    # Instance access
    a = Account(industrycode=7)
    print(a.industrycode)  # 7
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from .datatypes import _FieldBase

__all__ = [
    "PicklistOption",
    "PicklistBase",
    "MultiPicklist",
    "Picklist",
    "State",
    "Status",
]


# ---------------------------------------------------------------------------
# Named option value
# ---------------------------------------------------------------------------


class PicklistOption(int):
    """An integer option code with an associated display label.

    Behaves as a plain ``int`` in comparisons, arithmetic, and serialization;
    the ``label`` attribute carries the human-readable display string.

    :param value: The integer option code stored in Dataverse.
    :param label: Human-readable display label (optional).
    """

    label: str

    def __new__(cls, value: int, label: str = "") -> "PicklistOption":
        instance = int.__new__(cls, value)
        instance.label = label
        return instance

    def __str__(self) -> str:
        return self.label if self.label else super().__str__()

    def __repr__(self) -> str:
        return f"PicklistOption({int(self)}, {self.label!r})"


# ---------------------------------------------------------------------------
# Picklist descriptor base
# ---------------------------------------------------------------------------


class PicklistBase(_FieldBase[int], int):
    """Base descriptor for single-choice (picklist / state / status) columns.

    Subclass this to create typed option sets and annotate entity fields::

        class IndustryCode(PicklistBase):
            Technology = PicklistOption(7, "Technology")
            Consulting = PicklistOption(8, "Consulting")

        class Account(Entity, table="account"):
            industrycode = IndustryCode()

    Class attributes:

    - ``_label`` — display name of the option set (set by generator).
    - ``_is_global`` — ``True`` when this is a global option set.
    - ``_global_option_set`` — global option set name (if applicable).
    """

    _label: str = ""
    _is_global: bool = False
    _global_option_set: str = ""

    def __new__(
        cls,
        *,
        nullable: bool = True,
        default: Optional[int] = None,
        logical_name: str = "",
        label: str = "",
        writable_on_create: bool = True,
        writable_on_update: bool = True,
    ) -> "PicklistBase":
        instance = int.__new__(cls, 0)
        instance.nullable = nullable
        instance.default = default
        instance._logical_name = logical_name
        instance.label = label
        instance.writable_on_create = writable_on_create
        instance.writable_on_update = writable_on_update
        return instance

    # ------------------------------------------------------ option introspection

    @classmethod
    def options(cls) -> Dict[str, PicklistOption]:
        """Return all named :class:`PicklistOption` members defined on this class."""
        return {
            name: obj
            for name, obj in cls.__dict__.items()
            if isinstance(obj, PicklistOption)
        }

    @classmethod
    def from_value(cls, code: int) -> Optional[PicklistOption]:
        """Find a :class:`PicklistOption` by its integer code.

        :param code: The integer option code.
        :return: The matching :class:`PicklistOption`, or ``None`` if not found.
        """
        for opt in cls.options().values():
            if int(opt) == code:
                return opt
        return None

    @classmethod
    def from_label(cls, label: str) -> Optional[PicklistOption]:
        """Find a :class:`PicklistOption` by its display label (case-insensitive).

        :param label: The label to look up.
        :return: The matching :class:`PicklistOption`, or ``None`` if not found.
        """
        lower = label.lower()
        for opt in cls.options().values():
            if opt.label.lower() == lower:
                return opt
        return None


# ---------------------------------------------------------------------------
# Multi-select picklist descriptor
# ---------------------------------------------------------------------------


class MultiPicklist(_FieldBase[list]):
    """Descriptor for multi-select choice columns (Dataverse ``Multiselectpicklist``).

    The stored value is a ``list[int]`` of selected option codes.

    :param nullable: Whether the field may be ``None`` / absent.
    :param logical_name: Override the Python attribute name.
    :param label: Human-readable label.
    :param writable_on_create: Writable on create.
    :param writable_on_update: Writable on update.
    """

    def __init__(
        self,
        *,
        nullable: bool = True,
        logical_name: str = "",
        label: str = "",
        writable_on_create: bool = True,
        writable_on_update: bool = True,
    ) -> None:
        self.nullable = nullable
        self._logical_name = logical_name
        self.label = label
        self.writable_on_create = writable_on_create
        self.writable_on_update = writable_on_update


# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

Picklist = PicklistBase
State = PicklistBase
Status = PicklistBase
