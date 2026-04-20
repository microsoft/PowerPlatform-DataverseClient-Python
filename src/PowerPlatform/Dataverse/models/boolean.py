# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Boolean (yes/no) field descriptor classes for strongly-typed Dataverse entities.

Dataverse two-option (boolean) columns have named true/false options with
display labels (e.g. "Yes" / "No", "Active" / "Inactive").  This module
provides:

- :class:`BooleanOption` — an ``int`` subclass (``1`` or ``0``) that carries a
  ``label`` string, representing one side of the two-option set.
- :class:`BooleanBase` — the base descriptor class.  Subclasses define exactly
  one :class:`BooleanOption` with bool value ``True`` and one with ``False``.
- Type alias ``Boolean`` → ``BooleanBase``.

Example (code-first)::

    from PowerPlatform.Dataverse.models.boolean import BooleanBase, BooleanOption

    class AccountCreditOnHold(BooleanBase):
        Yes = BooleanOption(True,  "Credit On Hold")
        No  = BooleanOption(False, "No Credit Hold")

    class Account(Entity, table="account"):
        creditonhold = AccountCreditOnHold()

    # Introspection
    AccountCreditOnHold.true_option().label   # "Credit On Hold"
    AccountCreditOnHold.from_value(False)     # BooleanOption(0, "No Credit Hold")

    # Filter DSL
    expr = Account.creditonhold == True

    # Instance access
    a = Account(creditonhold=True)
    print(a.creditonhold)  # True
"""

from __future__ import annotations

from typing import Optional

from .datatypes import _FieldBase

__all__ = ["BooleanOption", "BooleanBase", "Boolean"]


# ---------------------------------------------------------------------------
# Named boolean option
# ---------------------------------------------------------------------------


class BooleanOption(int):
    """One side of a Dataverse two-option set, carrying a display label.

    Stored as ``int`` (1 for True, 0 for False) so it serialises correctly
    to OData JSON.  ``bool(BooleanOption(True, "Yes"))`` returns ``True``.

    :param value: ``True`` or ``False``.
    :param label: Human-readable display label (optional).
    """

    label: str

    def __new__(cls, value: bool, label: str = "") -> "BooleanOption":
        instance = int.__new__(cls, int(value))
        instance.label = label
        return instance

    def __bool__(self) -> bool:
        return bool(int(self))

    def __str__(self) -> str:
        return self.label if self.label else str(bool(self))

    def __repr__(self) -> str:
        return f"BooleanOption({bool(self)}, {self.label!r})"


# ---------------------------------------------------------------------------
# Boolean descriptor base
# ---------------------------------------------------------------------------


class BooleanBase(_FieldBase[bool], int):
    """Base descriptor for two-option (boolean) columns.

    Subclass and define exactly one :class:`BooleanOption` with a ``True``
    value and one with ``False``::

        class ActiveStatus(BooleanBase):
            Active   = BooleanOption(True,  "Active")
            Inactive = BooleanOption(False, "Inactive")

        class Account(Entity, table="account"):
            isdisabled = ActiveStatus()

    Class attributes:

    - ``_label`` — display name of the option set.
    - ``_is_global`` — ``True`` when this is a global two-option set.
    - ``_global_option_set`` — global option set name (if applicable).

    :raises TypeError: If a subclass does not define exactly one ``True``
        option and one ``False`` option.
    """

    _label: str = ""
    _is_global: bool = False
    _global_option_set: str = ""

    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__(**kwargs)
        options = [v for v in cls.__dict__.values() if isinstance(v, BooleanOption)]
        if options:
            true_opts  = [o for o in options if bool(o)]
            false_opts = [o for o in options if not bool(o)]
            if len(true_opts) != 1 or len(false_opts) != 1:
                raise TypeError(
                    f"{cls.__name__} must define exactly one True BooleanOption "
                    f"and one False BooleanOption; "
                    f"got {len(true_opts)} true and {len(false_opts)} false options"
                )

    def __new__(
        cls,
        *,
        nullable: bool = True,
        default: Optional[bool] = None,
        logical_name: str = "",
        label: str = "",
        writable_on_create: bool = True,
        writable_on_update: bool = True,
    ) -> "BooleanBase":
        instance = int.__new__(cls, 0)
        instance.nullable = nullable
        instance.default = default
        instance._logical_name = logical_name
        instance.label = label
        instance.writable_on_create = writable_on_create
        instance.writable_on_update = writable_on_update
        return instance

    # ------------------------------------------------- option introspection

    @classmethod
    def true_option(cls) -> Optional[BooleanOption]:
        """Return the :class:`BooleanOption` whose value is ``True``."""
        for v in cls.__dict__.values():
            if isinstance(v, BooleanOption) and bool(v):
                return v
        return None

    @classmethod
    def false_option(cls) -> Optional[BooleanOption]:
        """Return the :class:`BooleanOption` whose value is ``False``."""
        for v in cls.__dict__.values():
            if isinstance(v, BooleanOption) and not bool(v):
                return v
        return None

    @classmethod
    def from_value(cls, value: bool) -> Optional[BooleanOption]:
        """Find a :class:`BooleanOption` by its boolean value.

        :param value: ``True`` or ``False``.
        :return: The matching option, or ``None`` if not found.
        """
        return cls.true_option() if value else cls.false_option()

    @classmethod
    def from_label(cls, label: str) -> Optional[BooleanOption]:
        """Find a :class:`BooleanOption` by its display label (case-insensitive).

        :param label: The label to look up.
        :return: The matching option, or ``None`` if not found.
        """
        lower = label.lower()
        for v in cls.__dict__.values():
            if isinstance(v, BooleanOption) and v.label.lower() == lower:
                return v
        return None


# ---------------------------------------------------------------------------
# Type alias
# ---------------------------------------------------------------------------

Boolean = BooleanBase
