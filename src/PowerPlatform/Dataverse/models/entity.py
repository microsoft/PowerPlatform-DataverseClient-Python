# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Strongly typed entity base classes for the Dataverse SDK.

Provides :class:`Entity` and :class:`FieldDescriptor` so that generated
entity classes can be used in place of plain table-name strings throughout
the SDK.  All existing string-based usage is unaffected.

Example — a generated entity class::

    # Types/account.py  (auto-generated)
    from PowerPlatform.Dataverse.models.entity import Entity, FieldDescriptor

    class Account(Entity, table="account", primary_key="accountid"):
        accountid  = FieldDescriptor("accountid",  str)
        name       = FieldDescriptor("name",       str)
        telephone1 = FieldDescriptor("telephone1", str)
        revenue    = FieldDescriptor("revenue",    float)
        statecode  = FieldDescriptor("statecode",  int)

Usage with the existing QueryBuilder::

    # Typed WHERE conditions — no SDK changes required
    for record in (client.query.builder("account")
                   .select("name", "revenue")
                   .where(Account.statecode == 0)      # FieldDescriptor → FilterExpression
                   .where(Account.revenue > 1_000_000)
                   .execute()):
        account = Account.from_record(record)
        print(account.name)

    # Or pass the entity class to builder() directly
    for account in (client.query.builder(Account)
                    .select(Account.name, Account.revenue)
                    .where(Account.statecode == 0)
                    .execute()):
        print(account.name)   # typed attribute, not dict lookup
"""

from __future__ import annotations

from typing import Any, ClassVar, Collection, Optional, Union, TYPE_CHECKING

from .filters import (
    FilterExpression,
    eq, ne, gt, ge, lt, le,
    contains, startswith, endswith,
    is_null, is_not_null,
    filter_in, between,
)

if TYPE_CHECKING:
    from .record import Record

__all__ = ["Entity", "FieldDescriptor", "resolve_table"]


def resolve_table(table: "Union[str, type[Entity]]") -> str:
    """Resolve a *table* argument to a plain schema-name string.

    Accepts either:

    - A plain string (e.g. ``"account"`` or ``"new_MyTable"``) — returned
      unchanged.
    - An :class:`Entity` subclass (e.g. ``Account``) — returns
      ``Account.__table__``.

    Used internally by ``client.records.*`` and ``client.tables.*`` so that
    callers can write ``client.records.create(Account, ...)`` instead of
    ``client.records.create(Account.__table__, ...)``.

    :raises TypeError: If *table* is neither a string nor an Entity subclass.
    :raises ValueError: If the Entity subclass has no ``__table__`` set.
    """
    if isinstance(table, str):
        return table
    if isinstance(table, type) and issubclass(table, Entity):
        if not table.__table__:
            raise ValueError(
                f"{table.__name__} has no __table__ set. "
                "Pass table= when defining the entity class."
            )
        return table.__table__
    raise TypeError(
        f"table must be a str or an Entity subclass, got {type(table).__name__!r}"
    )


class FieldDescriptor:
    """A typed field descriptor for :class:`Entity` subclasses.

    Serves two roles depending on how it is accessed:

    - **Class access** (``Account.statecode``): returns the descriptor
      itself; Python comparison operators (``==``, ``!=``, ``>``, ``>=``,
      ``<``, ``<=``) produce :class:`~PowerPlatform.Dataverse.models.filters.FilterExpression`
      objects that can be passed directly to :meth:`~PowerPlatform.Dataverse.models.query_builder.QueryBuilder.where`.

    - **Instance access** (``account.statecode``): returns the stored
      field value (``int``, ``str``, ``float``, etc.).

    :param name: OData logical name of the field (e.g. ``"statecode"``).
    :type name: str
    :param python_type: Python type of the field value (e.g. ``int``, ``str``).
    :type python_type: type

    .. note::
        Because ``__eq__`` is overridden to return a
        :class:`~PowerPlatform.Dataverse.models.filters.FilterExpression`,
        descriptors are not directly comparable with ``==`` at the class
        level. :meth:`__hash__` is explicitly defined so descriptors
        remain usable in sets and as dict keys.

    Example::

        Account.statecode == 0          # FilterExpression: statecode eq 0
        Account.revenue > 1_000_000     # FilterExpression: revenue gt 1000000
        Account.name.contains("Corp")   # FilterExpression: contains(name, 'Corp')
        Account.statecode.in_([0, 1])   # FilterExpression: Microsoft.Dynamics.CRM.In(...)
    """

    def __init__(self, name: str, python_type: type = object) -> None:
        self.name = name
        self.python_type = python_type
        self._private = f"_fld_{name}"

    def __set_name__(self, owner: type, attr: str) -> None:
        self._private = f"_fld_{self.name}"

    # ---------------------------------------------------------------- descriptor protocol

    def __get__(self, obj: Any, objtype: Optional[type] = None) -> Any:
        if obj is None:
            # Class-level access: Account.statecode → return descriptor for DSL
            return self
        # Instance-level access: account.statecode → return stored value
        return getattr(obj, self._private, None)

    def __set__(self, obj: Any, value: Any) -> None:
        setattr(obj, self._private, value)

    # ---------------------------------------------------------------- comparison operators → FilterExpression

    def __eq__(self, value: Any) -> FilterExpression:  # type: ignore[override]
        return eq(self.name, value)

    def __ne__(self, value: Any) -> FilterExpression:  # type: ignore[override]
        return ne(self.name, value)

    def __gt__(self, value: Any) -> FilterExpression:
        return gt(self.name, value)

    def __ge__(self, value: Any) -> FilterExpression:
        return ge(self.name, value)

    def __lt__(self, value: Any) -> FilterExpression:
        return lt(self.name, value)

    def __le__(self, value: Any) -> FilterExpression:
        return le(self.name, value)

    def __hash__(self) -> int:
        # Required because __eq__ is overridden.
        return hash(self.name)

    # ---------------------------------------------------------------- convenience filter methods

    def contains(self, value: str) -> FilterExpression:
        """Filter: ``contains(field, value)``."""
        return contains(self.name, value)

    def startswith(self, value: str) -> FilterExpression:
        """Filter: ``startswith(field, value)``."""
        return startswith(self.name, value)

    def endswith(self, value: str) -> FilterExpression:
        """Filter: ``endswith(field, value)``."""
        return endswith(self.name, value)

    def is_null(self) -> FilterExpression:
        """Filter: ``field eq null``."""
        return is_null(self.name)

    def is_not_null(self) -> FilterExpression:
        """Filter: ``field ne null``."""
        return is_not_null(self.name)

    def in_(self, values: Collection[Any]) -> FilterExpression:
        """Filter: ``Microsoft.Dynamics.CRM.In(PropertyName='field', ...)``."""
        return filter_in(self.name, values)

    def between(self, low: Any, high: Any) -> FilterExpression:
        """Filter: ``(field ge low and field le high)``."""
        return between(self.name, low, high)

    # ---------------------------------------------------------------- misc

    def __repr__(self) -> str:
        return f"FieldDescriptor({self.name!r}, {self.python_type.__name__})"


class Entity:
    """Base class for strongly typed Dataverse entity classes.

    Subclass using ``table=`` and ``primary_key=`` keyword arguments:

    .. code-block:: python

        class Account(Entity, table="account", primary_key="accountid"):
            accountid  = FieldDescriptor("accountid",  str)
            name       = FieldDescriptor("name",       str)
            revenue    = FieldDescriptor("revenue",    float)
            statecode  = FieldDescriptor("statecode",  int)

    The class-level :class:`FieldDescriptor` attributes act as a filter DSL:

    .. code-block:: python

        Account.statecode == 0          # → FilterExpression
        Account.revenue > 1_000_000     # → FilterExpression

    Instance attributes return the actual field values after hydration via
    :meth:`from_record`.
    """

    __table__: ClassVar[str] = ""
    __primary_key__: ClassVar[str] = ""

    def __init_subclass__(
        cls,
        table: str = "",
        primary_key: str = "",
        **kwargs: Any,
    ) -> None:
        super().__init_subclass__(**kwargs)
        if table:
            cls.__table__ = table
        if primary_key:
            cls.__primary_key__ = primary_key

        # Auto-create FieldDescriptors from plain type annotations so that
        # subclasses can use annotation-only syntax instead of explicit
        # FieldDescriptor(...) assignments:
        #
        #   class Account(Entity, table="account", primary_key="accountid"):
        #       accountid: str     # auto-creates FieldDescriptor("accountid", str)
        #       name:      str
        #       revenue:   float
        #
        # Explicit FieldDescriptor(...) assignments still work and take precedence.
        import typing as _typing
        own_annotations = cls.__dict__.get("__annotations__", {})
        if own_annotations:
            try:
                resolved = _typing.get_type_hints(cls)
            except Exception:
                resolved = {}
            for field_name, raw_ann in own_annotations.items():
                # Skip ClassVar fields (e.g. __table__: ClassVar[str])
                ann = resolved.get(field_name, raw_ann)
                if getattr(ann, "__origin__", None) is _typing.ClassVar:
                    continue
                if isinstance(raw_ann, str) and raw_ann.startswith("ClassVar"):
                    continue
                # Skip if already a FieldDescriptor — explicit definition wins
                if isinstance(cls.__dict__.get(field_name), FieldDescriptor):
                    continue
                python_type = ann if isinstance(ann, type) else object
                setattr(cls, field_name, FieldDescriptor(field_name, python_type))

    # ---------------------------------------------------------------- instance construction

    def __init__(self, **kwargs: Any) -> None:
        """Construct a typed entity instance for use as a create/update payload.

        Field names must match the :class:`FieldDescriptor` attributes defined
        on the entity class. Raises :class:`ValueError` for unknown names,
        catching typos at authoring time rather than at runtime.

        The constructed instance can be passed directly to
        ``client.records.create()`` or ``client.records.update()`` in place
        of a plain ``dict``.

        Example::

            demo = WalkthroughDemo(
                new_title="Complete project documentation",
                new_quantity=5,
                new_completed=False,
            )
            record_id = client.records.create(demo)

            client.records.update(WalkthroughDemo, record_id,
                                  WalkthroughDemo(new_completed=True))
        """
        descriptors = type(self)._field_descriptors()
        unknown = set(kwargs) - set(descriptors)
        if unknown:
            raise ValueError(
                f"{type(self).__name__}: unknown field(s): {', '.join(sorted(unknown))}. "
                f"Valid fields: {', '.join(sorted(descriptors))}"
            )
        self._provided_fields: frozenset[str] = frozenset(kwargs)
        self.id: Optional[str] = None
        for field_name, value in kwargs.items():
            descriptors[field_name].__set__(self, value)

    def to_dict(self) -> dict[str, Any]:
        """Return the OData field dict for this entity instance.

        For instances created via :meth:`__init__`, returns only the fields
        that were explicitly provided — identical to what :meth:`row` would
        return for the same kwargs.

        For instances hydrated via :meth:`from_record`, returns all non-``None``
        fields (those that were present in the server response).

        Used internally by ``client.records.create()`` and
        ``client.records.update()`` when an entity instance is passed as the
        data/changes argument.
        """
        descriptors = type(self)._field_descriptors()
        provided: Optional[frozenset] = getattr(self, '_provided_fields', None)
        if provided is not None:
            return {
                descriptors[attr].name: descriptors[attr].__get__(self, type(self))
                for attr in provided
            }
        return {
            d.name: v
            for d in descriptors.values()
            if (v := d.__get__(self, type(self))) is not None
        }

    # ---------------------------------------------------------------- class helpers

    @classmethod
    def _field_descriptors(cls) -> dict[str, FieldDescriptor]:
        """Return a mapping of attribute name → :class:`FieldDescriptor` for this class.

        Traverses the MRO so that descriptors defined on base entity classes
        are included, with subclass descriptors taking precedence.
        """
        result: dict[str, FieldDescriptor] = {}
        for klass in reversed(cls.__mro__):
            for attr, val in vars(klass).items():
                if isinstance(val, FieldDescriptor):
                    result[attr] = val
        return result

    @classmethod
    def row(cls, **kwargs: Any) -> dict[str, Any]:
        """Build a validated OData field dict from keyword arguments.

        Keyword argument names must match :class:`FieldDescriptor` attributes
        defined on the entity class. Raises :class:`ValueError` for unknown
        field names, catching typos at authoring time rather than at runtime.

        :returns: A plain ``dict`` mapping OData field names to values,
            suitable for passing to ``client.records.create()``,
            ``client.records.update()``, etc.
        :raises ValueError: If any kwarg does not match a known field.

        Example::

            Account.row(name="Contoso Ltd", telephone1="555-0100")
            # → {"name": "Contoso Ltd", "telephone1": "555-0100"}
        """
        descriptors = cls._field_descriptors()
        unknown = set(kwargs) - set(descriptors)
        if unknown:
            raise ValueError(
                f"{cls.__name__}: unknown field(s): {', '.join(sorted(unknown))}. "
                f"Valid fields: {', '.join(sorted(descriptors))}"
            )
        return {descriptors[attr].name: value for attr, value in kwargs.items()}

    @classmethod
    def from_record(cls, record: "Record") -> "Entity":
        """Hydrate a typed entity instance from a :class:`~PowerPlatform.Dataverse.models.record.Record`.

        Each :class:`FieldDescriptor` attribute is populated from the
        record's data dict.  Fields that were not selected in the query
        are set to ``None``.

        :param record: A :class:`~PowerPlatform.Dataverse.models.record.Record`
            returned by the SDK.
        :returns: A typed instance of this entity class.

        Example::

            record = client.records.get("account", account_id)
            account = Account.from_record(record)
            print(account.name)       # str
            print(account.statecode)  # int
        """
        instance = cls.__new__(cls)
        instance.id = record.id  # type: ignore[attr-defined]
        for descriptor in cls._field_descriptors().values():
            descriptor.__set__(instance, record.data.get(descriptor.name))
        return instance

    # ---------------------------------------------------------------- misc

    def __repr__(self) -> str:
        cls = type(self)
        parts = []
        for attr, descriptor in cls._field_descriptors().items():
            value = descriptor.__get__(self, cls)
            if value is not None:
                parts.append(f"{attr}={value!r}")
        return f"{cls.__name__}({', '.join(parts)})"
