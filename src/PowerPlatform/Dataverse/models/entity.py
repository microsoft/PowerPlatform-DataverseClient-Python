# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Strongly-typed entity base class for Dataverse tables.

:class:`Entity` is the base for all code-first or generated typed entity
classes.  It provides:

- A ``_data: dict[str, Any]`` backing store that all field descriptors read
  from and write to.
- ``__init_subclass__`` keyword arguments for declaring table metadata without
  boilerplate class attributes.
- ``as_dict()`` for serializing the full field set to a plain dict.
- ``to_create_payload()`` / ``to_update_payload()`` for automatically stripping
  fields that are not writable on create or update.
- ``from_dict()`` for hydrating a typed instance from a plain dict (e.g. an
  OData response).
- ``fields()`` for inspecting the field descriptors defined on a class.

Example (code-first, no generator)::

    from PowerPlatform.Dataverse.models.entity import Entity
    from PowerPlatform.Dataverse.models.datatypes import Guid, Text, Integer, Money
    from PowerPlatform.Dataverse.models.lookup import Lookup

    class Account(Entity, table="account", primary_key="accountid"):
        accountid  = Guid(writable_on_create=False, writable_on_update=False)
        name       = Text(nullable=False, max_length=160)
        employees  = Integer(min_value=0)
        revenue    = Money()
        primarycontactid = Lookup(target="contact")

    # Construction with keyword arguments (Pylance validates field names)
    account = Account(name="Contoso", employees=500)

    # Typed field access
    print(account.name)       # "Contoso"
    print(account.employees)  # 500
    print(account.revenue)    # None  (not set)

    # OData filter DSL on class-level descriptors
    from PowerPlatform.Dataverse.models.query_builder import QueryBuilder
    qb = (QueryBuilder(Account)
          .where((Account.name == "Contoso") & (Account.employees > 100)))

    # Payload helpers
    payload = account.to_create_payload()   # strips writable_on_create=False fields
    print(payload.as_dict())  # {"name": "Contoso", "employees": 500}
"""

from __future__ import annotations

from typing import Any, Dict, Optional, TypeVar

__all__ = ["Entity", "_EntityT"]

_EntityT = TypeVar("_EntityT", bound="Entity")


class Entity:
    """Base class for strongly-typed Dataverse entity classes.

    Subclass with keyword arguments to declare table metadata::

        class Account(Entity, table="account", primary_key="accountid",
                      entity_set="accounts"):
            ...

    All field values are stored in the instance's ``_data`` dictionary, keyed
    by the field's logical name.  Field descriptors (instances of
    :class:`~PowerPlatform.Dataverse.models.datatypes._FieldBase` and its
    subclasses) intercept attribute access and redirect to ``_data``.

    Class-level attributes:

    - ``_logical_name`` — Dataverse table logical name (e.g. ``"account"``).
    - ``_entity_set`` — OData entity set name (e.g. ``"accounts"``).
    - ``_primary_id`` — Logical name of the primary key attribute.
    - ``_primary_name`` — Logical name of the primary name attribute.
    - ``_label`` — Human-readable display label.
    """

    _logical_name: str = ""
    _entity_set: str = ""
    _primary_id: str = ""
    _primary_name: str = ""
    _label: str = ""

    # ------------------------------------------------------ subclass config

    def __init_subclass__(
        cls,
        *,
        table: str = "",
        primary_key: str = "",
        entity_set: str = "",
        primary_name: str = "",
        label: str = "",
        **kwargs: Any,
    ) -> None:
        super().__init_subclass__(**kwargs)
        if table:
            cls._logical_name = table
        if primary_key:
            cls._primary_id = primary_key
        if entity_set:
            cls._entity_set = entity_set
        if primary_name:
            cls._primary_name = primary_name
        if label:
            cls._label = label

    # ------------------------------------------------------ construction

    def __init__(self, **kwargs: Any) -> None:
        object.__setattr__(self, "_data", {})
        for name, value in kwargs.items():
            setattr(self, name, value)

    def __setattr__(self, name: str, value: Any) -> None:
        for cls in type(self).__mro__:
            descriptor = cls.__dict__.get(name)
            if descriptor is not None and hasattr(descriptor, "__set__"):
                descriptor.__set__(self, value)
                return
        object.__setattr__(self, name, value)

    # ------------------------------------------------------ serialization

    def as_dict(self) -> Dict[str, Any]:
        """Return a shallow copy of the entity's ``_data`` dictionary.

        Keys are the Dataverse logical names; values are the Python objects
        stored for each field.  Only fields that have been explicitly set are
        included.

        :return: ``{logical_name: value, ...}``
        :rtype: dict
        """
        return dict(self._data)

    def to_create_payload(self: _EntityT) -> _EntityT:
        """Return a new instance containing only fields writable on create.

        Fields whose descriptor has ``writable_on_create=False`` (e.g. primary
        keys, system-generated columns) are excluded.  Fields that have not been
        set on this instance (not present in ``_data``) are also excluded.

        :return: A new instance of the same type with the filtered ``_data``.
        :rtype: same type as ``self``
        """
        descriptors = type(self).fields()
        writable = {
            d._logical_name
            for d in descriptors.values()
            if getattr(d, "writable_on_create", True)
        }
        filtered = {k: v for k, v in self._data.items() if k in writable}
        return type(self).from_dict(filtered)

    def to_update_payload(self: _EntityT) -> _EntityT:
        """Return a new instance containing only fields writable on update.

        Fields whose descriptor has ``writable_on_update=False`` (e.g. primary
        keys, create-only columns) are excluded.  Fields that have not been
        set on this instance are also excluded.

        :return: A new instance of the same type with the filtered ``_data``.
        :rtype: same type as ``self``
        """
        descriptors = type(self).fields()
        writable = {
            d._logical_name
            for d in descriptors.values()
            if getattr(d, "writable_on_update", True)
        }
        filtered = {k: v for k, v in self._data.items() if k in writable}
        return type(self).from_dict(filtered)

    # ------------------------------------------------------ class methods

    @classmethod
    def from_dict(cls: type[_EntityT], data: Dict[str, Any]) -> _EntityT:
        """Hydrate a typed entity instance from a plain dictionary.

        The dictionary keys must be Dataverse logical names.  Extra keys (e.g.
        OData metadata annotations) are stored in ``_data`` and silently ignored
        when accessed through field descriptors that have no matching
        ``_logical_name``.

        :param data: Dictionary of ``{logical_name: value}`` pairs.
        :type data: dict
        :return: A new instance of this class with ``_data`` populated from
            ``data``.
        :rtype: same class as caller
        """
        instance = cls.__new__(cls)
        object.__setattr__(instance, "_data", dict(data))
        return instance

    @classmethod
    def fields(cls) -> Dict[str, Any]:
        """Return all field descriptors defined on this class and its bases.

        Traverses the MRO (base-to-derived) so that subclass descriptors
        override parent descriptors with the same attribute name.

        :return: ``{attribute_name: descriptor}`` for every field descriptor
            (identified by having both a ``__set__`` method and a
            ``_is_field_descriptor`` marker).
        :rtype: dict
        """
        result: Dict[str, Any] = {}
        for klass in reversed(cls.__mro__):
            for name, obj in klass.__dict__.items():
                if (
                    not name.startswith("_")
                    and getattr(obj, "_is_field_descriptor", False)
                    and hasattr(obj, "__get__")
                    and hasattr(obj, "__set__")
                ):
                    result[name] = obj
        return result

    # ------------------------------------------------------ dunder helpers

    def __repr__(self) -> str:
        cls_name = type(self).__name__
        table = type(self)._logical_name
        return f"{cls_name}(table={table!r}, data={self._data!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Entity):
            return NotImplemented
        return type(self) is type(other) and self._data == other._data

    def __hash__(self) -> int:
        return id(self)
