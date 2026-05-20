# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""DataverseModel structural Protocol for typed entity integration."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

__all__ = ["DataverseModel"]


@runtime_checkable
class DataverseModel(Protocol):
    """Structural Protocol enabling typed entity instances to be passed to
    ``records.create()`` and ``records.update()``.

    Implement this Protocol on any entity class (dataclass, Pydantic model,
    hand-rolled) to enable it to be passed directly to CRUD operations without
    specifying the table name or converting to dict manually.

    Required class variables:

    - ``__entity_logical_name__`` — Dataverse logical entity name (e.g. ``"account"``)
    - ``__entity_set_name__`` — OData entity set name (e.g. ``"accounts"``)

    Required instance methods:

    - ``to_dict()`` — return record payload as ``dict``
    - ``from_dict(data)`` — classmethod to reconstruct from a response ``dict``

    Example::

        from dataclasses import dataclass
        from PowerPlatform.Dataverse import DataverseModel

        @dataclass
        class Account:
            __entity_logical_name__ = "account"
            __entity_set_name__ = "accounts"
            name: str = ""
            telephone1: str = ""

            def to_dict(self) -> dict:
                return {"name": self.name, "telephone1": self.telephone1}

            @classmethod
            def from_dict(cls, data: dict) -> "Account":
                return cls(
                    name=data.get("name", ""),
                    telephone1=data.get("telephone1", ""),
                )

        # isinstance() works today — Protocol is runtime_checkable:
        assert isinstance(Account(), DataverseModel)

        # Type your own helpers against the Protocol now:
        def save(entity: DataverseModel) -> None:
            data = entity.to_dict()
            client.records.create(entity.__entity_logical_name__, data)

    Note:
        Direct dispatch (``client.records.create(entity)`` without a table name
        or dict) is not yet supported and will be added in a future release.
    """

    __entity_logical_name__: str
    __entity_set_name__: str

    def to_dict(self) -> dict:
        """Return the record payload as a plain dictionary."""
        ...

    @classmethod
    def from_dict(cls, data: dict) -> DataverseModel:
        """Reconstruct an instance from a response dictionary."""
        ...
