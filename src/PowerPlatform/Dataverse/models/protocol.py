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
        from PowerPlatform.Dataverse.models.protocol import DataverseModel

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

        # Use the entity directly with records operations:
        guid = client.records.create(Account(name="Contoso"))
        client.records.update(Account(name="Contoso Updated"), guid)
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
