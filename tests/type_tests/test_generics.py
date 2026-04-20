# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Static type tests for generics introduced in the typed entity system.

Run with pyright to verify Pylance/pyright infers the correct types:

    pip install pyright
    pyright tests/type_tests/test_generics.py

Each reveal_type() call documents the expected inferred type.
Pyright prints these to stdout — no assertion failures, but wrong types
are caught as pyright errors.
"""

from __future__ import annotations

from typing import Iterable, List, Optional

from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.models.entity import Entity, Field
from PowerPlatform.Dataverse.models.record import Record
from PowerPlatform.Dataverse.models.query_builder import QueryBuilder
from PowerPlatform.Dataverse.models.filters import FilterExpression


# ---------------------------------------------------------------------------
# Minimal entity classes for type testing — no live Dataverse needed
# ---------------------------------------------------------------------------

from PowerPlatform.Dataverse.models.entity import Field


class Account(Entity, table="account", primary_key="accountid"):
    accountid = Field("accountid", str,   writable_on_create=False, writable_on_update=False)
    name      = Field("name",      str)
    revenue   = Field("revenue",   float)
    statecode = Field("statecode", int)


class Contact(Entity, table="contact", primary_key="contactid"):
    contactid:    str
    firstname:    str
    emailaddress1: str


# ---------------------------------------------------------------------------
# records.get() — single record
# ---------------------------------------------------------------------------

def check_records_get_typed(client: DataverseClient, guid: str) -> None:
    account = client.records.get(Account, guid)
    reveal_type(account)                    # Expected: Account
    reveal_type(account.name)               # Expected: str | None  (Field[str].__get__ on instance)

    contact = client.records.get(Contact, guid)
    reveal_type(contact)                    # Expected: Contact


def check_records_get_string(client: DataverseClient, guid: str) -> None:
    record = client.records.get("account", guid)
    reveal_type(record)                     # Expected: Record


# ---------------------------------------------------------------------------
# query.builder() + execute()
# ---------------------------------------------------------------------------

def check_builder_typed(client: DataverseClient) -> None:
    qb = client.query.builder(Account)
    reveal_type(qb)                         # Expected: QueryBuilder[Account]

    results = qb.select(Account.name).top(10).execute()
    reveal_type(results)                    # Expected: Iterable[Account] | Iterable[list[Account]]

    for acct in qb.top(1).execute():
        reveal_type(acct)                   # Expected: Account


def check_builder_string(client: DataverseClient) -> None:
    qb = client.query.builder("account")
    reveal_type(qb)                         # Expected: QueryBuilder[Record]

    for record in qb.select("name").top(1).execute():
        reveal_type(record)                 # Expected: Record


# ---------------------------------------------------------------------------
# records.create() — returns str regardless of typed/untyped
# ---------------------------------------------------------------------------

def check_records_create(client: DataverseClient) -> None:
    guid = client.records.create(Account(name="Contoso"))
    reveal_type(guid)                       # Expected: str


# ---------------------------------------------------------------------------
# Different entity types don't bleed into each other
# ---------------------------------------------------------------------------

def check_no_type_bleed(client: DataverseClient, guid: str) -> None:
    account = client.records.get(Account, guid)
    contact = client.records.get(Contact, guid)

    reveal_type(account)                    # Expected: Account  (not Contact, not Entity)
    reveal_type(contact)                    # Expected: Contact  (not Account, not Entity)


# ---------------------------------------------------------------------------
# Field[T] — instance access returns T | None
# ---------------------------------------------------------------------------

def check_field_instance_type(client: DataverseClient, guid: str) -> None:
    account = client.records.get(Account, guid)
    reveal_type(account.name)               # Expected: str | None
    reveal_type(account.revenue)            # Expected: float | None
    reveal_type(account.statecode)          # Expected: int | None

    name: Optional[str] = account.name     # must not be a type error
    rev:  Optional[float] = account.revenue


# ---------------------------------------------------------------------------
# to_create_payload() / to_update_payload() — preserve concrete type
# ---------------------------------------------------------------------------

def check_payload_methods(client: DataverseClient, guid: str) -> None:
    account = client.records.get(Account, guid)
    create_payload = account.to_create_payload()
    update_payload = account.to_update_payload()

    reveal_type(create_payload)             # Expected: Account
    reveal_type(update_payload)             # Expected: Account

    reveal_type(create_payload.name)        # Expected: str | None


# ---------------------------------------------------------------------------
# from_record() — preserves concrete type
# ---------------------------------------------------------------------------

def check_from_record_type(record: "Record") -> None:
    account = Account.from_record(record)
    contact = Contact.from_record(record)

    reveal_type(account)                    # Expected: Account
    reveal_type(contact)                    # Expected: Contact
