# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import pytest
from dataverse_sdk.core.errors import MetadataError
from tests.unit.test_helpers import (
    TestableClient,
    MD_ACCOUNT,
    make_entity_create_headers,
    make_entity_metadata
)

# Additional metadata for this test file
MD_SAMPLE = make_entity_metadata("new_sampleitem", "new_sampleitems", "new_Sampleitem", "new_sampleitemid")


def test_single_create_update_delete_get():
    guid = "11111111-2222-3333-4444-555555555555"
    # Sequence: metadata lookup, single create, single get, update, delete
    responses = [
        (200, {}, MD_ACCOUNT),  # metadata for account
        (204, make_entity_create_headers("accounts", guid), {}),  # create
        (200, {}, {"accountid": guid, "name": "Acme"}),  # get
        (204, {}, {}),  # update (no body)
        (204, {}, {}),  # delete
    ]
    c = TestableClient(responses)
    entity_set = c._entity_set_from_logical("account")
    rid = c._create(entity_set, "account", {"name": "Acme"})
    assert rid == guid
    rec = c._get("account", rid, select="accountid,name")
    assert rec["accountid"] == guid and rec["name"] == "Acme"
    c._update("account", rid, {"telephone1": "555"})  # returns None
    c._delete("account", rid)  # returns None


def test_bulk_create_and_update():
    g1 = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    g2 = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    # Sequence: metadata, bulk create, bulk update (broadcast), bulk update (1:1)
    responses = [
        (200, {}, MD_ACCOUNT),
        (200, {}, {"Ids": [g1, g2]}),  # CreateMultiple
        (204, {}, {}),  # UpdateMultiple broadcast
        (204, {}, {}),  # UpdateMultiple 1:1
    ]
    c = TestableClient(responses)
    entity_set = c._entity_set_from_logical("account")
    ids = c._create_multiple(entity_set, "account", [{"name": "A"}, {"name": "B"}])
    assert ids == [g1, g2]
    c._update_by_ids("account", ids, {"statecode": 1})  # broadcast
    c._update_by_ids("account", ids, [{"name": "A1"}, {"name": "B1"}])  # per-record


def test_get_multiple_paging():
    # metadata, first page, second page
    responses = [
        (200, {}, MD_ACCOUNT),
        (200, {}, {"value": [{"accountid": "1"}], "@odata.nextLink": "https://org.example/api/data/v9.2/accounts?$skip=1"}),
        (200, {}, {"value": [{"accountid": "2"}]}),
    ]
    c = TestableClient(responses)
    pages = list(c._get_multiple("account", select=["accountid"], page_size=1))
    assert pages == [[{"accountid": "1"}], [{"accountid": "2"}]]


def test_unknown_logical_name_raises():
    responses = [
        (200, {}, {"value": []}),  # metadata lookup returns empty
    ]
    c = TestableClient(responses)
    with pytest.raises(MetadataError):
        c._entity_set_from_logical("nonexistent")