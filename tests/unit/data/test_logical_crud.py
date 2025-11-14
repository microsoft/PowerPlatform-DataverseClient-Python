# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import types
import pytest
from PowerPlatform.Dataverse.data.odata import ODataClient
from PowerPlatform.Dataverse.core.errors import MetadataError

class DummyAuth:
    def acquire_token(self, scope):
        class T: access_token = "x"
        return T()

class DummyHTTPClient:
    def __init__(self, responses):
        self._responses = responses
        self.calls = []
    def request(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if not self._responses:
            raise AssertionError("No more dummy responses configured")
        status, headers, body = self._responses.pop(0)
        resp = types.SimpleNamespace()
        resp.status_code = status
        resp.headers = headers
        resp.text = "" if body is None else ("{}" if isinstance(body, dict) else str(body))
        def raise_for_status():
            if status >= 400:
                raise RuntimeError(f"HTTP {status}")
            return None
        def json_func():
            return body if isinstance(body, dict) else {}
        resp.raise_for_status = raise_for_status
        resp.json = json_func
        return resp

class MockableClient(ODataClient):
    def __init__(self, responses):
        super().__init__(DummyAuth(), "https://org.example", None)
        self._http = DummyHTTPClient(responses)
    def _convert_labels_to_ints(self, table_schema_name, record):  # pragma: no cover - test shim
        return record

# Helper metadata response for logical name resolution
MD_ACCOUNT = {
    "value": [
        {
            "LogicalName": "account",
            "EntitySetName": "accounts",
            "PrimaryIdAttribute": "accountid"
        }
    ]
}

MD_SAMPLE = {
    "value": [
        {
            "LogicalName": "new_sampleitem",
            "EntitySetName": "new_sampleitems",
            "PrimaryIdAttribute": "new_sampleitemid"
        }
    ]
}

def make_entity_create_headers(entity_set, guid):
    return {"OData-EntityId": f"https://org.example/api/data/v9.2/{entity_set}({guid})"}


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
    c = MockableClient(responses)
    entity_set = c._entity_set_from_schema_name("account")
    rid = c._create(entity_set, "account", {"name": "Acme"})
    assert rid == guid
    rec = c._get("account", rid, select=["accountid", "name"])
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
    c = MockableClient(responses)
    entity_set = c._entity_set_from_schema_name("account")
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
    c = MockableClient(responses)
    pages = list(c._get_multiple("account", select=["accountid"], page_size=1))
    assert pages == [[{"accountid": "1"}], [{"accountid": "2"}]]


def test_unknown_table_schema_name_raises():
    responses = [
        (200, {}, {"value": []}),  # metadata lookup returns empty
    ]
    c = MockableClient(responses)
    with pytest.raises(MetadataError):
        c._entity_set_from_schema_name("nonexistent")