# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import pytest
from PowerPlatform.Dataverse.core.errors import HttpError
from PowerPlatform.Dataverse.core.error_codes import HTTP_404, HTTP_429, HTTP_500
from PowerPlatform.Dataverse.data.odata import ODataClient

class DummyAuth:
    def acquire_token(self, scope):
        class T: access_token = "x"
        return T()

class DummyHTTP:
    def __init__(self, responses):
        self._responses = responses
    def request(self, method, url, **kwargs):
        if not self._responses:
            raise AssertionError("No more responses")
        status, headers, body = self._responses.pop(0)
        class R:
            pass
        r = R()
        r.status_code = status
        r.headers = headers
        if isinstance(body, dict):
            import json
            r.text = json.dumps(body)
            def json_func(): return body
            r.json = json_func
        else:
            r.text = body or ""
            def json_fail(): raise ValueError("non-json")
            r.json = json_fail
        return r

class MockClient(ODataClient):
    def __init__(self, responses):
        super().__init__(DummyAuth(), "https://org.example", None)
        self._http = DummyHTTP(responses)

# --- Tests ---

def test_http_404_subcode_and_service_code():
    responses = [(
        404,
        {"x-ms-correlation-request-id": "cid1"},
        {"error": {"code": "0x800404", "message": "Not found"}},
    )]
    c = MockClient(responses)
    with pytest.raises(HttpError) as ei:
        c._request("get", c.api + "/accounts(abc)")
    err = ei.value.to_dict()
    assert err["subcode"] == HTTP_404
    assert err["details"]["service_error_code"] == "0x800404"


def test_http_429_transient_and_retry_after():
    responses = [(
        429,
        {"Retry-After": "7"},
        {"error": {"message": "Throttle"}},
    )]
    c = MockClient(responses)
    with pytest.raises(HttpError) as ei:
        c._request("get", c.api + "/accounts")
    err = ei.value.to_dict()
    assert err["is_transient"] is True
    assert err["subcode"] == HTTP_429
    assert err["details"]["retry_after"] == 7


def test_http_500_body_excerpt():
    responses = [(
        500,
        {},
        "Internal failure XYZ stack truncated",
    )]
    c = MockClient(responses)
    with pytest.raises(HttpError) as ei:
        c._request("get", c.api + "/accounts")
    err = ei.value.to_dict()
    assert err["subcode"] == HTTP_500
    assert "XYZ stack" in err["details"]["body_excerpt"]


def test_http_non_mapped_status_code_subcode_fallback():
    responses = [(
        418,  # I'm a teapot (not in map)
        {},
        {"error": {"message": "Teapot"}},
    )]
    c = MockClient(responses)
    with pytest.raises(HttpError) as ei:
        c._request("get", c.api + "/accounts")
    err = ei.value.to_dict()
    assert err["subcode"] == "http_418"
