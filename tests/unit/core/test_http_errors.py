# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import pytest
from dataverse_sdk.core.errors import HttpError
from dataverse_sdk.core import error_codes as ec
from dataverse_sdk.data.odata import ODataClient
from tests.unit.test_helpers import DummyAuth, DummyHTTPClient

class TestClient(ODataClient):
    """Test client for HTTP error testing."""
    def __init__(self, responses):
        super().__init__(DummyAuth(), "https://org.example", None)
        self._http = DummyHTTPClient(responses)

# --- Tests ---

def test_http_404_subcode_and_service_code():
    responses = [(
        404,
        {"x-ms-correlation-request-id": "cid1"},
        {"error": {"code": "0x800404", "message": "Not found"}},
    )]
    c = TestClient(responses)
    with pytest.raises(HttpError) as ei:
        c._request("get", c.api + "/accounts(abc)")
    err = ei.value.to_dict()
    assert err["subcode"] == ec.HTTP_404
    assert err["details"]["service_error_code"] == "0x800404"


def test_http_429_transient_and_retry_after():
    responses = [(
        429,
        {"Retry-After": "7"},
        {"error": {"message": "Throttle"}},
    )]
    c = TestClient(responses)
    with pytest.raises(HttpError) as ei:
        c._request("get", c.api + "/accounts")
    err = ei.value.to_dict()
    assert err["is_transient"] is True
    assert err["subcode"] == ec.HTTP_429
    assert err["details"]["retry_after"] == 7


def test_http_500_body_excerpt():
    responses = [(
        500,
        {},
        "Internal failure XYZ stack truncated",
    )]
    c = TestClient(responses)
    with pytest.raises(HttpError) as ei:
        c._request("get", c.api + "/accounts")
    err = ei.value.to_dict()
    assert err["subcode"] == ec.HTTP_500
    assert "XYZ stack" in err["details"]["body_excerpt"]


def test_http_non_mapped_status_code_subcode_fallback():
    responses = [(
        418,  # I'm a teapot (not in map)
        {},
        {"error": {"message": "Teapot"}},
    )]
    c = TestClient(responses)
    with pytest.raises(HttpError) as ei:
        c._request("get", c.api + "/accounts")
    err = ei.value.to_dict()
    assert err["subcode"] == "http_418"
