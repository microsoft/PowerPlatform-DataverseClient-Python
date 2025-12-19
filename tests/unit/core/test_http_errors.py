# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import pytest
from azure.core.credentials import TokenCredential
from PowerPlatform.Dataverse.client import DataverseClient
from PowerPlatform.Dataverse.core.config import DataverseConfig
from PowerPlatform.Dataverse.core.errors import HttpError
from PowerPlatform.Dataverse.core._error_codes import HTTP_404, HTTP_429, HTTP_500
from PowerPlatform.Dataverse.data._odata import _ODataClient


class DummyAuth:
    def _acquire_token(self, scope):
        class T:
            access_token = "x"

        return T()


class DummyHTTP:
    def __init__(self, responses):
        self._responses = responses

    def _request(self, method, url, **kwargs):
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

            def json_func():
                return body

            r.json = json_func
        else:
            r.text = body or ""

            def json_fail():
                raise ValueError("non-json")

            r.json = json_fail
        return r


class MockClient(_ODataClient):
    def __init__(self, responses):
        super().__init__(DummyAuth(), "https://org.example", None)
        self._http = DummyHTTP(responses)


class RecordingHTTP(DummyHTTP):
    def __init__(self, responses):
        super().__init__(responses)
        self.recorded_headers = []

    def _request(self, method, url, **kwargs):
        headers = (kwargs.get("headers") or {}).copy()
        self.recorded_headers.append(headers)
        return super()._request(method, url, **kwargs)


class DummyCredential(TokenCredential):
    def get_token(self, *scopes, **kwargs):
        class Tok:
            token = "dummy-token"

        return Tok()


# --- Tests ---


def test_http_404_subcode_and_service_code():
    responses = [
        (
            404,
            {"x-ms-correlation-request-id": "cid1"},
            {"error": {"code": "0x800404", "message": "Not found"}},
        )
    ]
    c = MockClient(responses)
    with pytest.raises(HttpError) as ei:
        c._request("get", c.api + "/accounts(abc)")
    err = ei.value.to_dict()
    assert err["subcode"] == HTTP_404
    assert err["details"]["service_error_code"] == "0x800404"


def test_http_429_transient_and_retry_after():
    responses = [
        (
            429,
            {"Retry-After": "7"},
            {"error": {"message": "Throttle"}},
        )
    ]
    c = MockClient(responses)
    with pytest.raises(HttpError) as ei:
        c._request("get", c.api + "/accounts")
    err = ei.value.to_dict()
    assert err["is_transient"] is True
    assert err["subcode"] == HTTP_429
    assert err["details"]["retry_after"] == 7


def test_http_500_body_excerpt():
    responses = [
        (
            500,
            {},
            "Internal failure XYZ stack truncated",
        )
    ]
    c = MockClient(responses)
    with pytest.raises(HttpError) as ei:
        c._request("get", c.api + "/accounts")
    err = ei.value.to_dict()
    assert err["subcode"] == HTTP_500
    assert "XYZ stack" in err["details"]["body_excerpt"]


def test_http_non_mapped_status_code_subcode_fallback():
    responses = [
        (
            418,  # I'm a teapot (not in map)
            {},
            {"error": {"message": "Teapot"}},
        )
    ]
    c = MockClient(responses)
    with pytest.raises(HttpError) as ei:
        c._request("get", c.api + "/accounts")
    err = ei.value.to_dict()
    assert err["subcode"] == "http_418"


def test_correlation_id_diff_without_scope():
    responses = [
        (200, {}, {"value": []}),
        (200, {}, {"value": []}),
    ]
    c = MockClient([])
    recorder = RecordingHTTP(responses)
    c._http = recorder
    c._request("get", c.api + "/accounts")
    c._request("get", c.api + "/accounts")
    assert len(recorder.recorded_headers) == 2
    h1, h2 = recorder.recorded_headers
    assert h1["x-ms-client-request-id"] != h2["x-ms-client-request-id"]
    cid1 = h1.get("x-ms-correlation-request-id")
    cid2 = h2.get("x-ms-correlation-request-id")
    if cid1 is not None and cid2 is not None:
        assert cid1 != cid2
    else:
        assert cid1 is cid2 is None


def test_correlation_id_shared_inside_call_scope():
    responses = [
        (200, {}, {"value": []}),
        (200, {}, {"value": []}),
    ]
    c = MockClient([])
    recorder = RecordingHTTP(responses)
    c._http = recorder
    with c._call_scope():
        c._request("get", c.api + "/accounts")
        c._request("get", c.api + "/accounts")
    assert len(recorder.recorded_headers) == 2
    h1, h2 = recorder.recorded_headers
    assert h1["x-ms-client-request-id"] != h2["x-ms-client-request-id"]
    assert h1["x-ms-correlation-id"] == h2["x-ms-correlation-id"]
