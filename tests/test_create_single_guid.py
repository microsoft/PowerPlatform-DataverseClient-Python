import types
from dataverse_sdk.odata import ODataClient, _GUID_RE

class DummyAuth:
    def acquire_token(self, scope):
        class T: access_token = "x"
        return T()

class DummyHTTP:
    def __init__(self, headers):
        self._headers = headers
    def request(self, method, url, **kwargs):
        # Simulate minimal Response-like object (subset of requests.Response API used by code)
        resp = types.SimpleNamespace()
        resp.headers = self._headers
        resp.status_code = 204
        resp.text = ""
        def raise_for_status():
            return None
        def json_func():
            return {}
        resp.raise_for_status = raise_for_status
        resp.json = json_func
        return resp

class TestableOData(ODataClient):
    def __init__(self, headers):
        super().__init__(DummyAuth(), "https://org.example", None)
        # Monkey-patch http client
        self._http = types.SimpleNamespace(request=lambda method, url, **kwargs: DummyHTTP(headers).request(method, url, **kwargs))
    # Bypass optionset label conversion to keep response sequence stable for tests
    def _convert_labels_to_ints(self, logical_name, record):  # pragma: no cover - test shim
        return record

def test__create_uses_odata_entityid():
    guid = "11111111-2222-3333-4444-555555555555"
    headers = {"OData-EntityId": f"https://org.example/api/data/v9.2/accounts({guid})"}
    c = TestableOData(headers)
    # Current signature requires logical name explicitly
    result = c._create("accounts", "account", {"name": "x"})
    assert result == guid

def test__create_fallback_location():
    guid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    headers = {"Location": f"https://org.example/api/data/v9.2/contacts({guid})"}
    c = TestableOData(headers)
    result = c._create("contacts", "contact", {"firstname": "x"})
    assert result == guid

def test__create_missing_headers_raises():
    c = TestableOData({})
    import pytest
    with pytest.raises(RuntimeError):
        c._create("accounts", "account", {"name": "x"})
