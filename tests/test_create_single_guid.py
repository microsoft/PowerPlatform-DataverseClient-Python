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
        # Simulate minimal Response-like object
        resp = types.SimpleNamespace()
        resp.headers = self._headers
        resp.status_code = 204
        def raise_for_status():
            return None
        resp.raise_for_status = raise_for_status
        return resp

class TestableOData(ODataClient):
    def __init__(self, headers):
        super().__init__(DummyAuth(), "https://org.example", None)
        # Monkey-patch http client
        self._http = types.SimpleNamespace(request=lambda method, url, **kwargs: DummyHTTP(headers).request(method, url, **kwargs))

def test__create_single_uses_odata_entityid():
    guid = "11111111-2222-3333-4444-555555555555"
    headers = {"OData-EntityId": f"https://org.example/api/data/v9.2/accounts({guid})"}
    c = TestableOData(headers)
    result = c._create_single("accounts", {"name": "x"})
    assert result == guid

def test__create_single_fallback_location():
    guid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    headers = {"Location": f"https://org.example/api/data/v9.2/contacts({guid})"}
    c = TestableOData(headers)
    result = c._create_single("contacts", {"firstname": "x"})
    assert result == guid

def test__create_single_missing_headers_raises():
    c = TestableOData({})
    import pytest
    with pytest.raises(RuntimeError):
        c._create_single("accounts", {"name": "x"})
