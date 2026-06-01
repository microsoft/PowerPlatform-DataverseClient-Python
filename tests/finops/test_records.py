"""Unit tests for ``PowerPlatform.FinOps.operations.records``.

These tests do not hit a real FinOps environment. They patch the SDK's
``HttpClient.request`` method to assert the SDK builds the right URLs,
headers, and bodies for the four CRUD verbs.
"""
from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from PowerPlatform.FinOps import (
    FinOpsClient,
    FinOpsConcurrencyError,
    FinOpsHttpError,
    FinOpsNotFoundError,
)
from PowerPlatform.FinOps.operations.records import _format_key, _format_value


# ---------------------------------------------------------------------- #
# Fixtures                                                               #
# ---------------------------------------------------------------------- #

ENV_URL = "https://my-finops-env.cloudax.dynamics.com"


class _FakeCredential:
    """Minimal stand-in for ``azure.core.credentials.TokenCredential``."""

    def __init__(self, token: str = "test-token", ttl: int = 3600) -> None:
        self._token = token
        self._ttl = ttl
        self.calls = 0

    def get_token(self, *scopes: str, **_: object):
        self.calls += 1
        return MagicMock(token=self._token, expires_on=int(time.time()) + self._ttl)


def _make_response(
    status: int = 200,
    *,
    json_body: object = None,
    headers: dict | None = None,
    text: str = "",
):
    resp = MagicMock()
    resp.status_code = status
    resp.headers = headers or {}
    resp.url = "https://stubbed/"
    resp.request = MagicMock(method="GET", url="https://stubbed/")
    if json_body is not None:
        resp.json = MagicMock(return_value=json_body)
        resp.content = b"{}"
    else:
        resp.json = MagicMock(side_effect=ValueError())
        resp.content = text.encode() if text else b""
    resp.text = text
    return resp


@pytest.fixture
def client():
    cred = _FakeCredential()
    c = FinOpsClient(ENV_URL, cred)
    yield c
    c.close()


# ---------------------------------------------------------------------- #
# OData key formatting                                                   #
# ---------------------------------------------------------------------- #


class TestKeyFormatting:
    def test_string_key_quoted(self):
        assert _format_key("M0001") == "'M0001'"

    def test_int_key_unquoted(self):
        assert _format_key(42) == "42"

    def test_string_key_escapes_single_quotes(self):
        assert _format_key("O'Brien") == "'O''Brien'"

    def test_composite_key(self):
        out = _format_key({"dataAreaId": "usmf", "ItemNumber": "M0001"})
        assert out == "dataAreaId='usmf',ItemNumber='M0001'"

    def test_bool_value(self):
        assert _format_value(True) == "true"
        assert _format_value(False) == "false"

    def test_empty_composite_rejected(self):
        with pytest.raises(ValueError):
            _format_key({})

    def test_unsupported_type_rejected(self):
        with pytest.raises(TypeError):
            _format_value(object())


# ---------------------------------------------------------------------- #
# CREATE                                                                 #
# ---------------------------------------------------------------------- #


class TestCreate:
    def test_post_to_collection_returns_entity_id_header(self, client):
        location = (
            f"{ENV_URL}/data/CustomersV3"
            "(dataAreaId='usmf',CustomerAccount='TEST')"
        )
        resp = _make_response(201, headers={"OData-EntityId": location})
        with patch.object(client._http, "request", return_value=resp) as m:
            result = client.records.create("CustomersV3", {"CustomerAccount": "TEST"})
        assert result == location
        m.assert_called_once()
        args, kwargs = m.call_args
        assert args == ("POST", f"{ENV_URL}/data/CustomersV3")
        assert kwargs["json"] == {"CustomerAccount": "TEST"}
        assert kwargs["headers"]["Content-Type"] == "application/json"

    def test_falls_back_to_response_body_when_no_header(self, client):
        body = {"CustomerAccount": "TEST"}
        resp = _make_response(201, json_body=body)
        with patch.object(client._http, "request", return_value=resp):
            assert client.records.create("CustomersV3", {"CustomerAccount": "TEST"}) == body

    def test_rejects_non_mapping_data(self, client):
        with pytest.raises(TypeError):
            client.records.create("CustomersV3", ["not", "a", "mapping"])  # type: ignore[arg-type]


# ---------------------------------------------------------------------- #
# RETRIEVE                                                               #
# ---------------------------------------------------------------------- #


class TestGet:
    def test_get_with_composite_key_and_select(self, client):
        body = {"CustomerAccount": "TEST", "OrganizationName": "Acme"}
        resp = _make_response(200, json_body=body)
        with patch.object(client._http, "request", return_value=resp) as m:
            row = client.records.get(
                "CustomersV3",
                {"dataAreaId": "usmf", "CustomerAccount": "TEST"},
                select=["CustomerAccount", "OrganizationName"],
            )
        assert row == body
        args, kwargs = m.call_args
        assert args == (
            "GET",
            f"{ENV_URL}/data/CustomersV3"
            "(dataAreaId='usmf',CustomerAccount='TEST')",
        )
        assert kwargs["params"] == {"$select": "CustomerAccount,OrganizationName"}

    def test_get_scalar_string_key(self, client):
        resp = _make_response(200, json_body={})
        with patch.object(client._http, "request", return_value=resp) as m:
            client.records.get("Items", "M0001")
        args, _ = m.call_args
        assert args[1] == f"{ENV_URL}/data/Items('M0001')"


# ---------------------------------------------------------------------- #
# UPDATE                                                                 #
# ---------------------------------------------------------------------- #


class TestUpdate:
    def test_patch_with_default_etag(self, client):
        resp = _make_response(204)
        with patch.object(client._http, "request", return_value=resp) as m:
            client.records.update(
                "CustomersV3",
                {"dataAreaId": "usmf", "CustomerAccount": "TEST"},
                {"OrganizationName": "Updated"},
            )
        args, kwargs = m.call_args
        assert args[0] == "PATCH"
        assert args[1].endswith("(dataAreaId='usmf',CustomerAccount='TEST')")
        assert kwargs["json"] == {"OrganizationName": "Updated"}
        assert kwargs["headers"]["If-Match"] == "*"

    def test_patch_with_explicit_etag(self, client):
        resp = _make_response(204)
        with patch.object(client._http, "request", return_value=resp) as m:
            client.records.update(
                "Items", "M0001", {"ItemDescription": "x"}, etag='W/"123"'
            )
        assert m.call_args.kwargs["headers"]["If-Match"] == 'W/"123"'

    def test_empty_changes_rejected(self, client):
        with pytest.raises(ValueError):
            client.records.update("Items", "M0001", {})

    def test_non_mapping_changes_rejected(self, client):
        with pytest.raises(TypeError):
            client.records.update("Items", "M0001", ["bad"])  # type: ignore[arg-type]


# ---------------------------------------------------------------------- #
# DELETE                                                                 #
# ---------------------------------------------------------------------- #


class TestDelete:
    def test_delete_with_default_etag(self, client):
        resp = _make_response(204)
        with patch.object(client._http, "request", return_value=resp) as m:
            client.records.delete("Items", "M0001")
        args, kwargs = m.call_args
        assert args[0] == "DELETE"
        assert args[1] == f"{ENV_URL}/data/Items('M0001')"
        assert kwargs["headers"]["If-Match"] == "*"


# ---------------------------------------------------------------------- #
# Error mapping                                                          #
# ---------------------------------------------------------------------- #


class TestErrorMapping:
    """End-to-end exception hierarchy checks via _http._raise."""

    def _make_session_response(self, status, headers=None, body=None):
        resp = MagicMock()
        resp.status_code = status
        resp.headers = headers or {}
        resp.url = "https://stubbed/x"
        resp.request = MagicMock(method="GET", url=resp.url)
        if body is not None:
            resp.json = MagicMock(return_value=body)
        else:
            resp.json = MagicMock(side_effect=ValueError())
            resp.text = ""
        resp.content = b"{}"
        return resp

    def test_404_maps_to_not_found(self, client):
        bad = self._make_session_response(404, body={"error": "x"})
        with patch.object(client._http._session, "request", return_value=bad):
            with pytest.raises(FinOpsNotFoundError) as ei:
                client.records.get("Items", "missing")
        assert ei.value.status_code == 404

    def test_412_maps_to_concurrency(self, client):
        bad = self._make_session_response(
            412,
            headers={"ms-dyn-activityid": "act-123"},
            body={"error": "etag"},
        )
        with patch.object(client._http._session, "request", return_value=bad):
            with pytest.raises(FinOpsConcurrencyError) as ei:
                client.records.update("Items", "M0001", {"x": 1}, etag='W/"old"')
        assert ei.value.activity_id == "act-123"

    def test_500_maps_to_generic_http_error(self, client):
        bad = self._make_session_response(500, body={"error": "boom"})
        with patch.object(client._http._session, "request", return_value=bad):
            with pytest.raises(FinOpsHttpError):
                client.records.delete("Items", "M0001")


# ---------------------------------------------------------------------- #
# Auth wiring                                                            #
# ---------------------------------------------------------------------- #


class TestAuth:
    def test_token_cached_across_calls(self, client):
        resp = _make_response(200, json_body={})
        with patch.object(client._http._session, "request", return_value=resp):
            client.records.get("Items", "A")
            client.records.get("Items", "B")
        # _FakeCredential.calls == 1 means token was cached.
        assert client._token_provider._credential.calls == 1  # type: ignore[attr-defined]

    def test_default_scope_is_env_dot_default(self, client):
        assert client.scope == f"{ENV_URL}/.default"

    def test_data_url_format(self, client):
        assert client.data_url == f"{ENV_URL}/data"
