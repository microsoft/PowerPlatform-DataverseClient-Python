"""Unit tests for ``MetadataOperations``.

These tests do not hit a real FinOps environment; they patch
``HttpClient.request`` to assert the SDK builds the right URLs against
the ``/metadata/...`` surface and follows ``@odata.nextLink``.
"""
from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from PowerPlatform.FinOps import FinOpsClient

ENV_URL = "https://my-finops-env.cloudax.dynamics.com"


class _FakeCredential:
    def get_token(self, *scopes: str, **_: object):
        return MagicMock(token="t", expires_on=int(time.time()) + 3600)


def _make_response(json_body):
    resp = MagicMock()
    resp.status_code = 200
    resp.headers = {}
    resp.url = "https://stubbed/"
    resp.request = MagicMock(method="GET", url="https://stubbed/")
    resp.json = MagicMock(return_value=json_body)
    resp.content = b"{}"
    resp.text = ""
    return resp


@pytest.fixture
def client():
    c = FinOpsClient(ENV_URL, _FakeCredential())
    yield c
    c.close()


class TestListDataEntities:
    def test_url_and_no_params_by_default(self, client):
        with patch.object(
            client._http, "request", return_value=_make_response({"value": []})
        ) as m:
            list(client.metadata.list_data_entities())
        args, kwargs = m.call_args
        assert args == ("GET", f"{ENV_URL}/metadata/DataEntities")
        assert kwargs["params"] is None

    def test_filter_select_top(self, client):
        with patch.object(
            client._http, "request", return_value=_make_response({"value": []})
        ) as m:
            list(
                client.metadata.list_data_entities(
                    filter="IsReadOnly eq false",
                    top=10,
                )
            )
        params = m.call_args.kwargs["params"]
        # Server rejects $select on /metadata/* with HTTP 400, so the SDK
        # intentionally does not expose it. Only $filter and $top are sent.
        assert params == {
            "$filter": "IsReadOnly eq false",
            "$top": "10",
        }

    def test_follows_nextlink(self, client):
        next_url = f"{ENV_URL}/metadata/DataEntities?cookie=zzz"
        responses = [
            _make_response({"value": [{"Name": "A"}], "@odata.nextLink": next_url}),
            _make_response({"value": [{"Name": "B"}]}),
        ]
        with patch.object(client._http, "request", side_effect=responses) as m:
            rows = list(client.metadata.list_data_entities())
        assert [r["Name"] for r in rows] == ["A", "B"]
        assert m.call_args_list[1].args[1] == next_url
        assert m.call_args_list[1].kwargs["params"] is None


class TestGetDataEntity:
    def test_url(self, client):
        with patch.object(
            client._http, "request", return_value=_make_response({"Name": "Customers"})
        ) as m:
            row = client.metadata.get_data_entity("Customers")
        assert row == {"Name": "Customers"}
        assert m.call_args.args == (
            "GET",
            f"{ENV_URL}/metadata/DataEntities('Customers')",
        )

    def test_escapes_single_quote(self, client):
        with patch.object(
            client._http, "request", return_value=_make_response({"x": 1})
        ) as m:
            client.metadata.get_data_entity("foo'bar")
        # OData v4 string literal escape: '' inside the literal.
        assert m.call_args.args[1] == f"{ENV_URL}/metadata/DataEntities('foo''bar')"

    def test_empty_name_raises(self, client):
        with pytest.raises(ValueError):
            client.metadata.get_data_entity("")


class TestPublicEntities:
    def test_list_url(self, client):
        with patch.object(
            client._http, "request", return_value=_make_response({"value": []})
        ) as m:
            list(client.metadata.list_public_entities(top=1))
        assert m.call_args.args == ("GET", f"{ENV_URL}/metadata/PublicEntities")
        assert m.call_args.kwargs["params"]["$top"] == "1"

    def test_get_url(self, client):
        with patch.object(
            client._http, "request", return_value=_make_response({})
        ) as m:
            client.metadata.get_public_entity("CustomerV3Entity")
        assert m.call_args.args == (
            "GET",
            f"{ENV_URL}/metadata/PublicEntities('CustomerV3Entity')",
        )


class TestPublicEnumerations:
    def test_list_url(self, client):
        with patch.object(
            client._http, "request", return_value=_make_response({"value": []})
        ) as m:
            list(client.metadata.list_public_enumerations())
        assert m.call_args.args == (
            "GET",
            f"{ENV_URL}/metadata/PublicEnumerations",
        )

    def test_get_url(self, client):
        with patch.object(
            client._http, "request", return_value=_make_response({})
        ) as m:
            client.metadata.get_public_enumeration("NoYes")
        assert m.call_args.args == (
            "GET",
            f"{ENV_URL}/metadata/PublicEnumerations('NoYes')",
        )

    def test_empty_name_raises(self, client):
        with pytest.raises(ValueError):
            client.metadata.get_public_enumeration("")
