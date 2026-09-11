"""Unit tests for ``records.list`` (paginated iterator).

These tests do not hit a real FinOps environment; they patch
``HttpClient.request`` to assert the SDK builds the right OData URLs and
correctly follows ``@odata.nextLink`` continuation tokens.
"""
from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import pytest

from PowerPlatform.FinOps import FinOpsClient

ENV_URL = "https://my-finops-env.cloudax.dynamics.com"


class _FakeCredential:
    def __init__(self) -> None:
        self.calls = 0

    def get_token(self, *scopes: str, **_: object):
        self.calls += 1
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


class TestRecordsList:
    def test_yields_single_page(self, client):
        page = {"value": [{"id": 1}, {"id": 2}, {"id": 3}]}
        with patch.object(
            client._http, "request", return_value=_make_response(page)
        ) as m:
            rows = list(client.records.list("CustomersV3"))
        assert rows == [{"id": 1}, {"id": 2}, {"id": 3}]
        assert m.call_count == 1
        # First positional args: method, url
        args, kwargs = m.call_args
        assert args[0] == "GET"
        assert args[1] == f"{ENV_URL}/data/CustomersV3"
        assert kwargs["params"] is None  # no $-options requested

    def test_follows_nextlink_across_pages(self, client):
        next_url = f"{ENV_URL}/data/CustomersV3?cookie=abc"
        responses = [
            _make_response({"value": [{"id": 1}], "@odata.nextLink": next_url}),
            _make_response({"value": [{"id": 2}], "@odata.nextLink": next_url}),
            _make_response({"value": [{"id": 3}]}),
        ]
        with patch.object(client._http, "request", side_effect=responses) as m:
            rows = list(client.records.list("CustomersV3"))
        assert rows == [{"id": 1}, {"id": 2}, {"id": 3}]
        assert m.call_count == 3
        # Page 2 and 3 must be GET against the nextLink URL with params=None.
        for call in m.call_args_list[1:]:
            assert call.args[1] == next_url
            assert call.kwargs["params"] is None

    def test_emits_filter_select_expand_orderby(self, client):
        with patch.object(
            client._http, "request", return_value=_make_response({"value": []})
        ) as m:
            list(
                client.records.list(
                    "CustomersV3",
                    filter="dataAreaId eq 'usmf'",
                    select=["CustomerAccount", "OrganizationName"],
                    expand=["Contacts"],
                    orderby=["CreatedDateTime desc", "CustomerAccount"],
                )
            )
        params = m.call_args.kwargs["params"]
        assert params == {
            "$filter": "dataAreaId eq 'usmf'",
            "$select": "CustomerAccount,OrganizationName",
            "$expand": "Contacts",
            "$orderby": "CreatedDateTime desc,CustomerAccount",
        }

    def test_top_caps_clientside_when_server_overruns(self, client):
        # Server returns more than $top requested — SDK must enforce client-side cap.
        page = {"value": [{"i": i} for i in range(10)]}
        with patch.object(
            client._http, "request", return_value=_make_response(page)
        ) as m:
            rows = list(client.records.list("CustomersV3", top=3))
        assert rows == [{"i": 0}, {"i": 1}, {"i": 2}]
        assert m.call_args.kwargs["params"]["$top"] == "3"

    def test_top_zero_yields_nothing_no_request(self, client):
        with patch.object(client._http, "request") as m:
            rows = list(client.records.list("CustomersV3", top=0))
        assert rows == []
        assert m.call_count == 0

    def test_page_size_emits_prefer_header(self, client):
        with patch.object(
            client._http, "request", return_value=_make_response({"value": []})
        ) as m:
            list(client.records.list("CustomersV3", page_size=500))
        assert m.call_args.kwargs["headers"] == {
            "Prefer": "odata.maxpagesize=500"
        }

    def test_page_size_invalid_raises(self, client):
        with pytest.raises(ValueError):
            # Materialize the generator to trigger the validation.
            list(client.records.list("CustomersV3", page_size=0))

    def test_orderby_string_passthrough(self, client):
        with patch.object(
            client._http, "request", return_value=_make_response({"value": []})
        ) as m:
            list(client.records.list("X", orderby="CreatedDateTime desc"))
        assert m.call_args.kwargs["params"]["$orderby"] == "CreatedDateTime desc"

    def test_top_stops_paging_early(self, client):
        responses = [
            _make_response({"value": [{"i": 0}, {"i": 1}], "@odata.nextLink": "u"}),
            _make_response({"value": [{"i": 2}, {"i": 3}], "@odata.nextLink": "u"}),
        ]
        with patch.object(client._http, "request", side_effect=responses) as m:
            rows = list(client.records.list("X", top=3))
        assert rows == [{"i": 0}, {"i": 1}, {"i": 2}]
        # Should have stopped after page 2 yielded the 3rd row.
        assert m.call_count == 2


def test_cross_company_flag_emits_query_param(client):
    """`cross_company=True` -> `cross-company=true` query string (FinOps OData quirk)."""
    with patch.object(
        client._http, "request", return_value=_make_response({"value": []})
    ) as m:
        list(client.records.list("CustomerGroups", cross_company=True, top=1))
    params = m.call_args.kwargs["params"]
    assert params["cross-company"] == "true"
    assert params["$top"] == "1"


def test_cross_company_default_off(client):
    with patch.object(
        client._http, "request", return_value=_make_response({"value": []})
    ) as m:
        list(client.records.list("CustomerGroups", top=1))
    params = m.call_args.kwargs["params"]
    assert "cross-company" not in params
