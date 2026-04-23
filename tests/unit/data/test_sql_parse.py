# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from unittest.mock import MagicMock, patch
from urllib.parse import parse_qs, urlparse

import pytest
from PowerPlatform.Dataverse.data._odata import _ODataClient, _extract_pagingcookie


class DummyAuth:
    def _acquire_token(self, scope):
        class T:
            access_token = "x"  # no real token needed for parsing tests

        return T()


def _client():
    return _ODataClient(DummyAuth(), "https://org.example", None)


# ---------------------------------------------------------------------------
# Helpers for _build_sql tests
# ---------------------------------------------------------------------------

_BARE = object.__new__(_ODataClient)
_BARE.api = "https://org.crm.dynamics.com/api/data/v9.2"
_ENTITY_SET = "accounts"


def _build(sql: str) -> str:
    with patch.object(_BARE, "_entity_set_from_schema_name", return_value=_ENTITY_SET):
        return _BARE._build_sql(sql).url


def _sql_param(url: str) -> str:
    return parse_qs(urlparse(url).query)["sql"][0]


def test_basic_from():
    c = _client()
    assert c._extract_logical_table("SELECT a FROM account") == "account"


def test_underscore_name():
    c = _client()
    assert c._extract_logical_table("select x FROM new_sampleitem where x=1") == "new_sampleitem"


def test_startfrom_identifier():
    c = _client()
    # Ensure we pick the real table 'case', not 'from' portion inside 'startfrom'
    assert c._extract_logical_table("SELECT col, startfrom FROM case") == "case"


def test_case_insensitive_keyword():
    c = _client()
    assert c._extract_logical_table("SeLeCt 1 FrOm ACCOUNT") == "account"


def test_missing_from_raises():
    c = _client()
    with pytest.raises(ValueError):
        c._extract_logical_table("SELECT 1")


def test_from_as_value_not_table():
    c = _client()
    # Table should still be 'incident'; word 'from' earlier shouldn't interfere
    sql = "SELECT 'from something', col FROM incident"
    assert c._extract_logical_table(sql) == "incident"


# ---------------------------------------------------------------------------
# _build_sql URL encoding
# ---------------------------------------------------------------------------


def test_build_sql_plain_select_round_trips():
    sql = "SELECT accountid FROM account"
    assert _sql_param(_build(sql)) == sql


def test_build_sql_forward_slash_is_percent_encoded():
    sql = "SELECT accountid FROM account WHERE name = 'a/b'"
    url = _build(sql)
    assert "a/b" not in url.split("?", 1)[1]
    assert "%2F" in url


def test_build_sql_space_is_percent_encoded():
    sql = "SELECT accountid FROM account WHERE name = 'hello world'"
    assert " " not in _build(sql).split("?", 1)[1]


def test_build_sql_ampersand_is_percent_encoded():
    sql = "SELECT accountid FROM account WHERE name = 'a&b'"
    url = _build(sql)
    assert "name=a&b" not in url.split("?", 1)[1]
    assert "%26" in url


def test_build_sql_equals_in_value_is_percent_encoded():
    sql = "SELECT accountid FROM account WHERE name = 'x=y'"
    assert "%3D" in _build(sql)


def test_build_sql_decoded_param_matches_input():
    sql = "SELECT accountid, name FROM account WHERE statecode = 0"
    assert _sql_param(_build(sql)) == sql


# ---------------------------------------------------------------------------
# _query_sql pagination
# ---------------------------------------------------------------------------


def _make_response(rows, next_link=None):
    """Build a mock HTTP response whose .json() returns an OData page."""
    body = {"value": rows}
    if next_link:
        body["@odata.nextLink"] = next_link
    resp = MagicMock()
    resp.json.return_value = body
    return resp


def _query_sql_client():
    """Return a bare _ODataClient suitable for _query_sql patching."""
    client = object.__new__(_ODataClient)
    client.api = "https://org.crm.dynamics.com/api/data/v9.2"
    return client


def test_query_sql_single_page_returns_all_rows():
    client = _query_sql_client()
    page = _make_response([{"id": 1}, {"id": 2}])
    with (
        patch.object(client, "_execute_raw", return_value=page),
        patch.object(client, "_build_sql", return_value=MagicMock()),
    ):
        result = client._query_sql("SELECT id FROM account")
    assert result == [{"id": 1}, {"id": 2}]


def test_query_sql_follows_next_link():
    client = _query_sql_client()
    page1 = _make_response([{"id": i} for i in range(5000)], next_link="https://org.example/page2")
    page2 = _make_response([{"id": i} for i in range(5000, 6000)])

    mock_request_resp = MagicMock()
    mock_request_resp.json.return_value = page2.json.return_value

    with (
        patch.object(client, "_execute_raw", return_value=page1),
        patch.object(client, "_build_sql", return_value=MagicMock()),
        patch.object(client, "_request", return_value=mock_request_resp) as mock_req,
    ):
        result = client._query_sql("SELECT id FROM account")

    assert len(result) == 6000
    mock_req.assert_called_once_with("get", "https://org.example/page2")


def test_query_sql_follows_odata_next_link_variant():
    """Older OData format uses 'odata.nextLink' without the @ prefix."""
    client = _query_sql_client()
    page1_body = {"value": [{"id": 1}], "odata.nextLink": "https://org.example/page2"}
    page2_body = {"value": [{"id": 2}]}

    resp1 = MagicMock()
    resp1.json.return_value = page1_body
    resp2 = MagicMock()
    resp2.json.return_value = page2_body

    with (
        patch.object(client, "_execute_raw", return_value=resp1),
        patch.object(client, "_build_sql", return_value=MagicMock()),
        patch.object(client, "_request", return_value=resp2),
    ):
        result = client._query_sql("SELECT id FROM account")

    assert result == [{"id": 1}, {"id": 2}]


def test_query_sql_multipage_collects_all():
    """Three pages: verifies the loop continues past the second page."""
    client = _query_sql_client()
    page1 = _make_response([{"id": 1}], next_link="https://org.example/p2")
    page2_body = {"value": [{"id": 2}], "@odata.nextLink": "https://org.example/p3"}
    page3_body = {"value": [{"id": 3}]}

    resp2 = MagicMock()
    resp2.json.return_value = page2_body
    resp3 = MagicMock()
    resp3.json.return_value = page3_body

    with (
        patch.object(client, "_execute_raw", return_value=page1),
        patch.object(client, "_build_sql", return_value=MagicMock()),
        patch.object(client, "_request", side_effect=[resp2, resp3]),
    ):
        result = client._query_sql("SELECT id FROM account")

    assert result == [{"id": 1}, {"id": 2}, {"id": 3}]


def test_query_sql_mid_pagination_error_warns_and_returns_partial():
    """A failing page mid-pagination emits a RuntimeWarning and returns rows collected so far."""
    client = _query_sql_client()
    page1 = _make_response([{"id": 1}], next_link="https://org.example/p2")

    bad_resp = MagicMock()
    bad_resp.json.side_effect = ValueError("not JSON")

    with (
        patch.object(client, "_execute_raw", return_value=page1),
        patch.object(client, "_build_sql", return_value=MagicMock()),
        patch.object(client, "_request", return_value=bad_resp),
    ):
        with pytest.warns(RuntimeWarning, match="pagination stopped"):
            result = client._query_sql("SELECT id FROM account")

    assert result == [{"id": 1}]


def test_query_sql_repeated_next_link_warns_and_stops():
    """If the server keeps returning the same @odata.nextLink a RuntimeWarning is emitted and
    the loop stops without running forever."""
    client = _query_sql_client()
    # Both pages return the same next_link — simulates a server that re-executes the SQL
    repeating_body = {"value": [{"id": 1}], "@odata.nextLink": "https://org.example/page2"}

    resp1 = MagicMock()
    resp1.json.return_value = repeating_body
    resp2 = MagicMock()
    resp2.json.return_value = repeating_body  # same link again

    with (
        patch.object(client, "_execute_raw", return_value=resp1),
        patch.object(client, "_build_sql", return_value=MagicMock()),
        patch.object(client, "_request", return_value=resp2) as mock_req,
    ):
        with pytest.warns(RuntimeWarning, match="pagination stopped"):
            result = client._query_sql("SELECT id FROM account")

    # fetched page2 once, then detected the cycle and stopped
    mock_req.assert_called_once_with("get", "https://org.example/page2")
    assert result == [{"id": 1}, {"id": 1}]


def test_query_sql_empty_page_stops_pagination():
    """If a page returns an empty value array (but includes @odata.nextLink), stop — no infinite loop."""
    client = _query_sql_client()
    page1 = _make_response([{"id": 1}], next_link="https://org.example/p2")
    empty_page_body = {"value": [], "@odata.nextLink": "https://org.example/p3"}

    resp2 = MagicMock()
    resp2.json.return_value = empty_page_body

    with (
        patch.object(client, "_execute_raw", return_value=page1),
        patch.object(client, "_build_sql", return_value=MagicMock()),
        patch.object(client, "_request", return_value=resp2) as mock_req,
    ):
        result = client._query_sql("SELECT id FROM account")

    assert result == [{"id": 1}]
    mock_req.assert_called_once()  # fetched p2, did not follow p3


def test_query_sql_non_string_next_link_stops_pagination():
    """A non-string @odata.nextLink value (e.g. a boolean) does not trigger a request."""
    client = _query_sql_client()
    page1_body = {"value": [{"id": 1}], "@odata.nextLink": True}

    resp1 = MagicMock()
    resp1.json.return_value = page1_body

    with (
        patch.object(client, "_execute_raw", return_value=resp1),
        patch.object(client, "_build_sql", return_value=MagicMock()),
        patch.object(client, "_request") as mock_req,
    ):
        result = client._query_sql("SELECT id FROM account")

    assert result == [{"id": 1}]
    mock_req.assert_not_called()


def test_query_sql_stuck_pagingcookie_warns_and_stops():
    """When the server returns the same pagingcookie on successive pages (server-side bug),
    pagination must stop and a RuntimeWarning must be emitted."""
    import warnings
    from urllib.parse import quote as _url_quote

    client = _query_sql_client()

    # Build a next_link that carries a recognisable pagingcookie.
    # The pagingcookie attribute value is itself URL-encoded inside the skiptoken
    # (matching the double-encoding the real Dataverse server produces).
    inner_cookie = "%3ccookie%20page%3d%221%22%3e%3caccountid%20last%3d%22%7bAAA%7d%22%20first%3d%22%7bBBB%7d%22%20%2f%3e%3c%2fcookie%3e"
    skiptoken_xml = f'<cookie pagenumber="2" pagingcookie="{inner_cookie}" istracking="False" />'
    encoded_skiptoken = _url_quote(skiptoken_xml)
    next_link_p2 = f"https://org.example/api/data/v9.2?$skiptoken={encoded_skiptoken}"
    next_link_p3 = f"https://org.example/api/data/v9.2?$skiptoken={encoded_skiptoken}&extra=1"

    page1_body = {"value": [{"id": 1}], "@odata.nextLink": next_link_p2}
    # Page 2 carries a *different* URL but the same pagingcookie content → server bug
    page2_body = {"value": [{"id": 2}], "@odata.nextLink": next_link_p3}

    resp1 = MagicMock()
    resp1.json.return_value = page1_body
    resp2 = MagicMock()
    resp2.json.return_value = page2_body

    with (
        patch.object(client, "_execute_raw", return_value=resp1),
        patch.object(client, "_build_sql", return_value=MagicMock()),
        patch.object(client, "_request", return_value=resp2) as mock_req,
    ):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = client._query_sql("SELECT id FROM account")

    # Page 2 was fetched; page 3 was not (cookie repeat detected after page 2)
    mock_req.assert_called_once_with("get", next_link_p2)
    assert result == [{"id": 1}, {"id": 2}]

    assert len(caught) == 1
    w = caught[0]
    assert issubclass(w.category, RuntimeWarning)
    assert "pagingcookie" in str(w.message).lower()
    assert "server" in str(w.message).lower()


# ---------------------------------------------------------------------------
# _extract_pagingcookie unit tests
# ---------------------------------------------------------------------------


def _make_next_link(pagingcookie_inner: str, pagenumber: int = 2) -> str:
    """Build a double-encoded nextLink URL matching the real Dataverse format."""
    from urllib.parse import quote as _url_quote

    skiptoken_xml = (
        f'<cookie pagenumber="{pagenumber}" ' f'pagingcookie="{pagingcookie_inner}" ' f'istracking="False" />'
    )
    return (
        f"https://org.example/api/data/v9.2?$sql=SELECT%20name%20FROM%20account&$skiptoken={_url_quote(skiptoken_xml)}"
    )


def test_extract_pagingcookie_returns_cookie_value():
    """Returns the pagingcookie attribute value from a well-formed nextLink."""
    inner = "%3ccookie%20page%3d%221%22%3e%3caccountid%20last%3d%22%7bAAA%7d%22%20first%3d%22%7bBBB%7d%22%20%2f%3e%3c%2fcookie%3e"
    url = _make_next_link(inner)
    result = _extract_pagingcookie(url)
    assert result == inner


def test_extract_pagingcookie_no_skiptoken_returns_none():
    """Returns None when the URL has no $skiptoken parameter."""
    url = "https://org.example/api/data/v9.2?$sql=SELECT%20name%20FROM%20account"
    assert _extract_pagingcookie(url) is None


def test_extract_pagingcookie_empty_skiptoken_returns_none():
    """Returns None when $skiptoken is present but empty."""
    url = "https://org.example/api/data/v9.2?$sql=SELECT%20name%20FROM%20account&$skiptoken="
    assert _extract_pagingcookie(url) is None


def test_extract_pagingcookie_no_pagingcookie_attr_returns_none():
    """Returns None when $skiptoken exists but contains no pagingcookie attribute."""
    from urllib.parse import quote as _url_quote

    skiptoken_xml = '<cookie pagenumber="2" istracking="False" />'
    url = f"https://org.example/api/data/v9.2?$skiptoken={_url_quote(skiptoken_xml)}"
    assert _extract_pagingcookie(url) is None


def test_extract_pagingcookie_different_pagenumbers_same_cookie():
    """Two URLs with different pagenumbers but the same pagingcookie produce equal return values."""
    inner = "%3ccookie%20page%3d%221%22%3e%3caccountid%20last%3d%22%7bAAA%7d%22%20first%3d%22%7bBBB%7d%22%20%2f%3e%3c%2fcookie%3e"
    url_p2 = _make_next_link(inner, pagenumber=2)
    url_p3 = _make_next_link(inner, pagenumber=3)
    assert _extract_pagingcookie(url_p2) == _extract_pagingcookie(url_p3)


def test_extract_pagingcookie_different_cookies_not_equal():
    """Two URLs with different pagingcookie GUIDs produce different return values."""
    inner_1 = "%3ccookie%20page%3d%221%22%3e%3caccountid%20last%3d%22%7bAAA%7d%22%20first%3d%22%7bBBB%7d%22%20%2f%3e%3c%2fcookie%3e"
    inner_2 = "%3ccookie%20page%3d%222%22%3e%3caccountid%20last%3d%22%7bCCC%7d%22%20first%3d%22%7bDDD%7d%22%20%2f%3e%3c%2fcookie%3e"
    url_p2 = _make_next_link(inner_1, pagenumber=2)
    url_p3 = _make_next_link(inner_2, pagenumber=3)
    assert _extract_pagingcookie(url_p2) != _extract_pagingcookie(url_p3)


def test_extract_pagingcookie_malformed_url_returns_none():
    """Returns None gracefully when given a non-URL string."""
    assert _extract_pagingcookie("not a url at all !!!") is None


def test_extract_pagingcookie_exception_returns_none():
    """Returns None when an unexpected exception is raised during URL parsing (except branch)."""
    with patch("PowerPlatform.Dataverse.data._odata_base.urlparse", side_effect=RuntimeError("boom")):
        assert _extract_pagingcookie("https://org.example/?$skiptoken=x") is None


def test_query_sql_request_exception_warns_and_returns_partial():
    """When _request raises an exception mid-pagination a RuntimeWarning is emitted and
    the rows collected so far are returned."""
    client = _query_sql_client()
    page1 = _make_response([{"id": 1}], next_link="https://org.example/p2")

    with (
        patch.object(client, "_execute_raw", return_value=page1),
        patch.object(client, "_build_sql", return_value=MagicMock()),
        patch.object(client, "_request", side_effect=ConnectionError("network timeout")),
    ):
        with pytest.warns(RuntimeWarning, match="pagination stopped"):
            result = client._query_sql("SELECT id FROM account")

    assert result == [{"id": 1}]


def test_query_sql_non_dict_page_body_stops_pagination():
    """When a pagination response contains valid JSON that is not a dict (e.g. a list),
    pagination stops silently and the rows collected so far are returned."""
    client = _query_sql_client()
    page1 = _make_response([{"id": 1}], next_link="https://org.example/p2")

    bad_resp = MagicMock()
    bad_resp.json.return_value = [{"id": 2}]  # a list, not a dict

    with (
        patch.object(client, "_execute_raw", return_value=page1),
        patch.object(client, "_build_sql", return_value=MagicMock()),
        patch.object(client, "_request", return_value=bad_resp) as mock_req,
    ):
        result = client._query_sql("SELECT id FROM account")

    mock_req.assert_called_once_with("get", "https://org.example/p2")
    assert result == [{"id": 1}]
