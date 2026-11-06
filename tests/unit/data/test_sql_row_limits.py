# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""SQL limits apply to the combined result, not to each response page."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from PowerPlatform.Dataverse.aio.data._async_odata import _AsyncODataClient
from PowerPlatform.Dataverse.data._odata import _ODataClient
from PowerPlatform.Dataverse.data._odata_base import _ODataBase


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT TOP 3 name FROM account", 3),
        ("select distinct top ( 3 ) name from account", 3),
        ("SELECT ALL TOP 0 name FROM account", 0),
        ("SELECT TOP(12) [name] FROM account", 12),
        ("/* prefix */ SELECT TOP /* count */ 2 name FROM account", 2),
        ("-- prefix\nSELECT TOP 2 name FROM account", 2),
        ("SELECT name FROM account ORDER BY name OFFSET 5 ROWS FETCH NEXT 3 ROWS ONLY", 3),
        ("select name from account order by name offset 0 row fetch first 1 row only", 1),
        ("SELECT name FROM account ORDER BY name OFFSET 0 ROWS FETCH NEXT 3 ROWS ONLY -- end", 3),
        ("SELECT name FROM account", None),
        ("/* outer /* inner */ SELECT TOP 1 name FROM account -- */\nSELECT name FROM account", None),
        ("/* outer /* inner */ end */ SELECT TOP 1 name FROM account", None),
        ("SELECT name FROM account ORDER BY name OFFSET 5 ROWS", None),
        ("SELECT 'TOP 1' AS name FROM account", None),
        ("SELECT [TOP 1] FROM account", None),
        ('SELECT "TOP 1" FROM account', None),
        ("SELECT name FROM account WHERE name = 'OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY'", None),
        ("SELECT name FROM account -- OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY", None),
        ("SELECT name FROM account /* OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY */", None),
        ("SELECT 'it''s -- TOP 1' AS name FROM account", None),
        ("SELECT [a]] OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY] FROM account", None),
        ("SELECT TOP 10 PERCENT name FROM account", None),
        ("SELECT TOP (10) WITH TIES name FROM account ORDER BY name", None),
        ("SELECT TOP (10) PERCENT WITH TIES name FROM account ORDER BY name", None),
        ("SELECT TOP (@limit) name FROM account", None),
        ("SELECT TOP (1 + 2) name FROM account", None),
        ("SELECT TOP 1.5 name FROM account", None),
        ("SELECT name FROM account ORDER BY name OFFSET 0 ROWS FETCH NEXT @limit ROWS ONLY", None),
    ],
)
def test_sql_row_limit(sql, expected):
    assert _ODataBase._sql_row_limit(sql) == expected


@pytest.fixture(params=[False, True], ids=["sync", "async"])
def sql_client(request):
    asynchronous = request.param
    client = object.__new__(_AsyncODataClient if asynchronous else _ODataClient)
    client.api = "https://org.example/api/data/v9.2"
    return client, asynchronous


def _response(body):
    response = MagicMock()
    response.json.return_value = body
    return response


async def _run(sql_client, sql, pages):
    client, asynchronous = sql_client
    response_mock = AsyncMock if asynchronous else MagicMock
    with (
        patch.object(client, "_execute_raw", response_mock(return_value=_response(pages[0]))) as first,
        patch.object(client, "_build_sql", return_value=MagicMock()) as build,
        patch.object(
            client, "_request", response_mock(side_effect=[_response(page) for page in pages[1:]])
        ) as following,
    ):
        result = await client._query_sql(sql) if asynchronous else client._query_sql(sql)
    build.assert_called_once_with(sql)
    first.assert_called_once()
    return result, following


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT TOP 1 id FROM account",
        "SELECT DISTINCT TOP (1) id FROM account",
        "SELECT id FROM account ORDER BY id OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY",
        "SELECT id FROM account ORDER BY id OFFSET 7 ROWS FETCH NEXT 1 ROWS ONLY",
    ],
)
async def test_limit_stops_before_next_page(sql_client, sql):
    rows, following = await _run(sql_client, sql, [{"value": [{"id": 8}], "@odata.nextLink": "https://org.example/p2"}])
    assert rows == [{"id": 8}]
    following.assert_not_called()


@pytest.mark.parametrize("link_key", ["@odata.nextLink", "odata.nextLink"])
async def test_limit_spans_pages_and_trims_final_page(sql_client, link_key):
    rows, following = await _run(
        sql_client,
        "SELECT TOP 3 id FROM account",
        [
            {"value": [{"id": 1}], link_key: "https://org.example/p2"},
            {"value": [{"id": 2}, None, {"id": 3}, {"id": 4}], link_key: "https://org.example/p3"},
        ],
    )
    assert rows == [{"id": 1}, {"id": 2}, {"id": 3}]
    following.assert_called_once_with("get", "https://org.example/p2")


@pytest.mark.parametrize("bare_list", [False, True])
async def test_oversized_first_page_is_trimmed(sql_client, bare_list):
    body = [{"id": 1}, None, {"id": 2}]
    rows, following = await _run(sql_client, "SELECT TOP 1 id FROM account", [body if bare_list else {"value": body}])
    assert rows == [{"id": 1}]
    following.assert_not_called()


async def test_top_zero_keeps_initial_request_but_returns_no_rows(sql_client):
    rows, following = await _run(
        sql_client,
        "SELECT TOP 0 id FROM account",
        [{"value": [{"id": 1}], "@odata.nextLink": "https://org.example/p2"}],
    )
    assert rows == []
    following.assert_not_called()


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT id FROM account",
        "SELECT id FROM account WHERE name = 'TOP 1'",
        "/* outer /* inner */ SELECT TOP 1 id FROM account -- */\nSELECT id FROM account",
        "SELECT id FROM account WHERE name = 'OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY'",
        "SELECT TOP 10 PERCENT id FROM account",
        "SELECT TOP 1 WITH TIES id FROM account ORDER BY id",
    ],
)
async def test_unbounded_queries_keep_pagination(sql_client, sql):
    rows, following = await _run(
        sql_client,
        sql,
        [{"value": [{"id": 1}], "@odata.nextLink": "https://org.example/p2"}, {"value": [{"id": 2}]}],
    )
    assert rows == [{"id": 1}, {"id": 2}]
    following.assert_called_once_with("get", "https://org.example/p2")


async def test_limit_keeps_cycle_protection(sql_client):
    page = {"value": [{"id": 1}], "@odata.nextLink": "https://org.example/p2"}
    with pytest.warns(RuntimeWarning, match="same nextLink"):
        rows, following = await _run(sql_client, "SELECT TOP 5 id FROM account", [page, page])
    assert rows == [{"id": 1}, {"id": 1}]
    following.assert_called_once()


async def test_short_result_is_not_padded(sql_client):
    rows, following = await _run(sql_client, "SELECT TOP 5 id FROM account", [{"value": [{"id": 1}]}])
    assert rows == [{"id": 1}]
    following.assert_not_called()
