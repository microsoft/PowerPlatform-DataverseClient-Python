# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for SELECT * auto-expansion in _query_sql."""

import pytest
from unittest.mock import MagicMock

from PowerPlatform.Dataverse.data._odata import _ODataClient


class DummyAuth:
    def _acquire_token(self, scope):
        class T:
            access_token = "x"

        return T()


def _client():
    return _ODataClient(DummyAuth(), "https://org.example", None)


# --- _expand_select_star ---


class TestExpandSelectStar:
    """Tests for _ODataClient._expand_select_star."""

    def test_no_star_unchanged(self):
        c = _client()
        c._list_columns = MagicMock()
        sql = "SELECT name, revenue FROM account WHERE statecode = 0"
        result = c._expand_select_star(sql, "account")
        assert result == sql
        c._list_columns.assert_not_called()

    def test_basic_select_star(self):
        c = _client()
        c._list_columns = MagicMock(
            return_value=[
                {"LogicalName": "name"},
                {"LogicalName": "accountid"},
                {"LogicalName": "revenue"},
            ]
        )
        sql = "SELECT * FROM account"
        result = c._expand_select_star(sql, "account")
        assert "SELECT accountid, name, revenue FROM" in result
        assert "*" not in result

    def test_select_star_with_top(self):
        c = _client()
        c._list_columns = MagicMock(
            return_value=[
                {"LogicalName": "name"},
                {"LogicalName": "accountid"},
            ]
        )
        sql = "SELECT TOP 10 * FROM account"
        result = c._expand_select_star(sql, "account")
        assert "TOP 10" in result
        assert "accountid, name" in result
        assert "*" not in result

    def test_select_star_with_distinct(self):
        c = _client()
        c._list_columns = MagicMock(
            return_value=[
                {"LogicalName": "name"},
            ]
        )
        sql = "SELECT DISTINCT * FROM account"
        result = c._expand_select_star(sql, "account")
        assert "DISTINCT" in result
        assert "name" in result
        assert "*" not in result

    def test_select_star_with_distinct_top(self):
        c = _client()
        c._list_columns = MagicMock(
            return_value=[
                {"LogicalName": "name"},
                {"LogicalName": "accountid"},
            ]
        )
        sql = "SELECT DISTINCT TOP 5 * FROM account"
        result = c._expand_select_star(sql, "account")
        assert "DISTINCT" in result
        assert "TOP 5" in result
        assert "accountid, name" in result
        assert "*" not in result

    def test_star_in_count_not_expanded(self):
        c = _client()
        c._list_columns = MagicMock()
        sql = "SELECT COUNT(*) FROM account"
        result = c._expand_select_star(sql, "account")
        # COUNT(*) should NOT trigger expansion since the * is inside parens
        assert result == sql
        c._list_columns.assert_not_called()

    def test_skips_virtual_columns(self):
        c = _client()
        c._list_columns = MagicMock(
            return_value=[
                {"LogicalName": "name"},
            ]
        )
        sql = "SELECT * FROM account"
        c._expand_select_star(sql, "account")
        c._list_columns.assert_called_once_with(
            "account",
            select=["LogicalName"],
            filter="AttributeType ne 'Virtual'",
        )

    def test_empty_columns_unchanged(self):
        c = _client()
        c._list_columns = MagicMock(return_value=[])
        sql = "SELECT * FROM account"
        result = c._expand_select_star(sql, "account")
        assert result == sql

    def test_where_clause_preserved(self):
        c = _client()
        c._list_columns = MagicMock(
            return_value=[
                {"LogicalName": "name"},
                {"LogicalName": "accountid"},
            ]
        )
        sql = "SELECT * FROM account WHERE statecode = 0"
        result = c._expand_select_star(sql, "account")
        assert "WHERE statecode = 0" in result
        assert "*" not in result

    def test_case_insensitive_select(self):
        c = _client()
        c._list_columns = MagicMock(
            return_value=[
                {"LogicalName": "name"},
            ]
        )
        sql = "select * from account"
        result = c._expand_select_star(sql, "account")
        assert "name" in result
        assert "*" not in result


# --- _SELECT_STAR_RE pattern tests ---


class TestSelectStarRegex:
    """Verify the regex correctly identifies SELECT * patterns."""

    def test_matches_simple_star(self):
        assert _ODataClient._SELECT_STAR_RE.search("SELECT * FROM account")

    def test_matches_star_with_top(self):
        assert _ODataClient._SELECT_STAR_RE.search("SELECT TOP 10 * FROM account")

    def test_matches_star_with_distinct(self):
        assert _ODataClient._SELECT_STAR_RE.search("SELECT DISTINCT * FROM account")

    def test_matches_star_with_distinct_top(self):
        assert _ODataClient._SELECT_STAR_RE.search("SELECT DISTINCT TOP 50 * FROM account")

    def test_no_match_count_star(self):
        assert not _ODataClient._SELECT_STAR_RE.search("SELECT COUNT(*) FROM account")

    def test_no_match_named_columns(self):
        assert not _ODataClient._SELECT_STAR_RE.search("SELECT name, revenue FROM account")

    def test_matches_top_percent(self):
        assert _ODataClient._SELECT_STAR_RE.search("SELECT TOP 50 PERCENT * FROM account")

    def test_case_insensitive(self):
        assert _ODataClient._SELECT_STAR_RE.search("select * from account")

    def test_no_match_alias_star(self):
        # a.* is not SELECT * -- it's a table-qualified wildcard not at toplevel
        assert not _ODataClient._SELECT_STAR_RE.search("SELECT a.name, b.* FROM account a")


# --- Integration: _query_sql calls _expand_select_star ---


@pytest.mark.filterwarnings("ignore::UserWarning")
class TestQuerySqlSelectStarIntegration:
    """Verify _query_sql calls _expand_select_star when SELECT * is used."""

    def test_query_sql_expands_select_star(self):
        c = _client()
        c._entity_set_from_schema_name = MagicMock(return_value="accounts")
        c._list_columns = MagicMock(
            return_value=[
                {"LogicalName": "name"},
                {"LogicalName": "accountid"},
            ]
        )
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": [{"name": "Contoso", "accountid": "1"}]}
        mock_response.status_code = 200
        c._request = MagicMock(return_value=mock_response)

        rows = c._query_sql("SELECT * FROM account")

        # Verify _list_columns was called (SELECT * expansion)
        c._list_columns.assert_called_once()
        # Verify the SQL sent to server has explicit columns, not *
        call_args = c._request.call_args
        sent_sql = call_args[1]["params"]["sql"] if "params" in call_args[1] else call_args[0][2]["sql"]
        assert "*" not in sent_sql or "COUNT(*)" in sent_sql
        assert "accountid" in sent_sql
        assert "name" in sent_sql
        assert len(rows) == 1

    def test_query_sql_skips_expansion_for_named_columns(self):
        c = _client()
        c._entity_set_from_schema_name = MagicMock(return_value="accounts")
        c._list_columns = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": [{"name": "Contoso"}]}
        mock_response.status_code = 200
        c._request = MagicMock(return_value=mock_response)

        c._query_sql("SELECT name FROM account")

        # _list_columns should NOT be called for explicit column queries
        c._list_columns.assert_not_called()

    def test_query_sql_skips_expansion_for_count_star(self):
        c = _client()
        c._entity_set_from_schema_name = MagicMock(return_value="accounts")
        c._list_columns = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": [{"cnt": 42}]}
        mock_response.status_code = 200
        c._request = MagicMock(return_value=mock_response)

        c._query_sql("SELECT COUNT(*) FROM account")

        c._list_columns.assert_not_called()

    def test_query_sql_with_join_no_star_no_expansion(self):
        c = _client()
        c._entity_set_from_schema_name = MagicMock(return_value="accounts")
        c._list_columns = MagicMock()
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": []}
        mock_response.status_code = 200
        c._request = MagicMock(return_value=mock_response)

        c._query_sql("SELECT a.name, c.fullname FROM account a " "JOIN contact c ON a.accountid = c.parentcustomerid")

        c._list_columns.assert_not_called()
