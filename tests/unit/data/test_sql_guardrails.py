# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for SQL guardrails in _query_sql."""

import warnings

import pytest
from unittest.mock import MagicMock

from PowerPlatform.Dataverse.core.errors import ValidationError
from PowerPlatform.Dataverse.data._odata import _ODataClient


class DummyAuth:
    def _acquire_token(self, scope):
        class T:
            access_token = "x"

        return T()


def _client():
    return _ODataClient(DummyAuth(), "https://org.example", None)


# ===================================================================
# 1. Block write statements
# ===================================================================


class TestBlockWriteStatements:
    """Write SQL (INSERT/UPDATE/DELETE/DROP/etc.) must be blocked."""

    @pytest.mark.parametrize(
        "sql",
        [
            "DELETE FROM account WHERE name = 'test'",
            "UPDATE account SET name = 'hacked' WHERE 1=1",
            "INSERT INTO account (name) VALUES ('injected')",
            "DROP TABLE account",
            "TRUNCATE TABLE account",
            "ALTER TABLE account ADD hackedcol VARCHAR(100)",
            "CREATE TABLE hacked (id INT)",
            "EXEC sp_helptext 'account'",
            "GRANT SELECT ON account TO public",
            "REVOKE SELECT ON account FROM public",
            "BULK INSERT account FROM '/tmp/data.csv'",
        ],
    )
    def test_write_statement_raises(self, sql):
        c = _client()
        with pytest.raises(ValidationError, match="read-only"):
            c._sql_guardrails(sql)

    @pytest.mark.parametrize(
        "sql",
        [
            "delete FROM account",
            "  DELETE FROM account",
            "  update account SET x = 1",
            "\n\tINSERT INTO t (c) VALUES (1)",
        ],
    )
    def test_write_case_insensitive_and_whitespace(self, sql):
        c = _client()
        with pytest.raises(ValidationError):
            c._sql_guardrails(sql)

    def test_select_not_blocked(self):
        c = _client()
        # SELECT should pass through without raising
        result = c._sql_guardrails("SELECT TOP 10 name FROM account")
        assert "SELECT" in result

    def test_select_with_delete_in_where_not_blocked(self):
        c = _client()
        # "DELETE" appearing in a WHERE value should not trigger the guard
        result = c._sql_guardrails("SELECT TOP 10 name FROM account WHERE name = 'DELETE ME'")
        assert "SELECT" in result


# ===================================================================
# 2. Server enforces TOP 5000 (no client-side injection needed)
# ===================================================================


class TestNoTopInjection:
    """Verify the SDK does NOT inject TOP -- server handles the 5000 cap."""

    def test_no_top_passes_through_unchanged(self):
        c = _client()
        sql = "SELECT name FROM account"
        result = c._sql_guardrails(sql)
        assert result == sql
        assert "TOP" not in result

    def test_existing_top_not_modified(self):
        c = _client()
        result = c._sql_guardrails("SELECT TOP 100 name FROM account")
        assert "TOP 100" in result

    def test_offset_passes_through(self):
        c = _client()
        sql = "SELECT name FROM account ORDER BY name OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY"
        result = c._sql_guardrails(sql)
        assert result == sql

    def test_join_without_top_not_modified(self):
        c = _client()
        sql = "SELECT a.name, c.fullname FROM account a " "JOIN contact c ON a.accountid = c.parentcustomerid"
        result = c._sql_guardrails(sql)
        assert result == sql
        assert "TOP" not in result


# ===================================================================
# 3. Warn on leading-wildcard LIKE
# ===================================================================


class TestLeadingWildcardWarning:
    """LIKE '%...' patterns should emit a UserWarning."""

    def test_leading_wildcard_warns(self):
        c = _client()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            c._sql_guardrails("SELECT TOP 10 name FROM account WHERE name LIKE '%test'")
            like_warnings = [x for x in w if "leading-wildcard" in str(x.message).lower()]
            assert len(like_warnings) == 1

    def test_mid_wildcard_warns(self):
        c = _client()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            c._sql_guardrails("SELECT TOP 10 name FROM account WHERE name LIKE '%test%'")
            like_warnings = [x for x in w if "leading-wildcard" in str(x.message).lower()]
            assert len(like_warnings) == 1

    def test_trailing_wildcard_no_warning(self):
        c = _client()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            c._sql_guardrails("SELECT TOP 10 name FROM account WHERE name LIKE 'test%'")
            like_warnings = [x for x in w if "leading-wildcard" in str(x.message).lower()]
            assert len(like_warnings) == 0

    def test_no_like_no_warning(self):
        c = _client()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            c._sql_guardrails("SELECT TOP 10 name FROM account")
            like_warnings = [x for x in w if "wildcard" in str(x.message).lower()]
            assert len(like_warnings) == 0


# ===================================================================
# 4. Warn on implicit cross joins (server allows, SDK warns)
# ===================================================================


class TestImplicitCrossJoinWarning:
    """FROM a, b (comma syntax) should emit UserWarning (not error)."""

    def test_comma_join_warns(self):
        c = _client()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            c._sql_guardrails("SELECT TOP 10 a.name, c.fullname FROM account a, contact c")
            cross_warnings = [x for x in w if "cross join" in str(x.message).lower()]
            assert len(cross_warnings) == 1

    def test_explicit_join_no_warning(self):
        c = _client()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            c._sql_guardrails(
                "SELECT TOP 10 a.name FROM account a " "JOIN contact c ON a.accountid = c.parentcustomerid"
            )
            cross_warnings = [x for x in w if "cross join" in str(x.message).lower()]
            assert len(cross_warnings) == 0

    def test_single_table_no_warning(self):
        c = _client()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            c._sql_guardrails("SELECT TOP 10 name FROM account")
            cross_warnings = [x for x in w if "cross join" in str(x.message).lower()]
            assert len(cross_warnings) == 0


# ===================================================================
# 5. SELECT * with JOIN warning (from _expand_select_star)
# ===================================================================


class TestSelectStarJoinWarning:
    """SELECT * with JOIN should warn that only first table columns are used."""

    def test_select_star_with_join_warns(self):
        c = _client()
        c._list_columns = MagicMock(return_value=[{"LogicalName": "name"}, {"LogicalName": "accountid"}])
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            c._expand_select_star(
                "SELECT * FROM account a JOIN contact c ON a.accountid = c.parentcustomerid",
                "account",
            )
            join_warnings = [x for x in w if "JOIN" in str(x.message)]
            assert len(join_warnings) == 1
            assert "first table only" in str(join_warnings[0].message)

    def test_select_star_no_join_no_warning(self):
        c = _client()
        c._list_columns = MagicMock(return_value=[{"LogicalName": "name"}])
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            c._expand_select_star("SELECT * FROM account", "account")
            join_warnings = [x for x in w if "JOIN" in str(x.message)]
            assert len(join_warnings) == 0

    def test_no_star_with_join_no_warning(self):
        c = _client()
        c._list_columns = MagicMock()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            c._expand_select_star(
                "SELECT a.name, c.fullname FROM account a " "JOIN contact c ON a.accountid = c.parentcustomerid",
                "account",
            )
            # _list_columns not called (no star), so no JOIN warning
            c._list_columns.assert_not_called()
            join_warnings = [x for x in w if "JOIN" in str(x.message)]
            assert len(join_warnings) == 0


# ===================================================================
# 6. Integration: _query_sql applies guardrails
# ===================================================================


class TestQuerySqlGuardrailIntegration:
    """Verify _query_sql applies guardrails before sending to server."""

    def test_write_blocked_before_server_call(self):
        c = _client()
        c._request = MagicMock()
        with pytest.raises(ValidationError, match="read-only"):
            c._query_sql("DELETE FROM account WHERE name = 'x'")
        c._request.assert_not_called()

    def test_no_top_injection_in_server_request(self):
        """Server manages the 5000 cap -- SDK should not inject TOP."""
        c = _client()
        c._entity_set_from_schema_name = MagicMock(return_value="accounts")
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": []}
        mock_response.status_code = 200
        c._request = MagicMock(return_value=mock_response)

        c._query_sql("SELECT name FROM account")

        call_args = c._request.call_args
        sent_params = call_args[1].get("params", {})
        sent_sql = sent_params.get("sql", "")
        # SDK should NOT inject TOP 5000
        assert "TOP 5000" not in sent_sql
        assert sent_sql == "SELECT name FROM account"

    def test_explicit_top_preserved_in_server_request(self):
        c = _client()
        c._entity_set_from_schema_name = MagicMock(return_value="accounts")
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": []}
        mock_response.status_code = 200
        c._request = MagicMock(return_value=mock_response)

        c._query_sql("SELECT TOP 50 name FROM account")

        call_args = c._request.call_args
        sent_params = call_args[1].get("params", {})
        sent_sql = sent_params.get("sql", "")
        assert "TOP 50" in sent_sql
