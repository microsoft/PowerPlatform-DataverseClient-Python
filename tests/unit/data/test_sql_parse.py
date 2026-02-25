# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlparse

import pytest
from PowerPlatform.Dataverse.data._odata import _ODataClient


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
