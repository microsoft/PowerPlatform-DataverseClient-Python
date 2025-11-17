# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import pytest
from PowerPlatform.Dataverse.data.odata import ODataClient


class DummyAuth:
    def acquire_token(self, scope):
        class T:
            access_token = "x"  # no real token needed for parsing tests

        return T()


def _client():
    return ODataClient(DummyAuth(), "https://org.example", None)


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
