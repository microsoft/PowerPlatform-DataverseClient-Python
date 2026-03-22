# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import pytest
from PowerPlatform.Dataverse.data._odata import _ODataClient


class DummyAuth:
    def _acquire_token(self, scope):
        class T:
            access_token = "x"  # no real token needed for parsing tests

        return T()


def _client():
    return _ODataClient(DummyAuth(), "https://org.example", None)


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


# --- JOIN syntax (multi-table SQL) ---


def test_inner_join_extracts_first_table():
    c = _client()
    sql = "SELECT a.name, c.fullname FROM account a " "INNER JOIN contact c ON a.accountid = c.parentcustomerid"
    assert c._extract_logical_table(sql) == "account"


def test_left_join_extracts_first_table():
    c = _client()
    sql = "SELECT a.name FROM account a " "LEFT JOIN contact c ON a.accountid = c.parentcustomerid"
    assert c._extract_logical_table(sql) == "account"


def test_multi_join_extracts_first_table():
    c = _client()
    sql = (
        "SELECT a.name, c.fullname, o.name "
        "FROM account a "
        "JOIN contact c ON a.accountid = c.parentcustomerid "
        "JOIN opportunity o ON a.accountid = o.parentaccountid"
    )
    assert c._extract_logical_table(sql) == "account"


def test_join_with_alias():
    c = _client()
    sql = "SELECT t.name FROM account AS t JOIN contact c ON t.accountid = c.parentcustomerid"
    assert c._extract_logical_table(sql) == "account"


def test_table_alias_without_as():
    c = _client()
    sql = "SELECT a.name FROM account a WHERE a.statecode = 0"
    assert c._extract_logical_table(sql) == "account"


def test_table_alias_with_as():
    c = _client()
    sql = "SELECT a.name FROM account AS a WHERE a.statecode = 0"
    assert c._extract_logical_table(sql) == "account"


def test_custom_table_with_join():
    c = _client()
    sql = (
        "SELECT t.new_code, tk.new_title "
        "FROM new_sqldemotask tk "
        "INNER JOIN new_sqldemoteam t ON tk._new_teamid_value = t.new_sqldemoteamid"
    )
    assert c._extract_logical_table(sql) == "new_sqldemotask"


def test_aggregate_with_join():
    c = _client()
    sql = (
        "SELECT a.name, COUNT(c.contactid) as cnt "
        "FROM account a "
        "JOIN contact c ON a.accountid = c.parentcustomerid "
        "GROUP BY a.name"
    )
    assert c._extract_logical_table(sql) == "account"


def test_offset_fetch():
    c = _client()
    sql = "SELECT name FROM account " "ORDER BY name OFFSET 10 ROWS FETCH NEXT 5 ROWS ONLY"
    assert c._extract_logical_table(sql) == "account"


def test_polymorphic_owner_join():
    c = _client()
    sql = "SELECT a.name, su.fullname " "FROM account a " "JOIN systemuser su ON a._ownerid_value = su.systemuserid"
    assert c._extract_logical_table(sql) == "account"


def test_audit_trail_multi_join():
    c = _client()
    sql = (
        "SELECT a.name, creator.fullname, modifier.fullname "
        "FROM account a "
        "JOIN systemuser creator ON a._createdby_value = creator.systemuserid "
        "JOIN systemuser modifier ON a._modifiedby_value = modifier.systemuserid"
    )
    assert c._extract_logical_table(sql) == "account"


def test_select_star():
    c = _client()
    assert c._extract_logical_table("SELECT * FROM account") == "account"


def test_select_star_with_where():
    c = _client()
    assert c._extract_logical_table("SELECT * FROM account WHERE statecode = 0") == "account"


def test_distinct_top():
    c = _client()
    assert c._extract_logical_table("SELECT DISTINCT TOP 5 name FROM account") == "account"


def test_count_star():
    c = _client()
    assert c._extract_logical_table("SELECT COUNT(*) FROM account") == "account"
