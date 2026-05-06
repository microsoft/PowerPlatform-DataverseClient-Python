# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Phase 4 GA regression tests.

Covers:
- fetch_xml(): basic, pagination, missing-entity-element error
- sql_select / sql_join / sql_joins raise AttributeError (removed at GA)
- odata_select / odata_expand / odata_bind emit DeprecationWarning (deprecated at GA)
- sql_columns / odata_expands / sql() emit zero DeprecationWarning (still GA-clean)
"""

import unittest
import warnings
import xml.etree.ElementTree as ET
from unittest.mock import MagicMock, patch

from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.models.record import QueryResult, Record


def _make_client():
    cred = MagicMock(spec=TokenCredential)
    from PowerPlatform.Dataverse.client import DataverseClient

    client = DataverseClient("https://example.crm.dynamics.com", cred)
    client._odata = MagicMock()
    client._odata._entity_set_from_schema_name = MagicMock(side_effect=lambda t: t + "s")
    client._odata.api = "https://example.crm.dynamics.com/api/data/v9.2"
    return client


# ---------------------------------------------------------------------------
# fetch_xml()
# ---------------------------------------------------------------------------


class TestFetchXml(unittest.TestCase):

    def setUp(self):
        self.client = _make_client()

    def _fetch_xml(self, entity="account"):
        return f"""<fetch top="5"><entity name="{entity}"><attribute name="name"/></entity></fetch>"""

    def _mock_response(self, records, more=False, cookie=""):
        resp = MagicMock()
        payload = {"value": records}
        if more:
            payload["@Microsoft.Dynamics.CRM.morerecords"] = True
            payload["@Microsoft.Dynamics.CRM.fetchxmlpagingcookie"] = cookie
        else:
            payload["@Microsoft.Dynamics.CRM.morerecords"] = False
        resp.json.return_value = payload
        return resp

    def test_fetch_xml_inert_no_http_request(self):
        """fetch_xml() alone must not fire any HTTP request."""
        from PowerPlatform.Dataverse.models.fetch_xml_query import FetchXmlQuery

        query = self.client.query.fetch_xml(self._fetch_xml())
        self.assertIsInstance(query, FetchXmlQuery)
        self.client._odata._request.assert_not_called()

    def test_basic_returns_query_result(self):
        self.client._odata._request.return_value = self._mock_response([{"name": "Contoso", "accountid": "1"}])
        result = self.client.query.fetch_xml(self._fetch_xml()).execute()
        self.assertIsInstance(result, QueryResult)

    def test_basic_record_count(self):
        self.client._odata._request.return_value = self._mock_response([{"name": "A"}, {"name": "B"}])
        result = self.client.query.fetch_xml(self._fetch_xml()).execute()
        self.assertEqual(len(result), 2)

    def test_record_values_accessible(self):
        self.client._odata._request.return_value = self._mock_response([{"name": "Contoso", "accountid": "abc-123"}])
        result = self.client.query.fetch_xml(self._fetch_xml()).execute()
        self.assertEqual(result.first()["name"], "Contoso")

    def test_empty_result_returns_empty_query_result(self):
        self.client._odata._request.return_value = self._mock_response([])
        result = self.client.query.fetch_xml(self._fetch_xml()).execute()
        self.assertIsInstance(result, QueryResult)
        self.assertEqual(len(result), 0)
        self.assertFalse(result)

    def test_pagination_fetches_all_pages(self):
        """execute_pages() drives the HTTP loop; each page yields one QueryResult."""
        cookie_raw = "%25253Cpagingcookie%252520pagingcookie%25253D%252522"
        page1 = self._mock_response([{"name": "A"}], more=True, cookie=cookie_raw)
        page2 = self._mock_response([{"name": "B"}], more=False)
        self.client._odata._request.side_effect = [page1, page2]

        pages = list(self.client.query.fetch_xml(self._fetch_xml()).execute_pages())
        self.assertEqual(len(pages), 2)
        self.assertEqual(self.client._odata._request.call_count, 2)

    def test_pagination_second_request_includes_page_and_cookie(self):
        """execute_pages() injects the decoded paging cookie into the second request."""
        cookie_raw = "%25253Cpagingcookie%252520test%25253D%252522"
        page1 = self._mock_response([{"name": "A"}], more=True, cookie=cookie_raw)
        page2 = self._mock_response([{"name": "B"}], more=False)
        self.client._odata._request.side_effect = [page1, page2]

        list(self.client.query.fetch_xml(self._fetch_xml()).execute_pages())

        second_call_kwargs = self.client._odata._request.call_args_list[1]
        params = (
            second_call_kwargs.kwargs.get("params") or second_call_kwargs.args[2]
            if len(second_call_kwargs.args) > 2
            else {}
        )
        if not params:
            params = second_call_kwargs[1].get("params", {})
        xml_sent = params.get("fetchXml", "")
        fetch_el = ET.fromstring(xml_sent)
        self.assertEqual(fetch_el.get("page"), "2")
        self.assertIsNotNone(fetch_el.get("paging-cookie"))

    def test_missing_entity_element_raises_value_error(self):
        with self.assertRaises(ValueError) as ctx:
            self.client.query.fetch_xml("<fetch top='5'></fetch>")
        self.assertIn("entity", str(ctx.exception).lower())

    def test_entity_missing_name_attr_raises_value_error(self):
        with self.assertRaises(ValueError) as ctx:
            self.client.query.fetch_xml("<fetch><entity></entity></fetch>")
        self.assertIn("name", str(ctx.exception).lower())

    def test_entity_set_resolved_from_entity_name(self):
        self.client._odata._request.return_value = self._mock_response([])
        self.client.query.fetch_xml(self._fetch_xml("account")).execute()
        self.client._odata._entity_set_from_schema_name.assert_called_with("account")

    def test_request_uses_prefer_header(self):
        self.client._odata._request.return_value = self._mock_response([])
        self.client.query.fetch_xml(self._fetch_xml()).execute()
        call_kwargs = self.client._odata._request.call_args
        headers = call_kwargs.kwargs.get("headers", {})
        self.assertIn("Prefer", headers)
        self.assertIn("fetchxmlpagingcookie", headers["Prefer"])

    def test_result_iterable(self):
        self.client._odata._request.return_value = self._mock_response([{"name": "A"}, {"name": "B"}])
        result = self.client.query.fetch_xml(self._fetch_xml()).execute()
        names = [r["name"] for r in result]
        self.assertEqual(names, ["A", "B"])

    def test_result_to_dataframe(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not installed")
        self.client._odata._request.return_value = self._mock_response([{"name": "Contoso"}, {"name": "Fabrikam"}])
        result = self.client.query.fetch_xml(self._fetch_xml()).execute()
        df = result.to_dataframe()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)

    def test_no_deprecation_warning_emitted(self):
        self.client._odata._request.return_value = self._mock_response([])
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.client.query.fetch_xml(self._fetch_xml()).execute()
        deprecations = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(deprecations), 0, "fetch_xml().execute() should not emit DeprecationWarning")

    def test_execute_pages_returns_iterator_of_query_result(self):
        """execute_pages() yields QueryResult objects, one per HTTP page."""
        cookie_raw = "%25253Cpagingcookie%252520pagingcookie%25253D%252522"
        page1 = self._mock_response([{"name": "A"}], more=True, cookie=cookie_raw)
        page2 = self._mock_response([{"name": "B"}], more=False)
        self.client._odata._request.side_effect = [page1, page2]

        pages = list(self.client.query.fetch_xml(self._fetch_xml()).execute_pages())
        self.assertEqual(len(pages), 2)
        for page in pages:
            self.assertIsInstance(page, QueryResult)

    def test_execute_pages_one_http_call_per_page(self):
        """Each execute_pages() iteration fires exactly one HTTP request."""
        cookie_raw = "%25253Cpagingcookie%252520pagingcookie%25253D%252522"
        page1 = self._mock_response([{"name": "A"}], more=True, cookie=cookie_raw)
        page2 = self._mock_response([{"name": "B"}], more=False)
        self.client._odata._request.side_effect = [page1, page2]

        count = 0
        for _page in self.client.query.fetch_xml(self._fetch_xml()).execute_pages():
            count += 1
        self.assertEqual(self.client._odata._request.call_count, 2)
        self.assertEqual(count, 2)

    def test_execute_pages_per_page_records(self):
        """Each page yielded by execute_pages() contains only its own records."""
        cookie_raw = "%25253Cpagingcookie%252520pagingcookie%25253D%252522"
        page1 = self._mock_response([{"name": "A"}], more=True, cookie=cookie_raw)
        page2 = self._mock_response([{"name": "B"}, {"name": "C"}], more=False)
        self.client._odata._request.side_effect = [page1, page2]

        pages = list(self.client.query.fetch_xml(self._fetch_xml()).execute_pages())
        self.assertEqual(len(pages[0]), 1)
        self.assertEqual(len(pages[1]), 2)
        self.assertEqual(pages[0].first()["name"], "A")
        self.assertEqual(pages[1].first()["name"], "B")


# ---------------------------------------------------------------------------
# Removed SQL helpers — raise AttributeError
# ---------------------------------------------------------------------------


class TestRemovedSqlHelpers(unittest.TestCase):

    def setUp(self):
        self.client = _make_client()

    def test_sql_select_raises_attribute_error(self):
        with self.assertRaises(AttributeError):
            self.client.query.sql_select("account")

    def test_sql_joins_raises_attribute_error(self):
        with self.assertRaises(AttributeError):
            self.client.query.sql_joins("contact")

    def test_sql_join_raises_attribute_error(self):
        with self.assertRaises(AttributeError):
            self.client.query.sql_join("contact", "account")


# ---------------------------------------------------------------------------
# Deprecated OData helpers — emit DeprecationWarning, still functional
# ---------------------------------------------------------------------------


class TestDeprecatedOdataHelpers(unittest.TestCase):

    def setUp(self):
        self.client = _make_client()

    # --- odata_select ---

    def test_odata_select_emits_deprecation_warning(self):
        self.client._odata._list_columns.return_value = []
        with self.assertWarns(DeprecationWarning):
            self.client.query.odata_select("account")

    def test_odata_select_still_returns_list(self):
        self.client._odata._list_columns.return_value = [
            {
                "LogicalName": "name",
                "AttributeType": "String",
                "IsPrimaryId": False,
                "IsPrimaryName": True,
                "DisplayName": {},
            }
        ]
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            cols = self.client.query.odata_select("account")
        self.assertIsInstance(cols, list)
        self.assertIn("name", cols)

    # --- odata_expand ---

    def _contact_to_account_rel(self):
        return [
            {
                "ReferencingEntity": "contact",
                "ReferencingAttribute": "parentcustomerid",
                "ReferencedEntity": "account",
                "ReferencedAttribute": "accountid",
                "ReferencingEntityNavigationPropertyName": "parentcustomerid_account",
                "SchemaName": "contact_customer_accounts",
            }
        ]

    def test_odata_expand_emits_deprecation_warning(self):
        self.client._odata._list_table_relationships.return_value = self._contact_to_account_rel()
        with self.assertWarns(DeprecationWarning):
            self.client.query.odata_expand("contact", "account")

    def test_odata_expand_still_returns_nav_property(self):
        self.client._odata._list_table_relationships.return_value = self._contact_to_account_rel()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            nav = self.client.query.odata_expand("contact", "account")
        self.assertEqual(nav, "parentcustomerid_account")

    def test_odata_expand_no_match_raises_value_error(self):
        self.client._odata._list_table_relationships.return_value = []
        with self.assertRaises(ValueError):
            self.client.query.odata_expand("contact", "nonexistent")

    # --- odata_bind ---

    def test_odata_bind_emits_deprecation_warning(self):
        self.client._odata._list_table_relationships.return_value = self._contact_to_account_rel()
        with self.assertWarns(DeprecationWarning):
            self.client.query.odata_bind("contact", "account", "some-guid")

    def test_odata_bind_still_returns_bind_dict(self):
        self.client._odata._list_table_relationships.return_value = self._contact_to_account_rel()
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            result = self.client.query.odata_bind("contact", "account", "guid-123")
        self.assertIsInstance(result, dict)
        key = list(result.keys())[0]
        self.assertEqual(key, "parentcustomerid_account@odata.bind")
        self.assertIn("guid-123", result[key])

    def test_odata_bind_no_match_raises_value_error(self):
        self.client._odata._list_table_relationships.return_value = []
        with self.assertRaises(ValueError):
            self.client.query.odata_bind("contact", "nonexistent", "guid")


# ---------------------------------------------------------------------------
# GA-clean methods: no DeprecationWarning
# ---------------------------------------------------------------------------


class TestGaCleanMethods(unittest.TestCase):

    def setUp(self):
        self.client = _make_client()

    def test_sql_columns_no_warning(self):
        self.client._odata._list_columns.return_value = []
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.client.query.sql_columns("account")
        deps = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(deps), 0)

    def test_odata_expands_no_warning(self):
        self.client._odata._list_table_relationships.return_value = []
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.client.query.odata_expands("contact")
        deps = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(deps), 0)

    def test_sql_no_warning(self):
        self.client._odata._query_sql.return_value = []
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.client.query.sql("SELECT name FROM account")
        deps = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(deps), 0)

    def test_builder_no_warning(self):
        self.client._odata._get_multiple.return_value = iter([[]])
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            list(self.client.query.builder("account").select("name").execute())
        deps = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(deps), 0)


# ---------------------------------------------------------------------------
# Codemod — execute(by_page=...) transforms
# ---------------------------------------------------------------------------


class TestCodemodByPage(unittest.TestCase):
    """migrate_source() rewrites literal by_page arguments."""

    @classmethod
    def setUpClass(cls):
        try:
            from tools.migrate_v0_to_v1 import migrate_source

            cls.migrate = staticmethod(migrate_source)
        except ImportError:
            cls.migrate = None

    def setUp(self):
        if self.migrate is None:
            self.skipTest("libcst not installed or tools package not on path")

    def test_execute_by_page_true_becomes_execute_pages(self):
        src = "result = builder.execute(by_page=True)\n"
        out = self.migrate(src)
        self.assertIn("execute_pages()", out)
        self.assertNotIn("by_page", out)
        self.assertNotIn("execute(", out)

    def test_execute_by_page_false_removes_flag(self):
        src = "result = builder.execute(by_page=False)\n"
        out = self.migrate(src)
        self.assertIn("execute()", out)
        self.assertNotIn("by_page", out)

    def test_execute_by_page_variable_not_rewritten(self):
        """Variable by_page argument must not be rewritten — requires manual review."""
        src = "result = builder.execute(by_page=flag)\n"
        out = self.migrate(src)
        self.assertIn("by_page=flag", out)

    def test_idempotent_execute_pages(self):
        """Codemod is idempotent — running again changes nothing."""
        src = "result = builder.execute(by_page=True)\n"
        once = self.migrate(src)
        twice = self.migrate(once)
        self.assertEqual(once, twice)

    def test_idempotent_execute_no_flag(self):
        src = "result = builder.execute(by_page=False)\n"
        once = self.migrate(src)
        twice = self.migrate(once)
        self.assertEqual(once, twice)


if __name__ == "__main__":
    unittest.main()
