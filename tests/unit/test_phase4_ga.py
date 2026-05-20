# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Phase 4 GA regression tests.

Covers:
- fetchxml(): basic, pagination, missing-entity-element error
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
# fetchxml()
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

    def test_fetchxml_inert_no_http_request(self):
        """fetchxml() alone must not fire any HTTP request."""
        from PowerPlatform.Dataverse.models.fetchxml_query import FetchXmlQuery

        query = self.client.query.fetchxml(self._fetch_xml())
        self.assertIsInstance(query, FetchXmlQuery)
        self.client._odata._request.assert_not_called()

    def test_basic_returns_query_result(self):
        self.client._odata._request.return_value = self._mock_response([{"name": "Contoso", "accountid": "1"}])
        result = self.client.query.fetchxml(self._fetch_xml()).execute()
        self.assertIsInstance(result, QueryResult)

    def test_basic_record_count(self):
        self.client._odata._request.return_value = self._mock_response([{"name": "A"}, {"name": "B"}])
        result = self.client.query.fetchxml(self._fetch_xml()).execute()
        self.assertEqual(len(result), 2)

    def test_record_values_accessible(self):
        self.client._odata._request.return_value = self._mock_response([{"name": "Contoso", "accountid": "abc-123"}])
        result = self.client.query.fetchxml(self._fetch_xml()).execute()
        self.assertEqual(result.first()["name"], "Contoso")

    def test_empty_result_returns_empty_query_result(self):
        self.client._odata._request.return_value = self._mock_response([])
        result = self.client.query.fetchxml(self._fetch_xml()).execute()
        self.assertIsInstance(result, QueryResult)
        self.assertEqual(len(result), 0)
        self.assertFalse(result)

    def test_pagination_fetches_all_pages(self):
        """execute_pages() drives the HTTP loop; each page yields one QueryResult."""
        # Annotation is outer XML; pagingcookie attribute is double URL-encoded inner cookie.
        cookie_raw = '<cookie pagenumber="2" pagingcookie="%253Cc%252F%253E" istracking="False" />'
        page1 = self._mock_response([{"name": "A"}], more=True, cookie=cookie_raw)
        page2 = self._mock_response([{"name": "B"}], more=False)
        self.client._odata._request.side_effect = [page1, page2]

        pages = list(self.client.query.fetchxml(self._fetch_xml()).execute_pages())
        self.assertEqual(len(pages), 2)
        self.assertEqual(self.client._odata._request.call_count, 2)

    def test_pagination_second_request_includes_page_and_cookie(self):
        """execute_pages() injects the decoded paging cookie into the second request."""
        # pagingcookie="%253Cc%252F%253E": double URL-decode gives "<c/>" (the inner cookie XML).
        cookie_raw = '<cookie pagenumber="2" pagingcookie="%253Cc%252F%253E" istracking="False" />'
        page1 = self._mock_response([{"name": "A"}], more=True, cookie=cookie_raw)
        page2 = self._mock_response([{"name": "B"}], more=False)
        self.client._odata._request.side_effect = [page1, page2]

        list(self.client.query.fetchxml(self._fetch_xml()).execute_pages())

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
            self.client.query.fetchxml("<fetch top='5'></fetch>")
        self.assertIn("entity", str(ctx.exception).lower())

    def test_entity_missing_name_attr_raises_value_error(self):
        with self.assertRaises(ValueError) as ctx:
            self.client.query.fetchxml("<fetch><entity></entity></fetch>")
        self.assertIn("name", str(ctx.exception).lower())

    def test_entity_set_resolved_from_entity_name(self):
        self.client._odata._request.return_value = self._mock_response([])
        self.client.query.fetchxml(self._fetch_xml("account")).execute()
        self.client._odata._entity_set_from_schema_name.assert_called_with("account")

    def test_request_uses_prefer_header(self):
        self.client._odata._request.return_value = self._mock_response([])
        self.client.query.fetchxml(self._fetch_xml()).execute()
        call_kwargs = self.client._odata._request.call_args
        headers = call_kwargs.kwargs.get("headers", {})
        self.assertIn("Prefer", headers)
        self.assertIn("fetchxmlpagingcookie", headers["Prefer"])

    def test_result_iterable(self):
        self.client._odata._request.return_value = self._mock_response([{"name": "A"}, {"name": "B"}])
        result = self.client.query.fetchxml(self._fetch_xml()).execute()
        names = [r["name"] for r in result]
        self.assertEqual(names, ["A", "B"])

    def test_result_to_dataframe(self):
        try:
            import pandas as pd
        except ImportError:
            self.skipTest("pandas not installed")
        self.client._odata._request.return_value = self._mock_response([{"name": "Contoso"}, {"name": "Fabrikam"}])
        result = self.client.query.fetchxml(self._fetch_xml()).execute()
        df = result.to_dataframe()
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 2)

    def test_no_deprecation_warning_emitted(self):
        self.client._odata._request.return_value = self._mock_response([])
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            self.client.query.fetchxml(self._fetch_xml()).execute()
        deprecations = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        self.assertEqual(len(deprecations), 0, "fetchxml().execute() should not emit DeprecationWarning")

    def test_execute_pages_returns_iterator_of_query_result(self):
        """execute_pages() yields QueryResult objects, one per HTTP page."""
        cookie_raw = '<cookie pagenumber="2" pagingcookie="%253Cc%252F%253E" istracking="False" />'
        page1 = self._mock_response([{"name": "A"}], more=True, cookie=cookie_raw)
        page2 = self._mock_response([{"name": "B"}], more=False)
        self.client._odata._request.side_effect = [page1, page2]

        pages = list(self.client.query.fetchxml(self._fetch_xml()).execute_pages())
        self.assertEqual(len(pages), 2)
        for page in pages:
            self.assertIsInstance(page, QueryResult)

    def test_execute_pages_one_http_call_per_page(self):
        """Each execute_pages() iteration fires exactly one HTTP request."""
        cookie_raw = '<cookie pagenumber="2" pagingcookie="%253Cc%252F%253E" istracking="False" />'
        page1 = self._mock_response([{"name": "A"}], more=True, cookie=cookie_raw)
        page2 = self._mock_response([{"name": "B"}], more=False)
        self.client._odata._request.side_effect = [page1, page2]

        count = 0
        for _page in self.client.query.fetchxml(self._fetch_xml()).execute_pages():
            count += 1
        self.assertEqual(self.client._odata._request.call_count, 2)
        self.assertEqual(count, 2)

    def test_execute_pages_per_page_records(self):
        """Each page yielded by execute_pages() contains only its own records."""
        cookie_raw = '<cookie pagenumber="2" pagingcookie="%253Cc%252F%253E" istracking="False" />'
        page1 = self._mock_response([{"name": "A"}], more=True, cookie=cookie_raw)
        page2 = self._mock_response([{"name": "B"}, {"name": "C"}], more=False)
        self.client._odata._request.side_effect = [page1, page2]

        pages = list(self.client.query.fetchxml(self._fetch_xml()).execute_pages())
        self.assertEqual(len(pages[0]), 1)
        self.assertEqual(len(pages[1]), 2)
        self.assertEqual(pages[0].first()["name"], "A")
        self.assertEqual(pages[1].first()["name"], "B")

    # ------------------------------------------------------------------
    # Input validation
    # ------------------------------------------------------------------

    def test_non_string_input_raises_validation_error(self):
        from PowerPlatform.Dataverse.core.errors import ValidationError

        with self.assertRaises(ValidationError):
            self.client.query.fetchxml(123)

    def test_empty_string_raises_validation_error(self):
        from PowerPlatform.Dataverse.core.errors import ValidationError

        with self.assertRaises(ValidationError):
            self.client.query.fetchxml("")

    def test_whitespace_only_raises_validation_error(self):
        from PowerPlatform.Dataverse.core.errors import ValidationError

        with self.assertRaises(ValidationError):
            self.client.query.fetchxml("   ")

    def test_malformed_xml_raises_validation_error(self):
        from PowerPlatform.Dataverse.core.errors import ValidationError

        with self.assertRaises(ValidationError):
            self.client.query.fetchxml("<fetch><unclosed>")

    def test_url_too_long_raises_validation_error(self):
        """XML whose URL-encoded form exceeds 32,768 chars is rejected before any HTTP."""
        from PowerPlatform.Dataverse.core.errors import ValidationError

        # Alphanumeric chars are URL-safe and don't expand; a 32,769-char name attribute
        # value pushes the encoded XML over the limit.
        long_name = "a" * 32_769
        big_xml = f'<fetch><entity name="{long_name}"><attribute name="x"/></entity></fetch>'
        with self.assertRaises(ValidationError):
            self.client.query.fetchxml(big_xml)

    # ------------------------------------------------------------------
    # Paging behaviour
    # ------------------------------------------------------------------

    def test_morerecords_string_true_continues_paging(self):
        """morerecords annotation as string "true" (not bool) is handled correctly."""
        cookie_raw = '<cookie pagenumber="2" pagingcookie="%253Cc%252F%253E" istracking="False" />'
        page1_payload = {
            "value": [{"name": "A"}],
            "@Microsoft.Dynamics.CRM.morerecords": "true",
            "@Microsoft.Dynamics.CRM.fetchxmlpagingcookie": cookie_raw,
        }
        page2_payload = {
            "value": [{"name": "B"}],
            "@Microsoft.Dynamics.CRM.morerecords": False,
        }
        r1, r2 = MagicMock(), MagicMock()
        r1.json.return_value = page1_payload
        r2.json.return_value = page2_payload
        self.client._odata._request.side_effect = [r1, r2]

        result = self.client.query.fetchxml(self._fetch_xml()).execute()
        self.assertEqual(len(result), 2)
        self.assertEqual(self.client._odata._request.call_count, 2)

    def test_simple_paging_fallback_emits_user_warning(self):
        """No cookie returned with morerecords=True triggers a UserWarning."""
        page1 = self._mock_response([{"name": "A"}], more=True, cookie="")
        page2 = self._mock_response([{"name": "B"}], more=False)
        self.client._odata._request.side_effect = [page1, page2]

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            list(self.client.query.fetchxml(self._fetch_xml()).execute_pages())

        user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        self.assertEqual(len(user_warnings), 1)
        self.assertIn("simple paging", str(user_warnings[0].message).lower())

    def test_simple_paging_fallback_fetches_all_pages(self):
        """Simple paging fallback continues iterating; caller still gets all records."""
        page1 = self._mock_response([{"name": "A"}], more=True, cookie="")
        page2 = self._mock_response([{"name": "B"}], more=False)
        self.client._odata._request.side_effect = [page1, page2]

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            result = self.client.query.fetchxml(self._fetch_xml()).execute()

        self.assertEqual(len(result), 2)
        self.assertEqual(self.client._odata._request.call_count, 2)

    def test_malformed_cookie_xml_warns_distinctly(self):
        """A cookie that is not valid XML emits a 'could not be parsed' warning, not the no-cookie warning."""
        page1 = self._mock_response([{"name": "A"}], more=True, cookie="not-valid-xml")
        page2 = self._mock_response([{"name": "B"}], more=False)
        self.client._odata._request.side_effect = [page1, page2]

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = self.client.query.fetchxml(self._fetch_xml()).execute()

        self.assertEqual(len(result), 2)
        user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        self.assertEqual(len(user_warnings), 1)
        self.assertIn("could not be parsed", str(user_warnings[0].message).lower())

    def test_corrupt_pagenumber_warns_distinctly(self):
        """Valid XML cookie with non-integer pagenumber emits a 'could not be parsed' warning."""
        bad_cookie = '<cookie pagenumber="NaN" pagingcookie="%253Cc%252F%253E" istracking="False" />'
        page1 = self._mock_response([{"name": "A"}], more=True, cookie=bad_cookie)
        page2 = self._mock_response([{"name": "B"}], more=False)
        self.client._odata._request.side_effect = [page1, page2]

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = self.client.query.fetchxml(self._fetch_xml()).execute()

        self.assertEqual(len(result), 2)
        user_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        self.assertEqual(len(user_warnings), 1)
        self.assertIn("could not be parsed", str(user_warnings[0].message).lower())

    def test_max_pages_exceeded_raises(self):
        """Paging loop raises ValidationError after exceeding _MAX_PAGES."""
        from PowerPlatform.Dataverse.core.errors import ValidationError

        cookie_raw = '<cookie pagenumber="2" pagingcookie="%253Cc%252F%253E" istracking="False" />'
        always_more = self._mock_response([{"name": "A"}], more=True, cookie=cookie_raw)
        self.client._odata._request.return_value = always_more

        with patch("PowerPlatform.Dataverse.models.fetchxml_query._MAX_PAGES", 3):
            with self.assertRaises(ValidationError):
                list(self.client.query.fetchxml(self._fetch_xml()).execute_pages())


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
            from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import migrate_source

            cls.migrate = staticmethod(migrate_source)
        except ImportError:
            cls.migrate = None

    def setUp(self):
        if self.migrate is None:
            self.skipTest("libcst not installed")

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

    def test_client_var_default_rewrites_client(self):
        """Default client_var='client' rewrites client.create(...)."""
        src = "client.create('account', data)\n"
        out = self.migrate(src)
        self.assertIn("client.records.create", out)

    def test_client_var_custom_rewrites_matching_name(self):
        """custom client_var rewrites that variable name, not 'client'."""
        src = "svc.create('account', data)\n"
        out = self.migrate(src, client_var="svc")
        self.assertIn("svc.records.create", out)

    def test_client_var_custom_does_not_rewrite_default_name(self):
        """When client_var='svc', the literal name 'client' is left untouched."""
        src = "client.create('account', data)\n"
        out = self.migrate(src, client_var="svc")
        self.assertNotIn("client.records.create", out)
        self.assertIn("client.create", out)


class TestManualReviewFinder(unittest.TestCase):
    """find_manual_patterns() detects patterns that require manual migration."""

    @classmethod
    def setUpClass(cls):
        try:
            from PowerPlatform.Dataverse.migration.migrate_v0_to_v1 import find_manual_patterns

            cls.find = staticmethod(find_manual_patterns)
        except ImportError:
            cls.find = None

    def setUp(self):
        if self.find is None:
            self.skipTest("libcst not installed")

    def test_records_get_flagged(self):
        src = "result = client.records.get('account', record_id)\n"
        findings = self.find(src)
        self.assertTrue(any("records.get" in f for f in findings))

    def test_dataframe_get_flagged(self):
        src = "df = client.dataframe.get('account', select=['name'])\n"
        findings = self.find(src)
        self.assertTrue(any("dataframe.get" in f for f in findings))

    def test_execute_by_page_variable_flagged(self):
        src = "result = builder.execute(by_page=flag)\n"
        findings = self.find(src)
        self.assertTrue(any("by_page" in f for f in findings))

    def test_execute_by_page_literal_not_flagged(self):
        """Literal True/False is handled by the transformer — not a manual item."""
        src = "result = builder.execute(by_page=True)\n"
        findings = self.find(src)
        self.assertFalse(any("by_page" in f for f in findings))

    def test_sql_select_flagged(self):
        src = "cols = client.query.sql_select('account')\n"
        findings = self.find(src)
        self.assertTrue(any("sql_select" in f for f in findings))

    def test_sql_join_flagged(self):
        src = "j = client.query.sql_join('account', 'contact')\n"
        findings = self.find(src)
        self.assertTrue(any("sql_join" in f for f in findings))

    def test_clean_code_no_findings(self):
        src = "result = client.records.list('account', filter='statecode eq 0')\n"
        self.assertEqual(self.find(src), [])

    def test_custom_client_var(self):
        src = "svc.records.get('account', guid)\n"
        self.assertEqual(self.find(src, client_var="client"), [])
        findings = self.find(src, client_var="svc")
        self.assertTrue(any("records.get" in f for f in findings))


if __name__ == "__main__":
    unittest.main()
