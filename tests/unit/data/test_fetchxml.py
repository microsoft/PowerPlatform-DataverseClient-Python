# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock, patch

from PowerPlatform.Dataverse.core.errors import HttpError, MetadataError, ValidationError
from PowerPlatform.Dataverse.core._error_codes import (
    METADATA_ENTITYSET_NOT_FOUND,
    VALIDATION_FETCHXML_NOT_STRING,
    VALIDATION_FETCHXML_EMPTY,
    VALIDATION_FETCHXML_MALFORMED,
    VALIDATION_FETCHXML_TOO_LONG,
    VALIDATION_FETCHXML_INVALID_PAGE_SIZE,
)
from PowerPlatform.Dataverse.data._odata import _ODataClient


def _make_odata_client() -> _ODataClient:
    """Return an _ODataClient with HTTP calls mocked out."""
    mock_auth = MagicMock()
    mock_auth._acquire_token.return_value = MagicMock(access_token="token")
    client = _ODataClient(mock_auth, "https://example.crm.dynamics.com")
    client._request = MagicMock()
    return client


class TestExtractEntityFromFetchxml(unittest.TestCase):
    """Unit tests for _ODataClient._extract_entity_from_fetchxml static helper."""

    def test_extract_entity_from_fetchxml(self):
        """Extract entity name from valid FetchXML."""
        fetchxml = "<fetch><entity name='account'><attribute name='name' /></entity></fetch>"
        result = _ODataClient._extract_entity_from_fetchxml(fetchxml)
        self.assertEqual(result, "account")

    def test_extract_entity_from_fetchxml_preserves_lowercase(self):
        """Entity name is lowercased."""
        fetchxml = "<fetch><entity name='Account'><attribute name='name' /></entity></fetch>"
        result = _ODataClient._extract_entity_from_fetchxml(fetchxml)
        self.assertEqual(result, "account")

    def test_extract_entity_from_fetchxml_missing_entity(self):
        """Raise ValidationError on missing entity element."""
        fetchxml = "<fetch><attribute name='name' /></fetch>"
        with self.assertRaises(ValidationError) as ctx:
            _ODataClient._extract_entity_from_fetchxml(fetchxml)
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_MALFORMED)

    def test_extract_entity_from_fetchxml_malformed_xml(self):
        """Raise ValidationError on invalid XML."""
        fetchxml = "<fetch><entity name='account'"
        with self.assertRaises(ValidationError) as ctx:
            _ODataClient._extract_entity_from_fetchxml(fetchxml)
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_MALFORMED)

    def test_extract_entity_from_fetchxml_non_fetch_root(self):
        """Raise ValidationError when root element is not <fetch>."""
        fetchxml = "<query><entity name='account' /></query>"
        with self.assertRaises(ValidationError) as ctx:
            _ODataClient._extract_entity_from_fetchxml(fetchxml)
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_MALFORMED)
        self.assertIn("root element must be <fetch>", str(ctx.exception))


class TestQueryFetchxml(unittest.TestCase):
    """Unit tests for _ODataClient._query_fetchxml."""

    def setUp(self):
        self.od = _make_odata_client()

    def _setup_entity_set(self):
        """Patch _entity_set_from_schema_name to return accounts without HTTP."""
        self.od._entity_set_from_schema_name = MagicMock(return_value="accounts")

    def test_query_fetchxml_validation_not_string(self):
        """Raise ValidationError for non-string input."""
        self._setup_entity_set()
        with self.assertRaises(ValidationError) as ctx:
            list(self.od._query_fetchxml(123))
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_NOT_STRING)

    def test_query_fetchxml_validation_empty(self):
        """Raise ValidationError for empty string."""
        self._setup_entity_set()
        with self.assertRaises(ValidationError) as ctx:
            list(self.od._query_fetchxml(""))
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_EMPTY)

    def test_query_fetchxml_validation_whitespace_only(self):
        """Raise ValidationError for whitespace-only string."""
        self._setup_entity_set()
        with self.assertRaises(ValidationError) as ctx:
            list(self.od._query_fetchxml("   \n\t  "))
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_EMPTY)

    def test_query_fetchxml_wrong_entity_case_raises_http_error(self):
        """Wrong entity case (PascalCase) in FetchXML causes API 400; HttpError propagates."""
        self._setup_entity_set()
        self.od._request.side_effect = HttpError(
            "The entity with a name = 'Contact' was not found",
            status_code=400,
        )
        fetchxml = "<fetch top='1'><entity name='Contact'><attribute name='contactid' /></entity></fetch>"
        with self.assertRaises(HttpError) as ctx:
            list(self.od._query_fetchxml(fetchxml))
        self.assertEqual(ctx.exception.status_code, 400)

    def test_query_fetchxml_wrong_entity_name_raises_metadata_error(self):
        """Non-existent entity name raises MetadataError from entity set resolution."""
        self.od._entity_set_from_schema_name = MagicMock(
            side_effect=MetadataError(
                "Unable to resolve entity set for table schema name 'NonexistentEntity123'.",
                subcode=METADATA_ENTITYSET_NOT_FOUND,
            )
        )
        fetchxml = "<fetch top='1'><entity name='NonexistentEntity123'><attribute name='id' /></entity></fetch>"
        with self.assertRaises(MetadataError) as ctx:
            list(self.od._query_fetchxml(fetchxml))
        self.assertEqual(ctx.exception.subcode, METADATA_ENTITYSET_NOT_FOUND)
        self.assertIn("NonexistentEntity123", str(ctx.exception))

    def test_query_fetchxml_wrong_attribute_name_raises_http_error(self):
        """Invalid attribute name in FetchXML causes API 400; HttpError propagates."""
        self._setup_entity_set()
        self.od._request.side_effect = HttpError(
            "Invalid attribute 'InvalidColumnName123'",
            status_code=400,
        )
        fetchxml = "<fetch top='1'><entity name='account'><attribute name='InvalidColumnName123' /></entity></fetch>"
        with self.assertRaises(HttpError) as ctx:
            list(self.od._query_fetchxml(fetchxml))
        self.assertEqual(ctx.exception.status_code, 400)

    def test_query_fetchxml_single_page(self):
        """Mock HTTP response with no more records, verify single page yielded."""
        self._setup_entity_set()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "value": [{"accountid": "1", "name": "Contoso"}, {"accountid": "2", "name": "Fabrikam"}],
        }
        self.od._request.return_value = mock_response

        fetchxml = "<fetch><entity name='account'><attribute name='name' /></entity></fetch>"
        pages = list(self.od._query_fetchxml(fetchxml))

        self.assertEqual(len(pages), 1)
        self.assertEqual(len(pages[0]), 2)
        self.assertEqual(pages[0][0]["name"], "Contoso")
        self.assertEqual(pages[0][1]["name"], "Fabrikam")
        self.od._request.assert_called_once()

    def test_query_fetchxml_empty_result(self):
        """Empty value list yields no pages (matches _get_multiple pattern)."""
        self._setup_entity_set()
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": []}
        self.od._request.return_value = mock_response

        fetchxml = "<fetch><entity name='account' /></fetch>"
        pages = list(self.od._query_fetchxml(fetchxml))

        self.assertEqual(len(pages), 0)

    def test_query_fetchxml_multi_page(self):
        """Mock HTTP responses with paging cookie, verify multiple pages yielded."""
        self._setup_entity_set()
        # First page: 2 records, more records exist
        # Second page: 1 record, no more records
        from urllib.parse import quote

        cookie_inner = quote(quote("<cookie pagenumber='2' />"))
        resp1 = MagicMock()
        resp1.json.return_value = {
            "value": [{"accountid": "1", "name": "A"}, {"accountid": "2", "name": "B"}],
            "@Microsoft.Dynamics.CRM.morerecords": True,
            "@Microsoft.Dynamics.CRM.fetchxmlpagingcookie": f'<cookie pagenumber="2" pagingcookie="{cookie_inner}" istracking="False" />',
        }
        resp2 = MagicMock()
        resp2.json.return_value = {
            "value": [{"accountid": "3", "name": "C"}],
        }
        self.od._request.side_effect = [resp1, resp2]

        fetchxml = "<fetch count='2'><entity name='account'><attribute name='name' /></entity></fetch>"
        pages = list(self.od._query_fetchxml(fetchxml))

        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0], [{"accountid": "1", "name": "A"}, {"accountid": "2", "name": "B"}])
        self.assertEqual(pages[1], [{"accountid": "3", "name": "C"}])
        self.assertEqual(self.od._request.call_count, 2)

    def test_query_fetchxml_paging_cookie_decode(self):
        """Verify double URL-decode of paging cookie and injection into fetch element."""
        self._setup_entity_set()
        from urllib.parse import quote

        # Cookie value is double-encoded
        inner = "<cookie pagenumber='2' />"
        encoded_once = quote(inner)
        encoded_twice = quote(encoded_once)

        resp1 = MagicMock()
        resp1.json.return_value = {
            "value": [{"accountid": "1"}],
            "@Microsoft.Dynamics.CRM.morerecords": True,
            "@Microsoft.Dynamics.CRM.fetchxmlpagingcookie": f'<cookie pagenumber="2" pagingcookie="{encoded_twice}" istracking="False" />',
        }
        resp2 = MagicMock()
        resp2.json.return_value = {"value": []}
        self.od._request.side_effect = [resp1, resp2]

        fetchxml = "<fetch count='1'><entity name='account' /></fetch>"
        list(self.od._query_fetchxml(fetchxml))

        # Second request URL should contain the decoded paging cookie
        second_call_url = self.od._request.call_args_list[1][0][1]
        self.assertIn("fetchXml=", second_call_url)
        # The decoded inner cookie should appear in the URL (after double decode)
        self.assertIn("pagenumber", second_call_url)

    def test_query_fetchxml_page_size_injected(self):
        """Verify count attribute set on fetch when page_size provided."""
        self._setup_entity_set()
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": []}
        self.od._request.return_value = mock_response

        fetchxml = "<fetch><entity name='account' /></fetch>"
        list(self.od._query_fetchxml(fetchxml, page_size=50))

        # The request URL should contain count=50 in the fetchXml
        call_url = self.od._request.call_args[0][1]
        self.assertIn("fetchXml=", call_url)
        self.assertIn("count", call_url)
        self.assertIn("50", call_url)

    def test_query_fetchxml_existing_count_not_overridden(self):
        """page_size does not override existing count attribute in FetchXML."""
        self._setup_entity_set()
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": []}
        self.od._request.return_value = mock_response

        fetchxml = "<fetch count='10'><entity name='account' /></fetch>"
        list(self.od._query_fetchxml(fetchxml, page_size=50))

        call_url = self.od._request.call_args[0][1]
        self.assertIn("count", call_url)
        self.assertIn("10", call_url)
        self.assertNotIn("50", call_url)

    def test_query_fetchxml_existing_page_preserved(self):
        """Pre-set page attribute is preserved and not overridden to '1'."""
        self._setup_entity_set()
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": [{"accountid": "1"}]}
        self.od._request.return_value = mock_response

        fetchxml = "<fetch count='5' page='3'><entity name='account' /></fetch>"
        list(self.od._query_fetchxml(fetchxml))

        call_url = self.od._request.call_args[0][1]
        self.assertIn("page", call_url)
        self.assertIn("3", call_url)

    def test_query_fetchxml_page_non_integer_raises(self):
        """Raise ValidationError when page attribute is not an integer."""
        self._setup_entity_set()
        fetchxml = "<fetch page='abc'><entity name='account'><attribute name='name' /></entity></fetch>"
        with self.assertRaises(ValidationError) as ctx:
            list(self.od._query_fetchxml(fetchxml))
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_MALFORMED)
        self.assertIn("page", str(ctx.exception).lower())

    def test_query_fetchxml_page_zero_raises(self):
        """Raise ValidationError when page attribute is zero."""
        self._setup_entity_set()
        fetchxml = "<fetch page='0'><entity name='account'><attribute name='name' /></entity></fetch>"
        with self.assertRaises(ValidationError) as ctx:
            list(self.od._query_fetchxml(fetchxml))
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_MALFORMED)

    def test_query_fetchxml_page_negative_raises(self):
        """Raise ValidationError when page attribute is negative."""
        self._setup_entity_set()
        fetchxml = "<fetch page='-1'><entity name='account'><attribute name='name' /></entity></fetch>"
        with self.assertRaises(ValidationError) as ctx:
            list(self.od._query_fetchxml(fetchxml))
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_MALFORMED)

    def test_query_fetchxml_morerecords_string_true(self):
        """When morerecords is string 'true' (not boolean), paging continues."""
        self._setup_entity_set()
        from urllib.parse import quote

        cookie_inner = quote(quote("<cookie pagenumber='2' />"))
        resp1 = MagicMock()
        resp1.json.return_value = {
            "value": [{"accountid": "1", "name": "A"}],
            "@Microsoft.Dynamics.CRM.morerecords": "true",  # string, not boolean
            "@Microsoft.Dynamics.CRM.fetchxmlpagingcookie": f'<cookie pagenumber="2" pagingcookie="{cookie_inner}" istracking="False" />',
        }
        resp2 = MagicMock()
        resp2.json.return_value = {"value": [{"accountid": "2", "name": "B"}]}
        self.od._request.side_effect = [resp1, resp2]

        fetchxml = "<fetch count='1'><entity name='account'><attribute name='name' /></entity></fetch>"
        pages = list(self.od._query_fetchxml(fetchxml))

        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0], [{"accountid": "1", "name": "A"}])
        self.assertEqual(pages[1], [{"accountid": "2", "name": "B"}])
        self.assertEqual(self.od._request.call_count, 2)

    def test_query_fetchxml_simple_paging_fallback(self):
        """When morerecords=True but no paging cookie, fall back to simple page increment."""
        self._setup_entity_set()
        resp1 = MagicMock()
        resp1.json.return_value = {
            "value": [{"accountid": "1", "name": "A"}],
            "@Microsoft.Dynamics.CRM.morerecords": True,
        }
        resp2 = MagicMock()
        resp2.json.return_value = {
            "value": [{"accountid": "2", "name": "B"}],
        }
        self.od._request.side_effect = [resp1, resp2]

        fetchxml = "<fetch count='1' page='1'><entity name='account'><attribute name='name' /></entity></fetch>"
        pages = list(self.od._query_fetchxml(fetchxml))

        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0], [{"accountid": "1", "name": "A"}])
        self.assertEqual(pages[1], [{"accountid": "2", "name": "B"}])
        self.assertEqual(self.od._request.call_count, 2)

        second_call_url = self.od._request.call_args_list[1][0][1]
        self.assertIn("page", second_call_url)
        self.assertIn("2", second_call_url)

    def test_query_fetchxml_top_with_page_size_raises(self):
        """Raise ValidationError when FetchXML has top and page_size is provided."""
        self._setup_entity_set()
        fetchxml = "<fetch top='5'><entity name='account'><attribute name='name' /></entity></fetch>"
        with self.assertRaises(ValidationError) as ctx:
            list(self.od._query_fetchxml(fetchxml, page_size=10))
        self.assertIn("top", str(ctx.exception).lower())
        self.assertIn("page_size", str(ctx.exception).lower())
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_INVALID_PAGE_SIZE)

    def test_query_fetchxml_negative_page_size_raises(self):
        """Raise ValidationError when page_size is negative."""
        self._setup_entity_set()
        fetchxml = "<fetch><entity name='account' /></fetch>"
        with self.assertRaises(ValidationError) as ctx:
            list(self.od._query_fetchxml(fetchxml, page_size=-5))
        self.assertIn("page_size", str(ctx.exception).lower())
        self.assertIn("positive", str(ctx.exception).lower())
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_INVALID_PAGE_SIZE)

    def test_query_fetchxml_zero_page_size_raises(self):
        """Raise ValidationError when page_size is zero."""
        self._setup_entity_set()
        fetchxml = "<fetch><entity name='account' /></fetch>"
        with self.assertRaises(ValidationError) as ctx:
            list(self.od._query_fetchxml(fetchxml, page_size=0))
        self.assertIn("page_size", str(ctx.exception).lower())
        self.assertIn("positive", str(ctx.exception).lower())
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_INVALID_PAGE_SIZE)

    def test_query_fetchxml_boolean_true_page_size_raises(self):
        """Raise ValidationError for boolean page_size (bool is int subclass)."""
        self._setup_entity_set()
        fetchxml = "<fetch><entity name='account' /></fetch>"
        with self.assertRaises(ValidationError) as ctx:
            list(self.od._query_fetchxml(fetchxml, page_size=True))
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_INVALID_PAGE_SIZE)

    def test_query_fetchxml_boolean_false_page_size_raises(self):
        """Raise ValidationError for False page_size (bool is int subclass)."""
        self._setup_entity_set()
        fetchxml = "<fetch><entity name='account' /></fetch>"
        with self.assertRaises(ValidationError) as ctx:
            list(self.od._query_fetchxml(fetchxml, page_size=False))
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_INVALID_PAGE_SIZE)

    def test_query_fetchxml_non_numeric_page_size_raises(self):
        """Raise ValidationError for non-numeric page_size."""
        self._setup_entity_set()
        fetchxml = "<fetch><entity name='account' /></fetch>"
        with self.assertRaises(ValidationError) as ctx:
            list(self.od._query_fetchxml(fetchxml, page_size="abc"))
        self.assertIn("page_size", str(ctx.exception).lower())
        self.assertIn("integer", str(ctx.exception).lower())
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_INVALID_PAGE_SIZE)

    def test_query_fetchxml_fractional_page_size_raises(self):
        """Raise ValidationError for fractional page_size (e.g. from config/CLI parsing)."""
        self._setup_entity_set()
        fetchxml = "<fetch><entity name='account' /></fetch>"
        with self.assertRaises(ValidationError) as ctx:
            list(self.od._query_fetchxml(fetchxml, page_size=1.9))
        self.assertIn("page_size", str(ctx.exception).lower())
        self.assertIn("integer", str(ctx.exception).lower())
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_INVALID_PAGE_SIZE)

    def test_query_fetchxml_top_no_paging(self):
        """FetchXML with top returns single page, no page/count attributes injected."""
        self._setup_entity_set()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "value": [{"accountid": "1", "name": "A"}, {"accountid": "2", "name": "B"}],
        }
        self.od._request.return_value = mock_response

        fetchxml = "<fetch top='5'><entity name='account'><attribute name='name' /></entity></fetch>"
        pages = list(self.od._query_fetchxml(fetchxml))

        self.assertEqual(len(pages), 1)
        self.assertEqual(len(pages[0]), 2)
        self.od._request.assert_called_once()

        call_url = self.od._request.call_args[0][1]
        self.assertNotIn("page=", call_url)
        self.assertNotIn("count=", call_url)

    def test_query_fetchxml_malformed_paging_cookie_raises(self):
        """Raise ValidationError when paging cookie XML is malformed."""
        self._setup_entity_set()
        resp1 = MagicMock()
        resp1.json.return_value = {
            "value": [{"accountid": "1"}],
            "@Microsoft.Dynamics.CRM.morerecords": True,
            "@Microsoft.Dynamics.CRM.fetchxmlpagingcookie": "<cookie pagenumber='2' pagingcookie='<invalid",
        }
        self.od._request.return_value = resp1

        fetchxml = "<fetch count='1'><entity name='account' /></fetch>"
        with self.assertRaises(ValidationError) as ctx:
            list(self.od._query_fetchxml(fetchxml))
        self.assertIn("paging cookie", str(ctx.exception).lower())
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_MALFORMED)

    def test_query_fetchxml_missing_pagingcookie_attr_raises(self):
        """Raise ValidationError when paging cookie element lacks pagingcookie attribute."""
        self._setup_entity_set()
        resp1 = MagicMock()
        resp1.json.return_value = {
            "value": [{"accountid": "1"}],
            "@Microsoft.Dynamics.CRM.morerecords": True,
            "@Microsoft.Dynamics.CRM.fetchxmlpagingcookie": "<cookie pagenumber='2' />",
        }
        self.od._request.return_value = resp1

        fetchxml = "<fetch count='1'><entity name='account' /></fetch>"
        with self.assertRaises(ValidationError) as ctx:
            list(self.od._query_fetchxml(fetchxml))
        self.assertIn("pagingcookie", str(ctx.exception).lower())
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_MALFORMED)

    def test_query_fetchxml_aggregate_no_paging(self):
        """Aggregate FetchXML returns single page, no page/count attributes injected."""
        self._setup_entity_set()
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "value": [{"accountid": "1", "aggregate_column": 42}],
        }
        self.od._request.return_value = mock_response

        fetchxml = (
            "<fetch aggregate='true'>"
            "<entity name='account'>"
            "<attribute name='accountid' aggregate='count' />"
            "</entity></fetch>"
        )
        pages = list(self.od._query_fetchxml(fetchxml))

        self.assertEqual(len(pages), 1)
        self.assertEqual(len(pages[0]), 1)
        self.od._request.assert_called_once()

        call_url = self.od._request.call_args[0][1]
        self.assertNotIn("page=", call_url)
        self.assertNotIn("count=", call_url)

    def test_query_fetchxml_excessive_length_raises(self):
        """Raise ValidationError when FetchXML exceeds maximum length."""
        self._setup_entity_set()
        from PowerPlatform.Dataverse.data._odata import _MAX_FETCHXML_LENGTH

        fetchxml = "<fetch><entity name='account' /></fetch>"
        long_fetchxml = fetchxml + "x" * (_MAX_FETCHXML_LENGTH - len(fetchxml) + 1)

        with self.assertRaises(ValidationError) as ctx:
            list(self.od._query_fetchxml(long_fetchxml))
        self.assertIn("exceeds maximum", str(ctx.exception).lower())
        self.assertIn(str(_MAX_FETCHXML_LENGTH), str(ctx.exception))
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_TOO_LONG)

    def test_query_fetchxml_aggregate_with_page_size_raises(self):
        """Raise ValidationError when page_size is used with aggregate FetchXML."""
        self._setup_entity_set()
        fetchxml = (
            "<fetch aggregate='true'>"
            "<entity name='account'>"
            "<attribute name='accountid' aggregate='count' />"
            "</entity></fetch>"
        )
        with self.assertRaises(ValidationError) as ctx:
            list(self.od._query_fetchxml(fetchxml, page_size=50))
        self.assertIn("aggregate", str(ctx.exception).lower())
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_INVALID_PAGE_SIZE)

    def test_query_fetchxml_distinct_without_order_raises(self):
        """Raise ValidationError for distinct query without order element."""
        self._setup_entity_set()
        fetchxml = "<fetch distinct='true'><entity name='account'><attribute name='name' /></entity></fetch>"
        with self.assertRaises(ValidationError) as ctx:
            list(self.od._query_fetchxml(fetchxml))
        self.assertIn("distinct", str(ctx.exception).lower())
        self.assertIn("order", str(ctx.exception).lower())
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_MALFORMED)

    def test_query_fetchxml_max_pages_exceeded_raises(self):
        """Raise ValidationError when paging loop exceeds maximum page limit."""
        self._setup_entity_set()
        from urllib.parse import quote

        cookie_inner = quote(quote("<cookie pagenumber='2' />"))
        resp_with_more = MagicMock()
        resp_with_more.json.return_value = {
            "value": [{"accountid": "1"}],
            "@Microsoft.Dynamics.CRM.morerecords": True,
            "@Microsoft.Dynamics.CRM.fetchxmlpagingcookie": f'<cookie pagenumber="2" pagingcookie="{cookie_inner}" istracking="False" />',
        }
        self.od._request.return_value = resp_with_more

        fetchxml = "<fetch count='1'><entity name='account' /></fetch>"
        with patch("PowerPlatform.Dataverse.data._odata._MAX_FETCHXML_PAGES", 3):
            with self.assertRaises(ValidationError) as ctx:
                list(self.od._query_fetchxml(fetchxml))
        self.assertIn("maximum page limit", str(ctx.exception).lower())
        self.assertIn("3", str(ctx.exception))
        self.assertEqual(ctx.exception.subcode, VALIDATION_FETCHXML_MALFORMED)
        self.assertEqual(self.od._request.call_count, 3)


if __name__ == "__main__":
    unittest.main()
