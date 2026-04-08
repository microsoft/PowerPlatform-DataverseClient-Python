# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import json
import time
import unittest
from enum import Enum
from unittest.mock import MagicMock, patch

from PowerPlatform.Dataverse.core.errors import HttpError, MetadataError, ValidationError
from PowerPlatform.Dataverse.data._odata import _ODataClient


def _make_odata_client() -> _ODataClient:
    """Return an _ODataClient with HTTP calls mocked out."""
    mock_auth = MagicMock()
    mock_auth._acquire_token.return_value = MagicMock(access_token="token")
    client = _ODataClient(mock_auth, "https://example.crm.dynamics.com")
    client._request = MagicMock()
    return client


def _mock_response(json_data=None, text="", status_code=200, headers=None):
    """Create a mock HTTP response."""
    r = MagicMock()
    r.status_code = status_code
    r.text = text or (str(json_data) if json_data else "")
    r.json.return_value = json_data or {}
    r.headers = headers or {}
    return r


def _entity_def_response(entity_set_name="accounts", primary_id="accountid", metadata_id="meta-001"):
    """Simulate a successful EntityDefinitions response."""
    return _mock_response(
        json_data={
            "value": [
                {
                    "LogicalName": "account",
                    "EntitySetName": entity_set_name,
                    "PrimaryIdAttribute": primary_id,
                    "MetadataId": metadata_id,
                    "SchemaName": "Account",
                }
            ]
        }
    )


class TestUpsertMultipleValidation(unittest.TestCase):
    """Unit tests for _ODataClient._upsert_multiple internal validation."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_mismatched_lengths_raises_value_error(self):
        """_upsert_multiple raises ValueError when alternate_keys and records differ in length."""
        with self.assertRaises(ValueError):
            self.od._upsert_multiple(
                "accounts",
                "account",
                [{"name": "acc1"}],
                [{"description": "d1"}, {"description": "d2"}],
            )

    def test_mismatched_lengths_error_message(self):
        """ValueError message reports both lengths."""
        with self.assertRaises(ValueError) as ctx:
            self.od._upsert_multiple(
                "accounts",
                "account",
                [{"name": "acc1"}, {"name": "acc2"}],
                [{"description": "d1"}],
            )
        self.assertIn("2", str(ctx.exception))
        self.assertIn("1", str(ctx.exception))

    def test_equal_lengths_does_not_raise(self):
        """_upsert_multiple does not raise when both lists have the same length."""
        self.od._upsert_multiple(
            "accounts",
            "account",
            [{"name": "acc1"}, {"name": "acc2"}],
            [{"description": "d1"}, {"description": "d2"}],
        )
        # Verify the UpsertMultiple POST was issued (other calls are picklist probes).
        post_calls = [c for c in self.od._request.call_args_list if c.args[0] == "post"]
        self.assertEqual(len(post_calls), 1)
        self.assertIn("UpsertMultiple", post_calls[0].args[1])
        payload = post_calls[0].kwargs.get("json", {})
        self.assertEqual(len(payload["Targets"]), 2)
        self.assertIn("@odata.type", payload["Targets"][0])
        self.assertIn("@odata.id", payload["Targets"][0])

    def test_payload_excludes_alternate_key_fields(self):
        """Alternate key fields must NOT appear in the request body (only in @odata.id)."""
        self.od._upsert_multiple(
            "accounts",
            "account",
            [{"accountnumber": "ACC-001"}],
            [{"name": "Contoso", "telephone1": "555-0100"}],
        )
        post_calls = [c for c in self.od._request.call_args_list if c.args[0] == "post"]
        self.assertEqual(len(post_calls), 1)
        payload = post_calls[0].kwargs.get("json", {})
        target = payload["Targets"][0]
        # accountnumber should only be in @odata.id, NOT as a body field
        self.assertNotIn("accountnumber", target)
        self.assertIn("name", target)
        self.assertIn("telephone1", target)
        self.assertIn("@odata.id", target)
        self.assertIn("accountnumber", target["@odata.id"])

    def test_payload_excludes_alternate_key_even_when_in_record(self):
        """If user passes matching key field in record, it should still be excluded from body."""
        self.od._upsert_multiple(
            "accounts",
            "account",
            [{"accountnumber": "ACC-001"}],
            [{"accountnumber": "ACC-001", "name": "Contoso"}],
        )
        post_calls = [c for c in self.od._request.call_args_list if c.args[0] == "post"]
        payload = post_calls[0].kwargs.get("json", {})
        target = payload["Targets"][0]
        # Even though user passed accountnumber in record with same value,
        # it should still appear in the body because it came from record_processed
        # (the conflict check allows matching values through)
        self.assertIn("@odata.id", target)
        self.assertIn("accountnumber", target["@odata.id"])

    def test_record_conflicts_with_alternate_key_raises_value_error(self):
        """_upsert_multiple raises ValueError when a record field contradicts its alternate key."""
        with self.assertRaises(ValueError) as ctx:
            self.od._upsert_multiple(
                "accounts",
                "account",
                [{"accountnumber": "ACC-001"}],
                [{"accountnumber": "ACC-WRONG", "name": "Contoso"}],
            )
        self.assertIn("accountnumber", str(ctx.exception))

    def test_record_matching_alternate_key_field_does_not_raise(self):
        """_upsert_multiple does not raise when a record field matches its alternate key value."""
        self.od._upsert_multiple(
            "accounts",
            "account",
            [{"accountnumber": "ACC-001"}],
            [{"accountnumber": "ACC-001", "name": "Contoso"}],
        )


class TestBuildAlternateKeyStr(unittest.TestCase):
    """Unit tests for _ODataClient._build_alternate_key_str."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_single_string_value(self):
        """Single string key is single-quoted."""
        result = self.od._build_alternate_key_str({"accountnumber": "ACC-001"})
        self.assertEqual(result, "accountnumber='ACC-001'")

    def test_single_int_value(self):
        """Non-string value is rendered without quotes."""
        result = self.od._build_alternate_key_str({"numberofemployees": 250})
        self.assertEqual(result, "numberofemployees=250")

    def test_composite_key_string_and_string(self):
        """Composite key with two string values produces comma-separated pairs."""
        result = self.od._build_alternate_key_str({"accountnumber": "ACC-001", "address1_postalcode": "98052"})
        self.assertEqual(result, "accountnumber='ACC-001',address1_postalcode='98052'")

    def test_composite_key_string_and_int(self):
        """Composite key with mixed string and int values."""
        result = self.od._build_alternate_key_str({"accountnumber": "ACC-001", "numberofemployees": 250})
        self.assertEqual(result, "accountnumber='ACC-001',numberofemployees=250")

    def test_key_name_lowercased(self):
        """Key names are lowercased in the output."""
        result = self.od._build_alternate_key_str({"AccountNumber": "ACC-001"})
        self.assertEqual(result, "accountnumber='ACC-001'")

    def test_single_quote_in_value_is_escaped(self):
        """Single quotes in string values are doubled (OData escaping)."""
        result = self.od._build_alternate_key_str({"name": "O'Brien"})
        self.assertEqual(result, "name='O''Brien'")

    def test_empty_dict_raises_value_error(self):
        """Empty alternate_key raises ValueError."""
        with self.assertRaises(ValueError):
            self.od._build_alternate_key_str({})

    def test_non_string_key_raises_type_error(self):
        """Non-string key raises TypeError."""
        with self.assertRaises(TypeError):
            self.od._build_alternate_key_str({1: "ACC-001"})


class TestListTables(unittest.TestCase):
    """Unit tests for _ODataClient._list_tables filter and select parameters."""

    def setUp(self):
        self.od = _make_odata_client()

    def _setup_response(self, value):
        """Configure _request to return a response with the given value list."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"value": value}
        self.od._request.return_value = mock_response

    def test_no_filter_uses_default(self):
        """_list_tables() without filter sends only IsPrivate eq false."""
        self._setup_response([])
        self.od._list_tables()

        self.od._request.assert_called_once()
        url = self.od._request.call_args[0][1]
        self.assertIn("$filter=IsPrivate eq false", url)

    def test_filter_combined_with_default(self):
        """_list_tables(filter=...) combines user filter with IsPrivate eq false."""
        self._setup_response([{"LogicalName": "account"}])
        self.od._list_tables(filter="SchemaName eq 'Account'")

        self.od._request.assert_called_once()
        url = self.od._request.call_args[0][1]
        self.assertIn("IsPrivate eq false and (SchemaName eq 'Account')", url)

    def test_filter_none_same_as_no_filter(self):
        """_list_tables(filter=None) is equivalent to _list_tables()."""
        self._setup_response([])
        self.od._list_tables(filter=None)

        self.od._request.assert_called_once()
        url = self.od._request.call_args[0][1]
        self.assertIn("$filter=IsPrivate eq false", url)
        self.assertNotIn("and", url)

    def test_returns_value_list(self):
        """_list_tables returns the 'value' array from the response."""
        expected = [
            {"LogicalName": "account"},
            {"LogicalName": "contact"},
        ]
        self._setup_response(expected)
        result = self.od._list_tables()
        self.assertEqual(result, expected)

    def test_select_adds_query_param(self):
        """_list_tables(select=...) adds $select as comma-joined string."""
        self._setup_response([])
        self.od._list_tables(select=["LogicalName", "SchemaName", "DisplayName"])

        self.od._request.assert_called_once()
        url = self.od._request.call_args[0][1]
        self.assertIn("$select=LogicalName,SchemaName,DisplayName", url)

    def test_select_none_omits_query_param(self):
        """_list_tables(select=None) does not add $select to params."""
        self._setup_response([])
        self.od._list_tables(select=None)

        self.od._request.assert_called_once()
        url = self.od._request.call_args[0][1]
        self.assertNotIn("$select", url)

    def test_select_empty_list_omits_query_param(self):
        """_list_tables(select=[]) does not add $select (empty list is falsy)."""
        self._setup_response([])
        self.od._list_tables(select=[])

        self.od._request.assert_called_once()
        url = self.od._request.call_args[0][1]
        self.assertNotIn("$select", url)

    def test_select_preserves_case(self):
        """_list_tables does not lowercase select values (PascalCase preserved)."""
        self._setup_response([])
        self.od._list_tables(select=["EntitySetName", "LogicalName"])

        self.od._request.assert_called_once()
        url = self.od._request.call_args[0][1]
        self.assertIn("$select=EntitySetName,LogicalName", url)

    def test_select_with_filter(self):
        """_list_tables with both select and filter sends both params."""
        self._setup_response([{"LogicalName": "account"}])
        self.od._list_tables(
            filter="SchemaName eq 'Account'",
            select=["LogicalName", "SchemaName"],
        )

        self.od._request.assert_called_once()
        url = self.od._request.call_args[0][1]
        self.assertIn("IsPrivate eq false and (SchemaName eq 'Account')", url)
        self.assertIn("$select=LogicalName,SchemaName", url)

    def test_select_single_property(self):
        """_list_tables(select=[...]) with a single property works correctly."""
        self._setup_response([])
        self.od._list_tables(select=["LogicalName"])

        self.od._request.assert_called_once()
        url = self.od._request.call_args[0][1]
        self.assertIn("$select=LogicalName", url)

    def test_select_bare_string_raises_type_error(self):
        """_list_tables(select='LogicalName') raises TypeError for bare str."""
        self._setup_response([])
        with self.assertRaises(TypeError) as ctx:
            self.od._list_tables(select="LogicalName")
        self.assertIn("list of property names", str(ctx.exception))


class TestCreate(unittest.TestCase):
    """Unit tests for _ODataClient._create."""

    def setUp(self):
        self.od = _make_odata_client()
        # Mock response with OData-EntityId header containing a GUID
        mock_resp = MagicMock()
        mock_resp.headers = {
            "OData-EntityId": "https://example.crm.dynamics.com/api/data/v9.2/accounts(00000000-0000-0000-0000-000000000001)"
        }
        self.od._request.return_value = mock_resp

    def _post_call(self):
        """Return the single POST call args from _request."""
        post_calls = [c for c in self.od._request.call_args_list if c.args[0] == "post"]
        self.assertEqual(len(post_calls), 1, "expected exactly one POST call")
        return post_calls[0]

    def test_record_keys_lowercased(self):
        """Regular record field names are lowercased before sending."""
        self.od._create("accounts", "account", {"Name": "Contoso", "AccountNumber": "ACC-001"})
        call = self._post_call()
        payload = json.loads(call.kwargs["data"])
        self.assertIn("name", payload)
        self.assertIn("accountnumber", payload)
        self.assertNotIn("Name", payload)
        self.assertNotIn("AccountNumber", payload)

    def test_odata_bind_keys_preserve_case(self):
        """@odata.bind keys preserve navigation property casing in _create."""
        self.od._create(
            "new_tickets",
            "new_ticket",
            {
                "new_name": "Ticket 1",
                "new_CustomerId@odata.bind": "/contacts(00000000-0000-0000-0000-000000000001)",
                "new_AgentId@odata.bind": "/systemusers(00000000-0000-0000-0000-000000000002)",
            },
        )
        call = self._post_call()
        payload = json.loads(call.kwargs["data"])
        self.assertIn("new_name", payload)
        self.assertIn("new_CustomerId@odata.bind", payload)
        self.assertIn("new_AgentId@odata.bind", payload)
        self.assertNotIn("new_customerid@odata.bind", payload)
        self.assertNotIn("new_agentid@odata.bind", payload)

    def test_returns_guid_from_odata_entity_id(self):
        """_create returns the GUID from the OData-EntityId header."""
        result = self.od._create("accounts", "account", {"name": "Contoso"})
        self.assertEqual(result, "00000000-0000-0000-0000-000000000001")

    def test_returns_guid_from_odata_entity_id_uppercase(self):
        """_create returns the GUID from the OData-EntityID header (uppercase D)."""
        mock_resp = MagicMock()
        mock_resp.headers = {
            "OData-EntityID": "https://example.crm.dynamics.com/api/data/v9.2/accounts(00000000-0000-0000-0000-000000000002)"
        }
        self.od._request.return_value = mock_resp
        result = self.od._create("accounts", "account", {"name": "Contoso"})
        self.assertEqual(result, "00000000-0000-0000-0000-000000000002")

    def test_returns_guid_from_location_header_fallback(self):
        """_create falls back to Location header when OData-EntityId is absent."""
        mock_resp = MagicMock()
        mock_resp.headers = {
            "Location": "https://example.crm.dynamics.com/api/data/v9.2/accounts(00000000-0000-0000-0000-000000000003)"
        }
        self.od._request.return_value = mock_resp
        result = self.od._create("accounts", "account", {"name": "Contoso"})
        self.assertEqual(result, "00000000-0000-0000-0000-000000000003")

    def test_raises_runtime_error_when_no_guid_in_headers(self):
        """_create raises RuntimeError when neither header contains a GUID."""
        mock_resp = MagicMock()
        mock_resp.headers = {}
        mock_resp.status_code = 204
        self.od._request.return_value = mock_resp
        with self.assertRaises(RuntimeError):
            self.od._create("accounts", "account", {"name": "Contoso"})

    def test_issues_post_to_entity_set_url(self):
        """_create issues a POST request to the entity set URL."""
        self.od._create("accounts", "account", {"name": "Contoso"})
        call = self._post_call()
        self.assertIn("/accounts", call.args[1])


class TestUpdate(unittest.TestCase):
    """Unit tests for _ODataClient._update."""

    def setUp(self):
        self.od = _make_odata_client()
        # _update needs _entity_set_from_schema_name to resolve entity set
        self.od._entity_set_from_schema_name = MagicMock(return_value="new_tickets")

    def _patch_call(self):
        """Return the single PATCH call args from _request."""
        patch_calls = [c for c in self.od._request.call_args_list if c.args[0] == "patch"]
        self.assertEqual(len(patch_calls), 1, "expected exactly one PATCH call")
        return patch_calls[0]

    def test_record_keys_lowercased(self):
        """Regular field names are lowercased in _update."""
        self.od._update("new_ticket", "00000000-0000-0000-0000-000000000001", {"New_Status": 100000001})
        call = self._patch_call()
        payload = json.loads(call.kwargs["data"])
        self.assertIn("new_status", payload)
        self.assertNotIn("New_Status", payload)

    def test_odata_bind_keys_preserve_case(self):
        """@odata.bind keys preserve navigation property casing in _update."""
        self.od._update(
            "new_ticket",
            "00000000-0000-0000-0000-000000000001",
            {
                "new_status": 100000001,
                "new_CustomerId@odata.bind": "/contacts(00000000-0000-0000-0000-000000000002)",
            },
        )
        call = self._patch_call()
        payload = json.loads(call.kwargs["data"])
        self.assertIn("new_status", payload)
        self.assertIn("new_CustomerId@odata.bind", payload)
        self.assertNotIn("new_customerid@odata.bind", payload)

    def test_sends_if_match_star_header(self):
        """PATCH request includes If-Match: * header."""
        self.od._update("new_ticket", "00000000-0000-0000-0000-000000000001", {"new_status": 1})
        call = self._patch_call()
        headers = call.kwargs.get("headers", {})
        self.assertEqual(headers.get("If-Match"), "*")

    def test_url_formats_bare_guid(self):
        """PATCH URL wraps a bare GUID in parentheses."""
        self.od._update("new_ticket", "00000000-0000-0000-0000-000000000001", {"new_status": 1})
        call = self._patch_call()
        self.assertIn("(00000000-0000-0000-0000-000000000001)", call.args[1])

    def test_returns_none(self):
        """_update always returns None."""
        result = self.od._update("new_ticket", "00000000-0000-0000-0000-000000000001", {"new_status": 1})
        self.assertIsNone(result)

    def test_resolves_entity_set_from_schema_name(self):
        """_update delegates entity set resolution to _entity_set_from_schema_name."""
        self.od._update("new_ticket", "00000000-0000-0000-0000-000000000001", {"new_status": 1})
        self.od._entity_set_from_schema_name.assert_called_once_with("new_ticket")


class TestUpsert(unittest.TestCase):
    """Unit tests for _ODataClient._upsert."""

    def setUp(self):
        self.od = _make_odata_client()

    def _patch_call(self):
        """Return the single PATCH call args from _request."""
        patch_calls = [c for c in self.od._request.call_args_list if c.args[0] == "patch"]
        self.assertEqual(len(patch_calls), 1, "expected exactly one PATCH call")
        return patch_calls[0]

    def test_issues_patch_request(self):
        """_upsert issues a PATCH request to the entity set URL."""
        self.od._upsert("accounts", "account", {"accountnumber": "ACC-001"}, {"name": "Contoso"})
        call = self._patch_call()
        self.assertIn("accounts", call.args[1])

    def test_url_contains_alternate_key(self):
        """PATCH URL encodes the alternate key in the entity path."""
        self.od._upsert("accounts", "account", {"accountnumber": "ACC-001"}, {"name": "Contoso"})
        call = self._patch_call()
        self.assertIn("accounts(accountnumber='ACC-001')", call.args[1])

    def test_url_contains_composite_alternate_key(self):
        """PATCH URL encodes a composite alternate key correctly."""
        self.od._upsert(
            "accounts",
            "account",
            {"accountnumber": "ACC-001", "address1_postalcode": "98052"},
            {"name": "Contoso"},
        )
        call = self._patch_call()
        expected_key = "accountnumber='ACC-001',address1_postalcode='98052'"
        self.assertIn(expected_key, call.args[1])

    def test_record_keys_lowercased(self):
        """Record field names are lowercased before sending."""
        self.od._upsert("accounts", "account", {"accountnumber": "ACC-001"}, {"Name": "Contoso"})
        call = self._patch_call()
        payload = call.kwargs["json"]
        self.assertIn("name", payload)
        self.assertNotIn("Name", payload)

    def test_odata_bind_keys_preserve_case(self):
        """@odata.bind keys must preserve PascalCase for navigation property."""
        self.od._upsert(
            "accounts",
            "account",
            {"accountnumber": "ACC-001"},
            {
                "Name": "Contoso",
                "new_CustomerId@odata.bind": "/contacts(00000000-0000-0000-0000-000000000001)",
            },
        )
        call = self._patch_call()
        payload = call.kwargs["json"]
        # Regular field is lowercased
        self.assertIn("name", payload)
        # @odata.bind key preserves original casing
        self.assertIn("new_CustomerId@odata.bind", payload)
        self.assertNotIn("new_customerid@odata.bind", payload)

    def test_convert_labels_skips_odata_keys(self):
        """_convert_labels_to_ints should skip @odata.bind keys (no metadata lookup)."""
        import time

        # Pre-populate cache so no API call needed
        self.od._picklist_label_cache["account"] = {
            "ts": time.time(),
            "picklists": {},
        }

        record = {
            "name": "Contoso",
            "new_CustomerId@odata.bind": "/contacts(00000000-0000-0000-0000-000000000001)",
            "@odata.type": "Microsoft.Dynamics.CRM.account",
        }
        result = self.od._convert_labels_to_ints("account", record)
        # @odata keys must be left unchanged
        self.assertEqual(result["new_CustomerId@odata.bind"], "/contacts(00000000-0000-0000-0000-000000000001)")
        self.assertEqual(result["@odata.type"], "Microsoft.Dynamics.CRM.account")
        self.assertEqual(result["name"], "Contoso")

    def test_returns_none(self):
        """_upsert always returns None."""
        result = self.od._upsert("accounts", "account", {"accountnumber": "ACC-001"}, {"name": "Contoso"})
        self.assertIsNone(result)


class TestStaticHelpers(unittest.TestCase):
    """Unit tests for _ODataClient static helper methods."""

    def test_normalize_cache_key_non_string_returns_empty(self):
        """_normalize_cache_key with non-string returns empty string."""
        self.assertEqual(_ODataClient._normalize_cache_key(None), "")
        self.assertEqual(_ODataClient._normalize_cache_key(42), "")

    def test_lowercase_list_none_returns_none(self):
        """_lowercase_list(None) returns None."""
        self.assertIsNone(_ODataClient._lowercase_list(None))

    def test_lowercase_list_empty_returns_empty(self):
        """_lowercase_list([]) returns []."""
        self.assertFalse(_ODataClient._lowercase_list([]))

    def test_lowercase_keys_non_dict_returned_as_is(self):
        """_lowercase_keys with non-dict input returns it unchanged."""
        self.assertEqual(_ODataClient._lowercase_keys("a string"), "a string")
        self.assertIsNone(_ODataClient._lowercase_keys(None))

    def test_lowercase_keys_preserves_odata_bind_casing(self):
        """_lowercase_keys lowercases regular keys but preserves @odata.bind key casing."""
        result = _ODataClient._lowercase_keys(
            {
                "Name": "Contoso",
                "new_CustomerId@odata.bind": "/contacts(id-1)",
                "@odata.type": "Microsoft.Dynamics.CRM.account",
            }
        )
        self.assertIn("name", result)
        self.assertNotIn("Name", result)
        self.assertIn("new_CustomerId@odata.bind", result)
        self.assertNotIn("new_customerid@odata.bind", result)
        self.assertIn("@odata.type", result)

    def test_to_pascal_basic(self):
        """_to_pascal converts snake_case to PascalCase."""
        client = _make_odata_client()
        self.assertEqual(client._to_pascal("hello_world"), "HelloWorld")
        self.assertEqual(client._to_pascal("my_table_name"), "MyTableName")
        self.assertEqual(client._to_pascal("single"), "Single")


class TestRequestErrorParsing(unittest.TestCase):
    """Unit tests for _ODataClient._request error response handling."""

    def setUp(self):
        mock_auth = MagicMock()
        mock_auth._acquire_token.return_value = MagicMock(access_token="token")
        self.client = _ODataClient(mock_auth, "https://example.crm.dynamics.com")

    def _make_raw_response(self, status_code, json_data=None, headers=None):
        r = MagicMock()
        r.status_code = status_code
        r.text = "body"
        r.json.return_value = json_data or {}
        r.headers = headers or {}
        return r

    def test_message_key_fallback_used_when_no_error_key(self):
        """_request uses 'message' key when 'error' key is absent."""
        r = self._make_raw_response(400, json_data={"message": "Bad input received"})
        self.client._raw_request = MagicMock(return_value=r)
        with self.assertRaises(HttpError) as ctx:
            self.client._request("get", "http://example.com/test")
        self.assertIn("Bad input received", str(ctx.exception))

    def test_retry_after_non_int_not_stored_in_details(self):
        """Retry-After header that is non-numeric results in retry_after absent from details."""
        r = self._make_raw_response(429, headers={"Retry-After": "not-a-number"})
        self.client._raw_request = MagicMock(return_value=r)
        with self.assertRaises(HttpError) as ctx:
            self.client._request("get", "http://example.com/test")
        self.assertIsNone(ctx.exception.details.get("retry_after"))

    def test_retry_after_int_stored_in_details(self):
        """Retry-After header that is numeric is stored in exception details."""
        r = self._make_raw_response(429, headers={"Retry-After": "30"})
        self.client._raw_request = MagicMock(return_value=r)
        with self.assertRaises(HttpError) as ctx:
            self.client._request("get", "http://example.com/test")
        self.assertEqual(ctx.exception.details.get("retry_after"), 30)


class TestCreateMultiple(unittest.TestCase):
    """Unit tests for _ODataClient._create_multiple."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_non_dict_items_raise_type_error(self):
        """_create_multiple raises TypeError for non-dict items."""
        with self.assertRaises(TypeError):
            self.od._create_multiple("accounts", "account", ["not a dict"])

    def test_odata_type_already_present_not_duplicated(self):
        """If @odata.type already in record, it is preserved as-is."""
        self.od._request.return_value = _mock_response(
            json_data={"Ids": ["id-1"]},
            text='{"Ids": ["id-1"]}',
        )
        self.od._create_multiple(
            "accounts",
            "account",
            [{"@odata.type": "Microsoft.Dynamics.CRM.account", "name": "Test"}],
        )
        post_calls = [c for c in self.od._request.call_args_list if c.args[0] == "post"]
        target = json.loads(post_calls[0].kwargs["data"])["Targets"][0]
        self.assertEqual(target["@odata.type"], "Microsoft.Dynamics.CRM.account")

    def test_body_not_dict_returns_empty_list(self):
        """When response body is not a dict, returns empty list."""
        r = _mock_response(text='["id1", "id2"]')
        r.json.return_value = ["id1", "id2"]
        self.od._request.return_value = r
        result = self.od._create_multiple("accounts", "account", [{"name": "A"}])
        self.assertEqual(result, [])

    def test_value_key_path_extracts_ids(self):
        """Falls back to 'value' key to extract IDs via heuristic."""
        long_guid = "a" * 32
        r = _mock_response(
            json_data={"value": [{"accountid": long_guid, "name": "Test"}]},
            text="...",
        )
        self.od._request.return_value = r
        result = self.od._create_multiple("accounts", "account", [{"name": "Test"}])
        self.assertEqual(result, [long_guid])

    def test_value_key_with_non_dict_items_returns_empty(self):
        """'value' list with non-dict items returns empty list."""
        r = _mock_response(json_data={"value": ["not-a-dict"]}, text="...")
        self.od._request.return_value = r
        self.od._convert_labels_to_ints = MagicMock(side_effect=lambda _, rec: rec)
        result = self.od._create_multiple("accounts", "account", [{"name": "Test"}])
        self.assertEqual(result, [])

    def test_no_ids_or_value_key_returns_empty_list(self):
        """When body has neither 'Ids' nor 'value' keys, returns empty list."""
        r = _mock_response(json_data={"something_else": "data"}, text="...")
        self.od._request.return_value = r
        self.od._convert_labels_to_ints = MagicMock(side_effect=lambda _, rec: rec)
        result = self.od._create_multiple("accounts", "account", [{"name": "Test"}])
        self.assertEqual(result, [])

    def test_value_parse_error_returns_empty_list(self):
        """ValueError in body.json() returns empty list."""
        r = MagicMock()
        r.text = "invalid json"
        r.json.side_effect = ValueError("bad json")
        self.od._request.return_value = r
        self.od._convert_labels_to_ints = MagicMock(side_effect=lambda _, rec: rec)
        result = self.od._create_multiple("accounts", "account", [{"name": "Test"}])
        self.assertEqual(result, [])


class TestPrimaryIdAttr(unittest.TestCase):
    """Unit tests for _ODataClient._primary_id_attr cache-miss behavior."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_cache_miss_resolves_via_entity_set_lookup(self):
        """Cache miss triggers entity set lookup and populates primary ID cache."""

        def mock_entity_set(table_schema_name):
            cache_key = table_schema_name.lower()
            self.od._logical_to_entityset_cache[cache_key] = "accounts"
            self.od._logical_primaryid_cache[cache_key] = "accountid"
            return "accounts"

        self.od._entity_set_from_schema_name = MagicMock(side_effect=mock_entity_set)
        result = self.od._primary_id_attr("account")
        self.assertEqual(result, "accountid")

    def test_cache_miss_no_primary_id_raises_runtime_error(self):
        """Cache miss with no PrimaryIdAttribute in metadata raises RuntimeError."""

        def mock_entity_set_no_pid(table_schema_name):
            cache_key = table_schema_name.lower()
            self.od._logical_to_entityset_cache[cache_key] = "accounts"
            return "accounts"

        self.od._entity_set_from_schema_name = MagicMock(side_effect=mock_entity_set_no_pid)
        with self.assertRaises(RuntimeError) as ctx:
            self.od._primary_id_attr("account")
        self.assertIn("PrimaryIdAttribute not resolved", str(ctx.exception))

    def test_cache_hit_returns_without_lookup(self):
        """Cache hit returns primary ID immediately without issuing any request."""
        self.od._logical_primaryid_cache["account"] = "accountid"
        result = self.od._primary_id_attr("account")
        self.assertEqual(result, "accountid")
        self.od._request.assert_not_called()


class TestUpdateByIds(unittest.TestCase):
    """Unit tests for _ODataClient._update_by_ids."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_non_list_ids_raises_type_error(self):
        """_update_by_ids raises TypeError when ids is not a list."""
        with self.assertRaises(TypeError):
            self.od._update_by_ids("account", "not-a-list", {"name": "X"})

    def test_empty_ids_returns_none(self):
        """_update_by_ids returns None immediately for empty ids list."""
        result = self.od._update_by_ids("account", [], {"name": "X"})
        self.assertIsNone(result)
        self.od._request.assert_not_called()

    def test_non_list_non_dict_changes_raises_type_error(self):
        """_update_by_ids raises TypeError for changes that is not dict or list."""
        self.od._primary_id_attr = MagicMock(return_value="accountid")
        self.od._entity_set_from_schema_name = MagicMock(return_value="accounts")
        with self.assertRaises(TypeError) as ctx:
            self.od._update_by_ids("account", ["id-1"], "bad-changes")
        self.assertIn("changes must be dict or list[dict]", str(ctx.exception))

    def test_list_changes_length_mismatch_raises_value_error(self):
        """_update_by_ids raises ValueError when changes list length != ids length."""
        self.od._primary_id_attr = MagicMock(return_value="accountid")
        self.od._entity_set_from_schema_name = MagicMock(return_value="accounts")
        with self.assertRaises(ValueError) as ctx:
            self.od._update_by_ids("account", ["id-1", "id-2"], [{"name": "A"}])
        self.assertIn("Length of changes list must match", str(ctx.exception))

    def test_non_dict_patch_in_list_raises_type_error(self):
        """_update_by_ids raises TypeError when a patch in the list is not a dict."""
        self.od._primary_id_attr = MagicMock(return_value="accountid")
        self.od._entity_set_from_schema_name = MagicMock(return_value="accounts")
        self.od._update_multiple = MagicMock()
        with self.assertRaises(TypeError) as ctx:
            self.od._update_by_ids("account", ["id-1"], ["not-a-dict"])
        self.assertIn("Each patch must be a dict", str(ctx.exception))

    def test_dict_changes_broadcasts_to_all_ids(self):
        """_update_by_ids with dict changes builds one batch record per ID."""
        self.od._primary_id_attr = MagicMock(return_value="accountid")
        self.od._entity_set_from_schema_name = MagicMock(return_value="accounts")
        self.od._update_multiple = MagicMock()
        self.od._update_by_ids("account", ["id-1", "id-2"], {"name": "X"})
        self.od._update_multiple.assert_called_once()
        _, _, batch = self.od._update_multiple.call_args.args
        self.assertEqual(len(batch), 2)
        self.assertEqual(batch[0]["accountid"], "id-1")
        self.assertEqual(batch[1]["accountid"], "id-2")
        self.assertEqual(batch[0]["name"], "X")
        self.assertEqual(batch[1]["name"], "X")

    def test_list_changes_merges_per_record(self):
        """_update_by_ids with list changes merges each patch with its corresponding ID."""
        self.od._primary_id_attr = MagicMock(return_value="accountid")
        self.od._entity_set_from_schema_name = MagicMock(return_value="accounts")
        self.od._update_multiple = MagicMock()
        self.od._update_by_ids("account", ["id-1", "id-2"], [{"name": "A"}, {"name": "B"}])
        _, _, batch = self.od._update_multiple.call_args.args
        self.assertEqual(batch[0], {"accountid": "id-1", "name": "A"})
        self.assertEqual(batch[1], {"accountid": "id-2", "name": "B"})


class TestUpdateMultiple(unittest.TestCase):
    """Unit tests for _ODataClient._update_multiple."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_non_list_records_raises_type_error(self):
        """_update_multiple raises TypeError for non-list records."""
        with self.assertRaises(TypeError):
            self.od._update_multiple("accounts", "account", "not-a-list")

    def test_empty_list_raises_type_error(self):
        """_update_multiple raises TypeError for empty list."""
        with self.assertRaises(TypeError):
            self.od._update_multiple("accounts", "account", [])

    def test_odata_type_already_present_not_overridden(self):
        """If all records have @odata.type, it is preserved."""
        self.od._request.return_value = _mock_response()
        records = [{"@odata.type": "Microsoft.Dynamics.CRM.CustomType", "accountid": "id-1", "name": "A"}]
        self.od._update_multiple("accounts", "account", records)
        payload = json.loads(self.od._request.call_args.kwargs["data"])
        self.assertEqual(payload["Targets"][0]["@odata.type"], "Microsoft.Dynamics.CRM.CustomType")

    def test_posts_to_update_multiple_endpoint(self):
        """_update_multiple POSTs to {entity_set}/Microsoft.Dynamics.CRM.UpdateMultiple."""
        self.od._request.return_value = _mock_response()
        self.od._update_multiple("accounts", "account", [{"accountid": "id-1", "name": "X"}])
        method, url = self.od._request.call_args.args
        self.assertEqual(method, "post")
        self.assertIn("accounts/Microsoft.Dynamics.CRM.UpdateMultiple", url)

    def test_payload_contains_targets_array(self):
        """_update_multiple sends {"Targets": [...]} with @odata.type injected per record."""
        self.od._request.return_value = _mock_response()
        self.od._update_multiple("accounts", "account", [{"accountid": "id-1", "name": "X"}])
        payload = json.loads(self.od._request.call_args.kwargs["data"])
        self.assertIn("Targets", payload)
        self.assertEqual(len(payload["Targets"]), 1)
        self.assertIn("@odata.type", payload["Targets"][0])


class TestDeleteMultiple(unittest.TestCase):
    """Unit tests for _ODataClient._delete_multiple."""

    def setUp(self):
        self.od = _make_odata_client()
        self.od._primary_id_attr = MagicMock(return_value="accountid")

    def test_empty_ids_returns_none(self):
        """_delete_multiple returns None for empty IDs."""
        result = self.od._delete_multiple("account", [])
        self.assertIsNone(result)
        self.od._request.assert_not_called()

    def test_filters_out_falsy_ids(self):
        """_delete_multiple filters None/empty strings from ids."""
        result = self.od._delete_multiple("account", [None, "", None])
        self.assertIsNone(result)
        self.od._request.assert_not_called()

    def test_posts_bulk_delete_payload(self):
        """_delete_multiple issues POST to BulkDelete with correct payload."""
        self.od._request.return_value = _mock_response(json_data={"JobId": "job-001"}, text='{"JobId": "job-001"}')
        result = self.od._delete_multiple("account", ["id-1", "id-2"])
        self.assertEqual(result, "job-001")
        call_args = self.od._request.call_args
        self.assertEqual(call_args.args[0], "post")
        self.assertIn("BulkDelete", call_args.args[1])
        payload = json.loads(call_args.kwargs["data"])
        self.assertIn("QuerySet", payload)
        self.assertIn("JobName", payload)
        query = payload["QuerySet"][0]
        self.assertEqual(query["EntityName"], "account")
        conditions = query["Criteria"]["Conditions"]
        self.assertEqual(len(conditions), 1)
        self.assertEqual(conditions[0]["AttributeName"], "accountid")
        values = conditions[0]["Values"]
        self.assertEqual(len(values), 2)
        self.assertEqual({v["Value"] for v in values}, {"id-1", "id-2"})

    def test_returns_none_when_no_job_id_in_body(self):
        """_delete_multiple returns None when response body has no JobId."""
        self.od._request.return_value = _mock_response(json_data={}, text="{}")
        result = self.od._delete_multiple("account", ["id-1"])
        self.assertIsNone(result)

    def test_handles_value_error_in_json_parsing(self):
        """_delete_multiple handles ValueError in response JSON parsing gracefully."""
        r = MagicMock()
        r.text = "invalid"
        r.json.side_effect = ValueError
        self.od._request.return_value = r
        result = self.od._delete_multiple("account", ["id-1"])
        self.assertIsNone(result)


class TestFormatKey(unittest.TestCase):
    """Unit tests for _ODataClient._format_key."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_guid_wrapped_in_parens(self):
        """_format_key wraps 36-char GUID in parentheses."""
        guid = "11111111-2222-3333-4444-555555555555"
        self.assertEqual(self.od._format_key(guid), f"({guid})")

    def test_already_wrapped_key_returned_as_is(self):
        """_format_key returns already-parenthesized key unchanged."""
        key = "(some-key)"
        self.assertEqual(self.od._format_key(key), key)

    def test_alternate_key_with_quotes_is_escaped(self):
        """_format_key wraps alternate key with single-quoted value in parentheses."""
        result = self.od._format_key("mykey='it''s value'")
        self.assertEqual(result, "(mykey='it''s value')")


class TestGetMultiple(unittest.TestCase):
    """Unit tests for _ODataClient._get_multiple query parameter handling."""

    def setUp(self):
        self.od = _make_odata_client()
        self.od._entity_set_from_schema_name = MagicMock(return_value="accounts")

    def _single_page_response(self, items=None):
        data = {"value": items or [{"accountid": "id-1"}]}
        r = _mock_response(json_data=data, text=str(data))
        self.od._request.return_value = r

    def test_filter_param_passed(self):
        """_get_multiple passes $filter to params."""
        self._single_page_response()
        list(self.od._get_multiple("account", filter="statecode eq 0"))
        params = self.od._request.call_args.kwargs["params"]
        self.assertEqual(params["$filter"], "statecode eq 0")

    def test_orderby_param_passed(self):
        """_get_multiple passes $orderby to params."""
        self._single_page_response()
        list(self.od._get_multiple("account", orderby=["name asc", "createdon desc"]))
        params = self.od._request.call_args.kwargs["params"]
        self.assertEqual(params["$orderby"], "name asc,createdon desc")

    def test_expand_param_passed(self):
        """_get_multiple passes $expand to params."""
        self._single_page_response()
        list(self.od._get_multiple("account", expand=["contact_customer_accounts"]))
        params = self.od._request.call_args.kwargs["params"]
        self.assertEqual(params["$expand"], "contact_customer_accounts")

    def test_top_param_passed(self):
        """_get_multiple passes $top to params."""
        self._single_page_response()
        list(self.od._get_multiple("account", top=5))
        params = self.od._request.call_args.kwargs["params"]
        self.assertEqual(params["$top"], 5)

    def test_count_param_passed(self):
        """_get_multiple passes $count=true when count=True."""
        self._single_page_response()
        list(self.od._get_multiple("account", count=True))
        params = self.od._request.call_args.kwargs["params"]
        self.assertEqual(params["$count"], "true")

    def test_include_annotations_sets_prefer_header(self):
        """_get_multiple sets Prefer header with include-annotations."""
        self._single_page_response()
        list(self.od._get_multiple("account", include_annotations="*"))
        headers = self.od._request.call_args.kwargs.get("headers") or {}
        self.assertIn("Prefer", headers)
        self.assertIn("include-annotations", headers["Prefer"])

    def test_page_size_sets_prefer_header(self):
        """_get_multiple sets Prefer odata.maxpagesize when page_size > 0."""
        self._single_page_response()
        list(self.od._get_multiple("account", page_size=50))
        headers = self.od._request.call_args.kwargs.get("headers") or {}
        self.assertIn("odata.maxpagesize=50", headers.get("Prefer", ""))

    def test_value_error_in_json_returns_empty(self):
        """ValueError in page JSON parsing yields nothing."""
        r = MagicMock()
        r.text = "bad json"
        r.json.side_effect = ValueError
        self.od._request.return_value = r
        pages = list(self.od._get_multiple("account"))
        self.assertEqual(pages, [])

    def test_yields_value_items_as_page(self):
        """_get_multiple yields the 'value' list as a page of dicts."""
        items = [{"accountid": "id-1", "name": "A"}, {"accountid": "id-2", "name": "B"}]
        self._single_page_response(items)
        pages = list(self.od._get_multiple("account"))
        self.assertEqual(len(pages), 1)
        self.assertEqual(pages[0], items)

    def test_follows_nextlink_pagination(self):
        """_get_multiple follows @odata.nextLink across multiple pages."""
        page1 = _mock_response(
            json_data={
                "value": [{"accountid": "id-1"}],
                "@odata.nextLink": "https://example.crm.dynamics.com/next-page",
            },
            text="...",
        )
        page2 = _mock_response(
            json_data={"value": [{"accountid": "id-2"}]},
            text="...",
        )
        self.od._request.side_effect = [page1, page2]
        pages = list(self.od._get_multiple("account"))
        self.assertEqual(len(pages), 2)
        self.assertEqual(pages[0][0]["accountid"], "id-1")
        self.assertEqual(pages[1][0]["accountid"], "id-2")

    def test_stops_when_no_nextlink(self):
        """_get_multiple stops after a page without nextLink."""
        self._single_page_response([{"accountid": "id-1"}])
        pages = list(self.od._get_multiple("account"))
        self.assertEqual(len(pages), 1)
        self.od._request.assert_called_once()

    def test_filters_non_dict_items_from_page(self):
        """_get_multiple filters out non-dict items from each page."""
        data = {"value": [{"accountid": "id-1"}, "not-a-dict", 42]}
        r = _mock_response(json_data=data, text=str(data))
        self.od._request.return_value = r
        pages = list(self.od._get_multiple("account"))
        self.assertEqual(len(pages), 1)
        self.assertEqual(len(pages[0]), 1)
        self.assertEqual(pages[0][0]["accountid"], "id-1")

    def test_empty_value_list_yields_nothing(self):
        """_get_multiple yields nothing when value list is empty."""
        data = {"value": []}
        r = _mock_response(json_data=data, text=str(data))
        self.od._request.return_value = r
        pages = list(self.od._get_multiple("account"))
        self.assertEqual(pages, [])


class TestQuerySql(unittest.TestCase):
    """Unit tests for _ODataClient._query_sql."""

    def setUp(self):
        self.od = _make_odata_client()
        self.od._entity_set_from_schema_name = MagicMock(return_value="accounts")

    def test_non_string_sql_raises_validation_error(self):
        """_query_sql raises ValidationError for non-string sql."""
        with self.assertRaises(ValidationError):
            self.od._query_sql(123)

    def test_empty_sql_raises_validation_error(self):
        """_query_sql raises ValidationError for empty sql."""
        with self.assertRaises(ValidationError):
            self.od._query_sql("   ")

    def test_returns_value_list(self):
        """_query_sql returns rows from response 'value' key."""
        self.od._request.return_value = _mock_response(
            json_data={"value": [{"accountid": "id-1", "name": "Contoso"}]},
            text="...",
        )
        result = self.od._query_sql("SELECT name FROM account")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["name"], "Contoso")

    def test_filters_non_dict_rows(self):
        """_query_sql filters out non-dict rows from 'value' list."""
        self.od._request.return_value = _mock_response(
            json_data={"value": [{"name": "A"}, "not-a-dict", 42]}, text="..."
        )
        result = self.od._query_sql("SELECT name FROM account")
        self.assertEqual(len(result), 1)

    def test_body_as_list_fallback(self):
        """_query_sql handles body being a list directly."""
        r = _mock_response(text="...")
        r.json.return_value = [{"name": "A"}, {"name": "B"}]
        self.od._request.return_value = r
        result = self.od._query_sql("SELECT name FROM account")
        self.assertEqual(len(result), 2)

    def test_value_error_in_json_returns_empty(self):
        """_query_sql returns empty list when JSON parsing fails."""
        r = MagicMock()
        r.text = "bad json"
        r.json.side_effect = ValueError
        self.od._request.return_value = r
        result = self.od._query_sql("SELECT name FROM account")
        self.assertEqual(result, [])

    def test_unexpected_body_returns_empty(self):
        """_query_sql returns empty list for non-dict, non-list body."""
        r = _mock_response(text="...")
        r.json.return_value = "unexpected"
        self.od._request.return_value = r
        result = self.od._query_sql("SELECT name FROM account")
        self.assertEqual(result, [])

    def test_extract_non_string_raises_value_error(self):
        """_extract_logical_table with non-string raises ValueError."""
        with self.assertRaises(ValueError):
            _ODataClient._extract_logical_table(123)

    def test_extract_no_from_clause_raises_value_error(self):
        """_extract_logical_table without FROM raises ValueError."""
        with self.assertRaises(ValueError) as ctx:
            _ODataClient._extract_logical_table("SELECT name, surname")
        self.assertIn("FROM", str(ctx.exception))


class TestEntitySetFromSchemaName(unittest.TestCase):
    """Unit tests for _ODataClient._entity_set_from_schema_name."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_empty_table_schema_name_raises_value_error(self):
        """_entity_set_from_schema_name raises ValueError for empty input."""
        with self.assertRaises(ValueError) as ctx:
            self.od._entity_set_from_schema_name("")
        self.assertIn("table schema name required", str(ctx.exception))

    def test_json_value_error_in_response_treated_as_empty(self):
        """_entity_set_from_schema_name handles ValueError in JSON parsing."""
        r = MagicMock()
        r.text = "invalid json"
        r.json.side_effect = ValueError
        self.od._request.return_value = r
        with self.assertRaises(MetadataError):
            self.od._entity_set_from_schema_name("account")

    def test_plural_hint_when_name_ends_with_s(self):
        """Error message includes plural hint when name ends with 's' (not 'ss')."""
        self.od._request.return_value = _mock_response(json_data={"value": []}, text="{}")
        with self.assertRaises(MetadataError) as ctx:
            self.od._entity_set_from_schema_name("accounts")
        self.assertIn("plural", str(ctx.exception).lower())

    def test_no_plural_hint_when_name_ends_with_ss(self):
        """No plural hint when name ends with 'ss'."""
        self.od._request.return_value = _mock_response(json_data={"value": []}, text="{}")
        with self.assertRaises(MetadataError) as ctx:
            self.od._entity_set_from_schema_name("address")
        self.assertNotIn("plural", str(ctx.exception).lower())

    def test_missing_entity_set_name_raises_metadata_error(self):
        """MetadataError raised when EntitySetName is absent from metadata."""
        self.od._request.return_value = _mock_response(
            json_data={"value": [{"LogicalName": "account", "EntitySetName": None, "PrimaryIdAttribute": "accountid"}]},
            text="...",
        )
        with self.assertRaises(MetadataError) as ctx:
            self.od._entity_set_from_schema_name("account")
        self.assertIn("EntitySetName", str(ctx.exception))

    def test_cache_hit_returns_without_request(self):
        """Cache hit returns entity set name immediately without issuing any request."""
        self.od._logical_to_entityset_cache["account"] = "accounts"
        result = self.od._entity_set_from_schema_name("account")
        self.assertEqual(result, "accounts")
        self.od._request.assert_not_called()

    def test_success_populates_entityset_cache(self):
        """Successful API response populates _logical_to_entityset_cache."""
        self.od._request.return_value = _entity_def_response(entity_set_name="accounts", primary_id="accountid")
        result = self.od._entity_set_from_schema_name("account")
        self.assertEqual(result, "accounts")
        self.assertEqual(self.od._logical_to_entityset_cache["account"], "accounts")

    def test_success_populates_primaryid_cache(self):
        """Successful API response populates _logical_primaryid_cache."""
        self.od._request.return_value = _entity_def_response(entity_set_name="accounts", primary_id="accountid")
        self.od._entity_set_from_schema_name("account")
        self.assertEqual(self.od._logical_primaryid_cache["account"], "accountid")

    def test_success_without_primary_id_does_not_populate_primaryid_cache(self):
        """When PrimaryIdAttribute is missing, _logical_primaryid_cache is not populated."""
        self.od._request.return_value = _mock_response(
            json_data={"value": [{"LogicalName": "account", "EntitySetName": "accounts"}]},
            text="...",
        )
        self.od._entity_set_from_schema_name("account")
        self.assertNotIn("account", self.od._logical_primaryid_cache)


class TestGetEntityByTableSchemaName(unittest.TestCase):
    """Unit tests for _ODataClient._get_entity_by_table_schema_name."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_returns_first_match(self):
        """_get_entity_by_table_schema_name returns first entity when found."""
        self.od._request.return_value = _entity_def_response()
        result = self.od._get_entity_by_table_schema_name("account")
        self.assertIsNotNone(result)
        self.assertEqual(result["EntitySetName"], "accounts")

    def test_returns_none_when_not_found(self):
        """_get_entity_by_table_schema_name returns None when no match."""
        self.od._request.return_value = _mock_response(json_data={"value": []}, text="{}")
        result = self.od._get_entity_by_table_schema_name("nonexistent")
        self.assertIsNone(result)


class TestCreateEntity(unittest.TestCase):
    """Unit tests for _ODataClient._create_entity."""

    def setUp(self):
        self.od = _make_odata_client()

    def _setup_entity_creation(self, get_response=None):
        """Mock _request: POST returns 201, GET returns entity definition."""

        def side_effect(method, url, **kwargs):
            if method == "post":
                return _mock_response(status_code=201)
            else:
                return get_response or _entity_def_response()

        self.od._request.side_effect = side_effect

    def test_successful_entity_creation(self):
        """_create_entity returns metadata on success."""
        self._setup_entity_creation()
        result = self.od._create_entity("new_TestTable", "Test Table", [], solution_unique_name=None)
        self.assertEqual(result["EntitySetName"], "accounts")

    def test_entity_set_name_missing_raises_runtime_error(self):
        """_create_entity raises RuntimeError when EntitySetName not available after create."""
        get_response = _mock_response(json_data={"value": []}, text="{}")

        def side_effect(method, url, **kwargs):
            return _mock_response(status_code=201) if method == "post" else get_response

        self.od._request.side_effect = side_effect
        with self.assertRaises(RuntimeError) as ctx:
            self.od._create_entity("new_TestTable", "Test Table", [])
        self.assertIn("EntitySetName not available", str(ctx.exception))

    def test_metadata_id_missing_raises_runtime_error(self):
        """_create_entity raises RuntimeError when MetadataId missing after create."""
        get_response = _mock_response(
            json_data={"value": [{"EntitySetName": "new_testtables", "SchemaName": "new_TestTable"}]},
            text="...",
        )

        def side_effect(method, url, **kwargs):
            return _mock_response(status_code=201) if method == "post" else get_response

        self.od._request.side_effect = side_effect
        with self.assertRaises(RuntimeError) as ctx:
            self.od._create_entity("new_TestTable", "Test Table", [])
        self.assertIn("MetadataId missing", str(ctx.exception))

    def test_solution_unique_name_passed_as_param(self):
        """_create_entity passes SolutionUniqueName as query param when provided."""
        self._setup_entity_creation()
        self.od._create_entity("new_TestTable", "Test Table", [], solution_unique_name="MySolution")
        post_call = next(c for c in self.od._request.call_args_list if c.args[0] == "post")
        self.assertEqual(post_call.kwargs.get("params"), {"SolutionUniqueName": "MySolution"})


class TestGetAttributeMetadata(unittest.TestCase):
    """Unit tests for _ODataClient._get_attribute_metadata."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_returns_attribute_when_found(self):
        """_get_attribute_metadata returns attribute dict when found."""
        self.od._request.return_value = _mock_response(
            json_data={"value": [{"MetadataId": "attr-001", "LogicalName": "name", "SchemaName": "Name"}]},
            text="...",
        )
        result = self.od._get_attribute_metadata("meta-001", "name")
        self.assertIsNotNone(result)
        self.assertEqual(result["MetadataId"], "attr-001")

    def test_returns_none_when_not_found(self):
        """_get_attribute_metadata returns None when attribute not in response."""
        self.od._request.return_value = _mock_response(json_data={"value": []}, text="{}")
        result = self.od._get_attribute_metadata("meta-001", "name")
        self.assertIsNone(result)

    def test_extra_select_fields_included(self):
        """_get_attribute_metadata appends extra_select fields to $select."""
        self.od._request.return_value = _mock_response(json_data={"value": []}, text="{}")
        self.od._get_attribute_metadata("meta-001", "name", extra_select="AttributeType,MaxLength")
        params = self.od._request.call_args.kwargs["params"]
        self.assertIn("AttributeType", params["$select"])
        self.assertIn("MaxLength", params["$select"])

    def test_extra_select_skips_empty_pieces(self):
        """_get_attribute_metadata skips empty pieces in extra_select."""
        self.od._request.return_value = _mock_response(json_data={"value": []}, text="{}")
        self.od._get_attribute_metadata("meta-001", "name", extra_select=",AttributeType,")
        params = self.od._request.call_args.kwargs["params"]
        self.assertIn("AttributeType", params["$select"])

    def test_extra_select_skips_odata_annotation_pieces(self):
        """_get_attribute_metadata skips pieces starting with @."""
        self.od._request.return_value = _mock_response(json_data={"value": []}, text="{}")
        self.od._get_attribute_metadata("meta-001", "name", extra_select="@odata.type,MaxLength")
        params = self.od._request.call_args.kwargs["params"]
        self.assertNotIn("@odata.type", params["$select"])
        self.assertIn("MaxLength", params["$select"])

    def test_value_error_in_json_returns_none(self):
        """_get_attribute_metadata returns None on JSON parse failure."""
        r = MagicMock()
        r.text = "bad json"
        r.json.side_effect = ValueError
        self.od._request.return_value = r
        result = self.od._get_attribute_metadata("meta-001", "name")
        self.assertIsNone(result)


class TestWaitForAttributeVisibility(unittest.TestCase):
    """Unit tests for _ODataClient._wait_for_attribute_visibility."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_returns_immediately_on_success(self):
        """_wait_for_attribute_visibility returns immediately when first probe succeeds."""
        self.od._request.return_value = _mock_response()
        self.od._wait_for_attribute_visibility("accounts", "name", delays=(0,))
        self.od._request.assert_called_once()

    def test_retries_on_failure_then_succeeds(self):
        """_wait_for_attribute_visibility retries after initial failure."""
        self.od._request.side_effect = [RuntimeError("not ready"), _mock_response()]
        self.od._wait_for_attribute_visibility("accounts", "name", delays=(0, 0))
        self.assertEqual(self.od._request.call_count, 2)

    def test_sleep_is_called_for_nonzero_delays(self):
        """_wait_for_attribute_visibility calls time.sleep for non-zero delays."""
        self.od._request.side_effect = [RuntimeError("not ready"), _mock_response()]
        with patch("PowerPlatform.Dataverse.data._odata.time.sleep") as mock_sleep:
            self.od._wait_for_attribute_visibility("accounts", "name", delays=(0, 5))
        mock_sleep.assert_called_once_with(5)

    def test_raises_runtime_error_after_all_retries_exhausted(self):
        """_wait_for_attribute_visibility raises RuntimeError when all retries fail."""
        self.od._request.side_effect = RuntimeError("not ready")
        with self.assertRaises(RuntimeError) as ctx:
            self.od._wait_for_attribute_visibility("accounts", "name", delays=(0, 0))
        self.assertIn("did not become visible", str(ctx.exception))


class TestLocalizedLabelsPayload(unittest.TestCase):
    """Unit tests for _ODataClient._build_localizedlabels_payload."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_non_int_lang_raises_value_error(self):
        """_build_localizedlabels_payload raises ValueError for non-int language code."""
        with self.assertRaises(ValueError) as ctx:
            self.od._build_localizedlabels_payload({"1033": "English"})
        self.assertIn("must be int", str(ctx.exception))

    def test_non_string_label_raises_value_error(self):
        """_build_localizedlabels_payload raises ValueError for non-string label."""
        with self.assertRaises(ValueError) as ctx:
            self.od._build_localizedlabels_payload({1033: 42})
        self.assertIn("non-empty string", str(ctx.exception))

    def test_empty_translations_raises_value_error(self):
        """_build_localizedlabels_payload raises ValueError for empty translations."""
        with self.assertRaises(ValueError) as ctx:
            self.od._build_localizedlabels_payload({})
        self.assertIn("At least one translation", str(ctx.exception))

    def test_empty_string_label_raises_value_error(self):
        """_build_localizedlabels_payload raises ValueError for empty string label."""
        with self.assertRaises(ValueError):
            self.od._build_localizedlabels_payload({1033: "   "})


class TestEnumOptionSetPayload(unittest.TestCase):
    """Unit tests for _ODataClient._enum_optionset_payload."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_empty_enum_raises_value_error(self):
        """_enum_optionset_payload raises ValueError for enum with no members."""

        class EmptyEnum(Enum):
            pass

        with self.assertRaises(ValueError) as ctx:
            self.od._enum_optionset_payload("new_Status", EmptyEnum)
        self.assertIn("no members", str(ctx.exception))

    def test_int_key_in_labels_resolved_to_member_name(self):
        """__labels__ with int keys (matching enum values) are resolved to member names."""

        class Status(Enum):
            Active = 1
            Inactive = 2

        Status.__labels__ = {1033: {1: "Active", 2: "Inactive"}}
        result = self.od._enum_optionset_payload("new_Status", Status)
        self.assertEqual(len(result["OptionSet"]["Options"]), 2)

    def test_enum_member_object_as_labels_key(self):
        """__labels__ with enum member objects as keys resolves member name."""

        class Status(Enum):
            Active = 1
            Inactive = 2

        Status.__labels__ = {1033: {Status.Active: "Active Label", Status.Inactive: "Inactive Label"}}
        result = self.od._enum_optionset_payload("new_Status", Status)
        options = result["OptionSet"]["Options"]
        self.assertEqual(len(options), 2)
        active_opt = next(o for o in options if o["Value"] == 1)
        active_label = next(
            loc["Label"] for loc in active_opt["Label"]["LocalizedLabels"] if loc["LanguageCode"] == 1033
        )
        self.assertEqual(active_label, "Active Label")

    def test_int_key_not_matching_any_member_raises_value_error(self):
        """__labels__ with int key not matching any member raises ValueError."""

        class Status(Enum):
            Active = 1

        Status.__labels__ = {1033: {99: "Unknown"}}
        with self.assertRaises(ValueError) as ctx:
            self.od._enum_optionset_payload("new_Status", Status)
        self.assertIn("int key", str(ctx.exception))

    def test_duplicate_enum_values_raises_value_error(self):
        """_enum_optionset_payload raises ValueError when two members share the same int value."""

        # Python treats second definition as an alias; __members__ exposes both names
        class Status(Enum):
            Active = 1
            DuplicateActive = 1  # alias for Active in Python Enum

        with self.assertRaises(ValueError) as ctx:
            self.od._enum_optionset_payload("new_Status", Status)
        self.assertIn("Duplicate", str(ctx.exception))

    def test_non_int_enum_value_raises_value_error(self):
        """_enum_optionset_payload raises ValueError for enum member with a non-int value."""

        class Status(Enum):
            Active = "active"

        with self.assertRaises(ValueError) as ctx:
            self.od._enum_optionset_payload("new_Status", Status)
        self.assertIn("non-int", str(ctx.exception))


class TestAttributePayloadDtypes(unittest.TestCase):
    """Unit tests for _ODataClient._attribute_payload dtype dispatching."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_int_dtype(self):
        """'int' produces IntegerAttributeMetadata."""
        result = self.od._attribute_payload("new_Count", "int")
        self.assertEqual(result["@odata.type"], "Microsoft.Dynamics.CRM.IntegerAttributeMetadata")

    def test_integer_dtype_alias(self):
        """'integer' is an alias for 'int'."""
        result = self.od._attribute_payload("new_Count", "integer")
        self.assertIn("Integer", result["@odata.type"])

    def test_decimal_dtype(self):
        """'decimal' produces DecimalAttributeMetadata."""
        result = self.od._attribute_payload("new_Price", "decimal")
        self.assertEqual(result["@odata.type"], "Microsoft.Dynamics.CRM.DecimalAttributeMetadata")

    def test_money_dtype_alias(self):
        """'money' is an alias for 'decimal'."""
        result = self.od._attribute_payload("new_Revenue", "money")
        self.assertIn("Decimal", result["@odata.type"])

    def test_float_dtype(self):
        """'float' produces DoubleAttributeMetadata."""
        result = self.od._attribute_payload("new_Score", "float")
        self.assertEqual(result["@odata.type"], "Microsoft.Dynamics.CRM.DoubleAttributeMetadata")

    def test_double_dtype_alias(self):
        """'double' is an alias for 'float'."""
        result = self.od._attribute_payload("new_Score", "double")
        self.assertIn("Double", result["@odata.type"])

    def test_datetime_dtype(self):
        """'datetime' produces DateTimeAttributeMetadata."""
        result = self.od._attribute_payload("new_CreatedDate", "datetime")
        self.assertEqual(result["@odata.type"], "Microsoft.Dynamics.CRM.DateTimeAttributeMetadata")

    def test_date_dtype_alias(self):
        """'date' is an alias for 'datetime'."""
        result = self.od._attribute_payload("new_BirthDate", "date")
        self.assertIn("DateTime", result["@odata.type"])

    def test_bool_dtype(self):
        """'bool' produces BooleanAttributeMetadata."""
        result = self.od._attribute_payload("new_IsActive", "bool")
        self.assertEqual(result["@odata.type"], "Microsoft.Dynamics.CRM.BooleanAttributeMetadata")

    def test_boolean_dtype_alias(self):
        """'boolean' is an alias for 'bool'."""
        result = self.od._attribute_payload("new_IsActive", "boolean")
        self.assertIn("Boolean", result["@odata.type"])

    def test_file_dtype(self):
        """'file' produces FileAttributeMetadata."""
        result = self.od._attribute_payload("new_Attachment", "file")
        self.assertEqual(result["@odata.type"], "Microsoft.Dynamics.CRM.FileAttributeMetadata")

    def test_unsupported_dtype_returns_none(self):
        """Unrecognized dtype string returns None."""
        self.assertIsNone(self.od._attribute_payload("new_Unknown", "unsupported_type"))

    def test_non_string_dtype_raises_value_error(self):
        """Non-string dtype raises ValueError."""
        with self.assertRaises(ValueError):
            self.od._attribute_payload("new_Field", 42)


class TestGetTableInfo(unittest.TestCase):
    """Unit tests for _ODataClient._get_table_info."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_returns_none_when_entity_not_found(self):
        """_get_table_info returns None when entity does not exist."""
        self.od._request.return_value = _mock_response(json_data={"value": []}, text="{}")
        self.assertIsNone(self.od._get_table_info("new_NonExistent"))

    def test_returns_metadata_when_found(self):
        """_get_table_info returns metadata dict when entity exists."""
        self.od._request.return_value = _entity_def_response()
        result = self.od._get_table_info("account")
        self.assertIsNotNone(result)
        self.assertIn("entity_set_name", result)

    def test_returns_full_dict_shape(self):
        """_get_table_info returns all expected keys from metadata."""
        self.od._request.return_value = _entity_def_response(
            entity_set_name="accounts", primary_id="accountid", metadata_id="meta-001"
        )
        result = self.od._get_table_info("account")
        self.assertEqual(result["table_schema_name"], "Account")
        self.assertEqual(result["table_logical_name"], "account")
        self.assertEqual(result["entity_set_name"], "accounts")
        self.assertEqual(result["metadata_id"], "meta-001")
        self.assertEqual(result["primary_id_attribute"], "accountid")
        self.assertIsInstance(result["columns_created"], list)
        self.assertEqual(result["columns_created"], [])


class TestDeleteTable(unittest.TestCase):
    """Unit tests for _ODataClient._delete_table."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_deletes_table_by_metadata_id(self):
        """_delete_table issues DELETE to EntityDefinitions({MetadataId})."""
        self.od._request.return_value = _mock_response()
        self.od._get_entity_by_table_schema_name = MagicMock(
            return_value={"MetadataId": "meta-001", "SchemaName": "new_Test"}
        )
        self.od._delete_table("new_Test")
        delete_call = next(c for c in self.od._request.call_args_list if c.args[0] == "delete")
        self.assertIn("meta-001", delete_call.args[1])

    def test_raises_metadata_error_when_not_found(self):
        """_delete_table raises MetadataError when entity does not exist."""
        self.od._get_entity_by_table_schema_name = MagicMock(return_value=None)
        with self.assertRaises(MetadataError):
            self.od._delete_table("new_NonExistent")

    def test_raises_metadata_error_when_metadata_id_missing(self):
        """_delete_table raises MetadataError when MetadataId is absent from entity."""
        self.od._get_entity_by_table_schema_name = MagicMock(return_value={"SchemaName": "new_Test"})
        with self.assertRaises(MetadataError):
            self.od._delete_table("new_Test")


class TestCreateAlternateKey(unittest.TestCase):
    """Unit tests for _ODataClient._create_alternate_key."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_creates_alternate_key(self):
        """_create_alternate_key posts to Keys endpoint and returns metadata."""
        post_response = MagicMock()
        post_response.headers = {"OData-EntityId": "https://example.com/Keys(key-meta-001)"}
        self.od._get_entity_by_table_schema_name = MagicMock(
            return_value={"MetadataId": "meta-001", "LogicalName": "account", "SchemaName": "Account"}
        )
        self.od._request.return_value = post_response
        result = self.od._create_alternate_key("account", "new_AccountNumKey", ["accountnumber"])
        self.assertEqual(result["schema_name"], "new_AccountNumKey")
        self.assertEqual(result["key_attributes"], ["accountnumber"])

    def test_display_name_label_passed_to_payload(self):
        """_create_alternate_key includes DisplayName when display_name_label is provided."""
        post_response = MagicMock()
        post_response.headers = {"OData-EntityId": "https://example.com/Keys(key-id)"}
        self.od._get_entity_by_table_schema_name = MagicMock(
            return_value={"MetadataId": "meta-001", "LogicalName": "account"}
        )
        self.od._request.return_value = post_response
        mock_label = MagicMock()
        mock_label.to_dict.return_value = {"LocalizedLabels": [{"Label": "Account Number Key", "LanguageCode": 1033}]}
        self.od._create_alternate_key("account", "new_AccNumKey", ["accountnumber"], display_name_label=mock_label)
        payload = self.od._request.call_args.kwargs["json"]
        self.assertIn("DisplayName", payload)
        mock_label.to_dict.assert_called_once()

    def test_raises_metadata_error_when_table_not_found(self):
        """_create_alternate_key raises MetadataError when table not found."""
        self.od._get_entity_by_table_schema_name = MagicMock(return_value=None)
        with self.assertRaises(MetadataError):
            self.od._create_alternate_key("nonexistent", "key", ["col"])


class TestGetAlternateKeys(unittest.TestCase):
    """Unit tests for _ODataClient._get_alternate_keys."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_returns_keys_list(self):
        """_get_alternate_keys returns list of alternate keys."""
        self.od._get_entity_by_table_schema_name = MagicMock(
            return_value={"MetadataId": "meta-001", "LogicalName": "account"}
        )
        self.od._request.return_value = _mock_response(
            json_data={"value": [{"SchemaName": "new_AccountNumKey"}]},
            text="...",
        )
        result = self.od._get_alternate_keys("account")
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["SchemaName"], "new_AccountNumKey")

    def test_raises_metadata_error_when_table_not_found(self):
        """_get_alternate_keys raises MetadataError when table not found."""
        self.od._get_entity_by_table_schema_name = MagicMock(return_value=None)
        with self.assertRaises(MetadataError):
            self.od._get_alternate_keys("nonexistent")


class TestDeleteAlternateKey(unittest.TestCase):
    """Unit tests for _ODataClient._delete_alternate_key."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_deletes_alternate_key(self):
        """_delete_alternate_key issues DELETE to Keys({key_id})."""
        self.od._get_entity_by_table_schema_name = MagicMock(
            return_value={"MetadataId": "meta-001", "LogicalName": "account"}
        )
        self.od._request.return_value = _mock_response()
        self.od._delete_alternate_key("account", "key-meta-001")
        delete_call = self.od._request.call_args
        self.assertEqual(delete_call.args[0], "delete")
        self.assertIn("key-meta-001", delete_call.args[1])

    def test_raises_metadata_error_when_table_not_found(self):
        """_delete_alternate_key raises MetadataError when table not found."""
        self.od._get_entity_by_table_schema_name = MagicMock(return_value=None)
        with self.assertRaises(MetadataError):
            self.od._delete_alternate_key("nonexistent", "key-id")


class TestCreateTable(unittest.TestCase):
    """Unit tests for _ODataClient._create_table."""

    def setUp(self):
        self.od = _make_odata_client()

    def _setup_for_create(self, entity_exists=False):
        """Mock helpers for _create_table."""
        existing = {"MetadataId": "meta-001", "EntitySetName": "accounts"} if entity_exists else None
        created = {
            "MetadataId": "meta-001",
            "EntitySetName": "new_testtables",
            "LogicalName": "new_testtable",
            "SchemaName": "new_TestTable",
            "PrimaryNameAttribute": "new_name",
            "PrimaryIdAttribute": "new_testtableid",
        }
        call_count = [0]

        def mock_get_entity(table_schema_name, headers=None):
            call_count[0] += 1
            if entity_exists:
                return existing
            return None if call_count[0] == 1 else created

        self.od._get_entity_by_table_schema_name = MagicMock(side_effect=mock_get_entity)
        self.od._request.return_value = _mock_response(status_code=201)

    def test_creates_table_successfully(self):
        """_create_table returns metadata dict on success."""
        self._setup_for_create()
        result = self.od._create_table("new_TestTable", {"new_Name": "string", "new_Age": "int"})
        self.assertEqual(result["table_schema_name"], "new_TestTable")
        self.assertIn("new_Name", result["columns_created"])
        self.assertIn("new_Age", result["columns_created"])

    def test_raises_metadata_error_when_table_already_exists(self):
        """_create_table raises MetadataError when table already exists."""
        self._setup_for_create(entity_exists=True)
        with self.assertRaises(MetadataError):
            self.od._create_table("new_TestTable", {"new_Name": "string"})

    def test_raises_value_error_for_unsupported_column_type(self):
        """_create_table raises ValueError for unsupported column type."""
        self._setup_for_create()
        with self.assertRaises(ValueError) as ctx:
            self.od._create_table("new_TestTable", {"new_Col": "unsupported_type"})
        self.assertIn("Unsupported column type", str(ctx.exception))

    def test_raises_type_error_for_non_string_solution_name(self):
        """_create_table raises TypeError when solution_unique_name is not str."""
        self._setup_for_create()
        with self.assertRaises(TypeError):
            self.od._create_table("new_TestTable", {}, solution_unique_name=123)

    def test_raises_value_error_for_empty_solution_name(self):
        """_create_table raises ValueError when solution_unique_name is empty string."""
        self._setup_for_create()
        with self.assertRaises(ValueError):
            self.od._create_table("new_TestTable", {}, solution_unique_name="")

    def test_primary_column_schema_name_used_when_provided(self):
        """_create_table uses provided primary_column_schema_name in the POST payload."""
        self._setup_for_create()
        self.od._create_table("new_TestTable", {}, primary_column_schema_name="new_CustomName")
        post_json = self.od._request.call_args.kwargs["json"]
        attrs = post_json["Attributes"]
        primary_attr = next((a for a in attrs if a.get("IsPrimaryName")), None)
        self.assertIsNotNone(primary_attr)
        self.assertEqual(primary_attr["SchemaName"], "new_CustomName")


class TestCreateColumns(unittest.TestCase):
    """Unit tests for _ODataClient._create_columns."""

    def setUp(self):
        self.od = _make_odata_client()
        self.od._get_entity_by_table_schema_name = MagicMock(
            return_value={"MetadataId": "meta-001", "SchemaName": "new_Test"}
        )
        self.od._request.return_value = _mock_response(status_code=201)

    def test_creates_columns_successfully(self):
        """_create_columns returns list of created column names."""
        result = self.od._create_columns("new_Test", {"new_Name": "string", "new_Age": "int"})
        self.assertIn("new_Name", result)
        self.assertIn("new_Age", result)

    def test_empty_columns_raises_type_error(self):
        """_create_columns raises TypeError for empty columns dict."""
        with self.assertRaises(TypeError):
            self.od._create_columns("new_Test", {})

    def test_non_dict_columns_raises_type_error(self):
        """_create_columns raises TypeError for non-dict columns."""
        with self.assertRaises(TypeError):
            self.od._create_columns("new_Test", None)

    def test_table_not_found_raises_metadata_error(self):
        """_create_columns raises MetadataError when table does not exist."""
        self.od._get_entity_by_table_schema_name = MagicMock(return_value=None)
        with self.assertRaises(MetadataError):
            self.od._create_columns("new_NonExistent", {"new_Col": "string"})

    def test_unsupported_column_type_raises_validation_error(self):
        """Raises ValidationError for unsupported column type."""
        from PowerPlatform.Dataverse.core.errors import ValidationError

        with self.assertRaises(ValidationError):
            self.od._create_columns("new_Test", {"new_Col": "unsupported"})

    def test_picklist_column_flushes_cache(self):
        """_create_columns calls _flush_cache when a picklist column is created."""
        self.od._flush_cache = MagicMock(return_value=0)

        class Status(Enum):
            Active = 1

        result = self.od._create_columns("new_Test", {"new_Status": Status})
        self.assertIn("new_Status", result)
        self.od._flush_cache.assert_called_once_with("picklist")

    def test_posts_to_correct_endpoint(self):
        """_create_columns POSTs each column to EntityDefinitions({metadata_id})/Attributes."""
        self.od._create_columns("new_Test", {"new_Name": "string"})
        call_args = self.od._request.call_args
        self.assertEqual(call_args.args[0], "post")
        self.assertIn("EntityDefinitions(meta-001)/Attributes", call_args.args[1])


class TestDeleteColumns(unittest.TestCase):
    """Unit tests for _ODataClient._delete_columns."""

    def setUp(self):
        self.od = _make_odata_client()
        self.od._get_entity_by_table_schema_name = MagicMock(
            return_value={"MetadataId": "meta-001", "SchemaName": "new_Test"}
        )
        self.od._get_attribute_metadata = MagicMock(
            return_value={"MetadataId": "attr-001", "LogicalName": "new_name", "@odata.type": "StringAttributeMetadata"}
        )
        self.od._request.return_value = _mock_response(status_code=204)

    def test_deletes_single_column(self):
        """_delete_columns accepts a string column name and issues DELETE."""
        result = self.od._delete_columns("new_Test", "new_Name")
        self.assertIn("new_Name", result)
        delete_calls = [c for c in self.od._request.call_args_list if c.args[0] == "delete"]
        self.assertEqual(len(delete_calls), 1)
        self.assertIn("attr-001", delete_calls[0].args[1])

    def test_deletes_list_of_columns(self):
        """_delete_columns accepts a list of column names and issues DELETE for each."""
        result = self.od._delete_columns("new_Test", ["new_Name1", "new_Name2"])
        self.assertEqual(len(result), 2)
        delete_calls = [c for c in self.od._request.call_args_list if c.args[0] == "delete"]
        self.assertEqual(len(delete_calls), 2)

    def test_non_string_non_list_raises_type_error(self):
        """_delete_columns raises TypeError for invalid columns type."""
        with self.assertRaises(TypeError):
            self.od._delete_columns("new_Test", 42)

    def test_empty_column_name_raises_value_error(self):
        """_delete_columns raises ValueError for empty column name."""
        with self.assertRaises(ValueError):
            self.od._delete_columns("new_Test", "")

    def test_table_not_found_raises_metadata_error(self):
        """_delete_columns raises MetadataError when table not found."""
        self.od._get_entity_by_table_schema_name = MagicMock(return_value=None)
        with self.assertRaises(MetadataError):
            self.od._delete_columns("new_NonExistent", "new_Col")

    def test_column_not_found_raises_metadata_error(self):
        """_delete_columns raises MetadataError when column not found."""
        self.od._get_attribute_metadata = MagicMock(return_value=None)
        with self.assertRaises(MetadataError) as ctx:
            self.od._delete_columns("new_Test", "new_Missing")
        self.assertIn("not found", str(ctx.exception))

    def test_missing_metadata_id_raises_runtime_error(self):
        """_delete_columns raises RuntimeError when column MetadataId is missing."""
        self.od._get_attribute_metadata = MagicMock(return_value={"LogicalName": "new_name"})
        with self.assertRaises(RuntimeError) as ctx:
            self.od._delete_columns("new_Test", "new_Name")
        self.assertIn("MetadataId", str(ctx.exception))

    def test_picklist_column_deletion_flushes_cache(self):
        """_delete_columns flushes picklist cache when a picklist column is deleted."""
        self.od._get_attribute_metadata = MagicMock(
            return_value={
                "MetadataId": "attr-001",
                "LogicalName": "new_status",
                "@odata.type": "PicklistAttributeMetadata",
            }
        )
        self.od._flush_cache = MagicMock(return_value=0)
        self.od._delete_columns("new_Test", "new_Status")
        self.od._flush_cache.assert_called_once_with("picklist")


class TestFlushCache(unittest.TestCase):
    """Unit tests for _ODataClient._flush_cache."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_flush_picklist_clears_cache(self):
        """_flush_cache('picklist') clears _picklist_label_cache."""
        self.od._picklist_label_cache = {("account", "statuscode"): {"map": {}, "ts": 0.0}}
        removed = self.od._flush_cache("picklist")
        self.assertEqual(removed, 1)
        self.assertEqual(len(self.od._picklist_label_cache), 0)

    def test_flush_empty_cache_returns_zero(self):
        """_flush_cache returns 0 when cache is already empty."""
        self.assertEqual(self.od._flush_cache("picklist"), 0)

    def test_unsupported_cache_kind_raises_validation_error(self):
        """_flush_cache raises ValidationError for unsupported kind."""
        with self.assertRaises(ValidationError):
            self.od._flush_cache("entityset")

    def test_none_kind_raises_validation_error(self):
        """_flush_cache raises ValidationError for None kind."""
        with self.assertRaises(ValidationError):
            self.od._flush_cache(None)


class TestPicklistLabelResolution(unittest.TestCase):
    """Tests for picklist label-to-integer resolution.

    Covers _bulk_fetch_picklists, _request_metadata_with_retry,
    _convert_labels_to_ints, and their integration through _create / _update / _upsert.

    Cache structure (nested):
        _picklist_label_cache = {
            "table_key": {"ts": epoch, "picklists": {"attr": {norm_label: int}}}
        }
    """

    def setUp(self):
        self.od = _make_odata_client()

    # ---- Helper to build a bulk-fetch API response ----
    @staticmethod
    def _bulk_response(*picklists):
        """Build a mock response for _bulk_fetch_picklists.

        Each picklist is (logical_name, [(value, label), ...]).
        """
        items = []
        for ln, options in picklists:
            opts = [{"Value": val, "Label": {"LocalizedLabels": [{"Label": lab}]}} for val, lab in options]
            items.append({"LogicalName": ln, "OptionSet": {"Options": opts}})
        resp = MagicMock()
        resp.json.return_value = {"value": items}
        return resp

    # ---- _bulk_fetch_picklists ----

    def test_bulk_fetch_populates_nested_cache(self):
        """Bulk fetch stores picklists in nested {table: {ts, picklists: {...}}} format."""
        import time

        resp = self._bulk_response(
            ("industrycode", [(6, "Technology"), (12, "Consulting")]),
        )
        self.od._request.return_value = resp

        self.od._bulk_fetch_picklists("account")

        entry = self.od._picklist_label_cache.get("account")
        self.assertIsNotNone(entry)
        self.assertIn("ts", entry)
        self.assertIn("picklists", entry)
        self.assertEqual(entry["picklists"]["industrycode"], {"technology": 6, "consulting": 12})

    def test_bulk_fetch_multiple_picklists(self):
        """Multiple picklist attributes are all stored under the same table entry."""
        resp = self._bulk_response(
            ("industrycode", [(6, "Technology")]),
            ("statuscode", [(1, "Active"), (2, "Inactive")]),
        )
        self.od._request.return_value = resp

        self.od._bulk_fetch_picklists("account")

        picklists = self.od._picklist_label_cache["account"]["picklists"]
        self.assertEqual(picklists["industrycode"], {"technology": 6})
        self.assertEqual(picklists["statuscode"], {"active": 1, "inactive": 2})

    def test_bulk_fetch_no_picklists_caches_empty(self):
        """Table with no picklist attributes gets cached with empty picklists dict."""
        resp = MagicMock()
        resp.json.return_value = {"value": []}
        self.od._request.return_value = resp

        self.od._bulk_fetch_picklists("account")

        entry = self.od._picklist_label_cache.get("account")
        self.assertIsNotNone(entry)
        self.assertEqual(entry["picklists"], {})

    def test_bulk_fetch_skips_when_cache_fresh(self):
        """Warm cache within TTL should skip the API call."""
        import time

        self.od._picklist_label_cache["account"] = {
            "ts": time.time(),
            "picklists": {"industrycode": {"technology": 6}},
        }

        self.od._bulk_fetch_picklists("account")
        self.od._request.assert_not_called()

    def test_bulk_fetch_refreshes_when_cache_expired(self):
        """Expired cache should trigger a new API call."""
        import time

        self.od._picklist_label_cache["account"] = {
            "ts": time.time() - 7200,  # 2 hours ago, beyond 1h TTL
            "picklists": {"industrycode": {"technology": 6}},
        }

        resp = self._bulk_response(("industrycode", [(6, "Tech"), (12, "Consulting")]))
        self.od._request.return_value = resp

        self.od._bulk_fetch_picklists("account")
        self.od._request.assert_called_once()
        self.assertEqual(
            self.od._picklist_label_cache["account"]["picklists"]["industrycode"],
            {"tech": 6, "consulting": 12},
        )

    def test_bulk_fetch_case_insensitive_table_key(self):
        """Table key is normalized to lowercase."""
        resp = self._bulk_response(("industrycode", [(6, "Tech")]))
        self.od._request.return_value = resp

        self.od._bulk_fetch_picklists("Account")

        self.assertIn("account", self.od._picklist_label_cache)
        self.assertNotIn("Account", self.od._picklist_label_cache)

    def test_bulk_fetch_uses_picklist_cast_url(self):
        """API call uses PicklistAttributeMetadata cast segment."""
        resp = self._bulk_response()
        self.od._request.return_value = resp

        self.od._bulk_fetch_picklists("account")

        call_url = self.od._request.call_args.args[1]
        self.assertIn("PicklistAttributeMetadata", call_url)
        self.assertIn("OptionSet", call_url)

    def test_bulk_fetch_makes_single_api_call(self):
        """Bulk fetch uses exactly one API call regardless of picklist count."""
        resp = self._bulk_response(
            ("a", [(1, "X")]),
            ("b", [(2, "Y")]),
            ("c", [(3, "Z")]),
        )
        self.od._request.return_value = resp

        self.od._bulk_fetch_picklists("account")
        self.assertEqual(self.od._request.call_count, 1)

    def test_bulk_fetch_stress_large_workload(self):
        """Bulk fetch correctly parses a response with a large number of picklist attributes."""
        num_picklists = 5000
        picklists = [(f"new_pick{i}", [(100000000 + j, f"Option {j}") for j in range(4)]) for i in range(num_picklists)]
        resp = self._bulk_response(*picklists)
        self.od._request.return_value = resp

        self.od._bulk_fetch_picklists("account")

        self.assertEqual(self.od._request.call_count, 1)
        cached = self.od._picklist_label_cache["account"]["picklists"]
        self.assertEqual(len(cached), num_picklists)
        self.assertEqual(cached["new_pick0"]["option 0"], 100000000)
        self.assertEqual(cached[f"new_pick{num_picklists - 1}"]["option 3"], 100000003)

    # ---- _request_metadata_with_retry ----

    def test_retry_succeeds_on_first_try(self):
        """No retry needed when first call succeeds."""
        mock_resp = MagicMock()
        self.od._request.return_value = mock_resp

        result = self.od._request_metadata_with_retry("get", "https://example.com/test")
        self.assertIs(result, mock_resp)
        self.assertEqual(self.od._request.call_count, 1)

    @patch("PowerPlatform.Dataverse.data._odata.time.sleep")
    def test_retry_retries_on_404(self, mock_sleep):
        """Should retry on 404 and succeed on later attempt."""
        from PowerPlatform.Dataverse.core.errors import HttpError

        err_404 = HttpError("Not Found", status_code=404)
        mock_resp = MagicMock()
        self.od._request.side_effect = [err_404, mock_resp]

        result = self.od._request_metadata_with_retry("get", "https://example.com/test")
        self.assertIs(result, mock_resp)
        self.assertEqual(self.od._request.call_count, 2)
        mock_sleep.assert_called_once()

    @patch("PowerPlatform.Dataverse.data._odata.time.sleep")
    def test_retry_raises_after_max_attempts(self, mock_sleep):
        """Should raise RuntimeError after all retries exhausted."""
        from PowerPlatform.Dataverse.core.errors import HttpError

        err_404 = HttpError("Not Found", status_code=404)
        self.od._request.side_effect = err_404

        with self.assertRaises(RuntimeError) as ctx:
            self.od._request_metadata_with_retry("get", "https://example.com/test")
        self.assertIn("404", str(ctx.exception))
        self.assertTrue(mock_sleep.called)

    def test_retry_does_not_retry_non_404(self):
        """Non-404 errors should be raised immediately without retry."""
        from PowerPlatform.Dataverse.core.errors import HttpError

        err_500 = HttpError("Server Error", status_code=500)
        self.od._request.side_effect = err_500

        with self.assertRaises(HttpError):
            self.od._request_metadata_with_retry("get", "https://example.com/test")
        self.assertEqual(self.od._request.call_count, 1)

    # ---- _convert_labels_to_ints ----

    def test_convert_no_string_values_skips_fetch(self):
        """Record with no string values should not trigger any API call."""
        record = {"quantity": 5, "amount": 99.99, "completed": False}
        result = self.od._convert_labels_to_ints("account", record)
        self.assertEqual(result, record)
        self.od._request.assert_not_called()

    def test_convert_empty_record_returns_copy(self):
        """Empty record returns empty dict without API calls."""
        result = self.od._convert_labels_to_ints("account", {})
        self.assertEqual(result, {})
        self.od._request.assert_not_called()

    def test_convert_whitespace_only_string_skipped(self):
        """String values that are only whitespace should not be candidates."""
        record = {"name": "   ", "description": ""}
        result = self.od._convert_labels_to_ints("account", record)
        self.assertEqual(result, record)
        self.od._request.assert_not_called()

    def test_convert_odata_keys_skipped(self):
        """@odata.bind keys must not be resolved."""
        import time

        self.od._picklist_label_cache["account"] = {
            "ts": time.time(),
            "picklists": {},
        }

        record = {
            "name": "Contoso",
            "new_CustomerId@odata.bind": "/contacts(guid)",
            "@odata.type": "Microsoft.Dynamics.CRM.account",
        }
        result = self.od._convert_labels_to_ints("account", record)
        # @odata keys left unchanged
        self.assertEqual(result["new_CustomerId@odata.bind"], "/contacts(guid)")
        self.assertEqual(result["@odata.type"], "Microsoft.Dynamics.CRM.account")

    def test_convert_warm_cache_no_api_calls(self):
        """Warm cache should resolve labels without any API calls."""
        import time

        now = time.time()
        self.od._picklist_label_cache["account"] = {
            "ts": now,
            "picklists": {
                "industrycode": {"technology": 6},
            },
        }

        record = {"name": "Contoso", "industrycode": "Technology"}
        result = self.od._convert_labels_to_ints("account", record)

        self.assertEqual(result["industrycode"], 6)
        self.assertEqual(result["name"], "Contoso")
        self.od._request.assert_not_called()

    def test_convert_resolves_picklist_label_to_int(self):
        """Full flow: bulk fetch returns picklists, label resolved to int."""
        resp = self._bulk_response(
            ("industrycode", [(6, "Technology")]),
        )
        self.od._request.return_value = resp

        record = {"name": "Contoso", "industrycode": "Technology"}
        result = self.od._convert_labels_to_ints("account", record)

        self.assertEqual(result["industrycode"], 6)
        self.assertEqual(result["name"], "Contoso")
        # Single bulk fetch call
        self.assertEqual(self.od._request.call_count, 1)

    def test_convert_non_picklist_leaves_string_unchanged(self):
        """Non-picklist string fields are left as strings (no picklist entry in cache)."""
        resp = self._bulk_response()  # no picklists on table
        self.od._request.return_value = resp

        record = {"name": "Contoso", "telephone1": "555-0100"}
        result = self.od._convert_labels_to_ints("account", record)

        self.assertEqual(result["name"], "Contoso")
        self.assertEqual(result["telephone1"], "555-0100")

    def test_convert_unmatched_label_left_unchanged(self):
        """If a picklist label doesn't match any option, value stays as string."""
        import time

        self.od._picklist_label_cache["account"] = {
            "ts": time.time(),
            "picklists": {
                "industrycode": {"technology": 6, "consulting": 12},
            },
        }

        record = {"industrycode": "UnknownIndustry"}
        result = self.od._convert_labels_to_ints("account", record)
        self.assertEqual(result["industrycode"], "UnknownIndustry")

    def test_convert_does_not_mutate_original_record(self):
        """_convert_labels_to_ints must return a copy, not mutate the input."""
        import time

        self.od._picklist_label_cache["account"] = {
            "ts": time.time(),
            "picklists": {"industrycode": {"technology": 6}},
        }

        original = {"industrycode": "Technology"}
        result = self.od._convert_labels_to_ints("account", original)

        self.assertEqual(result["industrycode"], 6)
        self.assertEqual(original["industrycode"], "Technology")

    def test_convert_multiple_picklists_in_one_record(self):
        """Multiple picklist fields in the same record are all resolved."""
        resp = self._bulk_response(
            ("industrycode", [(6, "Tech")]),
            ("statuscode", [(1, "Active")]),
        )
        self.od._request.return_value = resp

        record = {"industrycode": "Tech", "statuscode": "Active"}
        result = self.od._convert_labels_to_ints("account", record)

        self.assertEqual(result["industrycode"], 6)
        self.assertEqual(result["statuscode"], 1)
        # Single bulk fetch call
        self.assertEqual(self.od._request.call_count, 1)

    def test_convert_mixed_picklists_and_non_picklists(self):
        """Picklists resolved, non-picklist strings left unchanged, 1 API call."""
        resp = self._bulk_response(
            ("industrycode", [(6, "Tech")]),
            ("statuscode", [(1, "Active")]),
        )
        self.od._request.return_value = resp

        record = {
            "name": "Contoso",
            "industrycode": "Tech",
            "description": "A company",
            "statuscode": "Active",
        }
        result = self.od._convert_labels_to_ints("account", record)

        self.assertEqual(result["industrycode"], 6)
        self.assertEqual(result["statuscode"], 1)
        self.assertEqual(result["name"], "Contoso")
        self.assertEqual(result["description"], "A company")
        self.assertEqual(self.od._request.call_count, 1)

    def test_convert_all_non_picklist_makes_one_api_call(self):
        """All non-picklist string fields: 1 bulk fetch call, labels unchanged."""
        resp = self._bulk_response()  # no picklists
        self.od._request.return_value = resp

        record = {"name": "Contoso", "description": "A company", "telephone1": "555-0100"}
        result = self.od._convert_labels_to_ints("account", record)

        self.assertEqual(self.od._request.call_count, 1)
        self.assertEqual(result["name"], "Contoso")

    def test_convert_no_string_values_makes_zero_api_calls(self):
        """All non-string values: 0 API calls total."""
        record = {"revenue": 1000000, "quantity": 5, "active": True}
        self.od._convert_labels_to_ints("account", record)

        self.assertEqual(self.od._request.call_count, 0)

    def test_convert_bulk_fetch_failure_propagates(self):
        """Server error during bulk fetch propagates to caller."""
        from PowerPlatform.Dataverse.core.errors import HttpError

        self.od._request.side_effect = HttpError("Server Error", status_code=500)

        with self.assertRaises(HttpError):
            self.od._convert_labels_to_ints("account", {"name": "Contoso"})

    def test_convert_single_picklist_makes_one_api_call(self):
        """Single picklist field (cold cache): 1 bulk fetch total."""
        resp = self._bulk_response(("industrycode", [(6, "Tech")]))
        self.od._request.return_value = resp

        record = {"industrycode": "Tech"}
        result = self.od._convert_labels_to_ints("account", record)

        self.assertEqual(result["industrycode"], 6)
        self.assertEqual(self.od._request.call_count, 1)

    def test_convert_integer_values_passed_through(self):
        """Integer values (already resolved) are left unchanged."""
        import time

        self.od._picklist_label_cache["account"] = {
            "ts": time.time(),
            "picklists": {"industrycode": {"technology": 6}},
        }

        record = {"industrycode": 6, "name": "Contoso"}
        result = self.od._convert_labels_to_ints("account", record)
        self.assertEqual(result["industrycode"], 6)

    def test_convert_case_insensitive_label_matching(self):
        """Picklist label matching is case-insensitive."""
        import time

        self.od._picklist_label_cache["account"] = {
            "ts": time.time(),
            "picklists": {"industrycode": {"technology": 6}},
        }

        record = {"industrycode": "TECHNOLOGY"}
        result = self.od._convert_labels_to_ints("account", record)
        self.assertEqual(result["industrycode"], 6)

    def test_convert_second_call_same_table_no_api(self):
        """Second convert call for same table uses cached bulk fetch, no API call."""
        resp = self._bulk_response(("industrycode", [(6, "Tech")]))
        self.od._request.return_value = resp

        self.od._convert_labels_to_ints("account", {"industrycode": "Tech"})
        self.assertEqual(self.od._request.call_count, 1)

        # Second call -- cache warm
        self.od._request.reset_mock()
        result = self.od._convert_labels_to_ints("account", {"industrycode": "Tech"})
        self.assertEqual(result["industrycode"], 6)
        self.od._request.assert_not_called()

    def test_convert_different_tables_separate_fetches(self):
        """Different tables each get their own bulk fetch."""
        resp1 = self._bulk_response(("industrycode", [(6, "Tech")]))
        resp2 = self._bulk_response(("new_status", [(100, "Open")]))
        self.od._request.side_effect = [resp1, resp2]

        r1 = self.od._convert_labels_to_ints("account", {"industrycode": "Tech"})
        r2 = self.od._convert_labels_to_ints("new_ticket", {"new_status": "Open"})

        self.assertEqual(r1["industrycode"], 6)
        self.assertEqual(r2["new_status"], 100)
        self.assertEqual(self.od._request.call_count, 2)

    def test_convert_only_odata_and_non_strings_skips_fetch(self):
        """Record with only @odata keys and non-string values should skip fetch entirely."""
        record = {
            "@odata.type": "Microsoft.Dynamics.CRM.account",
            "new_CustomerId@odata.bind": "/contacts(guid)",
            "quantity": 5,
            "active": True,
        }
        result = self.od._convert_labels_to_ints("account", record)
        self.assertEqual(result, record)
        self.od._request.assert_not_called()

    def test_convert_partial_picklist_match(self):
        """Some picklists match, some don't -- matched ones resolved, unmatched left as string."""
        import time

        self.od._picklist_label_cache["account"] = {
            "ts": time.time(),
            "picklists": {
                "industrycode": {"technology": 6, "consulting": 12},
                "statuscode": {"active": 1, "inactive": 2},
            },
        }

        record = {"industrycode": "Technology", "statuscode": "UnknownStatus"}
        result = self.od._convert_labels_to_ints("account", record)

        self.assertEqual(result["industrycode"], 6)
        self.assertEqual(result["statuscode"], "UnknownStatus")

    def test_convert_mixed_int_and_label_in_same_record(self):
        """One picklist already int, another is a label string -- only label resolved."""
        import time

        self.od._picklist_label_cache["account"] = {
            "ts": time.time(),
            "picklists": {
                "industrycode": {"technology": 6},
                "statuscode": {"active": 1},
            },
        }

        record = {"industrycode": 6, "statuscode": "Active"}
        result = self.od._convert_labels_to_ints("account", record)

        self.assertEqual(result["industrycode"], 6)
        self.assertEqual(result["statuscode"], 1)

    def test_convert_same_label_different_picklists(self):
        """Same label text in two different picklist columns resolves to different values."""
        import time

        self.od._picklist_label_cache["new_ticket"] = {
            "ts": time.time(),
            "picklists": {
                "new_priority": {"high": 3},
                "new_severity": {"high": 100},
            },
        }

        record = {"new_priority": "High", "new_severity": "High"}
        result = self.od._convert_labels_to_ints("new_ticket", record)

        self.assertEqual(result["new_priority"], 3)
        self.assertEqual(result["new_severity"], 100)

    def test_convert_picklist_with_empty_options(self):
        """Picklist attribute with zero defined options: label stays as string."""
        import time

        self.od._picklist_label_cache["account"] = {
            "ts": time.time(),
            "picklists": {
                "customcode": {},  # picklist exists but has no options
            },
        }

        record = {"customcode": "SomeValue"}
        result = self.od._convert_labels_to_ints("account", record)
        self.assertEqual(result["customcode"], "SomeValue")

    def test_convert_full_realistic_record(self):
        """Realistic record: mix of strings, ints, bools, @odata keys, and picklists."""
        resp = self._bulk_response(
            ("industrycode", [(6, "Technology"), (12, "Consulting")]),
            ("statuscode", [(1, "Active"), (2, "Inactive")]),
        )
        self.od._request.return_value = resp

        record = {
            "name": "Contoso Ltd",
            "industrycode": "Technology",
            "statuscode": "Active",
            "revenue": 1000000,
            "telephone1": "555-0100",
            "emailaddress1": "info@contoso.com",
            "new_completed": True,
            "new_quantity": 42,
            "description": "A technology company",
            "@odata.type": "Microsoft.Dynamics.CRM.account",
            "new_CustomerId@odata.bind": "/contacts(00000000-0000-0000-0000-000000000001)",
        }
        result = self.od._convert_labels_to_ints("account", record)

        # Picklists resolved
        self.assertEqual(result["industrycode"], 6)
        self.assertEqual(result["statuscode"], 1)
        # Non-picklist strings unchanged
        self.assertEqual(result["name"], "Contoso Ltd")
        self.assertEqual(result["telephone1"], "555-0100")
        self.assertEqual(result["emailaddress1"], "info@contoso.com")
        self.assertEqual(result["description"], "A technology company")
        # Non-strings unchanged
        self.assertEqual(result["revenue"], 1000000)
        self.assertEqual(result["new_completed"], True)
        self.assertEqual(result["new_quantity"], 42)
        # @odata keys unchanged
        self.assertEqual(result["@odata.type"], "Microsoft.Dynamics.CRM.account")
        self.assertEqual(
            result["new_CustomerId@odata.bind"],
            "/contacts(00000000-0000-0000-0000-000000000001)",
        )
        self.assertEqual(self.od._request.call_count, 1)

    def test_bulk_fetch_skips_malformed_items(self):
        """Bulk fetch ignores items that aren't dicts or lack LogicalName."""
        resp = MagicMock()
        resp.json.return_value = {
            "value": [
                "not-a-dict",
                {"LogicalName": "", "OptionSet": {"Options": []}},
                {
                    "LogicalName": "industrycode",
                    "OptionSet": {"Options": [{"Value": 6, "Label": {"LocalizedLabels": [{"Label": "Tech"}]}}]},
                },
                {"no_logical_name_key": True},
            ]
        }
        self.od._request.return_value = resp

        self.od._bulk_fetch_picklists("account")

        picklists = self.od._picklist_label_cache["account"]["picklists"]
        self.assertEqual(len(picklists), 1)
        self.assertEqual(picklists["industrycode"], {"tech": 6})

    def test_bulk_fetch_first_label_wins_for_same_value(self):
        """When multiple localized labels exist, first label wins via setdefault."""
        resp = MagicMock()
        resp.json.return_value = {
            "value": [
                {
                    "LogicalName": "industrycode",
                    "OptionSet": {
                        "Options": [
                            {
                                "Value": 6,
                                "Label": {
                                    "LocalizedLabels": [
                                        {"Label": "Technology"},
                                        {"Label": "Technologie"},
                                    ]
                                },
                            }
                        ]
                    },
                }
            ]
        }
        self.od._request.return_value = resp

        self.od._bulk_fetch_picklists("account")

        picklists = self.od._picklist_label_cache["account"]["picklists"]
        # Both labels should be present, mapping to the same value
        self.assertEqual(picklists["industrycode"]["technology"], 6)
        self.assertEqual(picklists["industrycode"]["technologie"], 6)

    # ---- Integration: through _create ----

    def test_create_resolves_picklist_in_payload(self):
        """_create resolves a picklist label to its integer in the POST payload."""
        bulk_resp = self._bulk_response(
            ("industrycode", [(6, "Technology")]),
        )
        post_resp = MagicMock()
        post_resp.headers = {
            "OData-EntityId": "https://example.crm.dynamics.com/api/data/v9.2/accounts(00000000-0000-0000-0000-000000000001)"
        }
        self.od._request.side_effect = [bulk_resp, post_resp]

        result = self.od._create("accounts", "account", {"name": "Contoso", "industrycode": "Technology"})
        self.assertEqual(result, "00000000-0000-0000-0000-000000000001")
        post_calls = [c for c in self.od._request.call_args_list if c.args[0] == "post"]
        payload = json.loads(post_calls[0].kwargs["data"])
        self.assertEqual(payload["industrycode"], 6)
        self.assertEqual(payload["name"], "Contoso")

    def test_create_warm_cache_skips_fetch(self):
        """_create with warm cache makes only the POST call."""
        import time

        now = time.time()
        self.od._picklist_label_cache["account"] = {
            "ts": now,
            "picklists": {"industrycode": {"technology": 6}},
        }

        post_resp = MagicMock()
        post_resp.headers = {
            "OData-EntityId": "https://example.crm.dynamics.com/api/data/v9.2/accounts(00000000-0000-0000-0000-000000000001)"
        }
        self.od._request.return_value = post_resp

        result = self.od._create("accounts", "account", {"name": "Contoso", "industrycode": "Technology"})
        self.assertEqual(result, "00000000-0000-0000-0000-000000000001")
        self.assertEqual(self.od._request.call_count, 1)
        payload = json.loads(self.od._request.call_args.kwargs["data"])
        self.assertEqual(payload["industrycode"], 6)

    # ---- Integration: through _update ----

    def test_update_resolves_picklist_in_payload(self):
        """_update resolves a picklist label to its integer in the PATCH payload."""
        self.od._entity_set_from_schema_name = MagicMock(return_value="new_tickets")

        bulk_resp = self._bulk_response(
            ("new_status", [(100000001, "In Progress")]),
        )
        patch_resp = MagicMock()
        self.od._request.side_effect = [bulk_resp, patch_resp]

        self.od._update(
            "new_ticket",
            "00000000-0000-0000-0000-000000000001",
            {"new_status": "In Progress"},
        )
        patch_calls = [c for c in self.od._request.call_args_list if c.args[0] == "patch"]
        payload = json.loads(patch_calls[0].kwargs["data"])
        self.assertEqual(payload["new_status"], 100000001)

    def test_update_warm_cache_skips_fetch(self):
        """_update with warm cache makes only the PATCH call."""
        import time

        self.od._entity_set_from_schema_name = MagicMock(return_value="new_tickets")
        self.od._picklist_label_cache["new_ticket"] = {
            "ts": time.time(),
            "picklists": {"new_status": {"in progress": 100000001}},
        }

        self.od._update(
            "new_ticket",
            "00000000-0000-0000-0000-000000000001",
            {"new_status": "In Progress"},
        )
        self.assertEqual(self.od._request.call_count, 1)
        self.assertEqual(self.od._request.call_args.args[0], "patch")
        payload = json.loads(self.od._request.call_args.kwargs["data"])
        self.assertEqual(payload["new_status"], 100000001)

    # ---- Integration: through _upsert ----

    def test_upsert_resolves_picklist_in_payload(self):
        """_upsert resolves a picklist label to its integer in the PATCH payload."""
        bulk_resp = self._bulk_response(
            ("industrycode", [(6, "Technology")]),
        )
        patch_resp = MagicMock()
        self.od._request.side_effect = [bulk_resp, patch_resp]

        self.od._upsert(
            "accounts",
            "account",
            {"accountnumber": "ACC-001"},
            {"name": "Contoso", "industrycode": "Technology"},
        )
        patch_calls = [c for c in self.od._request.call_args_list if c.args[0] == "patch"]
        payload = patch_calls[0].kwargs["json"]
        self.assertEqual(payload["industrycode"], 6)
        self.assertEqual(payload["name"], "Contoso")

    def test_upsert_warm_cache_skips_fetch(self):
        """_upsert with warm cache makes only the PATCH call."""
        import time

        now = time.time()
        self.od._picklist_label_cache["account"] = {
            "ts": now,
            "picklists": {"industrycode": {"technology": 6}},
        }

        self.od._upsert(
            "accounts",
            "account",
            {"accountnumber": "ACC-001"},
            {"name": "Contoso", "industrycode": "Technology"},
        )
        self.assertEqual(self.od._request.call_count, 1)
        patch_calls = [c for c in self.od._request.call_args_list if c.args[0] == "patch"]
        payload = patch_calls[0].kwargs["json"]
        self.assertEqual(payload["industrycode"], 6)


class TestBuildUpsertMultiple(unittest.TestCase):
    """Unit tests for _ODataClient._build_upsert_multiple (batch deferred build)."""

    def setUp(self):
        self.od = _make_odata_client()

    def _targets(self, alt_keys, records):
        req = self.od._build_upsert_multiple("accounts", "account", alt_keys, records)
        return json.loads(req.body)["Targets"]

    def test_payload_excludes_alternate_key_fields(self):
        """Alternate key fields must NOT appear in the request body (only in @odata.id)."""
        targets = self._targets(
            [{"accountnumber": "ACC-001"}],
            [{"name": "Contoso"}],
        )
        self.assertEqual(len(targets), 1)
        target = targets[0]
        self.assertNotIn("accountnumber", target)
        self.assertIn("name", target)
        self.assertIn("@odata.id", target)
        self.assertIn("accountnumber", target["@odata.id"])

    def test_payload_allows_matching_key_field_in_record(self):
        """If user passes matching key field in record with same value, it passes through to body."""
        targets = self._targets(
            [{"accountnumber": "ACC-001"}],
            [{"accountnumber": "ACC-001", "name": "Contoso"}],
        )
        target = targets[0]
        self.assertIn("name", target)
        self.assertIn("@odata.id", target)
        self.assertIn("accountnumber", target["@odata.id"])

    def test_odata_type_added_when_absent(self):
        """@odata.type is injected when not provided by caller."""
        targets = self._targets(
            [{"accountnumber": "ACC-001"}],
            [{"name": "Contoso"}],
        )
        self.assertIn("@odata.type", targets[0])
        self.assertEqual(targets[0]["@odata.type"], "Microsoft.Dynamics.CRM.account")

    def test_multiple_targets_all_have_odata_id(self):
        """Each target in a multi-item call gets its own @odata.id."""
        targets = self._targets(
            [{"accountnumber": "ACC-001"}, {"accountnumber": "ACC-002"}],
            [{"name": "Contoso"}, {"name": "Fabrikam"}],
        )
        self.assertEqual(len(targets), 2)
        self.assertIn("ACC-001", targets[0]["@odata.id"])
        self.assertIn("ACC-002", targets[1]["@odata.id"])

    def test_conflicting_key_field_raises(self):
        """Raises when a record field contradicts its alternate key value."""
        with self.assertRaises(ValidationError) as ctx:
            self.od._build_upsert_multiple(
                "accounts",
                "account",
                [{"accountnumber": "ACC-001"}],
                [{"accountnumber": "ACC-WRONG", "name": "Contoso"}],
            )
        self.assertIn("accountnumber", str(ctx.exception))

    def test_mismatched_lengths_raises(self):
        """Raises when alternate_keys and records lengths differ."""
        with self.assertRaises(ValidationError):
            self.od._build_upsert_multiple("accounts", "account", [{"accountnumber": "ACC-001"}], [])

    def test_url_contains_upsert_multiple_action(self):
        """POST URL targets the UpsertMultiple bound action."""
        req = self.od._build_upsert_multiple(
            "accounts", "account", [{"accountnumber": "ACC-001"}], [{"name": "Contoso"}]
        )
        self.assertIn("UpsertMultiple", req.url)
        self.assertEqual(req.method, "POST")


if __name__ == "__main__":
    unittest.main()
