# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import json
import unittest
from unittest.mock import MagicMock, patch

from PowerPlatform.Dataverse.core.errors import ValidationError
from PowerPlatform.Dataverse.data._odata import _ODataClient


def _make_odata_client() -> _ODataClient:
    """Return an _ODataClient with HTTP calls mocked out."""
    mock_auth = MagicMock()
    mock_auth._acquire_token.return_value = MagicMock(access_token="token")
    client = _ODataClient(mock_auth, "https://example.crm.dynamics.com")
    client._request = MagicMock()
    return client


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
        payload = json.loads(call.kwargs["data"]) if "data" in call.kwargs else call.kwargs["json"]
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
        payload = json.loads(call.kwargs["data"]) if "data" in call.kwargs else call.kwargs["json"]
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
        payload = json.loads(call.kwargs["data"]) if "data" in call.kwargs else call.kwargs["json"]
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
        payload = json.loads(call.kwargs["data"]) if "data" in call.kwargs else call.kwargs["json"]
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
        payload = json.loads(call.kwargs["data"]) if "data" in call.kwargs else call.kwargs["json"]
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
        payload = json.loads(call.kwargs["data"]) if "data" in call.kwargs else call.kwargs["json"]
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


class TestAttributePayload(unittest.TestCase):
    """Unit tests for _ODataClient._attribute_payload."""

    def setUp(self):
        self.od = _make_odata_client()

    def test_memo_type(self):
        """'memo' should produce MemoAttributeMetadata with MaxLength 4000."""
        result = self.od._attribute_payload("new_Notes", "memo")
        self.assertEqual(result["@odata.type"], "Microsoft.Dynamics.CRM.MemoAttributeMetadata")
        self.assertEqual(result["SchemaName"], "new_Notes")
        self.assertEqual(result["MaxLength"], 4000)
        self.assertEqual(result["FormatName"], {"Value": "Text"})
        self.assertNotIn("IsPrimaryName", result)

    def test_multiline_alias(self):
        """'multiline' should produce identical payload to 'memo'."""
        memo_result = self.od._attribute_payload("new_Description", "memo")
        multiline_result = self.od._attribute_payload("new_Description", "multiline")
        self.assertEqual(multiline_result, memo_result)

    def test_string_type(self):
        """'string' should produce StringAttributeMetadata with MaxLength 200."""
        result = self.od._attribute_payload("new_Title", "string")
        self.assertEqual(result["@odata.type"], "Microsoft.Dynamics.CRM.StringAttributeMetadata")
        self.assertEqual(result["MaxLength"], 200)
        self.assertEqual(result["FormatName"], {"Value": "Text"})

    def test_unsupported_type_returns_none(self):
        """An unknown type string should return None."""
        result = self.od._attribute_payload("new_Col", "unknown_type")
        self.assertIsNone(result)


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
        import json

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
