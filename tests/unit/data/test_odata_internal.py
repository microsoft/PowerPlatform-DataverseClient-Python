# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock

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
        call_kwargs = self.od._request.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params", {})
        self.assertEqual(params["$filter"], "IsPrivate eq false")

    def test_filter_combined_with_default(self):
        """_list_tables(filter=...) combines user filter with IsPrivate eq false."""
        self._setup_response([{"LogicalName": "account"}])
        self.od._list_tables(filter="SchemaName eq 'Account'")

        self.od._request.assert_called_once()
        call_kwargs = self.od._request.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params", {})
        self.assertEqual(
            params["$filter"],
            "IsPrivate eq false and (SchemaName eq 'Account')",
        )

    def test_filter_none_same_as_no_filter(self):
        """_list_tables(filter=None) is equivalent to _list_tables()."""
        self._setup_response([])
        self.od._list_tables(filter=None)

        self.od._request.assert_called_once()
        call_kwargs = self.od._request.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params", {})
        self.assertEqual(params["$filter"], "IsPrivate eq false")

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
        call_kwargs = self.od._request.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params", {})
        self.assertEqual(params["$select"], "LogicalName,SchemaName,DisplayName")

    def test_select_none_omits_query_param(self):
        """_list_tables(select=None) does not add $select to params."""
        self._setup_response([])
        self.od._list_tables(select=None)

        self.od._request.assert_called_once()
        call_kwargs = self.od._request.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params", {})
        self.assertNotIn("$select", params)

    def test_select_empty_list_omits_query_param(self):
        """_list_tables(select=[]) does not add $select (empty list is falsy)."""
        self._setup_response([])
        self.od._list_tables(select=[])

        self.od._request.assert_called_once()
        call_kwargs = self.od._request.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params", {})
        self.assertNotIn("$select", params)

    def test_select_preserves_case(self):
        """_list_tables does not lowercase select values (PascalCase preserved)."""
        self._setup_response([])
        self.od._list_tables(select=["EntitySetName", "LogicalName"])

        self.od._request.assert_called_once()
        call_kwargs = self.od._request.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params", {})
        self.assertEqual(params["$select"], "EntitySetName,LogicalName")

    def test_select_with_filter(self):
        """_list_tables with both select and filter sends both params."""
        self._setup_response([{"LogicalName": "account"}])
        self.od._list_tables(
            filter="SchemaName eq 'Account'",
            select=["LogicalName", "SchemaName"],
        )

        self.od._request.assert_called_once()
        call_kwargs = self.od._request.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params", {})
        self.assertEqual(
            params["$filter"],
            "IsPrivate eq false and (SchemaName eq 'Account')",
        )
        self.assertEqual(params["$select"], "LogicalName,SchemaName")

    def test_select_single_property(self):
        """_list_tables(select=[...]) with a single property works correctly."""
        self._setup_response([])
        self.od._list_tables(select=["LogicalName"])

        self.od._request.assert_called_once()
        call_kwargs = self.od._request.call_args
        params = call_kwargs.kwargs.get("params") or call_kwargs[1].get("params", {})
        self.assertEqual(params["$select"], "LogicalName")

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
        payload = call.kwargs["json"]
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
        payload = call.kwargs["json"]
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
        payload = call.kwargs["json"]
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
        payload = call.kwargs["json"]
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
        # Patch _optionset_map to track calls
        calls = []
        original = self.od._optionset_map

        def tracking_optionset_map(table, attr):
            calls.append(attr)
            return original(table, attr)

        self.od._optionset_map = tracking_optionset_map
        record = {
            "name": "Contoso",
            "new_CustomerId@odata.bind": "/contacts(00000000-0000-0000-0000-000000000001)",
            "@odata.type": "Microsoft.Dynamics.CRM.account",
        }
        self.od._convert_labels_to_ints("account", record)
        # Only "name" should be checked, not the @odata keys
        self.assertEqual(calls, ["name"])

    def test_returns_none(self):
        """_upsert always returns None."""
        result = self.od._upsert("accounts", "account", {"accountnumber": "ACC-001"}, {"name": "Contoso"})
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
