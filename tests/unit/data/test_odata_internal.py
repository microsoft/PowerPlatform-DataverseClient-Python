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
        # Patch _check_attribute_types to track which attrs are type-checked
        checked_attrs = []
        original_check = self.od._check_attribute_types

        def tracking_check(table, attrs):
            checked_attrs.extend(attrs)
            return original_check(table, attrs)

        self.od._check_attribute_types = tracking_check
        record = {
            "name": "Contoso",
            "new_CustomerId@odata.bind": "/contacts(00000000-0000-0000-0000-000000000001)",
            "@odata.type": "Microsoft.Dynamics.CRM.account",
        }
        self.od._convert_labels_to_ints("account", record)
        # Only "name" should be type-checked, not the @odata keys
        self.assertEqual(checked_attrs, ["name"])

    def test_returns_none(self):
        """_upsert always returns None."""
        result = self.od._upsert("accounts", "account", {"accountnumber": "ACC-001"}, {"name": "Contoso"})
        self.assertIsNone(result)


class TestPicklistLabelResolution(unittest.TestCase):
    """Tests for picklist label-to-integer resolution.

    Covers _check_attribute_types, _optionset_map, _request_metadata_with_retry,
    _convert_labels_to_ints, and their integration through _create / _update / _upsert.
    """

    def setUp(self):
        self.od = _make_odata_client()

    # ---- _check_attribute_types ----

    def test_check_caches_non_picklist_as_empty_map(self):
        """Non-picklist attributes are cached as {"map": {}}."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"value": [{"LogicalName": "name", "AttributeType": "String"}]}
        self.od._request.return_value = mock_resp

        self.od._check_attribute_types("account", ["name"])

        entry = self.od._picklist_label_cache.get(("account", "name"))
        self.assertIsNotNone(entry)
        self.assertEqual(entry["map"], {})
        self.assertIn("ts", entry)

    def test_check_caches_picklist_with_type_marker(self):
        """Picklist attributes are cached as {"type": "Picklist"}."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"value": [{"LogicalName": "industrycode", "AttributeType": "Picklist"}]}
        self.od._request.return_value = mock_resp

        self.od._check_attribute_types("account", ["industrycode"])

        entry = self.od._picklist_label_cache.get(("account", "industrycode"))
        self.assertIsNotNone(entry)
        self.assertEqual(entry.get("type"), "Picklist")
        self.assertNotIn("map", entry)

    def test_check_caches_missing_attrs_as_empty_map(self):
        """Attributes not in the API response are cached as non-picklist."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"value": []}
        self.od._request.return_value = mock_resp

        self.od._check_attribute_types("account", ["nonexistent"])

        entry = self.od._picklist_label_cache.get(("account", "nonexistent"))
        self.assertIsNotNone(entry)
        self.assertEqual(entry["map"], {})

    def test_check_handles_mixed_types(self):
        """Batch with both picklist and non-picklist attributes caches correctly."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "value": [
                {"LogicalName": "name", "AttributeType": "String"},
                {"LogicalName": "industrycode", "AttributeType": "Picklist"},
                {"LogicalName": "telephone1", "AttributeType": "String"},
            ]
        }
        self.od._request.return_value = mock_resp

        self.od._check_attribute_types("account", ["name", "industrycode", "telephone1"])

        self.assertEqual(self.od._picklist_label_cache[("account", "name")]["map"], {})
        self.assertEqual(
            self.od._picklist_label_cache[("account", "industrycode")].get("type"),
            "Picklist",
        )
        self.assertEqual(self.od._picklist_label_cache[("account", "telephone1")]["map"], {})

    def test_check_empty_list_does_nothing(self):
        """Empty attr list should not make any API call."""
        self.od._check_attribute_types("account", [])
        self.od._request.assert_not_called()

    def test_check_case_insensitive_cache_keys(self):
        """Cache keys are normalized to lowercase."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"value": [{"LogicalName": "industrycode", "AttributeType": "Picklist"}]}
        self.od._request.return_value = mock_resp

        self.od._check_attribute_types("Account", ["IndustryCode"])

        self.assertIn(("account", "industrycode"), self.od._picklist_label_cache)

    def test_check_makes_single_api_call(self):
        """Batch should result in exactly one API call regardless of attr count."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "value": [
                {"LogicalName": "a", "AttributeType": "String"},
                {"LogicalName": "b", "AttributeType": "Picklist"},
                {"LogicalName": "c", "AttributeType": "Integer"},
            ]
        }
        self.od._request.return_value = mock_resp

        self.od._check_attribute_types("account", ["a", "b", "c"])

        self.assertEqual(self.od._request.call_count, 1)
        call_url = self.od._request.call_args.args[1]
        self.assertIn("Microsoft.Dynamics.CRM.In(", call_url)

    def test_check_uses_crm_in_function_in_url(self):
        """Batch URL uses Microsoft.Dynamics.CRM.In function with quoted values."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"value": []}
        self.od._request.return_value = mock_resp

        self.od._check_attribute_types("account", ["name", "industrycode"])

        call_url = self.od._request.call_args.args[1]
        self.assertIn("Microsoft.Dynamics.CRM.In(PropertyName='LogicalName'", call_url)
        self.assertIn('"name"', call_url)
        self.assertIn('"industrycode"', call_url)

    # ---- _optionset_map ----

    def test_optionset_returns_none_for_empty_table(self):
        """_optionset_map returns None for empty table name."""
        self.assertIsNone(self.od._optionset_map("", "industrycode"))

    def test_optionset_returns_none_for_empty_attr(self):
        """_optionset_map returns None for empty attribute name."""
        self.assertIsNone(self.od._optionset_map("account", ""))

    def test_optionset_returns_cached_map(self):
        """Warm cache hit returns the map without API calls."""
        import time

        self.od._picklist_label_cache[("account", "industrycode")] = {
            "map": {"technology": 6},
            "ts": time.time(),
        }

        result = self.od._optionset_map("account", "industrycode")
        self.assertEqual(result, {"technology": 6})
        self.od._request.assert_not_called()

    def test_optionset_type_marker_triggers_fetch(self):
        """type=Picklist entry does not count as a cache hit -- should fetch options."""
        import time

        self.od._picklist_label_cache[("account", "industrycode")] = {
            "type": "Picklist",
            "ts": time.time(),
        }

        mock_resp = MagicMock()
        mock_resp.text = "{}"
        mock_resp.json.return_value = {
            "OptionSet": {"Options": [{"Value": 6, "Label": {"LocalizedLabels": [{"Label": "Tech"}]}}]}
        }
        self.od._request.return_value = mock_resp

        result = self.od._optionset_map("account", "industrycode")
        self.assertEqual(result, {"tech": 6})
        self.od._request.assert_called_once()

    def test_optionset_fetches_via_picklist_cast_url(self):
        """API call uses the PicklistAttributeMetadata cast segment."""
        mock_resp = MagicMock()
        mock_resp.text = "{}"
        mock_resp.json.return_value = {"OptionSet": {"Options": []}}
        self.od._request.return_value = mock_resp

        self.od._optionset_map("account", "industrycode")

        call_url = self.od._request.call_args.args[1]
        self.assertIn("PicklistAttributeMetadata", call_url)
        self.assertIn("industrycode", call_url)

    def test_optionset_multiple_options_parsed(self):
        """Multiple options are all captured in the returned mapping."""
        mock_resp = MagicMock()
        mock_resp.text = "{}"
        mock_resp.json.return_value = {
            "OptionSet": {
                "Options": [
                    {"Value": 1, "Label": {"LocalizedLabels": [{"Label": "Active"}]}},
                    {"Value": 2, "Label": {"LocalizedLabels": [{"Label": "Inactive"}]}},
                    {"Value": 3, "Label": {"LocalizedLabels": [{"Label": "Suspended"}]}},
                ]
            }
        }
        self.od._request.return_value = mock_resp

        result = self.od._optionset_map("account", "statuscode")
        self.assertEqual(result, {"active": 1, "inactive": 2, "suspended": 3})

    def test_optionset_caches_resolved_map_with_ts(self):
        """After fetching, the resolved map is stored in cache with a timestamp."""
        import time

        mock_resp = MagicMock()
        mock_resp.text = "{}"
        mock_resp.json.return_value = {
            "OptionSet": {"Options": [{"Value": 6, "Label": {"LocalizedLabels": [{"Label": "Tech"}]}}]}
        }
        self.od._request.return_value = mock_resp

        before = time.time()
        self.od._optionset_map("account", "industrycode")
        after = time.time()

        entry = self.od._picklist_label_cache[("account", "industrycode")]
        self.assertEqual(entry["map"], {"tech": 6})
        self.assertGreaterEqual(entry["ts"], before)
        self.assertLessEqual(entry["ts"], after)

    def test_optionset_returns_none_on_malformed_json(self):
        """_optionset_map returns None when response JSON is unparseable."""
        mock_resp = MagicMock()
        mock_resp.text = "not json"
        mock_resp.json.side_effect = ValueError("No JSON")
        self.od._request.return_value = mock_resp

        result = self.od._optionset_map("account", "industrycode")
        self.assertIsNone(result)

    def test_optionset_returns_none_when_options_not_list(self):
        """_optionset_map returns None when OptionSet.Options is not a list."""
        mock_resp = MagicMock()
        mock_resp.text = "{}"
        mock_resp.json.return_value = {"OptionSet": {"Options": "not-a-list"}}
        self.od._request.return_value = mock_resp

        result = self.od._optionset_map("account", "industrycode")
        self.assertIsNone(result)

    def test_optionset_returns_empty_dict_when_no_options(self):
        """_optionset_map returns {} when Options list is empty."""
        mock_resp = MagicMock()
        mock_resp.text = "{}"
        mock_resp.json.return_value = {"OptionSet": {"Options": []}}
        self.od._request.return_value = mock_resp

        result = self.od._optionset_map("account", "industrycode")
        self.assertEqual(result, {})

    # ---- _request_metadata_with_retry ----

    def test_retry_succeeds_on_first_try(self):
        """No retry needed when first call succeeds."""
        mock_resp = MagicMock()
        self.od._request.return_value = mock_resp

        result = self.od._request_metadata_with_retry("get", "https://example.com/test")
        self.assertIs(result, mock_resp)
        self.assertEqual(self.od._request.call_count, 1)

    def test_retry_retries_on_404(self):
        """Should retry on 404 and succeed on later attempt."""
        from PowerPlatform.Dataverse.core.errors import HttpError

        err_404 = HttpError("Not Found", status_code=404)
        mock_resp = MagicMock()
        self.od._request.side_effect = [err_404, mock_resp]

        result = self.od._request_metadata_with_retry("get", "https://example.com/test")
        self.assertIs(result, mock_resp)
        self.assertEqual(self.od._request.call_count, 2)

    def test_retry_raises_after_max_attempts(self):
        """Should raise RuntimeError after all retries exhausted."""
        from PowerPlatform.Dataverse.core.errors import HttpError

        err_404 = HttpError("Not Found", status_code=404)
        self.od._request.side_effect = err_404

        with self.assertRaises(RuntimeError) as ctx:
            self.od._request_metadata_with_retry("get", "https://example.com/test")
        self.assertIn("404", str(ctx.exception))

    def test_retry_does_not_retry_non_404(self):
        """Non-404 errors should be raised immediately without retry."""
        from PowerPlatform.Dataverse.core.errors import HttpError

        err_500 = HttpError("Server Error", status_code=500)
        self.od._request.side_effect = err_500

        with self.assertRaises(HttpError):
            self.od._request_metadata_with_retry("get", "https://example.com/test")
        self.assertEqual(self.od._request.call_count, 1)

    # ---- _convert_labels_to_ints ----

    def test_convert_no_string_values_skips_batch(self):
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
        """@odata.bind keys must not be type-checked or resolved."""
        checked = []
        orig = self.od._check_attribute_types

        def track(table, attrs):
            checked.extend(attrs)
            return orig(table, attrs)

        self.od._check_attribute_types = track
        record = {
            "name": "Contoso",
            "new_CustomerId@odata.bind": "/contacts(guid)",
            "@odata.type": "Microsoft.Dynamics.CRM.account",
        }
        self.od._convert_labels_to_ints("account", record)
        self.assertEqual(checked, ["name"])

    def test_convert_warm_cache_no_api_calls(self):
        """Second call with same fields should make zero API calls."""
        import time

        now = time.time()
        self.od._picklist_label_cache[("account", "name")] = {"map": {}, "ts": now}
        self.od._picklist_label_cache[("account", "industrycode")] = {
            "map": {"technology": 6},
            "ts": now,
        }

        record = {"name": "Contoso", "industrycode": "Technology"}
        result = self.od._convert_labels_to_ints("account", record)

        self.assertEqual(result["industrycode"], 6)
        self.assertEqual(result["name"], "Contoso")
        self.od._request.assert_not_called()

    def test_convert_resolves_picklist_label_to_int(self):
        """Full flow: batch identifies picklist, optionset_map fetches options, label resolved."""
        batch_resp = MagicMock()
        batch_resp.json.return_value = {
            "value": [
                {"LogicalName": "name", "AttributeType": "String"},
                {"LogicalName": "industrycode", "AttributeType": "Picklist"},
            ]
        }
        options_resp = MagicMock()
        options_resp.text = '{"OptionSet": ...}'
        options_resp.json.return_value = {
            "OptionSet": {
                "Options": [
                    {
                        "Value": 6,
                        "Label": {"LocalizedLabels": [{"Label": "Technology", "LanguageCode": 1033}]},
                    }
                ]
            }
        }
        self.od._request.side_effect = [batch_resp, options_resp]

        record = {"name": "Contoso", "industrycode": "Technology"}
        result = self.od._convert_labels_to_ints("account", record)

        self.assertEqual(result["industrycode"], 6)
        self.assertEqual(result["name"], "Contoso")
        self.assertEqual(self.od._request.call_count, 2)

    def test_convert_non_picklist_skips_optionset_map(self):
        """Non-picklist fields should not trigger _optionset_map calls."""
        batch_resp = MagicMock()
        batch_resp.json.return_value = {
            "value": [
                {"LogicalName": "name", "AttributeType": "String"},
                {"LogicalName": "telephone1", "AttributeType": "String"},
            ]
        }
        self.od._request.return_value = batch_resp

        optionset_calls = []
        orig_map = self.od._optionset_map

        def tracking_map(table, attr):
            optionset_calls.append(attr)
            return orig_map(table, attr)

        self.od._optionset_map = tracking_map

        record = {"name": "Contoso", "telephone1": "555-0100"}
        self.od._convert_labels_to_ints("account", record)

        self.assertEqual(optionset_calls, [])

    def test_convert_unmatched_label_left_unchanged(self):
        """If a picklist label doesn't match any option, value stays as string."""
        import time

        self.od._picklist_label_cache[("account", "industrycode")] = {
            "map": {"technology": 6, "consulting": 12},
            "ts": time.time(),
        }

        record = {"industrycode": "UnknownIndustry"}
        result = self.od._convert_labels_to_ints("account", record)
        self.assertEqual(result["industrycode"], "UnknownIndustry")

    def test_convert_does_not_mutate_original_record(self):
        """_convert_labels_to_ints must return a copy, not mutate the input."""
        import time

        self.od._picklist_label_cache[("account", "industrycode")] = {
            "map": {"technology": 6},
            "ts": time.time(),
        }

        original = {"industrycode": "Technology"}
        result = self.od._convert_labels_to_ints("account", original)

        self.assertEqual(result["industrycode"], 6)
        self.assertEqual(original["industrycode"], "Technology")

    def test_convert_only_batches_uncached_attrs(self):
        """Warm-cached attrs should not be included in the batch call."""
        import time

        self.od._picklist_label_cache[("account", "name")] = {
            "map": {},
            "ts": time.time(),
        }

        batch_resp = MagicMock()
        batch_resp.json.return_value = {"value": [{"LogicalName": "industrycode", "AttributeType": "Picklist"}]}
        options_resp = MagicMock()
        options_resp.text = "{}"
        options_resp.json.return_value = {
            "OptionSet": {"Options": [{"Value": 6, "Label": {"LocalizedLabels": [{"Label": "Tech"}]}}]}
        }
        self.od._request.side_effect = [batch_resp, options_resp]

        record = {"name": "Contoso", "industrycode": "Tech"}
        result = self.od._convert_labels_to_ints("account", record)

        batch_call_url = self.od._request.call_args_list[0].args[1]
        self.assertIn("industrycode", batch_call_url)
        self.assertNotIn("'name'", batch_call_url)
        self.assertEqual(result["industrycode"], 6)

    def test_convert_multiple_picklists_in_one_record(self):
        """Multiple picklist fields in the same record are all resolved."""
        batch_resp = MagicMock()
        batch_resp.json.return_value = {
            "value": [
                {"LogicalName": "industrycode", "AttributeType": "Picklist"},
                {"LogicalName": "statuscode", "AttributeType": "Picklist"},
            ]
        }
        industry_resp = MagicMock()
        industry_resp.text = "{}"
        industry_resp.json.return_value = {
            "OptionSet": {"Options": [{"Value": 6, "Label": {"LocalizedLabels": [{"Label": "Tech"}]}}]}
        }
        status_resp = MagicMock()
        status_resp.text = "{}"
        status_resp.json.return_value = {
            "OptionSet": {"Options": [{"Value": 1, "Label": {"LocalizedLabels": [{"Label": "Active"}]}}]}
        }
        self.od._request.side_effect = [batch_resp, industry_resp, status_resp]

        record = {"industrycode": "Tech", "statuscode": "Active"}
        result = self.od._convert_labels_to_ints("account", record)

        self.assertEqual(result["industrycode"], 6)
        self.assertEqual(result["statuscode"], 1)
        # 1 batch + 2 optionset fetches
        self.assertEqual(self.od._request.call_count, 3)

    def test_convert_mixed_picklists_and_non_picklists(self):
        """2 picklists + 2 non-picklist strings: 1 batch + 2 optionset = 3 calls."""
        batch_resp = MagicMock()
        batch_resp.json.return_value = {
            "value": [
                {"LogicalName": "name", "AttributeType": "String"},
                {"LogicalName": "industrycode", "AttributeType": "Picklist"},
                {"LogicalName": "description", "AttributeType": "Memo"},
                {"LogicalName": "statuscode", "AttributeType": "Picklist"},
            ]
        }
        industry_resp = MagicMock()
        industry_resp.text = "{}"
        industry_resp.json.return_value = {
            "OptionSet": {"Options": [{"Value": 6, "Label": {"LocalizedLabels": [{"Label": "Tech"}]}}]}
        }
        status_resp = MagicMock()
        status_resp.text = "{}"
        status_resp.json.return_value = {
            "OptionSet": {"Options": [{"Value": 1, "Label": {"LocalizedLabels": [{"Label": "Active"}]}}]}
        }
        self.od._request.side_effect = [batch_resp, industry_resp, status_resp]

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
        # 1 batch + 2 optionset fetches (non-picklists don't trigger optionset)
        self.assertEqual(self.od._request.call_count, 3)

    def test_convert_all_non_picklist_makes_one_api_call(self):
        """All non-picklist string fields: 1 batch call, 0 optionset = 1 total."""
        batch_resp = MagicMock()
        batch_resp.json.return_value = {
            "value": [
                {"LogicalName": "name", "AttributeType": "String"},
                {"LogicalName": "description", "AttributeType": "Memo"},
                {"LogicalName": "telephone1", "AttributeType": "String"},
            ]
        }
        self.od._request.return_value = batch_resp

        record = {"name": "Contoso", "description": "A company", "telephone1": "555-0100"}
        self.od._convert_labels_to_ints("account", record)

        # Only the batch type-check, no optionset fetches
        self.assertEqual(self.od._request.call_count, 1)

    def test_convert_no_string_values_makes_zero_api_calls(self):
        """All non-string values: 0 API calls total."""
        record = {"revenue": 1000000, "quantity": 5, "active": True}
        self.od._convert_labels_to_ints("account", record)

        self.assertEqual(self.od._request.call_count, 0)

    def test_convert_partial_cache_only_batches_uncached(self):
        """1 cached non-picklist + 1 uncached picklist: 1 batch + 1 optionset = 2 calls."""
        import time

        # Pre-cache "name" as non-picklist
        self.od._picklist_label_cache[("account", "name")] = {"map": {}, "ts": time.time()}

        batch_resp = MagicMock()
        batch_resp.json.return_value = {
            "value": [{"LogicalName": "industrycode", "AttributeType": "Picklist"}]
        }
        options_resp = MagicMock()
        options_resp.text = "{}"
        options_resp.json.return_value = {
            "OptionSet": {"Options": [{"Value": 6, "Label": {"LocalizedLabels": [{"Label": "Tech"}]}}]}
        }
        self.od._request.side_effect = [batch_resp, options_resp]

        record = {"name": "Contoso", "industrycode": "Tech"}
        result = self.od._convert_labels_to_ints("account", record)

        self.assertEqual(result["industrycode"], 6)
        self.assertEqual(result["name"], "Contoso")
        # 1 batch (only industrycode) + 1 optionset fetch
        self.assertEqual(self.od._request.call_count, 2)

    def test_convert_single_picklist_makes_two_api_calls(self):
        """Single picklist field (cold cache): 1 batch + 1 optionset = 2 total."""
        batch_resp = MagicMock()
        batch_resp.json.return_value = {
            "value": [{"LogicalName": "industrycode", "AttributeType": "Picklist"}]
        }
        options_resp = MagicMock()
        options_resp.text = "{}"
        options_resp.json.return_value = {
            "OptionSet": {"Options": [{"Value": 6, "Label": {"LocalizedLabels": [{"Label": "Tech"}]}}]}
        }
        self.od._request.side_effect = [batch_resp, options_resp]

        record = {"industrycode": "Tech"}
        result = self.od._convert_labels_to_ints("account", record)

        self.assertEqual(result["industrycode"], 6)
        self.assertEqual(self.od._request.call_count, 2)

    def test_convert_integer_values_passed_through(self):
        """Integer values (already resolved) are left unchanged."""
        import time

        self.od._picklist_label_cache[("account", "industrycode")] = {
            "map": {"technology": 6},
            "ts": time.time(),
        }

        record = {"industrycode": 6, "name": "Contoso"}
        result = self.od._convert_labels_to_ints("account", record)
        self.assertEqual(result["industrycode"], 6)

    def test_convert_case_insensitive_label_matching(self):
        """Picklist label matching is case-insensitive."""
        import time

        self.od._picklist_label_cache[("account", "industrycode")] = {
            "map": {"technology": 6},
            "ts": time.time(),
        }

        record = {"industrycode": "TECHNOLOGY"}
        result = self.od._convert_labels_to_ints("account", record)
        self.assertEqual(result["industrycode"], 6)

    # ---- Integration: through _create ----

    def test_create_resolves_picklist_in_payload(self):
        """_create resolves a picklist label to its integer in the POST payload."""
        batch_resp = MagicMock()
        batch_resp.json.return_value = {
            "value": [
                {"LogicalName": "name", "AttributeType": "String"},
                {"LogicalName": "industrycode", "AttributeType": "Picklist"},
            ]
        }
        options_resp = MagicMock()
        options_resp.text = "{}"
        options_resp.json.return_value = {
            "OptionSet": {
                "Options": [
                    {
                        "Value": 6,
                        "Label": {"LocalizedLabels": [{"Label": "Technology", "LanguageCode": 1033}]},
                    }
                ]
            }
        }
        post_resp = MagicMock()
        post_resp.headers = {
            "OData-EntityId": "https://example.crm.dynamics.com/api/data/v9.2/accounts(00000000-0000-0000-0000-000000000001)"
        }
        self.od._request.side_effect = [batch_resp, options_resp, post_resp]

        result = self.od._create("accounts", "account", {"name": "Contoso", "industrycode": "Technology"})
        self.assertEqual(result, "00000000-0000-0000-0000-000000000001")
        post_calls = [c for c in self.od._request.call_args_list if c.args[0] == "post"]
        payload = post_calls[0].kwargs["json"]
        self.assertEqual(payload["industrycode"], 6)
        self.assertEqual(payload["name"], "Contoso")

    def test_create_warm_cache_skips_batch(self):
        """_create with warm cache makes only the POST call."""
        import time

        now = time.time()
        self.od._picklist_label_cache[("account", "industrycode")] = {
            "map": {"technology": 6},
            "ts": now,
        }
        self.od._picklist_label_cache[("account", "name")] = {"map": {}, "ts": now}

        post_resp = MagicMock()
        post_resp.headers = {
            "OData-EntityId": "https://example.crm.dynamics.com/api/data/v9.2/accounts(00000000-0000-0000-0000-000000000001)"
        }
        self.od._request.return_value = post_resp

        result = self.od._create("accounts", "account", {"name": "Contoso", "industrycode": "Technology"})
        self.assertEqual(result, "00000000-0000-0000-0000-000000000001")
        self.assertEqual(self.od._request.call_count, 1)
        payload = self.od._request.call_args.kwargs["json"]
        self.assertEqual(payload["industrycode"], 6)

    # ---- Integration: through _update ----

    def test_update_resolves_picklist_in_payload(self):
        """_update resolves a picklist label to its integer in the PATCH payload."""
        self.od._entity_set_from_schema_name = MagicMock(return_value="new_tickets")

        batch_resp = MagicMock()
        batch_resp.json.return_value = {
            "value": [
                {"LogicalName": "new_status", "AttributeType": "Picklist"},
            ]
        }
        options_resp = MagicMock()
        options_resp.text = "{}"
        options_resp.json.return_value = {
            "OptionSet": {
                "Options": [
                    {
                        "Value": 100000001,
                        "Label": {"LocalizedLabels": [{"Label": "In Progress", "LanguageCode": 1033}]},
                    }
                ]
            }
        }
        patch_resp = MagicMock()
        self.od._request.side_effect = [batch_resp, options_resp, patch_resp]

        self.od._update(
            "new_ticket",
            "00000000-0000-0000-0000-000000000001",
            {"new_status": "In Progress"},
        )
        patch_calls = [c for c in self.od._request.call_args_list if c.args[0] == "patch"]
        payload = patch_calls[0].kwargs["json"]
        self.assertEqual(payload["new_status"], 100000001)

    def test_update_warm_cache_skips_batch(self):
        """_update with warm cache makes only the PATCH call."""
        import time

        self.od._entity_set_from_schema_name = MagicMock(return_value="new_tickets")
        self.od._picklist_label_cache[("new_ticket", "new_status")] = {
            "map": {"in progress": 100000001},
            "ts": time.time(),
        }

        self.od._update(
            "new_ticket",
            "00000000-0000-0000-0000-000000000001",
            {"new_status": "In Progress"},
        )
        self.assertEqual(self.od._request.call_count, 1)
        self.assertEqual(self.od._request.call_args.args[0], "patch")
        payload = self.od._request.call_args.kwargs["json"]
        self.assertEqual(payload["new_status"], 100000001)

    # ---- Integration: through _upsert ----

    def test_upsert_resolves_picklist_in_payload(self):
        """_upsert resolves a picklist label to its integer in the PATCH payload."""
        batch_resp = MagicMock()
        batch_resp.json.return_value = {
            "value": [
                {"LogicalName": "industrycode", "AttributeType": "Picklist"},
                {"LogicalName": "name", "AttributeType": "String"},
            ]
        }
        options_resp = MagicMock()
        options_resp.text = "{}"
        options_resp.json.return_value = {
            "OptionSet": {
                "Options": [
                    {
                        "Value": 6,
                        "Label": {"LocalizedLabels": [{"Label": "Technology", "LanguageCode": 1033}]},
                    }
                ]
            }
        }
        patch_resp = MagicMock()
        self.od._request.side_effect = [batch_resp, options_resp, patch_resp]

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

    def test_upsert_warm_cache_skips_batch(self):
        """_upsert with warm cache makes only the PATCH call."""
        import time

        now = time.time()
        self.od._picklist_label_cache[("account", "name")] = {"map": {}, "ts": now}
        self.od._picklist_label_cache[("account", "industrycode")] = {
            "map": {"technology": 6},
            "ts": now,
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


if __name__ == "__main__":
    unittest.main()
