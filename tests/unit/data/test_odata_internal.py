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

    def test_returns_none(self):
        """_upsert always returns None."""
        result = self.od._upsert("accounts", "account", {"accountnumber": "ACC-001"}, {"name": "Contoso"})
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
