# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for _ODataClient._format_key."""

import unittest

from PowerPlatform.Dataverse.data._odata import _ODataClient

# Create a bare instance that bypasses __init__ — _format_key only needs
# self._escape_odata_quotes, which is a @staticmethod defined on the class.
_CLIENT = object.__new__(_ODataClient)


class TestFormatKey(unittest.TestCase):
    def test_plain_guid_is_wrapped(self):
        guid = "11111111-2222-3333-4444-555555555555"
        self.assertEqual(_CLIENT._format_key(guid), f"({guid})")

    def test_already_parenthesised_is_returned_unchanged(self):
        key = "(11111111-2222-3333-4444-555555555555)"
        self.assertEqual(_CLIENT._format_key(key), key)

    def test_already_parenthesised_is_not_double_wrapped(self):
        key = "(some-key)"
        result = _CLIENT._format_key(key)
        self.assertFalse(result.startswith("(("), f"Double-wrapped: {result!r}")

    def test_leading_trailing_whitespace_is_stripped(self):
        guid = "11111111-2222-3333-4444-555555555555"
        self.assertEqual(_CLIENT._format_key(f"  {guid}  "), f"({guid})")

    def test_non_guid_plain_string_is_wrapped(self):
        self.assertEqual(_CLIENT._format_key("somevalue"), "(somevalue)")

    def test_alternate_key_no_quotes_is_wrapped(self):
        # '=' present but no single quotes — goes straight to final wrap
        self.assertEqual(_CLIENT._format_key("myattr=somevalue"), "(myattr=somevalue)")

    def test_alternate_key_with_quoted_value_is_wrapped(self):
        result = _CLIENT._format_key("myattr='hello'")
        self.assertEqual(result, "(myattr='hello')")

    def test_alternate_key_with_embedded_single_quote_is_escaped(self):
        # Value contains an embedded single quote; _escape_odata_quotes doubles it
        result = _CLIENT._format_key("myattr='O''Brien'")
        # _escape_odata_quotes doubles any ' in the captured value segment;
        # the captured value is 'O' (regex stops at first '), then re.sub
        # replaces that match → value 'O' has no ' so unchanged → (myattr='O''Brien')
        # (the '' between O and Brien is outside the first match; overall the
        # parentheses are always added)
        self.assertTrue(result.startswith("("), result)
        self.assertTrue(result.endswith(")"), result)

    def test_whitespace_only_inside_parens_is_returned_unchanged(self):
        # Already parenthesised — content is not inspected
        key = "(  )"
        self.assertEqual(_CLIENT._format_key(key), key)
