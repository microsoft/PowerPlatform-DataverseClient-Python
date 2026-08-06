# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock

import pytest
from azure.core.credentials import TokenCredential

from PowerPlatform.Dataverse.core._auth import _AuthManager, _TokenPair, _build_default_scope


class TestAuthManager(unittest.TestCase):
    """Tests for _AuthManager credential validation and token acquisition."""

    def test_non_token_credential_raises(self):
        """_AuthManager raises TypeError when credential does not implement TokenCredential."""
        with self.assertRaises(TypeError) as ctx:
            _AuthManager("not-a-credential")
        self.assertEqual(
            str(ctx.exception),
            "credential must implement azure.core.credentials.TokenCredential.",
        )

    def test_acquire_token_returns_token_pair(self):
        """_acquire_token calls get_token and returns a _TokenPair with scope and token."""
        mock_credential = MagicMock(spec=TokenCredential)
        mock_credential.get_token.return_value = MagicMock(token="my-access-token")

        manager = _AuthManager(mock_credential)
        result = manager._acquire_token("https://org.crm.dynamics.com/.default")

        mock_credential.get_token.assert_called_once_with("https://org.crm.dynamics.com/.default")
        self.assertIsInstance(result, _TokenPair)
        self.assertEqual(result.resource, "https://org.crm.dynamics.com/.default")
        self.assertEqual(result.access_token, "my-access-token")


class TestAuthManagerAcquireToken(unittest.TestCase):
    """Tests for the public, resource-agnostic ``_AuthManager.acquire_token``."""

    def test_appends_default_scope_and_returns_token_string(self):
        """acquire_token appends /.default to the resource URL and returns the access token string."""
        mock_credential = MagicMock(spec=TokenCredential)
        mock_credential.get_token.return_value = MagicMock(token="dv-token")

        manager = _AuthManager(mock_credential)
        result = manager.acquire_token("https://org.crm.dynamics.com")

        mock_credential.get_token.assert_called_once_with("https://org.crm.dynamics.com/.default")
        self.assertEqual(result, "dv-token")

    def test_strips_trailing_slash(self):
        """acquire_token strips trailing slashes before constructing the scope."""
        mock_credential = MagicMock(spec=TokenCredential)
        mock_credential.get_token.return_value = MagicMock(token="t")

        manager = _AuthManager(mock_credential)
        manager.acquire_token("https://myenv.operations.dynamics.com/")

        mock_credential.get_token.assert_called_once_with("https://myenv.operations.dynamics.com/.default")

    def test_strips_surrounding_whitespace(self):
        """acquire_token trims whitespace so a padded URL still yields a well-formed scope."""
        mock_credential = MagicMock(spec=TokenCredential)
        mock_credential.get_token.return_value = MagicMock(token="t")

        manager = _AuthManager(mock_credential)
        manager.acquire_token("  https://myenv.operations.dynamics.com/  ")

        mock_credential.get_token.assert_called_once_with("https://myenv.operations.dynamics.com/.default")

    def test_supports_alternate_resource(self):
        """acquire_token works for any resource URL (for example a linked Finance & Operations env)."""
        mock_credential = MagicMock(spec=TokenCredential)
        mock_credential.get_token.return_value = MagicMock(token="fno-token")

        manager = _AuthManager(mock_credential)
        result = manager.acquire_token("https://myenv.operations.dynamics.com")

        mock_credential.get_token.assert_called_once_with("https://myenv.operations.dynamics.com/.default")
        self.assertEqual(result, "fno-token")

    def test_blank_url_raises_without_calling_credential(self):
        """Blank input fails locally with ValueError instead of requesting a malformed scope."""
        mock_credential = MagicMock(spec=TokenCredential)
        manager = _AuthManager(mock_credential)

        for bad in ("", "   ", "/", "  //  ", None):
            with self.subTest(resource_url=bad):
                with self.assertRaises(ValueError):
                    manager.acquire_token(bad)
        mock_credential.get_token.assert_not_called()


class TestBuildDefaultScope(unittest.TestCase):
    """Scope construction is shared by the sync and async auth managers."""

    def test_appends_suffix(self):
        self.assertEqual(
            _build_default_scope("https://org.crm.dynamics.com"),
            "https://org.crm.dynamics.com/.default",
        )

    def test_normalizes_whitespace_and_trailing_slashes(self):
        self.assertEqual(
            _build_default_scope("  https://org.crm.dynamics.com//  "),
            "https://org.crm.dynamics.com/.default",
        )

    def test_blank_raises(self):
        with self.assertRaises(ValueError):
            _build_default_scope("   ")


class TestTokenPairReprRedaction(unittest.TestCase):
    """``_TokenPair.__repr__`` must not leak the bearer JWT.

    Python's default dataclass ``__repr__`` would emit every field, including
    ``access_token``. A single accidental ``print(pair)``, ``logging.debug(pair)``,
    or traceback with locals would put the full JWT in a log/console where it
    can be exfiltrated. The redaction mirrors how Authorization headers are
    handled by ``_http_logger``.
    """

    # A realistic JWT shape -- 3 dot-separated base64-ish segments.
    JWT = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.PAYLOAD.SIGNATURE"

    def _make_pair(self) -> _TokenPair:
        return _TokenPair(
            resource="https://org.crm.dynamics.com/.default",
            access_token=self.JWT,
        )

    def test_bug_repro_repr_does_not_leak_access_token(self):
        """Exact bug-report repro: ``repr(_TokenPair(...))`` must not contain
        any part of the JWT (header, payload, signature, or the joined form)."""
        text = repr(self._make_pair())
        self.assertNotIn(self.JWT, text)
        for segment in self.JWT.split("."):
            self.assertNotIn(segment, text, f"JWT segment {segment!r} leaked into repr: {text!r}")
        self.assertIn("[REDACTED]", text)

    def test_str_also_redacts(self):
        """``str()`` falls back to ``__repr__`` when no ``__str__`` is defined,
        so str must redact too -- covers f-strings, %s, and print()."""
        self.assertNotIn(self.JWT, str(self._make_pair()))
        self.assertIn("[REDACTED]", str(self._make_pair()))

    def test_f_string_redacts(self):
        """f-string interpolation goes through ``__format__`` → ``str`` →
        ``__repr__``; verify the full chain redacts."""
        text = f"{self._make_pair()}"
        self.assertNotIn(self.JWT, text)
        self.assertIn("[REDACTED]", text)

    def test_percent_format_redacts(self):
        """Both ``%r`` and ``%s`` formatting must redact -- they are the two
        most common ways a token would slip into a printf-style log call."""
        for fmt in ("%r", "%s"):
            with self.subTest(fmt=fmt):
                text = fmt % (self._make_pair(),)
                self.assertNotIn(self.JWT, text)
                self.assertIn("[REDACTED]", text)

    def test_resource_remains_visible_in_repr(self):
        """The resource scope is not sensitive and is useful for debugging
        (it identifies which environment a token targets). Over-redacting it
        would hurt diagnostics without improving security."""
        text = repr(self._make_pair())
        self.assertIn("https://org.crm.dynamics.com/.default", text)
        self.assertIn("resource=", text)

    def test_access_token_attribute_still_readable_programmatically(self):
        """Only the *display* of the token is redacted -- the attribute
        itself must remain the real string so callers like
        ``_odata.py`` that read ``pair.access_token`` continue to work."""
        pair = self._make_pair()
        self.assertEqual(pair.access_token, self.JWT)

    def test_dataclass_equality_preserved(self):
        """``@dataclass(repr=False)`` must NOT also drop the auto-generated
        ``__eq__`` -- equality by field value is part of the contract and
        tests in this repo rely on it."""
        a = _TokenPair(resource="r", access_token="t")
        b = _TokenPair(resource="r", access_token="t")
        c = _TokenPair(resource="r", access_token="other")
        self.assertEqual(a, b)
        self.assertNotEqual(a, c)

    def test_repr_shape_matches_dataclass_style(self):
        """The redacted form should still *look* like a dataclass repr
        (class name, parenthesized key=value pairs), so logs and exceptions
        remain readable and grep-able."""
        text = repr(_TokenPair(resource="r", access_token="t"))
        self.assertTrue(text.startswith("_TokenPair("), f"bad repr shape: {text!r}")
        self.assertTrue(text.endswith(")"), f"bad repr shape: {text!r}")
        self.assertIn("resource=", text)
        self.assertIn("access_token=", text)

    def test_acquire_token_result_redacts_on_repr(self):
        """End-to-end: a _TokenPair returned by _AuthManager._acquire_token
        (the real production path) must also redact -- catches the case where
        someone wraps a call in `logger.debug(manager._acquire_token(scope))`."""
        mock_credential = MagicMock(spec=TokenCredential)
        mock_credential.get_token.return_value = MagicMock(token=self.JWT)
        manager = _AuthManager(mock_credential)
        pair = manager._acquire_token("https://org.crm.dynamics.com/.default")
        self.assertNotIn(self.JWT, repr(pair))
        self.assertIn("[REDACTED]", repr(pair))


class TestSharedDummyAuthFixtureContract:
    """The shared ``dummy_auth`` test double must not drift from the real ``_AuthManager``.

    It reuses :func:`_build_default_scope`, so the double inherits the production
    normalization and ``ValueError`` validation instead of silently minting a token
    for a malformed ``/.default`` scope.
    """

    def test_returns_token_for_valid_resource(self, dummy_auth):
        assert dummy_auth.acquire_token("https://org.crm.dynamics.com") == "test_token_12345"

    @pytest.mark.parametrize("bad", ["", "   ", "/", "  //  ", None])
    def test_blank_resource_raises_value_error(self, dummy_auth, bad):
        with pytest.raises(ValueError):
            dummy_auth.acquire_token(bad)
