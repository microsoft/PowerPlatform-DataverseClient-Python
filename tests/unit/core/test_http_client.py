# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

import unittest
from unittest.mock import MagicMock, patch, call

import requests

from PowerPlatform.Dataverse.core._http import _HttpClient


class TestHttpClientTimeout(unittest.TestCase):
    """Tests for automatic timeout selection in _HttpClient._request."""

    def _make_response(self, status=200):
        resp = MagicMock(spec=requests.Response)
        resp.status_code = status
        return resp

    def test_get_uses_10s_default_timeout(self):
        """GET requests use 10s default when no timeout is specified."""
        client = _HttpClient(retries=1)
        with patch("requests.request", return_value=self._make_response()) as mock_req:
            client._request("get", "https://example.com/data")
            _, kwargs = mock_req.call_args
            self.assertEqual(kwargs["timeout"], 10)

    def test_post_uses_120s_default_timeout(self):
        """POST requests use 120s default when no timeout is specified."""
        client = _HttpClient(retries=1)
        with patch("requests.request", return_value=self._make_response()) as mock_req:
            client._request("post", "https://example.com/data")
            _, kwargs = mock_req.call_args
            self.assertEqual(kwargs["timeout"], 120)

    def test_delete_uses_120s_default_timeout(self):
        """DELETE requests use 120s default when no timeout is specified."""
        client = _HttpClient(retries=1)
        with patch("requests.request", return_value=self._make_response()) as mock_req:
            client._request("delete", "https://example.com/data")
            _, kwargs = mock_req.call_args
            self.assertEqual(kwargs["timeout"], 120)

    def test_default_timeout_overrides_per_method_default(self):
        """Explicit default_timeout on the client overrides per-method defaults."""
        client = _HttpClient(retries=1, timeout=30.0)
        with patch("requests.request", return_value=self._make_response()) as mock_req:
            client._request("get", "https://example.com/data")
            _, kwargs = mock_req.call_args
            self.assertEqual(kwargs["timeout"], 30.0)

    def test_explicit_timeout_in_kwargs_is_not_overridden(self):
        """If timeout is already in kwargs it is passed through unchanged."""
        client = _HttpClient(retries=1, timeout=30.0)
        with patch("requests.request", return_value=self._make_response()) as mock_req:
            client._request("get", "https://example.com/data", timeout=5)
            _, kwargs = mock_req.call_args
            self.assertEqual(kwargs["timeout"], 5)


class TestHttpClientRequester(unittest.TestCase):
    """Tests for session vs direct requests.request routing."""

    def _make_response(self):
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        return resp

    def test_uses_requests_request_when_no_session(self):
        """Without a session, _request uses requests.request directly."""
        client = _HttpClient(retries=1)
        with patch("requests.request", return_value=self._make_response()) as mock_req:
            client._request("get", "https://example.com/data")
            mock_req.assert_called_once()

    def test_uses_session_request_when_session_provided(self):
        """With a session, _request uses session.request instead of requests.request."""
        mock_session = MagicMock(spec=requests.Session)
        mock_session.request.return_value = self._make_response()
        client = _HttpClient(retries=1, session=mock_session)
        with patch("requests.request") as mock_req:
            client._request("get", "https://example.com/data")
            mock_session.request.assert_called_once()
            mock_req.assert_not_called()


class TestHttpClientRetry(unittest.TestCase):
    """Tests for retry behavior on RequestException."""

    def test_retries_on_request_exception_and_succeeds(self):
        """Retries after a RequestException and returns response on second attempt."""
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        client = _HttpClient(retries=2, backoff=0)
        with patch("requests.request", side_effect=[requests.exceptions.ConnectionError(), resp]) as mock_req:
            with patch("time.sleep"):
                result = client._request("get", "https://example.com/data")
        self.assertEqual(mock_req.call_count, 2)
        self.assertIs(result, resp)

    def test_raises_after_all_retries_exhausted(self):
        """Raises RequestException after all retry attempts fail."""
        client = _HttpClient(retries=3, backoff=0)
        with patch("requests.request", side_effect=requests.exceptions.ConnectionError("timeout")):
            with patch("time.sleep"):
                with self.assertRaises(requests.exceptions.RequestException):
                    client._request("get", "https://example.com/data")

    def test_backoff_delay_between_retries(self):
        """Sleeps with exponential backoff between retry attempts."""
        resp = MagicMock(spec=requests.Response)
        resp.status_code = 200
        client = _HttpClient(retries=3, backoff=1.0)
        side_effects = [
            requests.exceptions.ConnectionError(),
            requests.exceptions.ConnectionError(),
            resp,
        ]
        with patch("requests.request", side_effect=side_effects):
            with patch("time.sleep") as mock_sleep:
                client._request("get", "https://example.com/data")
        # First retry: delay = 1.0 * 2^0 = 1.0, second retry: 1.0 * 2^1 = 2.0
        mock_sleep.assert_has_calls([call(1.0), call(2.0)])
