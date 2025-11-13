# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from unittest.mock import Mock, patch
import pytest
import requests

from PowerPlatform.Dataverse.core.http import HttpClient


class TestHttpClientRetryLogic:
    """Test comprehensive retry logic in HttpClient."""

    def test_default_configuration(self):
        """Test that HttpClient uses proper defaults."""
        client = HttpClient()
        assert client.max_attempts == 5
        assert client.base_delay == 0.5
        assert client.max_backoff == 60.0
        assert client.jitter is True
        assert client.retry_transient_errors is True
        assert client.transient_status_codes == {429, 502, 503, 504}

    def test_custom_configuration(self):
        """Test HttpClient with custom configuration."""
        client = HttpClient(
            retries=3,
            backoff=1.0,
            max_backoff=30.0,
            jitter=False,
            retry_transient_errors=False
        )
        assert client.max_attempts == 3
        assert client.base_delay == 1.0
        assert client.max_backoff == 30.0
        assert client.jitter is False
        assert client.retry_transient_errors is False

    @patch('requests.request')
    def test_successful_request_no_retry(self, mock_request):
        """Test that successful requests don't trigger retries."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_request.return_value = mock_response
        
        client = HttpClient()
        response = client.request("GET", "https://test.example.com")
        
        assert response.status_code == 200
        assert mock_request.call_count == 1

    @patch('requests.request')
    @patch('time.sleep')
    def test_network_error_retry(self, mock_sleep, mock_request):
        """Test retry on network errors (requests.exceptions.RequestException)."""
        # First two calls fail, third succeeds
        mock_request.side_effect = [
            requests.exceptions.ConnectionError("Network error"),
            requests.exceptions.ConnectionError("Network error"),
            Mock(status_code=200)
        ]
        
        client = HttpClient(jitter=False)  # Disable jitter for predictable testing
        response = client.request("GET", "https://test.example.com")
        
        assert response.status_code == 200
        assert mock_request.call_count == 3
        assert mock_sleep.call_count == 2
        # Check exponential backoff: 0.5, 1.0
        mock_sleep.assert_any_call(0.5)
        mock_sleep.assert_any_call(1.0)

    @patch('requests.request')
    @patch('time.sleep')
    def test_transient_http_error_retry(self, mock_sleep, mock_request):
        """Test retry on transient HTTP status codes."""
        # First call returns 429, second call succeeds
        mock_429_response = Mock(status_code=429, headers={})
        mock_200_response = Mock(status_code=200, headers={})
        mock_request.side_effect = [mock_429_response, mock_200_response]
        
        client = HttpClient(jitter=False)
        response = client.request("GET", "https://test.example.com")
        
        assert response.status_code == 200
        assert mock_request.call_count == 2
        assert mock_sleep.call_count == 1
        mock_sleep.assert_called_with(0.5)  # Base delay

    @patch('requests.request')
    @patch('time.sleep')
    def test_retry_after_header_respected(self, mock_sleep, mock_request):
        """Test that Retry-After header is respected for 429 responses."""
        mock_429_response = Mock(status_code=429, headers={"Retry-After": "5"})
        mock_200_response = Mock(status_code=200, headers={})
        mock_request.side_effect = [mock_429_response, mock_200_response]
        
        client = HttpClient(jitter=False)
        response = client.request("GET", "https://test.example.com")
        
        assert response.status_code == 200
        assert mock_request.call_count == 2
        assert mock_sleep.call_count == 1
        mock_sleep.assert_called_with(5)  # Retry-After value

    @patch('requests.request')
    @patch('time.sleep')
    def test_retry_after_header_capped_at_max_backoff(self, mock_sleep, mock_request):
        """Test that Retry-After header is capped at max_backoff."""
        mock_429_response = Mock(status_code=429, headers={"Retry-After": "120"})  # 2 minutes
        mock_200_response = Mock(status_code=200, headers={})
        mock_request.side_effect = [mock_429_response, mock_200_response]
        
        client = HttpClient(jitter=False, max_backoff=30.0)
        response = client.request("GET", "https://test.example.com")
        
        assert response.status_code == 200
        assert mock_sleep.call_count == 1
        mock_sleep.assert_called_with(30.0)  # Capped at max_backoff

    @patch('requests.request')
    @patch('time.sleep')
    def test_invalid_retry_after_header_fallback(self, mock_sleep, mock_request):
        """Test fallback to exponential backoff when Retry-After is invalid."""
        mock_429_response = Mock(status_code=429, headers={"Retry-After": "invalid"})
        mock_200_response = Mock(status_code=200, headers={})
        mock_request.side_effect = [mock_429_response, mock_200_response]
        
        client = HttpClient(jitter=False)
        response = client.request("GET", "https://test.example.com")
        
        assert response.status_code == 200
        assert mock_sleep.call_count == 1
        mock_sleep.assert_called_with(0.5)  # Falls back to exponential backoff

    @patch('requests.request')
    def test_non_transient_error_no_retry(self, mock_request):
        """Test that non-transient HTTP errors are not retried."""
        mock_404_response = Mock(status_code=404, headers={})
        mock_request.return_value = mock_404_response
        
        client = HttpClient()
        response = client.request("GET", "https://test.example.com")
        
        assert response.status_code == 404
        assert mock_request.call_count == 1

    @patch('requests.request')
    @patch('time.sleep')
    def test_retry_disabled_for_transient_errors(self, mock_sleep, mock_request):
        """Test that transient error retry can be disabled."""
        mock_429_response = Mock(status_code=429, headers={})
        mock_request.return_value = mock_429_response
        
        client = HttpClient(retry_transient_errors=False)
        response = client.request("GET", "https://test.example.com")
        
        assert response.status_code == 429
        assert mock_request.call_count == 1
        assert mock_sleep.call_count == 0

    @patch('requests.request')
    @patch('time.sleep')
    def test_max_attempts_respected(self, mock_sleep, mock_request):
        """Test that max_attempts is respected."""
        mock_request.side_effect = requests.exceptions.ConnectionError("Network error")
        
        client = HttpClient(retries=2, jitter=False)
        
        with pytest.raises(requests.exceptions.ConnectionError):
            client.request("GET", "https://test.example.com")
        
        assert mock_request.call_count == 2  # max_attempts
        assert mock_sleep.call_count == 1  # One retry

    @patch('requests.request')
    @patch('time.sleep')
    def test_exponential_backoff_capped(self, mock_sleep, mock_request):
        """Test that exponential backoff is capped at max_backoff."""
        mock_request.side_effect = [
            requests.exceptions.ConnectionError("Network error"),
            requests.exceptions.ConnectionError("Network error"),
            requests.exceptions.ConnectionError("Network error"),
            Mock(status_code=200)
        ]
        
        client = HttpClient(retries=4, backoff=10.0, max_backoff=15.0, jitter=False)
        response = client.request("GET", "https://test.example.com")
        
        assert response.status_code == 200
        assert mock_sleep.call_count == 3
        # Delays should be: 10.0, 15.0 (capped), 15.0 (capped)
        calls = mock_sleep.call_args_list
        assert calls[0][0][0] == 10.0  # 10.0 * (2^0)
        assert calls[1][0][0] == 15.0  # 10.0 * (2^1) = 20.0, capped at 15.0
        assert calls[2][0][0] == 15.0  # 10.0 * (2^2) = 40.0, capped at 15.0

    @patch('requests.request')
    @patch('time.sleep')
    @patch('random.uniform')
    def test_jitter_applied(self, mock_uniform, mock_sleep, mock_request):
        """Test that jitter is applied when enabled."""
        mock_uniform.return_value = 0.1  # Fixed jitter value for testing
        mock_request.side_effect = [
            requests.exceptions.ConnectionError("Network error"),
            Mock(status_code=200)
        ]
        
        client = HttpClient(jitter=True, backoff=1.0)
        response = client.request("GET", "https://test.example.com")
        
        assert response.status_code == 200
        assert mock_uniform.called
        # Jitter should be ±25% of delay, so for 1.0s delay: ±0.25
        mock_uniform.assert_called_with(-0.25, 0.25)
        # Final delay should be 1.0 + 0.1 = 1.1
        mock_sleep.assert_called_with(1.1)

    @patch('requests.request')
    @patch('time.sleep')
    def test_method_specific_timeouts(self, mock_sleep, mock_request):
        """Test that method-specific default timeouts are applied."""
        mock_request.return_value = Mock(status_code=200)
        
        client = HttpClient()
        
        # Test GET request (should get 10s timeout)
        client.request("GET", "https://test.example.com")
        args, kwargs = mock_request.call_args
        assert kwargs["timeout"] == 10
        
        # Test POST request (should get 120s timeout)
        client.request("POST", "https://test.example.com")
        args, kwargs = mock_request.call_args
        assert kwargs["timeout"] == 120
        
        # Test DELETE request (should get 120s timeout)
        client.request("DELETE", "https://test.example.com")
        args, kwargs = mock_request.call_args
        assert kwargs["timeout"] == 120

    @patch('requests.request')
    def test_custom_timeout_respected(self, mock_request):
        """Test that custom timeout overrides defaults."""
        mock_request.return_value = Mock(status_code=200)
        
        client = HttpClient(timeout=30.0)
        client.request("GET", "https://test.example.com")
        
        args, kwargs = mock_request.call_args
        assert kwargs["timeout"] == 30.0

    @patch('requests.request')
    @patch('time.sleep')
    def test_all_transient_status_codes_retried(self, mock_sleep, mock_request):
        """Test that all transient status codes are retried."""
        for status_code in [429, 502, 503, 504]:
            mock_sleep.reset_mock()
            mock_request.reset_mock()
            
            mock_error_response = Mock(status_code=status_code, headers={})
            mock_success_response = Mock(status_code=200, headers={})
            mock_request.side_effect = [mock_error_response, mock_success_response]
            
            client = HttpClient(jitter=False)
            response = client.request("GET", "https://test.example.com")
            
            assert response.status_code == 200
            assert mock_request.call_count == 2
            assert mock_sleep.call_count == 1
            mock_sleep.assert_called_with(0.5)  # Base delay