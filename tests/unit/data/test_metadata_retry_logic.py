# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

from unittest.mock import Mock, patch
import pytest

from PowerPlatform.Dataverse.core.errors import HttpError
from PowerPlatform.Dataverse.data.odata import ODataClient


class DummyAuth:
    def acquire_token(self, scope):
        class T: 
            access_token = "test_token"
        return T()


class TestMetadataRetryLogic:
    """Test metadata-specific retry logic in ODataClient."""

    def setup_method(self):
        """Set up test client."""
        self.auth = DummyAuth()
        self.base_url = "https://test.example.com"
        self.client = ODataClient(self.auth, self.base_url)

    @patch.object(ODataClient, '_request')
    def test_metadata_retry_success_first_attempt(self, mock_request):
        """Test successful metadata request on first attempt."""
        mock_response = Mock()
        mock_response.json.return_value = {"test": "data"}
        mock_request.return_value = mock_response
        
        response = self.client._request_metadata_with_retry("GET", "https://test.url")
        
        assert response == mock_response
        assert mock_request.call_count == 1

    @patch('time.sleep')
    @patch.object(ODataClient, '_request')
    def test_metadata_retry_404_then_success(self, mock_request, mock_sleep):
        """Test retry on 404 error then success."""
        # First call raises 404, second succeeds
        http_error = HttpError("Not found", status_code=404)
        mock_response = Mock()
        mock_request.side_effect = [http_error, mock_response]
        
        response = self.client._request_metadata_with_retry("GET", "https://test.url")
        
        assert response == mock_response
        assert mock_request.call_count == 2
        assert mock_sleep.call_count == 1
        mock_sleep.assert_called_with(0.4)  # First retry delay

    @patch('time.sleep')
    @patch.object(ODataClient, '_request')
    def test_metadata_retry_multiple_404s_then_success(self, mock_request, mock_sleep):
        """Test multiple 404 retries then success."""
        # Two 404s, then success
        http_error = HttpError("Not found", status_code=404)
        mock_response = Mock()
        mock_request.side_effect = [http_error, http_error, mock_response]
        
        response = self.client._request_metadata_with_retry("GET", "https://test.url")
        
        assert response == mock_response
        assert mock_request.call_count == 3
        assert mock_sleep.call_count == 2
        # Check exponential backoff: 0.4s, 0.8s
        assert mock_sleep.call_args_list[0][0][0] == 0.4
        assert mock_sleep.call_args_list[1][0][0] == 0.8

    @patch('time.sleep')
    @patch.object(ODataClient, '_request')
    def test_metadata_retry_exhausted_404s(self, mock_request, mock_sleep):
        """Test that 404 retries are exhausted and error is raised."""
        # All three attempts return 404
        http_error = HttpError("Not found", status_code=404)
        mock_request.side_effect = [http_error, http_error, http_error]
        
        with pytest.raises(HttpError) as exc_info:
            self.client._request_metadata_with_retry("GET", "https://test.url")
        
        assert exc_info.value.status_code == 404
        assert mock_request.call_count == 3
        assert mock_sleep.call_count == 2  # Two retries

    @patch.object(ODataClient, '_request')
    def test_metadata_retry_non_404_error_no_retry(self, mock_request):
        """Test that non-404 errors are not retried."""
        # 500 error should not be retried in metadata operations
        http_error = HttpError("Server error", status_code=500)
        mock_request.side_effect = [http_error]
        
        with pytest.raises(HttpError) as exc_info:
            self.client._request_metadata_with_retry("GET", "https://test.url")
        
        assert exc_info.value.status_code == 500
        assert mock_request.call_count == 1  # No retry for non-404

    @patch.object(ODataClient, '_request_metadata_with_retry')
    def test_optionset_map_uses_metadata_retry(self, mock_metadata_retry):
        """Test that _optionset_map calls the metadata retry method."""
        # First call should succeed, but we're testing that the method is called
        http_error = HttpError("Not found", status_code=404)
        mock_metadata_retry.side_effect = [http_error]
        
        # This should raise RuntimeError after retries, but proves retry method was called
        with pytest.raises(RuntimeError) as exc_info:
            self.client._optionset_map("test_entity", "test_attribute")
        
        assert "Picklist attribute metadata not found after retries" in str(exc_info.value)
        assert mock_metadata_retry.call_count == 1  # Should call our retry method

    @patch.object(ODataClient, '_request_metadata_with_retry')
    def test_optionset_map_handles_metadata_404_gracefully(self, mock_metadata_retry):
        """Test that _optionset_map handles metadata 404s gracefully."""
        # Simulate 404 after retries
        http_error = HttpError("Not found", status_code=404)
        mock_metadata_retry.side_effect = [http_error]
        
        with pytest.raises(RuntimeError) as exc_info:
            self.client._optionset_map("test_entity", "test_attribute")
        
        assert "Picklist attribute metadata not found after retries" in str(exc_info.value)
        assert mock_metadata_retry.call_count == 1