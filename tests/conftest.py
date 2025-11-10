# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Shared pytest fixtures and configuration for Dataverse SDK tests.

This module provides common test fixtures, mock objects, and configuration
that can be used across all test modules.
"""

import pytest
from unittest.mock import Mock
from dataverse_sdk.core.config import DataverseConfig


@pytest.fixture
def dummy_auth():
    """Mock authentication object for testing."""
    class DummyAuth:
        def acquire_token(self, scope):
            class Token:
                access_token = "test_token_12345"
            return Token()
    return DummyAuth()


@pytest.fixture  
def test_config():
    """Test configuration with safe defaults."""
    return DataverseConfig(
        language_code=1033,
        http_retries=0,
        http_backoff=0.1,
        http_timeout=5
    )


@pytest.fixture
def mock_http_client():
    """Mock HTTP client for unit tests."""
    mock = Mock()
    mock.request.return_value = Mock()
    return mock


@pytest.fixture
def sample_base_url():
    """Standard test base URL."""
    return "https://org.example.com"


@pytest.fixture
def sample_entity_data():
    """Sample entity data for testing."""
    return {
        "name": "Test Account",
        "telephone1": "555-0100", 
        "websiteurl": "https://example.com"
    }


@pytest.fixture
def sample_guid():
    """Sample GUID for testing."""
    return "11111111-2222-3333-4444-555555555555"