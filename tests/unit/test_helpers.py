# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Shared test utilities for unit tests.

Provides mock objects for authentication, HTTP clients, and common test data
to reduce duplication across test files.
"""

import types
from dataverse_sdk.data.odata import ODataClient


class DummyAuth:
    """Mock authentication provider for testing.
    
    Returns a simple token object with an access_token attribute.
    """
    def acquire_token(self, scope):
        class Token:
            access_token = "test_token"
        return Token()


class DummyHTTPClient:
    """Mock HTTP client that returns pre-configured responses.
    
    Args:
        responses: List of (status_code, headers, body) tuples to return in sequence.
        
    Attributes:
        calls: List of (method, url, kwargs) tuples recording all requests made.
    """
    def __init__(self, responses):
        self._responses = list(responses)  # Make a copy
        self.calls = []
    
    def request(self, method, url, **kwargs):
        """Mock HTTP request that returns the next pre-configured response."""
        self.calls.append((method, url, kwargs))
        if not self._responses:
            raise AssertionError("No more dummy responses configured")
        
        status, headers, body = self._responses.pop(0)
        resp = types.SimpleNamespace()
        resp.status_code = status
        resp.headers = headers
        resp.text = "" if body is None else ("{}" if isinstance(body, dict) else str(body))
        
        def raise_for_status():
            if status >= 400:
                raise RuntimeError(f"HTTP {status}")
            return None
        
        def json_func():
            return body if isinstance(body, dict) else {}
        
        resp.raise_for_status = raise_for_status
        resp.json = json_func
        return resp


class TestableClient(ODataClient):
    """ODataClient with mocked HTTP for testing.
    
    Args:
        responses: List of (status_code, headers, body) tuples for the mock HTTP client.
        org_url: Organization URL (default: "https://org.example").
        config: Optional config object (default: None).
    """
    def __init__(self, responses, org_url="https://org.example", config=None):
        super().__init__(DummyAuth(), org_url, config)
        self._http = DummyHTTPClient(responses)
    
    def _convert_labels_to_ints(self, logical_name, record):
        """Test shim - no-op conversion for simplicity."""
        return record


class DummyConfig:
    """Minimal config stub for tests that need config attributes.
    
    Args:
        language_code: Language code for localized labels (default: 1033).
    """
    def __init__(self, language_code=1033):
        self.language_code = language_code
        # HTTP settings referenced during ODataClient construction
        self.http_retries = 0
        self.http_backoff = 0
        self.http_timeout = 5


# ============================================================================
# Common Test Data - Metadata Responses
# ============================================================================

def make_entity_metadata(logical_name, entity_set_name, schema_name, primary_id_attr):
    """Create a standard EntityDefinitions metadata response.
    
    Args:
        logical_name: Logical name of the entity (e.g., "account").
        entity_set_name: Entity set name for the collection (e.g., "accounts").
        schema_name: Schema name (e.g., "Account").
        primary_id_attr: Primary ID attribute name (e.g., "accountid").
        
    Returns:
        Dict representing an EntityDefinitions query response with one entity.
    """
    return {
        "value": [
            {
                "LogicalName": logical_name,
                "EntitySetName": entity_set_name,
                "SchemaName": schema_name,
                "PrimaryIdAttribute": primary_id_attr
            }
        ]
    }


def make_entity_create_headers(entity_set_name, guid):
    """Create standard headers returned by entity create operations.
    
    Args:
        entity_set_name: Entity set name (e.g., "accounts").
        guid: GUID string for the created entity.
        
    Returns:
        Dict with OData-EntityId header.
    """
    return {
        "OData-EntityId": f"https://org.example/api/data/v9.2/{entity_set_name}({guid})"
    }


def make_attribute_metadata(logical_name, schema_name, metadata_id, odata_type=None, attribute_type=None):
    """Create a standard Attribute metadata response.
    
    Args:
        logical_name: Logical name of the attribute (e.g., "new_category").
        schema_name: Schema name of the attribute (e.g., "new_Category").
        metadata_id: GUID string for the attribute metadata.
        odata_type: Optional @odata.type value (e.g., "Microsoft.Dynamics.CRM.PicklistAttributeMetadata").
        attribute_type: Optional AttributeType value (e.g., "Picklist").
        
    Returns:
        Dict representing an Attribute metadata response.
    """
    result = {
        "LogicalName": logical_name,
        "SchemaName": schema_name,
        "MetadataId": metadata_id
    }
    if odata_type:
        result["@odata.type"] = odata_type
    if attribute_type:
        result["AttributeType"] = attribute_type
    return result


# Common metadata responses for frequently used entities
MD_ACCOUNT = make_entity_metadata("account", "accounts", "Account", "accountid")
MD_CONTACT = make_entity_metadata("contact", "contacts", "Contact", "contactid")
MD_SAMPLE_ITEM = make_entity_metadata("new_sampleitem", "new_sampleitems", "new_Sampleitem", "new_sampleitemid")
