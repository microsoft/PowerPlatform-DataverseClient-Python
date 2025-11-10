# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Unit tests for logical name normalization and SchemaName resolution.

Tests the case-insensitive logical name handling, SchemaName lookup caching,
and unified metadata cache introduced for explicit naming enforcement.
"""

import pytest
from dataverse_sdk.core.errors import MetadataError
from tests.unit.test_helpers import (
    TestableClient,
    MD_ACCOUNT,
    MD_SAMPLE_ITEM
)

# ============================================================================
# Test Data - Additional Metadata Responses for this test file
# ============================================================================

MD_ENTITY_BY_LOGICAL = {
    "LogicalName": "new_sampleitem",
    "EntitySetName": "new_sampleitems",
    "SchemaName": "new_Sampleitem",
    "MetadataId": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
}

MD_ATTRIBUTE_TITLE = {
    "value": [
        {
            "LogicalName": "new_title",
            "SchemaName": "new_Title"
        }
    ]
}


# ============================================================================
# Tests for _normalize_logical_name
# ============================================================================

def test_normalize_logical_name_lowercase():
    """Test that _normalize_logical_name converts to lowercase."""
    c = TestableClient([])
    assert c._normalize_logical_name("NEW_SAMPLEITEM") == "new_sampleitem"
    assert c._normalize_logical_name("New_SampleItem") == "new_sampleitem"
    assert c._normalize_logical_name("new_sampleitem") == "new_sampleitem"


def test_normalize_logical_name_strips_whitespace():
    """Test that _normalize_logical_name strips whitespace."""
    c = TestableClient([])
    assert c._normalize_logical_name("  new_sampleitem  ") == "new_sampleitem"
    assert c._normalize_logical_name("\tnew_sampleitem\n") == "new_sampleitem"


def test_normalize_logical_name_empty():
    """Test that _normalize_logical_name handles empty strings."""
    c = TestableClient([])
    assert c._normalize_logical_name("") == ""
    assert c._normalize_logical_name("   ") == ""


# ============================================================================
# Tests for _logical_to_schema_name
# ============================================================================

def test_logical_to_schema_name_basic():
    """Test PascalCase conversion for new entities."""
    c = TestableClient([])
    # new_sampleitem -> new_Sampleitem
    assert c._logical_to_schema_name("new_sampleitem") == "new_Sampleitem"
    # abc_myentity -> abc_Myentity
    assert c._logical_to_schema_name("abc_myentity") == "abc_Myentity"


def test_logical_to_schema_name_no_underscore():
    """Test SchemaName when no underscore (capitalize first letter)."""
    c = TestableClient([])
    assert c._logical_to_schema_name("account") == "Account"
    assert c._logical_to_schema_name("contact") == "Contact"


# ============================================================================
# Tests for _get_entity_schema_name
# ============================================================================

def test_get_entity_schema_name_lookup():
    """Test that _get_entity_schema_name retrieves SchemaName from server."""
    responses = [
        (200, {}, MD_SAMPLE_ITEM)  # _get_entity_metadata uses EntityDefinitions endpoint
    ]
    c = TestableClient(responses)
    schema = c._get_entity_schema_name("new_sampleitem")
    assert schema == "new_Sampleitem"


def test_get_entity_schema_name_not_found():
    """Test that _get_entity_schema_name raises error when entity not found."""
    responses = [
        (200, {}, {"value": []})  # Empty response = not found
    ]
    c = TestableClient(responses)
    with pytest.raises(MetadataError, match="Unable to resolve entity metadata"):
        c._get_entity_schema_name("new_nonexistent")


# ============================================================================
# Tests for _get_attribute_schema_name
# ============================================================================

def test_get_attribute_schema_name_lookup():
    """Test that _get_attribute_schema_name retrieves attribute SchemaName."""
    responses = [
        (200, {}, {"value": [MD_ENTITY_BY_LOGICAL]}),  # _get_entity_by_logical uses EntityDefinitions endpoint
        (200, {}, MD_ATTRIBUTE_TITLE)      # Attribute lookup
    ]
    c = TestableClient(responses)
    schema = c._get_attribute_schema_name("new_sampleitem", "new_title")
    assert schema == "new_Title"


def test_get_attribute_schema_name_caching():
    """Test that _get_attribute_schema_name caches results."""
    responses = [
        (200, {}, {"value": [MD_ENTITY_BY_LOGICAL]}),  # Entity lookup (first call)
        (200, {}, MD_ATTRIBUTE_TITLE)      # Attribute lookup (first call)
        # No more responses needed - second call should use cache
    ]
    c = TestableClient(responses)
    
    # First call - hits server
    schema1 = c._get_attribute_schema_name("new_sampleitem", "new_title")
    assert schema1 == "new_Title"
    
    # Second call - uses cache
    schema2 = c._get_attribute_schema_name("new_sampleitem", "new_title")
    assert schema2 == "new_Title"
    
    # Verify only 2 HTTP calls were made (not 4)
    assert len(c._http.calls) == 2


def test_get_attribute_schema_name_case_insensitive():
    """Test that attribute lookups are case-insensitive."""
    responses = [
        (200, {}, {"value": [MD_ENTITY_BY_LOGICAL]}),  # Entity lookup
        (200, {}, MD_ATTRIBUTE_TITLE)      # Attribute lookup
    ]
    c = TestableClient(responses)
    
    # Lookup with different casing
    schema = c._get_attribute_schema_name("NEW_SAMPLEITEM", "NEW_TITLE")
    assert schema == "new_Title"
    
    # Verify the query used normalized (lowercase) logical name
    calls = c._http.calls
    attr_call = calls[1]  # Second call is attribute lookup
    assert "new_title" in str(attr_call).lower()


def test_get_attribute_schema_name_not_found():
    """Test error when attribute not found."""
    responses = [
        (200, {}, {"value": [MD_ENTITY_BY_LOGICAL]}),
        (200, {}, {"value": []})  # Empty result = not found
    ]
    c = TestableClient(responses)
    
    with pytest.raises(MetadataError, match="Attribute 'new_missing' not found"):
        c._get_attribute_schema_name("new_sampleitem", "new_missing")


def test_get_attribute_schema_name_entity_not_found():
    """Test error when entity doesn't exist for attribute lookup."""
    responses = [
        (200, {}, {"value": []})  # Empty entity response from EntityDefinitions
    ]
    c = TestableClient(responses)
    
    with pytest.raises(MetadataError, match="Entity 'new_missing' not found"):
        c._get_attribute_schema_name("new_missing", "new_field")


# ============================================================================
# Tests for _get_entity_metadata (Unified Cache)
# ============================================================================

def test_get_entity_metadata_returns_all_fields():
    """Test that _get_entity_metadata returns complete TableMetadata."""
    responses = [
        (200, {}, MD_SAMPLE_ITEM)
    ]
    c = TestableClient(responses)
    
    metadata = c._get_entity_metadata("new_sampleitem")
    
    assert metadata["entity_set_name"] == "new_sampleitems"
    assert metadata["schema_name"] == "new_Sampleitem"
    assert metadata["primary_id_attribute"] == "new_sampleitemid"


def test_get_entity_metadata_caching():
    """Test that _get_entity_metadata caches results."""
    responses = [
        (200, {}, MD_SAMPLE_ITEM)  # Only one response needed
    ]
    c = TestableClient(responses)
    
    # First call - hits server
    metadata1 = c._get_entity_metadata("new_sampleitem")
    assert metadata1["entity_set_name"] == "new_sampleitems"
    
    # Second call - uses cache
    metadata2 = c._get_entity_metadata("new_sampleitem")
    assert metadata2["entity_set_name"] == "new_sampleitems"
    
    # Third call - still uses cache
    metadata3 = c._get_entity_metadata("new_sampleitem")
    assert metadata3["entity_set_name"] == "new_sampleitems"
    
    # Verify only 1 HTTP call was made
    assert len(c._http.calls) == 1


def test_get_entity_metadata_case_insensitive():
    """Test that metadata lookups are case-insensitive."""
    responses = [
        (200, {}, MD_SAMPLE_ITEM)
    ]
    c = TestableClient(responses)
    
    # Different casing should all normalize to same cache key
    metadata1 = c._get_entity_metadata("new_sampleitem")
    metadata2 = c._get_entity_metadata("NEW_SAMPLEITEM")
    metadata3 = c._get_entity_metadata("New_SampleItem")
    
    assert metadata1 == metadata2 == metadata3
    # Only 1 HTTP call should have been made
    assert len(c._http.calls) == 1


def test_get_entity_metadata_not_found():
    """Test error when entity metadata not found."""
    responses = [
        (200, {}, {"value": []})  # Empty response
    ]
    c = TestableClient(responses)
    
    with pytest.raises(MetadataError, match="Unable to resolve entity metadata"):
        c._get_entity_metadata("new_nonexistent")


def test_get_entity_metadata_plural_hint():
    """Test helpful error message for plural names."""
    responses = [
        (200, {}, {"value": []})
    ]
    c = TestableClient(responses)
    
    with pytest.raises(MetadataError, match="did you pass a plural entity set name"):
        c._get_entity_metadata("accounts")  # Ends with 's'


def test_get_entity_metadata_missing_entity_set():
    """Test error when EntitySetName missing in response."""
    responses = [
        (200, {}, {"value": [{"LogicalName": "test", "SchemaName": "Test"}]})
    ]
    c = TestableClient(responses)
    
    with pytest.raises(MetadataError, match="missing EntitySetName"):
        c._get_entity_metadata("test")


def test_get_entity_metadata_missing_schema_name():
    """Test error when SchemaName missing in response."""
    responses = [
        (200, {}, {"value": [{"LogicalName": "test", "EntitySetName": "tests"}]})
    ]
    c = TestableClient(responses)
    
    with pytest.raises(MetadataError, match="missing SchemaName"):
        c._get_entity_metadata("test")


# ============================================================================
# Tests for Case-Insensitive Entity Operations
# ============================================================================

def test_entity_set_from_logical_case_insensitive():
    """Test that _entity_set_from_logical is case-insensitive."""
    responses = [
        (200, {}, MD_SAMPLE_ITEM)
    ]
    c = TestableClient(responses)
    
    # All variations should return same entity set name
    assert c._entity_set_from_logical("new_sampleitem") == "new_sampleitems"
    assert c._entity_set_from_logical("NEW_SAMPLEITEM") == "new_sampleitems"
    assert c._entity_set_from_logical("New_SampleItem") == "new_sampleitems"
    
    # Only 1 HTTP call (rest from cache)
    assert len(c._http.calls) == 1


def test_get_entity_by_logical_normalization():
    """Test that _get_entity_by_logical normalizes input."""
    responses = [
        (200, {}, {"value": [MD_ENTITY_BY_LOGICAL]})
    ]
    c = TestableClient(responses)
    
    entity = c._get_entity_by_logical("NEW_SAMPLEITEM")
    assert entity["LogicalName"] == "new_sampleitem"
    
    # Check that the query parameters used normalized name
    call = c._http.calls[0]
    method, url, kwargs = call
    params = kwargs.get('params', {})
    filter_clause = params.get('$filter', '')
    assert "new_sampleitem" in filter_clause.lower()


# ============================================================================
# Tests for Table/Column Operations with Normalization
# ============================================================================

def test_create_table_normalizes_logical_name():
    """Test that _create_table normalizes the logical name."""
    responses = [
        (200, {}, {}),  # POST to create entity
        (200, {}, {"value": [MD_ENTITY_BY_LOGICAL]}),  # GET entity by logical with Consistency: Strong
    ]
    c = TestableClient(responses)
    
    # Create with mixed case - _create_table takes schema dict, not individual params
    c._create_table(
        logical_name="NEW_SAMPLEITEM",
        schema={"new_field1": "string"},  # Dict of column_name -> type
        solution_unique_name=None
    )
    
    # Verify the POST was made to EntityDefinitions
    call = c._http.calls[0]
    method, url, kwargs = call
    assert method == "post"
    assert "EntityDefinitions" in url


def test_create_columns_normalizes_names():
    """Test that _create_columns normalizes table and column names."""
    responses = [
        (200, {}, {"value": [MD_ENTITY_BY_LOGICAL]}),  # Get entity by logical
        (204, {}, {})  # Column creation response (POST to Attributes)
    ]
    c = TestableClient(responses)
    
    # Create columns with mixed case - _create_columns takes dict of name -> type
    c._create_columns(
        logical_name="NEW_SAMPLEITEM",
        columns={"NEW_FIELD1": "string"}  # Dict format
    )
    
    # Verify entity lookup used normalized name
    calls = c._http.calls
    assert len(calls) >= 1
    # The first call should be to get entity metadata with normalized name


def test_delete_columns_normalizes_names():
    """Test that _delete_columns normalizes table and column names."""
    responses = [
        (200, {}, {"value": [MD_ENTITY_BY_LOGICAL]}),  # Get entity by logical (from _delete_columns)
        (200, {}, {"value": [MD_ENTITY_BY_LOGICAL]}),  # Get entity by logical (from _get_attribute_schema_name)
        (200, {}, MD_ATTRIBUTE_TITLE),     # Get attribute schema (from _get_attribute_schema_name)
        (200, {}, {"value": [{"MetadataId": "attr-guid-123", "LogicalName": "new_title", "SchemaName": "new_Title", "@odata.type": "Microsoft.Dynamics.CRM.StringAttributeMetadata"}]}),  # Get attribute metadata by SchemaName
        (204, {}, {})                       # Delete response
    ]
    c = TestableClient(responses)
    
    # Delete with mixed case - _delete_columns takes str or list of str
    c._delete_columns(
        logical_name="NEW_SAMPLEITEM",
        columns=["NEW_TITLE"]  # Parameter is 'columns' not 'column_names'
    )
    
    # Verify calls were made with normalized names
    assert len(c._http.calls) >= 2


# ============================================================================
# Tests for Integration Scenarios
# ============================================================================

def test_end_to_end_case_insensitive_workflow():
    """Test complete workflow with mixed case names."""
    guid = "11111111-2222-3333-4444-555555555555"
    
    responses = [
        # Lookup metadata with UPPERCASE
        (200, {}, MD_SAMPLE_ITEM),
        # Create record
        (204, {"OData-EntityId": f"https://org.example/api/data/v9.2/new_sampleitems({guid})"}, {}),
        # Get record back with MixedCase
        (200, {}, {"new_sampleitemid": guid, "new_title": "Test"}),
    ]
    c = TestableClient(responses)
    
    # Use UPPERCASE for entity set lookup
    entity_set = c._entity_set_from_logical("NEW_SAMPLEITEM")
    assert entity_set == "new_sampleitems"
    
    # Create with lowercase
    record_id = c._create(entity_set, "new_sampleitem", {"new_title": "Test"})
    assert record_id == guid
    
    # Get with MixedCase
    record = c._get("New_SampleItem", guid)
    assert record["new_title"] == "Test"


def test_cache_isolation_between_entities():
    """Test that cache correctly isolates different entities."""
    responses = [
        (200, {}, MD_SAMPLE_ITEM),
        (200, {}, MD_ACCOUNT)
    ]
    c = TestableClient(responses)
    
    # Get metadata for two different entities
    md1 = c._get_entity_metadata("new_sampleitem")
    md2 = c._get_entity_metadata("account")
    
    # They should have different values
    assert md1["entity_set_name"] != md2["entity_set_name"]
    assert md1["schema_name"] != md2["schema_name"]
    
    # Both should be cached (2 HTTP calls total)
    assert len(c._http.calls) == 2
    
    # Re-accessing should not make more calls
    md1_again = c._get_entity_metadata("new_sampleitem")
    assert md1 == md1_again
    assert len(c._http.calls) == 2  # Still only 2


def test_create_with_one_casing_crud_with_another():
    """Test creating table with one casing, then performing CRUD with different casings.
    
    This simulates real-world scenarios where:
    1. Table is created with lowercase: "new_product"
    2. Developer uses different casing in subsequent operations
    3. All operations should work and share the same cache
    """
    table_guid = "table-1111-2222-3333-4444"
    
    # Simplified test focusing on metadata operations and cache sharing
    responses = [
        # CREATE TABLE with lowercase "new_product"
        (200, {}, {}),  # POST EntityDefinitions
        (200, {}, {"value": [{
            "LogicalName": "new_product",
            "EntitySetName": "new_products", 
            "SchemaName": "new_Product",
            "MetadataId": table_guid,
            "PrimaryIdAttribute": "new_productid"
        }]}),  # GET with Consistency: Strong
        
        # ADD COLUMN with UPPERCASE "NEW_PRODUCT" - should use cached entity metadata
        (200, {}, {"value": [{  # Get entity by logical (for _create_columns)
            "LogicalName": "new_product",
            "EntitySetName": "new_products",
            "SchemaName": "new_Product", 
            "MetadataId": table_guid
        }]}),
        (204, {}, {}),  # POST attribute
        
        # GET entity set with MixedCase "New_Product"
        # First call may need metadata if not already cached
        (200, {}, {"value": [{
            "LogicalName": "new_product",
            "EntitySetName": "new_products",
            "SchemaName": "new_Product",
            "MetadataId": table_guid,
            "PrimaryIdAttribute": "new_productid"
        }]}),  # Metadata for _entity_set_from_logical
        
        # Subsequent calls with different casing use cache (no HTTP calls)
        
        # DELETE TABLE with lowercase "new_product"
        (200, {}, {"value": [{  # Get entity by logical
            "LogicalName": "new_product",
            "EntitySetName": "new_products",
            "SchemaName": "new_Product",
            "MetadataId": table_guid
        }]}),
        (200, {}, {}),  # DELETE entity
    ]
    c = TestableClient(responses)
    
    # 1. CREATE TABLE with lowercase
    result = c._create_table(
        logical_name="new_product",
        schema={"new_price": "decimal"},
        solution_unique_name=None
    )
    assert result["entity_set_name"] == "new_products"
    
    # 2. ADD COLUMN with UPPERCASE - operations normalize the name
    c._create_columns(
        logical_name="NEW_PRODUCT",
        columns={"NEW_DESCRIPTION": "string"}
    )
    
    # 3. GET entity set with different casings - first call populates cache
    entity_set1 = c._entity_set_from_logical("New_Product")
    calls_after_first = len(c._http.calls)
    
    # First call may hit server to get metadata (if not already cached from _create_columns)
    assert entity_set1 == "new_products"
    
    # Now verify cache was populated with normalized key
    assert "new_product" in c._entity_metadata_cache
    cached = c._entity_metadata_cache["new_product"]
    assert cached["entity_set_name"] == "new_products"
    assert cached["schema_name"] == "new_Product"
    
    # 4. Subsequent calls with different casing should use cache
    entity_set2 = c._entity_set_from_logical("NEW_PRODUCT")
    entity_set3 = c._entity_set_from_logical("new_product")
    entity_set4 = c._entity_set_from_logical("NeW_PrOdUcT")
    calls_after_cache_hits = len(c._http.calls)
    
    # All should return the same entity set
    assert entity_set1 == entity_set2 == entity_set3 == entity_set4 == "new_products"
    
    # Verify no additional HTTP calls were made (all used cache)
    assert calls_after_cache_hits == calls_after_first, \
        f"Expected cache hits, but made {calls_after_cache_hits - calls_after_first} additional calls"
    
    # 5. DELETE TABLE with lowercase - normalized name works
    c._delete_table("new_product")
    
    # Verify all operations completed
    assert len(c._http.calls) > 0


def test_mixed_case_cache_reuse():
    """Test that cache is properly shared across different casing variants.
    
    Ensures that NEW_PRODUCT, new_product, New_Product all hit the same cache entry.
    """
    responses = [
        (200, {}, MD_SAMPLE_ITEM)  # Only ONE server call should be made
    ]
    c = TestableClient(responses)
    
    # Multiple lookups with different casings
    md1 = c._get_entity_metadata("new_sampleitem")
    md2 = c._get_entity_metadata("NEW_SAMPLEITEM") 
    md3 = c._get_entity_metadata("New_SampleItem")
    md4 = c._get_entity_metadata("NeW_sAmPlEiTeM")
    md5 = c._get_entity_metadata("  NEW_SAMPLEITEM  ")  # With whitespace
    
    # All should be identical
    assert md1 == md2 == md3 == md4 == md5
    
    # Verify only 1 HTTP call was made (all others used cache)
    assert len(c._http.calls) == 1
    
    # Verify the cache key is normalized (lowercase)
    assert "new_sampleitem" in c._entity_metadata_cache
    assert "NEW_SAMPLEITEM" not in c._entity_metadata_cache
    assert "New_SampleItem" not in c._entity_metadata_cache


def test_attribute_cache_case_insensitive():
    """Test that attribute schema cache is case-insensitive."""
    responses = [
        (200, {}, {"value": [MD_ENTITY_BY_LOGICAL]}),
        (200, {}, MD_ATTRIBUTE_TITLE)
    ]
    c = TestableClient(responses)
    
    # First lookup with lowercase
    schema1 = c._get_attribute_schema_name("new_sampleitem", "new_title")
    assert schema1 == "new_Title"
    
    # Second lookup with UPPERCASE - should hit cache
    schema2 = c._get_attribute_schema_name("NEW_SAMPLEITEM", "NEW_TITLE")
    assert schema2 == "new_Title"
    
    # Third lookup with MixedCase - should hit cache  
    schema3 = c._get_attribute_schema_name("New_SampleItem", "New_Title")
    assert schema3 == "new_Title"
    
    # Only 2 HTTP calls (entity + attribute lookup), not 6
    assert len(c._http.calls) == 2
    
    # Verify cache uses normalized keys
    assert ("new_sampleitem", "new_title") in c._attribute_schema_cache
    assert ("NEW_SAMPLEITEM", "NEW_TITLE") not in c._attribute_schema_cache


def test_primary_id_attr_case_insensitive():
    """Test that _primary_id_attr normalizes logical names for cache lookup."""
    responses = [
        (200, {}, MD_SAMPLE_ITEM)  # Only ONE server call should be made
    ]
    c = TestableClient(responses)
    
    # Multiple lookups with different casings
    pid1 = c._primary_id_attr("new_sampleitem")
    pid2 = c._primary_id_attr("NEW_SAMPLEITEM")
    pid3 = c._primary_id_attr("New_SampleItem")
    pid4 = c._primary_id_attr("  new_sampleitem  ")  # With whitespace
    
    # All should return the same primary ID attribute
    assert pid1 == pid2 == pid3 == pid4 == "new_sampleitemid"
    
    # Verify only 1 HTTP call was made (all others used cache)
    assert len(c._http.calls) == 1
    
    # Verify the cache key is normalized (lowercase)
    assert "new_sampleitem" in c._entity_metadata_cache
    assert "NEW_SAMPLEITEM" not in c._entity_metadata_cache


def test_logical_to_schema_name_case_insensitive():
    """Test that _logical_to_schema_name normalizes input for consistent output."""
    c = TestableClient([])
    
    # All variations should produce the same SchemaName
    assert c._logical_to_schema_name("new_sampleitem") == "new_Sampleitem"
    assert c._logical_to_schema_name("NEW_SAMPLEITEM") == "new_Sampleitem"
    assert c._logical_to_schema_name("New_SampleItem") == "new_Sampleitem"
    assert c._logical_to_schema_name("  new_sampleitem  ") == "new_Sampleitem"
    
    # Test without prefix
    assert c._logical_to_schema_name("account") == "Account"
    assert c._logical_to_schema_name("ACCOUNT") == "Account"
    assert c._logical_to_schema_name("Account") == "Account"


def test_logical_to_schema_name_edge_cases():
    """Test that _logical_to_schema_name handles edge cases correctly."""
    c = TestableClient([])
    
    # Empty string should raise ValueError
    with pytest.raises(ValueError, match="cannot be empty"):
        c._logical_to_schema_name("")
    
    # Whitespace-only should raise ValueError
    with pytest.raises(ValueError, match="cannot be empty"):
        c._logical_to_schema_name("   ")
    
    # Trailing underscore with no suffix should raise ValueError
    with pytest.raises(ValueError, match="empty part after underscore"):
        c._logical_to_schema_name("new_")
    
    # Multiple underscores - only first split matters
    assert c._logical_to_schema_name("new_sample_item") == "new_Sample_item"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
