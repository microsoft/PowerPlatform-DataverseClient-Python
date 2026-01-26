# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Unit tests for result types in PowerPlatform.Dataverse.core.results.

Tests cover:
- RequestMetadata dataclass behavior
- DataverseResponse dataclass behavior
- FluentResult wrapper with .with_detail_response() pattern
- Magic method behaviors for FluentResult (iteration, indexing, equality, etc.)
"""

import pytest
from PowerPlatform.Dataverse.core.results import (
    RequestMetadata,
    DataverseResponse,
    FluentResult,
    # Legacy types
    OperationResult,
    CreateResult,
    UpdateResult,
    DeleteResult,
    GetResult,
    PagedResult,
)


class TestRequestMetadata:
    """Tests for RequestMetadata dataclass."""

    def test_default_values(self):
        """Test that RequestMetadata has correct default values."""
        metadata = RequestMetadata()
        assert metadata.client_request_id is None
        assert metadata.correlation_id is None
        assert metadata.service_request_id is None
        assert metadata.http_status_code is None
        assert metadata.timing_ms is None

    def test_with_all_values(self):
        """Test RequestMetadata with all values provided."""
        metadata = RequestMetadata(
            client_request_id="client-123",
            correlation_id="corr-456",
            service_request_id="service-789",
            http_status_code=201,
            timing_ms=150.5
        )
        assert metadata.client_request_id == "client-123"
        assert metadata.correlation_id == "corr-456"
        assert metadata.service_request_id == "service-789"
        assert metadata.http_status_code == 201
        assert metadata.timing_ms == 150.5

    def test_is_frozen(self):
        """Test that RequestMetadata is immutable (frozen)."""
        metadata = RequestMetadata(client_request_id="test")
        with pytest.raises(AttributeError):
            metadata.client_request_id = "new-value"  # type: ignore

    def test_equality(self):
        """Test RequestMetadata equality comparison."""
        m1 = RequestMetadata(client_request_id="test", http_status_code=200)
        m2 = RequestMetadata(client_request_id="test", http_status_code=200)
        m3 = RequestMetadata(client_request_id="other", http_status_code=200)
        assert m1 == m2
        assert m1 != m3


class TestDataverseResponse:
    """Tests for DataverseResponse dataclass."""

    def test_with_string_result(self):
        """Test DataverseResponse with a string result (single create)."""
        response = DataverseResponse(
            result="guid-123",
            telemetry={"timing_ms": 100}
        )
        assert response.result == "guid-123"
        assert response.telemetry["timing_ms"] == 100

    def test_with_list_result(self):
        """Test DataverseResponse with a list result (bulk create)."""
        response = DataverseResponse(
            result=["guid-1", "guid-2", "guid-3"],
            telemetry={"batch_info": {"total": 3, "success": 3}}
        )
        assert response.result == ["guid-1", "guid-2", "guid-3"]
        assert len(response.result) == 3
        assert response.telemetry["batch_info"]["total"] == 3

    def test_with_none_result(self):
        """Test DataverseResponse with None result (update/delete)."""
        response = DataverseResponse(
            result=None,
            telemetry={"http_status_code": 204}
        )
        assert response.result is None
        assert response.telemetry["http_status_code"] == 204

    def test_default_telemetry(self):
        """Test DataverseResponse default telemetry is empty dict."""
        response = DataverseResponse(result="test")
        assert response.telemetry == {}

    def test_telemetry_structure(self):
        """Test typical telemetry structure."""
        telemetry = {
            "client_request_id": "client-123",
            "correlation_id": "corr-456",
            "service_request_id": "service-789",
            "http_status_code": 200,
            "timing_ms": 150.5,
            "batch_info": {"total": 2, "success": 2, "failures": 0}
        }
        response = DataverseResponse(result=["id1", "id2"], telemetry=telemetry)
        assert response.telemetry["timing_ms"] == 150.5
        assert response.telemetry["batch_info"]["success"] == 2


class TestFluentResult:
    """Tests for FluentResult wrapper class."""

    def test_value_property(self):
        """Test the value property returns the result directly."""
        metadata = RequestMetadata()
        result = FluentResult("guid-123", metadata)
        assert result.value == "guid-123"

    def test_with_detail_response_single(self):
        """Test with_detail_response() for single record result."""
        metadata = RequestMetadata(
            client_request_id="client-123",
            correlation_id="corr-456",
            http_status_code=201,
            timing_ms=100.5
        )
        result = FluentResult("guid-123", metadata)
        response = result.with_detail_response()

        assert isinstance(response, DataverseResponse)
        assert response.result == "guid-123"
        assert response.telemetry["client_request_id"] == "client-123"
        assert response.telemetry["correlation_id"] == "corr-456"
        assert response.telemetry["http_status_code"] == 201
        assert response.telemetry["timing_ms"] == 100.5
        assert "batch_info" not in response.telemetry

    def test_with_detail_response_bulk(self):
        """Test with_detail_response() for bulk result with batch_info."""
        metadata = RequestMetadata(
            client_request_id="client-123",
            http_status_code=200,
            timing_ms=250.0
        )
        batch_info = {"total": 3, "success": 3, "failures": 0}
        result = FluentResult(["id1", "id2", "id3"], metadata, batch_info=batch_info)
        response = result.with_detail_response()

        assert response.result == ["id1", "id2", "id3"]
        assert response.telemetry["batch_info"] == batch_info
        assert response.telemetry["batch_info"]["total"] == 3

    def test_iteration_list(self):
        """Test iteration over a list result."""
        metadata = RequestMetadata()
        result = FluentResult(["a", "b", "c"], metadata)
        items = list(result)
        assert items == ["a", "b", "c"]

    def test_iteration_single(self):
        """Test iteration over a single value result."""
        metadata = RequestMetadata()
        result = FluentResult("single", metadata)
        items = list(result)
        assert items == ["single"]

    def test_getitem_list(self):
        """Test indexing into a list result."""
        metadata = RequestMetadata()
        result = FluentResult(["a", "b", "c"], metadata)
        assert result[0] == "a"
        assert result[1] == "b"
        assert result[-1] == "c"

    def test_getitem_dict(self):
        """Test key access for dict result."""
        metadata = RequestMetadata()
        result = FluentResult({"name": "Contoso", "id": "123"}, metadata)
        assert result["name"] == "Contoso"
        assert result["id"] == "123"

    def test_len_list(self):
        """Test len() for list result."""
        metadata = RequestMetadata()
        result = FluentResult(["a", "b", "c"], metadata)
        assert len(result) == 3

    def test_len_dict(self):
        """Test len() for dict result."""
        metadata = RequestMetadata()
        result = FluentResult({"a": 1, "b": 2}, metadata)
        assert len(result) == 2

    def test_len_single(self):
        """Test len() for single value result."""
        metadata = RequestMetadata()
        result = FluentResult("single", metadata)
        assert len(result) == 1

    def test_str(self):
        """Test string conversion."""
        metadata = RequestMetadata()
        result = FluentResult("test-value", metadata)
        assert str(result) == "test-value"

        result_list = FluentResult(["a", "b"], metadata)
        assert str(result_list) == "['a', 'b']"

    def test_repr(self):
        """Test repr for debugging."""
        metadata = RequestMetadata()
        result = FluentResult("test", metadata)
        assert repr(result) == "FluentResult('test')"

    def test_equality_fluent_result(self):
        """Test equality between FluentResult instances."""
        metadata = RequestMetadata()
        result1 = FluentResult("same", metadata)
        result2 = FluentResult("same", metadata)
        result3 = FluentResult("different", metadata)
        assert result1 == result2
        assert result1 != result3

    def test_equality_raw_value(self):
        """Test equality between FluentResult and raw value."""
        metadata = RequestMetadata()
        result = FluentResult("test", metadata)
        assert result == "test"
        assert result != "other"

        result_list = FluentResult([1, 2, 3], metadata)
        assert result_list == [1, 2, 3]
        assert result_list != [1, 2]

    def test_bool_truthy(self):
        """Test bool conversion for truthy results."""
        metadata = RequestMetadata()
        assert bool(FluentResult("non-empty", metadata)) is True
        assert bool(FluentResult([1, 2], metadata)) is True
        assert bool(FluentResult({"key": "value"}, metadata)) is True

    def test_bool_falsy(self):
        """Test bool conversion for falsy results."""
        metadata = RequestMetadata()
        assert bool(FluentResult("", metadata)) is False
        assert bool(FluentResult([], metadata)) is False
        assert bool(FluentResult({}, metadata)) is False
        assert bool(FluentResult(None, metadata)) is False

    def test_contains_list(self):
        """Test 'in' operator for list result."""
        metadata = RequestMetadata()
        result = FluentResult(["a", "b", "c"], metadata)
        assert "a" in result
        assert "b" in result
        assert "x" not in result

    def test_contains_dict(self):
        """Test 'in' operator for dict result (checks keys)."""
        metadata = RequestMetadata()
        result = FluentResult({"name": "Contoso", "id": "123"}, metadata)
        assert "name" in result
        assert "id" in result
        assert "unknown" not in result

    def test_contains_string(self):
        """Test 'in' operator for string result."""
        metadata = RequestMetadata()
        result = FluentResult("hello world", metadata)
        assert "hello" in result
        assert "world" in result
        assert "xyz" not in result

    def test_hash_string(self):
        """Test hash for hashable string result."""
        metadata = RequestMetadata()
        result = FluentResult("hashable", metadata)
        # Should not raise
        h = hash(result)
        assert isinstance(h, int)

    def test_hash_unhashable_list(self):
        """Test hash raises for unhashable list result."""
        metadata = RequestMetadata()
        result = FluentResult(["not", "hashable"], metadata)
        with pytest.raises(TypeError, match="unhashable type"):
            hash(result)

    def test_hash_unhashable_dict(self):
        """Test hash raises for unhashable dict result."""
        metadata = RequestMetadata()
        result = FluentResult({"not": "hashable"}, metadata)
        with pytest.raises(TypeError, match="unhashable type"):
            hash(result)


class TestLegacyResultTypes:
    """Tests for legacy result types to ensure backward compatibility."""

    def test_operation_result(self):
        """Test OperationResult base class."""
        result = OperationResult(
            client_request_id="client-123",
            correlation_id="corr-456",
            service_request_id="service-789"
        )
        assert result.client_request_id == "client-123"
        assert result.correlation_id == "corr-456"
        assert result.service_request_id == "service-789"

    def test_create_result(self):
        """Test CreateResult with IDs."""
        result = CreateResult(
            client_request_id="client-123",
            ids=["guid-1", "guid-2"]
        )
        assert result.ids == ["guid-1", "guid-2"]
        assert result.client_request_id == "client-123"

    def test_update_result(self):
        """Test UpdateResult (no additional fields)."""
        result = UpdateResult(client_request_id="client-123")
        assert result.client_request_id == "client-123"

    def test_delete_result(self):
        """Test DeleteResult with bulk job ID."""
        result = DeleteResult(
            client_request_id="client-123",
            bulk_job_id="job-456"
        )
        assert result.bulk_job_id == "job-456"

    def test_get_result(self):
        """Test GetResult with record data."""
        record_data = {"accountid": "guid-123", "name": "Contoso"}
        result = GetResult(
            client_request_id="client-123",
            record=record_data
        )
        assert result.record == record_data
        assert result.record["name"] == "Contoso"

    def test_paged_result(self):
        """Test PagedResult for pagination."""
        records = [
            {"id": "1", "name": "A"},
            {"id": "2", "name": "B"}
        ]
        result = PagedResult(
            client_request_id="client-123",
            records=records,
            page_number=1,
            has_more=True
        )
        assert result.records == records
        assert result.page_number == 1
        assert result.has_more is True

    def test_legacy_types_frozen(self):
        """Test that all legacy types are frozen (immutable)."""
        result = CreateResult(ids=["guid-1"])
        with pytest.raises(AttributeError):
            result.ids = ["new"]  # type: ignore


class TestFluentResultBackwardCompatibility:
    """Tests demonstrating FluentResult backward compatibility with existing code patterns."""

    def test_list_unpacking(self):
        """Test that FluentResult supports list unpacking."""
        metadata = RequestMetadata()
        result = FluentResult(["a", "b", "c"], metadata)
        first, second, third = result
        assert first == "a"
        assert second == "b"
        assert third == "c"

    def test_loop_iteration(self):
        """Test that FluentResult works in for loops."""
        metadata = RequestMetadata()
        result = FluentResult(["id1", "id2"], metadata)
        collected = []
        for item in result:
            collected.append(item)
        assert collected == ["id1", "id2"]

    def test_list_conversion(self):
        """Test that FluentResult can be converted to list."""
        metadata = RequestMetadata()
        result = FluentResult(["a", "b"], metadata)
        as_list = list(result)
        assert as_list == ["a", "b"]

    def test_slice_access(self):
        """Test that FluentResult supports slice access."""
        metadata = RequestMetadata()
        result = FluentResult([1, 2, 3, 4, 5], metadata)
        assert result[1:3] == [2, 3]
        assert result[:2] == [1, 2]
        assert result[::2] == [1, 3, 5]

    def test_conditional_check(self):
        """Test FluentResult in conditional expressions."""
        metadata = RequestMetadata()

        # Non-empty result
        result = FluentResult(["id1"], metadata)
        if result:
            passed = True
        else:
            passed = False
        assert passed is True

        # Empty result
        empty_result = FluentResult([], metadata)
        if empty_result:
            passed = True
        else:
            passed = False
        assert passed is False

    def test_print_statement(self):
        """Test that FluentResult prints correctly."""
        metadata = RequestMetadata()
        result = FluentResult("test-guid", metadata)
        # This should not raise and should produce readable output
        output = str(result)
        assert output == "test-guid"
