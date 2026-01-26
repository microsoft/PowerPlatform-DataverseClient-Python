# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for core result types."""

import pytest

from PowerPlatform.Dataverse.core.results import (
    RequestTelemetryData,
    DataverseResponse,
    OperationResult,
)


class TestRequestTelemetryData:
    """Tests for RequestTelemetryData dataclass."""

    def test_default_values(self):
        """RequestTelemetryData should have None defaults for all fields."""
        metadata = RequestTelemetryData()
        assert metadata.client_request_id is None
        assert metadata.correlation_id is None
        assert metadata.service_request_id is None

    def test_with_values(self):
        """RequestTelemetryData should store provided values."""
        metadata = RequestTelemetryData(
            client_request_id="client-123",
            correlation_id="corr-456",
            service_request_id="svc-789",
        )
        assert metadata.client_request_id == "client-123"
        assert metadata.correlation_id == "corr-456"
        assert metadata.service_request_id == "svc-789"

    def test_is_frozen(self):
        """RequestTelemetryData should be immutable (frozen=True)."""
        metadata = RequestTelemetryData(client_request_id="test")
        with pytest.raises(AttributeError):
            metadata.client_request_id = "new-value"  # type: ignore[misc]


class TestDataverseResponse:
    """Tests for DataverseResponse dataclass."""

    def test_default_telemetry(self):
        """DataverseResponse should have empty dict as default telemetry."""
        response = DataverseResponse(result="test-result")
        assert response.result == "test-result"
        assert response.telemetry == {}

    def test_with_telemetry(self):
        """DataverseResponse should store provided telemetry."""
        telemetry = {
            "client_request_id": "client-123",
            "correlation_id": "corr-456",
            "service_request_id": "svc-789",
        }
        response = DataverseResponse(result=["id1", "id2"], telemetry=telemetry)
        assert response.result == ["id1", "id2"]
        assert response.telemetry == telemetry

    def test_generic_typing(self):
        """DataverseResponse should support generic typing."""
        # String result
        str_response: DataverseResponse[str] = DataverseResponse(result="single-id")
        assert str_response.result == "single-id"

        # List result
        list_response: DataverseResponse[list] = DataverseResponse(result=["id1", "id2"])
        assert list_response.result == ["id1", "id2"]


class TestOperationResult:
    """Tests for OperationResult class."""

    @pytest.fixture
    def sample_telemetry_data(self):
        """Create sample metadata for tests."""
        return RequestTelemetryData(
            client_request_id="client-123",
            correlation_id="corr-456",
            service_request_id="svc-789",
        )

    def test_value_property(self, sample_telemetry_data):
        """OperationResult.value should return the underlying result."""
        result = OperationResult(result="test-value", telemetry_data=sample_telemetry_data)
        assert result.value == "test-value"

    def test_with_response_details(self, sample_telemetry_data):
        """with_response_details() should return DataverseResponse with telemetry."""
        result = OperationResult(result=["id1", "id2"], telemetry_data=sample_telemetry_data)
        response = result.with_response_details()

        assert isinstance(response, DataverseResponse)
        assert response.result == ["id1", "id2"]
        assert response.telemetry["client_request_id"] == "client-123"
        assert response.telemetry["correlation_id"] == "corr-456"
        assert response.telemetry["service_request_id"] == "svc-789"

    def test_with_response_details_none_telemetry(self):
        """with_response_details() should handle None telemetry values."""
        telemetry_data = RequestTelemetryData()  # All None
        result = OperationResult(result="test", telemetry_data=telemetry_data)
        response = result.with_response_details()

        assert response.telemetry["client_request_id"] is None
        assert response.telemetry["correlation_id"] is None
        assert response.telemetry["service_request_id"] is None


class TestOperationResultIteration:
    """Tests for OperationResult iteration behavior."""

    @pytest.fixture
    def sample_telemetry_data(self):
        return RequestTelemetryData()

    def test_iter_with_list(self, sample_telemetry_data):
        """Iteration over OperationResult with list result should yield list elements."""
        result = OperationResult(result=["id1", "id2", "id3"], telemetry_data=sample_telemetry_data)
        items = list(result)
        assert items == ["id1", "id2", "id3"]

    def test_iter_with_tuple(self, sample_telemetry_data):
        """Iteration over OperationResult with tuple result should yield tuple elements."""
        result = OperationResult(result=("a", "b"), telemetry_data=sample_telemetry_data)
        items = list(result)
        assert items == ["a", "b"]

    def test_iter_with_single_value(self, sample_telemetry_data):
        """Iteration over OperationResult with single value should yield that value."""
        result = OperationResult(result="single-id", telemetry_data=sample_telemetry_data)
        items = list(result)
        assert items == ["single-id"]

    def test_for_loop_iteration(self, sample_telemetry_data):
        """OperationResult should work in for loops."""
        result = OperationResult(result=["a", "b", "c"], telemetry_data=sample_telemetry_data)
        collected = []
        for item in result:
            collected.append(item)
        assert collected == ["a", "b", "c"]


class TestOperationResultIndexing:
    """Tests for OperationResult indexing behavior."""

    @pytest.fixture
    def sample_telemetry_data(self):
        return RequestTelemetryData()

    def test_getitem_with_list(self, sample_telemetry_data):
        """Indexing OperationResult with list result should work."""
        result = OperationResult(result=["id1", "id2", "id3"], telemetry_data=sample_telemetry_data)
        assert result[0] == "id1"
        assert result[1] == "id2"
        assert result[2] == "id3"
        assert result[-1] == "id3"

    def test_getitem_with_dict(self, sample_telemetry_data):
        """Indexing OperationResult with dict result should work."""
        result = OperationResult(result={"name": "Contoso", "id": "123"}, telemetry_data=sample_telemetry_data)
        assert result["name"] == "Contoso"
        assert result["id"] == "123"

    def test_dict_get_method(self, sample_telemetry_data):
        """Dict .get() method should work via __getattr__ delegation."""
        result = OperationResult(result={"name": "Contoso", "id": "123"}, telemetry_data=sample_telemetry_data)
        assert result.get("name") == "Contoso"
        assert result.get("missing") is None
        assert result.get("missing", "default") == "default"

    def test_getitem_slice(self, sample_telemetry_data):
        """Slicing OperationResult with list result should work."""
        result = OperationResult(result=["a", "b", "c", "d"], telemetry_data=sample_telemetry_data)
        assert result[1:3] == ["b", "c"]


class TestOperationResultLength:
    """Tests for OperationResult length behavior."""

    @pytest.fixture
    def sample_telemetry_data(self):
        return RequestTelemetryData()

    def test_len_with_list(self, sample_telemetry_data):
        """len() on OperationResult with list should return list length."""
        result = OperationResult(result=["a", "b", "c"], telemetry_data=sample_telemetry_data)
        assert len(result) == 3

    def test_len_with_tuple(self, sample_telemetry_data):
        """len() on OperationResult with tuple should return tuple length."""
        result = OperationResult(result=(1, 2), telemetry_data=sample_telemetry_data)
        assert len(result) == 2

    def test_len_with_dict(self, sample_telemetry_data):
        """len() on OperationResult with dict should return dict length."""
        result = OperationResult(result={"a": 1, "b": 2}, telemetry_data=sample_telemetry_data)
        assert len(result) == 2

    def test_len_with_single_value(self, sample_telemetry_data):
        """len() on OperationResult with single value should return 1."""
        result = OperationResult(result="single", telemetry_data=sample_telemetry_data)
        assert len(result) == 1

    def test_len_with_empty_list(self, sample_telemetry_data):
        """len() on OperationResult with empty list should return 0."""
        result = OperationResult(result=[], telemetry_data=sample_telemetry_data)
        assert len(result) == 0


class TestOperationResultStringConversion:
    """Tests for OperationResult string conversion."""

    @pytest.fixture
    def sample_telemetry_data(self):
        return RequestTelemetryData()

    def test_str_with_string(self, sample_telemetry_data):
        """str() on OperationResult should return string of result."""
        result = OperationResult(result="test-id", telemetry_data=sample_telemetry_data)
        assert str(result) == "test-id"

    def test_str_with_list(self, sample_telemetry_data):
        """str() on OperationResult with list should return string of list."""
        result = OperationResult(result=["a", "b"], telemetry_data=sample_telemetry_data)
        assert str(result) == "['a', 'b']"

    def test_repr(self, sample_telemetry_data):
        """repr() on OperationResult should show class name and result."""
        result = OperationResult(result=["id1"], telemetry_data=sample_telemetry_data)
        assert repr(result) == "OperationResult(['id1'])"


class TestOperationResultEquality:
    """Tests for OperationResult equality comparison."""

    @pytest.fixture
    def sample_telemetry_data(self):
        return RequestTelemetryData()

    def test_eq_with_same_result(self, sample_telemetry_data):
        """OperationResult should equal another with same result."""
        result1 = OperationResult(result=["a", "b"], telemetry_data=sample_telemetry_data)
        result2 = OperationResult(result=["a", "b"], telemetry_data=sample_telemetry_data)
        assert result1 == result2

    def test_eq_with_different_result(self, sample_telemetry_data):
        """OperationResult should not equal another with different result."""
        result1 = OperationResult(result=["a", "b"], telemetry_data=sample_telemetry_data)
        result2 = OperationResult(result=["c", "d"], telemetry_data=sample_telemetry_data)
        assert result1 != result2

    def test_eq_with_raw_value(self, sample_telemetry_data):
        """OperationResult should equal the raw result value."""
        result = OperationResult(result=["a", "b"], telemetry_data=sample_telemetry_data)
        assert result == ["a", "b"]

    def test_eq_with_string(self, sample_telemetry_data):
        """OperationResult with string result should equal that string."""
        result = OperationResult(result="test-id", telemetry_data=sample_telemetry_data)
        assert result == "test-id"


class TestOperationResultBool:
    """Tests for OperationResult boolean conversion."""

    @pytest.fixture
    def sample_telemetry_data(self):
        return RequestTelemetryData()

    def test_bool_truthy_string(self, sample_telemetry_data):
        """OperationResult with non-empty string should be truthy."""
        result = OperationResult(result="id", telemetry_data=sample_telemetry_data)
        assert bool(result) is True

    def test_bool_truthy_list(self, sample_telemetry_data):
        """OperationResult with non-empty list should be truthy."""
        result = OperationResult(result=["a"], telemetry_data=sample_telemetry_data)
        assert bool(result) is True

    def test_bool_falsy_empty_string(self, sample_telemetry_data):
        """OperationResult with empty string should be falsy."""
        result = OperationResult(result="", telemetry_data=sample_telemetry_data)
        assert bool(result) is False

    def test_bool_falsy_empty_list(self, sample_telemetry_data):
        """OperationResult with empty list should be falsy."""
        result = OperationResult(result=[], telemetry_data=sample_telemetry_data)
        assert bool(result) is False

    def test_bool_falsy_none(self, sample_telemetry_data):
        """OperationResult with None should be falsy."""
        result = OperationResult(result=None, telemetry_data=sample_telemetry_data)
        assert bool(result) is False

    def test_in_if_statement(self, sample_telemetry_data):
        """OperationResult should work in if statements."""
        result_truthy = OperationResult(result=["id"], telemetry_data=sample_telemetry_data)
        result_falsy = OperationResult(result=[], telemetry_data=sample_telemetry_data)

        if result_truthy:
            passed_truthy = True
        else:
            passed_truthy = False

        if result_falsy:
            passed_falsy = True
        else:
            passed_falsy = False

        assert passed_truthy is True
        assert passed_falsy is False


class TestOperationResultUsagePatterns:
    """Tests for common usage patterns."""

    @pytest.fixture
    def sample_telemetry_data(self):
        return RequestTelemetryData(
            client_request_id="client-123",
            correlation_id="corr-456",
            service_request_id="svc-789",
        )

    def test_single_create_indexing(self, sample_telemetry_data):
        """Single create pattern: ids = client.create(...) then ids[0]."""
        ids = OperationResult(result=["guid-123"], telemetry_data=sample_telemetry_data)
        assert ids[0] == "guid-123"

    def test_multi_create_iteration(self, sample_telemetry_data):
        """Multi-create pattern: iterate over created IDs."""
        ids = OperationResult(result=["guid-1", "guid-2"], telemetry_data=sample_telemetry_data)
        collected = []
        for id in ids:
            collected.append(id)
        assert collected == ["guid-1", "guid-2"]

    def test_telemetry_access(self, sample_telemetry_data):
        """Access telemetry via with_response_details()."""
        ids = OperationResult(result=["guid-1", "guid-2"], telemetry_data=sample_telemetry_data)
        response = ids.with_response_details()
        assert response.result == ["guid-1", "guid-2"]
        assert response.telemetry["client_request_id"] == "client-123"
        assert response.telemetry["correlation_id"] == "corr-456"
        assert response.telemetry["service_request_id"] == "svc-789"


class TestOperationResultConcatenation:
    """Tests for OperationResult concatenation with + operator."""

    @pytest.fixture
    def sample_telemetry_data(self):
        return RequestTelemetryData(
            client_request_id="client-123",
            correlation_id="corr-456",
            service_request_id="svc-789",
        )

    def test_add_two_operation_results(self, sample_telemetry_data):
        """Adding two OperationResults should concatenate their results."""
        result1 = OperationResult(result=["a", "b"], telemetry_data=sample_telemetry_data)
        result2 = OperationResult(result=["c", "d"], telemetry_data=sample_telemetry_data)
        combined = result1 + result2
        assert combined == ["a", "b", "c", "d"]
        # Result should be raw list, not OperationResult
        assert isinstance(combined, list)

    def test_add_operation_result_with_list(self, sample_telemetry_data):
        """Adding OperationResult with a list should work."""
        result = OperationResult(result=["a", "b"], telemetry_data=sample_telemetry_data)
        combined = result + ["c", "d"]
        assert combined == ["a", "b", "c", "d"]
        assert isinstance(combined, list)

    def test_radd_list_with_operation_result(self, sample_telemetry_data):
        """Right-hand addition: list + OperationResult should work."""
        result = OperationResult(result=["c", "d"], telemetry_data=sample_telemetry_data)
        combined = ["a", "b"] + result
        assert combined == ["a", "b", "c", "d"]
        assert isinstance(combined, list)

    def test_add_empty_lists(self, sample_telemetry_data):
        """Adding empty OperationResults should return empty list."""
        result1 = OperationResult(result=[], telemetry_data=sample_telemetry_data)
        result2 = OperationResult(result=[], telemetry_data=sample_telemetry_data)
        combined = result1 + result2
        assert combined == []
        assert isinstance(combined, list)

    def test_add_with_empty_list(self, sample_telemetry_data):
        """Adding OperationResult with empty list should work."""
        result = OperationResult(result=["a", "b"], telemetry_data=sample_telemetry_data)
        combined = result + []
        assert combined == ["a", "b"]

    def test_radd_empty_list(self, sample_telemetry_data):
        """Right-hand addition with empty list should work."""
        result = OperationResult(result=["a", "b"], telemetry_data=sample_telemetry_data)
        combined = [] + result
        assert combined == ["a", "b"]

    def test_concatenate_multiple_batches(self, sample_telemetry_data):
        """Simulate combining multiple page batches."""
        batch1 = OperationResult(result=[{"id": "1"}, {"id": "2"}], telemetry_data=sample_telemetry_data)
        batch2 = OperationResult(result=[{"id": "3"}, {"id": "4"}], telemetry_data=sample_telemetry_data)
        batch3 = OperationResult(result=[{"id": "5"}], telemetry_data=sample_telemetry_data)

        all_records = batch1 + batch2 + batch3
        assert len(all_records) == 5
        assert all_records[0]["id"] == "1"
        assert all_records[4]["id"] == "5"

    def test_string_concatenation(self, sample_telemetry_data):
        """String concatenation should work."""
        result = OperationResult(result="Hello ", telemetry_data=sample_telemetry_data)
        combined = result + "World"
        assert combined == "Hello World"

    def test_radd_string_concatenation(self, sample_telemetry_data):
        """Right-hand string concatenation should work."""
        result = OperationResult(result="World", telemetry_data=sample_telemetry_data)
        combined = "Hello " + result
        assert combined == "Hello World"
