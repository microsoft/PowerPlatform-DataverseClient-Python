# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for _AsyncRelationshipOperationsMixin."""

from __future__ import annotations

import pytest
from unittest.mock import AsyncMock, MagicMock

from PowerPlatform.Dataverse.aio.data._async_relationships import _AsyncRelationshipOperationsMixin

# ---------------------------------------------------------------------------
# Test client
# ---------------------------------------------------------------------------


class _MockRelationshipClient(_AsyncRelationshipOperationsMixin):
    """Minimal async client that satisfies mixin dependencies."""

    def __init__(self):
        self.api = "https://example.crm.dynamics.com/api/data/v9.2"
        self._request = AsyncMock()

    def _escape_odata_quotes(self, value: str) -> str:
        return value.replace("'", "''")


def _mock_response(json_data=None, status_code=200, headers=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = headers or {}
    resp.json.return_value = json_data if json_data is not None else {}
    resp.text = str(json_data) if json_data is not None else ""
    return resp


# ---------------------------------------------------------------------------
# _create_one_to_many_relationship
# ---------------------------------------------------------------------------


class TestCreateOneToManyRelationship:
    async def test_returns_correct_dict(self):
        client = _MockRelationshipClient()
        lookup = MagicMock(schema_name="account_contact_lookup")
        lookup.to_dict.return_value = {"SchemaName": "account_contact_lookup"}
        relationship = MagicMock(
            schema_name="account_contact_rel",
            referenced_entity="account",
            referencing_entity="contact",
        )
        relationship.to_dict.return_value = {"SchemaName": "account_contact_rel"}
        client._request.return_value = _mock_response(
            headers={"OData-EntityId": "https://example.com/Relationships(11111111-1111-1111-1111-111111111111)"}
        )

        result = await client._create_one_to_many_relationship(lookup, relationship)

        assert result["relationship_id"] == "11111111-1111-1111-1111-111111111111"
        assert result["relationship_schema_name"] == "account_contact_rel"
        assert result["lookup_schema_name"] == "account_contact_lookup"
        assert result["referenced_entity"] == "account"
        assert result["referencing_entity"] == "contact"

    async def test_posts_to_relationship_definitions_url(self):
        client = _MockRelationshipClient()
        lookup = MagicMock(schema_name="lk")
        lookup.to_dict.return_value = {}
        relationship = MagicMock(schema_name="rel", referenced_entity="a", referencing_entity="b")
        relationship.to_dict.return_value = {}
        client._request.return_value = _mock_response(
            headers={"OData-EntityId": "https://x.com/R(33333333-3333-3333-3333-333333333333)"}
        )

        await client._create_one_to_many_relationship(lookup, relationship)

        call = client._request.call_args
        assert call[0][0] == "post"
        assert call[0][1].endswith("/RelationshipDefinitions")

    async def test_solution_header_set_when_provided(self):
        client = _MockRelationshipClient()
        lookup = MagicMock(schema_name="lk")
        lookup.to_dict.return_value = {}
        relationship = MagicMock(schema_name="rel", referenced_entity="a", referencing_entity="b")
        relationship.to_dict.return_value = {}
        client._request.return_value = _mock_response(
            headers={"OData-EntityId": "https://x.com/R(33333333-3333-3333-3333-333333333333)"}
        )

        await client._create_one_to_many_relationship(lookup, relationship, solution="MySolution")

        headers = client._request.call_args[1].get("headers", {})
        assert headers.get("MSCRM.SolutionUniqueName") == "MySolution"

    async def test_no_solution_header_when_not_provided(self):
        client = _MockRelationshipClient()
        lookup = MagicMock(schema_name="lk")
        lookup.to_dict.return_value = {}
        relationship = MagicMock(schema_name="rel", referenced_entity="a", referencing_entity="b")
        relationship.to_dict.return_value = {}
        client._request.return_value = _mock_response(
            headers={"OData-EntityId": "https://x.com/R(33333333-3333-3333-3333-333333333333)"}
        )

        await client._create_one_to_many_relationship(lookup, relationship)

        headers = client._request.call_args[1].get("headers") or {}
        assert "MSCRM.SolutionUniqueName" not in headers

    async def test_relationship_id_none_when_header_missing(self):
        client = _MockRelationshipClient()
        lookup = MagicMock(schema_name="lk")
        lookup.to_dict.return_value = {}
        relationship = MagicMock(schema_name="rel", referenced_entity="a", referencing_entity="b")
        relationship.to_dict.return_value = {}
        client._request.return_value = _mock_response(headers={})

        result = await client._create_one_to_many_relationship(lookup, relationship)

        assert result["relationship_id"] is None


# ---------------------------------------------------------------------------
# _create_many_to_many_relationship
# ---------------------------------------------------------------------------


class TestCreateManyToManyRelationship:
    async def test_returns_correct_dict(self):
        client = _MockRelationshipClient()
        relationship = MagicMock(
            schema_name="account_tag_rel",
            entity1_logical_name="account",
            entity2_logical_name="tag",
        )
        relationship.to_dict.return_value = {"SchemaName": "account_tag_rel"}
        client._request.return_value = _mock_response(
            headers={"OData-EntityId": "https://example.com/Relationships(22222222-2222-2222-2222-222222222222)"}
        )

        result = await client._create_many_to_many_relationship(relationship)

        assert result["relationship_id"] == "22222222-2222-2222-2222-222222222222"
        assert result["relationship_schema_name"] == "account_tag_rel"
        assert result["entity1_logical_name"] == "account"
        assert result["entity2_logical_name"] == "tag"

    async def test_solution_header_set_when_provided(self):
        client = _MockRelationshipClient()
        relationship = MagicMock(schema_name="rel", entity1_logical_name="a", entity2_logical_name="b")
        relationship.to_dict.return_value = {}
        client._request.return_value = _mock_response(
            headers={"OData-EntityId": "https://x.com/R(33333333-3333-3333-3333-333333333333)"}
        )

        await client._create_many_to_many_relationship(relationship, solution="AnotherSolution")

        headers = client._request.call_args[1].get("headers", {})
        assert headers.get("MSCRM.SolutionUniqueName") == "AnotherSolution"

    async def test_no_solution_header_when_not_provided(self):
        client = _MockRelationshipClient()
        relationship = MagicMock(schema_name="rel", entity1_logical_name="a", entity2_logical_name="b")
        relationship.to_dict.return_value = {}
        client._request.return_value = _mock_response(
            headers={"OData-EntityId": "https://x.com/R(33333333-3333-3333-3333-333333333333)"}
        )

        await client._create_many_to_many_relationship(relationship)

        headers = client._request.call_args[1].get("headers") or {}
        assert "MSCRM.SolutionUniqueName" not in headers


# ---------------------------------------------------------------------------
# _delete_relationship
# ---------------------------------------------------------------------------


class TestDeleteRelationship:
    async def test_sends_delete_to_correct_url(self):
        client = _MockRelationshipClient()
        client._request.return_value = _mock_response(status_code=204)

        await client._delete_relationship("rel-id-99")

        call = client._request.call_args
        assert call[0][0] == "delete"
        assert "rel-id-99" in call[0][1]

    async def test_sends_if_match_star_header(self):
        client = _MockRelationshipClient()
        client._request.return_value = _mock_response(status_code=204)

        await client._delete_relationship("rel-id-99")

        headers = client._request.call_args[1].get("headers", {})
        assert headers.get("If-Match") == "*"

    async def test_returns_none(self):
        client = _MockRelationshipClient()
        client._request.return_value = _mock_response(status_code=204)

        result = await client._delete_relationship("rel-id-99")

        assert result is None


# ---------------------------------------------------------------------------
# _get_relationship
# ---------------------------------------------------------------------------


class TestGetRelationship:
    async def test_returns_first_result_when_found(self):
        client = _MockRelationshipClient()
        rel = {"SchemaName": "account_contact_rel", "id": "r1"}
        client._request.return_value = _mock_response(json_data={"value": [rel]})

        result = await client._get_relationship("account_contact_rel")

        assert result == rel

    async def test_returns_none_when_not_found(self):
        client = _MockRelationshipClient()
        client._request.return_value = _mock_response(json_data={"value": []})

        result = await client._get_relationship("nonexistent_rel")

        assert result is None

    async def test_filter_param_contains_schema_name(self):
        client = _MockRelationshipClient()
        client._request.return_value = _mock_response(json_data={"value": []})

        await client._get_relationship("my_rel")

        params = client._request.call_args[1].get("params", {})
        assert "my_rel" in params.get("$filter", "")

    async def test_single_quotes_in_schema_name_escaped(self):
        client = _MockRelationshipClient()
        client._request.return_value = _mock_response(json_data={"value": []})

        await client._get_relationship("it's_rel")

        params = client._request.call_args[1].get("params", {})
        assert "it''s_rel" in params.get("$filter", "")


# ---------------------------------------------------------------------------
# _extract_id_from_header (inherited from sync mixin)
# ---------------------------------------------------------------------------


class TestExtractIdFromHeader:
    def test_extracts_guid_from_url(self):
        client = _MockRelationshipClient()
        result = client._extract_id_from_header("https://example.com/RelationshipDefinitions(abc123-def456)")
        assert result == "abc123-def456"

    def test_returns_none_for_none_input(self):
        client = _MockRelationshipClient()
        assert client._extract_id_from_header(None) is None

    def test_returns_none_for_empty_string(self):
        client = _MockRelationshipClient()
        assert client._extract_id_from_header("") is None

    def test_returns_none_when_no_parens(self):
        client = _MockRelationshipClient()
        assert client._extract_id_from_header("https://example.com/no-guid-here") is None
