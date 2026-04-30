# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for _AsyncRelationshipOperationsMixin."""

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from PowerPlatform.Dataverse.aio.data._async_odata import _AsyncODataClient
from PowerPlatform.Dataverse.core.errors import MetadataError
from PowerPlatform.Dataverse.models.relationship import (
    LookupAttributeMetadata,
    ManyToManyRelationshipMetadata,
    OneToManyRelationshipMetadata,
)
from PowerPlatform.Dataverse.models.labels import Label, LocalizedLabel

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client() -> _AsyncODataClient:
    """Return _AsyncODataClient with _request mocked at the HTTP boundary."""
    auth = MagicMock()
    auth._acquire_token = AsyncMock(return_value=MagicMock(access_token="token"))
    client = _AsyncODataClient(auth, "https://example.crm.dynamics.com")
    client._request = AsyncMock()
    return client


def _resp(json_data=None, status=200, headers=None):
    """Create a mock aiohttp-compatible response."""
    r = MagicMock()
    r.status = status
    r.headers = headers or {}
    r.text = AsyncMock(return_value="")
    r.json = AsyncMock(return_value=json_data if json_data is not None else {})
    r.read = AsyncMock(return_value=b"")
    return r


def _entity_def(meta_id="meta-001", logical="account"):
    """Return a minimal EntityDefinitions value-list response body."""
    return {
        "value": [
            {
                "LogicalName": logical,
                "EntitySetName": "accounts",
                "PrimaryIdAttribute": "accountid",
                "MetadataId": meta_id,
                "SchemaName": "Account",
            }
        ]
    }


def _label(text: str = "Test") -> Label:
    """Return a Label with a single English localized label."""
    return Label(localized_labels=[LocalizedLabel(label=text, language_code=1033)])


def _seed_cache(client, table="account", entity_set="accounts", pk="accountid"):
    """Pre-populate entity-set and primary-ID caches to bypass HTTP for schema-name lookups."""
    key = client._normalize_cache_key(table)
    client._logical_to_entityset_cache[key] = entity_set
    client._logical_primaryid_cache[key] = pk


# ---------------------------------------------------------------------------
# _extract_id_from_header (sync)
# ---------------------------------------------------------------------------


class TestExtractIdFromHeader:
    """Tests for _extract_id_from_header(), which parses GUIDs from OData-EntityId URLs.

    The regex matches only hex characters and dashes inside parentheses, so
    only proper UUID-format strings are extracted.
    """

    def test_extracts_guid_from_url(self):
        """A UUID enclosed in parentheses at the end of a URL is returned."""
        client = _make_client()
        guid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        header = f"https://example.crm.dynamics.com/api/data/v9.2/RelationshipDefinitions({guid})"
        result = client._extract_id_from_header(header)
        assert result == guid

    def test_returns_none_for_empty_header(self):
        """None is returned for both None and empty-string inputs."""
        client = _make_client()
        assert client._extract_id_from_header(None) is None
        assert client._extract_id_from_header("") is None

    def test_returns_none_when_no_guid(self):
        """None is returned when the header contains no hex UUID in parentheses."""
        client = _make_client()
        assert client._extract_id_from_header("no-guid-here") is None


# ---------------------------------------------------------------------------
# _create_one_to_many_relationship()
# ---------------------------------------------------------------------------


class TestCreateOneToManyRelationship:
    """Tests for _create_one_to_many_relationship() one-to-many relationship creation."""

    async def test_success(self):
        """The relationship ID, schema name, and lookup schema name are returned on success."""
        client = _make_client()
        guid = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
        client._request.return_value = _resp(
            status=204,
            headers={
                "OData-EntityId": f"https://example.crm.dynamics.com/api/data/v9.2/RelationshipDefinitions({guid})"
            },
        )
        lookup = LookupAttributeMetadata(schema_name="new_DeptId", display_name=_label("Dept"))
        relationship = OneToManyRelationshipMetadata(
            schema_name="new_Dept_Emp",
            referenced_entity="new_dept",
            referencing_entity="new_employee",
            referenced_attribute="new_deptid",
        )
        result = await client._create_one_to_many_relationship(lookup, relationship)
        assert result["relationship_id"] == guid
        assert result["relationship_schema_name"] == "new_Dept_Emp"
        assert result["lookup_schema_name"] == "new_DeptId"

    async def test_with_solution(self):
        """The MSCRM.SolutionUniqueName header is injected when a solution name is supplied."""
        client = _make_client()
        client._request.return_value = _resp(status=204, headers={})
        lookup = LookupAttributeMetadata(schema_name="new_DeptId", display_name=_label("Dept"))
        relationship = OneToManyRelationshipMetadata(
            schema_name="new_Dept_Emp",
            referenced_entity="new_dept",
            referencing_entity="new_employee",
            referenced_attribute="new_deptid",
        )
        await client._create_one_to_many_relationship(lookup, relationship, solution="MySolution")
        call_kwargs = client._request.call_args.kwargs
        headers = call_kwargs.get("headers", {})
        assert "MSCRM.SolutionUniqueName" in headers
        assert headers["MSCRM.SolutionUniqueName"] == "MySolution"


# ---------------------------------------------------------------------------
# _create_many_to_many_relationship()
# ---------------------------------------------------------------------------


class TestCreateManyToManyRelationship:
    """Tests for _create_many_to_many_relationship() many-to-many relationship creation."""

    async def test_success(self):
        """The relationship ID and entity names are returned on success."""
        client = _make_client()
        guid = "b2c3d4e5-f6a7-8901-bcde-f12345678901"
        client._request.return_value = _resp(
            status=204,
            headers={
                "OData-EntityId": f"https://example.crm.dynamics.com/api/data/v9.2/RelationshipDefinitions({guid})"
            },
        )
        relationship = ManyToManyRelationshipMetadata(
            schema_name="new_emp_proj",
            entity1_logical_name="new_employee",
            entity2_logical_name="new_project",
        )
        result = await client._create_many_to_many_relationship(relationship)
        assert result["relationship_id"] == guid
        assert result["relationship_schema_name"] == "new_emp_proj"
        assert result["entity1_logical_name"] == "new_employee"
        assert result["entity2_logical_name"] == "new_project"

    async def test_with_solution(self):
        """The MSCRM.SolutionUniqueName header is injected when a solution name is supplied."""
        client = _make_client()
        client._request.return_value = _resp(status=204, headers={})
        relationship = ManyToManyRelationshipMetadata(
            schema_name="new_emp_proj",
            entity1_logical_name="new_employee",
            entity2_logical_name="new_project",
        )
        await client._create_many_to_many_relationship(relationship, solution="MySol")
        headers = client._request.call_args.kwargs.get("headers", {})
        assert headers.get("MSCRM.SolutionUniqueName") == "MySol"


# ---------------------------------------------------------------------------
# _delete_relationship()
# ---------------------------------------------------------------------------


class TestDeleteRelationship:
    """Tests for _delete_relationship() relationship removal by GUID."""

    async def test_issues_delete(self):
        """A DELETE request is issued containing the relationship GUID in the URL."""
        client = _make_client()
        client._request.return_value = _resp(status=204)
        await client._delete_relationship("rel-guid-1")
        call_args = client._request.call_args
        assert call_args.args[0] == "delete"
        assert "rel-guid-1" in call_args.args[1]

    async def test_sets_if_match_header(self):
        """An If-Match: * header is sent to prevent accidental deletion of a stale version."""
        client = _make_client()
        client._request.return_value = _resp(status=204)
        await client._delete_relationship("rel-guid-1")
        headers = client._request.call_args.kwargs.get("headers", {})
        assert headers.get("If-Match") == "*"


# ---------------------------------------------------------------------------
# _get_relationship()
# ---------------------------------------------------------------------------


class TestGetRelationship:
    """Tests for _get_relationship() single-relationship lookup by schema name."""

    async def test_returns_relationship_dict(self):
        """The first matching relationship dict from the value list is returned."""
        client = _make_client()
        rel = {"SchemaName": "new_Dept_Emp", "RelationshipId": "rel-1"}
        client._request.return_value = _resp(json_data={"value": [rel]})
        result = await client._get_relationship("new_Dept_Emp")
        assert result == rel

    async def test_returns_none_when_not_found(self):
        """None is returned when the API returns an empty value list."""
        client = _make_client()
        client._request.return_value = _resp(json_data={"value": []})
        result = await client._get_relationship("nonexistent")
        assert result is None


# ---------------------------------------------------------------------------
# _list_relationships()
# ---------------------------------------------------------------------------


class TestListRelationships:
    """Tests for _list_relationships() global relationship listing."""

    async def test_returns_all_relationships(self):
        """The full value list is returned when no filter is applied."""
        client = _make_client()
        rels = [{"SchemaName": "rel1"}, {"SchemaName": "rel2"}]
        client._request.return_value = _resp(json_data={"value": rels})
        result = await client._list_relationships()
        assert result == rels

    async def test_with_filter_and_select(self):
        """Optional filter and select parameters are forwarded as OData query params."""
        client = _make_client()
        client._request.return_value = _resp(json_data={"value": []})
        result = await client._list_relationships(
            filter="RelationshipType eq 'OneToMany'",
            select=["SchemaName"],
        )
        assert result == []
        call_kwargs = client._request.call_args.kwargs
        params = call_kwargs.get("params", {})
        assert "$filter" in params
        assert "$select" in params


# ---------------------------------------------------------------------------
# _list_table_relationships()
# ---------------------------------------------------------------------------


class TestListTableRelationships:
    """Tests for _list_table_relationships() which aggregates all three relationship types."""

    async def test_combines_three_relationship_types(self):
        """One-to-many, many-to-one, and many-to-many relationships are combined into one list."""
        client = _make_client()
        entity_resp = _resp(json_data=_entity_def())
        otm_resp = _resp(json_data={"value": [{"SchemaName": "rel_otm"}]})
        mto_resp = _resp(json_data={"value": [{"SchemaName": "rel_mto"}]})
        mtm_resp = _resp(json_data={"value": [{"SchemaName": "rel_mtm"}]})
        client._request.side_effect = [entity_resp, otm_resp, mto_resp, mtm_resp]
        result = await client._list_table_relationships("account")
        assert len(result) == 3
        schema_names = [r["SchemaName"] for r in result]
        assert "rel_otm" in schema_names
        assert "rel_mto" in schema_names
        assert "rel_mtm" in schema_names

    async def test_table_not_found_raises(self):
        """MetadataError is raised when the table does not exist in entity metadata."""
        client = _make_client()
        client._request.return_value = _resp(json_data={"value": []})
        with pytest.raises(MetadataError, match="not found"):
            await client._list_table_relationships("nonexistent")

    async def test_with_filter_and_select(self):
        """Optional filter and select parameters are forwarded to all three relationship requests."""
        client = _make_client()
        entity_resp = _resp(json_data=_entity_def())
        empty_resp = _resp(json_data={"value": []})
        client._request.side_effect = [entity_resp, empty_resp, empty_resp, empty_resp]
        result = await client._list_table_relationships(
            "account",
            filter="IsCustomRelationship eq true",
            select=["SchemaName"],
        )
        assert result == []
