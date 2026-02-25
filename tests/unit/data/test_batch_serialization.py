# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Unit tests for the internal batch multipart serialisation and response parsing."""

import json
import unittest
from unittest.mock import MagicMock

from PowerPlatform.Dataverse.data._batch import (
    _BatchClient,
    _ChangeSet,
    _ChangeSetBatchItem,
    _RecordCreate,
    _RecordDelete,
    _RecordGet,
    _TableGet,
    _TableList,
    _QuerySql,
    _extract_boundary,
    _split_multipart,
    _parse_mime_part,
    _parse_http_response_part,
    _CRLF,
)
from PowerPlatform.Dataverse.data._raw_request import _RawRequest
from PowerPlatform.Dataverse.models.batch import BatchItemResponse, BatchResult


def _make_od():
    """Return a minimal mock _ODataClient."""
    od = MagicMock()
    od.api = "https://org.crm.dynamics.com/api/data/v9.2"
    return od


class TestExtractBoundary(unittest.TestCase):
    def test_quoted_boundary(self):
        ct = 'multipart/mixed; boundary="batch_abc123"'
        self.assertEqual(_extract_boundary(ct), "batch_abc123")

    def test_unquoted_boundary(self):
        ct = "multipart/mixed; boundary=batch_abc123"
        self.assertEqual(_extract_boundary(ct), "batch_abc123")

    def test_no_boundary_returns_none(self):
        self.assertIsNone(_extract_boundary("application/json"))

    def test_empty_string_returns_none(self):
        self.assertIsNone(_extract_boundary(""))

    def test_boundary_with_uuid(self):
        ct = 'multipart/mixed; boundary="batch_11111111-2222-3333-4444-555555555555"'
        self.assertEqual(
            _extract_boundary(ct),
            "batch_11111111-2222-3333-4444-555555555555",
        )


class TestParseHttpResponsePart(unittest.TestCase):
    def test_no_content_204(self):
        text = "HTTP/1.1 204 No Content\r\n\r\n"
        item = _parse_http_response_part(text, content_id=None)
        self.assertIsNotNone(item)
        self.assertEqual(item.status_code, 204)
        self.assertTrue(item.is_success)
        self.assertIsNone(item.body)
        self.assertIsNone(item.entity_id)

    def test_created_with_entity_id(self):
        guid = "11111111-2222-3333-4444-555555555555"
        text = (
            f"HTTP/1.1 201 Created\r\n"
            f"OData-EntityId: https://org.crm.dynamics.com/api/data/v9.2/accounts({guid})\r\n"
            f"\r\n"
        )
        item = _parse_http_response_part(text, content_id=None)
        self.assertEqual(item.status_code, 201)
        self.assertEqual(item.entity_id, guid)

    def test_get_response_with_body(self):
        body = {"accountid": "abc", "name": "Contoso"}
        body_str = json.dumps(body)
        text = f"HTTP/1.1 200 OK\r\n" f"Content-Type: application/json\r\n" f"\r\n" f"{body_str}"
        item = _parse_http_response_part(text, content_id=None)
        self.assertEqual(item.status_code, 200)
        self.assertEqual(item.body, body)
        self.assertIsNone(item.error_message)

    def test_error_response(self):
        error = {"error": {"code": "0x80040217", "message": "Object does not exist"}}
        body_str = json.dumps(error)
        text = f"HTTP/1.1 404 Not Found\r\n" f"Content-Type: application/json\r\n" f"\r\n" f"{body_str}"
        item = _parse_http_response_part(text, content_id=None)
        self.assertEqual(item.status_code, 404)
        self.assertFalse(item.is_success)
        self.assertEqual(item.error_message, "Object does not exist")
        self.assertEqual(item.error_code, "0x80040217")
        self.assertIsNone(item.body)

    def test_content_id_passed_through(self):
        text = "HTTP/1.1 204 No Content\r\n\r\n"
        item = _parse_http_response_part(text, content_id="1")
        self.assertEqual(item.content_id, "1")

    def test_empty_text_returns_none(self):
        self.assertIsNone(_parse_http_response_part("", content_id=None))

    def test_no_http_status_line_returns_none(self):
        self.assertIsNone(_parse_http_response_part("Not an HTTP response", content_id=None))


class TestSerializeRawRequest(unittest.TestCase):
    def _client(self):
        od = _make_od()
        return _BatchClient(od)

    def test_get_request_no_body(self):
        req = _RawRequest(method="GET", url="https://org/api/data/v9.2/accounts")
        client = self._client()
        part = client._serialize_raw_request(req, "boundary_xyz")
        self.assertIn("--boundary_xyz", part)
        self.assertIn("Content-Type: application/http", part)
        self.assertIn("GET https://org/api/data/v9.2/accounts HTTP/1.1", part)
        self.assertNotIn("Content-Type: application/json", part)

    def test_post_request_with_body(self):
        req = _RawRequest(
            method="POST",
            url="https://org/api/data/v9.2/accounts",
            body='{"name":"Contoso"}',
        )
        client = self._client()
        part = client._serialize_raw_request(req, "bnd")
        self.assertIn("Content-Type: application/json; type=entry", part)
        self.assertIn('{"name":"Contoso"}', part)

    def test_delete_request_with_if_match_header(self):
        req = _RawRequest(
            method="DELETE",
            url="https://org/api/data/v9.2/accounts(guid)",
            headers={"If-Match": "*"},
        )
        client = self._client()
        part = client._serialize_raw_request(req, "bnd")
        self.assertIn("If-Match: *", part)

    def test_content_id_header_emitted(self):
        req = _RawRequest(
            method="POST",
            url="https://org/api/data/v9.2/accounts",
            body="{}",
            content_id=3,
        )
        client = self._client()
        part = client._serialize_raw_request(req, "bnd")
        self.assertIn("Content-ID: 3", part)

    def test_no_content_id_when_none(self):
        req = _RawRequest(method="GET", url="https://org/api/data/v9.2/accounts")
        client = self._client()
        part = client._serialize_raw_request(req, "bnd")
        self.assertNotIn("Content-ID", part)

    def test_crlf_line_endings(self):
        req = _RawRequest(method="GET", url="https://org/api/data/v9.2/accounts")
        client = self._client()
        part = client._serialize_raw_request(req, "bnd")
        self.assertIn(_CRLF, part)


class TestBuildBatchBody(unittest.TestCase):
    def _client(self):
        od = _make_od()
        return _BatchClient(od)

    def test_single_request_body_ends_with_closing_boundary(self):
        req = _RawRequest(method="GET", url="https://org/api/data/v9.2/accounts")
        client = self._client()
        body = client._build_batch_body([req], "batch_bnd")
        self.assertIn("--batch_bnd--", body)

    def test_multiple_requests_all_in_body(self):
        r1 = _RawRequest(method="GET", url="https://org/api/data/v9.2/accounts")
        r2 = _RawRequest(
            method="DELETE",
            url="https://org/api/data/v9.2/accounts(guid)",
            headers={"If-Match": "*"},
        )
        client = self._client()
        body = client._build_batch_body([r1, r2], "bnd")
        self.assertEqual(body.count("--bnd\r\n"), 2)

    def test_changeset_produces_nested_multipart(self):
        r1 = _RawRequest(method="POST", url="https://org/api/data/v9.2/accounts", body="{}")
        cs = _ChangeSetBatchItem(requests=[r1])
        client = self._client()
        body = client._build_batch_body([cs], "outer_bnd")
        self.assertIn("Content-Type: multipart/mixed", body)
        self.assertIn("changeset_", body)


class TestResolveBatchItems(unittest.TestCase):
    """Tests that _BatchClient._resolve_item calls the correct _build_* methods."""

    def _client_and_od(self):
        od = _make_od()
        od._entity_set_from_schema_name.return_value = "accounts"
        od._primary_id_attr.return_value = "accountid"
        client = _BatchClient(od)
        return client, od

    def test_resolve_record_create_single(self):
        client, od = self._client_and_od()
        mock_req = MagicMock()
        od._build_create.return_value = mock_req

        op = _RecordCreate(table="account", data={"name": "Contoso"})
        result = client._resolve_record_create(op)

        od._build_create.assert_called_once()
        self.assertEqual(result, [mock_req])

    def test_resolve_record_create_list(self):
        client, od = self._client_and_od()
        mock_req = MagicMock()
        od._build_create_multiple.return_value = mock_req

        op = _RecordCreate(table="account", data=[{"name": "A"}, {"name": "B"}])
        result = client._resolve_record_create(op)

        od._build_create_multiple.assert_called_once()
        self.assertEqual(result, [mock_req])

    def test_resolve_record_get(self):
        client, od = self._client_and_od()
        mock_req = MagicMock()
        od._build_get.return_value = mock_req

        op = _RecordGet(table="account", record_id="guid-1", select=["name"])
        result = client._resolve_record_get(op)

        od._build_get.assert_called_once_with("account", "guid-1", select=["name"])
        self.assertEqual(result, [mock_req])

    def test_resolve_record_delete_single(self):
        client, od = self._client_and_od()
        mock_req = MagicMock()
        od._build_delete.return_value = mock_req

        op = _RecordDelete(table="account", ids="guid-1")
        result = client._resolve_record_delete(op)

        od._build_delete.assert_called_once_with("account", "guid-1", content_id=None)
        self.assertEqual(result, [mock_req])

    def test_resolve_table_get(self):
        client, od = self._client_and_od()
        mock_req = MagicMock()
        od._build_get_entity.return_value = mock_req

        op = _TableGet(table="account")
        result = client._resolve_table_get(op)

        od._build_get_entity.assert_called_once_with("account")
        self.assertEqual(result, [mock_req])

    def test_resolve_table_list(self):
        client, od = self._client_and_od()
        mock_req = MagicMock()
        od._build_list_entities.return_value = mock_req

        op = _TableList()
        result = client._resolve_table_list(op)

        od._build_list_entities.assert_called_once()
        self.assertEqual(result, [mock_req])

    def test_resolve_query_sql(self):
        client, od = self._client_and_od()
        mock_req = MagicMock()
        od._build_sql.return_value = mock_req

        op = _QuerySql(sql="SELECT name FROM account")
        result = client._resolve_query_sql(op)

        od._build_sql.assert_called_once_with("SELECT name FROM account")
        self.assertEqual(result, [mock_req])

    def test_resolve_unknown_item_raises(self):
        client, od = self._client_and_od()
        from PowerPlatform.Dataverse.core.errors import ValidationError

        with self.assertRaises(ValidationError):
            client._resolve_item("not_a_valid_intent")


class TestBatchSizeLimit(unittest.TestCase):
    def test_exceeds_1000_raises(self):
        od = _make_od()
        od._entity_set_from_schema_name.return_value = "accounts"
        od._build_get.return_value = _RawRequest(method="GET", url="https://x/accounts(g)")
        client = _BatchClient(od)

        items = [_RecordGet(table="account", record_id=f"guid-{i}") for i in range(1001)]
        from PowerPlatform.Dataverse.core.errors import ValidationError

        with self.assertRaises(ValidationError):
            client.execute(items)


class TestChangeSetInternal(unittest.TestCase):
    def test_add_create_returns_dollar_n(self):
        cs = _ChangeSet()
        ref = cs.add_create("account", {"name": "X"})
        self.assertEqual(ref, "$1")

    def test_add_create_increments_content_id(self):
        cs = _ChangeSet()
        r1 = cs.add_create("account", {"name": "A"})
        r2 = cs.add_create("contact", {"firstname": "B"})
        self.assertEqual(r1, "$1")
        self.assertEqual(r2, "$2")

    def test_add_update_increments_content_id(self):
        cs = _ChangeSet()
        cs.add_create("account", {"name": "A"})
        cs.add_update("account", "guid-1", {"name": "B"})
        self.assertEqual(cs._next_content_id, 3)

    def test_operations_in_order(self):
        cs = _ChangeSet()
        cs.add_create("account", {"name": "A"})
        cs.add_delete("account", "guid-1")
        self.assertEqual(len(cs.operations), 2)
        self.assertIsInstance(cs.operations[0], _RecordCreate)
        self.assertIsInstance(cs.operations[1], _RecordDelete)


if __name__ == "__main__":
    unittest.main()
