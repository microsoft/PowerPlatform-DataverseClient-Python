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
    _RecordUpdate,
    _RecordUpsert,
    _TableCreate,
    _TableDelete,
    _TableGet,
    _TableList,
    _TableAddColumns,
    _TableRemoveColumns,
    _TableCreateOneToMany,
    _TableCreateManyToMany,
    _TableDeleteRelationship,
    _TableGetRelationship,
    _TableCreateLookupField,
    _QuerySql,
    _extract_boundary,
    _raise_top_level_batch_error,
    _parse_mime_part,
    _parse_http_response_part,
    _CRLF,
)
from PowerPlatform.Dataverse.core.errors import HttpError, MetadataError, ValidationError
from PowerPlatform.Dataverse.models.upsert import UpsertItem
from PowerPlatform.Dataverse.data._raw_request import _RawRequest


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
        self.assertIsNone(item.data)
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
        self.assertEqual(item.data, body)
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
        self.assertIsNone(item.data)

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

    def test_resolve_record_update_single(self):
        client, od = self._client_and_od()
        mock_req = MagicMock()
        od._build_update.return_value = mock_req

        op = _RecordUpdate(table="account", ids="guid-1", changes={"name": "Updated"})
        result = client._resolve_record_update(op)

        od._build_update.assert_called_once_with("account", "guid-1", {"name": "Updated"}, content_id=None)
        self.assertEqual(result, [mock_req])

    def test_resolve_record_update_multiple(self):
        client, od = self._client_and_od()
        mock_req = MagicMock()
        od._build_update_multiple.return_value = mock_req

        op = _RecordUpdate(
            table="account",
            ids=["guid-1", "guid-2"],
            changes=[{"name": "A"}, {"name": "B"}],
        )
        result = client._resolve_record_update(op)

        od._build_update_multiple.assert_called_once()
        self.assertEqual(result, [mock_req])

    def test_resolve_record_update_single_with_list_changes_raises(self):
        client, od = self._client_and_od()

        op = _RecordUpdate(table="account", ids="guid-1", changes=[{"name": "A"}])
        with self.assertRaises(TypeError):
            client._resolve_record_update(op)

    def test_resolve_record_delete_multiple_ids(self):
        client, od = self._client_and_od()
        mock_req = MagicMock()
        od._build_delete_multiple.return_value = mock_req

        op = _RecordDelete(table="account", ids=["guid-1", "guid-2", "guid-3"])
        result = client._resolve_record_delete(op)

        od._build_delete_multiple.assert_called_once_with("account", ["guid-1", "guid-2", "guid-3"])
        self.assertEqual(result, [mock_req])

    def test_resolve_record_delete_multiple_no_bulk(self):
        client, od = self._client_and_od()
        mock_req = MagicMock()
        od._build_delete.return_value = mock_req

        op = _RecordDelete(table="account", ids=["guid-1", "guid-2"], use_bulk_delete=False)
        result = client._resolve_record_delete(op)

        self.assertEqual(od._build_delete.call_count, 2)
        self.assertEqual(len(result), 2)

    def test_resolve_record_delete_empty_ids_returns_empty(self):
        client, od = self._client_and_od()

        op = _RecordDelete(table="account", ids=[])
        result = client._resolve_record_delete(op)

        self.assertEqual(result, [])

    def test_resolve_record_delete_filters_empty_strings(self):
        client, od = self._client_and_od()
        mock_req = MagicMock()
        od._build_delete_multiple.return_value = mock_req

        op = _RecordDelete(table="account", ids=["guid-1", "", "guid-2", ""])
        result = client._resolve_record_delete(op)

        od._build_delete_multiple.assert_called_once_with("account", ["guid-1", "guid-2"])

    def test_resolve_record_delete_all_empty_strings_returns_empty(self):
        client, od = self._client_and_od()

        op = _RecordDelete(table="account", ids=["", "", ""])
        result = client._resolve_record_delete(op)

        self.assertEqual(result, [])

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
        with self.assertRaises(ValidationError):
            client.execute(items)


class TestContinueOnError(unittest.TestCase):
    """execute() sends Prefer: odata.continue-on-error when requested."""

    def setUp(self):
        self.od = _make_od()
        self.od._build_get.return_value = _RawRequest(method="GET", url="https://x/accounts(g)")
        mock_resp = MagicMock()
        mock_resp.headers = {"Content-Type": 'multipart/mixed; boundary="batch_x"'}
        mock_resp.status_code = 200
        mock_resp.text = "--batch_x\r\n\r\nHTTP/1.1 204 No Content\r\n\r\n\r\n--batch_x--"
        self.od._request.return_value = mock_resp
        self.client = _BatchClient(self.od)

    def test_continue_on_error_header_sent(self):
        """Prefer: odata.continue-on-error header is included when continue_on_error=True."""
        self.client.execute([_RecordGet(table="account", record_id="guid-1")], continue_on_error=True)
        _, kwargs = self.od._request.call_args
        self.assertEqual(kwargs.get("headers", {}).get("Prefer"), "odata.continue-on-error")

    def test_no_continue_on_error_header_by_default(self):
        """Prefer header is absent when continue_on_error is not set."""
        self.client.execute([_RecordGet(table="account", record_id="guid-1")])
        _, kwargs = self.od._request.call_args
        self.assertNotIn("Prefer", kwargs.get("headers", {}))


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
        self.assertEqual(cs._counter[0], 3)

    def test_operations_in_order(self):
        cs = _ChangeSet()
        cs.add_create("account", {"name": "A"})
        cs.add_delete("account", "guid-1")
        self.assertEqual(len(cs.operations), 2)
        self.assertIsInstance(cs.operations[0], _RecordCreate)
        self.assertIsInstance(cs.operations[1], _RecordDelete)

    def test_two_changesets_shared_counter_produce_unique_content_ids(self):
        """Two _ChangeSets sharing a counter must emit batch-wide unique Content-IDs."""
        shared = [1]
        cs1 = _ChangeSet(_counter=shared)
        cs2 = _ChangeSet(_counter=shared)

        cs1.add_create("account", {"name": "A"})  # cid=1
        cs1.add_update("account", "guid-1", {"name": "B"})  # cid=2
        cs2.add_create("contact", {"firstname": "C"})  # cid=3
        cs2.add_update("contact", "guid-2", {"firstname": "D"})  # cid=4

        ids_cs1 = [op.content_id for op in cs1.operations]
        ids_cs2 = [op.content_id for op in cs2.operations]
        self.assertEqual(ids_cs1, [1, 2])
        self.assertEqual(ids_cs2, [3, 4])
        # No overlap
        self.assertEqual(len(set(ids_cs1) & set(ids_cs2)), 0)

    def test_standalone_changeset_still_starts_at_one(self):
        """A _ChangeSet created without a shared counter gets its own [1] counter."""
        cs = _ChangeSet()
        ref = cs.add_create("account", {"name": "X"})
        self.assertEqual(ref, "$1")
        self.assertEqual(cs._counter[0], 2)


class TestResolveBatchUpsert(unittest.TestCase):
    """Tests that _BatchClient._resolve_record_upsert calls the correct _build_* methods."""

    def _client_and_od(self):
        od = _make_od()
        od._entity_set_from_schema_name.return_value = "accounts"
        client = _BatchClient(od)
        return client, od

    def test_resolve_single_item_calls_build_upsert(self):
        client, od = self._client_and_od()
        mock_req = MagicMock()
        od._build_upsert.return_value = mock_req

        item = UpsertItem(alternate_key={"accountnumber": "ACC-001"}, record={"name": "Contoso"})
        op = _RecordUpsert(table="account", items=[item])
        result = client._resolve_record_upsert(op)

        od._build_upsert.assert_called_once_with(
            "accounts", "account", {"accountnumber": "ACC-001"}, {"name": "Contoso"}
        )
        self.assertEqual(result, [mock_req])

    def test_resolve_multiple_items_calls_build_upsert_multiple(self):
        client, od = self._client_and_od()
        mock_req = MagicMock()
        od._build_upsert_multiple.return_value = mock_req

        items = [
            UpsertItem(alternate_key={"accountnumber": "ACC-001"}, record={"name": "Contoso"}),
            UpsertItem(alternate_key={"accountnumber": "ACC-002"}, record={"name": "Fabrikam"}),
        ]
        op = _RecordUpsert(table="account", items=items)
        result = client._resolve_record_upsert(op)

        od._build_upsert_multiple.assert_called_once_with(
            "accounts",
            "account",
            [{"accountnumber": "ACC-001"}, {"accountnumber": "ACC-002"}],
            [{"name": "Contoso"}, {"name": "Fabrikam"}],
        )
        self.assertEqual(result, [mock_req])

    def test_resolve_item_dispatch_routes_to_upsert(self):
        client, od = self._client_and_od()
        mock_req = MagicMock()
        od._build_upsert.return_value = mock_req

        item = UpsertItem(alternate_key={"accountnumber": "ACC-001"}, record={"name": "Contoso"})
        op = _RecordUpsert(table="account", items=[item])
        result = client._resolve_item(op)

        self.assertEqual(result, [mock_req])


class TestBatchRecordOperationsUpsert(unittest.TestCase):
    """Tests for BatchRecordOperations.upsert (operations/batch.py)."""

    def _make_batch(self):
        from PowerPlatform.Dataverse.operations.batch import BatchRecordOperations

        batch = MagicMock()
        batch._items = []
        return BatchRecordOperations(batch), batch

    def test_upsert_single_upsert_item_appended(self):
        from PowerPlatform.Dataverse.operations.batch import BatchRecordOperations

        rec_ops, batch = self._make_batch()
        item = UpsertItem(alternate_key={"accountnumber": "ACC-001"}, record={"name": "Contoso"})
        rec_ops.upsert("account", [item])

        self.assertEqual(len(batch._items), 1)
        intent = batch._items[0]
        self.assertIsInstance(intent, _RecordUpsert)
        self.assertEqual(intent.table, "account")
        self.assertEqual(len(intent.items), 1)
        self.assertEqual(intent.items[0].alternate_key, {"accountnumber": "ACC-001"})

    def test_upsert_plain_dict_normalised_to_upsert_item(self):
        from PowerPlatform.Dataverse.operations.batch import BatchRecordOperations

        rec_ops, batch = self._make_batch()
        rec_ops.upsert("account", [{"alternate_key": {"accountnumber": "X"}, "record": {"name": "Y"}}])

        intent = batch._items[0]
        self.assertIsInstance(intent.items[0], UpsertItem)
        self.assertEqual(intent.items[0].record, {"name": "Y"})

    def test_upsert_empty_list_raises(self):
        from PowerPlatform.Dataverse.operations.batch import BatchRecordOperations

        rec_ops, _ = self._make_batch()
        with self.assertRaises(TypeError):
            rec_ops.upsert("account", [])

    def test_upsert_invalid_item_raises(self):
        from PowerPlatform.Dataverse.operations.batch import BatchRecordOperations

        rec_ops, _ = self._make_batch()
        with self.assertRaises(TypeError):
            rec_ops.upsert("account", ["not_a_valid_item"])

    def test_upsert_multiple_items_all_normalised(self):
        from PowerPlatform.Dataverse.operations.batch import BatchRecordOperations

        rec_ops, batch = self._make_batch()
        rec_ops.upsert(
            "account",
            [
                UpsertItem(alternate_key={"accountnumber": "A"}, record={"name": "Alpha"}),
                UpsertItem(alternate_key={"accountnumber": "B"}, record={"name": "Beta"}),
            ],
        )

        intent = batch._items[0]
        self.assertEqual(len(intent.items), 2)
        self.assertEqual(intent.items[1].alternate_key, {"accountnumber": "B"})


class TestRaiseTopLevelBatchError(unittest.TestCase):
    """_raise_top_level_batch_error surfaces Dataverse error details as HttpError."""

    def _make_response(self, status_code, json_body=None, text=None):
        resp = MagicMock()
        resp.status_code = status_code
        resp.text = text or ""
        if json_body is not None:
            resp.json.return_value = json_body
        else:
            resp.json.side_effect = ValueError("no JSON")
        return resp

    def test_raises_http_error(self):
        """Always raises HttpError, never returns."""
        resp = self._make_response(400, json_body={"error": {"code": "0x0", "message": "Bad batch"}})
        with self.assertRaises(HttpError):
            _raise_top_level_batch_error(resp)

    def test_status_code_preserved(self):
        """HttpError.status_code matches the response status code."""
        resp = self._make_response(400, json_body={"error": {"code": "0x0", "message": "Bad batch"}})
        with self.assertRaises(HttpError) as ctx:
            _raise_top_level_batch_error(resp)
        self.assertEqual(ctx.exception.status_code, 400)

    def test_service_message_in_exception(self):
        """The Dataverse error message is included in the raised exception."""
        resp = self._make_response(400, json_body={"error": {"code": "BadRequest", "message": "Malformed OData batch"}})
        with self.assertRaises(HttpError) as ctx:
            _raise_top_level_batch_error(resp)
        self.assertIn("Malformed OData batch", str(ctx.exception))

    def test_service_error_code_preserved(self):
        """The Dataverse error code is forwarded into HttpError.details."""
        resp = self._make_response(400, json_body={"error": {"code": "0x80040216", "message": "..."}})
        with self.assertRaises(HttpError) as ctx:
            _raise_top_level_batch_error(resp)
        self.assertEqual(ctx.exception.details.get("service_error_code"), "0x80040216")

    def test_falls_back_to_response_text_when_no_json(self):
        """Falls back to response.text when the body is not valid JSON."""
        resp = self._make_response(400, text="plain text error body")
        with self.assertRaises(HttpError) as ctx:
            _raise_top_level_batch_error(resp)
        self.assertIn("plain text error body", str(ctx.exception))

    def test_parse_batch_response_raises_on_missing_boundary(self):
        """_BatchClient._parse_batch_response raises HttpError for non-multipart responses."""
        od = _make_od()
        client = _BatchClient(od)
        resp = MagicMock()
        resp.headers = {"Content-Type": "application/json"}
        resp.status_code = 400
        resp.text = ""
        resp.json.return_value = {"error": {"code": "0x0", "message": "Invalid batch"}}
        with self.assertRaises(HttpError):
            client._parse_batch_response(resp)


class TestContinueOnError(unittest.TestCase):
    """execute() sends Prefer: odata.continue-on-error when requested."""

    def setUp(self):
        self.od = _make_od()
        self.od._build_get.return_value = _RawRequest(method="GET", url="https://x/accounts(g)")
        mock_resp = MagicMock()
        mock_resp.headers = {"Content-Type": 'multipart/mixed; boundary="batch_x"'}
        mock_resp.status_code = 200
        mock_resp.text = "--batch_x\r\n\r\nHTTP/1.1 204 No Content\r\n\r\n\r\n--batch_x--"
        self.od._request.return_value = mock_resp
        self.client = _BatchClient(self.od)

    def test_continue_on_error_header_sent(self):
        self.client.execute([_RecordGet(table="account", record_id="guid-1")], continue_on_error=True)
        _, kwargs = self.od._request.call_args
        self.assertEqual(kwargs.get("headers", {}).get("Prefer"), "odata.continue-on-error")

    def test_no_continue_on_error_header_by_default(self):
        self.client.execute([_RecordGet(table="account", record_id="guid-1")])
        _, kwargs = self.od._request.call_args
        self.assertNotIn("Prefer", kwargs.get("headers", {}))


class TestResolveItemDispatch(unittest.TestCase):
    """_resolve_item() routes each intent type to the correct resolver."""

    def _client_and_od(self):
        od = _make_od()
        client = _BatchClient(od)
        return client, od

    def test_dispatch_record_update(self):
        """_resolve_item routes _RecordUpdate to _resolve_record_update."""
        client, od = self._client_and_od()
        od._build_update.return_value = MagicMock()
        op = _RecordUpdate(table="account", ids="guid-1", changes={"name": "X"})
        result = client._resolve_item(op)
        od._build_update.assert_called_once()
        self.assertEqual(len(result), 1)

    def test_dispatch_record_delete(self):
        """_resolve_item routes _RecordDelete to _resolve_record_delete."""
        client, od = self._client_and_od()
        od._build_delete.return_value = MagicMock()
        op = _RecordDelete(table="account", ids="guid-1")
        result = client._resolve_item(op)
        od._build_delete.assert_called_once()
        self.assertEqual(len(result), 1)

    def test_dispatch_table_create(self):
        """_resolve_item routes _TableCreate to _build_create_entity."""
        client, od = self._client_and_od()
        od._build_create_entity.return_value = MagicMock()
        op = _TableCreate(table="new_Widget", columns={"new_name": str})
        result = client._resolve_item(op)
        od._build_create_entity.assert_called_once()
        self.assertEqual(len(result), 1)

    def test_dispatch_table_delete(self):
        """_resolve_item routes _TableDelete, resolving MetadataId before calling _build_delete_entity."""
        client, od = self._client_and_od()
        od._get_entity_by_table_schema_name.return_value = {"MetadataId": "meta-1"}
        od._build_delete_entity.return_value = MagicMock()
        op = _TableDelete(table="new_Widget")
        result = client._resolve_item(op)
        od._build_delete_entity.assert_called_once_with("meta-1")
        self.assertEqual(len(result), 1)

    def test_dispatch_table_get(self):
        """_resolve_item routes _TableGet to _build_get_entity."""
        client, od = self._client_and_od()
        od._build_get_entity.return_value = MagicMock()
        op = _TableGet(table="account")
        result = client._resolve_item(op)
        od._build_get_entity.assert_called_once()
        self.assertEqual(len(result), 1)

    def test_dispatch_table_list(self):
        """_resolve_item routes _TableList to _build_list_entities."""
        client, od = self._client_and_od()
        od._build_list_entities.return_value = MagicMock()
        op = _TableList()
        result = client._resolve_item(op)
        od._build_list_entities.assert_called_once()
        self.assertEqual(len(result), 1)

    def test_dispatch_table_add_columns(self):
        """_resolve_item routes _TableAddColumns, emitting one request per column."""
        client, od = self._client_and_od()
        od._get_entity_by_table_schema_name.return_value = {"MetadataId": "meta-1"}
        od._build_create_column.return_value = MagicMock()
        op = _TableAddColumns(table="account", columns={"new_col": str})
        result = client._resolve_item(op)
        od._build_create_column.assert_called_once()
        self.assertEqual(len(result), 1)

    def test_dispatch_table_remove_columns(self):
        """_resolve_item routes _TableRemoveColumns, fetching attribute metadata before deleting."""
        client, od = self._client_and_od()
        od._get_entity_by_table_schema_name.return_value = {"MetadataId": "meta-1"}
        od._get_attribute_metadata.return_value = {"MetadataId": "attr-1"}
        od._build_delete_column.return_value = MagicMock()
        op = _TableRemoveColumns(table="account", columns="new_col")
        result = client._resolve_item(op)
        od._build_delete_column.assert_called_once()
        self.assertEqual(len(result), 1)

    def test_dispatch_table_create_one_to_many(self):
        """_resolve_item routes _TableCreateOneToMany, merging lookup into relationship body."""
        client, od = self._client_and_od()
        od._build_create_relationship.return_value = MagicMock()
        lookup = MagicMock()
        lookup.to_dict.return_value = {}
        relationship = MagicMock()
        relationship.to_dict.return_value = {}
        op = _TableCreateOneToMany(lookup=lookup, relationship=relationship)
        result = client._resolve_item(op)
        od._build_create_relationship.assert_called_once()
        self.assertEqual(len(result), 1)

    def test_dispatch_table_create_many_to_many(self):
        """_resolve_item routes _TableCreateManyToMany to _build_create_relationship."""
        client, od = self._client_and_od()
        od._build_create_relationship.return_value = MagicMock()
        relationship = MagicMock()
        relationship.to_dict.return_value = {}
        op = _TableCreateManyToMany(relationship=relationship)
        result = client._resolve_item(op)
        od._build_create_relationship.assert_called_once()
        self.assertEqual(len(result), 1)

    def test_dispatch_table_delete_relationship(self):
        """_resolve_item routes _TableDeleteRelationship, passing relationship_id."""
        client, od = self._client_and_od()
        od._build_delete_relationship.return_value = MagicMock()
        op = _TableDeleteRelationship(relationship_id="rel-guid-1")
        result = client._resolve_item(op)
        od._build_delete_relationship.assert_called_once_with("rel-guid-1")
        self.assertEqual(len(result), 1)

    def test_dispatch_table_get_relationship(self):
        """_resolve_item routes _TableGetRelationship, passing schema_name."""
        client, od = self._client_and_od()
        od._build_get_relationship.return_value = MagicMock()
        op = _TableGetRelationship(schema_name="new_account_contact")
        result = client._resolve_item(op)
        od._build_get_relationship.assert_called_once_with("new_account_contact")
        self.assertEqual(len(result), 1)

    def test_dispatch_table_create_lookup_field(self):
        """_resolve_item routes _TableCreateLookupField, building lookup and relationship models."""
        client, od = self._client_and_od()
        lookup = MagicMock()
        lookup.to_dict.return_value = {}
        relationship = MagicMock()
        relationship.to_dict.return_value = {}
        od._build_lookup_field_models.return_value = (lookup, relationship)
        od._build_create_relationship.return_value = MagicMock()
        op = _TableCreateLookupField(
            referencing_table="new_Widget",
            lookup_field_name="new_accountid",
            referenced_table="account",
        )
        result = client._resolve_item(op)
        od._build_lookup_field_models.assert_called_once()
        od._build_create_relationship.assert_called_once()
        self.assertEqual(len(result), 1)

    def test_dispatch_query_sql(self):
        """_resolve_item routes _QuerySql to _build_sql, passing the SQL string."""
        client, od = self._client_and_od()
        od._build_sql.return_value = MagicMock()
        op = _QuerySql(sql="SELECT name FROM account")
        result = client._resolve_item(op)
        od._build_sql.assert_called_once_with("SELECT name FROM account")
        self.assertEqual(len(result), 1)


class TestResolveOneChangeset(unittest.TestCase):
    """_resolve_one() raises ValidationError when operation produces != 1 request."""

    def test_multi_request_op_in_changeset_raises(self):
        """use_bulk_delete=False with 2 ids produces 2 requests — not allowed in a changeset."""
        od = _make_od()
        client = _BatchClient(od)
        od._build_delete.return_value = MagicMock()
        op = _RecordDelete(table="account", ids=["guid-1", "guid-2"], use_bulk_delete=False)
        with self.assertRaises(ValidationError):
            client._resolve_one(op)


class TestRequireEntityMetadata(unittest.TestCase):
    """_require_entity_metadata raises MetadataError when table not found."""

    def test_missing_entity_raises_metadata_error(self):
        """MetadataError raised when _get_entity_by_table_schema_name returns None."""
        od = _make_od()
        od._get_entity_by_table_schema_name.return_value = None
        client = _BatchClient(od)
        with self.assertRaises(MetadataError):
            client._require_entity_metadata("new_Missing")

    def test_entity_without_metadata_id_raises(self):
        """MetadataError raised when entity exists but has no MetadataId field."""
        od = _make_od()
        od._get_entity_by_table_schema_name.return_value = {"LogicalName": "new_missing"}
        client = _BatchClient(od)
        with self.assertRaises(MetadataError):
            client._require_entity_metadata("new_Missing")

    def test_valid_entity_returns_metadata_id(self):
        """Returns MetadataId string when entity is found and has a MetadataId."""
        od = _make_od()
        od._get_entity_by_table_schema_name.return_value = {"MetadataId": "meta-abc"}
        client = _BatchClient(od)
        result = client._require_entity_metadata("account")
        self.assertEqual(result, "meta-abc")


class TestTableRemoveColumnsResolver(unittest.TestCase):
    """_resolve_table_remove_columns covers string input and missing column error."""

    def _client_and_od(self):
        od = _make_od()
        client = _BatchClient(od)
        return client, od

    def test_single_string_column_resolved(self):
        """A single string column name is accepted and resolved to one delete request."""
        client, od = self._client_and_od()
        od._get_entity_by_table_schema_name.return_value = {"MetadataId": "meta-1"}
        od._get_attribute_metadata.return_value = {"MetadataId": "attr-1"}
        od._build_delete_column.return_value = MagicMock()
        op = _TableRemoveColumns(table="account", columns="new_col")
        result = client._resolve_table_remove_columns(op)
        self.assertEqual(len(result), 1)

    def test_missing_column_raises_metadata_error(self):
        """MetadataError raised when attribute metadata is not found for the column."""
        client, od = self._client_and_od()
        od._get_entity_by_table_schema_name.return_value = {"MetadataId": "meta-1"}
        od._get_attribute_metadata.return_value = None
        op = _TableRemoveColumns(table="account", columns="new_missing")
        with self.assertRaises(MetadataError):
            client._resolve_table_remove_columns(op)

    def test_column_without_metadata_id_raises(self):
        """MetadataError raised when attribute metadata exists but has no MetadataId."""
        client, od = self._client_and_od()
        od._get_entity_by_table_schema_name.return_value = {"MetadataId": "meta-1"}
        od._get_attribute_metadata.return_value = {"AttributeType": "String"}
        op = _TableRemoveColumns(table="account", columns="new_col")
        with self.assertRaises(MetadataError):
            client._resolve_table_remove_columns(op)


class TestParseMimePartNoSeparator(unittest.TestCase):
    """_parse_mime_part handles raw string with no blank-line separator."""

    def test_no_double_newline_returns_empty_body(self):
        """When raw part has no blank-line separator, headers are parsed and body is empty."""
        raw = "Content-Type: application/http"
        headers, body = _parse_mime_part(raw)
        self.assertEqual(headers.get("content-type"), "application/http")
        self.assertEqual(body, "")


class TestParseHttpResponsePartMalformed(unittest.TestCase):
    """_parse_http_response_part returns None for malformed status lines."""

    def test_status_line_too_short_returns_none(self):
        """Returns None when status line has fewer than 2 tokens (no status code)."""
        result = _parse_http_response_part("HTTP/1.1\r\n\r\n", content_id=None)
        self.assertIsNone(result)

    def test_non_integer_status_code_returns_none(self):
        """Returns None when status code token is not a valid integer."""
        result = _parse_http_response_part("HTTP/1.1 XYZ OK\r\n\r\n", content_id=None)
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
