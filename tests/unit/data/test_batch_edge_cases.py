# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Edge case and corner scenario tests for batch operations.

Covers OData $batch spec compliance, error handling, and scenarios
derived from the Dataverse Web API public documentation:
https://learn.microsoft.com/en-us/power-apps/developer/data-platform/webapi/execute-batch-operations-using-web-api
"""

import json
import unittest
from unittest.mock import MagicMock, patch

from PowerPlatform.Dataverse.data._batch import (
    _BatchClient,
    _ChangeSet,
    _ChangeSetBatchItem,
    _RecordCreate,
    _RecordDelete,
    _RecordGet,
    _RecordUpdate,
    _QuerySql,
    _TableList,
    _extract_boundary,
    _raise_top_level_batch_error,
    _split_multipart,
    _parse_http_response_part,
    _CRLF,
    _MAX_BATCH_SIZE,
)
from PowerPlatform.Dataverse.core.errors import HttpError, ValidationError
from PowerPlatform.Dataverse.data._raw_request import _RawRequest
from PowerPlatform.Dataverse.models.batch import BatchItemResponse, BatchResult


def _make_od():
    """Return a minimal mock _ODataClient."""
    od = MagicMock()
    od.api = "https://org.crm.dynamics.com/api/data/v9.2"
    return od


# ---------------------------------------------------------------------------
# 1. Empty changeset handling
# ---------------------------------------------------------------------------


class TestEmptyChangeset(unittest.TestCase):
    """An empty changeset (no operations) should be silently skipped."""

    def test_empty_changeset_skipped_in_resolve(self):
        """_resolve_all skips empty changesets rather than producing empty multipart parts."""
        od = _make_od()
        client = _BatchClient(od)
        cs = _ChangeSet()  # no operations
        # Also include a non-changeset item to ensure the batch is not entirely empty
        get = _RecordGet(table="account", record_id="guid-1")
        od._build_get.return_value = _RawRequest(method="GET", url="https://org/api/data/v9.2/accounts(guid-1)")
        resolved = client._resolve_all([cs, get])
        # Should only have the GET, no changeset
        self.assertEqual(len(resolved), 1)
        self.assertIsInstance(resolved[0], _RawRequest)

    def test_empty_changeset_only_batch_returns_empty_result(self):
        """A batch with only empty changesets has no items and returns empty BatchResult."""
        od = _make_od()
        client = _BatchClient(od)
        cs = _ChangeSet()
        resolved = client._resolve_all([cs])
        self.assertEqual(len(resolved), 0)

    def test_changeset_with_operations_not_skipped(self):
        """A changeset with operations is not skipped."""
        od = _make_od()
        client = _BatchClient(od)
        cs = _ChangeSet()
        cs.add_create("account", {"name": "Test"})
        req = _RawRequest(method="POST", url="https://org/api/data/v9.2/accounts", body='{"name":"Test"}')
        od._build_create.return_value = req
        resolved = client._resolve_all([cs])
        self.assertEqual(len(resolved), 1)
        self.assertIsInstance(resolved[0], _ChangeSetBatchItem)
        self.assertEqual(len(resolved[0].requests), 1)


# ---------------------------------------------------------------------------
# 2. Changeset error/rollback response parsing
# ---------------------------------------------------------------------------


class TestChangeSetRollbackResponse(unittest.TestCase):
    """When a changeset fails, Dataverse returns a single error for the entire changeset."""

    def test_changeset_error_parsed_as_failed_item(self):
        """A changeset failure returns one response in the inner changeset boundary."""
        # Simulate a batch response where a changeset within it returned an error
        cs_error_body = json.dumps(
            {
                "error": {
                    "code": "0x80040237",
                    "message": "A record with matching key values already exists.",
                }
            }
        )
        inner_response = (
            "HTTP/1.1 409 Conflict\r\n"
            "Content-Type: application/json; odata.metadata=minimal\r\n"
            "OData-Version: 4.0\r\n"
            "\r\n"
            f"{cs_error_body}"
        )
        cs_boundary = "changesetresponse_abc123"
        inner_multipart = (
            f"--{cs_boundary}\r\n"
            "Content-Type: application/http\r\n"
            "Content-Transfer-Encoding: binary\r\n"
            "Content-ID: 1\r\n"
            "\r\n"
            f"{inner_response}\r\n"
            f"--{cs_boundary}--\r\n"
        )
        batch_boundary = "batchresponse_xyz789"
        full_response = (
            f"--{batch_boundary}\r\n"
            f'Content-Type: multipart/mixed; boundary="{cs_boundary}"\r\n'
            "\r\n"
            f"{inner_multipart}\r\n"
            f"--{batch_boundary}--\r\n"
        )

        mock_response = MagicMock()
        mock_response.headers = {"Content-Type": f'multipart/mixed; boundary="{batch_boundary}"'}
        mock_response.text = full_response

        od = _make_od()
        client = _BatchClient(od)
        result = client._parse_batch_response(mock_response)

        self.assertTrue(result.has_errors)
        self.assertEqual(len(result.failed), 1)
        self.assertEqual(result.failed[0].status_code, 409)
        self.assertEqual(result.failed[0].error_code, "0x80040237")
        self.assertIn("matching key", result.failed[0].error_message)

    def test_successful_changeset_returns_all_items(self):
        """A successful changeset returns 204 per create operation."""
        cs_boundary = "changesetresponse_ok123"
        inner = ""
        for i in range(1, 4):
            guid = f"0000000{i}-0000-0000-0000-000000000000"
            inner += (
                f"--{cs_boundary}\r\n"
                "Content-Type: application/http\r\n"
                "Content-Transfer-Encoding: binary\r\n"
                f"Content-ID: {i}\r\n"
                "\r\n"
                "HTTP/1.1 204 No Content\r\n"
                "OData-Version: 4.0\r\n"
                f"OData-EntityId: https://org.crm.dynamics.com/api/data/v9.2/tasks({guid})\r\n"
                "\r\n"
                "\r\n"
            )
        inner += f"--{cs_boundary}--\r\n"

        batch_boundary = "batchresponse_good789"
        full = (
            f"--{batch_boundary}\r\n"
            f'Content-Type: multipart/mixed; boundary="{cs_boundary}"\r\n'
            "\r\n"
            f"{inner}\r\n"
            f"--{batch_boundary}--\r\n"
        )

        mock_response = MagicMock()
        mock_response.headers = {"Content-Type": f'multipart/mixed; boundary="{batch_boundary}"'}
        mock_response.text = full

        od = _make_od()
        client = _BatchClient(od)
        result = client._parse_batch_response(mock_response)

        self.assertFalse(result.has_errors)
        self.assertEqual(len(result.succeeded), 3)
        self.assertEqual(len(result.created_ids), 3)
        # Verify content-IDs were extracted
        content_ids = [r.content_id for r in result.responses]
        self.assertEqual(content_ids, ["1", "2", "3"])


# ---------------------------------------------------------------------------
# 3. Content-ID in non-changeset response parts
# ---------------------------------------------------------------------------


class TestContentIdInStandaloneParts(unittest.TestCase):
    """Non-changeset parts in the batch response can have content-id headers."""

    def test_standalone_part_content_id_extracted(self):
        """Content-ID from standalone (non-changeset) MIME headers is propagated."""
        batch_boundary = "batchresponse_solo123"
        response_text = (
            f"--{batch_boundary}\r\n"
            "Content-Type: application/http\r\n"
            "Content-Transfer-Encoding: binary\r\n"
            "Content-ID: 42\r\n"
            "\r\n"
            "HTTP/1.1 204 No Content\r\n"
            "OData-Version: 4.0\r\n"
            "OData-EntityId: https://org.crm.dynamics.com/api/data/v9.2/accounts(11111111-1111-1111-1111-111111111111)\r\n"
            "\r\n"
            "\r\n"
            f"--{batch_boundary}--\r\n"
        )

        mock_response = MagicMock()
        mock_response.headers = {"Content-Type": f'multipart/mixed; boundary="{batch_boundary}"'}
        mock_response.text = response_text

        od = _make_od()
        client = _BatchClient(od)
        result = client._parse_batch_response(mock_response)

        self.assertEqual(len(result.responses), 1)
        self.assertEqual(result.responses[0].content_id, "42")


# ---------------------------------------------------------------------------
# 4. Mixed batch: changesets + standalone GETs
# ---------------------------------------------------------------------------


class TestMixedBatch(unittest.TestCase):
    """Batch with both changeset writes and standalone reads."""

    def test_changeset_plus_standalone_get_parsed(self):
        """Response with changeset (204s) followed by standalone GET (200 with body)."""
        cs_boundary = "changesetresponse_mix1"
        cs_part = (
            f"--{cs_boundary}\r\n"
            "Content-Type: application/http\r\n"
            "Content-Transfer-Encoding: binary\r\n"
            "Content-ID: 1\r\n"
            "\r\n"
            "HTTP/1.1 204 No Content\r\n"
            "OData-EntityId: https://org.crm.dynamics.com/api/data/v9.2/tasks(aaaaaaaa-0000-0000-0000-000000000000)\r\n"
            "\r\n"
            f"--{cs_boundary}--\r\n"
        )

        get_body = json.dumps({"@odata.context": "...", "value": [{"name": "Contoso"}]})
        batch_boundary = "batchresponse_mixed123"
        full = (
            f"--{batch_boundary}\r\n"
            f'Content-Type: multipart/mixed; boundary="{cs_boundary}"\r\n'
            "\r\n"
            f"{cs_part}\r\n"
            f"--{batch_boundary}\r\n"
            "Content-Type: application/http\r\n"
            "Content-Transfer-Encoding: binary\r\n"
            "\r\n"
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: application/json; odata.metadata=minimal\r\n"
            "OData-Version: 4.0\r\n"
            "\r\n"
            f"{get_body}\r\n"
            f"--{batch_boundary}--\r\n"
        )

        mock_response = MagicMock()
        mock_response.headers = {"Content-Type": f'multipart/mixed; boundary="{batch_boundary}"'}
        mock_response.text = full

        od = _make_od()
        client = _BatchClient(od)
        result = client._parse_batch_response(mock_response)

        self.assertEqual(len(result.responses), 2)
        # First: changeset create (204)
        self.assertEqual(result.responses[0].status_code, 204)
        self.assertEqual(result.responses[0].entity_id, "aaaaaaaa-0000-0000-0000-000000000000")
        # Second: standalone GET (200 with body data)
        self.assertEqual(result.responses[1].status_code, 200)
        self.assertIsNotNone(result.responses[1].data)


# ---------------------------------------------------------------------------
# 5. Multiple changesets in one batch
# ---------------------------------------------------------------------------


class TestMultipleChangesets(unittest.TestCase):
    """Batch with multiple changesets — content IDs must be globally unique."""

    def test_two_changesets_unique_content_ids(self):
        """Two changesets in the same batch get unique content IDs."""
        counter = [1]
        cs1 = _ChangeSet(_counter=counter)
        cs2 = _ChangeSet(_counter=counter)

        ref1 = cs1.add_create("account", {"name": "A"})
        ref2 = cs1.add_create("account", {"name": "B"})
        ref3 = cs2.add_create("contact", {"firstname": "C"})
        ref4 = cs2.add_update("contact", ref3, {"lastname": "D"})

        self.assertEqual(ref1, "$1")
        self.assertEqual(ref2, "$2")
        self.assertEqual(ref3, "$3")
        # Counter should now be at 5
        self.assertEqual(counter[0], 5)

        # All content IDs across both changesets must be unique
        all_cids = [op.content_id for op in cs1.operations + cs2.operations]
        self.assertEqual(len(all_cids), len(set(all_cids)))


# ---------------------------------------------------------------------------
# 6. Batch size limit with mixed changesets
# ---------------------------------------------------------------------------


class TestBatchSizeLimitMixed(unittest.TestCase):
    """Max 1000 operations counting across changesets and standalone items."""

    def test_changeset_ops_counted_toward_limit(self):
        """Operations inside changesets count toward the 1000 limit."""
        od = _make_od()
        client = _BatchClient(od)
        # 999 standalone + 2 in a changeset = 1001 > 1000
        cs = _ChangeSet()
        cs.add_create("a", {"name": "x"})
        cs.add_create("a", {"name": "y"})

        items = [_RecordGet(table="account", record_id=f"guid-{i}") for i in range(999)]
        items.append(cs)

        od._build_get.return_value = _RawRequest(method="GET", url="https://org/api/data/v9.2/accounts(x)")
        od._build_create.return_value = _RawRequest(
            method="POST", url="https://org/api/data/v9.2/accounts", body="{}"
        )

        with self.assertRaises(ValidationError) as ctx:
            client.execute(items)
        self.assertIn("1001", str(ctx.exception))
        self.assertIn("1000", str(ctx.exception))

    def test_exactly_1000_operations_allowed(self):
        """Exactly 1000 operations should not raise."""
        od = _make_od()
        client = _BatchClient(od)

        items = [_RecordGet(table="account", record_id=f"guid-{i}") for i in range(1000)]

        od._build_get.return_value = _RawRequest(method="GET", url="https://org/api/data/v9.2/accounts(x)")
        mock_resp = MagicMock()
        mock_resp.headers = {"Content-Type": 'multipart/mixed; boundary="resp_bnd"'}
        mock_resp.text = "--resp_bnd--\r\n"
        od._request.return_value = mock_resp

        # Should not raise
        result = client.execute(items)
        self.assertIsInstance(result, BatchResult)


# ---------------------------------------------------------------------------
# 7. Top-level batch error handling
# ---------------------------------------------------------------------------


class TestTopLevelBatchError(unittest.TestCase):
    """When Dataverse rejects the batch request itself (non-multipart response)."""

    def test_json_error_body_raised_as_http_error(self):
        """A 400 with JSON error body raises HttpError with the message."""
        mock_resp = MagicMock()
        mock_resp.status_code = 400
        mock_resp.json.return_value = {
            "error": {
                "code": "0x80048d19",
                "message": "The batch request must have Content-Type multipart/mixed.",
            }
        }
        mock_resp.text = json.dumps(mock_resp.json.return_value)

        with self.assertRaises(HttpError) as ctx:
            _raise_top_level_batch_error(mock_resp)
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertIn("multipart/mixed", str(ctx.exception))

    def test_non_json_body_raised_with_text(self):
        """A 500 with non-JSON body raises HttpError with the raw text."""
        mock_resp = MagicMock()
        mock_resp.status_code = 500
        mock_resp.json.side_effect = ValueError("not JSON")
        mock_resp.text = "Internal Server Error"

        with self.assertRaises(HttpError) as ctx:
            _raise_top_level_batch_error(mock_resp)
        self.assertEqual(ctx.exception.status_code, 500)
        self.assertIn("Internal Server Error", str(ctx.exception))

    def test_empty_body_raises_generic_error(self):
        """A 503 with empty body raises HttpError with a generic message."""
        mock_resp = MagicMock()
        mock_resp.status_code = 503
        mock_resp.json.side_effect = ValueError("empty")
        mock_resp.text = ""

        with self.assertRaises(HttpError) as ctx:
            _raise_top_level_batch_error(mock_resp)
        self.assertEqual(ctx.exception.status_code, 503)

    def test_error_code_preserved_as_service_error_code(self):
        """The error.code field is preserved in service_error_code."""
        mock_resp = MagicMock()
        mock_resp.status_code = 403
        mock_resp.json.return_value = {
            "error": {"code": "0x80040220", "message": "Principal user is missing privileges."}
        }
        mock_resp.text = ""

        with self.assertRaises(HttpError) as ctx:
            _raise_top_level_batch_error(mock_resp)
        self.assertEqual(ctx.exception.details.get("service_error_code"), "0x80040220")


# ---------------------------------------------------------------------------
# 8. Batch response without continue-on-error (first failure stops)
# ---------------------------------------------------------------------------


class TestBatchWithoutContinueOnError(unittest.TestCase):
    """Without Prefer: odata.continue-on-error, first failure stops the batch."""

    def test_single_error_response_parsed(self):
        """A 400 batch response with a single error in multipart body."""
        error_body = json.dumps(
            {
                "error": {
                    "code": "0x80044331",
                    "message": "The length of the 'subject' attribute exceeded the maximum allowed length of '200'.",
                }
            }
        )
        batch_boundary = "batchresponse_err123"
        body = (
            f"--{batch_boundary}\r\n"
            "Content-Type: application/http\r\n"
            "Content-Transfer-Encoding: binary\r\n"
            "\r\n"
            "HTTP/1.1 400 Bad Request\r\n"
            "Content-Type: application/json; odata.metadata=minimal\r\n"
            "OData-Version: 4.0\r\n"
            "\r\n"
            f"{error_body}\r\n"
            f"--{batch_boundary}--\r\n"
        )

        mock_response = MagicMock()
        mock_response.headers = {"Content-Type": f'multipart/mixed; boundary="{batch_boundary}"'}
        mock_response.text = body

        od = _make_od()
        client = _BatchClient(od)
        result = client._parse_batch_response(mock_response)

        self.assertTrue(result.has_errors)
        self.assertEqual(len(result.responses), 1)
        self.assertEqual(result.responses[0].status_code, 400)
        self.assertEqual(result.responses[0].error_code, "0x80044331")


# ---------------------------------------------------------------------------
# 9. Batch with continue-on-error: mixed success/failure
# ---------------------------------------------------------------------------


class TestBatchContinueOnError(unittest.TestCase):
    """With continue-on-error, successful items are returned alongside failures."""

    def test_mixed_success_and_failure(self):
        """One 400 error + two 204 successes parsed correctly."""
        error_body = json.dumps({"error": {"code": "0x80040237", "message": "record not found"}})
        batch_boundary = "batchresponse_coe123"
        body = (
            f"--{batch_boundary}\r\n"
            "Content-Type: application/http\r\n"
            "Content-Transfer-Encoding: binary\r\n"
            "\r\n"
            "HTTP/1.1 400 Bad Request\r\n"
            "Content-Type: application/json\r\n"
            "\r\n"
            f"{error_body}\r\n"
            f"--{batch_boundary}\r\n"
            "Content-Type: application/http\r\n"
            "Content-Transfer-Encoding: binary\r\n"
            "\r\n"
            "HTTP/1.1 204 No Content\r\n"
            "OData-EntityId: https://org.crm.dynamics.com/api/data/v9.2/tasks(11111111-1111-1111-1111-111111111111)\r\n"
            "\r\n"
            f"--{batch_boundary}\r\n"
            "Content-Type: application/http\r\n"
            "Content-Transfer-Encoding: binary\r\n"
            "\r\n"
            "HTTP/1.1 204 No Content\r\n"
            "OData-EntityId: https://org.crm.dynamics.com/api/data/v9.2/tasks(22222222-2222-2222-2222-222222222222)\r\n"
            "\r\n"
            f"--{batch_boundary}--\r\n"
        )

        mock_response = MagicMock()
        mock_response.headers = {"Content-Type": f'multipart/mixed; boundary="{batch_boundary}"'}
        mock_response.text = body

        od = _make_od()
        client = _BatchClient(od)
        result = client._parse_batch_response(mock_response)

        self.assertTrue(result.has_errors)
        self.assertEqual(len(result.succeeded), 2)
        self.assertEqual(len(result.failed), 1)
        self.assertEqual(result.failed[0].error_code, "0x80040237")
        self.assertEqual(len(result.created_ids), 2)


# ---------------------------------------------------------------------------
# 10. Serialization spec compliance
# ---------------------------------------------------------------------------


class TestSerializationCompliance(unittest.TestCase):
    """Verify serialized batch body matches OData $batch spec requirements."""

    def _client(self):
        od = _make_od()
        return _BatchClient(od)

    def test_crlf_line_endings_in_batch_body(self):
        """All line endings in the batch body must be CRLF per OData spec."""
        client = self._client()
        req = _RawRequest(method="GET", url="https://org/api/data/v9.2/accounts(guid)")
        part = client._serialize_raw_request(req, "batch_test")
        # Every newline should be CRLF
        lines = part.split("\r\n")
        self.assertGreater(len(lines), 1)
        # Ensure no bare LFs
        for line in lines:
            self.assertNotIn("\n", line.rstrip("\n"))

    def test_content_transfer_encoding_binary(self):
        """Each batch part must include Content-Transfer-Encoding: binary."""
        client = self._client()
        req = _RawRequest(method="POST", url="https://org/api/data/v9.2/accounts", body='{"name":"x"}')
        part = client._serialize_raw_request(req, "batch_test")
        self.assertIn("Content-Transfer-Encoding: binary", part)

    def test_content_type_application_http(self):
        """Each batch part must include Content-Type: application/http."""
        client = self._client()
        req = _RawRequest(method="GET", url="https://org/api/data/v9.2/accounts(guid)")
        part = client._serialize_raw_request(req, "batch_test")
        self.assertIn("Content-Type: application/http", part)

    def test_batch_body_ends_with_closing_boundary(self):
        """Batch body must end with --{boundary}-- terminator."""
        client = self._client()
        resolved = [_RawRequest(method="GET", url="https://org/api/data/v9.2/accounts(guid)")]
        body = client._build_batch_body(resolved, "batch_end_test")
        self.assertTrue(body.strip().endswith("--batch_end_test--"))

    def test_changeset_nested_boundary_different_from_batch(self):
        """Changeset uses a different boundary than the batch."""
        client = self._client()
        cs = _ChangeSetBatchItem(
            requests=[
                _RawRequest(method="POST", url="https://org/api/data/v9.2/accounts", body="{}", content_id=1),
            ]
        )
        body = client._build_batch_body([cs], "batch_outer")
        # Should contain both batch_outer and changeset_<uuid>
        self.assertIn("--batch_outer", body)
        self.assertIn("changeset_", body)

    def test_post_body_has_content_type_json_with_type_entry(self):
        """POST/PATCH body parts include Content-Type: application/json; type=entry."""
        client = self._client()
        req = _RawRequest(method="POST", url="https://org/api/data/v9.2/accounts", body='{"name":"x"}')
        part = client._serialize_raw_request(req, "bnd")
        self.assertIn("Content-Type: application/json; type=entry", part)

    def test_absolute_urls_in_batch_parts(self):
        """Batch parts use absolute URLs (required by Dataverse)."""
        client = self._client()
        url = "https://org.crm.dynamics.com/api/data/v9.2/accounts(guid)"
        req = _RawRequest(method="GET", url=url)
        part = client._serialize_raw_request(req, "bnd")
        self.assertIn(f"GET {url} HTTP/1.1", part)


# ---------------------------------------------------------------------------
# 11. BatchResult computed properties
# ---------------------------------------------------------------------------


class TestBatchResultProperties(unittest.TestCase):
    """Verify computed properties of BatchResult."""

    def test_created_ids_only_from_201_and_204(self):
        """created_ids should include entity_ids from all 2xx (including 201 and 204)."""
        responses = [
            BatchItemResponse(status_code=201, entity_id="id-201"),
            BatchItemResponse(status_code=204, entity_id="id-204"),
            BatchItemResponse(status_code=200, entity_id=None),  # GET success, no entity_id
            BatchItemResponse(status_code=400, entity_id=None),
        ]
        result = BatchResult(responses=responses)
        self.assertEqual(result.created_ids, ["id-201", "id-204"])

    def test_empty_batch_result_properties(self):
        """Empty BatchResult has correct defaults."""
        result = BatchResult()
        self.assertEqual(result.succeeded, [])
        self.assertEqual(result.failed, [])
        self.assertFalse(result.has_errors)
        self.assertEqual(result.created_ids, [])

    def test_all_success_no_errors(self):
        """All 2xx responses means has_errors is False."""
        responses = [
            BatchItemResponse(status_code=200),
            BatchItemResponse(status_code=204),
        ]
        result = BatchResult(responses=responses)
        self.assertFalse(result.has_errors)
        self.assertEqual(len(result.succeeded), 2)
        self.assertEqual(len(result.failed), 0)

    def test_single_failure_makes_has_errors_true(self):
        """Even one 4xx/5xx makes has_errors True."""
        responses = [
            BatchItemResponse(status_code=200),
            BatchItemResponse(status_code=404, error_message="not found"),
        ]
        result = BatchResult(responses=responses)
        self.assertTrue(result.has_errors)
        self.assertEqual(len(result.failed), 1)


# ---------------------------------------------------------------------------
# 12. Multipart parsing edge cases
# ---------------------------------------------------------------------------


class TestMultipartParsingEdgeCases(unittest.TestCase):
    """Edge cases in multipart response parsing."""

    def test_response_with_only_closing_boundary(self):
        """A response body with only the closing boundary produces no parts."""
        parts = _split_multipart("--bnd--\r\n", "bnd")
        self.assertEqual(len(parts), 0)

    def test_response_with_extra_whitespace_in_parts(self):
        """Parts with extra whitespace/blank lines should still parse."""
        body = (
            "--bnd\r\n"
            "Content-Type: application/http\r\n"
            "\r\n"
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: application/json\r\n"
            "\r\n"
            '{"value":[]}\r\n'
            "--bnd--\r\n"
        )
        parts = _split_multipart(body, "bnd")
        self.assertEqual(len(parts), 1)

    def test_parse_response_with_req_id_header(self):
        """Dataverse error responses include REQ_ID header — should not break parsing."""
        text = (
            "HTTP/1.1 400 Bad Request\r\n"
            "REQ_ID: 5ecd1cb3-1730-4ffc-909c-d44c22270026\r\n"
            "Content-Type: application/json; odata.metadata=minimal\r\n"
            "OData-Version: 4.0\r\n"
            "\r\n"
            '{"error":{"code":"0x80044331","message":"validation error"}}'
        )
        item = _parse_http_response_part(text, content_id=None)
        self.assertIsNotNone(item)
        self.assertEqual(item.status_code, 400)
        self.assertEqual(item.error_code, "0x80044331")
        self.assertEqual(item.error_message, "validation error")

    def test_entity_id_extracted_from_various_guid_formats(self):
        """GUID extraction works with different formats."""
        # Standard UUID
        text = (
            "HTTP/1.1 204 No Content\r\n"
            "OData-EntityId: https://org.crm.dynamics.com/api/data/v9.2/"
            "accounts(a1b2c3d4-e5f6-7890-abcd-ef1234567890)\r\n"
            "\r\n"
        )
        item = _parse_http_response_part(text, content_id=None)
        self.assertEqual(item.entity_id, "a1b2c3d4-e5f6-7890-abcd-ef1234567890")

    def test_no_entity_id_for_delete_response(self):
        """Delete responses typically have no OData-EntityId."""
        text = "HTTP/1.1 204 No Content\r\n\r\n"
        item = _parse_http_response_part(text, content_id=None)
        self.assertIsNone(item.entity_id)

    def test_get_response_body_parsed_as_data(self):
        """A 200 OK GET response should have body parsed into data."""
        body_data = {"@odata.context": "...", "name": "Contoso", "accountid": "guid-1"}
        text = (
            "HTTP/1.1 200 OK\r\n"
            "Content-Type: application/json; odata.metadata=minimal\r\n"
            "\r\n"
            f"{json.dumps(body_data)}"
        )
        item = _parse_http_response_part(text, content_id=None)
        self.assertEqual(item.status_code, 200)
        self.assertEqual(item.data["name"], "Contoso")
        self.assertIsNone(item.error_message)


# ---------------------------------------------------------------------------
# 13. Changeset content-ID reference validation
# ---------------------------------------------------------------------------


class TestContentIdReferences(unittest.TestCase):
    """Content-ID references ($n) in changesets."""

    def test_content_id_ref_format(self):
        """add_create returns $n format string starting from 1."""
        cs = _ChangeSet()
        ref = cs.add_create("account", {"name": "Test"})
        self.assertEqual(ref, "$1")
        self.assertTrue(ref.startswith("$"))

    def test_content_id_usable_in_odata_bind(self):
        """Content-ID reference can be used in @odata.bind field."""
        cs = _ChangeSet()
        lead_ref = cs.add_create("lead", {"firstname": "Ada"})
        cs.add_create(
            "account",
            {"name": "Babbage", "originatingleadid@odata.bind": lead_ref},
        )
        # The second create should have the ref in its data
        self.assertEqual(cs.operations[1].data["originatingleadid@odata.bind"], "$1")

    def test_content_id_usable_as_record_id_in_update(self):
        """Content-ID reference can be used as record_id for update."""
        cs = _ChangeSet()
        ref = cs.add_create("contact", {"firstname": "Alice"})
        cs.add_update("contact", ref, {"lastname": "Smith"})
        # The update should use the ref as record_id
        self.assertEqual(cs.operations[1].ids, "$1")

    def test_content_id_usable_as_record_id_in_delete(self):
        """Content-ID reference can be used as record_id for delete."""
        cs = _ChangeSet()
        ref = cs.add_create("temp", {"name": "Delete me"})
        cs.add_delete("temp", ref)
        self.assertEqual(cs.operations[1].ids, "$1")


# ---------------------------------------------------------------------------
# 14. Intent type validation
# ---------------------------------------------------------------------------


class TestIntentValidation(unittest.TestCase):
    """_resolve_item rejects unknown types."""

    def test_unknown_type_raises_validation_error(self):
        """An unsupported item type raises ValidationError."""
        od = _make_od()
        client = _BatchClient(od)

        with self.assertRaises(ValidationError):
            client._resolve_item("not a valid intent type")

    def test_none_item_raises_validation_error(self):
        """None as an item type raises ValidationError."""
        od = _make_od()
        client = _BatchClient(od)

        with self.assertRaises(ValidationError):
            client._resolve_item(None)


# ---------------------------------------------------------------------------
# 15. Batch boundary format
# ---------------------------------------------------------------------------


class TestBatchBoundaryFormat(unittest.TestCase):
    """Boundary identifiers should be unique and follow batch_ prefix convention."""

    def test_batch_boundary_in_content_type(self):
        """execute() sets Content-Type with batch_ prefixed boundary."""
        od = _make_od()
        client = _BatchClient(od)

        items = [_RecordGet(table="account", record_id="guid-1")]
        od._build_get.return_value = _RawRequest(method="GET", url="https://org/api/data/v9.2/accounts(guid-1)")

        mock_resp = MagicMock()
        mock_resp.headers = {"Content-Type": 'multipart/mixed; boundary="resp_bnd"'}
        mock_resp.text = "--resp_bnd--\r\n"
        od._request.return_value = mock_resp

        client.execute(items)

        # Verify the Content-Type header sent in the POST
        call_kwargs = od._request.call_args
        headers = call_kwargs.kwargs.get("headers", {})
        ct = headers.get("Content-Type", "")
        self.assertIn("multipart/mixed", ct)
        self.assertIn("batch_", ct)


if __name__ == "__main__":
    unittest.main()
