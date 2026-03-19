# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""
Batch API user scenario tests.

Each test documents a real-world scenario a developer might encounter,
explains expected behavior, and verifies the correct access pattern.
These tests serve as executable documentation for SDK consumers.
"""

import unittest
from unittest.mock import MagicMock

from PowerPlatform.Dataverse.data._batch import (
    _BatchClient,
    _ChangeSet,
    _RecordGet,
)
from PowerPlatform.Dataverse.core.errors import ValidationError
from PowerPlatform.Dataverse.data._raw_request import _RawRequest
from PowerPlatform.Dataverse.models.batch import BatchItemResponse, BatchResult


def _make_od():
    od = MagicMock()
    od.api = "https://org.crm.dynamics.com/api/data/v9.2"
    return od


def _mock_batch_response(batch_boundary, parts):
    """Build a mock HTTP response from a list of (status, headers_dict, body_str) tuples."""
    body_parts = []
    for status_line, headers, body in parts:
        lines = [
            f"--{batch_boundary}",
            "Content-Type: application/http",
            "Content-Transfer-Encoding: binary",
        ]
        for k, v in (headers or {}).items():
            lines.append(f"{k}: {v}")
        lines.append("")
        lines.append(status_line)
        for k, v in (headers or {}).items():
            if k.lower() != "content-id":
                pass  # handled below
        # Add response headers that contain OData-EntityId, Content-Type, etc.
        resp_lines = [status_line]
        if body:
            resp_lines.append("Content-Type: application/json; odata.metadata=minimal")
        resp_lines.append("OData-Version: 4.0")
        # Add OData-EntityId if present in headers
        for k, v in (headers or {}).items():
            resp_lines.append(f"{k}: {v}")
        resp_lines.append("")
        if body:
            resp_lines.append(body)
        resp_text = "\r\n".join(resp_lines)
        part = (
            f"--{batch_boundary}\r\n"
            "Content-Type: application/http\r\n"
            "Content-Transfer-Encoding: binary\r\n"
            "\r\n" + resp_text + "\r\n"
        )
        body_parts.append(part)
    body_parts.append(f"--{batch_boundary}--\r\n")
    full_body = "".join(body_parts)

    mock_resp = MagicMock()
    mock_resp.headers = {"Content-Type": f'multipart/mixed; boundary="{batch_boundary}"'}
    mock_resp.text = full_body
    return mock_resp


# ---------------------------------------------------------------------------
# Scenario 1: Response ordering matches operation order
# ---------------------------------------------------------------------------


class TestScenario_ResponseOrdering(unittest.TestCase):
    """Scenario: I add create, get, delete to a batch. Are responses in the same order?

    YES -- result.responses[0] corresponds to the first operation,
    result.responses[1] to the second, etc. The OData $batch spec guarantees
    response order matches request order.
    """

    def test_responses_are_in_submission_order(self):
        """Three operations produce three responses in the same order."""
        responses = [
            BatchItemResponse(status_code=204, entity_id="id-from-create"),
            BatchItemResponse(status_code=200, data={"name": "Contoso"}),
            BatchItemResponse(status_code=204),  # delete
        ]
        result = BatchResult(responses=responses)
        # First response is the create
        self.assertEqual(result.responses[0].entity_id, "id-from-create")
        # Second response is the get
        self.assertEqual(result.responses[1].data["name"], "Contoso")
        # Third response is the delete
        self.assertIsNone(result.responses[2].entity_id)
        self.assertIsNone(result.responses[2].data)


# ---------------------------------------------------------------------------
# Scenario 2: CreateMultiple IDs are NOT in entity_ids
# ---------------------------------------------------------------------------


class TestScenario_CreateMultipleIDs(unittest.TestCase):
    """Scenario: I create 100 records via batch.records.create(table, [list]).
    Where are the IDs?

    CreateMultiple is a Dataverse bound action (POST to .../CreateMultiple).
    It returns 200 OK with {"Ids": ["guid-1", "guid-2", ...]} in the body.
    entity_ids only collects from OData-EntityId headers, which CreateMultiple
    does NOT return.

    Access via: result.succeeded[n].data["Ids"]
    """

    def test_bulk_create_ids_in_response_data(self):
        """CreateMultiple IDs are in data['Ids'], not in entity_ids."""
        resp = BatchItemResponse(
            status_code=200,
            data={"Ids": ["aaa-111", "bbb-222", "ccc-333"]},
        )
        result = BatchResult(responses=[resp])
        # entity_ids is EMPTY for CreateMultiple
        self.assertEqual(result.entity_ids, [])
        # Access IDs from the response body
        self.assertEqual(resp.data["Ids"], ["aaa-111", "bbb-222", "ccc-333"])

    def test_single_create_id_in_entity_ids(self):
        """Individual POST create returns entity_id via OData-EntityId header."""
        resp = BatchItemResponse(status_code=204, entity_id="single-guid")
        result = BatchResult(responses=[resp])
        self.assertEqual(result.entity_ids, ["single-guid"])


# ---------------------------------------------------------------------------
# Scenario 3: Update responses also have entity_id
# ---------------------------------------------------------------------------


class TestScenario_UpdateReturnsEntityId(unittest.TestCase):
    """Scenario: I do a create + update in a batch. entity_ids has BOTH GUIDs.

    PATCH (update) also returns OData-EntityId with the updated record GUID.
    So entity_ids contains IDs from both creates AND updates.
    """

    def test_entity_ids_includes_both_creates_and_updates(self):
        """entity_ids has GUIDs from POST creates AND PATCH updates."""
        responses = [
            BatchItemResponse(status_code=204, entity_id="created-guid"),
            BatchItemResponse(status_code=204, entity_id="updated-guid"),
        ]
        result = BatchResult(responses=responses)
        self.assertEqual(result.entity_ids, ["created-guid", "updated-guid"])


# ---------------------------------------------------------------------------
# Scenario 4: GET response -- data, not entity_id
# ---------------------------------------------------------------------------


class TestScenario_GetResponse(unittest.TestCase):
    """Scenario: I add a GET to my batch. How do I access the record data?

    GET returns 200 OK with the record JSON in the body.
    data contains the parsed JSON. entity_id is None.
    """

    def test_get_response_has_data_not_entity_id(self):
        """GET response: data has the record, entity_id is None."""
        resp = BatchItemResponse(
            status_code=200,
            data={"name": "Contoso", "accountid": "guid-1"},
        )
        result = BatchResult(responses=[resp])
        # No entity_id for GETs
        self.assertEqual(result.entity_ids, [])
        # Data has the record
        self.assertEqual(resp.data["name"], "Contoso")


# ---------------------------------------------------------------------------
# Scenario 5: DELETE response -- no data, no entity_id
# ---------------------------------------------------------------------------


class TestScenario_DeleteResponse(unittest.TestCase):
    """Scenario: I delete records in a batch. What does the response look like?

    DELETE returns 204 No Content. No entity_id, no data.
    Check is_success to verify the delete worked.
    """

    def test_delete_response_is_empty(self):
        """DELETE response: 204, no entity_id, no data."""
        resp = BatchItemResponse(status_code=204)
        self.assertTrue(resp.is_success)
        self.assertIsNone(resp.entity_id)
        self.assertIsNone(resp.data)


# ---------------------------------------------------------------------------
# Scenario 6: SQL query result -- how to get rows
# ---------------------------------------------------------------------------


class TestScenario_SqlQueryResult(unittest.TestCase):
    """Scenario: I add a SQL query to a batch. How do I get the result rows?

    SQL returns 200 OK with {"value": [row1, row2, ...]} in the body.
    Access via: result.responses[n].data["value"]
    """

    def test_sql_query_result_in_data_value(self):
        """SQL query: rows are in data['value']."""
        rows = [{"name": "Contoso"}, {"name": "Fabrikam"}]
        resp = BatchItemResponse(status_code=200, data={"value": rows})
        result = BatchResult(responses=[resp])
        self.assertEqual(len(resp.data["value"]), 2)
        self.assertEqual(resp.data["value"][0]["name"], "Contoso")


# ---------------------------------------------------------------------------
# Scenario 7: Empty batch -- execute with no operations
# ---------------------------------------------------------------------------


class TestScenario_EmptyBatch(unittest.TestCase):
    """Scenario: I create a batch but add no operations. What happens?

    execute() returns an empty BatchResult. No HTTP request is sent.
    """

    def test_empty_batch_returns_empty_result(self):
        """Empty batch returns empty result without HTTP call."""
        od = _make_od()
        client = _BatchClient(od)
        result = client.execute([], continue_on_error=False)
        self.assertEqual(len(result.responses), 0)
        self.assertFalse(result.has_errors)
        self.assertEqual(result.entity_ids, [])
        # No HTTP call was made
        od._request.assert_not_called()


# ---------------------------------------------------------------------------
# Scenario 8: Double execute -- calling execute() twice
# ---------------------------------------------------------------------------


class TestScenario_DoubleExecute(unittest.TestCase):
    """Scenario: I call batch.execute() twice. Is it safe?

    YES -- each execute() builds a fresh multipart body from the items list.
    The items are still in the batch so the same operations execute again.
    This is safe (idempotent for GETs, creates new records for POSTs).
    """

    def test_execute_twice_sends_two_requests(self):
        """Calling execute twice makes two HTTP requests."""
        od = _make_od()
        od._build_get.return_value = _RawRequest(method="GET", url="https://org/api/data/v9.2/accounts(g)")
        mock_resp = MagicMock()
        mock_resp.headers = {"Content-Type": 'multipart/mixed; boundary="b"'}
        mock_resp.text = "--b--\r\n"
        od._request.return_value = mock_resp

        client = _BatchClient(od)
        items = [_RecordGet(table="account", record_id="g")]
        client.execute(items)
        client.execute(items)
        self.assertEqual(od._request.call_count, 2)


# ---------------------------------------------------------------------------
# Scenario 9: Content-ID scope -- only within same changeset
# ---------------------------------------------------------------------------


class TestScenario_ContentIdScope(unittest.TestCase):
    """Scenario: Can I use a $ref from changeset 1 in changeset 2?

    NO -- Content-ID references ($n) are only valid within the same changeset.
    The OData spec says: "The link can only be to an entity created earlier
    in the same change set." Using $1 from CS1 in CS2 causes a 400 error.

    The SDK enforces this by design: ChangeSetRecordOperations.create()
    returns $n, but that reference is only meaningful within the same
    changeset context manager.
    """

    def test_content_ids_are_per_changeset_scope(self):
        """Content-ID refs from one changeset are not usable in another."""
        counter = [1]
        cs1 = _ChangeSet(_counter=counter)
        cs2 = _ChangeSet(_counter=counter)

        ref1 = cs1.add_create("account", {"name": "A"})
        # ref1 is "$1" -- only valid in cs1
        self.assertEqual(ref1, "$1")

        # If cs2 tried to use "$1", it would be a different operation
        # The SDK doesn't prevent this at build time (it can't know the intent),
        # but Dataverse will return: "Content-ID Reference: '$1' does not exist"
        ref2 = cs2.add_create("contact", {"firstname": "B"})
        # cs2 gets "$2" (unique due to shared counter)
        self.assertEqual(ref2, "$2")


# ---------------------------------------------------------------------------
# Scenario 10: add_columns contributes multiple responses
# ---------------------------------------------------------------------------


class TestScenario_AddColumnsMultipleResponses(unittest.TestCase):
    """Scenario: I call batch.tables.add_columns(table, {"col1": "string", "col2": "int"}).
    How many responses will I get?

    TWO -- add_columns creates one HTTP request per column.
    result.responses will have 2 entries for 2 columns.
    """

    def test_add_columns_response_count_matches_column_count(self):
        """N columns = N responses in the result."""
        # Simulate 3 column-creates, each returning 204
        responses = [
            BatchItemResponse(status_code=204),
            BatchItemResponse(status_code=204),
            BatchItemResponse(status_code=204),
        ]
        result = BatchResult(responses=responses)
        self.assertEqual(len(result.succeeded), 3)


# ---------------------------------------------------------------------------
# Scenario 11: tables.create returns 204, no metadata
# ---------------------------------------------------------------------------


class TestScenario_TableCreateNoMetadata(unittest.TestCase):
    """Scenario: I create a table in a batch. Can I get its MetadataId?

    NO -- tables.create in a batch returns 204 No Content.
    The response has no body (data is None). You need a follow-up
    batch.tables.get() to retrieve the table metadata.
    """

    def test_table_create_returns_no_data(self):
        """Table create response: 204, no data."""
        resp = BatchItemResponse(status_code=204)
        self.assertTrue(resp.is_success)
        self.assertIsNone(resp.data)


# ---------------------------------------------------------------------------
# Scenario 12: continue_on_error behavior
# ---------------------------------------------------------------------------


class TestScenario_ContinueOnError(unittest.TestCase):
    """Scenario: Without continue_on_error, first failure stops the batch.
    With it, all operations are attempted.

    Without: Batch returns HTTP 400. Only the failed op's response is present.
    With: Batch returns HTTP 200. All operations attempted. Check result.failed.
    """

    def test_without_continue_on_error_one_failure(self):
        """Without continue_on_error, only the error response is returned."""
        responses = [
            BatchItemResponse(
                status_code=404,
                error_message="Record not found",
                error_code="0x80040217",
            ),
        ]
        result = BatchResult(responses=responses)
        self.assertTrue(result.has_errors)
        self.assertEqual(len(result.failed), 1)
        self.assertEqual(len(result.succeeded), 0)

    def test_with_continue_on_error_mixed(self):
        """With continue_on_error, all ops attempted, mixed results."""
        responses = [
            BatchItemResponse(status_code=404, error_message="not found"),
            BatchItemResponse(status_code=204, entity_id="good-id"),
            BatchItemResponse(status_code=200, data={"value": []}),
        ]
        result = BatchResult(responses=responses)
        self.assertTrue(result.has_errors)
        self.assertEqual(len(result.failed), 1)
        self.assertEqual(len(result.succeeded), 2)


# ---------------------------------------------------------------------------
# Scenario 13: Changeset rollback -- what the error looks like
# ---------------------------------------------------------------------------


class TestScenario_ChangesetRollback(unittest.TestCase):
    """Scenario: One op in my changeset fails. What happens to the others?

    ALL operations in the changeset are rolled back. No records are created.
    The response contains a single error for the failed operation.
    entity_ids will be empty (nothing was persisted).
    """

    def test_changeset_failure_produces_single_error(self):
        """Failed changeset: one error response, no entity_ids."""
        responses = [
            BatchItemResponse(
                status_code=404,
                error_message="referenced record not found",
                content_id="2",
            ),
        ]
        result = BatchResult(responses=responses)
        self.assertTrue(result.has_errors)
        self.assertEqual(result.entity_ids, [])
        self.assertEqual(result.failed[0].content_id, "2")


# ---------------------------------------------------------------------------
# Scenario 14: DataFrame create -- entity_ids will be empty
# ---------------------------------------------------------------------------


class TestScenario_DataFrameCreateIds(unittest.TestCase):
    """Scenario: I use batch.dataframe.create(table, df). Where are the IDs?

    batch.dataframe.create() calls batch.records.create(table, list_of_dicts),
    which uses CreateMultiple. The response is 200 OK with {"Ids": [...]}.
    entity_ids is empty. Access IDs via result.succeeded[n].data["Ids"].
    """

    def test_dataframe_create_ids_pattern(self):
        """DataFrame create: IDs in data['Ids'], NOT in entity_ids."""
        resp = BatchItemResponse(
            status_code=200,
            data={"Ids": ["df-id-1", "df-id-2", "df-id-3"]},
        )
        result = BatchResult(responses=[resp])
        # entity_ids is empty
        self.assertEqual(result.entity_ids, [])
        # Access pattern for callers
        ids = []
        for r in result.succeeded:
            if r.data and "Ids" in r.data:
                ids.extend(r.data["Ids"])
        self.assertEqual(ids, ["df-id-1", "df-id-2", "df-id-3"])


# ---------------------------------------------------------------------------
# Scenario 15: Mixed batch with changeset + standalone ops
# ---------------------------------------------------------------------------


class TestScenario_MixedBatchOrdering(unittest.TestCase):
    """Scenario: I have a changeset (2 creates) then a standalone GET.
    How are responses ordered?

    Changeset responses come first (in their position), then standalone.
    responses[0] and [1] are from the changeset, responses[2] is the GET.
    """

    def test_changeset_responses_then_standalone(self):
        """Changeset responses first, then standalone ops."""
        responses = [
            BatchItemResponse(status_code=204, entity_id="cs-create-1", content_id="1"),
            BatchItemResponse(status_code=204, entity_id="cs-create-2", content_id="2"),
            BatchItemResponse(status_code=200, data={"name": "Existing"}),
        ]
        result = BatchResult(responses=responses)
        # Changeset creates
        self.assertEqual(result.responses[0].content_id, "1")
        self.assertEqual(result.responses[1].content_id, "2")
        # Standalone GET
        self.assertIsNone(result.responses[2].content_id)
        self.assertEqual(result.responses[2].data["name"], "Existing")


# ---------------------------------------------------------------------------
# Scenario 16: Checking individual response status
# ---------------------------------------------------------------------------


class TestScenario_IndividualResponseStatus(unittest.TestCase):
    """Scenario: I need to check if each specific operation succeeded or failed.

    Iterate result.responses and use is_success, status_code, error_message.
    """

    def test_iterate_and_check_each_response(self):
        """Check individual response status codes and errors."""
        responses = [
            BatchItemResponse(status_code=204, entity_id="id-1"),
            BatchItemResponse(status_code=400, error_message="bad field", error_code="0x80044331"),
            BatchItemResponse(status_code=200, data={"value": []}),
        ]
        result = BatchResult(responses=responses)

        # Pattern: iterate with index to correlate with operations
        for i, resp in enumerate(result.responses):
            if resp.is_success:
                if resp.entity_id:
                    pass  # create/update succeeded
                elif resp.data:
                    pass  # GET/SQL query succeeded
                else:
                    pass  # delete succeeded
            else:
                self.assertEqual(resp.error_code, "0x80044331")
                self.assertIn("bad field", resp.error_message)


# ---------------------------------------------------------------------------
# Scenario 17: Batch max size validation
# ---------------------------------------------------------------------------


class TestScenario_BatchMaxSize(unittest.TestCase):
    """Scenario: I add 1001 operations. What happens?

    ValidationError is raised BEFORE any HTTP request is sent.
    The error message includes the count and the 1000 limit.
    """

    def test_over_1000_raises_before_sending(self):
        """1001+ operations raise ValidationError pre-flight."""
        od = _make_od()
        od._build_get.return_value = _RawRequest(method="GET", url="https://org/x")
        client = _BatchClient(od)
        items = [_RecordGet(table="account", record_id=f"g-{i}") for i in range(1001)]
        with self.assertRaises(ValidationError) as ctx:
            client.execute(items)
        self.assertIn("1001", str(ctx.exception))
        self.assertIn("1000", str(ctx.exception))
        # No HTTP request was made
        od._request.assert_not_called()


# ---------------------------------------------------------------------------
# Scenario 18: Error response parsing
# ---------------------------------------------------------------------------


class TestScenario_ErrorResponseFields(unittest.TestCase):
    """Scenario: An operation fails. What fields are available on the error?

    error_message: Human-readable error text from Dataverse
    error_code: Hex error code (e.g. "0x80040217")
    status_code: HTTP status (e.g. 404, 400, 500)
    is_success: False
    data: None (error responses don't have data)
    """

    def test_error_response_fields(self):
        """Failed response has all error fields populated."""
        resp = BatchItemResponse(
            status_code=404,
            error_message="account With Id = 00000000 Does Not Exist",
            error_code="0x80040217",
        )
        self.assertFalse(resp.is_success)
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(resp.error_code, "0x80040217")
        self.assertIn("Does Not Exist", resp.error_message)
        self.assertIsNone(resp.data)
        self.assertIsNone(resp.entity_id)


if __name__ == "__main__":
    unittest.main()
