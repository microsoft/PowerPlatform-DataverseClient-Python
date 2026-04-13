# Copyright (c) Microsoft Corporation.
# Licensed under the MIT license.

"""Comprehensive tests for _create_multiple / _update_multiple / _upsert_multiple
client-side chunking.
"""

import unittest
from unittest.mock import MagicMock

import time

from PowerPlatform.Dataverse.data._odata import _MULTIPLE_BATCH_SIZE, _ODataClient
from PowerPlatform.Dataverse.models.upsert import UpsertItem

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_odata_client() -> _ODataClient:
    """Return an _ODataClient with all HTTP calls mocked."""
    mock_auth = MagicMock()
    mock_auth._acquire_token.return_value = MagicMock(access_token="token")
    client = _ODataClient(mock_auth, "https://org.crm.dynamics.com")
    client._request = MagicMock()
    client._convert_labels_to_ints = MagicMock(side_effect=lambda _t, r: r)
    return client


def _mock_create_response(ids):
    """Mock HTTP response returning {"Ids": ids}."""
    resp = MagicMock()
    resp.text = "x"
    resp.json.return_value = {"Ids": ids}
    return resp


def _mock_update_response():
    """Mock HTTP response for UpdateMultiple."""
    resp = MagicMock()
    resp.text = ""
    return resp


# ---------------------------------------------------------------------------
# _create_multiple
# ---------------------------------------------------------------------------


class TestCreateMultiple(unittest.TestCase):
    """_create_multiple: chunking boundaries, chunk payloads, ID aggregation, and input validation."""

    def setUp(self):
        self.od = _make_odata_client()
        self.od._execute_raw = MagicMock(return_value=_mock_create_response([]))

    def _run(self, n, side_effects=None):
        """Call _create_multiple with n records; side_effects controls responses."""
        records = [{"name": f"R{i}"} for i in range(n)]
        if side_effects is None:
            side_effects = [_mock_create_response([f"id-{i}" for i in range(n)])]
        self.od._execute_raw = MagicMock(side_effect=side_effects)
        return self.od._create_multiple("accounts", "account", records)

    # --- boundaries ---

    def test_zero_records_no_request(self):
        """Empty list produces zero chunks so no request is sent."""
        result = self.od._create_multiple("accounts", "account", [])
        self.od._execute_raw.assert_not_called()
        self.assertEqual(result, [])

    def test_one_record_single_request(self):
        """Single record produces one request and one ID returned."""
        result = self._run(1, [_mock_create_response(["id-0"])])
        self.od._execute_raw.assert_called_once()
        self.assertEqual(result, ["id-0"])

    def test_batch_minus_one_single_request(self):
        """_MULTIPLE_BATCH_SIZE-1 records fit in one chunk."""
        ids = [f"id-{i}" for i in range(_MULTIPLE_BATCH_SIZE - 1)]
        result = self._run(_MULTIPLE_BATCH_SIZE - 1, [_mock_create_response(ids)])
        self.od._execute_raw.assert_called_once()
        self.assertEqual(len(result), _MULTIPLE_BATCH_SIZE - 1)

    def test_exact_batch_size_single_request(self):
        """Exactly _MULTIPLE_BATCH_SIZE records produces one chunk and one request."""
        ids = [f"id-{i}" for i in range(_MULTIPLE_BATCH_SIZE)]
        result = self._run(_MULTIPLE_BATCH_SIZE, [_mock_create_response(ids)])
        self.od._execute_raw.assert_called_once()
        self.assertEqual(len(result), _MULTIPLE_BATCH_SIZE)

    def test_batch_plus_one_two_requests(self):
        """_MULTIPLE_BATCH_SIZE+1 records produces two chunks and two requests."""
        ids1 = [f"id-{i}" for i in range(_MULTIPLE_BATCH_SIZE)]
        ids2 = ["id-last"]
        result = self._run(_MULTIPLE_BATCH_SIZE + 1, [_mock_create_response(ids1), _mock_create_response(ids2)])
        self.assertEqual(self.od._execute_raw.call_count, 2)
        self.assertEqual(len(result), _MULTIPLE_BATCH_SIZE + 1)

    def test_two_full_batches(self):
        """2*_MULTIPLE_BATCH_SIZE records produces two full chunks."""
        ids1 = [f"id-{i}" for i in range(_MULTIPLE_BATCH_SIZE)]
        ids2 = [f"id-{i}" for i in range(_MULTIPLE_BATCH_SIZE, 2 * _MULTIPLE_BATCH_SIZE)]
        result = self._run(2 * _MULTIPLE_BATCH_SIZE, [_mock_create_response(ids1), _mock_create_response(ids2)])
        self.assertEqual(self.od._execute_raw.call_count, 2)
        self.assertEqual(len(result), 2 * _MULTIPLE_BATCH_SIZE)

    def test_two_batches_plus_one(self):
        """2*_MULTIPLE_BATCH_SIZE+1 records produces three chunks."""
        se = [_mock_create_response([f"id-{j}" for j in range(_MULTIPLE_BATCH_SIZE)]) for _ in range(2)]
        se.append(_mock_create_response(["id-extra"]))
        result = self._run(2 * _MULTIPLE_BATCH_SIZE + 1, se)
        self.assertEqual(self.od._execute_raw.call_count, 3)
        self.assertEqual(len(result), 2 * _MULTIPLE_BATCH_SIZE + 1)

    # --- chunk payloads ---

    def test_first_chunk_has_batch_size_records(self):
        """The first chunk sent to the server has exactly _MULTIPLE_BATCH_SIZE records."""
        records = [{"name": f"R{i}"} for i in range(_MULTIPLE_BATCH_SIZE + 50)]
        captured = []

        original_build = self.od._build_create_multiple

        def capturing_build(entity_set, table, chunk):
            captured.append(len(chunk))
            return original_build(entity_set, table, chunk)

        self.od._build_create_multiple = capturing_build
        self.od._create_multiple("accounts", "account", records)

        self.assertEqual(captured[0], _MULTIPLE_BATCH_SIZE)
        self.assertEqual(captured[1], 50)

    def test_chunks_cover_all_records_no_overlap(self):
        """All input records appear in exactly one chunk, no duplicates or gaps."""
        n = _MULTIPLE_BATCH_SIZE + 7
        records = [{"name": f"R{i}", "idx": i} for i in range(n)]
        seen_indices = []

        original_build = self.od._build_create_multiple

        def capturing_build(entity_set, table, chunk):
            seen_indices.extend(r["idx"] for r in chunk)
            return original_build(entity_set, table, chunk)

        self.od._build_create_multiple = capturing_build
        self.od._create_multiple("accounts", "account", records)

        self.assertEqual(sorted(seen_indices), list(range(n)))

    def test_chunk_record_order_preserved(self):
        """Records appear within each chunk in the same relative order as the original input."""
        n = _MULTIPLE_BATCH_SIZE + 3
        records = [{"name": f"R{i}", "seq": i} for i in range(n)]
        sent_seq = []
        original_build = self.od._build_create_multiple

        def capturing_build(entity_set, table, chunk):
            sent_seq.extend(r["seq"] for r in chunk)
            return original_build(entity_set, table, chunk)

        self.od._build_create_multiple = capturing_build
        self.od._create_multiple("accounts", "account", records)

        self.assertEqual(sent_seq, list(range(n)))

    # --- ID aggregation ---

    def test_sequential_ids_preserved_in_order(self):
        """Sequential dispatch preserves chunk order in the returned IDs."""
        ids1 = ["a1", "a2"]
        ids2 = ["b1"]
        self.od._execute_raw = MagicMock(side_effect=[_mock_create_response(ids1), _mock_create_response(ids2)])
        result = self.od._create_multiple(
            "accounts", "account", [{"name": f"R{i}"} for i in range(_MULTIPLE_BATCH_SIZE + 1)]
        )
        self.assertEqual(result, ["a1", "a2", "b1"])

    def test_empty_ids_from_chunk_still_aggregated(self):
        """A chunk returning [] does not cause errors; its contribution is simply empty."""
        self.od._execute_raw = MagicMock(side_effect=[_mock_create_response([]), _mock_create_response(["id-1"])])
        result = self.od._create_multiple(
            "accounts", "account", [{"name": f"R{i}"} for i in range(_MULTIPLE_BATCH_SIZE + 1)]
        )
        self.assertEqual(result, ["id-1"])

    def test_value_fallback_response_still_aggregated(self):
        """Chunks whose response uses the 'value' key instead of 'Ids' are handled."""
        resp1 = MagicMock()
        resp1.text = "x"
        resp1.json.return_value = {"Ids": ["a1"]}

        resp2 = MagicMock()
        resp2.text = "x"
        # Simulate alternative response shape (no 'Ids', has 'value')
        resp2.json.return_value = {"value": []}

        self.od._execute_raw = MagicMock(side_effect=[resp1, resp2])
        result = self.od._create_multiple(
            "accounts", "account", [{"name": f"R{i}"} for i in range(_MULTIPLE_BATCH_SIZE + 1)]
        )
        self.assertEqual(result, ["a1"])

    # --- type validation ---

    def test_non_dict_in_list_raises_type_error(self):
        """A non-dict element raises TypeError before any HTTP call."""
        with self.assertRaises(TypeError):
            self.od._create_multiple("accounts", "account", [{"name": "ok"}, "bad"])
        self.od._execute_raw.assert_not_called()

    def test_non_list_input_raises_type_error(self):
        """A single dict (not wrapped in a list) raises TypeError before any HTTP call."""
        with self.assertRaises(TypeError):
            self.od._create_multiple("accounts", "account", {"name": "X"})
        self.od._execute_raw.assert_not_called()


# ---------------------------------------------------------------------------
# _update_multiple
# ---------------------------------------------------------------------------


class TestUpdateMultiple(unittest.TestCase):
    """_update_multiple: chunking boundaries, chunk payloads, and input validation."""

    def setUp(self):
        self.od = _make_odata_client()
        self.od._execute_raw = MagicMock(return_value=_mock_update_response())

    def _records(self, n):
        return [{"accountid": f"id-{i}", "name": f"N{i}"} for i in range(n)]

    # --- boundaries ---

    def test_one_record_single_request(self):
        """Single record produces one request."""
        self.od._update_multiple("accounts", "account", self._records(1))
        self.od._execute_raw.assert_called_once()

    def test_batch_minus_one_single_request(self):
        """_MULTIPLE_BATCH_SIZE-1 records fit in one chunk."""
        self.od._update_multiple("accounts", "account", self._records(_MULTIPLE_BATCH_SIZE - 1))
        self.od._execute_raw.assert_called_once()

    def test_exact_batch_single_request(self):
        """Exactly _MULTIPLE_BATCH_SIZE records produces one chunk and one request."""
        self.od._update_multiple("accounts", "account", self._records(_MULTIPLE_BATCH_SIZE))
        self.od._execute_raw.assert_called_once()

    def test_batch_plus_one_two_requests(self):
        """_MULTIPLE_BATCH_SIZE+1 records produces two chunks and two requests."""
        self.od._update_multiple("accounts", "account", self._records(_MULTIPLE_BATCH_SIZE + 1))
        self.assertEqual(self.od._execute_raw.call_count, 2)

    def test_two_full_batches(self):
        """2*_MULTIPLE_BATCH_SIZE records produces two full chunks."""
        self.od._update_multiple("accounts", "account", self._records(2 * _MULTIPLE_BATCH_SIZE))
        self.assertEqual(self.od._execute_raw.call_count, 2)

    def test_two_batches_plus_one(self):
        """2*_MULTIPLE_BATCH_SIZE+1 records produces three chunks."""
        self.od._update_multiple("accounts", "account", self._records(2 * _MULTIPLE_BATCH_SIZE + 1))
        self.assertEqual(self.od._execute_raw.call_count, 3)

    # --- chunk payloads ---

    def test_first_chunk_size_is_batch_size(self):
        """First chunk has exactly _MULTIPLE_BATCH_SIZE records."""
        n = _MULTIPLE_BATCH_SIZE + 17
        records = [{"accountid": f"id-{i}", "name": f"N{i}"} for i in range(n)]
        captured = []
        original = self.od._build_update_multiple_from_records

        def capturing(entity_set, table, chunk):
            captured.append(len(chunk))
            return original(entity_set, table, chunk)

        self.od._build_update_multiple_from_records = capturing
        self.od._update_multiple("accounts", "account", records)
        self.assertEqual(captured[0], _MULTIPLE_BATCH_SIZE)
        self.assertEqual(captured[1], 17)

    def test_records_are_not_duplicated_or_dropped(self):
        """All input IDs appear exactly once across all chunks."""
        n = _MULTIPLE_BATCH_SIZE + 3
        records = [{"accountid": f"id-{i}", "name": f"N{i}"} for i in range(n)]
        seen_ids = []
        original = self.od._build_update_multiple_from_records

        def capturing(entity_set, table, chunk):
            seen_ids.extend(r["accountid"] for r in chunk)
            return original(entity_set, table, chunk)

        self.od._build_update_multiple_from_records = capturing
        self.od._update_multiple("accounts", "account", records)
        self.assertEqual(sorted(seen_ids), sorted(f"id-{i}" for i in range(n)))

    def test_chunk_record_order_preserved(self):
        """Records appear within each chunk in the same relative order as the original input."""
        n = _MULTIPLE_BATCH_SIZE + 5
        records = [{"accountid": f"id-{i}", "name": f"N{i}"} for i in range(n)]
        sent_ids = []
        original = self.od._build_update_multiple_from_records

        def capturing(entity_set, table, chunk):
            sent_ids.extend(r["accountid"] for r in chunk)
            return original(entity_set, table, chunk)

        self.od._build_update_multiple_from_records = capturing
        self.od._update_multiple("accounts", "account", records)

        self.assertEqual(sent_ids, [f"id-{i}" for i in range(n)])

    # --- type validation ---

    def test_empty_list_raises_type_error(self):
        """Empty list raises TypeError before any HTTP call."""
        with self.assertRaises(TypeError):
            self.od._update_multiple("accounts", "account", [])
        self.od._execute_raw.assert_not_called()

    def test_non_list_raises_type_error(self):
        """Non-list input raises TypeError before any HTTP call."""
        with self.assertRaises(TypeError):
            self.od._update_multiple("accounts", "account", {"accountid": "x"})  # type: ignore
        self.od._execute_raw.assert_not_called()

    def test_non_dict_element_raises_type_error(self):
        """A non-dict element raises TypeError before any HTTP call."""
        with self.assertRaises(TypeError):
            self.od._update_multiple("accounts", "account", [{"accountid": "x"}, "bad"])
        self.od._execute_raw.assert_not_called()


# ---------------------------------------------------------------------------
# _upsert_multiple
# ---------------------------------------------------------------------------


def _alt_keys(n):
    return [{"accountnumber": f"ACC-{i}"} for i in range(n)]


def _upsert_records(n):
    return [{"name": f"N{i}"} for i in range(n)]


class TestUpsertMultiple(unittest.TestCase):
    """_upsert_multiple: chunking boundaries, chunk payloads, and input validation."""

    def setUp(self):
        self.od = _make_odata_client()
        self.od._request.return_value = MagicMock()

    # --- boundaries ---

    def test_one_record_single_request(self):
        """Single record produces one request."""
        self.od._upsert_multiple("accounts", "account", _alt_keys(1), _upsert_records(1))
        self.od._request.assert_called_once()

    def test_batch_minus_one_single_request(self):
        """_MULTIPLE_BATCH_SIZE-1 records fit in one chunk."""
        n = _MULTIPLE_BATCH_SIZE - 1
        self.od._upsert_multiple("accounts", "account", _alt_keys(n), _upsert_records(n))
        self.od._request.assert_called_once()

    def test_exact_batch_single_request(self):
        """Exactly _MULTIPLE_BATCH_SIZE records produces one chunk and one request."""
        self.od._upsert_multiple(
            "accounts", "account", _alt_keys(_MULTIPLE_BATCH_SIZE), _upsert_records(_MULTIPLE_BATCH_SIZE)
        )
        self.od._request.assert_called_once()

    def test_batch_plus_one_two_requests(self):
        """_MULTIPLE_BATCH_SIZE+1 records produces two chunks and two requests."""
        n = _MULTIPLE_BATCH_SIZE + 1
        self.od._upsert_multiple("accounts", "account", _alt_keys(n), _upsert_records(n))
        self.assertEqual(self.od._request.call_count, 2)

    def test_two_full_batches(self):
        """2*_MULTIPLE_BATCH_SIZE records produces two full chunks."""
        n = 2 * _MULTIPLE_BATCH_SIZE
        self.od._upsert_multiple("accounts", "account", _alt_keys(n), _upsert_records(n))
        self.assertEqual(self.od._request.call_count, 2)

    def test_two_batches_plus_one(self):
        """2*_MULTIPLE_BATCH_SIZE+1 records produces three chunks."""
        n = 2 * _MULTIPLE_BATCH_SIZE + 1
        self.od._upsert_multiple("accounts", "account", _alt_keys(n), _upsert_records(n))
        self.assertEqual(self.od._request.call_count, 3)

    def test_concurrent_dispatch_executes_all_chunks(self):
        """max_workers > 1 with multiple chunks dispatches concurrently and executes every chunk."""
        n = _MULTIPLE_BATCH_SIZE + 1  # 2 chunks
        self.od._upsert_multiple("accounts", "account", _alt_keys(n), _upsert_records(n), max_workers=2)
        self.assertEqual(self.od._request.call_count, 2)

    # --- chunk payloads ---

    def test_first_chunk_has_batch_size_targets(self):
        """First POST body has exactly _MULTIPLE_BATCH_SIZE Targets."""
        n = _MULTIPLE_BATCH_SIZE + 10
        self.od._upsert_multiple("accounts", "account", _alt_keys(n), _upsert_records(n))
        first_call = self.od._request.call_args_list[0]
        targets = first_call.kwargs["json"]["Targets"]
        self.assertEqual(len(targets), _MULTIPLE_BATCH_SIZE)

    def test_last_chunk_has_remainder_targets(self):
        """Last POST body has the remainder."""
        remainder = 7
        n = _MULTIPLE_BATCH_SIZE + remainder
        self.od._upsert_multiple("accounts", "account", _alt_keys(n), _upsert_records(n))
        last_call = self.od._request.call_args_list[-1]
        targets = last_call.kwargs["json"]["Targets"]
        self.assertEqual(len(targets), remainder)

    def test_all_targets_sent_no_duplicates(self):
        """All accountnumber values appear exactly once across all chunks."""
        n = _MULTIPLE_BATCH_SIZE + 5
        self.od._upsert_multiple("accounts", "account", _alt_keys(n), _upsert_records(n))
        all_numbers = []
        for c in self.od._request.call_args_list:
            for t in c.kwargs["json"]["Targets"]:
                # @odata.id contains the accountnumber
                all_numbers.append(t.get("@odata.id", ""))
        self.assertEqual(len(all_numbers), n)
        self.assertEqual(len(set(all_numbers)), n)  # no duplicates

    def test_odata_type_injected_in_each_target(self):
        """@odata.type is injected into every target in every chunk."""
        n = _MULTIPLE_BATCH_SIZE + 2
        self.od._upsert_multiple("accounts", "account", _alt_keys(n), _upsert_records(n))
        for c in self.od._request.call_args_list:
            for t in c.kwargs["json"]["Targets"]:
                self.assertIn("@odata.type", t)
                self.assertEqual(t["@odata.type"], "Microsoft.Dynamics.CRM.account")

    def test_post_url_uses_upsert_multiple_action(self):
        """Every chunk is POSTed to the UpsertMultiple action URL."""
        n = _MULTIPLE_BATCH_SIZE + 1
        self.od._upsert_multiple("accounts", "account", _alt_keys(n), _upsert_records(n))
        for c in self.od._request.call_args_list:
            self.assertIn("UpsertMultiple", c.args[1])
            self.assertEqual(c.args[0], "post")

    # --- validation ---

    def test_length_mismatch_raises_value_error(self):
        """Mismatched alternate_keys and records lengths raise ValueError before any HTTP call."""
        with self.assertRaises(ValueError) as ctx:
            self.od._upsert_multiple("accounts", "account", _alt_keys(3), _upsert_records(2))
        self.assertIn("alternate_keys and records must have the same length", str(ctx.exception))
        self.od._request.assert_not_called()

    def test_key_conflict_raises_value_error(self):
        """Conflicting value for a key field in the record payload raises ValueError before any HTTP call."""
        with self.assertRaises(ValueError) as ctx:
            self.od._upsert_multiple(
                "accounts",
                "account",
                [{"accountnumber": "ACC-1"}],
                [{"accountnumber": "ACC-DIFFERENT"}],
            )
        self.assertIn("record payload conflicts with alternate_key", str(ctx.exception))
        self.od._request.assert_not_called()

    def test_large_batch_with_conflict_in_chunk_raises(self):
        """Conflict detection happens before chunking so it catches errors in any position."""
        n = _MULTIPLE_BATCH_SIZE + 1
        alt_keys = _alt_keys(n)
        records = _upsert_records(n)
        # Inject a conflict in the last record (would be in the second chunk)
        records[-1] = {"accountnumber": "WRONG"}
        with self.assertRaises(ValueError):
            self.od._upsert_multiple("accounts", "account", alt_keys, records)
        self.od._request.assert_not_called()


# ---------------------------------------------------------------------------
# _update_by_ids — delegates to _update_multiple
# ---------------------------------------------------------------------------


class TestUpdateByIdsDelegation(unittest.TestCase):
    """_update_by_ids builds the correct batch and delegates to _update_multiple."""

    def setUp(self):
        self.od = _make_odata_client()
        self.od._update_multiple = MagicMock()
        self.od._primary_id_attr = MagicMock(return_value="accountid")
        self.od._entity_set_from_schema_name = MagicMock(return_value="accounts")

    def test_broadcast_delegates_correctly(self):
        """Broadcast mode builds one record per ID and delegates."""
        ids = ["id-1", "id-2"]
        self.od._update_by_ids("account", ids, {"name": "X"})
        self.od._update_multiple.assert_called_once_with(
            "accounts",
            "account",
            [{"accountid": "id-1", "name": "X"}, {"accountid": "id-2", "name": "X"}],
            max_workers=1,
        )

    def test_paired_delegates_correctly(self):
        """Paired mode merges id+patch per record and delegates."""
        ids = ["id-1", "id-2"]
        changes = [{"name": "A"}, {"name": "B"}]
        self.od._update_by_ids("account", ids, changes)
        self.od._update_multiple.assert_called_once_with(
            "accounts",
            "account",
            [{"accountid": "id-1", "name": "A"}, {"accountid": "id-2", "name": "B"}],
            max_workers=1,
        )

    def test_empty_ids_returns_none_without_delegating(self):
        """Empty ids list returns immediately without calling _update_multiple."""
        result = self.od._update_by_ids("account", [], {"name": "X"})
        self.assertIsNone(result)
        self.od._update_multiple.assert_not_called()

    def test_non_list_ids_raises_type_error(self):
        """Non-list ids raises TypeError before any delegation."""
        with self.assertRaises(TypeError):
            self.od._update_by_ids("account", "id-1", {"name": "X"})  # type: ignore
        self.od._update_multiple.assert_not_called()

    def test_changes_non_dict_non_list_raises_type_error(self):
        """changes that is neither dict nor list raises TypeError."""
        with self.assertRaises(TypeError):
            self.od._update_by_ids("account", ["id-1"], "invalid")  # type: ignore
        self.od._update_multiple.assert_not_called()

    def test_changes_list_length_mismatch_raises_value_error(self):
        """Paired changes list with different length from ids raises ValueError."""
        with self.assertRaises(ValueError):
            self.od._update_by_ids("account", ["id-1", "id-2"], [{"name": "A"}])
        self.od._update_multiple.assert_not_called()

    def test_changes_list_non_dict_element_raises_type_error(self):
        """Non-dict element in paired changes list raises TypeError."""
        with self.assertRaises(TypeError):
            self.od._update_by_ids("account", ["id-1", "id-2"], [{"name": "A"}, "bad"])  # type: ignore
        self.od._update_multiple.assert_not_called()

    def test_max_workers_is_forwarded_to_update_multiple(self):
        """max_workers is forwarded to _update_multiple."""
        self.od._update_by_ids("account", ["id-1", "id-2"], {"name": "X"}, max_workers=3)
        self.od._update_multiple.assert_called_once_with(
            "accounts",
            "account",
            [{"accountid": "id-1", "name": "X"}, {"accountid": "id-2", "name": "X"}],
            max_workers=3,
        )


# ---------------------------------------------------------------------------
# Public API: records.create / records.update / records.upsert
# ---------------------------------------------------------------------------


def _make_records_client():
    """Return a DataverseClient with _odata mocked for public API tests."""
    from contextlib import contextmanager

    from PowerPlatform.Dataverse.operations.records import RecordOperations

    mock_client = MagicMock()
    mock_odata = MagicMock()
    mock_odata._entity_set_from_schema_name.return_value = "accounts"
    mock_odata._create_multiple.return_value = ["guid-1", "guid-2"]
    mock_odata._update_by_ids.return_value = None
    mock_odata._upsert_multiple.return_value = None

    @contextmanager
    def scoped():
        yield mock_odata

    mock_client._scoped_odata = scoped
    ops = RecordOperations(mock_client)
    return ops, mock_odata


class TestPublicCreateDelegation(unittest.TestCase):
    """records.create delegates to _create_multiple for list input."""

    def test_list_delegates_to_create_multiple(self):
        """List input routes to _create_multiple, not _create."""
        ops, mock_odata = _make_records_client()
        ops.create("account", [{"name": "A"}, {"name": "B"}])
        mock_odata._create_multiple.assert_called_once_with(
            "accounts", "account", [{"name": "A"}, {"name": "B"}], max_workers=1
        )

    def test_single_dict_delegates_to_create(self):
        """Single-record create calls _create, not _create_multiple."""
        ops, mock_odata = _make_records_client()
        mock_odata._create.return_value = "guid-single"
        ops.create("account", {"name": "A"})
        mock_odata._create_multiple.assert_not_called()
        mock_odata._create.assert_called_once()

    def test_max_workers_is_forwarded(self):
        """max_workers is passed through to _create_multiple."""
        ops, mock_odata = _make_records_client()
        ops.create("account", [{"name": "A"}, {"name": "B"}], max_workers=3)
        mock_odata._create_multiple.assert_called_once_with(
            "accounts", "account", [{"name": "A"}, {"name": "B"}], max_workers=3
        )


class TestPublicUpdateDelegation(unittest.TestCase):
    """records.update delegates to _update_by_ids for list input."""

    def test_list_delegates_to_update_by_ids(self):
        """Broadcast list input routes to _update_by_ids, not _update."""
        ops, mock_odata = _make_records_client()
        ops.update("account", ["id-1", "id-2"], {"name": "X"})
        mock_odata._update_by_ids.assert_called_once_with("account", ["id-1", "id-2"], {"name": "X"}, max_workers=1)

    def test_list_paired_delegates_to_update_by_ids(self):
        """Paired list-of-patches passes through to _update_by_ids unchanged."""
        ops, mock_odata = _make_records_client()
        ops.update("account", ["id-1", "id-2"], [{"name": "A"}, {"name": "B"}])
        mock_odata._update_by_ids.assert_called_once_with(
            "account", ["id-1", "id-2"], [{"name": "A"}, {"name": "B"}], max_workers=1
        )

    def test_single_delegates_to_update(self):
        """Single-record update calls _update, not _update_by_ids."""
        ops, mock_odata = _make_records_client()
        ops.update("account", "id-1", {"name": "X"})
        mock_odata._update_by_ids.assert_not_called()
        mock_odata._update.assert_called_once()

    def test_max_workers_is_forwarded(self):
        """max_workers is passed through to _update_by_ids."""
        ops, mock_odata = _make_records_client()
        ops.update("account", ["id-1", "id-2"], {"name": "X"}, max_workers=3)
        mock_odata._update_by_ids.assert_called_once_with("account", ["id-1", "id-2"], {"name": "X"}, max_workers=3)


class TestPublicUpsertDelegation(unittest.TestCase):
    """records.upsert delegates to _upsert_multiple for multi-item input."""

    def test_multi_item_delegates_to_upsert_multiple(self):
        ops, mock_odata = _make_records_client()
        items = [
            UpsertItem(alternate_key={"accountnumber": "A1"}, record={"name": "Contoso"}),
            UpsertItem(alternate_key={"accountnumber": "A2"}, record={"name": "Fabrikam"}),
        ]
        ops.upsert("account", items)
        mock_odata._upsert_multiple.assert_called_once_with(
            "accounts",
            "account",
            [{"accountnumber": "A1"}, {"accountnumber": "A2"}],
            [{"name": "Contoso"}, {"name": "Fabrikam"}],
            max_workers=1,
        )

    def test_single_item_delegates_to_upsert(self):
        """A single-item list calls _upsert (PATCH), not _upsert_multiple."""
        ops, mock_odata = _make_records_client()
        ops.upsert("account", [UpsertItem(alternate_key={"accountnumber": "A1"}, record={"name": "X"})])
        mock_odata._upsert_multiple.assert_not_called()
        mock_odata._upsert.assert_called_once()

    def test_max_workers_is_forwarded(self):
        """max_workers is passed through to _upsert_multiple."""
        ops, mock_odata = _make_records_client()
        items = [
            UpsertItem(alternate_key={"accountnumber": "A1"}, record={"name": "X"}),
            UpsertItem(alternate_key={"accountnumber": "A2"}, record={"name": "Y"}),
        ]
        ops.upsert("account", items, max_workers=2)
        mock_odata._upsert_multiple.assert_called_once_with(
            "accounts",
            "account",
            [{"accountnumber": "A1"}, {"accountnumber": "A2"}],
            [{"name": "X"}, {"name": "Y"}],
            max_workers=2,
        )


# ---------------------------------------------------------------------------
# _dispatch_chunks: sequential path
# ---------------------------------------------------------------------------


class TestDispatchChunksSequential(unittest.TestCase):
    """_dispatch_chunks with max_workers=1 runs fn synchronously in order."""

    def setUp(self):
        from PowerPlatform.Dataverse.data._odata import _dispatch_chunks

        self._dispatch = _dispatch_chunks

    def test_returns_results_in_order(self):
        results = self._dispatch(lambda c: c[0], [[10], [20], [30]], max_workers=1)
        self.assertEqual(results, [10, 20, 30])

    def test_empty_chunks_returns_empty(self):
        called = []
        self._dispatch(lambda c: called.append(c), [], max_workers=1)
        self.assertEqual(called, [])

    def test_single_chunk_with_high_workers_still_sequential(self):
        """Single chunk should skip ThreadPoolExecutor even when max_workers > 1."""
        call_count = [0]

        def fn(_):
            call_count[0] += 1
            return "result"

        with self.assertWarns(UserWarning):
            results = self._dispatch(fn, [["only"]], max_workers=4)
        self.assertEqual(results, ["result"])
        self.assertEqual(call_count[0], 1)

    def test_exception_propagates_sequential(self):
        def fn(_):
            raise ValueError("boom")

        with self.assertRaises(ValueError):
            self._dispatch(fn, [["a"]], max_workers=1)

    def test_fn_called_once_per_chunk(self):
        """fn is invoked exactly once per chunk on the sequential path."""
        call_count = [0]

        def fn(chunk):
            call_count[0] += 1
            return chunk

        chunks = ["a", "b", "c", "d"]
        self._dispatch(fn, chunks, max_workers=1)
        self.assertEqual(call_count[0], len(chunks))


# ---------------------------------------------------------------------------
# _dispatch_chunks: concurrent path
# ---------------------------------------------------------------------------


class TestDispatchChunksConcurrent(unittest.TestCase):
    """_dispatch_chunks with max_workers > 1 dispatches via ThreadPoolExecutor."""

    def setUp(self):
        from PowerPlatform.Dataverse.data._odata import _dispatch_chunks

        self._dispatch = _dispatch_chunks

    def test_concurrent_results_preserve_submission_order(self):
        """Results are returned in chunk-submission order even when chunks finish out of order.

        Chunk 0 blocks until chunk 1 has signalled completion, so the actual
        completion order is [1, 0].  The returned list must still be
        ["chunk-0", "chunk-1"] (submission order), not ["chunk-1", "chunk-0"]
        (completion order), verifying that ``[f.result() for f in futures]``
        iterates futures in submission order rather than ``as_completed`` order.
        """
        import threading

        chunk_1_done = threading.Event()

        def fn(idx):
            if idx == 0:
                chunk_1_done.wait()  # hold until chunk 1 has finished
                return "chunk-0"
            else:  # idx == 1
                chunk_1_done.set()  # release chunk 0
                return "chunk-1"

        results = self._dispatch(fn, [0, 1], max_workers=2)
        # chunk 1 finished first, but submission order must be preserved
        self.assertEqual(results, ["chunk-0", "chunk-1"])

    def test_all_chunks_are_executed(self):
        executed = []
        lock = __import__("threading").Lock()

        def fn(chunk):
            with lock:
                executed.append(chunk)
            return chunk

        chunks = [["a"], ["b"], ["c"]]
        results = self._dispatch(fn, chunks, max_workers=3)
        self.assertEqual(sorted(executed), [["a"], ["b"], ["c"]])
        self.assertEqual(results, [["a"], ["b"], ["c"]])

    def test_exception_in_worker_propagates(self):
        def fn(chunk):
            if chunk == "bad":
                raise RuntimeError("worker failed")
            return chunk

        with self.assertRaises(RuntimeError):
            self._dispatch(fn, ["ok", "bad", "ok2"], max_workers=3)

    def test_max_workers_above_cap_is_capped(self):
        """ThreadPoolExecutor receives _MAX_WORKERS even when the caller passes a larger value."""
        from concurrent.futures import ThreadPoolExecutor

        from PowerPlatform.Dataverse.data._odata import _MAX_WORKERS

        chunks = list(range(5))
        with unittest.mock.patch(
            "PowerPlatform.Dataverse.data._odata.ThreadPoolExecutor",
            wraps=ThreadPoolExecutor,
        ) as mock_pool:
            with self.assertWarns(UserWarning):
                results = self._dispatch(lambda c: c, chunks, max_workers=_MAX_WORKERS + 100)

        mock_pool.assert_called_once_with(max_workers=_MAX_WORKERS)
        self.assertEqual(results, chunks)

    def test_contextvar_propagated_to_worker_threads(self):
        """Worker threads see the ContextVar set by the calling thread via copy_context."""
        from PowerPlatform.Dataverse.data._odata import _CALL_SCOPE_CORRELATION_ID

        captured = []

        def fn(chunk):
            captured.append(_CALL_SCOPE_CORRELATION_ID.get())
            return chunk

        token = _CALL_SCOPE_CORRELATION_ID.set("test-correlation-id")
        try:
            self._dispatch(fn, ["a", "b"], max_workers=2)
        finally:
            _CALL_SCOPE_CORRELATION_ID.reset(token)

        self.assertEqual(len(captured), 2)
        self.assertTrue(all(c == "test-correlation-id" for c in captured))


# ---------------------------------------------------------------------------
# _dispatch_chunks: transient error retry with jitter
# ---------------------------------------------------------------------------


class TestDispatchChunksTransientRetry(unittest.TestCase):
    """_dispatch_chunks retries on transient errors (429, 502, 503, 504) up to _CHUNK_RETRY_LIMIT times."""

    def setUp(self):
        from PowerPlatform.Dataverse.data._odata import (
            _CHUNK_RETRY_DEFAULT_WAIT,
            _CHUNK_RETRY_JITTER_MAX,
            _CHUNK_RETRY_LIMIT,
            _dispatch_chunks,
        )
        from PowerPlatform.Dataverse.core.errors import HttpError

        self._dispatch = _dispatch_chunks
        self._LIMIT = _CHUNK_RETRY_LIMIT
        self._DEFAULT_WAIT = _CHUNK_RETRY_DEFAULT_WAIT
        self._JITTER_MAX = _CHUNK_RETRY_JITTER_MAX
        self._HttpError = HttpError

    def _make_transient(self, status_code=429, retry_after=None):
        kwargs = {"status_code": status_code, "is_transient": True}
        if retry_after is not None:
            kwargs["retry_after"] = retry_after
        return self._HttpError("transient error", **kwargs)

    def test_retries_on_429_and_eventually_succeeds(self):
        """The bad chunk raises 429 twice then succeeds; sleep is called exactly twice.

        Two chunks are used so ``_dispatch_chunks`` takes the concurrent path.
        Only the "bad" chunk retries; the "ok" chunk completes immediately.
        ``_execute_with_retry`` wraps each chunk in both the sequential and
        concurrent paths.
        """
        bad_calls = [0]
        err = self._make_transient(retry_after=5)

        def fn(chunk):
            if chunk == "bad":
                bad_calls[0] += 1
                if bad_calls[0] < 3:
                    raise err
            return chunk

        with unittest.mock.patch("PowerPlatform.Dataverse.data._odata.time.sleep") as mock_sleep:
            result = self._dispatch(fn, ["bad", "ok"], max_workers=2)

        self.assertEqual(result, ["bad", "ok"])
        self.assertEqual(bad_calls[0], 3)
        self.assertEqual(mock_sleep.call_count, 2)
        # Each sleep value must be >= retry_after and <= retry_after + jitter_max
        for call_args in mock_sleep.call_args_list:
            wait = call_args[0][0]
            self.assertGreaterEqual(wait, 5)
            self.assertLessEqual(wait, 5 + self._JITTER_MAX)

    def test_transient_without_retry_after_uses_default_wait(self):
        """When Retry-After is absent, the sleep falls back to _CHUNK_RETRY_DEFAULT_WAIT.

        Two chunks are required to exercise the concurrent path via ``_execute_with_retry``.
        """
        bad_calls = [0]
        err = self._make_transient(retry_after=None)

        def fn(chunk):
            if chunk == "bad":
                bad_calls[0] += 1
                if bad_calls[0] < 2:
                    raise err
            return chunk

        with unittest.mock.patch("PowerPlatform.Dataverse.data._odata.time.sleep") as mock_sleep:
            self._dispatch(fn, ["bad", "ok"], max_workers=2)

        wait = mock_sleep.call_args[0][0]
        self.assertGreaterEqual(wait, self._DEFAULT_WAIT)
        self.assertLessEqual(wait, self._DEFAULT_WAIT + self._JITTER_MAX)

    def test_exhausts_retries_and_raises(self):
        """After _CHUNK_RETRY_LIMIT retries the transient HttpError is re-raised.

        A single chunk with max_workers=1 gives a deterministic call count:
        one initial attempt plus exactly _CHUNK_RETRY_LIMIT retries.
        """
        err = self._make_transient(retry_after=1)
        call_count = [0]

        def fn(_):
            call_count[0] += 1
            raise err

        with unittest.mock.patch("PowerPlatform.Dataverse.data._odata.time.sleep") as mock_sleep:
            with self.assertRaises(self._HttpError) as ctx:
                self._dispatch(fn, ["chunk"], max_workers=1)

        self.assertTrue(ctx.exception.is_transient)
        self.assertEqual(call_count[0], self._LIMIT + 1)  # 1 initial attempt + _LIMIT retries
        self.assertEqual(mock_sleep.call_count, self._LIMIT)

    def test_other_transient_codes_are_retried(self):
        """502, 503, and 504 HttpErrors with is_transient=True are retried just like 429."""
        for status in (502, 503, 504):
            bad_calls = [0]
            err = self._make_transient(status_code=status, retry_after=1)

            def fn(chunk, _err=err, _calls=bad_calls):
                if chunk == "bad":
                    _calls[0] += 1
                    if _calls[0] < 2:
                        raise _err
                return chunk

            with unittest.mock.patch("PowerPlatform.Dataverse.data._odata.time.sleep") as mock_sleep:
                result = self._dispatch(fn, ["bad", "ok"], max_workers=2)

            self.assertEqual(result, ["bad", "ok"], f"Expected retry to succeed for status={status}")
            self.assertEqual(bad_calls[0], 2, f"Expected exactly 2 fn calls (1 fail + 1 success) for status={status}")
            self.assertEqual(mock_sleep.call_count, 1, f"Expected exactly one retry sleep for status={status}")

    def test_non_transient_http_error_not_retried(self):
        """A non-transient HttpError (is_transient=False) propagates immediately without any retry.

        Two chunks ensure the concurrent path is used.  Only the "bad" chunk
        raises; it must be called exactly once with no sleep.
        """
        bad_calls = [0]
        err = self._HttpError("server error", status_code=500, is_transient=False)

        def fn(chunk):
            if chunk == "bad":
                bad_calls[0] += 1
                raise err
            return chunk

        with unittest.mock.patch("PowerPlatform.Dataverse.data._odata.time.sleep") as mock_sleep:
            with self.assertRaises(self._HttpError):
                self._dispatch(fn, ["bad", "ok"], max_workers=2)

        self.assertEqual(bad_calls[0], 1)
        mock_sleep.assert_not_called()

    def test_sequential_path_retries_on_transient_error(self):
        """The sequential path (max_workers=1) also retries transient errors.

        Before the retry refactor, only the concurrent path had retry logic.
        This verifies ``_execute_with_retry`` is applied to the sequential
        list-comprehension path as well.
        """
        calls = [0]
        err = self._make_transient(status_code=429, retry_after=2)

        def fn(_):
            calls[0] += 1
            if calls[0] < 3:
                raise err
            return "done"

        with unittest.mock.patch("PowerPlatform.Dataverse.data._odata.time.sleep") as mock_sleep:
            result = self._dispatch(fn, ["only"], max_workers=1)

        self.assertEqual(result, ["done"])
        self.assertEqual(calls[0], 3)
        self.assertEqual(mock_sleep.call_count, 2)

    def test_non_http_error_propagates_without_retry(self):
        """A non-HttpError exception (ValueError, RuntimeError, etc.) propagates immediately — no retry."""
        calls = [0]

        def fn(_):
            calls[0] += 1
            raise ValueError("logic error")

        with unittest.mock.patch("PowerPlatform.Dataverse.data._odata.time.sleep") as mock_sleep:
            with self.assertRaises(ValueError):
                self._dispatch(fn, ["only"], max_workers=1)

        self.assertEqual(calls[0], 1)
        mock_sleep.assert_not_called()


# ---------------------------------------------------------------------------
# max_workers validation in public API
# ---------------------------------------------------------------------------


class TestMaxWorkersValidation(unittest.TestCase):
    """records.create/update/upsert reject invalid max_workers values."""

    def setUp(self):
        from contextlib import contextmanager

        from PowerPlatform.Dataverse.operations.records import RecordOperations

        mock_client = MagicMock()
        mock_odata = MagicMock()
        mock_odata._entity_set_from_schema_name.return_value = "accounts"
        mock_odata._create_multiple.return_value = []

        @contextmanager
        def scoped():
            yield mock_odata

        mock_client._scoped_odata = scoped
        self.ops = RecordOperations(mock_client)

    def _assert_invalid(self, method, *args, **kwargs):
        with self.assertRaises(ValueError):
            method(*args, **kwargs)

    def test_create_zero_max_workers(self):
        """max_workers=0 is rejected — at least one worker is required."""
        self._assert_invalid(self.ops.create, "account", [{"name": "X"}], max_workers=0)

    def test_create_negative_max_workers(self):
        """Negative max_workers is rejected."""
        self._assert_invalid(self.ops.create, "account", [{"name": "X"}], max_workers=-1)

    def test_create_string_max_workers(self):
        """A string (e.g. "3") fails isinstance(int) and is rejected."""
        self._assert_invalid(self.ops.create, "account", [{"name": "X"}], max_workers="3")

    def test_update_zero_max_workers(self):
        """max_workers=0 is rejected for update, consistent with create."""
        self._assert_invalid(self.ops.update, "account", ["id-1"], {"name": "X"}, max_workers=0)

    def test_upsert_zero_max_workers(self):
        """max_workers=0 is rejected for upsert, consistent with create."""
        self._assert_invalid(
            self.ops.upsert,
            "account",
            [UpsertItem(alternate_key={"accountnumber": "A1"}, record={"name": "X"})],
            max_workers=0,
        )

    def test_create_above_cap_is_capped_not_rejected(self):
        """max_workers above _MAX_WORKERS is silently capped, not an error."""
        from PowerPlatform.Dataverse.data._odata import _MAX_WORKERS

        self.ops.create("account", [{"name": "X"}], max_workers=_MAX_WORKERS + 1)

    def test_create_float_max_workers_raises(self):
        """A float (e.g. 1.5) fails isinstance(int) even though it is numeric."""
        self._assert_invalid(self.ops.create, "account", [{"name": "X"}], max_workers=1.5)

    def test_create_false_max_workers_raises(self):
        """False equals 0, which fails the < 1 check despite isinstance(False, int) being True."""
        self._assert_invalid(self.ops.create, "account", [{"name": "X"}], max_workers=False)

    def test_create_true_max_workers_accepted(self):
        """True equals 1 and is a valid int subclass; accepted as max_workers=1 (sequential)."""
        self.ops.create("account", [{"name": "X"}], max_workers=True)


class TestDispatchChunksCap(unittest.TestCase):
    """_dispatch_chunks caps max_workers to _MAX_WORKERS and emits a UserWarning."""

    def setUp(self):
        from PowerPlatform.Dataverse.data._odata import _dispatch_chunks, _MAX_WORKERS

        self._dispatch = _dispatch_chunks
        self._cap = _MAX_WORKERS

    def test_above_cap_emits_warning(self):
        """max_workers above _MAX_WORKERS emits a UserWarning and still returns results."""
        called = []

        def fn(chunk):
            called.append(chunk)
            return chunk

        with self.assertWarns(UserWarning) as cm:
            result = self._dispatch(fn, ["a", "b"], max_workers=self._cap + 10)

        self.assertEqual(result, ["a", "b"])
        self.assertEqual(called, ["a", "b"])
        self.assertIn(str(self._cap + 10), str(cm.warning))
        self.assertIn(str(self._cap), str(cm.warning))

    def test_exactly_at_cap_no_warning(self):
        """max_workers == _MAX_WORKERS dispatches without capping or warning."""
        import warnings as _warnings

        with _warnings.catch_warnings():
            _warnings.simplefilter("error")
            results = self._dispatch(lambda c: c, ["x", "y"], max_workers=self._cap)
        self.assertEqual(results, ["x", "y"])

    def test_max_workers_1_is_accepted(self):
        """Minimum value max_workers=1 is accepted and runs sequentially."""
        results = self._dispatch(lambda c: c, ["a", "b", "c"], max_workers=1)
        self.assertEqual(results, ["a", "b", "c"])

    def test_below_cap_is_not_capped(self):
        """A max_workers value below _MAX_WORKERS is used as-is (no cap)."""
        assert self._cap >= 2, "_MAX_WORKERS must be >= 2 for this test"
        results = self._dispatch(lambda c: c, ["x", "y", "z"], max_workers=self._cap - 1)
        self.assertEqual(results, ["x", "y", "z"])

    def test_zero_raises_value_error(self):
        """max_workers=0 raises ValueError."""
        with self.assertRaises(ValueError):
            self._dispatch(lambda c: c, ["a"], max_workers=0)

    def test_negative_raises_value_error(self):
        """Negative max_workers raises ValueError."""
        with self.assertRaises(ValueError):
            self._dispatch(lambda c: c, ["a"], max_workers=-1)

    def test_non_int_raises_value_error(self):
        """Non-integer max_workers raises ValueError."""
        with self.assertRaises(ValueError):
            self._dispatch(lambda c: c, ["a"], max_workers="3")


# ---------------------------------------------------------------------------
# Picklist cache lock: concurrent cold-start
# ---------------------------------------------------------------------------


class TestPicklistCacheLock(unittest.TestCase):
    """Concurrent _bulk_fetch_picklists cold-starts should trigger only one HTTP call.

    ``_bulk_fetch_picklists`` uses double-checked locking: a lock-free fast path
    for the warm-cache case and a serialised slow path for cold-starts.  When
    many threads race to populate the cache simultaneously, only the first thread
    to acquire ``_picklist_cache_lock`` should make the metadata HTTP call; all
    others must observe the populated cache inside the lock and return without
    making a second request.
    """

    def test_concurrent_cold_start_fetches_once(self):
        """Eight threads racing on an empty cache must produce exactly one HTTP call.

        A ``threading.Barrier`` forces all threads to reach the cache-check
        simultaneously, guaranteeing a true race rather than relying on
        scheduler timing.  The mock sleeps briefly to keep the first thread
        inside the slow path while the others are blocked on the lock.
        """
        import threading

        NUM_THREADS = 8
        mock_auth = MagicMock()
        mock_auth._acquire_token.return_value = MagicMock(access_token="token")
        client = _ODataClient(mock_auth, "https://org.crm.dynamics.com")

        # _bulk_fetch_picklists calls _request_metadata_with_retry, not _request.
        # Mock that method to count invocations and return an empty picklist body.
        picklist_resp = MagicMock()
        picklist_resp.json.return_value = {"value": []}
        fetch_calls = [0]
        counter_lock = threading.Lock()

        def mock_metadata_request(*_):
            with counter_lock:
                fetch_calls[0] += 1
            time.sleep(0.01)  # hold the slow path open while others queue on the lock
            return picklist_resp

        client._request_metadata_with_retry = mock_metadata_request

        # Barrier forces all threads to enter _bulk_fetch_picklists simultaneously
        # so they all see an empty cache and race to acquire _picklist_cache_lock.
        start_barrier = threading.Barrier(NUM_THREADS)
        table = "account"

        def thread_fn():
            start_barrier.wait()
            client._bulk_fetch_picklists(table)

        threads = [threading.Thread(target=thread_fn) for _ in range(NUM_THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        self.assertEqual(fetch_calls[0], 1, "Expected exactly one HTTP call for concurrent cold-start")

    def test_warm_cache_does_not_refetch(self):
        """A second call after the cache is warm must not make another HTTP request."""
        mock_auth = MagicMock()
        mock_auth._acquire_token.return_value = MagicMock(access_token="token")
        client = _ODataClient(mock_auth, "https://org.crm.dynamics.com")

        fetch_calls = [0]
        picklist_resp = MagicMock()
        picklist_resp.json.return_value = {"value": []}

        def mock_metadata_request(*_):
            fetch_calls[0] += 1
            return picklist_resp

        client._request_metadata_with_retry = mock_metadata_request

        table = "account"
        client._bulk_fetch_picklists(table)  # cold start — triggers HTTP
        client._bulk_fetch_picklists(table)  # warm cache — must not re-fetch
        client._bulk_fetch_picklists(table)  # warm cache — must not re-fetch

        self.assertEqual(fetch_calls[0], 1, "Expected exactly one HTTP call — subsequent calls should hit the cache")

    def test_cold_start_exception_propagates_and_lock_is_released(self):
        """An exception raised during the cold-start fetch propagates to the caller and releases the lock.

        If _request_metadata_with_retry throws, the exception must escape
        _bulk_fetch_picklists (not be swallowed) and the lock must be released
        so that a subsequent call can retry the fetch successfully.  A broken
        implementation could swallow the exception or deadlock by holding the
        lock after the fetch throws.
        """
        mock_auth = MagicMock()
        mock_auth._acquire_token.return_value = MagicMock(access_token="token")
        client = _ODataClient(mock_auth, "https://org.crm.dynamics.com")

        call_count = [0]
        success_resp = MagicMock()
        success_resp.json.return_value = {"value": []}

        def flaky_fetch(*_):
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("simulated network error")
            return success_resp

        client._request_metadata_with_retry = flaky_fetch

        # First call: exception must propagate — not be swallowed
        with self.assertRaises(RuntimeError, msg="fetch exception must propagate to the caller"):
            client._bulk_fetch_picklists("account")

        # Lock must be released: a second call must be able to enter and succeed
        client._bulk_fetch_picklists("account")
        self.assertEqual(call_count[0], 2, "Second call must retry the fetch after the first failed")


if __name__ == "__main__":
    unittest.main()
